//! stub-client: runs on machine A next to OpenCode.
//!
//! Default mode (no subcommand): acts as `pathToClaudeCodeExecutable`. The SDK
//! spawns it with claude's argv; it dials the stub-server, sends a hello header
//! (argv + allowlisted env + cwd), then relays stdin/stdout byte-for-byte.
//! Everything above this pipe — control protocol, streaming, and the in-process
//! MCP bridge — is untouched.
//!
//! `sess` subcommand: one-shot session-projection RPC client (exists/messages/
//! fork/delete), also usable by scripts to poke at machine B's store.

use anyhow::{bail, Context, Result};
use claude_io_stub::{envf, proto};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

fn endpoint_from_env() -> Result<String> {
    std::env::var("CLAUDE_STUB_ENDPOINT")
        .context("CLAUDE_STUB_ENDPOINT (host:port of stub-server) is required")
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("sess") {
        return sess_cli(&args[1..]).await;
    }
    if args.first().map(String::as_str) == Some("tunnel") {
        return tunnel_cli(&args[1..]).await;
    }
    transport().await
}

// MCP helper mode, run on machine B by claude's --mcp-config stdio server.
// Dials the server on B's OWN loopback; the server pairs this stream with an
// A-initiated tunnel, so tool execution happens on A without any B->A dial.
// ws-line-v1: frames are exactly one MCP JSON-RPC line each (text frames).
async fn tunnel_cli(args: &[String]) -> Result<()> {
    let mut url: Option<String> = None;
    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--url" => {
                url = Some(args.get(i + 1).context("--url needs ws(s)://URL")?.clone());
                i += 2;
            }
            "--endpoint" => {
                i += 2; // legacy flag, ignored: the ws url carries the address
            }
            other => bail!("unknown tunnel arg {other}; usage: tunnel --url ws://host/v1/tunnel?side=b"),
        }
    }
    let url = url.context("--url ws(s)://URL required")?;

    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (ws, _resp) = tokio_tungstenite::connect_async(url)
        .await
        .with_context(|| "ws connect for tunnel")?;
    let (mut sink, mut stream) = ws.split();

    let mut stdout = tokio::io::stdout();
    let up = tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
        let mut buf = String::new();
        loop {
            buf.clear();
            use tokio::io::AsyncBufReadExt;
            match reader.read_line(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let line = buf.trim_end_matches('\n').to_string();
                    if sink.send(Message::Text(line)).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    while let Some(msg) = stream.next().await {
        match msg? {
            Message::Text(t) => {
                stdout.write_all(t.as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    up.abort();
    // Same Stdin blocking-worker rationale as transport mode.
    std::process::exit(0);
}

async fn transport() -> Result<()> {
    let endpoint = endpoint_from_env()?;
    let stream = TcpStream::connect(endpoint).await.context("dial stub-server")?;
    let (rx, mut tx) = stream.into_split();
    let hello = json!({
        "proto": "stdio-v1",
        "argv": std::env::args().skip(1).collect::<Vec<_>>(),
        "env": envf::collect(),
        "cwd": std::env::current_dir()?.to_string_lossy(),
    });
    proto::write_line(&mut tx, &hello).await?;

    let mut rx = rx;
    let ack = proto::read_line(&mut rx).await?.context("server closed before ack")?;
    let ack: Value = serde_json::from_str(&ack)?;
    if ack.get("ok") != Some(&json!(true)) {
        bail!(
            "stub-server refused: {}",
            ack.get("error").cloned().unwrap_or(json!("unknown error"))
        );
    }

    // Raw relay from here on: stdout carries nothing but child bytes, so the
    // SDK control protocol (including MCP tool calls) is transparent.
    let mut stdout = tokio::io::stdout();
    let up = tokio::spawn(async move {
        let mut stdin = tokio::io::stdin();
        let _ = tokio::io::copy(&mut stdin, &mut tx).await;
        let _ = tx.shutdown().await;
    });
    tokio::io::copy(&mut rx, &mut stdout).await?;
    stdout.flush().await?;
    up.abort();
    // Hard exit: the relay's job is done and machine A's stdin may still be
    // open. Tokio's Stdin worker is a blocking-pool thread parked in read(),
    // and a normal runtime shutdown would join it forever.
    std::process::exit(0);
}

async fn sess_cli(args: &[String]) -> Result<()> {
    let mut endpoint = std::env::var("CLAUDE_STUB_ENDPOINT").ok();
    let mut index = 0usize;
    if args.first().map(String::as_str) == Some("--endpoint") {
        endpoint = Some(args.get(1).context("--endpoint needs HOST:PORT")?.clone());
        index = 2;
    }
    let endpoint = endpoint.context("CLAUDE_STUB_ENDPOINT or --endpoint required")?;

    let op = args.get(index).context("missing sess op (exists|messages|fork|delete)")?;
    let need = |n: usize, what: &str| -> Result<&String> {
        args.get(n).with_context(|| format!("sess {op} needs {what}"))
    };
    let req = match op.as_str() {
        "exists" => json!({"id": 1, "op": "exists", "session": need(index + 1, "session")?}),
        "messages" => json!({"id": 1, "op": "messages", "session": need(index + 1, "session")?}),
        "fork" => json!({
            "id": 1,
            "op": "fork",
            "session": need(index + 1, "session")?,
            "upTo": need(index + 2, "upTo")?,
        }),
        "delete" => json!({"id": 1, "op": "delete", "session": need(index + 1, "session")?}),
        other => bail!("unknown sess op {other}"),
    };

    let stream = TcpStream::connect(endpoint).await.context("dial stub-server")?;
    let (mut rx, mut tx) = stream.into_split();
    proto::write_line(&mut tx, &json!({"proto": "sess-v1"})).await?;
    proto::write_line(&mut tx, &req).await?;
    while let Some(line) = proto::read_line(&mut rx).await? {
        let value: Value = serde_json::from_str(&line)?;
        if value.get("id").and_then(Value::as_u64) == Some(1) {
            println!("{}", serde_json::to_string_pretty(&value)?);
            if value.get("ok") == Some(&json!(true)) {
                return Ok(());
            }
            std::process::exit(2);
        }
    }
    bail!("server closed without a response")
}
