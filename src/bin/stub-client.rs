//! stub-client: runs on machine A next to OpenCode.
//!
//! Default mode (no subcommand): acts as `pathToClaudeCodeExecutable`. The SDK
//! spawns it with claude's argv; it dials the stub-server, sends a hello header
//! (argv + allowlisted env + cwd), then relays stdin/stdout byte-for-byte.
//! Everything above this pipe — control protocol, streaming, and the in-process
//! MCP bridge — is untouched.
//!
//! `sess` subcommand: one-shot session-projection RPC client (exists/messages/
//! fork/delete/list/merge), also usable by scripts to poke at machine B's
//! store.
//!
//! `sync` subcommand: bidirectional store mirror sync (ADR-0004). With
//! `CLAUDE_CONFIG_DIR` pointing at an A-local mirror of B's store, this makes
//! the Agent SDK's in-process store helpers (getSessionInfo / forkSession /
//! deleteSession — they read CLAUDE_CONFIG_DIR from the process environment)
//! work on the mirrored copy while the real store lives on B. Zero opencode
//! changes.

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
    if args.first().map(String::as_str) == Some("sync") {
        return sync_cli().await;
    }
    transport().await
}

async fn sess_rpc(endpoint: &str, req: &Value) -> Result<Value> {
    let stream = TcpStream::connect(endpoint).await.context("dial stub-server")?;
    let (mut rx, mut tx) = stream.into_split();
    proto::write_line(&mut tx, &json!({"proto": "sess-v1"})).await?;
    proto::write_line(&mut tx, req).await?;
    while let Some(line) = proto::read_line(&mut rx).await? {
        let value: Value = serde_json::from_str(&line)?;
        if value.get("id") == req.get("id") {
            if value.get("ok") == Some(&json!(true)) {
                return Ok(value);
            }
            bail!("sess {} failed: {}", req["op"], value["error"]);
        }
    }
    bail!("server closed without a response")
}

fn uuid_of(line: &Value) -> Option<&str> {
    line.get("uuid").and_then(Value::as_str)
}

/// Union by uuid: base order = existing file, then new incoming lines in
/// incoming order. Mirrors Store::merge on the client side of the mirror.
fn merge_lines(base: &mut Vec<Value>, incoming: Vec<Value>) -> usize {
    let mut seen: std::collections::HashSet<String> = base
        .iter()
        .filter_map(|l| uuid_of(l).map(str::to_string))
        .collect();
    let mut added = 0usize;
    for line in incoming {
        let Some(uuid) = uuid_of(&line) else { continue };
        if seen.insert(uuid.to_string()) {
            base.push(line);
            added += 1;
        }
    }
    added
}

fn write_mirror_lines(path: &std::path::Path, lines: &[Value]) -> Result<()> {
    let mut out = String::new();
    for line in lines {
        out.push_str(&line.to_string());
        out.push('\n');
    }
    std::fs::write(path, out).with_context(|| format!("writing {}", path.display()))
}

fn read_mirror_lines(path: &std::path::Path) -> Result<Vec<Value>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).context("mirror line is not JSON"))
        .collect()
}

fn mirror_root() -> Result<std::path::PathBuf> {
    std::env::var("CLAUDE_CONFIG_DIR")
        .context("CLAUDE_CONFIG_DIR (A-local mirror of B's store) is required for sync")
        .map(std::path::PathBuf::from)
}

/// Push: every *.jsonl under the mirror is merged into B's store under its
/// file stem. Pull: every session in B's store is merged into
/// `<mirror>/projects/lab/<session>.jsonl` (the SDK's helpers search all
/// project dirs, so the flat lab dir is enough).
async fn sync_once(endpoint: &str) -> Result<(usize, usize)> {
    let mirror = mirror_root()?;
    let mut pushed = 0usize;

    // ---- push ---------------------------------------------------------------
    let mut stack = vec![mirror.clone()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            if path.extension().is_none_or(|e| e != "jsonl") {
                continue;
            }
            let lines = read_mirror_lines(&path)?;
            let out = sess_rpc(
                endpoint,
                &json!({"id": 1, "op": "merge", "session": stem, "messages": lines}),
            )
            .await
            .with_context(|| format!("pushing {}", path.display()))?;
            pushed += out["added"].as_u64().unwrap_or(0) as usize;
        }
    }

    // ---- pull ---------------------------------------------------------------
    let mut pulled = 0usize;
    let sessions = sess_rpc(endpoint, &json!({"id": 1, "op": "list"}))
        .await?
        ["sessions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for session in sessions {
        let Some(session) = session.as_str().map(str::to_string) else {
            continue;
        };
        let remote = sess_rpc(
            endpoint,
            &json!({"id": 1, "op": "messages", "session": session}),
        )
        .await?["messages"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let dest = mirror.join("projects").join("lab");
        std::fs::create_dir_all(&dest)?;
        let path = dest.join(format!("{session}.jsonl"));
        let mut base = if path.exists() { read_mirror_lines(&path)? } else { Vec::new() };
        pulled += merge_lines(&mut base, remote);
        write_mirror_lines(&path, &base)?;
    }
    Ok((pushed, pulled))
}

async fn sync_cli() -> Result<()> {
    let endpoint = endpoint_from_env()?;
    let (pushed, pulled) = sync_once(&endpoint).await?;
    println!("sync ok: pushed {pushed} line(s), pulled {pulled} line(s)");
    Ok(())
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
    // Mirror mode (ADR-0004): deliver A-side forks to B before the turn, so a
    // resume/fork created by opencode's in-process SDK helpers is visible to
    // the claude child on B.
    if std::env::var("CLAUDE_STUB_MIRROR").is_ok_and(|v| v == "1") {
        sync_once(&endpoint).await.context("mirror push before relay")?;
    }
    let stream = TcpStream::connect(endpoint.as_str())
        .await
        .context("dial stub-server")?;
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
    // Post-turn pull, so opencode's next in-process store read (fork,
    // sessionExists) sees the messages the turn just wrote on B. Best-effort:
    // the model reply is already delivered; failing it now would only
    // destroy a completed turn.
    if std::env::var("CLAUDE_STUB_MIRROR").is_ok_and(|v| v == "1") {
        if let Err(e) = sync_once(&endpoint).await {
            eprintln!("stub-client: mirror pull after relay failed: {e:#}");
        }
    }
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

    let op = args.get(index).context("missing sess op (exists|messages|fork|delete|list|merge)")?;
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
        "list" => json!({"id": 1, "op": "list"}),
        "merge" => {
            let file = need(index + 2, "jsonl file")?;
            let raw = std::fs::read_to_string(file.as_str())
                .with_context(|| format!("reading {}", file))?;
            let messages: Vec<Value> = raw
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| serde_json::from_str(l).context("line is not JSON"))
                .collect::<Result<_, _>>()?;
            json!({"id": 1, "op": "merge", "session": need(index + 1, "session")?, "messages": messages})
        }
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
