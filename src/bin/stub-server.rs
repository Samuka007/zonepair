//! stub-server: runs on machine B. Accepts two connection types on one port:
//!   {"proto":"stdio-v1",...}  -> spawn one claude process per connection, raw relay
//!   {"proto":"sess-v1"}       -> JSON-lines session-store projection RPC
//!
//! Constraint enforced by construction: this binary only ever ACCEPTS
//! connections. There is no outbound dial anywhere in this file.

use anyhow::{bail, Context, Result};
use claude_io_stub::claudeproc;
use claude_io_stub::http;
use claude_io_stub::proto::{self, Hello, StdioHeader};
use claude_io_stub::store::Store;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;
use tokio::process::Child;

#[derive(Clone)]
struct Cfg {
    store: PathBuf,
    claude_cmd: Vec<String>,
    log_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Each option has an environment fallback so the binary can run straight
    // from a container image with no wrapper script; an explicit flag always
    // wins over the environment. An empty value counts as unset.
    let env = |key: &str| std::env::var(key).ok().filter(|v| !v.is_empty());
    let mut listen = "0.0.0.0:7788".to_string();
    let mut http_listen = env("HTTP_LISTEN");
    let mut http_token = env("HTTP_TOKEN");
    let mut store = env("STORE").or_else(|| env("CLAUDE_CONFIG_DIR"));
    let mut claude = env("CLAUDE_CMD").unwrap_or_else(|| "claude".to_string());
    let mut log_dir: Option<PathBuf> = env("LOG_DIR").map(PathBuf::from);

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => listen = args.next().context("--listen needs HOST:PORT")?,
            "--http-listen" => http_listen = Some(args.next().context("--http-listen needs HOST:PORT")?),
            "--http-token" => http_token = Some(args.next().context("--http-token needs VALUE")?),
            "--store" => store = Some(args.next().context("--store needs DIR")?),
            "--claude" => claude = args.next().context("--claude needs \"CMD [ARG ...]\"")?,
            "--log-dir" => log_dir = Some(PathBuf::from(args.next().context("--log-dir needs DIR")?)),
            other => bail!("unknown arg {other}; usage: stub-server [--listen H:P] [--store DIR] [--claude \"CMD [ARG ...]\"] [--log-dir DIR] [--http-listen H:P] [--http-token VALUE]; unset options fall back to env STORE / CLAUDE_CMD / LOG_DIR / HTTP_LISTEN / HTTP_TOKEN"),
        }
    }

    let store = PathBuf::from(store.unwrap_or_else(|| {
        format!("{}/.claude", std::env::var("HOME").unwrap_or_default())
    }));
    let log_dir = log_dir.unwrap_or_else(|| std::env::current_dir().unwrap().join("stub-logs"));
    std::fs::create_dir_all(&log_dir)?;
    let claude_cmd: Vec<String> = claude.split_whitespace().map(String::from).collect();
    if claude_cmd.is_empty() {
        bail!("--claude resolved to an empty command");
    }
    let cfg = Cfg { store: store.clone(), claude_cmd, log_dir };
    let store = Arc::new(Store::new(cfg.store.clone()));
    let seq = Arc::new(AtomicU64::new(0));

    if let Some(addr) = &http_listen {
        let state = http::AppState {
            store: store.clone(),
            claude_cmd: cfg.claude_cmd.clone(),
            log_dir: cfg.log_dir.clone(),
            token: http_token,
            seq: seq.clone(),
            tunnels: Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        };
        tokio::spawn(http::serve(state, addr.clone()));
    }

    let listener = TcpListener::bind(&listen).await?;
    eprintln!(
        "stub-server listening on {listen} store={} claude={:?} logs={}",
        cfg.store.display(),
        cfg.claude_cmd,
        cfg.log_dir.display()
    );
    loop {
        let (stream, peer) = listener.accept().await?;
        let cfg = cfg.clone();
        let store = store.clone();
        let seq = seq.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, cfg, store, seq).await {
                eprintln!("conn from {peer}: {e:#}");
            }
        });
    }
}

async fn handle(
    stream: tokio::net::TcpStream,
    cfg: Cfg,
    store: Arc<Store>,
    seq: Arc<AtomicU64>,
) -> Result<()> {
    let (rx, mut tx) = stream.into_split();
    let mut rx = rx;
    let first = proto::read_line(&mut rx).await?;
    let Some(first) = first else { return Ok(()) };
    let hello = match proto::parse_hello(&first) {
        Ok(h) => h,
        Err(e) => {
            proto::write_line(&mut tx, &json!({"ok": false, "error": e.to_string()})).await.ok();
            return Ok(());
        }
    };
    match hello {
        Hello::Sess => sess_loop(rx, tx, store).await,
        Hello::Stdio(header) => stdio_conn(rx, tx, header, cfg, seq).await,
        Hello::Tunnel { .. } => {
            bail!("tunnel-v1 moved to the ws-line-v1 listener (--http-listen)")
        }
    }
}

#[derive(serde::Deserialize)]
struct Req {
    id: u64,
    op: String,
    #[serde(default)]
    session: Option<String>,
    #[serde(default, rename = "upTo")]
    up_to: Option<String>,
    #[serde(default)]
    messages: Option<Vec<serde_json::Value>>,
}

async fn sess_loop(mut rx: OwnedReadHalf, mut tx: OwnedWriteHalf, store: Arc<Store>) -> Result<()> {
    while let Some(line) = proto::read_line(&mut rx).await? {
        let req: Req = match serde_json::from_str(&line) {
            Ok(req) => req,
            Err(e) => {
                proto::write_line(&mut tx, &json!({"id": 0, "ok": false, "error": format!("bad request: {e}")}))
                    .await?;
                continue;
            }
        };
        let id = req.id;
        let mut resp = match run_op(&store, &req).await {
            Ok(v) => v,
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        };
        resp["id"] = json!(id);
        proto::write_line(&mut tx, &resp).await?;
    }
    Ok(())
}

async fn run_op(store: &Store, req: &Req) -> Result<Value> {
    match req.op.as_str() {
        "exists" => {
            let session = req.session.as_deref().context("exists needs session")?;
            Ok(json!({"ok": true, "exists": store.exists(session).await}))
        }
        "messages" => {
            let session = req.session.as_deref().context("messages needs session")?;
            let messages = store.messages(session).await?;
            Ok(json!({"ok": true, "messages": messages}))
        }
        "fork" => {
            let session = req.session.as_deref().context("fork needs session")?;
            let up_to = req.up_to.as_deref().context("fork needs upTo")?;
            let out = store.fork(session, up_to).await?;
            Ok(json!({
                "ok": true,
                "sessionId": out.session_id,
                "pairs": out.pairs,
                "messages": out.messages,
            }))
        }
        "delete" => {
            let session = req.session.as_deref().context("delete needs session")?;
            let deleted = store.delete(session).await?;
            Ok(json!({"ok": true, "deleted": deleted}))
        }
        "list" => Ok(json!({"ok": true, "sessions": store.list().await?})),
        "merge" => {
            let session = req.session.as_deref().context("merge needs session")?;
            let messages = req.messages.as_deref().context("merge needs messages")?;
            let out = store.merge(session, messages).await?;
            Ok(json!({"ok": true, "added": out.added, "total": out.total}))
        }
        other => bail!("unknown op {other:?}"),
    }
}

async fn stdio_conn(
    mut rx: OwnedReadHalf,
    mut tx: OwnedWriteHalf,
    header: StdioHeader,
    cfg: Cfg,
    seq: Arc<AtomicU64>,
) -> Result<()> {
    let id = seq.fetch_add(1, Ordering::Relaxed);
    let mut child: Child = match claudeproc::spawn(
        &cfg.store,
        &cfg.claude_cmd,
        &cfg.log_dir,
        &header,
        &id.to_string(),
    ) {
        Ok(child) => child,
        Err(e) => {
            proto::write_line(&mut tx, &json!({"ok": false, "error": e.to_string()})).await.ok();
            return Ok(());
        }
    };
    proto::write_line(&mut tx, &json!({"ok": true})).await?;

    let mut child_stdin = child.stdin.take().context("child stdin")?;
    let mut child_stdout = child.stdout.take().context("child stdout")?;

    // Client stdin -> child stdin. On client EOF we close the child's stdin so
    // the (protocol-driven) claude process finishes and exits on its own.
    let up = tokio::spawn(async move {
        let _ = tokio::io::copy(&mut rx, &mut child_stdin).await;
        let _ = child_stdin.shutdown().await;
    });
    // Child stdout -> client. Closed only after the child exits, so the
    // control protocol stream stays byte-pristine for MCP round trips.
    let down = tokio::spawn(async move {
        let _ = tokio::io::copy(&mut child_stdout, &mut tx).await;
        let _ = tx.shutdown().await;
    });

    let status = child.wait().await?;
    let _ = up.await;
    let _ = down.await;
    eprintln!("conn #{id} child exit {:?}", status.code());
    Ok(())
}
