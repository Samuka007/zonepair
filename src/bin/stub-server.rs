//! stub-server: runs on machine B. One HTTP/WS entry (the `ws-line-v1`
//! carrier):
//!   WS /v1/stdio                    -> spawn one claude process per connection
//!   /v1/store/…                     -> session-store projection over HTTP
//!
//! Constraint enforced by construction: this binary only ever ACCEPTS
//! connections. There is no outbound dial anywhere in this file
//! (scripts/assert-nodial used to police it; the check lives in the Rust
//! suite now).

use anyhow::{bail, Context, Result};
use claude_io_stub::http;
use claude_io_stub::store::Store;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Each option has an environment fallback so the binary can run straight
    // from a container image with no wrapper script; an explicit flag always
    // wins over the environment. An empty value counts as unset.
    let env = |key: &str| std::env::var(key).ok().filter(|v| !v.is_empty());
    let mut listen = env("LISTEN").unwrap_or_else(|| "0.0.0.0:7789".to_string());
    let mut token = env("TOKEN");
    let mut store = env("STORE").or_else(|| env("CLAUDE_CONFIG_DIR"));
    let mut claude = env("CLAUDE_CMD").unwrap_or_else(|| "claude".to_string());
    let mut log_dir: Option<PathBuf> = env("LOG_DIR").map(PathBuf::from);

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => listen = args.next().context("--listen needs HOST:PORT")?,
            "--token" => token = Some(args.next().context("--token needs VALUE")?),
            "--store" => store = Some(args.next().context("--store needs DIR")?),
            "--claude" => claude = args.next().context("--claude needs \"CMD [ARG ...]\"")?,
            "--log-dir" => log_dir = Some(PathBuf::from(args.next().context("--log-dir needs DIR")?)),
            other => bail!("unknown arg {other}; usage: stub-server [--listen H:P] [--token VALUE] [--store DIR] [--claude \"CMD [ARG ...]\"] [--log-dir DIR]; unset options fall back to env LISTEN / TOKEN / STORE / CLAUDE_CMD / LOG_DIR"),
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

    let state = http::AppState {
        store: Arc::new(Store::new(store.clone())),
        claude_cmd: claude_cmd.clone(),
        log_dir: log_dir.clone(),
        token,
        seq: Arc::new(AtomicU64::new(0)),
    };

    eprintln!(
        "stub-server http/ws listening on {listen} store={} claude={:?} logs={}",
        store.display(),
        claude_cmd,
        log_dir.display()
    );
    http::serve(state, listen)
        .await
        .context("http/ws listener")
}
