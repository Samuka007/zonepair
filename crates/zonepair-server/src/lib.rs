//! stub-server's library half: the ws-line-v1 listener, session-store
//! projection and claude child spawn (machine B).

use anyhow::{bail, Context, Result};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub mod claudeproc;
pub mod http;
pub mod store;

use store::Store;

/// Run the listener with fully resolved options (flag > env > built-in
/// default is the binary's job).
pub async fn run(
    listen: String,
    token: Option<String>,
    store: Option<String>,
    claude: String,
    log_dir: Option<PathBuf>,
) -> Result<()> {
    let store = PathBuf::from(store.unwrap_or_else(|| {
        // std::env::home_dir (see stub-client's mirror_default note).
        let home = std::env::home_dir()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        format!("{home}/.claude")
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
