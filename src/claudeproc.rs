//! Shared claude child spawn for both the raw and HTTP/WS listeners.

use anyhow::{Context, Result};
use crate::proto::StdioHeader;
use std::path::Path;
use std::process::Stdio;
use tokio::process::{Child, Command};

pub fn spawn(
    store: &Path,
    claude_cmd: &[String],
    log_dir: &Path,
    header: &StdioHeader,
    conn_tag: &str,
) -> Result<Child> {
    let mut cmd = Command::new(&claude_cmd[0]);
    cmd.args(&claude_cmd[1..]).args(&header.argv);

    let cwd = header.cwd.clone().unwrap_or_else(|| ".".to_string());
    std::fs::create_dir_all(&cwd).with_context(|| format!("creating cwd {cwd}"))?;
    cmd.current_dir(&cwd);

    // Server base env is minimal on purpose: the client's allowlisted env is
    // merged on top, and the child's session store is pinned to --store so a
    // real `claude` lands its JSONL where the projection RPC reads.
    cmd.env_clear();
    for key in ["PATH", "HOME", "LANG"] {
        if let Ok(value) = std::env::var(key) {
            cmd.env(key, value);
        }
    }
    for (key, value) in &header.env {
        cmd.env(key, value);
    }
    // Pinned LAST: the store is a server-side invariant, so a hello header
    // carrying A's own CLAUDE_CONFIG_DIR (e.g. the mirror dir) must not
    // redirect where the child writes its transcripts.
    cmd.env("CLAUDE_CONFIG_DIR", store);

    let log_path = log_dir.join(format!("stderr-{conn_tag}.log"));
    let log = std::fs::File::create(&log_path)?;
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(log)
        .kill_on_drop(true);

    cmd.spawn().with_context(|| format!("spawn {claude_cmd:?}"))
}
