//! stub-client: runs on machine A next to OpenCode. Speaks only the
//! `ws-line-v1` carrier (see ADR-0005): stdio rides a WS stream, session
//! operations ride plain HTTP. There is no raw-TCP path anywhere in this
//! binary.
//!
//! Default mode (no subcommand): acts as `pathToClaudeCodeExecutable`. The SDK
//! spawns it with claude's argv; it dials the server's `/v1/stdio` WS, sends a
//! hello header (argv + allowlisted env + cwd), then relays stdin/stdout one
//! line per frame. Everything above this pipe — control protocol, streaming,
//! and the in-process MCP bridge — is untouched.
//!
//! `sess` subcommand: one-shot session-projection client (exists/messages/
//! fork/delete/list/merge) over the server's HTTP store endpoints.
//!
//! `sync` subcommand: the manual E5 trigger — writes a `reconcile{scope:"all"}`
//! journal intent; `--wait` polls the RFC-0002-a-state §9 structural drain predicate.
//! The real sync is pull-model now (RFC-0001-sync-protocol–0003): `zonepaird` is the sole
//! writer, short-lived processes only ever append intents.
//!
//! `zonepaird`: the sole-writer daemon (RFC-0003-zonepaird §3) — normally spawned
//! detached by `ensure_owner`; also runnable in the foreground.
//!
//! `prune`/`hydrate`: the materialization commands (RFC-0003-zonepaird §6.4) —
//! journal intents only (`evict`/`hydrate`); zonepaird executes them.
//!
//! `setup`: one-shot configuration for machine A. Interactive on a TTY —
//! prompts for URL / token / mirror dir with the currently resolved values
//! prefilled — or strict-flags non-interactively (`--url --token --mirror`
//! all together, `--token ""` = no token; anything partial fails without
//! writing). Stores the relay knobs in the client config file
//! (`zonepair/client.toml` under the platform config dir, ADR-0008) and
//! writes only the two keys
//! opencode itself consumes into the opencode server env
//! (`OPENCODE_CLAUDE_PATH`, `CLAUDE_CONFIG_DIR`).
//!
//! `unset`: the inverse of `setup` — erases the client config file and
//! removes the opencode server env keys it wrote.
//!
//! `doctor`: preflight — checks the endpoint, the opencode server env,
//! opencode's version and the mirror.


mod doctor;
mod hydrate;
mod prune;
mod sess;
mod setup;
mod sync;
mod transport;
#[cfg(test)]
mod testkit;
mod zonepaird;

use anyhow::{bail, Result};
use crate::cfg;

// ---- config resolution -------------------------------------------------------
// One order everywhere: flag > process env > client config file > built-in
// default (ADR-0008). The opencode server env is NOT a read source — only the
// two keys opencode itself consumes live there, written by `setup`.

pub(crate) fn nonempty(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.is_empty())
}

/// Parse a subcommand's argv (everything after the already-dispatched
/// subcommand name) with clap. `name` becomes argv[0] so usage reads
/// `name …`. Parse errors print clap's usage and exit 2; `--help` exits 0.
pub(crate) fn parse_cli<T: clap::Parser>(name: &str, args: &[String]) -> T {
    try_parse_cli(name, args).unwrap_or_else(|e| e.exit())
}

/// Fallible variant for tests: same argv shape, `Err` carries clap's
/// rendered diagnostic instead of exiting.
pub(crate) fn try_parse_cli<T: clap::Parser>(
    name: &str,
    args: &[String],
) -> std::result::Result<T, clap::Error> {
    let argv = std::iter::once(name.to_string()).chain(args.iter().cloned());
    T::try_parse_from(argv)
}

pub(crate) fn resolve_url(flag: Option<String>) -> Result<String> {
    if let Some(v) = nonempty(flag) {
        return Ok(v);
    }
    if let Some(v) = nonempty(std::env::var("CLAUDE_STUB_URL").ok()) {
        return Ok(v);
    }
    let file = cfg::load()?;
    if let Some(v) = nonempty(file.url) {
        return Ok(v);
    }
    bail!(
        "no CLAUDE_STUB_URL: pass --url, export CLAUDE_STUB_URL, or run `stub-client setup` (it writes {})",
        cfg::path()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "the client config file".to_string())
    );
}

pub(crate) fn resolve_token(flag: Option<String>) -> Option<String> {
    nonempty(flag)
        .or_else(|| nonempty(std::env::var("CLAUDE_STUB_TOKEN").ok()))
        .or_else(|| cfg::load().ok().and_then(|c| nonempty(c.token)))
}

pub(crate) fn resolve_mirror_dir(flag: Option<String>) -> String {
    nonempty(flag)
        .or_else(|| nonempty(std::env::var("CLAUDE_CONFIG_DIR").ok()))
        .or_else(|| cfg::load().ok().and_then(|c| nonempty(c.mirror_dir)))
        .unwrap_or_else(mirror_default)
}

pub(crate) fn mirror_enabled() -> bool {
    if std::env::var("CLAUDE_STUB_MIRROR").is_ok_and(|v| v == "1") {
        return true;
    }
    cfg::load()
        .map(|c| c.mirror.unwrap_or(false))
        .unwrap_or(false)
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("sess") => sess::sess_cli(&args[1..]).await,
        Some("sync") => sync::sync_cli(&args[1..]).await,
        Some("setup") => setup::setup_cli(&args[1..]).await,
        Some("unset") => setup::unset_cli(&args[1..]).await,
        Some("doctor") => doctor::doctor_cli(&args[1..]).await,
        Some("zonepaird") => zonepaird::zonepaird_cli(&args[1..]).await,
        Some("prune") => prune::prune_cli(&args[1..]),
        Some("hydrate") => hydrate::hydrate_cli(&args[1..]),
        _ => transport::transport(&args).await,
    }
}

pub(crate) fn mirror_default() -> String {
    // dirs::home_dir is USERPROFILE-first on windows, where HOME is typically
    // unset — hand rolling this once produced a root-relative
    // "/.claude-remote-mirror" there. unwrap_or_default keeps the flag/env
    // overrides able to rescue an exotic environment, same as before.
    let home = dirs::home_dir()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    format!("{home}/.claude-remote-mirror")
}
