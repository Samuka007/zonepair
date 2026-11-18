//! `zonepaird` subcommand: the daemon itself plus `zonepaird stop`
//! (RFC-0003-zonepaird §3/§6.4). The daemon does ONLY daemon work — the argv
//! adapter layer that writes intents on its behalf is T-09's, not
//! here.

use super::{parse_cli, resolve_mirror_dir, resolve_token, resolve_url};
use crate::daemon::{self, owner, startup, DaemonConfig, ExitReason};
use crate::sync::persist::journal::{self, Intent};
use crate::sync::state;
use crate::transport::parse_base;
use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

// ---- the intent enqueue path (RFC-0003-zonepaird §4.2) ----------------------------------
// Every short-lived process writing an intent runs this one sequence:
// `ensure_owner` first (a writer must exist to consume what we append),
// then the journal's five-step write. Shared by transport (E1/E2/E3),
// `sess delete` (E6), `sync` (E5) and `setup`'s bootstrap — the argv
// adapter layer upstream decides WHICH intents land; this is only the
// "make it durable + make sure somebody eats it" mechanics.

/// `ensure_owner` + `journal::append` (§4.2 steps 1–3). A failed
/// `ensure_owner` does NOT fail the enqueue (§3.4): the append is
/// durable on its own and the waiter's revive loop retries the spawn —
/// we log and continue. The append itself failing DOES propagate loudly:
/// an unconfirmed intent is worse than an immediate error.
pub(crate) fn enqueue(mirror: &Path, intent: Intent) -> Result<PathBuf> {
    let sync_dir = state::sync_dir(mirror);
    if let Err(e) = ensure_owner(&sync_dir, mirror) {
        eprintln!("stub-client: ensure_owner failed (the intent stays durable): {e:#}");
    }
    journal::append(&sync_dir.join("journal"), &intent)
}

/// §3.2's probe-and-spawn entry: `owner.lock` held → a live writer
/// exists, do nothing; free → spawn one detached `zonepaird` bound to
/// `mirror`. Spawn errors propagate — the caller decides whether
/// they're loud (`enqueue` logs and appends anyway).
pub(crate) fn ensure_owner(sync_dir: &Path, mirror: &Path) -> Result<()> {
    owner::ensure_owner_daemon(sync_dir, mirror).map(|_| ())
}

pub(crate) async fn zonepaird_cli(args: &[String]) -> Result<()> {
    match args.first().map(String::as_str) {
        Some("stop") => stop_cli(&args[1..]).await,
        _ => run_cli(args).await,
    }
}

#[derive(clap::Parser)]
#[command(name = "zonepaird")]
struct RunCli {
    /// A-local mirror directory
    #[arg(long)]
    mirror: Option<String>,
    /// stub-server base URL (ws:// or http(s)://)
    #[arg(long)]
    url: Option<String>,
    /// Bearer token; pass an empty string for none
    #[arg(long)]
    token: Option<String>,
    /// Boot then exit — no writer loop
    #[arg(long)]
    once: bool,
}

/// `zonepaird [--mirror <dir>] [--url <u>] [--token <t>] [--once]` —
/// the long-lived writer. Lock-held → silent exit (self-convergence,
/// §3.2): losing the race is not an error and must not spam logs.
async fn run_cli(args: &[String]) -> Result<()> {
    let cli: RunCli = parse_cli("zonepaird", args);
    let mirror = PathBuf::from(resolve_mirror_dir(cli.mirror));
    let token = resolve_token(cli.token);
    // The endpoint is optional by design: an unconfigured daemon still
    // replays journal and reconciles local state — workers degrade
    // ("no sync endpoint") instead of dying the moment a spawn can't
    // resolve a URL. A *malformed* configured URL is different: it is a
    // config error, not an absent one, and degrades loudly here rather
    // than silently syncing nothing forever.
    let base = match resolve_url(cli.url) {
        Ok(u) => Some(parse_base(&u).with_context(|| format!("malformed sync endpoint {u:?}"))?),
        Err(_) => None,
    };
    if base.is_none() {
        eprintln!("zonepaird: no sync endpoint configured — pulls/publishes degrade until one is set");
    }
    let cfg = DaemonConfig {
        once: cli.once,
        ..DaemonConfig::default()
    };
    let parts = daemon::exec_parts(base, token);
    match daemon::run(&mirror, parts, &cfg).await? {
        Err(startup::BootBlocked::AlreadyOwned) => Ok(()),
        Ok(ExitReason::Stopped) => Ok(()),
        Ok(ExitReason::BootedOnly) => Ok(()),
        Ok(reason @ (ExitReason::LockLost | ExitReason::HeartbeatExceeded)) => {
            // Abnormal yields exit nonzero — the self-convergence path
            // never sees them, a human looking at the daemon should.
            bail!("zonepaird exited abnormally: {reason:?}")
        }
    }
}

#[derive(clap::Parser)]
#[command(name = "zonepaird stop")]
struct StopCli {
    /// A-local mirror directory
    #[arg(long)]
    mirror: Option<String>,
}

/// `zonepaird stop [--mirror <dir>]`: write the stop marker for the
/// graceful exit, then escalate to SIGTERM on `owner.pid` (§3.3).
async fn stop_cli(args: &[String]) -> Result<()> {
    let cli: StopCli = parse_cli("zonepaird stop", args);
    let mirror = PathBuf::from(resolve_mirror_dir(cli.mirror));
    let sync_dir = state::ensure_sync_dir(&mirror)?;
    if daemon::stop_daemon(&sync_dir, &DaemonConfig::default())? {
        println!("zonepaird: stop requested");
    } else {
        println!("zonepaird: no writer running");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::try_parse_cli;

    fn s(v: &str) -> String {
        v.to_string()
    }

    #[test]
    fn t14_zonepaird_flags_and_flag_equals() {
        // --flag=value form (clap gain): the detached spawn still uses
        // `zonepaird --mirror <dir>`, but `--mirror=<dir>` parses too.
        let cli = try_parse_cli::<RunCli>(
            "zonepaird",
            &[
                s("--mirror=/tmp/m"),
                s("--url=ws://b:7789"),
                s("--token"),
                s(""),
                s("--once"),
            ],
        )
        .unwrap();
        assert_eq!(cli.mirror.as_deref(), Some("/tmp/m"));
        assert_eq!(cli.url.as_deref(), Some("ws://b:7789"));
        assert_eq!(cli.token.as_deref(), Some(""));
        assert!(cli.once);

        // No args: everything resolves from env/config at runtime.
        let cli = try_parse_cli::<RunCli>("zonepaird", &[]).unwrap();
        assert!(cli.mirror.is_none() && !cli.once);
        // Unknown args and bare words are clap errors now — a stray
        // `stop` after a flag still fails like the manual parser did.
        assert!(try_parse_cli::<RunCli>("zonepaird", &[s("bogus")]).is_err());
        assert!(try_parse_cli::<RunCli>("zonepaird", &[s("--once"), s("stop")]).is_err());
    }

    #[test]
    fn t14_zonepaird_stop_mirror_only() {
        let cli = try_parse_cli::<StopCli>("zonepaird stop", &[s("--mirror"), s("/d")]).unwrap();
        assert_eq!(cli.mirror.as_deref(), Some("/d"));
        assert!(try_parse_cli::<StopCli>("zonepaird stop", &[]).is_ok());
        assert!(try_parse_cli::<StopCli>("zonepaird stop", &[s("--once")]).is_err());
        assert!(try_parse_cli::<StopCli>("zonepaird stop", &[s("bogus")]).is_err());
    }
}

