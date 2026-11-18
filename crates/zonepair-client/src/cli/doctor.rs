//! doctor: preflight — endpoint, opencode server env, version, mirror.

use super::{mirror_default, parse_cli, resolve_token, resolve_url};
use crate::daemon::owner;
use crate::{api, transport};
use anyhow::{bail, Result};
use std::path::Path;

#[derive(clap::Parser)]
#[command(name = "doctor")]
struct DoctorCli {
    /// stub-server base URL (ws:// or http(s)://)
    #[arg(long)]
    url: Option<String>,
    /// Mirror directory to check instead of the configured/server one
    #[arg(long)]
    mirror: Option<String>,
}

const OPENCODE_MIN: (u64, u64, u64) = (2, 2, 3);

/// Heartbeat freshness bound for the liveness report: the daemon touches
/// `owner.lock`'s mtime every ~50 ms tick, so seconds-old means mid-cycle
/// at worst — anything past a minute is reported stale-but-held.
const HEARTBEAT_FRESH_SECS: u64 = 60;

enum Zonepaird {
    /// Lock held and heartbeat fresh.
    Live,
    /// Lock held but the mtime is old — hung or just-resumed writer.
    Stale(u64),
    /// No writer holds the lock (or `.sync` does not exist yet).
    Absent,
}

/// Probe-only liveness: the lock probe answers "is the lock held"
/// without ever spawning; freshness rides `owner.lock`'s mtime.
fn zonepaird_liveness(sync_dir: &Path) -> Zonepaird {
    let lock = sync_dir.join("owner.lock");
    if !lock.exists() || owner::lock_free(sync_dir) {
        return Zonepaird::Absent;
    }
    let age = lock
        .metadata()
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
        .map(|d| d.as_secs());
    match age {
        Some(a) if a > HEARTBEAT_FRESH_SECS => Zonepaird::Stale(a),
        _ => Zonepaird::Live,
    }
}

/// Parse `opencode --version` ("opencode v2.2.5") into a comparable triple.
fn parse_version(out: &str) -> Option<(u64, u64, u64)> {
    let token = out
        .split_whitespace()
        .find(|t| t.chars().next().is_some_and(|c| c.is_ascii_digit()) || t.starts_with('v'))?;
    let mut parts = token.trim_start_matches('v').split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next().unwrap_or("0").parse().ok()?;
    Some((major, minor, patch))
}

pub(crate) async fn doctor_cli(args: &[String]) -> Result<()> {
    let cli: DoctorCli = parse_cli("doctor", args);
    let mut failures = 0usize;
    let url_flag = cli.url;
    let explicit_mirror = cli.mirror;
    let url = resolve_url(url_flag)?;
    let base = transport::parse_base(&url)?;

    let warn = |msg: String| println!("  WARN  {msg}");
    let mut bad = |msg: String| {
        println!("  FAIL  {msg}");
        failures += 1;
    };

    println!("stub-client doctor  ({url})");

    // 1. the endpoint answers session-store HTTP at all
    let token = resolve_token(None);
    match api::store_list(&base, token.as_deref()).await {
        Ok(sessions) => println!(
            "  ok    store reachable ({} session(s) on B)",
            sessions.len()
        ),
        Err(e) => bad(format!("store unreachable: {e:#}")),
    }

    // 2. the opencode server must point the provider at a stub-client.
    //    This is the silent-degradation check: with the variable missing,
    //    opencode just runs a local `claude` that dials Anthropic directly.
    match std::process::Command::new("opencode")
        .args(["service", "get", "env", "OPENCODE_CLAUDE_PATH"])
        .output()
    {
        Ok(out) => {
            let value = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if value.is_empty() {
                bad("opencode server has no OPENCODE_CLAUDE_PATH — opencode would run a local claude directly, not through the stub".to_string());
            } else if !value.contains("stub-client") {
                bad(format!(
                    "OPENCODE_CLAUDE_PATH={value} does not look like stub-client"
                ));
            } else {
                println!("  ok    opencode server OPENCODE_CLAUDE_PATH={value}");
            }
        }
        Err(_) => warn("opencode not on PATH — cannot verify the server env".to_string()),
    }

    // 3. opencode new enough for the Claude Agent provider
    match std::process::Command::new("opencode")
        .arg("--version")
        .output()
    {
        Ok(out) => {
            let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
            match parse_version(&text) {
                Some(v) if v >= OPENCODE_MIN => println!("  ok    opencode {text}"),
                Some(v) => bad(format!(
                    "opencode {}.{}.{} < {}.{}.{} — the Claude Agent provider (and OPENCODE_CLAUDE_PATH) needs >= {}.{}.{}",
                    v.0, v.1, v.2, OPENCODE_MIN.0, OPENCODE_MIN.1, OPENCODE_MIN.2,
                    OPENCODE_MIN.0, OPENCODE_MIN.1, OPENCODE_MIN.2
                )),
                None => warn(format!("could not parse opencode version from {text:?}")),
            }
        }
        Err(_) => warn("opencode not on PATH — version unchecked".to_string()),
    }

    // 4. mirror present and non-empty, otherwise store helpers start blank.
    //    The authoritative value is the opencode server's CLAUDE_CONFIG_DIR
    //    (that is the process whose helpers read it).
    let server_mirror = std::process::Command::new("opencode")
        .args(["service", "get", "env", "CLAUDE_CONFIG_DIR"])
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty());
    let mirror = explicit_mirror
        .or(server_mirror)
        .unwrap_or_else(mirror_default);
    match std::fs::read_dir(&mirror) {
        Err(e) => warn(format!(
            "mirror {mirror} missing ({e}) — run `stub-client setup`"
        )),
        Ok(entries) => {
            let n = entries.filter_map(|e| e.ok()).count();
            if n == 0 {
                warn(format!(
                    "mirror {mirror} is empty — run `stub-client sync` once"
                ));
            } else {
                println!("  ok    mirror {mirror} ({n} entries)");
            }
        }
    }

    // 5. zonepaird liveness (RFC-0003-zonepaird §3): probe `.sync/owner.lock` —
    //    held means a writer owns the mirror; its heartbeat is the lock
    //    file's mtime, which the daemon refreshes every loop tick.
    //    Read-only by contract: doctor NEVER spawns a writer (§3.2's
    //    "doctor only probes, never launches"), it just reports what it finds.
    let sync_dir = crate::sync::persist::state::sync_dir(std::path::Path::new(&mirror));
    match zonepaird_liveness(&sync_dir) {
        Zonepaird::Live => println!("  ok    zonepaird holds .sync/owner.lock (heartbeat fresh)"),
        Zonepaird::Stale(age) => println!(
            "  ok    zonepaird holds .sync/owner.lock (heartbeat {age}s old — possibly mid-cycle)"
        ),
        Zonepaird::Absent => println!(
            "  ok    zonepaird idle (no writer; the next intent spawns one on demand)"
        ),
    }

    if failures > 0 {
        bail!("doctor found {failures} problem(s)");
    }
    println!("doctor: all checks passed");
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
    fn t14_doctor_flag_equals() {
        // --flag=value form (clap gain).
        let cli = try_parse_cli::<DoctorCli>(
            "doctor",
            &[s("--url=http://b:7789"), s("--mirror=/tmp/m")],
        )
        .unwrap();
        assert_eq!(cli.url.as_deref(), Some("http://b:7789"));
        assert_eq!(cli.mirror.as_deref(), Some("/tmp/m"));

        let cli = try_parse_cli::<DoctorCli>("doctor", &[]).unwrap();
        assert!(cli.url.is_none() && cli.mirror.is_none());

        let cli = try_parse_cli::<DoctorCli>(
            "doctor",
            &[s("--url"), s("ws://b"), s("--mirror"), s("/d")],
        )
        .unwrap();
        assert_eq!(cli.url.as_deref(), Some("ws://b"));

        assert!(try_parse_cli::<DoctorCli>("doctor", &[s("bogus")]).is_err());
    }
}
