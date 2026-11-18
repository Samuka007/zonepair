//! setup / unset: one-shot configuration for machine A (ADR-0008).

use super::doctor::doctor_cli;
use super::{nonempty, parse_cli, resolve_mirror_dir, resolve_token, resolve_url};
use crate::{api, cfg, transport};
use anyhow::{bail, Context, Result};

/// The only keys setup writes into the **opencode server's** environment: the
/// two opencode itself consumes (the provider path and the mirror dir its
/// in-process SDK helpers read). The relay's own knobs live in the client
/// config file (ADR-0008); the server env is not a read source. A shell export
/// is not enough either way: opencode usually runs as a daemon that never sees
/// the caller's shell, and stub-client is spawned by that server.
const OPENCODE_ENV_KEYS: [&str; 2] = ["OPENCODE_CLAUDE_PATH", "CLAUDE_CONFIG_DIR"];

#[derive(clap::Parser)]
#[command(name = "setup")]
struct SetupCli {
    /// stub-server base URL (ws:// or http(s)://)
    #[arg(long)]
    url: Option<String>,
    /// Bearer token; pass an empty string for none
    #[arg(long)]
    token: Option<String>,
    /// A-local mirror directory
    #[arg(long)]
    mirror: Option<String>,
}

pub(crate) async fn setup_cli(args: &[String]) -> Result<()> {
    let cli: SetupCli = parse_cli("setup", args);
    let [url_f, token_f, mirror_f] = [cli.url, cli.token, cli.mirror];
    let given = [url_f.is_some(), token_f.is_some(), mirror_f.is_some()]
        .into_iter()
        .filter(|given| *given)
        .count();

    let (url, token_raw, mirror_dir) = match given {
        0 => {
            // Interactive. Refuse without a TTY so scripts fail loudly instead
            // of hanging on a hidden prompt.
            use std::io::IsTerminal;
            if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
                bail!(
                    "interactive setup needs a TTY; pass all of --url URL --token TOKEN --mirror DIR (empty token: --token \"\")"
                );
            }
            let theme = dialoguer::theme::ColorfulTheme::default();
            let url: String = dialoguer::Input::with_theme(&theme)
                .with_prompt("stub-server URL")
                .default(resolve_url(None).unwrap_or_else(|_| "ws://127.0.0.1:7789".to_string()))
                .interact_text()
                .context("url prompt")?;
            let current_token = resolve_token(None).unwrap_or_default();
            let token_raw = if current_token.is_empty() {
                dialoguer::Password::new()
                    .with_prompt("Bearer token (empty = none)")
                    .allow_empty_password(true)
                    .report(false)
                    .interact()
                    .context("token prompt")?
            } else {
                let entered = dialoguer::Password::new()
                    .with_prompt("Bearer token (empty = keep current)")
                    .allow_empty_password(true)
                    .report(false)
                    .interact()
                    .context("token prompt")?;
                if entered.is_empty() { current_token } else { entered }
            };
            let mirror_dir: String = dialoguer::Input::with_theme(&theme)
                .with_prompt("Mirror directory (A-local copy of B's store)")
                .default(resolve_mirror_dir(None))
                .interact_text()
                .context("mirror prompt")?;
            (url, token_raw, mirror_dir)
        }
        3 => (url_f.unwrap(), token_f.unwrap(), mirror_f.unwrap()),
        _ => bail!(
            "setup is all-or-nothing: pass --url, --token (may be \"\") and --mirror together, or nothing for interactive setup"
        ),
    };

    // Validate before the first side effect: nothing is written unless the URL
    // parses and all three values are present.
    let base = transport::parse_base(&url)?;
    let token = nonempty(Some(token_raw));

    // Our store first, then the two opencode keys.
    let written = cfg::save(&cfg::ClientConfig {
        url: Some(url.clone()),
        token: token.clone(),
        mirror_dir: Some(mirror_dir.clone()),
        mirror: Some(true),
        ..Default::default()
    })?;
    println!("client config {}", written.display());

    std::fs::create_dir_all(&mirror_dir)
        .with_context(|| format!("creating mirror dir {mirror_dir}"))?;

    let exe = std::env::current_exe()
        .context("resolving our own path")?
        .canonicalize()
        .context("canonicalizing our own path")?
        .to_string_lossy()
        .to_string();
    // Presence probe: `service set env PROBE PROBE` proves an opencode with a
    // working `service` subcommand is on PATH. `service set env NAME VALUE` —
    // the key is the literal string "env" and the variable name is the second
    // argument.
    let probe = std::process::Command::new("opencode")
        .args(["service", "set", "env", "PROBE", "PROBE"])
        .output();
    match probe {
        Ok(_) => {
            for (key, value) in [
                ("OPENCODE_CLAUDE_PATH", &exe),
                ("CLAUDE_CONFIG_DIR", &mirror_dir),
            ] {
                // Capture output: opencode's subcommands are chatty on stdout
                // (`service restart` prints its listen URL) and would
                // otherwise pollute our transcript.
                let out = std::process::Command::new("opencode")
                    .args(["service", "set", "env", key, value])
                    .output()
                    .with_context(|| format!("opencode service set env {key}"))?;
                if !out.status.success() {
                    bail!(
                        "opencode service set env {key} failed ({:?}); set it by hand",
                        out.status.code()
                    );
                }
                println!("opencode server env {key}={value}");
            }
            // Drop the probe, then restart so a running daemon picks the new
            // environment up.
            let _ = std::process::Command::new("opencode")
                .args(["service", "unset", "env", "PROBE"])
                .output();
            let _ = std::process::Command::new("opencode")
                .arg("service")
                .arg("restart")
                .output();
        }
        Err(_) => {
            println!("opencode not on PATH — export these in whatever starts it:");
            println!("  export OPENCODE_CLAUDE_PATH={exe}");
            println!("  export CLAUDE_CONFIG_DIR={mirror_dir}");
        }
    }

    // Bootstrap the mirror so opencode's in-process store helpers start from
    // B's real state rather than an empty directory. Two pieces: the store
    // endpoint must answer at all (the connectivity probe — a dead B is
    // worth reporting now, not at first sync), and a `reconcile` intent
    // lands the real work on the newly configured mirror's journal (the
    // daemon pulls everything cold).
    match api::store_list(&base, token.as_deref()).await {
        Ok(sessions) => {
            println!(
                "mirror bootstrap: store reachable ({} session(s) to pull)",
                sessions.len()
            );
            match super::zonepaird::enqueue(
                std::path::Path::new(&mirror_dir),
                crate::sync::persist::journal::Intent::Reconcile {
                    enqueued_at: crate::sync::persist::state::now_rfc3339(),
                },
            ) {
                Ok(_) => println!("mirror bootstrap: reconcile intent enqueued"),
                Err(e) => println!("mirror bootstrap skipped: {e:#}"),
            }
        }
        Err(e) => {
            let msg = format!("{e:#}");
            if msg.contains("401") && token.is_none() {
                println!("mirror bootstrap skipped: {msg} — the server requires a token; re-run setup and enter one");
            } else {
                println!("mirror bootstrap skipped: {msg}");
            }
        }
    }

    println!();
    doctor_cli(&[
        "--url".to_string(),
        url.clone(),
        "--mirror".to_string(),
        mirror_dir.clone(),
    ])
    .await
}
/// Reverse of `setup`: erase the client config file and remove exactly the two
/// keys opencode itself consumes (plus the `PROBE` probe key a failed setup
/// can leave behind), then restart so a running daemon stops inheriting them.
/// Takes no `--url` and dials nothing — decommissioning B must not depend on B
/// answering. Legacy `CLAUDE_STUB_*` keys in the opencode server env predate
/// the client config file; migration is manual (docs/CLIENT.md) and `unset`
/// does not touch them. The mirror directory is user data and is read nowhere
/// here.
pub(crate) async fn unset_cli(args: &[String]) -> Result<()> {
    parse_cli::<UnsetCli>("unset", args);
    // The config file is ours; erase it before touching opencode so a failed
    // opencode step leaves a consistent, retryable "no config" state.
    match cfg::erase() {
        Ok(true) => println!("removed {}", cfg::path()?.display()),
        Ok(false) => {}
        Err(e) => bail!("removing the client config failed: {e}"),
    }
    // Presence probe, same trick as setup: setting PROBE proves an opencode
    // with a working `service` subcommand is on PATH. If a later removal fails
    // midway, PROBE lingers exactly like after a failed setup — re-running
    // unset sweeps it (removal is idempotent).
    let probe = std::process::Command::new("opencode")
        .args(["service", "set", "env", "PROBE", "PROBE"])
        .output();
    match probe {
        Ok(_) => {
            for key in OPENCODE_ENV_KEYS {
                let status = std::process::Command::new("opencode")
                    .args(["service", "unset", "env", key])
                    .status()
                    .with_context(|| format!("opencode service unset env {key}"))?;
                if !status.success() {
                    bail!("opencode service unset env {key} failed ({status}); unset it by hand");
                }
                println!("removed {key}");
            }
            let status = std::process::Command::new("opencode")
                .args(["service", "unset", "env", "PROBE"])
                .status()
                .context("opencode service unset env PROBE")?;
            if !status.success() {
                bail!("opencode service unset env PROBE failed ({status})");
            }
            println!("removed PROBE");
            let _ = std::process::Command::new("opencode")
                .arg("service")
                .arg("restart")
                .output();
        }
        Err(_) => {
            println!("opencode not on PATH — unset these in whatever starts it:");
            for key in OPENCODE_ENV_KEYS {
                println!("  unset {key}");
            }
        }
    }
    Ok(())
}

#[derive(clap::Parser)]
#[command(name = "unset")]
struct UnsetCli {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::try_parse_cli;

    fn s(v: &str) -> String {
        v.to_string()
    }

    #[test]
    fn t14_setup_flag_equals_and_all_or_nothing() {
        // --flag=value form (clap gain).
        let cli = try_parse_cli::<SetupCli>(
            "setup",
            &[s("--url=http://b:7789"), s("--token="), s("--mirror=/tmp/m")],
        )
        .unwrap();
        assert_eq!(cli.url.as_deref(), Some("http://b:7789"));
        assert_eq!(cli.token.as_deref(), Some(""));
        assert_eq!(cli.mirror.as_deref(), Some("/tmp/m"));

        // Space-separated still works; empty token is Some("").
        let cli = try_parse_cli::<SetupCli>(
            "setup",
            &[s("--url"), s("ws://b"), s("--token"), s(""), s("--mirror"), s("/d")],
        )
        .unwrap();
        assert_eq!(cli.token.as_deref(), Some(""));

        // No args = interactive mode (parses fine; the TTY check is runtime).
        assert!(try_parse_cli::<SetupCli>("setup", &[]).is_ok());
        // Partial flags still parse — the all-or-nothing bail is runtime.
        assert!(try_parse_cli::<SetupCli>("setup", &[s("--url"), s("x")]).is_ok());
        // Unknown args are clap errors now.
        assert!(try_parse_cli::<SetupCli>("setup", &[s("bogus")]).is_err());
    }

    #[test]
    fn t14_unset_takes_no_args() {
        assert!(try_parse_cli::<UnsetCli>("unset", &[]).is_ok());
        assert!(try_parse_cli::<UnsetCli>("unset", &[s("bogus")]).is_err());
        assert!(try_parse_cli::<UnsetCli>("unset", &[s("--url"), s("x")]).is_err());
    }
}
