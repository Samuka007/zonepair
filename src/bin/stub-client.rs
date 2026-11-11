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
//! `sync` subcommand: bidirectional store mirror sync (ADR-0004). With
//! `CLAUDE_CONFIG_DIR` pointing at an A-local mirror of B's store, this makes
//! the Agent SDK's in-process store helpers work on the mirrored copy while
//! the real store lives on B.
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
//! `doctor`: preflight — checks the endpoint, the opencode server env,
//! opencode's version and the mirror.

use anyhow::{bail, Context, Result};
use claude_io_stub::{cfg, client, envf};
use serde_json::{json, Value};

// ---- config resolution -------------------------------------------------------
// One order everywhere: flag > process env > client config file > built-in
// default (ADR-0008). The opencode server env is NOT a read source — only the
// two keys opencode itself consumes live there, written by `setup`.

fn nonempty(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.is_empty())
}

fn resolve_url(flag: Option<String>) -> Result<String> {
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

fn resolve_token(flag: Option<String>) -> Option<String> {
    nonempty(flag)
        .or_else(|| nonempty(std::env::var("CLAUDE_STUB_TOKEN").ok()))
        .or_else(|| cfg::load().ok().and_then(|c| nonempty(c.token)))
}

fn resolve_mirror_dir(flag: Option<String>) -> String {
    nonempty(flag)
        .or_else(|| nonempty(std::env::var("CLAUDE_CONFIG_DIR").ok()))
        .or_else(|| cfg::load().ok().and_then(|c| nonempty(c.mirror_dir)))
        .unwrap_or_else(mirror_default)
}

fn mirror_enabled() -> bool {
    if std::env::var("CLAUDE_STUB_MIRROR").is_ok_and(|v| v == "1") {
        return true;
    }
    cfg::load().map(|c| c.mirror.unwrap_or(false)).unwrap_or(false)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("sess") => sess_cli(&args[1..]).await,
        Some("sync") => sync_cli(&args[1..]).await,
        Some("setup") => setup_cli(&args[1..]).await,
        Some("unset") => unset_cli(&args[1..]).await,
        Some("doctor") => doctor_cli(&args[1..]).await,
        _ => transport(&args).await,
    }
}

// ---- setup / doctor ---------------------------------------------------------

const OPENCODE_MIN: (u64, u64, u64) = (2, 2, 3);

fn mirror_default() -> String {
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

/// The only keys setup writes into the **opencode server's** environment: the
/// two opencode itself consumes (the provider path and the mirror dir its
/// in-process SDK helpers read). The relay's own knobs live in the client
/// config file (ADR-0008); the server env is not a read source. A shell export
/// is not enough either way: opencode usually runs as a daemon that never sees
/// the caller's shell, and stub-client is spawned by that server.
const OPENCODE_ENV_KEYS: [&str; 2] = ["OPENCODE_CLAUDE_PATH", "CLAUDE_CONFIG_DIR"];

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

async fn setup_cli(args: &[String]) -> Result<()> {
    let mut flags: [Option<String>; 3] = [None, None, None]; // url, token, mirror
    let mut i = 0usize;
    while i < args.len() {
        let slot = match args[i].as_str() {
            "--url" => 0,
            "--token" => 1,
            "--mirror" => 2,
            other => bail!(
                "unknown setup arg {other}; usage: setup [--url URL --token TOKEN --mirror DIR] (all together, --token \"\" = none) | no args: interactive"
            ),
        };
        flags[slot] = Some(
            args.get(i + 1)
                .context("--url/--token/--mirror need values")?
                .clone(),
        );
        i += 2;
    }
    let [url_f, token_f, mirror_f] = flags;
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
    let base = client::parse_base(&url)?;
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

    std::fs::create_dir_all(&mirror_dir).with_context(|| format!("creating mirror dir {mirror_dir}"))?;

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
            for (key, value) in [("OPENCODE_CLAUDE_PATH", &exe), ("CLAUDE_CONFIG_DIR", &mirror_dir)] {
                // Capture output: opencode's subcommands are chatty on stdout
                // (`service restart` prints its listen URL) and would
                // otherwise pollute our transcript.
                let out = std::process::Command::new("opencode")
                    .args(["service", "set", "env", key, value])
                    .output()
                    .with_context(|| format!("opencode service set env {key}"))?;
                if !out.status.success() {
                    bail!("opencode service set env {key} failed ({:?}); set it by hand", out.status.code());
                }
                println!("opencode server env {key}={value}");
            }
            // Drop the probe, then restart so a running daemon picks the new
            // environment up.
            let _ = std::process::Command::new("opencode")
                .args(["service", "unset", "env", "PROBE"])
                .output();
            let _ = std::process::Command::new("opencode").arg("service").arg("restart").output();
        }
        Err(_) => {
            println!("opencode not on PATH — export these in whatever starts it:");
            println!("  export OPENCODE_CLAUDE_PATH={exe}");
            println!("  export CLAUDE_CONFIG_DIR={mirror_dir}");
        }
    }

    // Bootstrap the mirror so opencode's in-process store helpers start from
    // B's real state rather than an empty directory.
    match client::sync_mirror(&base, token.as_deref(), std::path::Path::new(&mirror_dir)).await {
        Ok((pushed, pulled)) => println!("mirror bootstrap: pushed {pushed} line(s), pulled {pulled} line(s)"),
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
    doctor_cli(&["--url".to_string(), url.clone(), "--mirror".to_string(), mirror_dir.clone()]).await
}

/// Reverse of `setup`: erase the client config file and remove exactly the two
/// keys opencode itself consumes (plus the `PROBE` probe key a failed setup
/// can leave behind), then restart so a running daemon stops inheriting them.
/// Takes no `--url` and dials nothing — decommissioning B must not depend on B
/// answering. Legacy `CLAUDE_STUB_*` keys in the opencode server env predate
/// the client config file; migration is manual (docs/CLIENT.md) and `unset`
/// does not touch them. The mirror directory is user data and is read nowhere
/// here.
async fn unset_cli(args: &[String]) -> Result<()> {
    if let Some(other) = args.first() {
        bail!("unknown unset arg {other}; usage: unset");
    }
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
            let _ = std::process::Command::new("opencode").arg("service").arg("restart").output();
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

async fn doctor_cli(args: &[String]) -> Result<()> {
    let mut url_flag: Option<String> = None;
    let mut failures = 0usize;
    let mut explicit_mirror = None;
    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--url" => {
                url_flag = Some(args.get(i + 1).context("--url needs URL")?.clone());
                i += 2;
            }
            "--mirror" => {
                explicit_mirror = Some(args.get(i + 1).context("--mirror needs DIR")?.clone());
                i += 2;
            }
            other => bail!("unknown doctor arg {other}"),
        }
    }
    let url = resolve_url(url_flag)?;
    let base = client::parse_base(&url)?;

    let warn = |msg: String| println!("  WARN  {msg}");
    let mut bad = |msg: String| {
        println!("  FAIL  {msg}");
        failures += 1;
    };

    println!("stub-client doctor  ({url})");

    // 1. the endpoint answers session-store HTTP at all
    let token = resolve_token(None);
    match client::store_list(&base, token.as_deref()).await {
        Ok(sessions) => println!("  ok    store reachable ({} session(s) on B)", sessions.len()),
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
                bad(format!("OPENCODE_CLAUDE_PATH={value} does not look like stub-client"));
            } else {
                println!("  ok    opencode server OPENCODE_CLAUDE_PATH={value}");
            }
        }
        Err(_) => warn("opencode not on PATH — cannot verify the server env".to_string()),
    }

    // 3. opencode new enough for the Claude Agent provider
    match std::process::Command::new("opencode").arg("--version").output() {
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
    let mirror = explicit_mirror.or(server_mirror).unwrap_or_else(mirror_default);
    match std::fs::read_dir(&mirror) {
        Err(e) => warn(format!("mirror {mirror} missing ({e}) — run `stub-client setup`")),
        Ok(entries) => {
            let n = entries.filter_map(|e| e.ok()).count();
            if n == 0 {
                warn(format!("mirror {mirror} is empty — run `stub-client sync` once"));
            } else {
                println!("  ok    mirror {mirror} ({n} entries)");
            }
        }
    }

    if failures > 0 {
        bail!("doctor found {failures} problem(s)");
    }
    println!("doctor: all checks passed");
    Ok(())
}

// ---- transport ---------------------------------------------------------------

fn stdio_header(argv: &[String]) -> Value {
    json!({
        "argv": argv,
        "env": envf::collect(),
        "cwd": std::env::current_dir().map(|p| p.display().to_string()).unwrap_or_default(),
    })
}

async fn transport(argv: &[String]) -> Result<()> {
    let base = client::parse_base(&resolve_url(None)?)?;
    let token = resolve_token(None);

    // Mirror push before the turn (ADR-0004): deliver A-side forks to B, so a
    // resume/fork created by opencode's in-process SDK helpers is visible to
    // the claude child on B. Enabled by CLAUDE_STUB_MIRROR=1 or the client
    // config file's `mirror` flag.
    if mirror_enabled() {
        let mirror = std::path::PathBuf::from(resolve_mirror_dir(None));
        client::sync_mirror(&base, token.as_deref(), &mirror)
            .await
            .context("mirror push before relay")?;
    }

    let stream = client::open_stdio(&base, &stdio_header(argv), token.as_deref()).await?;

    // Feed claude's stdin: one line per WS frame; on stdin EOF ask the server
    // to close the child's stdin so claude flushes and exits.
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    use tokio_tungstenite::tungstenite::Message;

    let mut tx = stream.tx;
    let mut rx = stream.rx;
    let stdin_task = tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut lines = tokio::io::BufReader::new(stdin).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if tx.send(Message::Text(line.into())).await.is_err() {
                        break;
                    }
                }
                _ => {
                    // ask the server to close the child's stdin: claude flushes on EOF
                    let _ = tx
                        .send(Message::Text(json!({"proto": "stdin-eof"}).to_string().into()))
                        .await;
                    break;
                }
            }
        }
    });

    // Child stdout: one line per frame back to the SDK.
    let mut stdout = tokio::io::stdout();
    while let Some(msg) = rx.next().await {
        let line = match msg {
            Ok(Message::Text(line)) => line,
            Ok(_) => continue,
            Err(e) => {
                eprintln!("stub-client: stream error: {e}");
                break;
            }
        };
        stdout.write_all(line.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    stdin_task.abort();

    // Post-turn pull, so opencode's next in-process store read sees the
    // messages the turn just wrote on B. Best-effort: the model reply is
    // already delivered; failing it now would only destroy a completed turn.
    if mirror_enabled() {
        let mirror = std::path::PathBuf::from(resolve_mirror_dir(None));
        if let Err(e) = client::sync_mirror(&base, token.as_deref(), &mirror).await {
            eprintln!("stub-client: mirror pull after relay failed: {e:#}");
        }
    }
    // Hard exit: the relay's job is done and machine A's stdin may still be
    // open. Tokio's Stdin worker is a blocking-pool thread parked in read(),
    // and a normal runtime shutdown would join it forever.
    std::process::exit(0);
}

// ---- sess ---------------------------------------------------------------------

async fn sess_cli(args: &[String]) -> Result<()> {
    let mut url_flag = None;
    let mut index = 0usize;
    if args.first().map(String::as_str) == Some("--url") {
        url_flag = Some(args.get(1).context("--url needs URL")?.clone());
        index = 2;
    }
    let base = client::parse_base(&resolve_url(url_flag)?)?;
    let token = resolve_token(None);

    let op = args.get(index).context("missing sess op (exists|messages|fork|delete|list|merge)")?;
    let need = |n: usize, what: &str| -> Result<&String> {
        args.get(n).with_context(|| format!("sess {op} needs {what}"))
    };
    let out = match op.as_str() {
        "exists" => {
            let session = need(index + 1, "session")?;
            json!({"exists": client::store_exists(&base, token.as_deref(), session).await?})
        }
        "messages" => {
            let session = need(index + 1, "session")?;
            let messages = client::store_messages(&base, token.as_deref(), session).await?;
            json!({"ok": true, "messages": messages})
        }
        "fork" => {
            let session = need(index + 1, "session")?;
            let up_to = need(index + 2, "upTo")?;
            let (session_id, pairs, messages) =
                client::store_fork(&base, token.as_deref(), session, up_to).await?;
            json!({"ok": true, "sessionId": session_id, "pairs": pairs, "messages": messages})
        }
        "delete" => {
            let session = need(index + 1, "session")?;
            let deleted = client::store_delete(&base, token.as_deref(), session).await?;
            json!({"ok": true, "deleted": deleted})
        }
        "list" => {
            let sessions = client::store_list(&base, token.as_deref()).await?;
            json!({"ok": true, "sessions": sessions})
        }
        "merge" => {
            let session = need(index + 1, "session")?;
            let file = need(index + 2, "jsonl file")?;
            let raw = std::fs::read_to_string(file.as_str()).with_context(|| format!("reading {}", file))?;
            let messages: Vec<Value> = raw
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| serde_json::from_str(l).context("line is not JSON"))
                .collect::<Result<_, _>>()?;
            let (added, total) = client::store_merge(&base, token.as_deref(), session, &messages).await?;
            json!({"ok": true, "added": added, "total": total})
        }
        other => bail!("unknown sess op {other}"),
    };

    println!("{}", serde_json::to_string_pretty(&out)?);
    if out.get("ok") == Some(&json!(false)) || out.get("exists") == Some(&json!(false)) {
        std::process::exit(2);
    }
    Ok(())
}

// ---- sync ---------------------------------------------------------------------

async fn sync_cli(args: &[String]) -> Result<()> {
    if let Some(other) = args.first() {
        bail!("unknown sync arg {other}; sync resolves URL/token/mirror from the environment and the client config file");
    }
    let base = client::parse_base(&resolve_url(None)?)?;
    let mirror = std::path::PathBuf::from(resolve_mirror_dir(None));
    let (pushed, pulled) = client::sync_mirror(&base, resolve_token(None).as_deref(), &mirror).await?;
    println!("sync ok: pushed {pushed} line(s), pulled {pulled} line(s)");
    Ok(())
}
