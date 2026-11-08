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
//! `setup` / `doctor`: one-shot configuration and preflight for machine A
//! (writes the four relay variables into the opencode **server's**
//! environment; checks the endpoint, the server env, opencode's version and
//! the mirror).

use anyhow::{bail, Context, Result};
use claude_io_stub::{client, envf};
use serde_json::{json, Value};

fn url_from_env() -> Result<client::Base> {
    let raw = std::env::var("CLAUDE_STUB_URL")
        .ok()
        .filter(|v| !v.is_empty())
        .context("CLAUDE_STUB_URL (e.g. ws://b:7789 or http://gateway.example/zonepair) is required")?;
    client::parse_base(&raw)
}

fn token_from_env() -> Option<String> {
    std::env::var("CLAUDE_STUB_TOKEN").ok().filter(|v| !v.is_empty())
}

fn mirror_from_env() -> Result<std::path::PathBuf> {
    std::env::var("CLAUDE_CONFIG_DIR")
        .context("CLAUDE_CONFIG_DIR (A-local mirror of B's store) is required for sync")
        .map(std::path::PathBuf::from)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("sess") => sess_cli(&args[1..]).await,
        Some("sync") => sync_cli().await,
        Some("setup") => setup_cli(&args[1..]).await,
        Some("doctor") => doctor_cli(&args[1..]).await,
        _ => transport(&args).await,
    }
}

// ---- setup / doctor ---------------------------------------------------------

const OPENCODE_MIN: (u64, u64, u64) = (2, 2, 3);

fn mirror_default() -> String {
    let home = std::env::var("HOME").unwrap_or_default();
    format!("{home}/.claude-remote-mirror")
}

/// The variables the relay needs in the **opencode server's** environment.
/// A shell export is not enough: opencode usually runs as a daemon that never
/// sees the caller's shell, and stub-client is spawned by that server.
fn desired_server_env(url: &str, mirror: &str, exe: &str) -> Vec<(&'static str, String)> {
    vec![
        ("CLAUDE_STUB_URL", url.to_string()),
        ("OPENCODE_CLAUDE_PATH", exe.to_string()),
        ("CLAUDE_CONFIG_DIR", mirror.to_string()),
        ("CLAUDE_STUB_MIRROR", "1".to_string()),
    ]
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

async fn setup_cli(args: &[String]) -> Result<()> {
    let mut url = std::env::var("CLAUDE_STUB_URL")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "ws://127.0.0.1:7789".to_string());
    let mut mirror = std::env::var("CLAUDE_CONFIG_DIR")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(mirror_default);
    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--url" => {
                url = args.get(i + 1).context("--url needs URL")?.clone();
                i += 2;
            }
            "--mirror" => {
                mirror = args.get(i + 1).context("--mirror needs DIR")?.clone();
                i += 2;
            }
            other => bail!("unknown setup arg {other}; usage: setup [--url URL] [--mirror DIR]"),
        }
    }

    let exe = std::env::current_exe()
        .context("resolving our own path")?
        .canonicalize()
        .context("canonicalizing our own path")?
        .to_string_lossy()
        .to_string();

    std::fs::create_dir_all(&mirror).with_context(|| format!("creating mirror dir {mirror}"))?;

    // Prefer the server env: it is what the daemon-spawned stub-client inherits.
    // `service set env NAME VALUE` — the key is the literal string "env" and the
    // variable name is the second argument.
    let probe = std::process::Command::new("opencode")
        .args(["service", "set", "env", "PROBE", "PROBE"])
        .output();
    match probe {
        Ok(_) => {
            for (key, value) in desired_server_env(&url, &mirror, &exe) {
                let status = std::process::Command::new("opencode")
                    .args(["service", "set", "env", key, &value])
                    .status()
                    .with_context(|| format!("opencode service set env {key}"))?;
                if !status.success() {
                    bail!("opencode service set env {key} failed ({status}); set it by hand");
                }
                println!("opencode server env {key}={value}");
            }
            // Drop the probe, then restart so a running daemon picks the new
            // environment up.
            let _ = std::process::Command::new("opencode")
                .args(["service", "unset", "env", "PROBE"])
                .status();
            let _ = std::process::Command::new("opencode").args(["service", "restart"]).status();
        }
        Err(_) => {
            println!("opencode not on PATH — export these in whatever starts it:");
            for (key, value) in desired_server_env(&url, &mirror, &exe) {
                println!("  export {key}={value}");
            }
        }
    }

    // Bootstrap the mirror so opencode's in-process store helpers start from
    // B's real state rather than an empty directory.
    let base = client::parse_base(&url)?;
    match client::sync_mirror(&base, token_from_env().as_deref(), std::path::Path::new(&mirror)).await {
        Ok((pushed, pulled)) => println!("mirror bootstrap: pushed {pushed} line(s), pulled {pulled} line(s)"),
        Err(e) => println!("mirror bootstrap skipped: {e:#}"),
    }

    println!();
    doctor_cli(&["--url".to_string(), url.clone(), "--mirror".to_string(), mirror.clone()]).await
}

async fn doctor_cli(args: &[String]) -> Result<()> {
    let mut url = std::env::var("CLAUDE_STUB_URL")
        .ok()
        .filter(|v| !v.is_empty())
        .context("CLAUDE_STUB_URL is required")?;
    let mut failures = 0usize;
    let mut explicit_mirror = None;
    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--url" => {
                url = args.get(i + 1).context("--url needs URL")?.clone();
                i += 2;
            }
            "--mirror" => {
                explicit_mirror = Some(args.get(i + 1).context("--mirror needs DIR")?.clone());
                i += 2;
            }
            other => bail!("unknown doctor arg {other}"),
        }
    }
    let base = client::parse_base(&url)?;

    let warn = |msg: String| println!("  WARN  {msg}");
    let mut bad = |msg: String| {
        println!("  FAIL  {msg}");
        failures += 1;
    };

    println!("stub-client doctor  ({url})");

    // 1. the endpoint answers session-store HTTP at all
    let token = token_from_env();
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
    let base = url_from_env()?;
    let token = token_from_env();

    // Mirror push before the turn (ADR-0004): deliver A-side forks to B, so a
    // resume/fork created by opencode's in-process SDK helpers is visible to
    // the claude child on B.
    if std::env::var("CLAUDE_STUB_MIRROR").is_ok_and(|v| v == "1") {
        let mirror = mirror_from_env()?;
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
                    if tx.send(Message::Text(line)).await.is_err() {
                        break;
                    }
                }
                _ => {
                    // ask the server to close the child's stdin: claude flushes on EOF
                    let _ = tx.send(Message::Text(json!({"proto": "stdin-eof"}).to_string())).await;
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
    if std::env::var("CLAUDE_STUB_MIRROR").is_ok_and(|v| v == "1") {
        let mirror = mirror_from_env()?;
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
    let mut url = std::env::var("CLAUDE_STUB_URL").ok();
    let mut index = 0usize;
    if args.first().map(String::as_str) == Some("--url") {
        url = Some(args.get(1).context("--url needs URL")?.clone());
        index = 2;
    }
    let url = url.filter(|v| !v.is_empty()).context("CLAUDE_STUB_URL or --url required")?;
    let base = client::parse_base(&url)?;
    let token = token_from_env();

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

async fn sync_cli() -> Result<()> {
    let base = url_from_env()?;
    let mirror = mirror_from_env()?;
    let (pushed, pulled) = client::sync_mirror(&base, token_from_env().as_deref(), &mirror).await?;
    println!("sync ok: pushed {pushed} line(s), pulled {pulled} line(s)");
    Ok(())
}
