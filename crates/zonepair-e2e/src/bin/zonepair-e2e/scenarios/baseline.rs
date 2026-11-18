use crate::fixtures::*;
use crate::nodial::{assert_no_dial, assert_no_dial_for};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use zonepair_client::{api, daemon, sync::persist::state, transport as client};

// ---- the suite -----------------------------------------------------------------

pub(crate) async fn basic_turn(base: &client::Base) {
    let mut stream = open_session(base, "sess-basic").await;
    let lines = drive_turn(&mut stream, "hello", true).await;
    let init = lines
        .iter()
        .find(|l| l.get("type").and_then(Value::as_str) == Some("system"))
        .unwrap_or_else(|| fail("no init message"));
    assert_eq!(init.get("session_id").and_then(Value::as_str), Some("sess-basic"));
    assert_eq!(assistant_text(&lines), "ECHO hello");
    let result = lines.last().unwrap();
    assert_eq!(result.get("subtype").and_then(Value::as_str), Some("success"));

    // The turn must land in B's store (claude-mock persists under
    // CLAUDE_CONFIG_DIR, which the server pins to the store).
    assert!(api::store_exists(base, None, "sess-basic").await.unwrap());
    let messages = api::store_messages(base, None, "sess-basic").await.unwrap();
    assert_eq!(messages.len(), 2, "user + assistant lines");
}

pub(crate) async fn tool_round_trip(base: &client::Base) {
    let mut stream = open_session(base, "sess-tool").await;

    // The mock turns any prompt containing __TOOL__ into a tool_use block.
    stream
        .send_line(&json!({
            "type": "user",
            "message": {"role": "user", "content": [{"type": "text", "text": "__TOOL__ run echo"}]},
        }))
        .await
        .unwrap();
    let mut saw_tool_use = false;
    let mut ack_text = String::new();
    let mut saw_result = false;
    let mut received: Vec<String> = Vec::new();
    loop {
        let Some(line) = stream.next_line().await.expect("frame") else { break };
        received.push(line.clone());
        let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
        if v.get("type").and_then(Value::as_str) == Some("assistant") {
            for block in v["message"]["content"].as_array().cloned().unwrap_or_default() {
                match block.get("type").and_then(Value::as_str) {
                    Some("tool_use") => {
                        saw_tool_use = true;
                        assert_eq!(block.get("name").and_then(Value::as_str), Some("mcp__opencode__echo"));
                        stream
                            .send_line(&json!({
                                "type": "user",
                                "message": {"role": "user", "content": [{
                                    "type": "tool_result",
                                    "tool_use_id": block["id"],
                                    "content": [{"type": "text", "text": "echo-payload-x"}],
                                }]},
                            }))
                            .await
                            .unwrap();
                    }
                    Some("text") => {
                        ack_text = block.get("text").and_then(Value::as_str).unwrap_or_default().to_string();
                    }
                    _ => {}
                }
            }
        }
        if v.get("type").and_then(Value::as_str) == Some("result") {
            saw_result = true;
            break;
        }
    }
    stream.send_eof().await.unwrap();
    if !saw_result {
        eprintln!("tool flow received {} frame(s):", received.len());
        for r in &received {
            eprintln!("  | {}", r.chars().take(160).collect::<String>());
        }
    }
    assert!(saw_tool_use, "no tool_use");
    assert_eq!(ack_text, "TOOL OK echo-payload-x", "tool result did not round trip");
    assert!(saw_result, "no result after tool round trip");
}

pub(crate) async fn sess_rides_http(base: &client::Base, server: &Server) {
    if server.child.is_some() {
        // Seeded on disk before the server started; the store reads the
        // directory per operation, so both sessions are immediately visible.
        seed_session(&server.store, "session_aaa", 5);
        seed_session(&server.store, "session_bbb", 3);
    } else {
        // External/remote server: seed over the wire instead.
        let lines: Vec<Value> = (0..5)
            .map(|i| {
                let parent = if i == 0 {
                    Value::Null
                } else {
                    Value::String(format!("u-a-{}", i - 1))
                };
                json!({
                    "type": "user",
                    "uuid": format!("u-a-{i}"),
                    "sessionId": "session_aaa",
                    "parentUuid": parent,
                    "message": {"role": "user", "content": format!("message {i}")},
                })
            })
            .collect();
        api::store_merge(base, None, "session_aaa", &lines).await.unwrap();
    }

    let sessions = api::store_list(base, None).await.unwrap();
    assert!(sessions.contains(&"session_aaa".to_string()));
    assert!(sessions.contains(&"session_bbb".to_string()));

    assert!(api::store_exists(base, None, "session_aaa").await.unwrap());
    assert!(!api::store_exists(base, None, "no-such-session").await.unwrap());

    let messages = api::store_messages(base, None, "session_aaa").await.unwrap();
    assert_eq!(messages.len(), 5);

    // Fork at the 3rd message: fresh session id, uuid chain rewritten, 3 lines.
    let (forked, pairs, fork_messages) =
        api::store_fork(base, None, "session_aaa", "u-session_aaa-2").await.unwrap();
    assert_ne!(forked, "session_aaa");
    assert_eq!(pairs.len(), 3, "one pair per copied message");
    assert_eq!(fork_messages.len(), 3);
    assert!(fork_messages
        .iter()
        .all(|m| m.get("sessionId").and_then(Value::as_str) == Some(forked.as_str())));
    // First line keeps its original parent (chain root); the rest point at
    // the previous NEW uuid, so the forked chain is internally consistent.
    assert_eq!(fork_messages[0]["parentUuid"], json!(null));
    for i in 1..fork_messages.len() {
        assert_eq!(
            fork_messages[i]["parentUuid"],
            pairs[i - 1][1],
            "line {i} must point at the fresh uuid of line {}",
            i - 1
        );
    }

    assert!(api::store_exists(base, None, &forked).await.unwrap());
    assert!(api::store_delete(base, None, &forked).await.unwrap());
    assert!(!api::store_exists(base, None, &forked).await.unwrap());
}

pub(crate) async fn three_clients_one_server(base: &client::Base) {
    let mut handles = Vec::new();
    for i in 0..3 {
        let base = base.clone();
        handles.push(tokio::spawn(async move {
            let session = format!("sess-par-{i}");
            let mut stream = open_session(&base, &session).await;
            let lines = drive_turn(&mut stream, &format!("ping {i}"), true).await;
            assert_eq!(assistant_text(&lines), format!("ECHO ping {i}"));
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    for i in 0..3 {
        assert!(api::store_exists(base, None, &format!("sess-par-{i}")).await.unwrap());
    }
}

/// ADR-0001, enforced: after traffic has flowed, the server process owns no
/// established socket that it did not accept on its listen port.
pub(crate) async fn server_never_dials(server: &Server) {
    let mut stream = open_session(&server.base, "sess-nodial").await;
    let lines = drive_turn(&mut stream, "hello", false).await;
    assert!(!lines.is_empty());
    assert_no_dial(server);
    stream.send_eof().await.unwrap();
    assert_no_dial(server);
}

// ---- real model (credentials from the environment) ---------------------------

/// Real claude on B, driven through the same stdio-v1 semantics: marker echo,
/// fork of the real store, and fork/resume reproducing the marker across
/// processes.
///
/// Opt in with `ZONEPAIR_REAL=1` and export the variables claude itself
/// consumes: `ANTHROPIC_BASE_URL` (any Anthropic-compatible endpoint),
/// `ANTHROPIC_AUTH_TOKEN`, and optionally `ANTHROPIC_MODEL`. Provider-specific
/// knowledge stays outside this repository.
pub(crate) async fn real_model_suite() {
    let claude_bin =
        std::env::var("ZONEPAIR_CLAUDE_BIN").unwrap_or_else(|_| "claude".to_string());
    for var in ["ANTHROPIC_BASE_URL", "ANTHROPIC_AUTH_TOKEN"] {
        if std::env::var(var).ok().filter(|v| !v.is_empty()).is_none() {
            fail(&format!("{var} must be exported for the real-model suite"));
        }
    }
    eprintln!(
        "real-model: base={} model={}",
        std::env::var("ANTHROPIC_BASE_URL").unwrap_or_default(),
        std::env::var("ANTHROPIC_MODEL").unwrap_or_default()
    );
    // envf allowlists ANTHROPIC_* / DISABLE_* — the wiring rides the hello
    // header to the claude child on B.
    std::env::set_var("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1");
    std::env::set_var("DISABLE_AUTOUPDATER", "1");

    let port = free_port();
    let workdir = tempdir();
    let store = workdir.join("store");
    let logs = workdir.join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    let child = Command::new(server_bin())
        .arg("--listen")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--store")
        .arg(&store)
        .arg("--claude")
        .arg(&claude_bin)
        .arg("--log-dir")
        .arg(&logs)
        .spawn()
        .expect("spawn stub-server");
    let base = client::parse_base(&format!("ws://127.0.0.1:{port}")).unwrap();
    wait_ready(&base).await;

    let session = format!("real-{}", free_port());
    let marker = format!("zp-marker-{}-{port}", std::process::id());
    let base_argv = [
        "-p",
        "--output-format",
        "stream-json",
        "--verbose",
        "--input-format",
        "stream-json",
    ];

    // ---- turn 1: marker echo through A -> B -> provider --------------------
    let mut stream = stdio_for(&base, &base_argv).await;
    stream
        .send_line(&json!({
            "type": "user",
            "message": {"role": "user", "content": [{
                "type": "text",
                "text": format!("Reply with exactly this phrase and nothing else: {marker}"),
            }]},
        }))
        .await
        .unwrap();
    stream.send_eof().await.unwrap();

    let mut init_session: Option<String> = None;
    let mut reply = String::new();
    loop {
        let Some(line) = stream.next_line().await.expect("frame (turn 1)") else {
            fail("closed before result (turn 1)");
        };
        let Ok(v) = serde_json::from_str::<Value>(&line) else {
            if line.starts_with("[claude-code:") { continue }
            continue;
        };
        if v.get("type").and_then(Value::as_str) == Some("system")
            && v.get("subtype").and_then(Value::as_str) == Some("init")
        {
            init_session = v.get("session_id").and_then(Value::as_str).map(str::to_string);
        }
        if v.get("type").and_then(Value::as_str) == Some("assistant") {
            if let Some(blocks) = v.pointer("/message/content").and_then(Value::as_array) {
                for b in blocks {
                    if let Some(t) = b.get("text").and_then(Value::as_str) {
                        reply.push_str(t);
                    }
                }
            }
        }
        if v.get("type").and_then(Value::as_str) == Some("result") {
            if v.get("subtype").and_then(Value::as_str) != Some("success") {
                fail(&format!("turn 1 result: {}", v.to_string().chars().take(200).collect::<String>()));
            }
            break;
        }
    }
    assert!(
        reply.contains(&marker),
        "model reply does not contain the marker: {:?}",
        reply.chars().take(200).collect::<String>()
    );
    let session = init_session
        .or(Some(session))
        .expect("no session id (turn 1)");
    eprintln!(
        "turn 1 ok: session={session} reply={}",
        reply.chars().take(120).collect::<String>()
    );
    drop(stream);

    // ---- the marker landed in B's store ------------------------------------
    let messages = api::store_messages(&base, None, &session).await.expect("store messages");
    assert!(messages.len() >= 2, "real session did not land in B's store");
    let first_user = messages
        .iter()
        .find(|m| m.get("type").and_then(Value::as_str) == Some("user"))
        .and_then(|m| m.get("uuid"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| fail("no user uuid in B's store"))
        .to_string();

    // ---- fork the real store ----------------------------------------------
    let (forked, _pairs, _fm) = api::store_fork(&base, None, &session, &first_user)
        .await
        .expect("fork real session");

    // ---- resume the FORKED session in a fresh claude process ---------------
    let mut resume_argv: Vec<&str> = base_argv.to_vec();
    resume_argv.push("--resume");
    resume_argv.push(forked.as_str());
    let mut stream2 = stdio_for(&base, &resume_argv).await;
    stream2
        .send_line(&json!({
            "type": "user",
            "message": {"role": "user", "content": [{
                "type": "text",
                "text": "Earlier I asked you to reply with exactly one marker phrase. Reply with only that phrase now.",
            }]},
        }))
        .await
        .unwrap();
    stream2.send_eof().await.unwrap();
    let mut resumed_reply = String::new();
    loop {
        let Some(line) = stream2.next_line().await.expect("frame (resume)") else {
            fail("closed before result (resume)");
        };
        let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
        if v.get("type").and_then(Value::as_str) == Some("assistant") {
            if let Some(blocks) = v.pointer("/message/content").and_then(Value::as_array) {
                for b in blocks {
                    if let Some(t) = b.get("text").and_then(Value::as_str) {
                        resumed_reply.push_str(t);
                    }
                }
            }
        }
        if v.get("type").and_then(Value::as_str) == Some("result") {
            break;
        }
    }
    assert!(
        resumed_reply.contains(&marker),
        "forked resume lost the marker: {:?}",
        resumed_reply.chars().take(200).collect::<String>()
    );
    eprintln!(
        "resume ok: fork={forked} reply={}",
        resumed_reply.chars().take(120).collect::<String>()
    );
    drop(stream2);

    assert!(api::store_delete(&base, None, &forked).await.unwrap(), "delete forked session");
    assert_no_dial_for(child.id(), port);
}

/// `doctor --url` must work with no `CLAUDE_STUB_URL` in the environment:
/// `stub-client setup` invokes doctor with the URL as a flag, and requiring
/// the env var first made the setup flow fail at its final step.
///
/// The doctor's opencode checks are also isolated from the host: a stub
/// `opencode` on PATH answers `service get env OPENCODE_CLAUDE_PATH` with the
/// client binary, so the check passes deterministically. Without this the
/// suite's verdict depends on whether the host happens to have an opencode
/// (and a configured server env) — a devshell with opencode 2.2.5 and no
/// configured server made doctor exit 1 through no fault of the stub.
pub(crate) async fn doctor_accepts_url(server: &Server) {
    let url = format!("ws://127.0.0.1:{}", server.port);
    let workdir = tempdir();
    let fake_bin = workdir.join("fake-opencode-bin");
    std::fs::create_dir_all(&fake_bin).unwrap();
    let fake = fake_bin.join("opencode");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\ncase \"$*\" in\n  *\"service get env OPENCODE_CLAUDE_PATH\"*) echo {};;\n  *\"--version\"*) echo \"opencode v2.2.5\";;\n  *) exit 0;;\nesac\n",
            client_bin(),
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let out = Command::new(client_bin())
        .args(["doctor", "--url", &url])
        .env_remove("CLAUDE_STUB_URL")
        .env_remove("CLAUDE_STUB_TOKEN")
        .env_remove("ZONEPAIR_CLIENT_CONFIG")
        .env("PATH", format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap_or_default()))
        .output();
    let out = match out {
        Ok(out) => out,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("  note: stub-client not found — doctor check skipped");
            return;
        }
        Err(e) => fail(&format!("spawn stub-client doctor: {e}")),
    };
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    if !out.status.success() {
        fail(&format!("doctor --url failed: {stdout}{stderr}"));
    }
    if stdout.contains("CLAUDE_STUB_URL is required") {
        fail("doctor --url still requires CLAUDE_STUB_URL in the environment");
    }
    eprintln!("doctor --url ok");
}

/// Bare `doctor` (no `--url`, no `CLAUDE_STUB_URL`) must resolve the relay
/// URL from the client config file (ADR-0008 order: flag > env > file) —
/// after `setup` writes the file, doctor has to work in a fresh shell.
pub(crate) async fn doctor_uses_config_file(server: &Server) {
    let url = format!("ws://127.0.0.1:{}", server.port);
    let workdir = tempdir();
    let fake_bin = workdir.join("fake-opencode-bin");
    std::fs::create_dir_all(&fake_bin).unwrap();
    let fake = fake_bin.join("opencode");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\ncase \"$*\" in\n  *\"service get env OPENCODE_CLAUDE_PATH\"*) echo {};;\n  *\"--version\"*) echo \"opencode v2.2.5\";;\n  *) exit 0;;\nesac\n",
            client_bin(),
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let cfg_file = workdir.join("client.toml");
    std::fs::write(&cfg_file, format!("url = \"{url}\"\n")).unwrap();

    let out = Command::new(client_bin())
        .arg("doctor")
        .env_remove("CLAUDE_STUB_URL")
        .env_remove("CLAUDE_STUB_TOKEN")
        .env("ZONEPAIR_CLIENT_CONFIG", &cfg_file)
        .env("PATH", format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap_or_default()))
        .output();
    let out = match out {
        Ok(out) => out,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("  note: stub-client not found — doctor check skipped");
            return;
        }
        Err(e) => fail(&format!("spawn stub-client doctor: {e}")),
    };
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    if !out.status.success() {
        fail(&format!("bare doctor with config file failed: {stdout}{stderr}"));
    }
    if !stdout.contains(&format!("stub-client doctor  ({url})")) {
        fail(&format!("doctor did not run against the config-file url {url}: {stdout}"));
    }
    eprintln!("doctor from config file ok");
}

/// Bare `doctor` with no env var and no config file must fail with the shared
/// resolution message pointing at `stub-client setup` — not the old env-only
/// text, which denied the config file as a URL source.
pub(crate) async fn doctor_without_url_fails() {
    let workdir = tempdir();
    let cfg_file = workdir.join("client.toml");
    let out = Command::new(client_bin())
        .arg("doctor")
        .env_remove("CLAUDE_STUB_URL")
        .env_remove("CLAUDE_STUB_TOKEN")
        .env("ZONEPAIR_CLIENT_CONFIG", &cfg_file)
        .output();
    let out = match out {
        Ok(out) => out,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("  note: stub-client not found — doctor check skipped");
            return;
        }
        Err(e) => fail(&format!("spawn stub-client doctor: {e}")),
    };
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    if out.status.success() {
        fail(&format!("bare doctor without env or config file succeeded: {stdout}{stderr}"));
    }
    if !stderr.contains("run `stub-client setup`") {
        fail(&format!("doctor failure does not point at `stub-client setup`: {stderr}"));
    }
    if stderr.contains("CLAUDE_STUB_URL or --url is required") {
        fail("doctor still fails with the old env-only message");
    }
    eprintln!("doctor without any url fails with the shared message");
}

/// Fake `opencode` modelling the service-env contract with a state file (one
/// `KEY=VALUE` per line): `service set/unset/get env KEY [VALUE]` mutate and
/// read it, `service restart` is silent, `--version` answers v2.2.5. Args:
/// `service <set|unset|get> env KEY [VALUE]`; `service get env KEY` prints the
/// value — the external behavior assertions read through it.
pub(crate) fn write_stateful_opencode(fake: &Path) {
    std::fs::write(
        fake,
        "#!/bin/sh\nSTATE=\"$ZONEPAIR_FAKE_OC_STATE\"\ncase \"$1 $2 $3\" in\n  \"service set env\")\n    grep -v \"^$4=\" \"$STATE\" 2>/dev/null > \"$STATE.tmp\" || true\n    echo \"$4=$5\" >> \"$STATE.tmp\"; mv \"$STATE.tmp\" \"$STATE\"; exit 0;;\n  \"service unset env\")\n    grep -v \"^$4=\" \"$STATE\" 2>/dev/null > \"$STATE.tmp\" || true\n    mv \"$STATE.tmp\" \"$STATE\"; exit 0;;\n  \"service get env\")\n    grep \"^$4=\" \"$STATE\" 2>/dev/null | cut -d= -f2-\n    exit 0;;\n  \"service restart\")\n    exit 0;;\nesac\ncase \"$1\" in --version) echo \"opencode v2.2.5\";; esac\nexit 0\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

/// `stub-client setup` (ADR-0008): flag mode writes the client config file and
/// exactly the two opencode keys; a fresh shell with no relay env runs `sync`
/// purely from the file; partial flags and a no-flag run outside a TTY fail
/// without writing anything.
pub(crate) async fn setup_and_bare_shell(server: &Server) {
    if server.child.is_none() {
        eprintln!("  note: external server — setup flow skipped");
        return;
    }
    let workdir = tempdir();
    let fake_bin = workdir.join("fake-opencode-bin");
    std::fs::create_dir_all(&fake_bin).unwrap();
    let state = workdir.join("service-env.txt");
    let fake = fake_bin.join("opencode");
    write_stateful_opencode(&fake);
    let client_abs: String = {
        let cb = client_bin();
        if cb.contains('/') {
            cb
        } else {
            std::env::split_paths(&std::env::var("PATH").unwrap_or_default())
                .map(|dir| dir.join(&cb))
                .find(|p| p.is_file())
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| cb.clone())
        }
    };
    let cfg_file = workdir.join("client.toml");
    let mirror = workdir.join("mirror");
    let url = format!("ws://127.0.0.1:{}", server.port);

    let run_setup = |args: &[&str]| -> std::process::Output {
        Command::new(&client_abs)
            .args(["setup"])
            .args(args)
            .env_remove("CLAUDE_STUB_URL")
            .env_remove("CLAUDE_STUB_TOKEN")
            .env_remove("CLAUDE_CONFIG_DIR")
            .env_remove("CLAUDE_STUB_MIRROR")
            .env("ZONEPAIR_CLIENT_CONFIG", &cfg_file)
            .env("ZONEPAIR_FAKE_OC_STATE", &state)
            .env("PATH", format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap_or_default()))
            .output()
            .unwrap_or_else(|e| fail(&format!("spawn stub-client setup: {e}")))
    };
    let env_get = |key: &str| -> String {
        let out = Command::new(&fake)
            .args(["service", "get", "env", key])
            .env("ZONEPAIR_FAKE_OC_STATE", &state)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };

    // Partial flags: loud failure, zero side effects — no file, no opencode env.
    let out = run_setup(&["--url", &url]);
    if out.status.success() {
        fail("setup with partial flags must fail");
    }
    if cfg_file.exists() {
        fail("partial setup must not write the client config file");
    }
    if !env_get("OPENCODE_CLAUDE_PATH").is_empty() {
        fail("partial setup must not touch the opencode env");
    }

    // No flags outside a TTY (spawned pipes): loud failure, still nothing.
    let out = run_setup(&[]);
    if out.status.success() {
        fail("no-flag setup outside a TTY must fail");
    }
    if !String::from_utf8_lossy(&out.stderr).contains("TTY") {
        fail(&format!("no-flag setup error must name the TTY gate: {}", String::from_utf8_lossy(&out.stderr)));
    }
    if cfg_file.exists() {
        fail("failed interactive setup must not write the client config file");
    }

    // Flag mode with an empty token: none stored, file + two keys written.
    let out = run_setup(&["--url", &url, "--token", "", "--mirror", &mirror.display().to_string()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() {
        fail(&format!("setup failed: {stdout}{}", String::from_utf8_lossy(&out.stderr)));
    }
    if !stdout.contains("opencode server env OPENCODE_CLAUDE_PATH=")
        || !stdout.contains("opencode server env CLAUDE_CONFIG_DIR=")
    {
        fail(&format!("setup did not report the two opencode keys: {stdout}"));
    }
    if !stdout.contains("mirror bootstrap:") {
        fail(&format!("mirror bootstrap did not run: {stdout}"));
    }
    let saved = std::fs::read_to_string(&cfg_file).unwrap();
    assert!(
        saved.contains(&format!("url = \"{url}\"")),
        "client config missing url: {saved}"
    );
    assert!(saved.contains("mirror = true"), "client config missing mirror flag: {saved}");
    assert!(!saved.contains("token ="), "empty token must be omitted: {saved}");
    if env_get("OPENCODE_CLAUDE_PATH") != client_abs {
        fail(&format!("wrong OPENCODE_CLAUDE_PATH: {:?}", env_get("OPENCODE_CLAUDE_PATH")));
    }
    if env_get("CLAUDE_CONFIG_DIR") != mirror.display().to_string() {
        fail(&format!("wrong CLAUDE_CONFIG_DIR: {:?}", env_get("CLAUDE_CONFIG_DIR")));
    }
    if !env_get("CLAUDE_STUB_URL").is_empty() {
        fail("setup must not write relay keys into the opencode env");
    }
    if !env_get("PROBE").is_empty() {
        fail("setup must sweep the PROBE key");
    }

    // A fresh shell with no relay env: `sync --wait` resolves everything
    // from the file, enqueues a reconcile intent, and returns once the
    // spawned zonepaird drains the backlog (RFC-0002-a-state §9 structural drain).
    let out = Command::new(&client_abs)
        .args(["sync", "--wait"])
        .env_remove("CLAUDE_STUB_URL")
        .env_remove("CLAUDE_STUB_TOKEN")
        .env_remove("CLAUDE_CONFIG_DIR")
        .env_remove("CLAUDE_STUB_MIRROR")
        .env("ZONEPAIR_CLIENT_CONFIG", &cfg_file)
        .output()
        .unwrap_or_else(|e| fail(&format!("spawn stub-client sync: {e}")));
    if !out.status.success() {
        fail(&format!(
            "bare-shell sync failed: {}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    if !String::from_utf8_lossy(&out.stdout).contains("sync ok") {
        fail("bare-shell sync did not report ok");
    }

    // Flag mode with a token: stored in the file, never echoed to stdout.
    let token = "e2e-secret-token-42";
    let out = run_setup(&["--url", &url, "--token", token, "--mirror", &mirror.display().to_string()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() {
        fail(&format!("token setup failed: {stdout}{}", String::from_utf8_lossy(&out.stderr)));
    }
    if stdout.contains(token) {
        fail("setup echoed the token to stdout");
    }
    let saved = std::fs::read_to_string(&cfg_file).unwrap();
    assert!(
        saved.contains(&format!("token = \"{token}\"")),
        "token missing from the client config: {saved}"
    );

    eprintln!("setup flow ok (strict flags, TTY gate, file store, two opencode keys, bare-shell sync)");

    // The bare-shell `sync --wait` spawned a detached zonepaird bound to
    // the workdir mirror (ensure_owner respawn semantics — it is NOT a
    // child of the sync process). Stop it: no writer may outlive the
    // suite, and nothing asserts on this mirror afterwards.
    let _ = daemon::stop_daemon(
        &state::sync_dir(&mirror),
        &daemon::DaemonConfig::default(),
    );
}

/// `stub-client unset` (issue #4): erases the client config file, removes
/// exactly the two keys `setup` writes into the opencode env plus a leftover
/// `PROBE`, is idempotent, contacts no server, prints one removal line per
/// key when opencode is absent, and fails loudly if opencode exists but a
/// removal fails. The fake `opencode` on PATH models the service-env contract
/// with a state file, so assertions run against what the environment then
/// answers (`service get env KEY` prints the value) — not on which internal
/// calls stub-client made. State file and config file live in a temp dir; the
/// host's real opencode env (if any) is untouched.
pub(crate) async fn unset_lifecycle(server: &Server) {
    let workdir = tempdir();
    let fake_bin = workdir.join("fake-opencode-bin");
    std::fs::create_dir_all(&fake_bin).unwrap();
    let state = workdir.join("service-env.txt");
    // Resolve the client binary to an absolute path *before* any PATH
    // override: spawn lookup uses the child env's PATH, and the branches
    // below deliberately restrict PATH (fake opencode / no opencode at
    // all). The docker harness runs this suite with ZONEPAIR_CLIENT_BIN
    // unset — `stub-client` is a bare name resolved from the container's
    // default PATH — so a bare name must be pinned here or the restricted
    // PATH hides the binary under test itself.
    let client_abs: String = {
        let cb = client_bin();
        if cb.contains('/') {
            cb
        } else {
            std::env::split_paths(&std::env::var("PATH").unwrap_or_default())
                .map(|dir| dir.join(&cb))
                .find(|p| p.is_file())
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| cb.clone())
        }
    };
    let fake = fake_bin.join("opencode");
    write_stateful_opencode(&fake);
    let run_fake = |args: &[&str]| -> String {
        let out = Command::new(&fake)
            .args(args)
            .env("ZONEPAIR_FAKE_OC_STATE", &state)
            .output()
            .unwrap();
        assert!(out.status.success(), "fake opencode {args:?} failed");
        String::from_utf8_lossy(&out.stdout).to_string()
    };

    // Simulate a configured relay: the two keys setup writes into the opencode
    // env, plus the PROBE a failed setup leaves behind. Legacy CLAUDE_STUB_*
    // keys are foreign state; unset does not touch them.
    for (k, v) in [
        ("OPENCODE_CLAUDE_PATH", client_bin()),
        ("CLAUDE_CONFIG_DIR", workdir.join("mirror").display().to_string()),
        ("PROBE", "PROBE".to_string()),
    ] {
        run_fake(&["service", "set", "env", k, &v]);
    }
    // A client config file from setup must be erased along with the env keys.
    let cfg_file = workdir.join("client.toml");
    std::fs::write(&cfg_file, "version = 1\nurl = \"ws://127.0.0.1:1\"\n").unwrap();

    let run_unset_with_path = |path: Option<std::path::PathBuf>| -> std::process::Output {
        let mut cmd = Command::new(&client_abs);
        cmd.arg("unset")
            .env_remove("CLAUDE_STUB_URL")
            .env_remove("CLAUDE_STUB_TOKEN")
            .env_remove("CLAUDE_CONFIG_DIR")
            .env_remove("CLAUDE_STUB_MIRROR")
            .env("ZONEPAIR_CLIENT_CONFIG", &cfg_file);
        match path {
            Some(p) => {
                cmd.env("PATH", &p);
            }
            None => {
                cmd.env("PATH", format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap_or_default()))
                    .env("ZONEPAIR_FAKE_OC_STATE", &state);
            }
        }
        cmd.output().unwrap_or_else(|e| fail(&format!("spawn stub-client unset: {e}")))
    };

    // Happy path: the config file erased, every env key removed, each
    // confirmed on stdout, and the environment answers empty through
    // `service get env` afterwards.
    let out = run_unset_with_path(None);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    if !out.status.success() {
        fail(&format!("unset failed: {stdout}{stderr}"));
    }
    for key in ["OPENCODE_CLAUDE_PATH", "CLAUDE_CONFIG_DIR", "PROBE"] {
        if !stdout.contains(&format!("removed {key}")) {
            fail(&format!("unset did not confirm removal of {key}: {stdout}"));
        }
        let got = run_fake(&["service", "get", "env", key]);
        if !got.trim().is_empty() {
            fail(&format!("{key} still set after unset: {got}"));
        }
    }
    if !stdout.contains(&format!("removed {}", cfg_file.display())) {
        fail(&format!("unset did not report removing the client config: {stdout}"));
    }
    if cfg_file.exists() {
        fail("client config file still present after unset");
    }

    // Idempotent: a second run on the now-clean environment is a no-op.
    let out = run_unset_with_path(None);
    if !out.status.success() {
        fail(&format!("unset not idempotent: {}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr)));
    }
    eprintln!("unset lifecycle ok (file erase, removal, probe sweep, idempotent, env answers empty)");

    // No server contact: unset never opened a connection to the relay.
    if let Some(child) = &server.child {
        assert_no_dial_for(child.id(), server.port);
    }

    // opencode absent: fallback prints one removal line per key, exit 0.
    let empty_path = workdir.join("empty-path");
    std::fs::create_dir_all(&empty_path).unwrap();
    let out = run_unset_with_path(Some(empty_path));
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() {
        fail(&format!("unset fallback failed: {stdout}{}", String::from_utf8_lossy(&out.stderr)));
    }
    for key in ["OPENCODE_CLAUDE_PATH", "CLAUDE_CONFIG_DIR"] {
        if !stdout.contains(&format!("unset {key}")) {
            fail(&format!("unset fallback missing removal line for {key}: {stdout}"));
        }
    }
    eprintln!("unset fallback ok (removal lines without opencode)");

    // opencode present but removal fails: non-zero exit, loud message.
    let broken_bin = workdir.join("broken-opencode-bin");
    std::fs::create_dir_all(&broken_bin).unwrap();
    let broken = broken_bin.join("opencode");
    std::fs::write(
        &broken,
        "#!/bin/sh\ncase \"$1 $2 $3\" in\n  \"service set env\") exit 0;;\n  \"service unset env\") echo \"simulated unset failure\" >&2; exit 3;;\nesac\nexit 0\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&broken, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let out = Command::new(&client_abs)
        .arg("unset")
        .env("PATH", &broken_bin)
        .env("ZONEPAIR_FAKE_OC_STATE", &state)
        .output()
        .unwrap();
    if out.status.success() {
        fail("unset exited 0 despite a failing removal command");
    }
    if !String::from_utf8_lossy(&out.stderr).contains("failed") {
        fail(&format!("unset hid the removal failure: {}", String::from_utf8_lossy(&out.stderr)));
    }
    eprintln!("unset failure propagation ok (non-zero on failed removal)");
}
