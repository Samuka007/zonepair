//! The integration suite as a runnable artifact.
//!
//! Native (default): spawns the stub-server binary and drives the whole mock
//! suite against it — this is what `cargo test` and `nix flake check` run.
//! Container: run it inside the server image (`ZONEPAIR_SERVER_BIN=/bin/stub-server
//! stub-client…/zonepair-e2e`) and it exercises the same suite in the
//! container's own network namespace, so the ADR-0001 no-dial check works
//! there too.
//!
//! `ZONEPAIR_EXTERNAL_URL=http://…`: drive an already-running server instead of
//! spawning one (host-against-container).

use claude_io_stub::client;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};

// ---- fixtures ----------------------------------------------------------------

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("zonepair-e2e-{}-{}", std::process::id(), free_port()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Seed one session with `count` uuid-chained user lines, the way the real
/// projection writes them (so fork chains are verifiable).
fn seed_session(store: &Path, session: &str, count: usize) {
    let proj = store.join("projects").join("proj-a");
    std::fs::create_dir_all(&proj).unwrap();
    let mut parent: Option<String> = None;
    let mut out = String::new();
    for i in 0..count {
        let uuid = format!("u-{session}-{i}");
        out.push_str(
            &json!({
                "type": "user",
                "uuid": uuid,
                "sessionId": session,
                "parentUuid": parent,
                "message": {"role": "user", "content": format!("message {i}")},
            })
            .to_string(),
        );
        out.push('\n');
        parent = Some(uuid);
    }
    std::fs::write(proj.join(format!("{session}.jsonl")), out).unwrap();
}

struct Server {
    child: Option<Child>,
    base: client::Base,
    port: u16,
    store: PathBuf,
    workdir: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        if std::env::var("ZONEPAIR_KEEP").is_err() {
            let _ = std::fs::remove_dir_all(&self.workdir);
        }
    }
}

fn server_bin() -> String {
    std::env::var("ZONEPAIR_SERVER_BIN").unwrap_or_else(|_| "stub-server".to_string())
}

fn mock_bin() -> String {
    std::env::var("ZONEPAIR_MOCK_BIN").unwrap_or_else(|_| "claude-mock".to_string())
}

async fn spawn_server() -> Server {
    let workdir = tempdir();
    let store = workdir.join("store");
    let logs = workdir.join("logs");
    std::fs::create_dir_all(&logs).unwrap();

    let port = free_port();
    let child = Command::new(server_bin())
        .arg("--listen")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--store")
        .arg(&store)
        .arg("--claude")
        .arg(mock_bin())
        .arg("--log-dir")
        .arg(&logs)
        .spawn()
        .expect("spawn stub-server");
    let server = Server {
        child: Some(child),
        base: client::parse_base(&format!("ws://127.0.0.1:{port}")).unwrap(),
        port,
        store,
        workdir,
    };
    wait_ready(&server.base).await;
    server
}

async fn external_server() -> Server {
    let url = std::env::var("ZONEPAIR_EXTERNAL_URL").expect("ZONEPAIR_EXTERNAL_URL required for external mode");
    let base = client::parse_base(&url).expect("bad ZONEPAIR_EXTERNAL_URL");
    let port = url
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .expect("ZONEPAIR_EXTERNAL_URL needs an explicit port");
    let server = Server {
        child: None,
        base,
        port,
        store: PathBuf::new(),
        workdir: PathBuf::new(),
    };
    wait_ready(&server.base).await;
    server
}

async fn spawn_or_external() -> Server {
    match std::env::var("ZONEPAIR_EXTERNAL_URL") {
        Ok(url) if !url.is_empty() => external_server().await,
        _ => spawn_server().await,
    }
}

async fn wait_ready(base: &client::Base) {
    for _ in 0..200 {
        if client::store_list(base, None).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("server never became ready");
}

/// Open a stdio stream for a mock claude session.
async fn open_session(base: &client::Base, session: &str) -> client::Stdio {
    stdio_for(base, &["--session-id", session]).await
}

/// Open a stdio stream with explicit claude argv. The hello header carries the
/// allowlisted env (`envf::collect()`), so provider wiring rides the same
/// handshake.
async fn stdio_for(base: &client::Base, argv: &[&str]) -> client::Stdio {
    let token = std::env::var("CLAUDE_STUB_TOKEN").ok().filter(|v| !v.is_empty());
    client::open_stdio(
        base,
        &json!({
            "argv": argv,
            "env": claude_io_stub::envf::collect(),
            "cwd": std::env::temp_dir(),
        }),
        token.as_deref(),
    )
    .await
    .expect("open stdio")
}

/// Drive one turn: send a user message, collect lines until `result`, then
/// signal EOF and drain. Returns every protocol line received.
async fn drive_turn(stream: &mut client::Stdio, text: &str, eof: bool) -> Vec<Value> {
    stream
        .send_line(&json!({
            "type": "user",
            "message": {"role": "user", "content": [{"type": "text", "text": text}]},
        }))
        .await
        .unwrap();
    if eof {
        stream.send_eof().await.unwrap();
    }
    let mut lines = Vec::new();
    loop {
        match stream.next_line().await.expect("frame") {
            Some(line) => {
                let v: Value = serde_json::from_str(&line).unwrap_or(Value::Null);
                let is_result = v.get("type").and_then(Value::as_str) == Some("result");
                lines.push(v);
                if is_result {
                    break;
                }
            }
            None => break,
        }
    }
    lines
}

fn assistant_text(lines: &[Value]) -> String {
    lines
        .iter()
        .filter(|l| l.get("type").and_then(Value::as_str) == Some("assistant"))
        .filter_map(|l| l["message"]["content"].as_array())
        .flat_map(|blocks| blocks.iter())
        .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|b| b.get("text").and_then(Value::as_str).map(str::to_string))
        .collect::<Vec<_>>()
        .join("")
}

fn fail(msg: &str) -> ! {
    eprintln!("FAIL: {msg}");
    std::process::exit(1);
}

// ---- no-dial (ADR-0001) ------------------------------------------------------

fn socket_inodes(pid: u32) -> HashSet<String> {
    let fd_dir = format!("/proc/{pid}/fd");
    let mut inodes = HashSet::new();
    let Ok(entries) = std::fs::read_dir(fd_dir) else {
        return inodes;
    };
    for entry in entries.flatten() {
        if let Ok(link) = std::fs::read_link(entry.path()) {
            let s = link.to_string_lossy().to_string();
            if let Some(inode) = s.strip_prefix("socket:[") {
                inodes.insert(inode.trim_end_matches(']').to_string());
            }
        }
    }
    inodes
}

fn established_rows() -> Vec<(u16, String)> {
    let mut rows = Vec::new();
    for file in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(text) = std::fs::read_to_string(file) else { continue };
        for line in text.lines().skip(1) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 10 || parts[3] != "01" {
                continue;
            }
            let local_port = u16::from_str_radix(parts[1].rsplit(':').next().unwrap_or("0"), 16).unwrap_or(0);
            rows.push((local_port, parts[9].to_string()));
        }
    }
    rows
}

/// ADR-0001, enforced: the server process must own no established socket that
/// it did not accept on its listen port.
fn assert_no_dial_for(child_id: u32, port: u16) {
    let inodes = socket_inodes(child_id);
    assert!(!inodes.is_empty(), "server has no open fds");
    let offenders: Vec<u16> = established_rows()
        .into_iter()
        .filter(|(_, inode)| inodes.contains(inode))
        .map(|(port, _)| port)
        .filter(|own| *own != port)
        .collect();
    assert!(
        offenders.is_empty(),
        "server holds established connection(s) on {offenders:?} — an outbound dial"
    );
}

fn assert_no_dial(server: &Server) {
    let Some(child) = &server.child else {
        eprintln!("  note: external server — no-dial not attributable, skipped");
        return;
    };
    assert_no_dial_for(child.id(), server.port);
}

// ---- the suite -----------------------------------------------------------------

async fn basic_turn(base: &client::Base) {
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
    assert!(client::store_exists(base, None, "sess-basic").await.unwrap());
    let messages = client::store_messages(base, None, "sess-basic").await.unwrap();
    assert_eq!(messages.len(), 2, "user + assistant lines");
}

async fn tool_round_trip(base: &client::Base) {
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

async fn sess_rides_http(base: &client::Base, server: &Server) {
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
        client::store_merge(base, None, "session_aaa", &lines).await.unwrap();
    }

    let sessions = client::store_list(base, None).await.unwrap();
    assert!(sessions.contains(&"session_aaa".to_string()));
    assert!(sessions.contains(&"session_bbb".to_string()));

    assert!(client::store_exists(base, None, "session_aaa").await.unwrap());
    assert!(!client::store_exists(base, None, "no-such-session").await.unwrap());

    let messages = client::store_messages(base, None, "session_aaa").await.unwrap();
    assert_eq!(messages.len(), 5);

    // Fork at the 3rd message: fresh session id, uuid chain rewritten, 3 lines.
    let (forked, pairs, fork_messages) =
        client::store_fork(base, None, "session_aaa", "u-session_aaa-2").await.unwrap();
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

    assert!(client::store_exists(base, None, &forked).await.unwrap());
    assert!(client::store_delete(base, None, &forked).await.unwrap());
    assert!(!client::store_exists(base, None, &forked).await.unwrap());
}

async fn mirror_sync_pushes_and_pulls(base: &client::Base, server: &Server) {
    let mirror = if server.workdir.as_os_str().is_empty() {
        tempdir()
    } else {
        let m = server.workdir.join("mirror");
        std::fs::create_dir_all(&m).unwrap();
        m
    };
    let proj = mirror.join("projects").join("lab");
    std::fs::create_dir_all(&proj).unwrap();

    // A-side fork written by opencode's in-process helpers: two uuid-chained
    // lines under a fresh session id.
    let lines = vec![
        json!({"type":"user","uuid":"u-mirror-0","sessionId":"sess-mirror","parentUuid":null,
               "message":{"role":"user","content":"local fork line 0"}}),
        json!({"type":"user","uuid":"u-mirror-1","sessionId":"sess-mirror","parentUuid":"u-mirror-0",
               "message":{"role":"user","content":"local fork line 1"}}),
    ];
    let mut body = String::new();
    for l in &lines {
        body.push_str(&l.to_string());
        body.push('\n');
    }
    std::fs::write(proj.join("sess-mirror.jsonl"), body).unwrap();

    let (pushed, pulled) = client::sync_mirror(base, None, &mirror).await.unwrap();
    assert!(pushed >= 2, "the local fork must be pushed");
    assert!(pulled > 0, "B's sessions must be pulled into the mirror");
    assert!(client::store_exists(base, None, "sess-mirror").await.unwrap());
}

async fn three_clients_one_server(base: &client::Base) {
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
        assert!(client::store_exists(base, None, &format!("sess-par-{i}")).await.unwrap());
    }
}

/// ADR-0001, enforced: after traffic has flowed, the server process owns no
/// established socket that it did not accept on its listen port.
async fn server_never_dials(server: &Server) {
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
async fn real_model_suite() {
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
    let messages = client::store_messages(&base, None, &session).await.expect("store messages");
    assert!(messages.len() >= 2, "real session did not land in B's store");
    let first_user = messages
        .iter()
        .find(|m| m.get("type").and_then(Value::as_str) == Some("user"))
        .and_then(|m| m.get("uuid"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| fail("no user uuid in B's store"))
        .to_string();

    // ---- fork the real store ----------------------------------------------
    let (forked, _pairs, _fm) = client::store_fork(&base, None, &session, &first_user)
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

    assert!(client::store_delete(&base, None, &forked).await.unwrap(), "delete forked session");
    assert_no_dial_for(child.id(), port);
}

async fn run_suite() {
    if std::env::var("ZONEPAIR_REAL").is_ok_and(|v| v == "1") {
        eprintln!("real-model suite");
        real_model_suite().await;
        eprintln!("REAL-MODEL E2E PASS");
        return;
    }

    let server = spawn_or_external().await;
    basic_turn(&server.base).await;
    tool_round_trip(&server.base).await;
    sess_rides_http(&server.base, &server).await;
    mirror_sync_pushes_and_pulls(&server.base, &server).await;
    three_clients_one_server(&server.base).await;
    if server.child.is_some() {
        server_never_dials(&server).await;
    } else {
        eprintln!("  note: external server — no-dial not attributable, skipped");
    }
    assert_no_dial(&server);
    eprintln!("E2E PASS (carrier=ws-line-v1, {} sessions on B)", {
        client::store_list(&server.base, None).await.unwrap().len()
    });
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(run_suite());
}
