use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use zonepair_client::{api, transport as client};

// ---- fixtures ----------------------------------------------------------------

pub(crate) fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub(crate) fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("zonepair-e2e-{}-{}", std::process::id(), free_port()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Seed one session with `count` uuid-chained user lines, the way the real
/// projection writes them (so fork chains are verifiable).
pub(crate) fn seed_session(store: &Path, session: &str, count: usize) {
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

pub(crate) struct Server {
    pub(crate) child: Option<Child>,
    pub(crate) base: client::Base,
    pub(crate) port: u16,
    pub(crate) store: PathBuf,
    pub(crate) workdir: PathBuf,
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

pub(crate) fn server_bin() -> String {
    std::env::var("ZONEPAIR_SERVER_BIN").unwrap_or_else(|_| "stub-server".to_string())
}

pub(crate) fn mock_bin() -> String {
    std::env::var("ZONEPAIR_MOCK_BIN").unwrap_or_else(|_| "claude-mock".to_string())
}

pub(crate) fn client_bin() -> String {
    std::env::var("ZONEPAIR_CLIENT_BIN").unwrap_or_else(|_| "stub-client".to_string())
}

pub(crate) async fn spawn_server() -> Server {
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

pub(crate) async fn external_server() -> Server {
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

pub(crate) async fn spawn_or_external() -> Server {
    match std::env::var("ZONEPAIR_EXTERNAL_URL") {
        Ok(url) if !url.is_empty() => external_server().await,
        _ => spawn_server().await,
    }
}

pub(crate) async fn wait_ready(base: &client::Base) {
    for _ in 0..200 {
        if api::store_list(base, None).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("server never became ready");
}

/// Open a stdio stream for a mock claude session.
pub(crate) async fn open_session(base: &client::Base, session: &str) -> client::Stdio {
    stdio_for(base, &["--session-id", session]).await
}

/// Open a stdio stream with explicit claude argv. The hello header carries the
/// allowlisted env (`envf::collect()`), so provider wiring rides the same
/// handshake.
pub(crate) async fn stdio_for(base: &client::Base, argv: &[&str]) -> client::Stdio {
    let token = std::env::var("CLAUDE_STUB_TOKEN").ok().filter(|v| !v.is_empty());
    client::open_stdio(
        base,
        &json!({
            "argv": argv,
            "env": zonepair_client::proto::envf::collect(),
            "cwd": std::env::temp_dir(),
        }),
        token.as_deref(),
    )
    .await
    .expect("open stdio")
}

/// Drive one turn: send a user message, collect lines until `result`, then
/// signal EOF and drain. Returns every protocol line received.
pub(crate) async fn drive_turn(stream: &mut client::Stdio, text: &str, eof: bool) -> Vec<Value> {
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

pub(crate) fn assistant_text(lines: &[Value]) -> String {
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

pub(crate) fn fail(msg: &str) -> ! {
    eprintln!("FAIL: {msg}");
    std::process::exit(1);
}
