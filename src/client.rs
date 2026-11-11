//! Client-side speaker for the `ws-line-v1` carrier: WS for stdio, plain HTTP
//! for the session store. This is the only way a client talks to the server;
//! there is no raw-TCP path.
//!
//! TLS: a `wss://` / `https://` base URL makes the client itself speak TLS
//! (rustls, webpki-roots) to the gateway edge — cloudflare tunnel, nginx, any
//! ADR-0005 terminator. Behind the gateway the carrier stays plain
//! `ws://` / `http://`; direct and loopback deployments keep dialing those.

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::{Arc, LazyLock};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::Connector;

type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;
pub type WsTx = futures_util::stream::SplitSink<WsStream, Message>;
pub type WsRx = futures_util::stream::SplitStream<WsStream>;

/// Base URL of a ws-line-v1 server: scheme + authority + optional path prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Base(String);

pub fn parse_base(raw: &str) -> Result<Base> {
    let raw = raw.trim().trim_end_matches('/');
    let Some((scheme, rest)) = raw.split_once("://") else {
        bail!("CLAUDE_STUB_URL must include a scheme (ws:// or http://), got {raw:?}");
    };
    match scheme {
        "ws" | "http" | "wss" | "https" => {}
        other => bail!("unknown scheme {other:?} in CLAUDE_STUB_URL"),
    }
    if rest.is_empty() {
        bail!("CLAUDE_STUB_URL needs a host");
    }
    Ok(Base(format!("{scheme}://{rest}")))
}

impl Base {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn url(&self, scheme: &str, path: &str) -> String {
        let rest = self.0.split_once("://").expect("parsed base").1;
        format!("{scheme}://{rest}{path}")
    }

    /// WS endpoint carrying stdio-v1. A TLS base dials `wss://`.
    pub fn stdio_ws(&self) -> String {
        self.url(if self.is_tls() { "wss" } else { "ws" }, "/v1/stdio")
    }

    fn is_tls(&self) -> bool {
        self.0.starts_with("https://") || self.0.starts_with("wss://")
    }
}

// ---- stdio ------------------------------------------------------------------

pub struct Stdio {
    pub tx: WsTx,
    pub rx: WsRx,
}

/// Shared rustls client config (webpki-roots: no system trust store needed,
/// which is what keeps the musl static build self-contained).
fn tls_config() -> Arc<rustls::ClientConfig> {
    static CONFIG: LazyLock<Arc<rustls::ClientConfig>> = LazyLock::new(|| {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    });
    CONFIG.clone()
}

fn ws_connector() -> Connector {
    Connector::Rustls(tls_config())
}

/// Dial `/v1/stdio`, send the hello header and consume the ack.
pub async fn open_stdio(base: &Base, header: &Value, token: Option<&str>) -> Result<Stdio> {
    let mut request = base
        .stdio_ws()
        .into_client_request()
        .context("bad ws url")?;
    if let Some(t) = token {
        let value = HeaderValue::from_str(&format!("Bearer {t}")).context("bad token header")?;
        request.headers_mut().insert(AUTHORIZATION, value);
    }
    let (ws, _) = tokio_tungstenite::connect_async_tls_with_config(
        request,
        None,
        false,
        Some(ws_connector()),
    )
    .await
    .with_context(|| format!("ws dial {}", base.stdio_ws()))?;
    let (mut tx, mut rx) = ws.split();

    tx.send(Message::Text(header.to_string().into()))
        .await
        .context("sending hello")?;
    let ack = loop {
        match rx.next().await {
            Some(Ok(Message::Text(t))) => break t,
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(e).context("ws error while waiting for ack"),
            None => bail!("connection closed before ack"),
        }
    };
    let ack: Value = serde_json::from_str(&ack).context("ack is not JSON")?;
    if ack.get("ok") != Some(&json!(true)) {
        bail!(
            "stub-server refused: {}",
            ack.get("error").cloned().unwrap_or(json!("unknown error"))
        );
    }
    Ok(Stdio { tx, rx })
}

impl Stdio {
    /// Send one protocol line (one text frame; the newline is implied).
    pub async fn send_line(&mut self, line: &Value) -> Result<()> {
        self.tx
            .send(Message::Text(line.to_string().into()))
            .await
            .context("sending frame")
    }

    /// Ask the server to close the child's stdin (claude flushes on EOF).
    pub async fn send_eof(&mut self) -> Result<()> {
        self.tx
            .send(Message::Text(
                json!({"proto": "stdin-eof"}).to_string().into(),
            ))
            .await
            .context("sending stdin-eof")
    }

    /// Next protocol line from the child. `None` when the stream closes.
    /// Transport control frames (`{"proto": …}`) are skipped: they are the
    /// carrier talking, not the protocol.
    pub async fn next_line(&mut self) -> Result<Option<String>> {
        loop {
            match self.rx.next().await {
                Some(Ok(Message::Text(frame))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&frame) {
                        if v.get("proto").is_some() {
                            continue; // carrier control frame, not the protocol
                        }
                    }
                    // Utf8Bytes derefs to str; return owned String (protocol
                    // lines are handed to callers that outlive the frame).
                    return Ok(Some(frame.to_string())); // non-JSON passthrough (claude warnings)
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(None),
            }
        }
    }
}

// ---- session store over HTTP -------------------------------------------------

fn http_client(tls: rustls::ClientConfig) -> Result<Client> {
    Client::builder()
        .tls_backend_preconfigured(tls)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .build()
        .context("building store HTTP client")
}

static HTTP_CLIENT: LazyLock<Client> = LazyLock::new(|| {
    http_client(tls_config().as_ref().clone()).expect("valid store HTTP client configuration")
});

async fn request(
    client: &Client,
    base: &Base,
    method: Method,
    path: &str,
    body: Option<&Value>,
    token: Option<&str>,
) -> Result<(u16, Value)> {
    let url = base.url(if base.is_tls() { "https" } else { "http" }, path);
    let mut request = client
        .request(method, url)
        .header("Accept", "application/json");
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = request.send().await.context("sending store request")?;
    let status = response.status().as_u16();
    let body = response.bytes().await.context("reading store response")?;
    let value = if body.iter().all(u8::is_ascii_whitespace) {
        Value::Null
    } else {
        serde_json::from_slice(&body).context("non-JSON store response")?
    };
    Ok((status, value))
}

/// Backoff between attempts of one store request. Three attempts total; the
/// delays are fixed (no jitter) so tests are deterministic and the whole
/// pre-relay sync cannot stall for more than ~0.4 s on a flaky link.
const RETRY_BACKOFF: [std::time::Duration; 2] = [
    std::time::Duration::from_millis(100),
    std::time::Duration::from_millis(300),
];

/// Transport-level failure worth one more attempt: connect refused/reset,
/// timeout, or a request/body error (a peer that hung up mid-exchange).
fn retryable_transport(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request() || error.is_body()
}

/// 5xx only. 4xx is definitive (`404` must stay "not stored") and is never
/// retried.
fn retryable_status(status: u16) -> bool {
    (500..600).contains(&status)
}

/// `request` with bounded retries on transport errors and 5xx responses. The
/// final failure keeps the same contextual error `request` produced.
async fn request_with_retry(
    client: &Client,
    base: &Base,
    method: Method,
    path: &str,
    body: Option<&Value>,
    token: Option<&str>,
) -> Result<(u16, Value)> {
    let mut attempt = 0;
    loop {
        let outcome = request(client, base, method.clone(), path, body, token).await;
        let retries_left = RETRY_BACKOFF.len() - attempt;
        let retry = retries_left > 0
            && match &outcome {
                Ok((status, _)) => retryable_status(*status),
                Err(error) => error
                    .downcast_ref::<reqwest::Error>()
                    .is_some_and(retryable_transport),
            };
        if !retry {
            return outcome;
        }
        tokio::time::sleep(RETRY_BACKOFF[attempt]).await;
        attempt += 1;
    }
}

fn ensure_ok(status: u16, value: &Value) -> Result<()> {
    if (200..300).contains(&status) && value.get("ok") != Some(&json!(false)) {
        return Ok(());
    }
    let error = value
        .get("error")
        .map(|e| e.to_string())
        .unwrap_or_else(|| value.to_string());
    bail!("store op failed ({status}): {error}")
}

pub async fn store_list(base: &Base, token: Option<&str>) -> Result<Vec<String>> {
    let (status, v) = request_with_retry(
        &HTTP_CLIENT,
        base,
        Method::GET,
        "/v1/store/sessions",
        None,
        token,
    )
    .await?;
    ensure_ok(status, &v)?;
    Ok(v["sessions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|s| s.as_str().map(str::to_string))
        .collect())
}

pub async fn store_exists(base: &Base, token: Option<&str>, session: &str) -> Result<bool> {
    let path = format!("/v1/store/sessions/{session}");
    let (status, v) =
        request_with_retry(&HTTP_CLIENT, base, Method::GET, &path, None, token).await?;
    if status == 404 {
        return Ok(false);
    }
    ensure_ok(status, &v)?;
    Ok(v.get("exists").and_then(Value::as_bool).unwrap_or(false))
}

pub async fn store_messages(base: &Base, token: Option<&str>, session: &str) -> Result<Vec<Value>> {
    let path = format!("/v1/store/sessions/{session}/messages");
    let (status, v) =
        request_with_retry(&HTTP_CLIENT, base, Method::GET, &path, None, token).await?;
    ensure_ok(status, &v)?;
    Ok(v["messages"].as_array().cloned().unwrap_or_default())
}

pub async fn store_fork(
    base: &Base,
    token: Option<&str>,
    session: &str,
    up_to: &str,
) -> Result<(String, Vec<Value>, Vec<Value>)> {
    let path = format!("/v1/store/sessions/{session}/fork");
    let (status, v) = request_with_retry(
        &HTTP_CLIENT,
        base,
        Method::POST,
        &path,
        Some(&json!({"upTo": up_to})),
        token,
    )
    .await?;
    ensure_ok(status, &v)?;
    let new = v
        .get("sessionId")
        .and_then(Value::as_str)
        .context("fork response missing sessionId")?
        .to_string();
    let pairs = v["pairs"].as_array().cloned().unwrap_or_default();
    let messages = v["messages"].as_array().cloned().unwrap_or_default();
    Ok((new, pairs, messages))
}

pub async fn store_delete(base: &Base, token: Option<&str>, session: &str) -> Result<bool> {
    let path = format!("/v1/store/sessions/{session}");
    let (status, v) =
        request_with_retry(&HTTP_CLIENT, base, Method::DELETE, &path, None, token).await?;
    ensure_ok(status, &v)?;
    Ok(v.get("deleted").and_then(Value::as_bool).unwrap_or(false))
}

pub async fn store_merge(
    base: &Base,
    token: Option<&str>,
    session: &str,
    messages: &[Value],
) -> Result<(u64, u64)> {
    let path = format!("/v1/store/sessions/{session}/messages");
    let (status, v) = request_with_retry(
        &HTTP_CLIENT,
        base,
        Method::PUT,
        &path,
        Some(&json!({"messages": messages})),
        token,
    )
    .await?;
    ensure_ok(status, &v)?;
    Ok((
        v["added"].as_u64().unwrap_or(0),
        v["total"].as_u64().unwrap_or(0),
    ))
}

// ---- incremental mirror push (issue #16) -------------------------------------

/// Per-session push progress, persisted at `<mirror>/.sync-state.json`.
/// `.sync-state.json` (not `.jsonl`) so the mirror walker skips it.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct SyncState {
    version: u32,
    sessions: std::collections::HashMap<String, SyncedSession>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncedSession {
    last_uuid: String,
    count: u64,
}

const SYNC_STATE_VERSION: u32 = 1;

fn sync_state_path(mirror: &Path) -> std::path::PathBuf {
    mirror.join(".sync-state.json")
}

/// Lenient load: a missing or corrupt state file means "nothing pushed yet",
/// never a sync failure.
fn load_sync_state(mirror: &Path) -> SyncState {
    std::fs::read_to_string(sync_state_path(mirror))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

/// Atomic write (temp file + rename in the same directory); a failed save
/// costs at most a redundant next-sync PUT, so it is best-effort.
fn save_sync_state(mirror: &Path, state: &mut SyncState) {
    state.version = SYNC_STATE_VERSION;
    let path = sync_state_path(mirror);
    let Ok(raw) = serde_json::to_vec_pretty(state) else {
        return;
    };
    let temp = mirror.join(".sync-state.json.tmp");
    if std::fs::write(&temp, raw).is_ok() && std::fs::rename(&temp, &path).is_err() {
        let _ = std::fs::remove_file(&temp);
    }
}

/// Push one mirror file into B's store using the recorded push state:
///
/// - clean session (state's last uuid == remote tail uuid, local count <=
///   remote count): 0 PUTs;
/// - new session (remote has none): PUT everything, record the state;
/// - otherwise PUT only the tail after the recorded last-pushed uuid — or all
///   lines when state and remote disagree (remote was reset/rewound; the
///   store-side union makes a full PUT safe). On success the state is updated
///   to the last uuid now pushed. Returns `(added, skipped)`.
async fn push_session(
    base: &Base,
    token: Option<&str>,
    session: &str,
    lines: &[Value],
    state: &mut SyncState,
) -> Result<(usize, bool)> {
    // Nothing comparable to push: skip before any network round trip.
    if lines.is_empty() || lines.last().and_then(uuid_of).is_none() {
        return Ok((0, true));
    }

    // Cheap existence probe. A recorded session whose remote copy was wiped
    // (or never landed) must not crash on a 404 from the messages read.
    let exists = store_exists(base, token, session).await?;
    let Some(recorded) = state.sessions.get(session).cloned() else {
        // First-time sync: one cheap exists probe above, then a full PUT
        // either way — the store unions by uuid, and only a full PUT captures
        // uuids the state file has never seen.
        let (added, _) = store_merge(base, token, session, lines).await?;
        record_pushed(state, session, lines);
        return Ok((added as usize, false));
    };

    if !exists {
        // Remote gone (reset/wiped): a full PUT restores the whole session;
        // union semantics keep it safe.
        let (added, _) = store_merge(base, token, session, lines).await?;
        record_pushed(state, session, lines);
        return Ok((added as usize, false));
    }

    // Recorded progress: compare against the remote tail.
    let remote = store_messages(base, token, session).await?;
    let remote_tail = remote.last().and_then(uuid_of).map(str::to_string);
    let local_tail = lines.last().and_then(uuid_of).map(str::to_string);

    // Clean: everything local is already stored. Skip with zero PUTs.
    if remote_tail == local_tail
        && local_tail.as_deref() == Some(recorded.last_uuid.as_str())
        && (lines.len() as u64) <= remote.len() as u64
    {
        return Ok((0, true));
    }

    // State disagrees with the remote (tail differs or the remote session was
    // reset/rewound to empty): fall back to a full PUT; union semantics keep
    // it safe and idempotent.
    if remote_tail.as_deref() != Some(recorded.last_uuid.as_str()) {
        let (added, _) = store_merge(base, token, session, lines).await?;
        record_pushed(state, session, lines);
        return Ok((added as usize, false));
    }

    // Remote tail is exactly the recorded last uuid: PUT only the lines after
    // the recorded position — but only if the local file still agrees with
    // what was recorded there.
    let recorded_count = recorded.count as usize;
    if lines.len() > recorded_count
        && recorded_count > 0
        && lines.get(recorded_count - 1).and_then(uuid_of) == Some(recorded.last_uuid.as_str())
    {
        let tail = &lines[recorded_count..];
        let (added, _) = store_merge(base, token, session, tail).await?;
        record_pushed(state, session, lines);
        return Ok((added as usize, false));
    }

    // Local file no longer lines up with the recorded position (mirror
    // rewritten, or state from another mirror): full PUT is the safe move.
    let (added, _) = store_merge(base, token, session, lines).await?;
    record_pushed(state, session, lines);
    Ok((added as usize, false))
}

/// Record that everything in `lines` is now stored remotely for `session`.
fn record_pushed(state: &mut SyncState, session: &str, lines: &[Value]) {
    if let Some(uuid) = lines.last().and_then(uuid_of) {
        state.sessions.insert(
            session.to_string(),
            SyncedSession {
                last_uuid: uuid.to_string(),
                count: lines.len() as u64,
            },
        );
    }
}

// ---- store mirror (ADR-0004) --------------------------------------------------

fn read_jsonl(path: &Path) -> Result<Vec<Value>> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).context("mirror line is not JSON"))
        .collect()
}

fn write_jsonl(path: &Path, lines: &[Value]) -> Result<()> {
    let mut out = String::new();
    for line in lines {
        out.push_str(&line.to_string());
        out.push('\n');
    }
    std::fs::write(path, out).with_context(|| format!("writing {}", path.display()))
}

fn uuid_of(line: &Value) -> Option<&str> {
    line.get("uuid").and_then(Value::as_str)
}

/// Union by uuid: base order = existing file, then incoming lines in incoming
/// order. Mirrors `Store::merge` on the mirror side.
fn merge_lines(base: &mut Vec<Value>, incoming: Vec<Value>) -> usize {
    let mut seen: std::collections::HashSet<String> = base
        .iter()
        .filter_map(|l| uuid_of(l).map(str::to_string))
        .collect();
    let mut added = 0usize;
    for line in incoming {
        let Some(uuid) = uuid_of(&line) else { continue };
        if seen.insert(uuid.to_string()) {
            base.push(line);
            added += 1;
        }
    }
    added
}

/// Push every `*.jsonl` under the mirror into B's store (merge by file stem),
/// then pull every B session into `<mirror>/projects/lab/<session>.jsonl`.
pub async fn sync_mirror(
    base: &Base,
    token: Option<&str>,
    mirror: &Path,
) -> Result<(usize, usize)> {
    let mut pushed = 0usize;
    let mut state = load_sync_state(mirror);

    let mut stack = vec![mirror.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            // Extension-only filter: `.sync-state.json` (and any other
            // non-jsonl file) is never treated as a mirror session.
            if path.extension().is_none_or(|e| e != "jsonl") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let lines = read_jsonl(&path)?;
            let (added, _) = push_session(base, token, stem, &lines, &mut state)
                .await
                .with_context(|| format!("pushing {}", path.display()))?;
            pushed += added;
        }
    }
    // Best-effort: a failed state save must not fail the sync; the next sync
    // then repeats idempotent PUTs (union semantics).
    save_sync_state(mirror, &mut state);

    let mut pulled = 0usize;
    for session in store_list(base, token).await? {
        let remote = store_messages(base, token, &session).await?;
        let dest = mirror.join("projects").join("lab");
        std::fs::create_dir_all(&dest)?;
        let path = dest.join(format!("{session}.jsonl"));
        let mut line_values = if path.exists() {
            read_jsonl(&path)?
        } else {
            Vec::new()
        };
        pulled += merge_lines(&mut line_values, remote);
        write_jsonl(&path, &line_values)?;
    }
    Ok((pushed, pulled))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    const CHUNKED_RESPONSE: &[u8] =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
          17\r\n{\"ok\":true,\"sessions\":[\r\n\
          a\r\n\"gateway\"]\r\n\
          1\r\n}\r\n0\r\n\r\n";

    async fn reply<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, response: &[u8]) {
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = stream.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0, "client closed before request headers");
            request.extend_from_slice(&buffer[..count]);
        }
        stream.write_all(response).await.unwrap();
        stream.flush().await.unwrap();
    }

    async fn serve_http(response: &'static [u8]) -> (Base, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = parse_base(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            reply(&mut stream, response).await;
            stream.shutdown().await.unwrap();
        });
        (base, server)
    }

    #[tokio::test]
    async fn store_list_decodes_chunked_gateway_response() {
        let (base, server) = serve_http(CHUNKED_RESPONSE).await;
        let sessions =
            tokio::time::timeout(std::time::Duration::from_secs(5), store_list(&base, None))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(sessions, ["gateway"]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn store_list_rejects_truncated_body() {
        let (base, server) = serve_http(
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n\
              {\"ok\":true,\"sessions\":[\"incomplete\"]}",
        )
        .await;
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(5), store_list(&base, None))
                .await
                .unwrap();
        assert!(
            result.is_err(),
            "accepted a body shorter than Content-Length"
        );
        server.await.unwrap();
    }

    async fn serve_tls(response: &'static [u8]) -> (Base, Client, tokio::task::JoinHandle<()>) {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.der().clone()).unwrap();
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let client = http_client(tls).unwrap();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(signing_key.serialize_der());
        let tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.der().clone()], key.into())
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = parse_base(&format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port(),
        ))
        .unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            reply(&mut stream, response).await;
            // Deliberately drop TCP without TLS close_notify, as gateways may.
        });
        (base, client, server)
    }

    #[tokio::test]
    async fn tls_chunked_body_completes_without_close_notify() {
        let (base, client, server) = serve_tls(CHUNKED_RESPONSE).await;
        let (status, value) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            request(
                &client,
                &base,
                Method::GET,
                "/v1/store/sessions",
                None,
                None,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(status, 200);
        assert_eq!(value, json!({"ok": true, "sessions": ["gateway"]}));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tls_truncated_body_is_not_an_empty_success() {
        let (base, client, server) =
            serve_tls(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n").await;
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            request(
                &client,
                &base,
                Method::GET,
                "/v1/store/sessions",
                None,
                None,
            ),
        )
        .await
        .unwrap()
        .expect_err("accepted a truncated TLS response as empty data");
        let cause = error
            .downcast_ref::<reqwest::Error>()
            .expect("HTTP body error");
        assert!(cause.is_body() || cause.is_decode(), "{error:#}");
        server.await.unwrap();
    }

    /// One scripted server response: respond with `bytes`, or drop the
    /// connection without answering (simulates a reset).
    enum Scripted {
        Reply(Vec<u8>),
        Drop,
    }

    struct ScriptedRequest {
        method_path: String,
        has_body: bool,
    }

    fn parse_request(request: &[u8]) -> ScriptedRequest {
        let header_end = request
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .expect("request headers");
        let head = std::str::from_utf8(&request[..header_end]).unwrap();
        let first_line = head.lines().next().unwrap();
        let mut parts = first_line.split_whitespace();
        let method = parts.next().unwrap();
        let path = parts.next().unwrap();
        let body_start = header_end + 4;
        ScriptedRequest {
            method_path: format!("{method} {path}"),
            has_body: request.len() > body_start,
        }
    }

    fn http_json(status: u16, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status} Z\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        )
        .into_bytes()
    }

    /// Serve exactly `script.len()` connections in the background: connection
    /// N reads one full request (headers plus Content-Length body), records
    /// it, and answers from `script[N]`. Handled inline and sequentially —
    /// the client is strictly sequential and closes each connection
    /// (`Connection: close`), so there is no accept/reply race. The accept
    /// loop ENDS after the last scripted connection: a script longer than the
    /// client's request sequence fails the join timeout (with the recorded
    /// list) instead of hanging, and an extra client connection is refused.
    async fn serve_script(
        script: Vec<Vec<Scripted>>,
    ) -> (
        Base,
        Arc<std::sync::Mutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = parse_base(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_task = Arc::clone(&seen);
        let server = tokio::spawn(async move {
            for responses in script {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                while !request.windows(4).any(|part| part == b"\r\n\r\n") {
                    let count = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(count, 0, "client closed before request headers");
                    request.extend_from_slice(&buffer[..count]);
                }
                // Read the body too, so the client never sees a truncated
                // request before we answer or drop.
                let head = std::str::from_utf8(&request).unwrap();
                if let Some(length) = head
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(str::to_string)
                    })
                    .and_then(|v| v.trim().parse::<usize>().ok())
                {
                    let body_start = request.windows(4).position(|p| p == b"\r\n\r\n").unwrap() + 4;
                    while request.len() - body_start < length {
                        let count = stream.read(&mut buffer).await.unwrap();
                        assert_ne!(count, 0, "client closed before request body");
                        request.extend_from_slice(&buffer[..count]);
                    }
                }
                let parsed = parse_request(&request);
                seen_task.lock().unwrap().push(format!(
                    "{} body={}",
                    parsed.method_path,
                    if parsed.has_body { "yes" } else { "no" }
                ));
                for response in responses {
                    match response {
                        Scripted::Reply(bytes) => {
                            stream.write_all(&bytes).await.unwrap();
                            stream.flush().await.unwrap();
                        }
                        // Drop without responding: the client sees a reset.
                        Scripted::Drop => break,
                    }
                }
                stream.shutdown().await.unwrap();
            }
        });
        (base, seen, server)
    }

    fn mirror_line(uuid: &str) -> Value {
        json!({"uuid": uuid, "text": format!("line {uuid}")})
    }

    fn write_mirror(path: &Path, session: &str, lines: &[Value]) -> std::path::PathBuf {
        let dir = path.join("projects").join("lab");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join(format!("{session}.jsonl"));
        std::fs::write(&file, {
            let mut out = String::new();
            for line in lines {
                out.push_str(&line.to_string());
                out.push('\n');
            }
            out
        })
        .unwrap();
        file
    }

    fn tempdir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("zonepair-sync-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Run one sync against `base` with a per-test mirror dir, bounded so a
    /// stuck retry loop FAILS the test (with the recorded request list)
    /// instead of hanging it.
    async fn run_sync(
        base: &Base,
        mirror: &std::path::Path,
        seen: &Arc<std::sync::Mutex<Vec<String>>>,
    ) -> (usize, usize) {
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            sync_mirror(base, None, mirror),
        )
        .await
        {
            Ok(result) => result.unwrap(),
            Err(_) => panic!("sync timed out; requests seen: {:?}", seen.lock().unwrap()),
        }
    }

    /// The server task must finish its whole script; a timeout means the
    /// client's request sequence diverged from the script — fail loudly with
    /// the recorded list rather than hang.
    async fn finish_server(
        server: tokio::task::JoinHandle<()>,
        seen: &Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        if tokio::time::timeout(std::time::Duration::from_secs(2), server)
            .await
            .is_err()
        {
            panic!(
                "server did not finish its script; requests seen: {:?}",
                seen.lock().unwrap()
            );
        }
    }

    #[tokio::test]
    async fn sync_skips_clean_sessions_without_put() {
        let session = "clean-session";
        let lines = [mirror_line("uuid-1"), mirror_line("uuid-2")];
        let mirror = tempdir("clean");
        write_mirror(&mirror, session, &lines);
        // State already records the push, and the remote tail agrees.
        std::fs::write(
            sync_state_path(&mirror),
            format!(
                r#"{{"version":1,"sessions":{{"{session}":{{"lastUuid":"uuid-2","count":2}}}}}}"#
            ),
        )
        .unwrap();
        let script = vec![
            vec![Scripted::Reply(http_json(
                200,
                "{\"ok\":true,\"exists\":true}",
            ))],
            vec![Scripted::Reply(http_json(
                200,
                "{\"ok\":true,\"messages\":[{\"uuid\":\"uuid-1\"},{\"uuid\":\"uuid-2\"}]}",
            ))],
            // Pull phase: nothing remote to pull back.
            vec![Scripted::Reply(http_json(
                200,
                "{\"ok\":true,\"sessions\":[]}",
            ))],
        ];
        let (base, seen, server) = serve_script(script).await;
        let (pushed, _) = run_sync(&base, &mirror, &seen).await;
        assert_eq!(pushed, 0);
        let log = seen.lock().unwrap().join("\n");
        assert_eq!(
            log,
            format!(
                "GET /v1/store/sessions/{session} body=no\nGET /v1/store/sessions/{session}/messages body=no\nGET /v1/store/sessions body=no"
            )
        );
        assert!(!log.contains("PUT"), "clean session must not be PUT: {log}");
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    #[tokio::test]
    async fn sync_retries_transient_failure_then_completes() {
        let session = "flaky-session";
        let lines = [mirror_line("uuid-1"), mirror_line("uuid-2")];
        let mirror = tempdir("flaky");
        write_mirror(&mirror, session, &lines);
        let script = vec![
            // No state file: first-time sync probes existence.
            vec![Scripted::Reply(http_json(
                404,
                "{\"ok\":true,\"exists\":false}",
            ))],
            // First PUT connection: drop without responding (reset).
            vec![Scripted::Drop],
            // Retry: answer success.
            vec![Scripted::Reply(http_json(
                200,
                "{\"ok\":true,\"added\":2,\"total\":2}",
            ))],
            // Pull phase: nothing remote to pull back.
            vec![Scripted::Reply(http_json(
                200,
                "{\"ok\":true,\"sessions\":[]}",
            ))],
        ];
        let (base, seen, server) = serve_script(script).await;
        let (pushed, _) = run_sync(&base, &mirror, &seen).await;
        assert_eq!(pushed, 2);
        let log = seen.lock().unwrap().join("\n");
        assert_eq!(
            log.matches(&format!(
                "PUT /v1/store/sessions/{session}/messages body=yes"
            ))
            .count(),
            2,
            "expected exactly two PUT attempts after the dropped connection: {log}"
        );
        // State file records the pushed uuid.
        let state: Value =
            serde_json::from_str(&std::fs::read_to_string(sync_state_path(&mirror)).unwrap())
                .unwrap();
        assert_eq!(state["sessions"][session]["lastUuid"], json!("uuid-2"));
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    #[tokio::test]
    async fn sync_first_run_pushes_all() {
        let session = "fresh-session";
        let lines = [mirror_line("uuid-1"), mirror_line("uuid-2")];
        let mirror = tempdir("fresh");
        write_mirror(&mirror, session, &lines);
        assert!(
            !sync_state_path(&mirror).exists(),
            "precondition: no state file"
        );
        let script = vec![
            vec![Scripted::Reply(http_json(
                404,
                "{\"ok\":true,\"exists\":false}",
            ))],
            vec![Scripted::Reply(http_json(
                200,
                "{\"ok\":true,\"added\":2,\"total\":2}",
            ))],
            // Pull phase: nothing remote to pull back.
            vec![Scripted::Reply(http_json(
                200,
                "{\"ok\":true,\"sessions\":[]}",
            ))],
        ];
        let (base, seen, server) = serve_script(script).await;
        let (pushed, _) = run_sync(&base, &mirror, &seen).await;
        assert_eq!(pushed, 2);
        let log = seen.lock().unwrap().join("\n");
        assert_eq!(
            log,
            format!(
                "GET /v1/store/sessions/{session} body=no\nPUT /v1/store/sessions/{session}/messages body=yes\nGET /v1/store/sessions body=no"
            )
        );
        let state: Value =
            serde_json::from_str(&std::fs::read_to_string(sync_state_path(&mirror)).unwrap())
                .unwrap();
        assert_eq!(state["version"], json!(1));
        assert_eq!(state["sessions"][session]["lastUuid"], json!("uuid-2"));
        assert_eq!(state["sessions"][session]["count"], json!(2));
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    #[test]
    fn parse_base_accepts_all_carrier_schemes() {
        for raw in [
            "ws://b:7789",
            "http://b:7789",
            "wss://stub.example.com",
            "https://stub.example.com/zonepair",
        ] {
            parse_base(raw).unwrap_or_else(|e| panic!("{raw}: {e:#}"));
        }
    }

    #[test]
    fn parse_base_rejects_unknown_and_schemeless() {
        assert!(parse_base("ftp://b:7789").is_err());
        assert!(parse_base("b:7789").is_err());
        assert!(parse_base("https://").is_err());
    }

    #[test]
    fn stdio_ws_matches_base_scheme() {
        assert_eq!(
            parse_base("http://b:7789").unwrap().stdio_ws(),
            "ws://b:7789/v1/stdio"
        );
        assert_eq!(
            parse_base("wss://stub.example.com").unwrap().stdio_ws(),
            "wss://stub.example.com/v1/stdio"
        );
        assert_eq!(
            parse_base("https://cf.example.com/zonepair")
                .unwrap()
                .stdio_ws(),
            "wss://cf.example.com/zonepair/v1/stdio"
        );
    }
}
