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
    let mut request = base.stdio_ws().into_client_request().context("bad ws url")?;
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
            .send(Message::Text(json!({"proto": "stdin-eof"}).to_string().into()))
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
    let mut request = client.request(method, url).header("Accept", "application/json");
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
    let (status, v) = request(&HTTP_CLIENT, base, Method::GET, "/v1/store/sessions", None, token).await?;
    ensure_ok(status, &v)?;
    Ok(v["sessions"].as_array().cloned().unwrap_or_default().iter().filter_map(|s| s.as_str().map(str::to_string)).collect())
}

pub async fn store_exists(base: &Base, token: Option<&str>, session: &str) -> Result<bool> {
    let path = format!("/v1/store/sessions/{session}");
    let (status, v) = request(&HTTP_CLIENT, base, Method::GET, &path, None, token).await?;
    if status == 404 {
        return Ok(false);
    }
    ensure_ok(status, &v)?;
    Ok(v.get("exists").and_then(Value::as_bool).unwrap_or(false))
}

pub async fn store_messages(base: &Base, token: Option<&str>, session: &str) -> Result<Vec<Value>> {
    let path = format!("/v1/store/sessions/{session}/messages");
    let (status, v) = request(&HTTP_CLIENT, base, Method::GET, &path, None, token).await?;
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
    let (status, v) = request(&HTTP_CLIENT, base, Method::POST, &path, Some(&json!({"upTo": up_to})), token).await?;
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
    let (status, v) = request(&HTTP_CLIENT, base, Method::DELETE, &path, None, token).await?;
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
    let (status, v) = request(&HTTP_CLIENT, base, Method::PUT, &path, Some(&json!({"messages": messages})), token).await?;
    ensure_ok(status, &v)?;
    Ok((
        v["added"].as_u64().unwrap_or(0),
        v["total"].as_u64().unwrap_or(0),
    ))
}

// ---- store mirror (ADR-0004) --------------------------------------------------

fn read_jsonl(path: &Path) -> Result<Vec<Value>> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
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
            if path.extension().is_none_or(|e| e != "jsonl") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else { continue };
            let lines = read_jsonl(&path)?;
            let (added, _) = store_merge(base, token, stem, &lines)
                .await
                .with_context(|| format!("pushing {}", path.display()))?;
            pushed += added as usize;
        }
    }

    let mut pulled = 0usize;
    for session in store_list(base, token).await? {
        let remote = store_messages(base, token, &session).await?;
        let dest = mirror.join("projects").join("lab");
        std::fs::create_dir_all(&dest)?;
        let path = dest.join(format!("{session}.jsonl"));
        let mut line_values = if path.exists() { read_jsonl(&path)? } else { Vec::new() };
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
        let sessions = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store_list(&base, None),
        ).await.unwrap().unwrap();
        assert_eq!(sessions, ["gateway"]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn store_list_rejects_truncated_body() {
        let (base, server) = serve_http(
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n\
              {\"ok\":true,\"sessions\":[\"incomplete\"]}",
        ).await;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store_list(&base, None),
        ).await.unwrap();
        assert!(result.is_err(), "accepted a body shorter than Content-Length");
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
            "https://localhost:{}", listener.local_addr().unwrap().port(),
        )).unwrap();
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
            request(&client, &base, Method::GET, "/v1/store/sessions", None, None),
        ).await.unwrap().unwrap();
        assert_eq!(status, 200);
        assert_eq!(value, json!({"ok": true, "sessions": ["gateway"]}));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tls_truncated_body_is_not_an_empty_success() {
        let (base, client, server) = serve_tls(
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n",
        ).await;
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            request(&client, &base, Method::GET, "/v1/store/sessions", None, None),
        ).await.unwrap().expect_err("accepted a truncated TLS response as empty data");
        let cause = error.downcast_ref::<reqwest::Error>().expect("HTTP body error");
        assert!(cause.is_body() || cause.is_decode(), "{error:#}");
        server.await.unwrap();
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
            parse_base("https://cf.example.com/zonepair").unwrap().stdio_ws(),
            "wss://cf.example.com/zonepair/v1/stdio"
        );
    }
}
