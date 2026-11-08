//! Client-side speaker for the `ws-line-v1` carrier: WS for stdio, plain HTTP
//! for the session store. This is the only way a client talks to the server;
//! there is no raw-TCP path.
//!
//! TLS is terminated by the gateway in front (ADR-0005), so this module dials
//! plain `ws://` / `http://`. `wss://` / `https://` parse but are rejected
//! until the client speaks TLS.

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::path::Path;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

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
        "ws" | "http" => {}
        "wss" | "https" => bail!(
            "{scheme}:// needs a TLS client, which the stub does not speak yet; \
             dial ws:// or http:// behind your gateway"
        ),
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

    /// WS endpoint carrying stdio-v1.
    pub fn stdio_ws(&self) -> String {
        self.url("ws", "/v1/stdio")
    }

    fn authority(&self) -> String {
        let rest = self.0.split_once("://").expect("parsed base").1;
        rest.split('/').next().unwrap_or_default().to_string()
    }
}

// ---- stdio ------------------------------------------------------------------

pub struct Stdio {
    pub tx: WsTx,
    pub rx: WsRx,
}

/// Dial `/v1/stdio`, send the hello header and consume the ack.
pub async fn open_stdio(base: &Base, header: &Value, token: Option<&str>) -> Result<Stdio> {
    let mut request = base.stdio_ws().into_client_request().context("bad ws url")?;
    if let Some(t) = token {
        let value = HeaderValue::from_str(&format!("Bearer {t}")).context("bad token header")?;
        request.headers_mut().insert(AUTHORIZATION, value);
    }
    let (ws, _) = tokio_tungstenite::connect_async(request)
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

async fn request(
    base: &Base,
    method: &str,
    path: &str,
    body: Option<&Value>,
    token: Option<&str>,
) -> Result<(u16, Value)> {
    let authority = base.authority();
    let prefix = {
        let rest = base.as_str().split_once("://").expect("parsed base").1;
        let after = &rest[authority.len()..];
        after.trim_end_matches('/').to_string()
    };
    let full = format!("{prefix}{path}");

    let stream = TcpStream::connect(&authority)
        .await
        .with_context(|| format!("dial {authority}"))?;
    let (mut rx, mut tx) = stream.into_split();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let body_text = body.map(|b| b.to_string());
    let mut req = format!(
        "{method} {full} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nAccept: application/json\r\n"
    );
    if let Some(t) = token {
        req.push_str(&format!("Authorization: Bearer {t}\r\n"));
    }
    if let Some(b) = &body_text {
        req.push_str(&format!("Content-Type: application/json\r\nContent-Length: {}\r\n", b.len()));
    }
    req.push_str("\r\n");
    tx.write_all(req.as_bytes()).await?;
    if let Some(b) = &body_text {
        tx.write_all(b.as_bytes()).await?;
    }
    tx.flush().await?;

    let mut raw = Vec::new();
    rx.read_to_end(&mut raw).await?;
    let text = String::from_utf8_lossy(&raw);
    let status = text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .context("no HTTP status in response")?;
    let body_start = text.find("\r\n\r\n").map(|i| i + 4).unwrap_or(text.len());
    let body = &text[body_start..];
    let value = if body.trim().is_empty() {
        json!(null)
    } else {
        serde_json::from_str(body).with_context(|| format!("non-JSON body: {}", &body[..body.len().min(200)]))?
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
    let (status, v) = request(base, "GET", "/v1/store/sessions", None, token).await?;
    ensure_ok(status, &v)?;
    Ok(v["sessions"].as_array().cloned().unwrap_or_default().iter().filter_map(|s| s.as_str().map(str::to_string)).collect())
}

pub async fn store_exists(base: &Base, token: Option<&str>, session: &str) -> Result<bool> {
    let path = format!("/v1/store/sessions/{session}");
    let (status, v) = request(base, "GET", &path, None, token).await?;
    if status == 404 {
        return Ok(false);
    }
    ensure_ok(status, &v)?;
    Ok(v.get("exists").and_then(Value::as_bool).unwrap_or(false))
}

pub async fn store_messages(base: &Base, token: Option<&str>, session: &str) -> Result<Vec<Value>> {
    let path = format!("/v1/store/sessions/{session}/messages");
    let (status, v) = request(base, "GET", &path, None, token).await?;
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
    let (status, v) = request(base, "POST", &path, Some(&json!({"upTo": up_to})), token).await?;
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
    let (status, v) = request(base, "DELETE", &path, None, token).await?;
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
    let (status, v) = request(base, "PUT", &path, Some(&json!({"messages": messages})), token).await?;
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
