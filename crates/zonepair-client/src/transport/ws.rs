//! stdio over WS: dial `/v1/stdio`, carry protocol lines one per text frame.

use super::http::tls_config;
use super::Base;
use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::Connector;

type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;
pub type WsTx = futures_util::stream::SplitSink<WsStream, Message>;
pub type WsRx = futures_util::stream::SplitStream<WsStream>;

// ---- stdio ------------------------------------------------------------------

pub struct Stdio {
    pub tx: WsTx,
    pub rx: WsRx,
}

/// Shared rustls client config (webpki-roots: no system trust store needed,
/// which is what keeps the musl static build self-contained).
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
