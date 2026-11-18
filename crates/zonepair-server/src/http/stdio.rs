//! stdio stream: one claude child per `/v1/stdio` WS connection.

use super::AppState;
use crate::claudeproc;
use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, State},
    response::Response,
};
use futures_util::SinkExt;
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use zonepair_proto::framing::{self, StdioHeader};

// ---- stdio stream -----------------------------------------------------------

pub(crate) async fn stdio_ws(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| stdio_socket(socket, state))
}

async fn stdio_socket(mut ws: WebSocket, state: AppState) {
    let Some(Ok(Message::Text(hello_line))) = ws.recv().await else {
        return;
    };
    let header: StdioHeader = match serde_json::from_str(&hello_line) {
        Ok(header) => header,
        Err(e) => {
            let _ = ws
                .send(Message::Text(
                    json!({"ok": false, "error": format!("bad hello: {e}")}).to_string().into(),
                ))
                .await;
            return;
        }
    };
    let conn = state.seq.fetch_add(1, Ordering::Relaxed);
    let mut child = match claudeproc::spawn(
        state.store.root(),
        &state.claude_cmd,
        &state.log_dir,
        &header,
        &conn.to_string(),
    ) {
        Ok(child) => child,
        Err(e) => {
            let _ = ws
                .send(Message::Text(json!({"ok": false, "error": e.to_string()}).to_string().into()))
                .await;
            return;
        }
    };
    let _ = ws.send(Message::Text(json!({"ok": true}).to_string().into())).await;

    // Option + explicit drop is load-bearing: tokio's ChildStdin::shutdown()
    // only flushes, it does NOT close the pipe. claude (stream-input mode)
    // exits on stdin EOF, so eof must come from dropping the write end.
    let mut child_stdin = Some(child.stdin.take().expect("child stdin"));
    let child_stdout = child.stdout.take().expect("child stdout");
    let mut lines = BufReader::new(child_stdout);
    let (line_tx, mut line_rx) = mpsc::channel::<String>(128);
    let reader = tokio::spawn(async move {
        let mut buf = String::new();
        loop {
            buf.clear();
            match lines.read_line(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if buf.len() > framing::MAX_LINE {
                        break;
                    }
                    let line = buf.trim_end_matches('\n').to_string();
                    if line_tx.send(line).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    loop {
        tokio::select! {
            out = line_rx.recv() => match out {
                Some(line) => {
                    if ws.send(Message::Text(line.into())).await.is_err() {
                        break;
                    }
                }
                None => break,
            },
            incoming = ws.recv() => match incoming {
                Some(Ok(Message::Text(frame))) => {
                    if is_control_frame(&frame) {
                        if frame.contains("stdin-eof") {
                            child_stdin = None; // drop => real EOF for claude
                        }
                        continue;
                    }
                    if let Some(stdin) = child_stdin.as_mut() {
                        if stdin
                            .write_all(format!("{frame}\n").as_bytes())
                            .await
                            .is_err()
                        {
                            break;
                        }
                        let _ = stdin.flush().await;
                    }
                }
                Some(Ok(Message::Binary(_))) => {
                    let _ = ws
                        .send(Message::Text(
                            json!({"proto": "error", "error": "binary frames violate ws-line-v1"})
                                .to_string()
                                .into(),
                        ))
                        .await;
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            },
        }
    }

    drop(child_stdin); // deliver EOF even when the frame never arrived
    let _ = reader.await;
    let _ = child.wait().await;
    let _ = ws.close().await;
    eprintln!("ws-stdio conn #{conn} child exit {:?}", child.id());
}

// Transport-level control frames live in a `{"proto": ...}` namespace that the
// carried protocols (claude stream-json, MCP JSON-RPC) never use (their lines
// start with {"type" / {"jsonrpc").
fn is_control_frame(frame: &str) -> bool {
    serde_json::from_str::<Value>(frame)
        .map(|v| v.get("proto").is_some())
        .unwrap_or(false)
}
