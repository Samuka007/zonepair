//! ws-line-v1 listener: HTTP/WS on one port. Frames on the WS streams are
//! exactly one protocol line each (text frames only; binary is a protocol
//! violation).

use crate::claudeproc;
use crate::proto::{self, StdioHeader};
use crate::store::Store;
use anyhow::{Context, Result};
use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Path as AxPath, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use futures_util::SinkExt;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub claude_cmd: Vec<String>,
    pub log_dir: PathBuf,
    pub token: Option<String>,
    pub seq: Arc<AtomicU64>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/stdio", get(stdio_ws))
        .route(
            "/v1/store/sessions/{id}",
            get(store_exists).delete(store_delete),
        )
        .route("/v1/store/sessions/{id}/messages", get(store_messages))
        .route("/v1/store/sessions/{id}/messages", put(store_merge))
        .route("/v1/store/sessions", get(store_list))
        .route("/v1/store/sessions/{id}/fork", post(store_fork))
        .with_state(state.clone())
        .layer(axum::middleware::from_fn_with_state(
            state,
            auth_middleware,
        ))
}

async fn auth_middleware(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if let Some(token) = &state.token {
        let expected = format!("Bearer {token}");
        let ok = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v == expected);
        if !ok {
            return (StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"})))
                .into_response();
        }
    }
    next.run(request).await
}

pub async fn serve(state: AppState, addr: String) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind http/ws listener {addr}"))?;
    eprintln!("stub-server http/ws listening on {addr}");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

// ---- stdio stream -----------------------------------------------------------

async fn stdio_ws(
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
                    if buf.len() > proto::MAX_LINE {
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

// ---- store (plain HTTP) -----------------------------------------------------

async fn store_exists(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Response {
    if state.store.exists(&id).await {
        Json(json!({"exists": true})).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"exists": false, "error": "not found"})),
        )
            .into_response()
    }
}

async fn store_messages(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Response {
    match state.store.messages(&id).await {
        Ok(messages) => Json(json!({"ok": true, "messages": messages})).into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({"ok": false, "error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(serde::Deserialize)]
struct ForkBody {
    #[serde(rename = "upTo")]
    up_to: String,
}

async fn store_fork(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Json(body): Json<ForkBody>,
) -> Response {
    match state.store.fork(&id, &body.up_to).await {
        Ok(out) => Json(json!({
            "ok": true,
            "sessionId": out.session_id,
            "pairs": out.pairs,
            "messages": out.messages,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn store_delete(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Response {
    let deleted = match state.store.delete(&id).await {
        Ok(deleted) => deleted,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"ok": false, "error": e.to_string()})),
            )
                .into_response()
        }
    };
    Json(json!({"ok": true, "deleted": deleted})).into_response()
}

async fn store_list(State(state): State<AppState>) -> Response {
    match state.store.list().await {
        Ok(sessions) => Json(json!({"ok": true, "sessions": sessions})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"ok": false, "error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(serde::Deserialize)]
struct MergeBody {
    messages: Vec<Value>,
}

async fn store_merge(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Json(body): Json<MergeBody>,
) -> Response {
    match state.store.merge(&id, &body.messages).await {
        Ok(out) => Json(json!({"ok": true, "added": out.added, "total": out.total}))
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": e.to_string()})),
        )
            .into_response(),
    }
}
