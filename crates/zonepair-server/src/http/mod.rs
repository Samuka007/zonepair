//! ws-line-v1 listener: HTTP/WS on one port. Frames on the WS streams are
//! exactly one protocol line each (text frames only; binary is a protocol
//! violation).

mod stdio;
mod store;
mod sync;

use crate::store::Store;
use anyhow::{Context, Result};
use axum::{
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// Body cap for the file-scale write routes (publish + merge). axum's
/// `DefaultBodyLimit` defaults to 2 MiB, which base64 inflation turns into
/// a ~1.5 MiB ceiling on real session bytes — a fork of any accumulated
/// session would 413 forever outside the protocol's error surface. 64 MiB
/// is the defensible bound: a session file is unbounded user data (RFC-0004-b-endpoints
/// budgets 300 MB files honestly), yet publish still buffers the whole body
/// via `Bytes`, so a cap exists to bound memory per request. At 64 MiB wire
/// the decoded content can still approach ~48 MiB — beyond that the §7.1
/// fallback note applies (bare-body upload, if the need ever materializes).
const MAX_STORE_WRITE_BODY: usize = 64 * 1024 * 1024;

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
        .route("/v1/stdio", get(stdio::stdio_ws))
        .route(
            "/v1/store/sessions/{id}",
            get(store::store_exists).delete(store::store_delete),
        )
        .route("/v1/store/sessions/{id}/messages", get(store::store_messages))
        .route(
            "/v1/store/sessions/{id}/messages",
            put(store::store_merge).route_layer(DefaultBodyLimit::max(MAX_STORE_WRITE_BODY)),
        )
        .route("/v1/store/sessions", get(store::store_list))
        .route("/v1/store/sessions/{id}/fork", post(store::store_fork))
        .route(
            "/v1/store/sessions/{id}/changes",
            get(sync::store_changes),
        )
        .route(
            "/v1/store/sessions/{id}/publish",
            post(sync::store_publish).route_layer(DefaultBodyLimit::max(MAX_STORE_WRITE_BODY)),
        )
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
