//! store (plain HTTP): the `/v1/store/*` session-projection endpoints.

use super::AppState;
use axum::{
    extract::{Path as AxPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};

// ---- store (plain HTTP) -----------------------------------------------------

pub(crate) async fn store_exists(
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

pub(crate) async fn store_messages(
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
pub(crate) struct ForkBody {
    #[serde(rename = "upTo")]
    up_to: String,
}

pub(crate) async fn store_fork(
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

pub(crate) async fn store_delete(
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

pub(crate) async fn store_list(State(state): State<AppState>) -> Response {
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
pub(crate) struct MergeBody {
    messages: Vec<Value>,
}

pub(crate) async fn store_merge(
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
