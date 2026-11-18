//! Session store over HTTP: the six endpoint calls `sess` and the mirror
//! sync drive (`store_list`/`exists`/`messages`/`fork`/`delete`/`merge`),
//! plus the one local file reader `sess merge` still needs.

use crate::transport::{ensure_ok, request_with_retry, Base, HTTP_CLIENT};
use anyhow::{Context, Result};
use reqwest::Method;
use serde_json::{json, Value};
use std::path::Path;

/// Parse a JSONL mirror/store file into line values — the `sess merge`
/// manual path (the only remaining reader; sync never parses content).
pub fn read_jsonl(path: &Path) -> Result<Vec<Value>> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).context("mirror line is not JSON"))
        .collect()
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
