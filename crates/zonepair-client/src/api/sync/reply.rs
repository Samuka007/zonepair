//! Reply classification for `changes`/`publish` (RFC-0001-sync-protocol §6.2/§7.2/§8) —
//! the response half of the wire layer, split from `sync.rs` so that file
//! reads as the two entry points alone.
//!
//! Status picks the lane: 2xx splits on `Content-Type`, 404 on the body's
//! `code`, everything else is `StatusError` so 5xx still retries through
//! `with_retry` (the caller runs this inside the retry closure — a reply
//! that fails its shape checks is a `TransportFault`, which only retries
//! when produced there).

use crate::transport::{StatusError, TransportFault};
use anyhow::{Context, Result};
use reqwest::header::HeaderMap;
use serde_json::Value;
use zonepair_proto::cursor;
use zonepair_proto::syncwire::{
    ChangesBody, ChangesState, PublishResponse, X_ZP_NEXT_CURSOR, X_ZP_STATE,
};

use super::{ChangesReply, PublishReply};

/// Classify one `changes` response (§6.2, §8): status picks the lane —
/// 2xx splits on `Content-Type`, 404 on the body's `code`, everything else
/// is `StatusError` so 5xx inside it still retries through `with_retry`.
pub(super) fn changes_reply(
    status: u16,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<ChangesReply> {
    match status {
        200..=299 => match content_type(headers).as_str() {
            "application/octet-stream" => octet_reply(headers, body),
            "application/json" => json_reply(body),
            other => Err(TransportFault::new(format!(
                "changes reply Content-Type {other:?} is neither json nor octet-stream"
            ))
            .into()),
        },
        404 => {
            // Positive match only (§8): anything but a parseable body with
            // code=="missing" means the server predates this protocol.
            let code = serde_json::from_slice::<Value>(body)
                .ok()
                .and_then(|v| v.get("code").and_then(Value::as_str).map(str::to_owned));
            match code.as_deref() {
                Some("missing") => Ok(ChangesReply::Missing),
                _ => Ok(ChangesReply::Unsupported),
            }
        }
        other => Err(StatusError::new(other, error_summary(body)).into()),
    }
}

fn content_type(headers: &HeaderMap) -> String {
    headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

/// One line of context for a failed status: the body's `error` field when it
/// parses as JSON, else the body verbatim — an empty or non-JSON body still
/// leaves the status in the message, which is all a human needs.
fn error_summary(body: &[u8]) -> String {
    match serde_json::from_slice::<Value>(body) {
        Ok(v) => v
            .get("error")
            .map(|e| e.to_string())
            .unwrap_or_else(|| v.to_string()),
        Err(_) => {
            let text = String::from_utf8_lossy(body);
            text.chars().take(200).collect()
        }
    }
}

/// `Content-Type: application/octet-stream` (§6.2): the verdict and the next
/// cursor ride in `X-ZP-*` headers, the body is the raw byte segment.
/// Missing or unparseable headers are `TransportFault` — the reply never
/// became meaningful data, the same failure class as a short read (§6.4).
fn octet_reply(headers: &HeaderMap, body: &[u8]) -> Result<ChangesReply> {
    let header = |name: &str| -> Result<&str> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                anyhow::Error::new(TransportFault::new(format!(
                    "changes reply is missing or has a bad {name} header"
                )))
            })
    };
    let state = header(X_ZP_STATE)?;
    let next_cursor = check_cursor(header(X_ZP_NEXT_CURSOR)?, X_ZP_NEXT_CURSOR)?;
    match state.parse::<ChangesState>() {
        Ok(ChangesState::Append) => Ok(ChangesReply::Append {
            body: body.to_vec(),
            next_cursor,
        }),
        Ok(ChangesState::Reset) => Ok(ChangesReply::Reset {
            body: body.to_vec(),
            next_cursor,
        }),
        // A body-free verdict must arrive as JSON; octet-stream with
        // `unchanged` — or any value outside the three — violates §6.2.
        _ => Err(
            TransportFault::new(format!("{X_ZP_STATE} header {state:?} cannot carry bytes")).into(),
        ),
    }
}

/// `Content-Type: application/json` (§6.2): only `unchanged` is a legal
/// verdict without bytes. `append`/`reset` in JSON means the peer broke the
/// reply contract — same transport-error class as a missing header.
fn json_reply(body: &[u8]) -> Result<ChangesReply> {
    let parsed: ChangesBody =
        serde_json::from_slice(body).context("changes JSON reply did not parse")?;
    match parsed.state {
        ChangesState::Unchanged => Ok(ChangesReply::Unchanged {
            next_cursor: check_cursor(&parsed.next_cursor, "nextCursor")?,
        }),
        other => Err(TransportFault::new(format!(
            "JSON changes reply claims {other} but carries no bytes"
        ))
        .into()),
    }
}

/// A server-minted cursor is a credential the caller persists verbatim —
/// accepting an unparseable one would defer a server contract violation
/// into a later "local cursor corrupt" misdiagnosis (§10-7 corruption
/// never masquerades in reverse). Shape-check on receipt: anything but a
/// valid `zpc1` string is
/// a `TransportFault` — the reply never became meaningful data.
fn check_cursor(raw: &str, where_: &str) -> Result<String> {
    match cursor::parse(raw) {
        Ok(_) => Ok(raw.to_string()),
        Err(e) => Err(TransportFault::new(format!(
            "sync reply carries an unparseable cursor in {where_}: {raw:?} ({e})"
        ))
        .into()),
    }
}

/// Classify one `publish` response (§7.2): 200 must carry the
/// `PublishResponse` shape AND a parseable confirmation cursor; 409 only
/// counts as the named `conflict` verdict; everything else is `StatusError`
/// so 5xx still retries inside `with_retry`.
pub(super) fn publish_reply(status: u16, body: &[u8]) -> Result<PublishReply> {
    match status {
        200..=299 => {
            let parsed: PublishResponse = serde_json::from_slice(body)
                .map_err(|e| {
                    anyhow::Error::new(TransportFault::new(format!(
                        "publish reply did not parse: {e}"
                    )))
                })?;
            Ok(PublishReply::Published {
                already: parsed.already,
                cursor: check_cursor(&parsed.cursor, "cursor")?,
            })
        }
        // §7.2 names exactly one conflict body; a 409 saying anything else
        // is a generic failure, not a divergence verdict.
        409 if serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_owned))
            .as_deref()
            == Some("conflict") =>
        {
            Ok(PublishReply::Conflict)
        }
        other => Err(StatusError::new(other, error_summary(body)).into()),
    }
}
