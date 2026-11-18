//! sync (RFC-0001-sync-protocol §6–§8, RFC-0004-b-endpoints §3–§9): the two protocol routes.
//!
//! `GET …/changes?cursor=` runs the §6.3 decision table on one pinned fd
//! snapshot: locate/fstat/stable_boundary/fingerprint all happen against the
//! same open file, and every response is rechecked against the request-open
//! `(size, mtime_ns)` pair before it ships — the only defence against a
//! same-inode in-place writer (RFC-0004-b-endpoints §4.1). `POST …/publish` is the
//! protocol's only A→B write: declare the full fork bytes or detect a
//! conflict, never merge.

use super::AppState;
use crate::store::{ReadSpan, Store};
use anyhow::{bail, Result};
use axum::{
    body::Bytes,
    extract::{Path as AxPath, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use zonepair_proto::cursor::{self, Cursor};
use zonepair_proto::syncwire::{
    ChangesBody, ChangesState, ErrorBody, PublishRequest, PublishResponse,
    X_ZP_NEXT_CURSOR, X_ZP_STATE,
};

/// Bounded retry for the fstat-recheck / short-read retry loop (RFC-0004-b-endpoints
/// §4.1): an in-place rewrite landing mid-request voids the whole round.
/// Three attempts is the anti-livelock cap — a file that never sits still
/// surfaces as a retriable 500 rather than an infinite spin.
const MAX_SNAPSHOT_ATTEMPTS: usize = 3;

/// One pass over the decision table returns a finished response or asks the
/// caller to start over from `locate` on a fresh fd.
enum Round {
    Done(Response),
    Retry,
}

// ---- GET /v1/store/sessions/{id}/changes -------------------------------------

pub(crate) async fn store_changes(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let cursor_param = query.get("cursor").map(String::as_str);
    let store = &state.store;
    for _attempt in 0..MAX_SNAPSHOT_ATTEMPTS {
        match changes_round(store, &id, cursor_param) {
            Ok(Round::Done(resp)) => return resp,
            Ok(Round::Retry) => continue,
            Err(e) => return internal(e),
        }
    }
    internal(anyhow::anyhow!(
        "session {id} file kept shifting under the request"
    ))
}

/// One full interpretation pass (RFC-0004-b-endpoints §4.1): locate → fstat → boundary →
/// decision table → assemble → before-response fstat recheck. Everything is
/// pinned on the fd `locate` returned; `Round::Retry` is the "snapshot
/// drifted, discard everything" signal.
fn changes_round(store: &Store, id: &str, cursor_param: Option<&str>) -> Result<Round> {
    // Session ids that would escape the root are caller error, not "missing"
    // (rejected before any scan, same as validate_sid inside locate).
    if invalid_sid(id) {
        return Ok(Round::Done(bad_request("invalid session id")));
    }
    // Existence is checked before the WHOLE decision table (RFC-0001-sync-protocol §6.3
    // note): a missing session answers 404 even when the cursor is corrupt.
    let Some(file) = store.locate(id)? else {
        return Ok(Round::Done(missing(id)));
    };
    let st = Store::fstat(&file)?;
    // A verified fingerprint slot means the file bytes did not move, so its
    // remembered boundary may be reused without rescanning (§5 cost clause ①).
    let boundary = match store.cached_cursor(id, &st) {
        Some((offset, _)) => offset,
        None => Store::stable_boundary(&file, st.size)?,
    };

    // §6.3 #1: absent/empty/zero cursor is the first pull — full stable prefix.
    if cursor::is_first_pull(cursor_param) {
        return respond_span(store, id, &file, &st, boundary, 0, ChangesState::Reset);
    }
    // §6.3 #2: corrupt or unknown-version cursor is caller corruption → 400,
    // never a silent reset.
    let raw = cursor_param.unwrap_or_default();
    let p = match cursor::parse(raw) {
        Ok(p) => p,
        Err(_) => return Ok(Round::Done(bad_cursor())),
    };
    // §6.3 #3: the cursor's length promise failed (prefix truncated/rebuilt).
    if p.offset > boundary {
        return respond_span(store, id, &file, &st, boundary, 0, ChangesState::Reset);
    }
    // §6.3 #4: the cursor's content promise failed — verify BEFORE the
    // unchanged/append split; unchanged does not skip fingerprinting
    // (equal-length rewrite).
    let actual = store.prefix_hash(id, &file, &st, p.offset)?;
    if actual != p.prefix_hash {
        return respond_span(store, id, &file, &st, boundary, 0, ChangesState::Reset);
    }
    if p.offset == boundary {
        // §6.3 #5: nothing new — JSON verdict + echo of the caller's cursor.
        if snapshot_drifted(&file, &st)? {
            return Ok(Round::Retry);
        }
        return Ok(Round::Done(
            Json(ChangesBody {
                ok: true,
                state: ChangesState::Unchanged,
                next_cursor: raw.to_string(),
            })
            .into_response(),
        ));
    }
    // §6.3 #6: new terminated bytes past the verified prefix.
    respond_span(store, id, &file, &st, boundary, p.offset, ChangesState::Append)
}

/// The `append`/`reset` shape (RFC-0001-sync-protocol §6.2): mint nextCursor over
/// `[0, boundary)`, read `[from, boundary)` from the pinned fd, then the
/// before-response `(size, mtime_ns)` recheck. `Short` reads and drifted
/// snapshots both discard the whole round.
fn respond_span(
    store: &Store,
    id: &str,
    file: &std::fs::File,
    st: &crate::store::StatSnap,
    boundary: u64,
    from: u64,
    state: ChangesState,
) -> Result<Round> {
    // seal_prefix is the cursor-generate path: unconditional recompute of
    // H(boundary) and the slot update that keeps the next unchanged poll O(1).
    let hash = store.seal_prefix(id, file, st, boundary)?;
    let body = match Store::read_range(file, from, boundary)? {
        ReadSpan::Full(bytes) => bytes,
        ReadSpan::Short { .. } => return Ok(Round::Retry),
    };
    if snapshot_drifted(file, st)? {
        return Ok(Round::Retry);
    }
    let next = Cursor::new(boundary, hash).encode();
    let mut resp = Response::new(body.into());
    *resp.status_mut() = StatusCode::OK;
    let headers = resp.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(X_ZP_STATE, HeaderValue::from_static(state.as_str()));
    headers.insert(
        X_ZP_NEXT_CURSOR,
        HeaderValue::from_str(&next).expect("cursor is ASCII"),
    );
    Ok(Round::Done(resp))
}

/// The §4.1 in-place-writer tripwire: the `(size, mtime_ns)` pair must still
/// equal the request-open values or everything assembled so far is void.
fn snapshot_drifted(file: &std::fs::File, st: &crate::store::StatSnap) -> Result<bool> {
    let now = Store::fstat(file)?;
    Ok((now.size, now.mtime_ns) != (st.size, st.mtime_ns))
}

// ---- POST /v1/store/sessions/{id}/publish ------------------------------------

pub(crate) async fn store_publish(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    body: Bytes,
) -> Response {
    match publish_round(&state.store, &id, &body) {
        Ok(resp) => resp,
        Err(e) => internal(e),
    }
}

fn publish_round(store: &Store, id: &str, body: &[u8]) -> Result<Response> {
    // Caller-error checks first — every 400 below returns before any write,
    // so a rejected publish never touches the disk.
    let req: PublishRequest = match serde_json::from_slice(body) {
        Ok(req) => req,
        Err(_) => return Ok(bad_request("malformed publish body")),
    };
    if req.session_id != id {
        return Ok(bad_request("sessionId does not match path"));
    }
    if invalid_sid(&req.session_id) {
        return Ok(bad_request("invalid session id"));
    }
    if !req.content_sha256_is_valid() {
        return Ok(bad_request("contentSha256 must be 64 lowercase hex chars"));
    }
    let content = match req.decode_content() {
        Ok(content) => content,
        Err(_) => return Ok(bad_request("content is not valid base64")),
    };
    let digest = format!("{:x}", Sha256::digest(&content));
    if digest != req.content_sha256 {
        return Ok(bad_request("contentSha256 does not match decoded content"));
    }
    // Line validation checks only, never rewrites (RFC-0001-sync-protocol §7.1): every
    // line must be `\n`-terminated and parse as JSON.
    if let Err(msg) = validate_jsonl(&content) {
        return Ok(bad_request(msg));
    }

    // "check existence → write/conflict" runs inside the global write lock
    // via `publish_create` (RFC-0001-sync-protocol §7.5 + RFC-0004-b-endpoints §9.3): a concurrent
    // merge/fork/delete on the same id is serialized by the lock, so no
    // rename can land between the check and the no-clobber create. An
    // out-of-lock external creator can still win create_exclusive's link
    // window — Ok(false) covers both "already there" and that case, and
    // both fall back to the same §9.2 sha comparison.
    if store.publish_create(&lab_dir(store), id, &content)? {
        return Ok(publish_ok(false, content.len() as u64, &req));
    }
    for _attempt in 0..MAX_SNAPSHOT_ATTEMPTS {
        match store.locate(id)? {
            Some(existing) => return existing_verdict(store, id, &existing, &req),
            // Existed at check time and vanished before the sha comparison
            // (concurrent delete): retry the whole existence phase.
            None => continue,
        }
    }
    bail!("session {id} existence kept shifting under publish")
}

/// §9.2's idempotence decision on an existing file: sha equal → already,
/// else terminal 409. The sha-equal path also warms the single fingerprint
/// slot so the first `changes` after publish hits the O(1) steady state.
fn existing_verdict(
    store: &Store,
    id: &str,
    file: &std::fs::File,
    req: &PublishRequest,
) -> Result<Response> {
    let bytes = read_stable(file)?;
    let have = format!("{:x}", Sha256::digest(&bytes));
    if have != req.content_sha256 {
        return Ok(conflict());
    }
    // The slot is keyed to THIS fstat tuple, so the sealed offset must come
    // from the same observation: sealing `st₂` at the earlier read's length
    // would freeze boundary at a stale value — an in-place append between
    // the two fstats would then be withheld as `unchanged` forever (the
    // tuple keeps verifying until the next write). Sealing at the live
    // stable boundary makes every append visible the same request.
    let st = Store::fstat(file)?;
    let boundary = Store::stable_boundary(file, st.size)?;
    store.seal_prefix(id, file, &st, boundary)?;
    Ok(publish_ok(true, bytes.len() as u64, req))
}

/// Read the pinned fd as one self-consistent snapshot (fstat → read →
/// recheck); in-place truncation mid-read retries a bounded number of times.
fn read_stable(file: &std::fs::File) -> Result<Vec<u8>> {
    for _attempt in 0..MAX_SNAPSHOT_ATTEMPTS {
        let st = Store::fstat(file)?;
        let read = if st.size == 0 {
            ReadSpan::Full(Vec::new())
        } else {
            Store::read_range(file, 0, st.size)?
        };
        let bytes = match read {
            ReadSpan::Full(bytes) => bytes,
            ReadSpan::Short { .. } => continue,
        };
        if !snapshot_drifted(file, &st)? {
            return Ok(bytes);
        }
    }
    bail!("file kept shifting under read")
}

/// §7.3 confirmation cursor: `zpc1.<len(content)>.<contentSha256[..32]>` —
/// the fingerprint segment is literally the request's own sha truncated, so
/// minting it costs nothing. Built through `Cursor::encode` — the proto
/// codec is the single construction site, so the publish cursor can never
/// drift from the `changes` encoding.
fn publish_ok(already: bool, len: u64, req: &PublishRequest) -> Response {
    Json(PublishResponse {
        ok: true,
        published: true,
        already,
        cursor: confirmation_cursor(len, &req.content_sha256),
    })
    .into_response()
}

/// `contentSha256` (64 lowercase hex, shape-validated upstream) → the
/// `[u8; 16]` prefix fingerprint `Cursor` wants, then the `zpc1` encoding.
fn confirmation_cursor(len: u64, sha256_hex: &str) -> String {
    let mut fp = [0u8; cursor::PREFIX_HASH_LEN];
    for (i, pair) in sha256_hex[..cursor::PREFIX_HASH_LEN * 2]
        .as_bytes()
        .chunks_exact(2)
        .enumerate()
    {
        // Shape-validated 64 lowercase hex upstream — never fails.
        fp[i] = u8::from_str_radix(std::str::from_utf8(pair).unwrap_or("00"), 16).unwrap_or(0);
    }
    Cursor::new(len, fp).encode()
}

/// `projects/lab/` — same landing directory merge uses for unknown sessions
/// (RFC-0004-b-endpoints §9.1's "follows existing convention").
fn lab_dir(store: &Store) -> PathBuf {
    store.root().join("projects").join("lab")
}

/// Every line `\n`-terminated and parseable as JSON; bytes are never
/// rewritten (RFC-0001-sync-protocol §7.1). Empty content is vacuously valid — zero lines.
fn validate_jsonl(content: &[u8]) -> Result<(), &'static str> {
    if content.is_empty() {
        return Ok(());
    }
    if !content.ends_with(b"\n") {
        return Err("every line must be newline-terminated");
    }
    let text = std::str::from_utf8(content).map_err(|_| "content is not UTF-8 JSONL")?;
    for line in text.lines() {
        if serde_json::from_str::<serde_json::Value>(line).is_err() {
            return Err("content line is not valid JSON");
        }
    }
    Ok(())
}

// ---- shared error shapes (RFC-0001-sync-protocol §8) ---------------------------------------

fn invalid_sid(id: &str) -> bool {
    id.is_empty() || id.contains('/') || id.contains("..") || id.contains('\0')
}

fn error_response(status: StatusCode, error: impl Into<String>, code: Option<&str>) -> Response {
    (
        status,
        Json(ErrorBody {
            ok: false,
            error: error.into(),
            code: code.map(str::to_string),
        }),
    )
        .into_response()
}

/// 404 `code:"missing"` — the positive signal that distinguishes "session
/// absent on B" from an old-server's bare 404 (RFC-0001-sync-protocol §8/§9).
fn missing(id: &str) -> Response {
    error_response(
        StatusCode::NOT_FOUND,
        format!("session {id} not found"),
        Some("missing"),
    )
}

/// §8: corrupt cursors answer the exact "bad cursor" wording — diverged
/// cursors get `reset`, corrupt ones get this, and the two never masquerade.
fn bad_cursor() -> Response {
    error_response(StatusCode::BAD_REQUEST, "bad cursor", None)
}

fn bad_request(error: impl Into<String>) -> Response {
    error_response(StatusCode::BAD_REQUEST, error, None)
}

/// 409 `{"ok":false,"error":"conflict"}` — terminal divergence: never merge,
/// never overwrite, never return a merged result.
fn conflict() -> Response {
    error_response(StatusCode::CONFLICT, "conflict", None)
}

fn internal(e: anyhow::Error) -> Response {
    error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string(), None)
}
