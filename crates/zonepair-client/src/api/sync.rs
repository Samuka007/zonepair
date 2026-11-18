//! `changes` and `publish` — the two RFC-0001-sync-protocol wire operations.
//!
//! Wire layer only: this module moves requests and replies between A and B
//! and classifies what came back. It never touches the mirror, never merges,
//! never parses the jsonl it carries — `Append`/`Reset` bodies are raw bytes
//! for the caller to copy down verbatim, and `publish` content is encoded,
//! never examined (RFC-0001-sync-protocol §4.2, §6.4). Response classification lives in
//! `reply`; the scripted-server harness in `testkit` (`#[cfg(test)]`).
//!
//! Reply classification follows §8's positive match rule: only
//! `404 + JSON + code=="missing"` is "session absent on B"; every other 404
//! shape — empty body, non-JSON, JSON without that code — is "server too
//! old" (`ChangesReply::Unsupported`), a definitive signal the caller must
//! fail on rather than silently degrade into a full pull (§9).

use crate::transport::{request_raw, with_retry, Base, HTTP_CLIENT};
use anyhow::{bail, Context, Result};
use reqwest::Method;
use zonepair_proto::syncwire::PublishRequest;

mod reply;
#[cfg(test)]
pub(crate) mod testkit;

/// What one `changes` round trip established (RFC-0001-sync-protocol §6.2/§8). The
/// byte-carrying verdicts hand the body over unexamined — verbatim copy-down
/// and the local-length precondition on `append` are the caller's problem,
/// not the wire layer's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangesReply {
    /// Nothing moved since `cursor`; `next_cursor` is the request cursor
    /// echoed back.
    Unchanged { next_cursor: String },
    /// New bytes: append `body` to the local file, then commit `next_cursor`.
    Append { body: Vec<u8>, next_cursor: String },
    /// The held prefix is void: `body` is the full stable prefix, to be
    /// swapped in whole; then commit `next_cursor`.
    Reset { body: Vec<u8>, next_cursor: String },
    /// 404 with `code:"missing"` — B has no session under this id.
    Missing,
    /// Any other 404 shape — B predates the sync protocol (§8/§9). The
    /// caller fails explicitly; silently falling back to a full pull would
    /// reintroduce exactly the mode this protocol removes.
    Unsupported,
}

/// Outcome of one `publish` call (RFC-0001-sync-protocol §7.2/§7.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishReply {
    /// 200: `cursor` attests the fork's prefix now sits on B — the same
    /// `zpc1` credential a `changes` reply hands back, ready to store as
    /// pull progress. `already` is false on first publish, true when B's
    /// existing file was byte-identical (idempotent replay after a lost
    /// response).
    Published { already: bool, cursor: String },
    /// 409 `{"ok":false,"error":"conflict"}`: same id, different bytes — a
    /// terminal divergence the wire layer reports and never retries.
    Conflict,
}

/// Session ids land in a URL path segment. The store never mints ids with
/// URL-significant characters, so rather than teach the caller about partial
/// encoding, reject anything outside it — a request built on a bad id is a
/// caller bug, not a remote condition.
fn validate_sid(sid: &str) -> Result<()> {
    if sid.is_empty()
        || !sid.bytes().all(|b| (0x21..=0x7e).contains(&b))
        || sid.bytes().any(|b| matches!(b, b'/' | b'?' | b'#' | b'%'))
    {
        bail!("session id {sid:?} cannot be sent in a URL path segment");
    }
    Ok(())
}

/// One `changes` call: `GET {base}/v1/store/sessions/{sid}/changes?cursor=<c>`
/// (RFC-0001-sync-protocol §6.1). An empty `cursor` omits the parameter — the canonical
/// "first pull" spelling alongside the zero-value cursor.
///
/// Retry semantics (§8): transport errors, 5xx, and replies that never
/// became meaningful data (unexpected `Content-Type`, missing or
/// unparseable `X-ZP-*` headers on an octet-stream body — the same class as
/// a short read per §6.4) consume the three-attempt budget. 4xx never
/// retries: `code:"missing"` → `Missing`, every other 404 → `Unsupported`,
/// anything else (400 bad cursor, 401, …) → `Err`.
pub async fn changes(
    base: &Base,
    token: Option<&str>,
    sid: &str,
    cursor: &str,
) -> Result<ChangesReply> {
    validate_sid(sid)?;
    // Query encoding rides reqwest's `Url` — the same form-urlencoded codec
    // `.query(&[("cursor", c)])` would apply (transport takes a path string,
    // so the pair is appended here instead). For contract inputs (`zpc1`)
    // the encoded bytes are identical to the old hand-rolled set; §5.2
    // already keeps cursors inside the unreserved alphabet.
    let mut url =
        reqwest::Url::parse("zp://sync").expect("a constant scheme + host pair always parses");
    url.set_path(&format!("/v1/store/sessions/{sid}/changes"));
    if !cursor.is_empty() {
        url.query_pairs_mut().append_pair("cursor", cursor);
    }
    let path = url
        .query()
        .map_or_else(|| url.path().to_string(), |q| format!("{}?{q}", url.path()));
    // The classifier runs inside the retry loop: a malformed octet-stream
    // verdict is a TransportFault, which only retries when produced here.
    let (_status, reply) = with_retry(|| async {
        let (status, headers, body) =
            request_raw(&HTTP_CLIENT, base, Method::GET, &path, None, token).await?;
        let reply = reply::changes_reply(status, &headers, &body)?;
        Ok((status, reply))
    })
    .await?;
    Ok(reply)
}

/// One `publish` call: `POST {base}/v1/store/sessions/{sid}/publish`
/// (RFC-0001-sync-protocol §7). `bytes` is the complete fork product, encoded base64 with
/// its SHA-256 hex — assembly only encodes; nothing here parses, merges or
/// reorders a single line.
///
/// The hex is lowercase by construction (`format!("{:x}", …)`): its first
/// 32 chars become the cursor fingerprint segment, which §5.2 requires to
/// be lowercase — uppercase here would mint cursors the next `changes`
/// call rejects as malformed.
///
/// Reply semantics (§7.2/§8): 200 → `Published` (`already` tells a fresh
/// publish from an idempotent replay), 409 `conflict` → `Conflict`, other
/// 4xx → `Err` (caller error or auth), 5xx/transport/reply-contract faults
/// → the retry budget. The classifier runs inside the loop for the same
/// reason `changes` does: a reply that fails its shape checks is a
/// `TransportFault`, which only retries when produced here.
pub async fn publish(
    base: &Base,
    token: Option<&str>,
    sid: &str,
    bytes: &[u8],
) -> Result<PublishReply> {
    validate_sid(sid)?;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use sha2::{Digest, Sha256};

    let wire = PublishRequest {
        session_id: sid.to_string(),
        content: STANDARD.encode(bytes),
        content_sha256: format!("{:x}", Sha256::digest(bytes)),
    };
    let body = serde_json::to_value(&wire).context("serializing publish request")?;
    let path = format!("/v1/store/sessions/{sid}/publish");
    let (_status, reply) = with_retry(|| async {
        let (status, _headers, body) =
            request_raw(&HTTP_CLIENT, base, Method::POST, &path, Some(&body), token).await?;
        let reply = reply::publish_reply(status, &body)?;
        Ok((status, reply))
    })
    .await?;
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::testkit::{
        finish_server, http_json, http_octets, lines, run, serve_script, Scripted,
    };
    use super::*;
    use crate::transport::{BODY_IDLE_TIMEOUT_MS, REQUEST_PHASE_TIMEOUT_MS};
    use serde_json::json;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    const CURSOR: &str = "zpc1.4000.0123456789abcdef0123456789abcdef";
    const NEXT: &str = "zpc1.5350.fedcba9876543210fedcba9876543210";

    // ---- changes (RFC-0001-sync-protocol §6, §8) --------------------------------------

    // T-401 wire side: an octet-stream `append` verdict decodes into raw
    // bytes plus the header cursor, and the request carries the caller's
    // cursor verbatim in the query.
    #[tokio::test]
    async fn changes_octet_stream_append_decodes_headers_and_body() {
        let script = vec![vec![Scripted::Reply(http_octets(
            "append",
            NEXT,
            b"{\"uuid\":\"u3\"}\n{\"uuid\":\"u4\"}\n",
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let reply = run(&seen, changes(&base, None, "s1", CURSOR)).await;
        assert_eq!(
            reply,
            ChangesReply::Append {
                body: b"{\"uuid\":\"u3\"}\n{\"uuid\":\"u4\"}\n".to_vec(),
                next_cursor: NEXT.to_string(),
            }
        );
        assert_eq!(
            seen.lock().unwrap()[0].request_line,
            format!("GET /v1/store/sessions/s1/changes?cursor={CURSOR} HTTP/1.1")
        );
        finish_server(server, &seen).await;
    }

    // §6.2 `reset`: same octet-stream shape, opposite verdict — the whole
    // stable prefix rides the body. An empty local cursor omits the query
    // parameter entirely (§6.3 branch 1: absent/empty/zero are equivalent).
    #[tokio::test]
    async fn changes_empty_cursor_omits_parameter_and_takes_reset() {
        let script = vec![vec![Scripted::Reply(http_octets(
            "reset",
            NEXT,
            b"{\"uuid\":\"u1\"}\n",
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let reply = run(&seen, changes(&base, None, "s1", "")).await;
        assert_eq!(
            reply,
            ChangesReply::Reset {
                body: b"{\"uuid\":\"u1\"}\n".to_vec(),
                next_cursor: NEXT.to_string(),
            }
        );
        assert_eq!(
            seen.lock().unwrap()[0].request_line,
            "GET /v1/store/sessions/s1/changes HTTP/1.1"
        );
        finish_server(server, &seen).await;
    }

    // T-408 wire side: a JSON `{"state":"unchanged"}` body is a verdict with
    // no bytes; the wire layer reports it and moves nothing.
    #[tokio::test]
    async fn changes_json_unchanged() {
        let script = vec![vec![Scripted::Reply(http_json(
            200,
            &json!({"ok": true, "state": "unchanged", "nextCursor": CURSOR}).to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let reply = run(&seen, changes(&base, None, "s1", CURSOR)).await;
        assert_eq!(
            reply,
            ChangesReply::Unchanged {
                next_cursor: CURSOR.to_string(),
            }
        );
        finish_server(server, &seen).await;
    }

    // T-405 half: the octet body arrives shorter than Content-Length — a
    // reqwest body error inside the retry budget, so all three attempts run
    // and the failure surfaces as a transport error, not a reply.
    #[tokio::test]
    async fn changes_short_read_retries_then_errors() {
        let script = vec![
            vec![Scripted::Reply(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-ZP-State: append\r\nX-ZP-Next-Cursor: zpc1.9.00000000000000000000000000000000\r\nContent-Length: 20\r\nConnection: close\r\n\r\nshort".to_vec(),
            )],
            vec![Scripted::Reply(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-ZP-State: append\r\nX-ZP-Next-Cursor: zpc1.9.00000000000000000000000000000000\r\nContent-Length: 20\r\nConnection: close\r\n\r\nshort".to_vec(),
            )],
            vec![Scripted::Reply(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-ZP-State: append\r\nX-ZP-Next-Cursor: zpc1.9.00000000000000000000000000000000\r\nContent-Length: 20\r\nConnection: close\r\n\r\nshort".to_vec(),
            )],
        ];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            changes(&base, None, "s1", CURSOR),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        assert!(outcome.is_err(), "short reads must surface as errors");
        assert_eq!(
            lines(&seen).len(),
            3,
            "a short read consumes the whole retry budget"
        );
        finish_server(server, &seen).await;
    }

    // T-405 other half: either missing `X-ZP-*` header is a TransportFault —
    // retryable like a short read, and definitively an error once the budget
    // is spent. Here `X-ZP-Next-Cursor` is absent on every attempt.
    #[tokio::test]
    async fn changes_missing_next_cursor_header_retries_then_errors() {
        let reply = |extra: &str| -> Vec<u8> {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-ZP-State: append\r\n{extra}Content-Length: 1\r\nConnection: close\r\n\r\nx"
            )
            .into_bytes()
        };
        let script = vec![
            vec![Scripted::Reply(reply(""))],
            vec![Scripted::Reply(reply(""))],
            vec![Scripted::Reply(reply(""))],
        ];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            changes(&base, None, "s1", CURSOR),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        let error = outcome.unwrap_err();
        assert!(
            format!("{error:#}").contains(zonepair_proto::syncwire::X_ZP_NEXT_CURSOR),
            "missing header must name itself: {error:#}"
        );
        assert_eq!(lines(&seen).len(), 3);
        finish_server(server, &seen).await;
    }

    // And the symmetric case: `X-ZP-Next-Cursor` present, `X-ZP-State` gone.
    #[tokio::test]
    async fn changes_missing_state_header_is_a_fault() {
        let reply = || -> Vec<u8> {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-ZP-Next-Cursor: {NEXT}\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx"
            )
            .into_bytes()
        };
        let script = vec![
            vec![Scripted::Reply(reply())],
            vec![Scripted::Reply(reply())],
            vec![Scripted::Reply(reply())],
        ];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            changes(&base, None, "s1", CURSOR),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        assert!(outcome.is_err());
        assert_eq!(lines(&seen).len(), 3);
        finish_server(server, &seen).await;
    }

    // A 200 whose Content-Type is neither JSON nor octet-stream is the same
    // contract break as a missing header: TransportFault, retried, then an
    // error when the budget is spent.
    #[tokio::test]
    async fn changes_unexpected_content_type_is_a_fault() {
        let reply = || -> Vec<u8> {
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 11\r\nConnection: close\r\n\r\n<html></html>".to_vec()
        };
        let script = vec![
            vec![Scripted::Reply(reply())],
            vec![Scripted::Reply(reply())],
            vec![Scripted::Reply(reply())],
        ];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            changes(&base, None, "s1", CURSOR),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        assert!(outcome.is_err());
        assert_eq!(lines(&seen).len(), 3);
        finish_server(server, &seen).await;
    }

    // Contract violation in the other direction: JSON claiming `append`
    // carries no bytes, so the verdict is meaningless — TransportFault.
    #[tokio::test]
    async fn changes_json_append_verdict_is_a_fault() {
        let script = vec![
            vec![Scripted::Reply(http_json(
                200,
                &json!({"ok": true, "state": "append", "nextCursor": NEXT}).to_string(),
            ))],
            vec![Scripted::Reply(http_json(
                200,
                &json!({"ok": true, "state": "append", "nextCursor": NEXT}).to_string(),
            ))],
            vec![Scripted::Reply(http_json(
                200,
                &json!({"ok": true, "state": "append", "nextCursor": NEXT}).to_string(),
            ))],
        ];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            changes(&base, None, "s1", CURSOR),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        assert!(outcome.is_err());
        assert_eq!(lines(&seen).len(), 3);
        finish_server(server, &seen).await;
    }

    // T-406: 404 with a parseable body and `code:"missing"` is the positive
    // signal for "session absent on B" — the only 404 that may mean it (§8).
    #[tokio::test]
    async fn changes_404_code_missing() {
        let script = vec![vec![Scripted::Reply(http_json(
            404,
            &json!({"ok": false, "error": "session s1 not found", "code": "missing"}).to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let reply = run(&seen, changes(&base, None, "s1", CURSOR)).await;
        assert_eq!(reply, ChangesReply::Missing);
        finish_server(server, &seen).await;
    }

    // T-316/T-608: every other 404 shape — empty body, non-JSON, JSON
    // without `code`, JSON with a different `code` — is an old server, an
    // explicit failure signal, never "missing" (§8 positive match, §9).
    #[tokio::test]
    async fn changes_404_other_shapes_are_unsupported() {
        for (name, response) in [
            (
                "empty body",
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_vec(),
            ),
            (
                "html body",
                b"HTTP/1.1 404 Not Found\r\nContent-Type: text/html\r\nContent-Length: 11\r\nConnection: close\r\n\r\n<html></html>".to_vec(),
            ),
            (
                "json without code",
                http_json(404, &json!({"ok": false, "error": "not found"}).to_string()),
            ),
            (
                "json with another code",
                http_json(
                    404,
                    &json!({"ok": false, "error": "nope", "code": "gone"}).to_string(),
                ),
            ),
        ] {
            let script = vec![vec![Scripted::Reply(response)]];
            let (base, seen, server) = serve_script(script).await;
            let reply = run(&seen, changes(&base, None, "s1", CURSOR)).await;
            assert_eq!(reply, ChangesReply::Unsupported, "{name}");
            finish_server(server, &seen).await;
        }
    }

    // T-317 half: a 4xx is definitive — a 400 (bad cursor) must not retry
    // and must surface its status in the error.
    #[tokio::test]
    async fn changes_400_does_not_retry() {
        let script = vec![vec![Scripted::Reply(http_json(
            400,
            &json!({"ok": false, "error": "bad cursor"}).to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            changes(&base, None, "s1", "bogus"),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        let error = outcome.unwrap_err();
        assert!(format!("{error:#}").contains("400"), "{error:#}");
        assert_eq!(
            lines(&seen).len(),
            1,
            "4xx must never consume the retry budget"
        );
        finish_server(server, &seen).await;
    }

    // T-317 other half: a 401 (Bearer middleware) is also definitive.
    #[tokio::test]
    async fn changes_401_does_not_retry() {
        let script = vec![vec![Scripted::Reply(http_json(
            401,
            &json!({"ok": false, "error": "unauthorized"}).to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            changes(&base, None, "s1", CURSOR),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        assert!(outcome.is_err());
        assert_eq!(lines(&seen).len(), 1);
        finish_server(server, &seen).await;
    }

    // §8: 5xx consumes the retry budget (three attempts, then the error
    // surfaces with its status).
    #[tokio::test]
    async fn changes_5xx_retries_to_exhaustion() {
        let script = vec![
            vec![Scripted::Reply(http_json(
                500,
                &json!({"ok": false, "error": "io error"}).to_string(),
            ))],
            vec![Scripted::Reply(http_json(
                503,
                &json!({"ok": false, "error": "unavailable"}).to_string(),
            ))],
            vec![Scripted::Reply(http_json(
                500,
                &json!({"ok": false, "error": "io error"}).to_string(),
            ))],
        ];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            changes(&base, None, "s1", CURSOR),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        let error = outcome.unwrap_err();
        assert!(format!("{error:#}").contains("500"), "{error:#}");
        assert_eq!(lines(&seen).len(), 3);
        finish_server(server, &seen).await;
    }

    // §8: a dropped connection is a transport error — same budget as 5xx,
    // and a retry after it can succeed (changes is an idempotent read).
    #[tokio::test]
    async fn changes_dropped_connection_retries() {
        let script = vec![
            vec![Scripted::Drop],
            vec![Scripted::Reply(http_json(
                200,
                &json!({"ok": true, "state": "unchanged", "nextCursor": CURSOR}).to_string(),
            ))],
        ];
        let (base, seen, server) = serve_script(script).await;
        let reply = run(&seen, changes(&base, None, "s1", CURSOR)).await;
        assert_eq!(
            reply,
            ChangesReply::Unchanged {
                next_cursor: CURSOR.to_string(),
            }
        );
        assert_eq!(lines(&seen).len(), 2);
        finish_server(server, &seen).await;
    }

    // ---- T-613 timeout layering (RFC-0003-zonepaird §6.2 timeout layering) --------

    /// All T-613 tests inject the SAME millisecond budgets: the statics
    /// are process-global and `cargo test` runs cases in parallel, so any
    /// divergence between tests would be a cross-test race. Every test
    /// below stays correct under 200ms for both clocks.
    fn inject_fast_timeouts() {
        REQUEST_PHASE_TIMEOUT_MS.store(200, Ordering::Relaxed);
        BODY_IDLE_TIMEOUT_MS.store(200, Ordering::Relaxed);
    }

    // A peer that accepts the connection and never writes a response head
    // must die on the REQUEST_PHASE deadline — a hung `send()` is exactly
    // the "worker lane wedged forever" failure the review flagged.
    #[tokio::test]
    async fn changes_no_response_head_times_out_and_retries() {
        inject_fast_timeouts();
        let script = vec![
            vec![Scripted::Hold(Vec::new())],
            vec![Scripted::Hold(Vec::new())],
            vec![Scripted::Hold(Vec::new())],
        ];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            changes(&base, None, "s1", CURSOR),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        let error = outcome.expect_err("a silent socket must not wait forever");
        assert!(
            format!("{error:#}").contains("no response head"),
            "phase timeout must name itself: {error:#}"
        );
        assert_eq!(lines(&seen).len(), 3, "a dead head consumes the retry budget");
        finish_server(server, &seen).await;
    }

    // Response head arrives, then the peer stalls mid-body: the body-idle
    // liveness deadline (per chunk, not total) kills it — and retries.
    #[tokio::test]
    async fn changes_stalled_body_times_out_and_retries() {
        inject_fast_timeouts();
        let head = b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-ZP-State: reset\r\nX-ZP-Next-Cursor: zpc1.9.00000000000000000000000000000000\r\nContent-Length: 64\r\nConnection: close\r\n\r\n".to_vec();
        let script = vec![
            vec![Scripted::Hold(head.clone())],
            vec![Scripted::Hold(head.clone())],
            vec![Scripted::Hold(head)],
        ];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            changes(&base, None, "s1", CURSOR),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        let error = outcome.expect_err("a stalled body must not wait forever");
        assert!(
            format!("{error:#}").contains("body stalled"),
            "idle timeout must name itself: {error:#}"
        );
        assert_eq!(lines(&seen).len(), 3, "a stalled body consumes the retry budget");
        finish_server(server, &seen).await;
    }

    // The mirror image: a reply whose pieces each arrive inside the idle
    // budget completes even though TOTAL transfer time exceeds it —
    // liveness is per chunk, so a slow big `reset` is never wall-clocked.
    // The head lands in the first write (no phase-timeout engagement);
    // only the body drips.
    #[tokio::test]
    async fn changes_slow_stream_is_not_wall_clocked() {
        inject_fast_timeouts();
        let wire = http_octets("reset", NEXT, b"{\"uuid\":\"u1\"}\n");
        let head_end = wire.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let mut drip = vec![wire[..head_end].to_vec()];
        // 14 body bytes in ~5B slices, 150ms apart ≈ 450ms of streaming
        // against a 200ms idle budget: alive per gap, over budget in total.
        drip.extend(wire[head_end..].chunks(5).map(|c| c.to_vec()));
        let script = vec![vec![Scripted::Drip(drip, Duration::from_millis(150))]];
        let (base, seen, server) = serve_script(script).await;
        let reply = run(&seen, changes(&base, None, "s1", "")).await;
        assert_eq!(
            reply,
            ChangesReply::Reset {
                body: b"{\"uuid\":\"u1\"}\n".to_vec(),
                next_cursor: NEXT.to_string(),
            }
        );
        finish_server(server, &seen).await;
    }

    // ---- publish (RFC-0001-sync-protocol §7, §8) ---------------------------------------

    fn sha256_hex(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(bytes))
    }

    // §7.1/§7.2: the request body is exactly `{sessionId, content, contentSha256}`
    // — base64 standard alphabet with padding, full lowercase-hex SHA-256 of
    // the raw bytes — and a 200 reply decodes `already` and the confirmation
    // cursor. The wire layer encodes; it never looks inside `bytes`.
    #[tokio::test]
    async fn publish_first_publish_returns_cursor() {
        let content = b"{\"uuid\":\"u1\"}\n{\"uuid\":\"u2\"}\n";
        let cursor = format!("zpc1.{}.{}", content.len(), &sha256_hex(content)[..32]);
        let script = vec![vec![Scripted::Reply(http_json(
            200,
            &json!({
                "ok": true,
                "published": true,
                "already": false,
                "cursor": cursor,
            })
            .to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let reply = run(&seen, publish(&base, None, "fork-1", content)).await;
        assert_eq!(
            reply,
            PublishReply::Published {
                already: false,
                cursor,
            }
        );
        {
            let seen = seen.lock().unwrap();
            assert_eq!(
                seen[0].request_line,
                "POST /v1/store/sessions/fork-1/publish HTTP/1.1"
            );
            let sent: serde_json::Value = serde_json::from_str(&seen[0].body).unwrap();
            assert_eq!(sent["sessionId"], "fork-1");
            assert_eq!(
                sent["content"],
                base64::engine::Engine::encode(&base64::engine::general_purpose::STANDARD, content)
            );
            assert_eq!(sent["contentSha256"], sha256_hex(content));
            assert_eq!(sent.as_object().unwrap().len(), 3);
        }
        finish_server(server, &seen).await;
    }

    // §7.2: byte-identical replay → `already:true`; the wire layer treats it
    // as published — the caller's queue semantics make it a success.
    #[tokio::test]
    async fn publish_idempotent_replay() {
        let content = b"{\"uuid\":\"u1\"}\n";
        let cursor = format!("zpc1.{}.{}", content.len(), &sha256_hex(content)[..32]);
        let script = vec![vec![Scripted::Reply(http_json(
            200,
            &json!({
                "ok": true,
                "published": true,
                "already": true,
                "cursor": cursor,
            })
            .to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let reply = run(&seen, publish(&base, None, "fork-1", content)).await;
        assert_eq!(
            reply,
            PublishReply::Published {
                already: true,
                cursor,
            }
        );
        finish_server(server, &seen).await;
    }

    // §7.2: same id, different bytes — 409 `conflict` is a terminal verdict
    // the wire layer reports and the retry budget never sees.
    #[tokio::test]
    async fn publish_conflict_is_terminal() {
        let script = vec![vec![Scripted::Reply(http_json(
            409,
            &json!({"ok": false, "error": "conflict"}).to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let reply = run(&seen, publish(&base, None, "fork-1", b"{}\n")).await;
        assert_eq!(reply, PublishReply::Conflict);
        assert_eq!(lines(&seen).len(), 1, "409 must not retry");
        finish_server(server, &seen).await;
    }

    // A 409 whose body is not the named conflict verdict is a generic
    // failure — it must not masquerade as `Conflict`.
    #[tokio::test]
    async fn publish_409_without_conflict_marker_errors() {
        let script = vec![vec![Scripted::Reply(http_json(
            409,
            &json!({"ok": false, "error": "gone"}).to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            publish(&base, None, "fork-1", b"{}\n"),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        let error = outcome.unwrap_err();
        assert!(format!("{error:#}").contains("409"), "{error:#}");
        finish_server(server, &seen).await;
    }

    // §8: publish 400 is a caller error — definitive, no retry, status in
    // the surfaced error.
    #[tokio::test]
    async fn publish_400_does_not_retry() {
        let script = vec![vec![Scripted::Reply(http_json(
            400,
            &json!({"ok": false, "error": "sha256 mismatch"}).to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            publish(&base, None, "fork-1", b"{}\n"),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        let error = outcome.unwrap_err();
        assert!(format!("{error:#}").contains("400"), "{error:#}");
        assert_eq!(lines(&seen).len(), 1);
        finish_server(server, &seen).await;
    }

    // §8: publish 5xx rides the same transport budget as everything else.
    #[tokio::test]
    async fn publish_5xx_retries_to_exhaustion() {
        let script = vec![
            vec![Scripted::Reply(http_json(
                500,
                &json!({"ok": false, "error": "io error"}).to_string(),
            ))],
            vec![Scripted::Reply(http_json(
                500,
                &json!({"ok": false, "error": "io error"}).to_string(),
            ))],
            vec![Scripted::Reply(http_json(
                500,
                &json!({"ok": false, "error": "io error"}).to_string(),
            ))],
        ];
        let (base, seen, server) = serve_script(script).await;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            publish(&base, None, "fork-1", b"{}\n"),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => panic!("request timed out; requests seen: {:?}", lines(&seen)),
        };
        assert!(outcome.is_err());
        assert_eq!(lines(&seen).len(), 3);
        finish_server(server, &seen).await;
    }

    // ---- cursor validation (§10-7 corruption never masquerades in reverse) -
    //
    // A server-minted cursor is a credential persisted verbatim — an
    // unparseable one means the reply never became meaningful data:
    // `TransportFault`, riding the retry budget, never stored as a
    // (later "corrupt") local cursor. Three ingresses, one rule:
    // publish's JSON `cursor`, changes' `X-ZP-Next-Cursor` header, and
    // changes' JSON `nextCursor`.
    #[tokio::test]
    async fn unparseable_cursors_are_transport_faults() {
        for (name, scripted_reply, is_publish) in [
            (
                "publish 200 cursor",
                http_json(
                    200,
                    &json!({"ok": true, "published": true, "already": false, "cursor": "bogus"})
                        .to_string(),
                ),
                true,
            ),
            (
                "changes octet X-ZP-Next-Cursor",
                b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-ZP-State: append\r\nX-ZP-Next-Cursor: garbage\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx".to_vec(),
                false,
            ),
            (
                "changes JSON nextCursor",
                http_json(
                    200,
                    &json!({"ok": true, "state": "unchanged", "nextCursor": "bogus"}).to_string(),
                ),
                false,
            ),
        ] {
            let script = vec![
                vec![Scripted::Reply(scripted_reply.clone())],
                vec![Scripted::Reply(scripted_reply.clone())],
                vec![Scripted::Reply(scripted_reply)],
            ];
            let (base, seen, server) = serve_script(script).await;
            let outcome = match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                async {
                    if is_publish {
                        publish(&base, None, "fork-1", b"{}\n").await.map(|r| format!("{r:?}"))
                    } else {
                        changes(&base, None, "s1", CURSOR).await.map(|r| format!("{r:?}"))
                    }
                },
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(_) => panic!("{name}: request timed out; seen: {:?}", lines(&seen)),
            };
            let error = outcome.expect_err(&format!("{name}"));
            assert!(
                format!("{error:#}").contains("unparseable cursor"),
                "{name}: bad cursor must name itself: {error:#}"
            );
            assert_eq!(lines(&seen).len(), 3, "{name}: rides the retry budget");
            finish_server(server, &seen).await;
        }
    }
}
