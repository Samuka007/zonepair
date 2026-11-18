//! T-3xx sync route contract tests (RFC-0001-sync-protocol §6–§9, RFC-0004-b-endpoints §3–§9).
//!
//! The loopback wire harness lives in `tests/support` — the crate's one
//! copy (S2 §2.1). `tower` is not a dependency and Cargo.toml stays
//! untouched, so `support::serve` drives the real `axum::serve` and the
//! codec parses HTTP off the wire — same assertions, strictly more
//! coverage (the bearer middleware, hyper's response framing and the
//! handler all run).

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use support::{get, post, request, serve, store_at, tmpdir, RawResponse};
use zonepair_proto::cursor::{self, Cursor};

mod support;

// ---- fixtures ----------------------------------------------------------------

fn write_session(root: &Path, id: &str, content: &[u8]) -> PathBuf {
    let path = root.join("projects/proj-a").join(format!("{id}.jsonl"));
    fs::write(&path, content).unwrap();
    path
}

/// The cursor a client holds after pulling `content` (covers its stable
/// prefix — the full content here, since fixtures end with `\n`).
fn cursor_for(content: &[u8]) -> String {
    Cursor::new(content.len() as u64, cursor::prefix_hash(content)).encode()
}

fn publish_body(sid: &str, content: &[u8]) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "sessionId": sid,
        "content": B64.encode(content),
        "contentSha256": format!("{:x}", Sha256::digest(content)),
    }))
    .unwrap()
}

/// The octet-stream + X-ZP-* wire shape of a reset reply: body is exactly
/// `[0, boundary)` and nextCursor is `zpc1.boundary.H(boundary)`.
fn assert_reset(resp: &RawResponse, stable_prefix: &[u8]) {
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("content-type"),
        Some("application/octet-stream")
    );
    assert_eq!(resp.header("x-zp-state"), Some("reset"));
    assert_eq!(resp.body, stable_prefix, "reset body = [0, boundary)");
    let next = resp.header("x-zp-next-cursor").expect("X-ZP-Next-Cursor");
    let parsed = cursor::parse(next).expect("next cursor parses");
    assert_eq!(parsed.offset, stable_prefix.len() as u64);
    assert_eq!(
        parsed.prefix_hash,
        cursor::prefix_hash(stable_prefix),
        "nextCursor = zpc1.boundary.H(boundary)"
    );
}

fn assert_404_missing(resp: &RawResponse) {
    assert_eq!(resp.status, 404);
    let body = resp.json();
    assert_eq!(body["ok"], json!(false));
    assert_eq!(
        body["code"],
        json!("missing"),
        "404 must carry code:\"missing\" so clients can tell it from an old server"
    );
    assert!(body["error"].as_str().unwrap().contains("not found"));
}

const S1_CONTENT: &[u8] =
    b"{\"uuid\":\"u1\",\"sessionId\":\"s1\"}\n{\"uuid\":\"u2\",\"sessionId\":\"s1\"}\n";

// ---- T-3xx -------------------------------------------------------------------

/// T-301 (§6.3 #1): absent, empty and zero-value cursors are one equivalence
/// class — first pull → reset with the full stable prefix.
#[tokio::test]
async fn t301_first_pull_equivalence_class_resets() {
    let root = tmpdir("t301");
    write_session(&root, "s1", S1_CONTENT);
    let server = serve(&root, None).await;

    for query in [
        "/v1/store/sessions/s1/changes".to_string(),
        "/v1/store/sessions/s1/changes?cursor=".to_string(),
        format!("/v1/store/sessions/s1/changes?cursor={}", cursor::ZERO),
    ] {
        let resp = get(&server, &query).await;
        assert_reset(&resp, S1_CONTENT);
    }
}

/// T-302 (§4.1): an unterminated tail never crosses the boundary — the reset
/// body carries only the stable prefix.
#[tokio::test]
async fn t302_unterminated_tail_excluded() {
    let root = tmpdir("t302");
    let mut content = S1_CONTENT.to_vec();
    content.extend_from_slice(b"{\"uuid\":\"u3\",\"par"); // half-written line
    write_session(&root, "s1", &content);
    let server = serve(&root, None).await;

    let resp = get(&server, "/v1/store/sessions/s1/changes").await;
    assert_reset(&resp, S1_CONTENT); // body stops at boundary == S1_CONTENT.len()
}

/// T-303 (§6.3 #2): parse failure and unknown version are 400, never reset.
#[tokio::test]
async fn t303_bad_cursor_is_400_not_reset() {
    let root = tmpdir("t303");
    write_session(&root, "s1", S1_CONTENT);
    let server = serve(&root, None).await;

    for bad in [
        "garbage",
        "zpc2.10.00000000000000000000000000000000",
        "zpc1.abc.00000000000000000000000000000000",
        "zpc1.10.SHORT",
    ] {
        let resp = get(&server, &format!("/v1/store/sessions/s1/changes?cursor={bad}")).await;
        assert_eq!(resp.status, 400, "cursor {bad:?}");
        assert_eq!(
            resp.header("content-type"),
            Some("application/json"),
            "errors are JSON, not octet-stream"
        );
        let body = resp.json();
        assert_eq!(body["ok"], json!(false));
        assert_eq!(body["error"], json!("bad cursor"));
    }
}

/// T-304 (§6.3 #3): cursor offset past the boundary — the length promise
/// failed → reset with the full stable prefix.
#[tokio::test]
async fn t304_offset_beyond_boundary_resets() {
    let root = tmpdir("t304");
    write_session(&root, "s1", S1_CONTENT);
    let server = serve(&root, None).await;

    let beyond = S1_CONTENT.len() as u64 + 100;
    let cursor = Cursor::new(beyond, [0xab; 16]).encode();
    let resp = get(
        &server,
        &format!("/v1/store/sessions/s1/changes?cursor={cursor}"),
    )
    .await;
    assert_reset(&resp, S1_CONTENT);
}

/// T-305 (§6.3 #4): merge's whole-file rewrite changes the prefix bytes →
/// fingerprint mismatch → reset.
#[tokio::test]
async fn t305_merge_rewrite_fingerprint_mismatch_resets() {
    let (root, store) = store_at("t305");
    // Non-canonical seed: merge's reserialize rewrites the prefix bytes.
    let seed: &[u8] = b"{ \"uuid\": \"u-0\", \"sessionId\": \"s1\", \"z\": 1 }\n";
    write_session(&root, "s1", seed);
    let server = serve(&root, None).await;
    let old_cursor = cursor_for(seed);

    store
        .merge("s1", &[json!({"uuid": "u-1", "sessionId": "s1"})])
        .await
        .unwrap();
    let new_content = fs::read(root.join("projects/proj-a/s1.jsonl")).unwrap();
    assert_ne!(new_content[..seed.len()], seed[..]);

    let resp = get(
        &server,
        &format!("/v1/store/sessions/s1/changes?cursor={old_cursor}"),
    )
    .await;
    assert_reset(&resp, &new_content);
}

/// T-306 (§6.3 #5 note): an equal-LENGTH rewrite still fails the fingerprint
/// check — `unchanged` never skips verification.
#[tokio::test]
async fn t306_equal_length_rewrite_resets() {
    let root = tmpdir("t306");
    write_session(&root, "s1", S1_CONTENT);
    let server = serve(&root, None).await;
    let old_cursor = cursor_for(S1_CONTENT);

    // Same byte length, different content: "u1"→"u9".
    let mut rewritten = S1_CONTENT.to_vec();
    let at = std::str::from_utf8(S1_CONTENT).unwrap().find("u1").unwrap();
    rewritten[at] = b'u';
    rewritten[at + 1] = b'9';
    assert_eq!(rewritten.len(), S1_CONTENT.len());
    assert_ne!(rewritten, S1_CONTENT);
    fs::write(root.join("projects/proj-a/s1.jsonl"), &rewritten).unwrap();

    let resp = get(
        &server,
        &format!("/v1/store/sessions/s1/changes?cursor={old_cursor}"),
    )
    .await;
    // p.offset == boundary AND size unchanged — only the fingerprint can see
    // the rewrite, and it must verdict reset.
    assert_reset(&resp, &rewritten);
}

/// T-307 (§6.3 #5): offset == boundary with matching fingerprint → JSON
/// `unchanged` + the original cursor echoed back.
#[tokio::test]
async fn t307_unchanged_echoes_cursor_as_json() {
    let root = tmpdir("t307");
    write_session(&root, "s1", S1_CONTENT);
    let server = serve(&root, None).await;
    let cursor = cursor_for(S1_CONTENT);

    let resp = get(
        &server,
        &format!("/v1/store/sessions/s1/changes?cursor={cursor}"),
    )
    .await;
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("content-type"), Some("application/json"));
    let body = resp.json();
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["state"], json!("unchanged"));
    assert_eq!(
        body["nextCursor"],
        json!(cursor),
        "unchanged echoes the caller's cursor verbatim"
    );
    // Polling again stays unchanged (steady-state slot path exercised).
    let again = get(
        &server,
        &format!("/v1/store/sessions/s1/changes?cursor={cursor}"),
    )
    .await;
    assert_eq!(again.json()["state"], json!("unchanged"));
}

/// T-308 (§6.3 #6): offset < boundary with matching fingerprint → append of
/// exactly `[p.offset, boundary)`.
#[tokio::test]
async fn t308_append_delta() {
    let root = tmpdir("t308");
    let prefix: &[u8] = b"{\"uuid\":\"u1\",\"sessionId\":\"s1\"}\n";
    let delta: &[u8] = b"{\"uuid\":\"u2\",\"sessionId\":\"s1\"}\n";
    let mut content = prefix.to_vec();
    content.extend_from_slice(delta);
    write_session(&root, "s1", &content);
    let server = serve(&root, None).await;

    let cursor = cursor_for(prefix); // covers only the first line
    let resp = get(
        &server,
        &format!("/v1/store/sessions/s1/changes?cursor={cursor}"),
    )
    .await;
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("content-type"),
        Some("application/octet-stream")
    );
    assert_eq!(resp.header("x-zp-state"), Some("append"));
    assert_eq!(resp.body, delta, "append body = [p.offset, boundary)");
    let content_length = delta.len().to_string();
    assert_eq!(resp.header("content-length"), Some(content_length.as_str()));
    let next = resp.header("x-zp-next-cursor").unwrap().to_string();
    let parsed = cursor::parse(&next).unwrap();
    assert_eq!(parsed.offset, content.len() as u64);
    assert_eq!(parsed.prefix_hash, cursor::prefix_hash(&content));

    // Holding the new cursor, the next poll is unchanged.
    let resp = get(
        &server,
        &format!("/v1/store/sessions/s1/changes?cursor={next}"),
    )
    .await;
    assert_eq!(resp.json()["state"], json!("unchanged"));
}

/// T-309 (§6.2/§6.3 note): the session-existence check precedes the whole
/// decision table — a missing session answers 404 `code:"missing"` even when
/// the cursor is corrupt (not 400).
#[tokio::test]
async fn t309_missing_session_404_precedes_cursor_parsing() {
    let root = tmpdir("t309");
    let server = serve(&root, None).await;

    // Missing session, valid cursor → 404.
    let resp = get(
        &server,
        &format!("/v1/store/sessions/ghost/changes?cursor={}", cursor::ZERO),
    )
    .await;
    assert_404_missing(&resp);

    // Missing session + corrupt cursor → still 404, not 400.
    let resp = get(&server, "/v1/store/sessions/ghost/changes?cursor=garbage").await;
    assert_404_missing(&resp);
}

/// T-311 (§7.2/§7.3): first publish lands `projects/lab/<id>.jsonl`,
/// answers `already:false` + `zpc1.<len>.<sha[:32]>`, and a `changes` poll
/// with that cursor is immediately `unchanged`.
#[tokio::test]
async fn t311_publish_first_time() {
    let root = tmpdir("t311");
    let server = serve(&root, None).await;
    let content = b"{\"uuid\":\"f1\",\"sessionId\":\"fork1\",\"forkedFrom\":{}}\n";

    let resp = post(
        &server,
        "/v1/store/sessions/fork1/publish",
        publish_body("fork1", content),
    )
    .await;
    assert_eq!(resp.status, 200);
    let body = resp.json();
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["published"], json!(true));
    assert_eq!(body["already"], json!(false));
    let expected_cursor = format!(
        "zpc1.{}.{}",
        content.len(),
        &format!("{:x}", Sha256::digest(content))[..32]
    );
    assert_eq!(body["cursor"], json!(expected_cursor));

    // Landed on disk, byte-identical.
    let landed = root.join("projects/lab/fork1.jsonl");
    assert_eq!(fs::read(&landed).unwrap(), content);

    // The confirmation cursor feeds straight into changes → unchanged.
    let resp = get(
        &server,
        &format!("/v1/store/sessions/fork1/changes?cursor={expected_cursor}"),
    )
    .await;
    assert_eq!(resp.json()["state"], json!("unchanged"));
}

/// T-312 (§7.2, 0004 §9.2): a byte-identical republish is the idempotent
/// replay — `already:true`, same cursor, file untouched.
#[tokio::test]
async fn t312_publish_idempotent_replay() {
    let root = tmpdir("t312");
    let server = serve(&root, None).await;
    let content = b"{\"uuid\":\"f1\",\"sessionId\":\"fork1\"}\n";

    let first = post(
        &server,
        "/v1/store/sessions/fork1/publish",
        publish_body("fork1", content),
    )
    .await;
    assert_eq!(first.json()["already"], json!(false));

    let replay = post(
        &server,
        "/v1/store/sessions/fork1/publish",
        publish_body("fork1", content),
    )
    .await;
    assert_eq!(replay.status, 200);
    let body = replay.json();
    assert_eq!(body["published"], json!(true));
    assert_eq!(body["already"], json!(true));
    assert_eq!(body["cursor"], first.json()["cursor"]);
    assert_eq!(
        fs::read(root.join("projects/lab/fork1.jsonl")).unwrap(),
        content
    );
}

/// T-313 (§7.2): same id, different bytes → terminal 409 conflict; the
/// existing file is never merged or overwritten.
#[tokio::test]
async fn t313_publish_conflict() {
    let root = tmpdir("t313");
    let server = serve(&root, None).await;
    let content = b"{\"uuid\":\"f1\",\"sessionId\":\"fork1\"}\n";
    let other = b"{\"uuid\":\"DIFFERENT\",\"sessionId\":\"fork1\"}\n";

    post(
        &server,
        "/v1/store/sessions/fork1/publish",
        publish_body("fork1", content),
    )
    .await;

    let resp = post(
        &server,
        "/v1/store/sessions/fork1/publish",
        publish_body("fork1", other),
    )
    .await;
    assert_eq!(resp.status, 409);
    assert_eq!(
        resp.json(),
        json!({"ok": false, "error": "conflict"}),
        "409 body is exactly ok:false + error:conflict"
    );
    assert_eq!(
        fs::read(root.join("projects/lab/fork1.jsonl")).unwrap(),
        content,
        "conflict never overwrites the existing file"
    );
}

/// T-314 (§7.1/§7.2): every caller-error publish answers 400 and writes
/// nothing to disk.
#[tokio::test]
async fn t314_publish_400_matrix_writes_nothing() {
    let root = tmpdir("t314");
    let server = serve(&root, None).await;
    let good = b"{\"uuid\":\"f1\"}\n";
    let sha = format!("{:x}", Sha256::digest(good));

    let cases: Vec<(&str, Vec<u8>)> = vec![
        // path id != body id
        (
            "/v1/store/sessions/p1/publish",
            serde_json::to_vec(&json!({
                "sessionId": "OTHER",
                "content": B64.encode(good),
                "contentSha256": &sha,
            }))
            .unwrap(),
        ),
        // sha does not match decoded content
        (
            "/v1/store/sessions/p2/publish",
            serde_json::to_vec(&json!({
                "sessionId": "p2",
                "content": B64.encode(good),
                "contentSha256": "0".repeat(64),
            }))
            .unwrap(),
        ),
        // line not newline-terminated
        (
            "/v1/store/sessions/p3/publish",
            publish_body("p3", b"{\"uuid\":\"f1\"}"),
        ),
        // line is not JSON
        (
            "/v1/store/sessions/p4/publish",
            publish_body("p4", b"not json at all\n"),
        ),
        // body unparseable
        ("/v1/store/sessions/p5/publish", b"{not json".to_vec()),
        // malformed sha field (uppercase fails the lowercase-hex check)
        (
            "/v1/store/sessions/p6/publish",
            serde_json::to_vec(&json!({
                "sessionId": "p6",
                "content": B64.encode(good),
                "contentSha256": sha.to_uppercase(),
            }))
            .unwrap(),
        ),
        // content not base64
        (
            "/v1/store/sessions/p7/publish",
            serde_json::to_vec(&json!({
                "sessionId": "p7",
                "content": "!!!not-base64!!!",
                "contentSha256": &sha,
            }))
            .unwrap(),
        ),
        // missing required field (contentSha256 absent)
        (
            "/v1/store/sessions/p8/publish",
            serde_json::to_vec(&json!({
                "sessionId": "p8",
                "content": B64.encode(good),
            }))
            .unwrap(),
        ),
    ];

    for (path, body) in cases {
        let resp = post(&server, path, body).await;
        assert_eq!(resp.status, 400, "{path}");
        assert_eq!(resp.json()["ok"], json!(false));
    }
    assert!(
        !root.join("projects/lab").exists(),
        "no rejected publish may create the landing directory"
    );
}

/// T-316 (§8/§9): our 404 body carries the positive `code:"missing"` signal
/// that separates "session absent" from "server too old"; axum's bare-404
/// route miss shows the old-server shape (empty/non-JSON, no code) that a
/// client MUST fail loudly on instead of classifying.
#[tokio::test]
async fn t316_404_body_distinguishes_missing_from_old_server() {
    let root = tmpdir("t316");
    write_session(&root, "s1", S1_CONTENT);
    let server = serve(&root, None).await;

    // Ours: 404 + parseable JSON + code == "missing" → client classifies.
    let resp = get(&server, "/v1/store/sessions/ghost/changes").await;
    assert_eq!(resp.status, 404);
    let body = resp.json(); // panics if not parseable JSON — part of the check
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["code"], json!("missing"));

    // An unmatched route is axum's bare 404 — the "server too old" shape:
    // no JSON, no code field. A client applying the §8 positive match on
    // this response must not see a "missing" verdict.
    let resp = get(&server, "/v1/store/sessions/ghost/nothing-here").await;
    assert_eq!(resp.status, 404);
    let parsed: Result<Value, _> = serde_json::from_slice(&resp.body);
    let is_missing = parsed
        .ok()
        .and_then(|v| v.get("code").cloned())
        .is_some_and(|c| c == "missing");
    assert!(!is_missing, "bare 404 must not satisfy the missing predicate");
}

/// R1-C1 regression: axum's `DefaultBodyLimit` (2 MiB) must not sit in
/// front of publish — a fork of an accumulated session is multi-MB. A
/// >2 MiB wire body must reach the handler and land the file.
#[tokio::test]
async fn publish_body_over_default_limit_succeeds() {
    let root = tmpdir("bodylimit");
    let server = serve(&root, None).await;

    // ~3 MiB of valid JSONL (every line terminated + parseable) — the
    // base64 JSON wraps to ~4 MiB on the wire, well past the old 2 MiB cap.
    let mut content = Vec::new();
    let mut i = 0;
    while content.len() < 3 * 1024 * 1024 {
        content.extend_from_slice(
            format!("{{\"uuid\":\"u-{i}\",\"pad\":\"{}\"}}\n", "x".repeat(64)).as_bytes(),
        );
        i += 1;
    }

    let resp = post(
        &server,
        "/v1/store/sessions/bigfork/publish",
        publish_body("bigfork", &content),
    )
    .await;
    assert_eq!(resp.status, 200, "publish must not 413 at the axum default");
    assert_eq!(resp.json()["published"], json!(true));
    assert_eq!(
        fs::read(root.join("projects/lab/bigfork.jsonl")).unwrap(),
        content
    );
}

/// M4 regression: a session id containing `.zp` (or `.tmp.<n>`) is a legal
/// publish target and must stay visible to `list` — the residue glob only
/// matches real staging names, not bare substrings.
#[tokio::test]
async fn zp_named_session_stays_listable() {
    let root = tmpdir("zpname");
    let server = serve(&root, None).await;
    let content = b"{\"uuid\":\"u1\"}\n";

    for sid in ["zp.zp42", "t.tmp.1", "plain"] {
        let resp = post(
            &server,
            &format!("/v1/store/sessions/{sid}/publish"),
            publish_body(sid, content),
        )
        .await;
        assert_eq!(resp.status, 200, "publish {sid}");
    }
    let resp = get(&server, "/v1/store/sessions").await;
    let sessions: Vec<String> = serde_json::from_value(resp.json()["sessions"].clone()).unwrap();
    assert!(sessions.contains(&"zp.zp42".to_string()), "{sessions:?}");
    assert!(sessions.contains(&"t.tmp.1".to_string()), "{sessions:?}");
    assert!(sessions.contains(&"plain".to_string()), "{sessions:?}");
}

/// T-317 (§8/§11): the routes inherit the Router's Bearer middleware — no
/// token and a wrong token both answer 401 before reaching any handler.
#[tokio::test]
async fn t317_bearer_auth_inherited() {
    let root = tmpdir("t317");
    write_session(&root, "s1", S1_CONTENT);
    let server = serve(&root, Some("secret-token")).await;

    // No Authorization header at all — both new routes refuse at the
    // middleware, before any handler work.
    let resp = request(
        &server,
        "GET",
        "/v1/store/sessions/s1/changes",
        &[],
        None,
    )
    .await;
    assert_eq!(resp.status, 401, "unauthenticated changes");
    let resp = request(
        &server,
        "POST",
        "/v1/store/sessions/s1/publish",
        publish_body("s1", S1_CONTENT).as_slice(),
        None,
    )
    .await;
    assert_eq!(resp.status, 401, "unauthenticated publish");

    // A wrong token is refused the same way.
    let resp = request(
        &server,
        "GET",
        "/v1/store/sessions/s1/changes",
        &[],
        Some("wrong-token"),
    )
    .await;
    assert_eq!(resp.status, 401, "bad token changes");
    let resp = request(
        &server,
        "POST",
        "/v1/store/sessions/s1/publish",
        publish_body("s1", S1_CONTENT).as_slice(),
        Some("wrong-token"),
    )
    .await;
    assert_eq!(resp.status, 401, "bad token publish");

    // The right token reaches the handlers on both routes.
    let resp = get(&server, "/v1/store/sessions/s1/changes").await;
    assert_eq!(resp.status, 200);
    let resp = post(
        &server,
        "/v1/store/sessions/new1/publish",
        publish_body("new1", b"{\"a\":1}\n"),
    )
    .await;
    assert_eq!(resp.status, 200);
}
