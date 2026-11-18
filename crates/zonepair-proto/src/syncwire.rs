//! Wire types for the `changes` and `publish` operations — RFC-0001-sync-protocol §6–§7.
//!
//! `changes` answers in two shapes chosen by `Content-Type`: a JSON verdict
//! body (`unchanged`) or raw bytes whose verdict rides in `X-ZP-*` headers
//! (`append`/`reset`). `publish` is the protocol's only A→B write: the whole
//! fork product as one JSON body, byte-identity checked via `contentSha256`.
//! Serde field names here are the wire contract — `contentSha256` and
//! `nextCursor` are camelCase on the line, keep the renames exact.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Response header carrying the `changes` verdict when bytes are attached.
pub const X_ZP_STATE: &str = "X-ZP-State";
/// Response header carrying the next cursor when bytes are attached.
pub const X_ZP_NEXT_CURSOR: &str = "X-ZP-Next-Cursor";

/// The three `changes` verdicts: nothing moved / new bytes appended / your
/// prefix is void, start over. Serializes lowercase in JSON and headers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangesState {
    Unchanged,
    Append,
    Reset,
}

impl ChangesState {
    /// Wire spelling, shared by the JSON body and `X-ZP-State` header.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unchanged => "unchanged",
            Self::Append => "append",
            Self::Reset => "reset",
        }
    }
}

impl fmt::Display for ChangesState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ChangesState {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "unchanged" => Ok(Self::Unchanged),
            "append" => Ok(Self::Append),
            "reset" => Ok(Self::Reset),
            _ => Err(()),
        }
    }
}

/// JSON body of an `unchanged` `changes` reply (§6.2): the verdict plus an
/// echo of the caller's cursor so progress submission stays one shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangesBody {
    pub ok: bool,
    pub state: ChangesState,
    /// `zpc1.<offset>.<fp>` covering the prefix after the body is applied;
    /// for `unchanged` this is the request cursor echoed back.
    pub next_cursor: String,
}

/// `publish` request body (§7.1). `content` is the complete jsonl file in
/// base64 (RFC 4648, with padding); `contentSha256` is the lowercase-hex
/// SHA-256 of the decoded bytes — its first 32 chars become the fingerprint
/// segment of the confirmation cursor, so uppercase would mint a cursor the
/// next `changes` call then rejects as malformed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishRequest {
    /// Must equal the `{id}` path segment.
    pub session_id: String,
    /// Complete file bytes, base64 (standard alphabet, padded).
    pub content: String,
    /// `contentSha256`: 64 lowercase-hex chars, SHA-256 of decoded content.
    pub content_sha256: String,
}

impl PublishRequest {
    /// `contentSha256` accepts only 64 lowercase-hex chars.
    pub fn content_sha256_is_valid(&self) -> bool {
        self.content_sha256.len() == 64
            && self
                .content_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }

    /// Base64-decode `content` (RFC 4648 standard alphabet, padding required).
    pub fn decode_content(&self) -> Result<Vec<u8>, base64::DecodeError> {
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        STANDARD.decode(&self.content)
    }
}

/// `publish` reply (§7.2). Every 200 carries `cursor` — the proof that the
/// fork's prefix landed on B — so A stores it and skips a `changes` round
/// trip. `already` distinguishes a fresh publish from an idempotent replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishResponse {
    pub ok: bool,
    pub published: bool,
    pub already: bool,
    /// `zpc1.<len(content)>.<contentSha256[..32]>` — present on every 200.
    pub cursor: String,
}

/// JSON error body shared by both operations: `{"ok":false,"error":…}` plus
/// an optional `code` — `code:"missing"` is the positive signal that a 404
/// means "session absent on B" rather than "server too old" (§8).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub ok: bool,
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub code: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use sha2::{Digest, Sha256};

    const SHA: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    // Header names are part of the wire contract — pin the exact strings.
    #[test]
    fn header_names_are_exact() {
        assert_eq!(X_ZP_STATE, "X-ZP-State");
        assert_eq!(X_ZP_NEXT_CURSOR, "X-ZP-Next-Cursor");
    }

    // T-3xx proto half: PublishRequest serializes with the exact camelCase
    // field names of §7.1 and parses that shape back.
    #[test]
    fn publish_request_serde_names() {
        let req = PublishRequest {
            session_id: "s1".into(),
            content: STANDARD.encode(b"hi\n"),
            content_sha256: SHA.into(),
        };
        let v = serde_json::to_value(&req).unwrap();
        let obj = v.as_object().unwrap();
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["content", "contentSha256", "sessionId"]);
        let round: PublishRequest =
            serde_json::from_str(&serde_json::to_string(&req).unwrap()).unwrap();
        assert_eq!(round, req);
    }

    // contentSha256 validation: only 64 lowercase-hex chars pass.
    #[test]
    fn publish_request_sha_validation() {
        let req = |sha: &str| PublishRequest {
            session_id: "s".into(),
            content: "eA==".into(),
            content_sha256: sha.into(),
        };
        assert!(req(SHA).content_sha256_is_valid());
        assert!(!req(&SHA[..63]).content_sha256_is_valid()); // too short
        assert!(!req(&format!("{SHA}0")).content_sha256_is_valid()); // too long
        assert!(!req(&SHA.to_uppercase()).content_sha256_is_valid()); // uppercase
        assert!(!req(&SHA.replacen('a', "g", 1)).content_sha256_is_valid()); // non-hex
        assert!(!req("").content_sha256_is_valid());
    }

    // content is base64 RFC 4648 padded; decoder round-trips and rejects
    // non-base64 input.
    #[test]
    fn publish_content_base64_roundtrip() {
        let bytes = b"{\"a\":1}\n{\"b\":2}\n".to_vec();
        let req = PublishRequest {
            session_id: "s".into(),
            content: STANDARD.encode(&bytes),
            content_sha256: format!("{:x}", Sha256::digest(&bytes)),
        };
        assert_eq!(req.decode_content().unwrap(), bytes);
        let bad = PublishRequest {
            content: "not base64!!!".into(),
            ..req
        };
        assert!(bad.decode_content().is_err());
    }

    // ChangesState: lowercase on the wire, both directions; unknown and
    // miscased header values fail to parse.
    #[test]
    fn changes_state_wire_spelling() {
        for (state, s) in [
            (ChangesState::Unchanged, "unchanged"),
            (ChangesState::Append, "append"),
            (ChangesState::Reset, "reset"),
        ] {
            assert_eq!(serde_json::to_string(&state).unwrap(), format!("\"{s}\""));
            assert_eq!(serde_json::from_str::<ChangesState>(&format!("\"{s}\"")).unwrap(), state);
            assert_eq!(state.as_str(), s);
            assert_eq!(state.to_string(), s);
            assert_eq!(s.parse::<ChangesState>().unwrap(), state);
        }
        for s in ["APPEND", "Unchanged", "bogus", "", "reset "] {
            assert!(s.parse::<ChangesState>().is_err(), "{s}");
        }
    }

    // ChangesBody is the unchanged-shape JSON: ok/state/nextCursor camelCase.
    #[test]
    fn changes_body_shape() {
        let body = ChangesBody {
            ok: true,
            state: ChangesState::Unchanged,
            next_cursor: crate::cursor::ZERO.into(),
        };
        let v = serde_json::to_value(&body).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["state"], "unchanged");
        assert_eq!(v["nextCursor"], crate::cursor::ZERO);
        let round: ChangesBody = serde_json::from_value(v).unwrap();
        assert_eq!(round, body);
    }

    // PublishResponse carries ok/published/already/cursor exactly as §7.2.
    #[test]
    fn publish_response_shape() {
        let resp = PublishResponse {
            ok: true,
            published: true,
            already: false,
            cursor: format!("zpc1.6.{}", &SHA[..32]),
        };
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["published"], true);
        assert_eq!(v["already"], false);
        assert!(v["cursor"].as_str().unwrap().starts_with("zpc1.6."));
        let round: PublishResponse = serde_json::from_value(v).unwrap();
        assert_eq!(round, resp);
    }

    // ErrorBody: `code` appears only when set — `{"ok":false,"error":"conflict"}`
    // vs `{"ok":false,"error":…,"code":"missing"}` (§8).
    #[test]
    fn error_body_code_optional() {
        let conflict = ErrorBody {
            ok: false,
            error: "conflict".into(),
            code: None,
        };
        let v = serde_json::to_value(&conflict).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"ok": false, "error": "conflict"})
        );

        let missing = ErrorBody {
            ok: false,
            error: "session s1 not found".into(),
            code: Some("missing".into()),
        };
        assert_eq!(
            serde_json::to_value(&missing).unwrap(),
            serde_json::json!({"ok": false, "error": "session s1 not found", "code": "missing"})
        );
        // Absent `code` also parses back to None.
        let parsed: ErrorBody =
            serde_json::from_str("{\"ok\":false,\"error\":\"x\"}").unwrap();
        assert_eq!(parsed.code, None);
    }
}
