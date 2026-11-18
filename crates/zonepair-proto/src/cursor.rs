//! Cursor (`zpc1.<offset>.<fp>`) codec — RFC-0001-sync-protocol §5.
//!
//! A cursor is the opaque progress token B mints and A holds verbatim: one
//! string carrying both where to resume (byte offset) and a consistency proof
//! (fingerprint of `[0, offset)`). This module is the only implementation of
//! the encoding; both machines share it so the wire format cannot drift.
//!
//! Segment rules: the literal version tag `zpc1`, a canonical decimal u64
//! byte count, and a 32-char lowercase-hex fingerprint (SHA-256 truncated to
//! `PREFIX_HASH_LEN` bytes). Corruption (`400`) and divergence (`reset`) are
//! distinct server verdicts — parse failures must not masquerade as a first
//! pull, and `zpc2.…` must surface as "unknown version", not "malformed".

use sha2::{Digest, Sha256};
use std::fmt;
use std::str::FromStr;

/// Byte length of the prefix fingerprint (SHA-256 truncated to 16 bytes,
/// rendered as 32 lowercase hex chars in the cursor's third segment).
pub const PREFIX_HASH_LEN: usize = 16;

/// The canonical zero-value cursor string: `zpc1.0.` + 32 × `'0'`.
pub const ZERO: &str = "zpc1.0.00000000000000000000000000000000";

/// A parsed `zpc1` cursor: byte offset `N` plus the fingerprint of `[0, N)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cursor {
    /// Byte count this cursor covers: it attests the prefix `[0, offset)`.
    pub offset: u64,
    /// Truncated SHA-256 of those `offset` bytes (`PREFIX_HASH_LEN` bytes).
    pub prefix_hash: [u8; PREFIX_HASH_LEN],
}

impl Cursor {
    /// Build a cursor for a prefix whose offset and fingerprint are known
    /// (e.g. server-side `changes`/`publish` after hashing the prefix).
    pub fn new(offset: u64, prefix_hash: [u8; PREFIX_HASH_LEN]) -> Self {
        Self {
            offset,
            prefix_hash,
        }
    }

    /// `true` when this is the zero-value cursor (`zpc1.0.` + 32 × `'0'`),
    /// the canonical spelling of "first pull".
    pub fn is_zero(&self) -> bool {
        self.offset == 0 && self.prefix_hash == [0; PREFIX_HASH_LEN]
    }

    /// Serialize back to `zpc1.<offset>.<32 lowercase hex>`.
    pub fn encode(&self) -> String {
        let mut s = String::with_capacity(4 + 1 + 20 + 1 + 32);
        s.push_str("zpc1.");
        s.push_str(&self.offset.to_string());
        s.push('.');
        s.push_str(&hex_encode(&self.prefix_hash));
        s
    }
}

/// Why a cursor string failed to parse. The two variants map to distinct
/// server 400 wording: an unknown version tag vs a malformed `zpc1` payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CursorError {
    /// First segment is present but isn't `zpc1` (e.g. `zpc2.…`).
    UnknownVersion,
    /// Wrong segment count, bad offset, or bad fingerprint.
    Malformed,
}

impl fmt::Display for CursorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVersion => f.write_str("unknown cursor version"),
            Self::Malformed => f.write_str("malformed cursor"),
        }
    }
}

impl std::error::Error for CursorError {}

/// Parse a `zpc1.<offset>.<fp>` string. `zpc1` is the only version this
/// module speaks; anything else is `UnknownVersion` so the server can say
/// so in its 400 instead of lumping it with format corruption.
pub fn parse(s: &str) -> Result<Cursor, CursorError> {
    s.parse()
}

impl FromStr for Cursor {
    type Err = CursorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('.');
        let (Some(version), Some(offset), Some(fp), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(CursorError::Malformed);
        };
        if version != "zpc1" {
            return Err(if version.starts_with("zpc") {
                CursorError::UnknownVersion
            } else {
                CursorError::Malformed
            });
        }
        // Canonical decimal only: all digits, no sign, no leading zeros —
        // `zpc1.007.…` is a different string that must not silently pass.
        if offset.is_empty()
            || !offset.bytes().all(|b| b.is_ascii_digit())
            || (offset.len() > 1 && offset.starts_with('0'))
        {
            return Err(CursorError::Malformed);
        }
        let offset = offset.parse::<u64>().map_err(|_| CursorError::Malformed)?;
        if fp.len() != PREFIX_HASH_LEN * 2 {
            return Err(CursorError::Malformed);
        }
        let mut prefix_hash = [0u8; PREFIX_HASH_LEN];
        for (i, pair) in fp.as_bytes().chunks_exact(2).enumerate() {
            let hi = nibble(pair[0]).ok_or(CursorError::Malformed)?;
            let lo = nibble(pair[1]).ok_or(CursorError::Malformed)?;
            prefix_hash[i] = (hi << 4) | lo;
        }
        Ok(Cursor {
            offset,
            prefix_hash,
        })
    }
}

/// Lowercase-hex nibble; `None` for uppercase or non-hex — §5.2 accepts only
/// lowercase because `contentSha256`'s first 32 chars flow into this segment.
fn nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

/// Lowercase-hex rendering — the cursor fingerprint segment's encoding,
/// shared by every `<hex>`-carrying field (`deleted.fileId`'s `sha256:`
/// suffix uses the same spelling).
pub fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
    }
    s
}

/// Fingerprint of a byte prefix: SHA-256 truncated to `PREFIX_HASH_LEN`.
/// This is the protocol's only consistency criterion — length, mtime and
/// inode are never trusted because B has out-of-band writers.
pub fn prefix_hash(bytes: &[u8]) -> [u8; PREFIX_HASH_LEN] {
    let digest = Sha256::digest(bytes);
    let mut out = [0u8; PREFIX_HASH_LEN];
    out.copy_from_slice(&digest[..PREFIX_HASH_LEN]);
    out
}

/// First-pull equivalence class: cursor parameter absent (`None`), empty,
/// or the zero-value cursor all mean "give me the whole stable prefix"
/// (§6.3 rule 1). A present-but-corrupt string is `false` — that path must
/// reach `parse` and fail as 400, never degrade into a silent reset.
pub fn is_first_pull(param: Option<&str>) -> bool {
    match param {
        None => true,
        Some("") => true,
        Some(s) => parse(s).map(|c| c.is_zero()).unwrap_or(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // T-101: a valid cursor parses into (offset, fp) and re-encodes to the
    // exact same string.
    #[test]
    fn t101_valid_cursor_roundtrips() {
        let s = "zpc1.34567.d3c9f1a0e2b48c01a7f6e5d49b83021c";
        let c = parse(s).expect("valid cursor");
        assert_eq!(c.offset, 34567);
        let mut fp = [0u8; PREFIX_HASH_LEN];
        for (i, pair) in "d3c9f1a0e2b48c01a7f6e5d49b83021c"
            .as_bytes()
            .chunks_exact(2)
            .enumerate()
        {
            fp[i] = u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap();
        }
        assert_eq!(c.prefix_hash, fp);
        assert_eq!(c.encode(), s);
    }

    // T-101 (extremes): offset 0 and u64::MAX both roundtrip.
    #[test]
    fn t101_offset_extremes_roundtrip() {
        for off in [0u64, 1, u64::MAX] {
            let c = Cursor::new(off, [0xab; PREFIX_HASH_LEN]);
            let parsed = parse(&c.encode()).unwrap();
            assert_eq!(parsed, c);
        }
    }

    // T-102: `zpc2.…` is UnknownVersion — distinguishable from Malformed so
    // the server's 400 can name the failure — and a missing `zpc` prefix is
    // Malformed. Neither may degrade into a first pull.
    #[test]
    fn t102_unknown_version_vs_malformed() {
        assert_eq!(
            parse("zpc2.34567.d3c9f1a0e2b48c01a7f6e5d49b83021c"),
            Err(CursorError::UnknownVersion)
        );
        assert_eq!(
            parse("34567.d3c9f1a0e2b48c01a7f6e5d49b83021c"),
            Err(CursorError::Malformed)
        );
        assert_eq!(parse(""), Err(CursorError::Malformed));
        assert_eq!(parse("garbage"), Err(CursorError::Malformed));
    }

    // T-103: every malformed shape fails — non-numeric / overflowing /
    // negative / zero-padded offsets, wrong-length / uppercase / non-hex
    // fingerprints, wrong segment counts.
    #[test]
    fn t103_malformed_offsets() {
        let fp = "d3c9f1a0e2b48c01a7f6e5d49b83021c";
        for s in [
            "zpc1.abc.".to_string() + fp,
            "zpc1..".to_string() + fp,
            "zpc1.-5.".to_string() + fp,
            "zpc1.+5.".to_string() + fp,
            "zpc1.007.".to_string() + fp,
            "zpc1. 5.".to_string() + fp,
            "zpc1.99999999999999999999.".to_string() + fp, // > u64::MAX
        ] {
            assert_eq!(parse(&s), Err(CursorError::Malformed), "{s}");
        }
    }

    #[test]
    fn t103_malformed_fingerprints() {
        let good = "d3c9f1a0e2b48c01a7f6e5d49b83021c";
        for fp in [
            &good[..31],                              // too short
            "d3c9f1a0e2b48c01a7f6e5d49b83021cd",       // too long
            "D3C9F1A0E2B48C01A7F6E5D49B83021C",       // uppercase
            "d3c9f1a0e2b48c01a7f6e5d49b83021g",       // non-hex
            "",                                       // empty
        ] {
            let s = format!("zpc1.34567.{fp}");
            assert_eq!(parse(&s), Err(CursorError::Malformed), "{s}");
        }
    }

    #[test]
    fn t103_wrong_segment_count() {
        for s in [
            "zpc1",
            "zpc1.34567",
            "zpc1.34567.",
            "zpc1.34567.d3c9f1a0e2b48c01a7f6e5d49b83021c.extra",
            "zpc1.34567.d3c9f1a0e2b48c01a7f6e5d49b83021c.",
        ] {
            assert_eq!(parse(s), Err(CursorError::Malformed), "{s}");
        }
    }

    // T-105: absent, empty and zero-value inputs are one equivalence class —
    // the first pull. A non-zero cursor and a corrupt cursor are not.
    #[test]
    fn t105_first_pull_equivalence_class() {
        assert!(is_first_pull(None));
        assert!(is_first_pull(Some("")));
        assert!(is_first_pull(Some(ZERO)));
        assert!(parse(ZERO).unwrap().is_zero());
        assert_eq!(parse(ZERO).unwrap().encode(), ZERO);

        assert!(!is_first_pull(Some(
            "zpc1.34567.d3c9f1a0e2b48c01a7f6e5d49b83021c"
        )));
        // Corrupt input is NOT a first pull — it must surface as 400.
        assert!(!is_first_pull(Some("zpc2.0.00000000000000000000000000000000")));
        assert!(!is_first_pull(Some("nonsense")));
    }

    // Zero-value detection requires offset==0 AND fp==0.
    #[test]
    fn t105_zero_requires_both_segments() {
        assert!(!Cursor::new(0, [0x11; PREFIX_HASH_LEN]).is_zero());
        assert!(!Cursor::new(1, [0; PREFIX_HASH_LEN]).is_zero());
        assert!(Cursor::new(0, [0; PREFIX_HASH_LEN]).is_zero());
    }

    // prefix_hash: SHA-256("abc") truncated to 16 bytes — computed vector.
    #[test]
    fn prefix_hash_matches_sha256_truncation() {
        // SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        let expected: [u8; PREFIX_HASH_LEN] = [
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23,
        ];
        assert_eq!(prefix_hash(b"abc"), expected);
        assert_eq!(prefix_hash(b""), prefix_hash(b""));
        assert_ne!(prefix_hash(b"abc"), prefix_hash(b"abd"));
    }
}
