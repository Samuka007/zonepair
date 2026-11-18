//! Apply a `changes` reply to the mirror on disk (RFC-0001-sync-protocol §6.4 / RFC-0003-zonepaird
//! §4.4). The write-order rule outranks everything: **file bytes land
//! fsync-complete before any cursor commits** — this module only touches
//! the filesystem and returns what the caller should commit; it never
//! writes `state.json` itself.
//!
//! | reply | disk path |
//! |---|---|
//! | `unchanged` | no file write; caller refreshes `lastUnchangedAt` |
//! | `append` | `local len == p.offset` pre-check → tail-write + fsync (0600) |
//! | `reset` | body → `<file>.zp<PID>` + fsync → atomic rename → dir fsync → tombstone re-check |
//!
//! A `404`/short-read never reaches this module — the API layer classifies
//! those; here only the three success verdicts exist.

use anyhow::{Context, Result};
use std::io::{self, Write};
use std::path::Path;

use crate::sync::persist::fs::{ensure_dir, fsync_dir, write_zp_atomic};
use crate::sync::persist::state::{self, Deleted};

/// The disk-relevant slice of a `changes` reply. `request_offset` is the
/// cursor byte count the request carried (`p.offset`) — the append
/// pre-check compares the local length against IT, not the next cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangesReply {
    /// Nothing moved; `next_cursor` echoes the request cursor.
    Unchanged { next_cursor: String },
    /// `body` is `[p.offset, boundary)` — appendable suffix bytes.
    Append {
        request_offset: u64,
        body: Vec<u8>,
        next_cursor: String,
    },
    /// `body` is the whole stable prefix `[0, boundary)` — start over.
    Reset { body: Vec<u8>, next_cursor: String },
}

/// What `apply` did. `Advanced` carries the next cursor the CALLER commits
/// into `pull.cursor` — committing it is the caller's transaction, this
/// module guarantees only that the bytes it names are already durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// `unchanged` — no bytes written; refresh `lastUnchangedAt` only.
    Unchanged,
    /// File updated; commit this cursor (after the fsyncs above).
    Advanced { next_cursor: String },
    /// `append` pre-check failed: `local len != p.offset` — an external
    /// writer touched the file; caller re-requests with the ZERO cursor
    /// (the only forced-reset input) instead of looping on the old one.
    LocalDiverged { local_len: u64, expected: u64 },
    /// `reset` landed but the post-rename tombstone re-check hit: the file
    /// was deleted again (a rename must not resurrect a deleted session);
    /// NO cursor commit — the caller leaves `pull` alone.
    TombstoneSuppressed,
}

/// Apply one reply for the session file at `file` (the `pull.file`
/// mirror-relative path resolved by the caller).
///
/// `tombstone` is the record's `deleted` field at apply time — the
/// post-rename re-check needs `(sid, deleted.fileId)` and the tombstone is
/// the only place that binding lives.
pub fn apply(reply: &ChangesReply, file: &Path, tombstone: Option<&Deleted>) -> Result<Outcome> {
    match reply {
        ChangesReply::Unchanged { .. } => Ok(Outcome::Unchanged),
        ChangesReply::Append {
            request_offset,
            body,
            next_cursor,
        } => apply_append(*request_offset, body, next_cursor, file, tombstone),
        ChangesReply::Reset {
            body,
            next_cursor,
        } => apply_reset(body, next_cursor, file, tombstone),
    }
}

/// `append`: ① local length must equal `p.offset` — a mismatch means the
/// local prefix can no longer prove itself (external writer); ② tail-write
/// the body + fsync (file 0600) — plus a DIR fsync when the file was just
/// created (a crash could otherwise leave the committed cursor naming a
/// file whose directory entry never landed); ③ the reset path's tombstone
/// re-check — a delete arriving mid-append must not let the write stand;
/// ④ caller commits `X-ZP-Next-Cursor`.
fn apply_append(
    request_offset: u64,
    body: &[u8],
    next_cursor: &str,
    file: &Path,
    tombstone: Option<&Deleted>,
) -> Result<Outcome> {
    let (local_len, created) = match file.metadata() {
        Ok(m) => (m.len(), false),
        Err(e) if e.kind() == io::ErrorKind::NotFound => (0, true),
        Err(e) => return Err(e).with_context(|| format!("stat {}", file.display())),
    };
    if local_len != request_offset {
        return Ok(Outcome::LocalDiverged {
            local_len,
            expected: request_offset,
        });
    }
    if let Some(dir) = file.parent() {
        ensure_dir(dir, 0o700)?;
    }
    let mut f = open_append_0600(file)?;
    f.write_all(body)
        .with_context(|| format!("appending to {}", file.display()))?;
    f.sync_all()?;
    // Pulled mirror files are 0600 (RFC-0002-a-state §3.1) — a pre-existing file
    // (e.g. SDK-helper written, umask 0644) gets tightened here; `create`
    // mode only applies on first creation.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600));
    }
    let dir = file
        .parent()
        .ok_or_else(|| anyhow::anyhow!("file {} has no parent dir", file.display()))?;
    if created {
        // The append created the file — five-step parity with reset:
        // the directory entry must fsync before the cursor names it.
        fsync_dir(dir)?;
    }
    // ③ tombstone re-check — append is create/extend semantics too, so
    // the same post-write guard applies: `fileId == null` seals the sid,
    // an instance fingerprint needs the WHOLE file's bytes.
    if let Some(outcome) = tombstone_suppressed(file, tombstone, None)? {
        return Ok(outcome);
    }
    Ok(Outcome::Advanced {
        next_cursor: next_cursor.to_string(),
    })
}

/// Post-write tombstone re-check shared by `append` and `reset` (RFC-0001-sync-protocol
/// §6.4 footnote — in the single-writer domain "recheck, then commit" is
/// airtight). `fileId == null` seals the sid: any write is a hit. An
/// instance fingerprint compares against `content` — the reset caller
/// already holds its body; append passes `None` and the file is re-read
/// (a read failure proves nothing → not a hit). A hit deletes the file,
/// dir-fsyncs, and yields `TombstoneSuppressed` so the caller skips the
/// cursor commit entirely.
fn tombstone_suppressed(
    file: &Path,
    tombstone: Option<&Deleted>,
    content: Option<&[u8]>,
) -> Result<Option<Outcome>> {
    let hit = match tombstone {
        Some(d) => match &d.file_id {
            None => true, // sealed sid — any write is a hit
            Some(id) => {
                let disk;
                let bytes = match content {
                    Some(b) => Some(b),
                    None => {
                        disk = std::fs::read(file).ok();
                        disk.as_deref()
                    }
                };
                bytes.is_some_and(|b| state::file_id_of(b) == *id)
            }
        },
        None => false,
    };
    if !hit {
        return Ok(None);
    }
    std::fs::remove_file(file)
        .with_context(|| format!("deleting tombstoned {}", file.display()))?;
    if let Some(dir) = file.parent() {
        let _ = fsync_dir(dir);
    }
    Ok(Some(Outcome::TombstoneSuppressed))
}

/// `reset`: ① body → same-dir temp `<name>.zp<PID>` + fsync; ② atomic
/// rename over the target + dir fsync; ③ **re-check the tombstone** — the
/// rename is create-semantics, and "delete this session" arriving mid-body
/// must not let the rename resurrect it: a hit deletes the file and skips
/// the cursor commit entirely (`fileId == null` seals: always a hit).
fn apply_reset(
    body: &[u8],
    next_cursor: &str,
    file: &Path,
    tombstone: Option<&Deleted>,
) -> Result<Outcome> {
    // `.zp<PID>` temp: a crashed reset's residue is swept, never
    // mistaken for a session file by the scan set.
    write_zp_atomic(file, body)?;

    // ③ tombstone re-check — the single-writer domain makes "recheck,
    // then commit" airtight (RFC-0001-sync-protocol §6.4 footnote).
    if let Some(outcome) = tombstone_suppressed(file, tombstone, Some(body))? {
        return Ok(outcome);
    }
    Ok(Outcome::Advanced {
        next_cursor: next_cursor.to_string(),
    })
}

#[cfg(unix)]
fn open_append_0600(path: &Path) -> io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_append_0600(path: &Path) -> io::Result<std::fs::File> {
    std::fs::OpenOptions::new().append(true).create(true).open(path)
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::persist::fs::testkit::tmp;
    use std::os::unix::fs::PermissionsExt;

    // T-401: append with `local len == p.offset` — tail bytes land, file
    // is 0600, outcome carries the cursor to commit.
    #[test]
    fn t401_append_tail_writes_and_advances() {
        let dir = tmp("apply", "t401");
        let file = dir.join("projects/lab/s1.jsonl");
        ensure_dir(file.parent().unwrap(), 0o700).unwrap();
        std::fs::write(&file, b"{\"a\":1}\n").unwrap();
        let reply = ChangesReply::Append {
            request_offset: 8,
            body: b"{\"b\":2}\n".to_vec(),
            next_cursor: "zpc1.15.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        };
        let out = apply(&reply, &file, None).unwrap();
        assert_eq!(
            out,
            Outcome::Advanced {
                next_cursor: "zpc1.15.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()
            }
        );
        assert_eq!(std::fs::read(&file).unwrap(), b"{\"a\":1}\n{\"b\":2}\n");
        assert_eq!(
            file.metadata().unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // T-402: append with `local len != p.offset` → LocalDiverged signal;
    // nothing is written and no cursor advances (caller re-requests zero).
    #[test]
    fn t402_append_length_mismatch_is_local_diverged() {
        let dir = tmp("apply", "t402");
        let file = dir.join("s1.jsonl");
        ensure_dir(&dir, 0o700).unwrap();
        std::fs::write(&file, b"{\"a\":1}\nEXTRA").unwrap();
        let reply = ChangesReply::Append {
            request_offset: 7,
            body: b"new\n".to_vec(),
            next_cursor: "zpc1.11.bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
        };
        let out = apply(&reply, &file, None).unwrap();
        assert_eq!(
            out,
            Outcome::LocalDiverged {
                local_len: 13,
                expected: 7
            }
        );
        // Untouched — the divergence is for the caller to reset away.
        assert_eq!(std::fs::read(&file).unwrap(), b"{\"a\":1}\nEXTRA");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // T-403: reset writes via temp+rename; a tombstone re-check hit deletes
    // the file and skips the cursor commit — `fileId:null` always hits.
    #[test]
    fn t403_reset_tombstone_recheck() {
        let dir = tmp("apply", "t403");
        let file = dir.join("s1.jsonl");
        ensure_dir(&dir, 0o700).unwrap();
        let body = b"{\"x\":1}\n".to_vec();
        let reply = ChangesReply::Reset {
            body: body.clone(),
            next_cursor: "zpc1.7.cccccccccccccccccccccccccccccccc".into(),
        };
        // Clean reset: bytes land atomically, cursor advances.
        let out = apply(&reply, &file, None).unwrap();
        assert!(matches!(out, Outcome::Advanced { .. }));
        assert_eq!(std::fs::read(&file).unwrap(), body);

        // Instance-bound tombstone matching the NEW content → hit.
        let tomb = Deleted {
            at: "t".into(),
            published_at_delete: false,
            file_id: Some(state::file_id_of(&body)),
        };
        let out = apply(&reply, &file, Some(&tomb)).unwrap();
        assert_eq!(out, Outcome::TombstoneSuppressed);
        assert!(!file.exists(), "renamed file must be deleted again");

        // Sealed tombstone (fileId null) always hits — any content.
        let sealed = Deleted {
            at: "t".into(),
            published_at_delete: false,
            file_id: None,
        };
        let out = apply(&reply, &file, Some(&sealed)).unwrap();
        assert_eq!(out, Outcome::TombstoneSuppressed);
        assert!(!file.exists());

        // Non-matching instance-bound tombstone → new instance lives.
        let other = Deleted {
            at: "t".into(),
            published_at_delete: false,
            file_id: Some(state::file_id_of(b"different")),
        };
        let out = apply(&reply, &file, Some(&other)).unwrap();
        assert!(matches!(out, Outcome::Advanced { .. }));
        assert!(file.exists());
        // No *.zp* residue left behind.
        let residue: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_str().is_some_and(|n| n.contains(".zp")))
            .collect();
        assert!(residue.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // R4-M4: the append path carries the reset path's tombstone
    // re-check — a delete committed mid-append must not let the write
    // stand. `fileId:null` seals: any append hits.
    #[test]
    fn append_tombstone_recheck() {
        let dir = tmp("apply", "append-tomb");
        let file = dir.join("s1.jsonl");
        ensure_dir(&dir, 0o700).unwrap();
        std::fs::write(&file, b"{\"a\":1}\n").unwrap();
        let reply = ChangesReply::Append {
            request_offset: 8,
            body: b"{\"b\":2}\n".to_vec(),
            next_cursor: "zpc1.16.bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
        };
        // Sealed tombstone → the just-written bytes go away, no cursor.
        let sealed = Deleted {
            at: "t".into(),
            published_at_delete: false,
            file_id: None,
        };
        let out = apply(&reply, &file, Some(&sealed)).unwrap();
        assert_eq!(out, Outcome::TombstoneSuppressed);
        assert!(!file.exists());

        // Instance tombstone matching the WHOLE post-append content → hit.
        std::fs::write(&file, b"{\"a\":1}\n").unwrap();
        let whole = b"{\"a\":1}\n{\"b\":2}\n";
        let inst = Deleted {
            at: "t".into(),
            published_at_delete: false,
            file_id: Some(state::file_id_of(whole)),
        };
        let out = apply(&reply, &file, Some(&inst)).unwrap();
        assert_eq!(out, Outcome::TombstoneSuppressed);
        assert!(!file.exists());

        // Non-matching instance → the append stands (new file instance).
        std::fs::write(&file, b"{\"a\":1}\n").unwrap();
        let other = Deleted {
            at: "t".into(),
            published_at_delete: false,
            file_id: Some(state::file_id_of(b"different")),
        };
        let out = apply(&reply, &file, Some(&other)).unwrap();
        assert!(matches!(out, Outcome::Advanced { .. }));
        assert_eq!(std::fs::read(&file).unwrap(), whole);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // T-408: unchanged writes nothing — the file stays absent and the
    // outcome tells the caller to update only `lastUnchangedAt`.
    #[test]
    fn t408_unchanged_writes_nothing() {
        let dir = tmp("apply", "t408");
        let file = dir.join("s1.jsonl");
        ensure_dir(&dir, 0o700).unwrap();
        let reply = ChangesReply::Unchanged {
            next_cursor: "zpc1.7.cccccccccccccccccccccccccccccccc".into(),
        };
        let out = apply(&reply, &file, None).unwrap();
        assert_eq!(out, Outcome::Unchanged);
        assert!(!file.exists(), "unchanged must not create the file");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
