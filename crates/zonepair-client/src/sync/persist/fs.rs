//! Filesystem primitives shared by `state`, `journal` and `apply`: the
//! `.sync/` dir's IO discipline lives here so the four-step atomic write
//! exists exactly once.
//!
//! Modes are part of the contract (RFC-0002-a-state §3.1): `.sync/` dirs are 0700,
//! record and session files 0600. Non-unix platforms keep the platform
//! default — the `mode` argument is ignored there.

use anyhow::{Context, Result};
use std::io;
use std::path::Path;

/// The four-step write used by state and journal/apply temp files: create
/// temp 0600 → write all → file fsync → rename → dir fsync.
pub(crate) fn write_atomic(tmp: &Path, target: &Path, bytes: &[u8]) -> Result<()> {
    let _ = std::fs::remove_file(tmp);
    let mut file = open_new(tmp, 0o600)?;
    use std::io::Write;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(tmp, target)?;
    if let Some(dir) = target.parent() {
        fsync_dir(dir)?;
    }
    Ok(())
}

/// `write_atomic` with the `<name>.zp<PID>` same-dir temp the session-file
/// writers use: reset bodies, stub shells, and `force_empty` all stage next
/// to the target. The `.zp` marker matters — a crashed write's residue is
/// swept by startup cleanup and never enters the `projects/**` scan set.
/// `file`'s parent is created (0700) first; a failed write unlinks the temp.
pub(crate) fn write_zp_atomic(file: &Path, bytes: &[u8]) -> Result<()> {
    let dir = file
        .parent()
        .ok_or_else(|| anyhow::anyhow!("file {} has no parent dir", file.display()))?;
    ensure_dir(dir, 0o700)?;
    let name = file
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("non-UTF8 file name {}", file.display()))?;
    let tmp = dir.join(format!("{name}.zp{}", std::process::id()));
    let result = write_atomic(&tmp, file, bytes);
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// fsync a directory entry (rename durability needs the dir, not just data).
pub(crate) fn fsync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("fsync dir {}", dir.display()))
}

/// Create `path` fresh with the given unix mode (0600 records, 0700 dirs
/// are set by `ensure_dir`); non-unix keeps the platform default.
#[cfg(unix)]
pub(crate) fn open_new(path: &Path, mode: u32) -> io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)
}

#[cfg(not(unix))]
pub(crate) fn open_new(path: &Path, _mode: u32) -> io::Result<std::fs::File> {
    std::fs::OpenOptions::new().write(true).create_new(true).open(path)
}

#[cfg(unix)]
pub(crate) fn ensure_dir(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(mode)
        .create(path)
        .with_context(|| format!("creating {}", path.display()))
}

#[cfg(not(unix))]
pub(crate) fn ensure_dir(path: &Path, _mode: u32) -> Result<()> {
    std::fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))
}

/// Every `<sid>.jsonl` under `projects/**`, sid → path, `*.zp*` excluded —
/// the E7 appearance edge's scan set. `*.zp*` temp names never carry a
/// `.jsonl` suffix; the filter additionally rejects names like
/// `a.zp1.jsonl` planted by a crashed writer. Duplicate sids keep the
/// lexicographic first hit (same rule as the server-side `locate`).
pub(crate) fn scan_session_files(mirror: &Path) -> Vec<(String, std::path::PathBuf)> {
    let root = mirror.join("projects");
    let mut stack = vec![root];
    let mut out: Vec<(String, std::path::PathBuf)> = Vec::new();
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(sid) = name.strip_suffix(".jsonl") else {
                continue;
            };
            if name.contains(".zp") || sid.is_empty() {
                continue;
            }
            out.push((sid.to_owned(), path));
        }
    }
    out.sort();
    let mut seen = std::collections::HashSet::new();
    out.retain(|(sid, _)| seen.insert(sid.clone()));
    out
}

#[cfg(test)]
pub(crate) mod testkit {
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::sync::engine::shared::Shared;
    use crate::sync::persist::state::{Pull, SessionRecord};

    /// Per-module fresh temp dir, named by the caller — the identical
    /// `tmp()` helper every sync test module used to carry.
    pub(crate) fn tmp(prefix: &str, name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("zonepair-{prefix}-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Write `bytes` at `mirror.join(rel)`, creating parents — the
    /// identical `write_file` helper worker and wait tests used to carry.
    pub(crate) fn write_file(mirror: &std::path::Path, rel: &str, bytes: &[u8]) {
        let p = mirror.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, bytes).unwrap();
    }

    /// Fresh mirror + opened `Shared` (corruption asserted away) — the
    /// `setup()` body materialize/wait/worker tests each carried.
    pub(crate) fn setup_shared(prefix: &str, name: &str) -> (PathBuf, Arc<Shared>) {
        let mirror = tmp(prefix, name);
        let (shared, corruption) = Shared::open(&mirror).unwrap();
        assert!(corruption.is_none());
        (mirror, shared)
    }

    /// A record carrying `pull` toward `projects/lab/{sid}.jsonl` — the
    /// canonical pulled-session shape every record-building test seeds.
    pub(crate) fn pull_rec(sid: &str, cursor: &str) -> SessionRecord {
        SessionRecord {
            pull: Some(Pull {
                cursor: cursor.into(),
                file: format!("projects/lab/{sid}.jsonl"),
            }),
            ..Default::default()
        }
    }

    /// Insert `rec` under `sid` and commit — the single-record seed.
    pub(crate) fn commit_rec(shared: &Shared, sid: &str, rec: SessionRecord) {
        shared
            .commit_state(|st| {
                st.sessions.insert(sid.into(), rec);
            })
            .unwrap();
    }
}
