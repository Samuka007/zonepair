//! Session-store projection on the server side (machine B).
//!
//! Format assumption (documented in the README): the store is a directory tree
//! of `<sessionID>.jsonl` files, each line a JSON object with at least
//! `uuid`, `sessionId`, optional `parentUuid`, plus arbitrary payload fields
//! (`type`, `message`, ...). Fork semantics mirror the Claude Agent SDK:
//! copy the prefix up to and including `upTo`, assign a fresh session id and
//! fresh uuids to every copied line, and rewrite the parentUuid chain.
//!
//! Production note: for a real deployment the fork should be performed by the
//! Agent SDK itself on machine B rather than by this re-implementation; the
//! RPC surface exists so machine A never needs filesystem access to B.
//!
//! RFC-0004-b-endpoints revision: at the file level the store is a byte projection, not
//! JSON. Read paths (`exists`/`messages`/`list`, plus the changes-pipeline
//! primitives) take no lock and work on one open-fd snapshot; write paths
//! (`merge`/`fork`/`delete`, and publish's check-and-create) hold one global
//! mutex and never write a live path in place — every content write is a
//! same-directory temp file + fsync + atomic rename + dir fsync, so a
//! concurrent reader's pinned fd only ever observes the old file or the new
//! file, never a torn mix.

use anyhow::{bail, Context, Result};
use parking_lot::Mutex;
use zonepair_proto::cursor::prefix_hash;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Write};
#[cfg(unix)]
use std::os::unix::fs::{FileExt, MetadataExt};
#[cfg(windows)]
use std::os::windows::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

/// `(ino, size, mtime_ns, ctime_ns)` from platform metadata. Unix maps the
/// stat tuple directly; Windows substitutes `file_index` for `ino`, derives
/// both timestamps from the FILETIME pair (creation stands in for ctime —
/// NTFS has no inode-change clock), and reports file identity at the same
/// granularity the fingerprint slot needs.
#[cfg(unix)]
fn file_stat(m: &std::fs::Metadata) -> (u64, u64, i64, i64) {
    (
        m.ino(),
        m.size(),
        m.mtime() * 1_000_000_000 + m.mtime_nsec(),
        m.ctime() * 1_000_000_000 + m.ctime_nsec(),
    )
}

#[cfg(windows)]
fn file_stat(m: &std::fs::Metadata) -> (u64, u64, i64, i64) {
    // FILETIME counts 100-ns ticks since 1601-01-01.
    const EPOCH_TICKS: i64 = 116_444_736_000_000_000;
    let to_ns = |t: u64| (t as i64 - EPOCH_TICKS) * 100;
    (
        // Stable std exposes no inode on windows (file_index is unstable),
        // so identity narrows to (size, mtime, ctime) — a write still moves
        // last_write_time, which is what the snapshot actually detects.
        0,
        m.file_size(),
        to_ns(m.last_write_time()),
        to_ns(m.creation_time()),
    )
}

/// Positional read: `pread` on unix, `seek_read` on windows.
#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], pos: u64) -> io::Result<usize> {
    file.read_at(buf, pos)
}

#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], pos: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt as _;
    file.seek_read(buf, pos)
}
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// Tail-window size for the `stable_boundary` backscan (RFC-0004-b-endpoints §5).
const BOUNDARY_WINDOW: u64 = 64 * 1024;

/// Fingerprint-cache capacity in sessions (RFC-0004-b-endpoints §6.2).
const FP_CACHE_CAP: usize = 256;

/// Chunk size for `pread` loops.
const PREAD_CHUNK: usize = 64 * 1024;

pub struct Store {
    root: PathBuf,
    /// Global write lock (merge/fork/delete, and publish's check-and-create).
    /// `parking_lot` rather than `tokio::sync::Mutex`: every critical section
    /// is synchronous filesystem work with no `.await` inside, and a panic
    /// mid-write must not poison the write path forever (RFC-0004-b-endpoints §11).
    lock: Mutex<()>,
    /// Per-session fingerprint slots (RFC-0004-b-endpoints §6.2). The map mutex is only
    /// ever held for lookup/insert — never across file IO, so one slow
    /// recompute cannot stall every other session's cache path (§6.3).
    fp: Mutex<FpCache>,
}

pub struct ForkOut {
    pub session_id: String,
    /// (old uuid, new uuid) in prefix order.
    pub pairs: Vec<(String, String)>,
    pub messages: Vec<Value>,
}

pub struct MergeOut {
    pub added: usize,
    pub total: usize,
}

/// `fstat` snapshot of an open session file (RFC-0004-b-endpoints §7.2).
///
/// `(ino, size, mtime_ns, ctime_ns)` doubles as the fingerprint-slot
/// revalidation tuple (§6.2): any byte write moves `ctime_ns`, which `utimes`
/// cannot forge. `(size, mtime_ns)` alone is the §4.1 before-response
/// recheck pair the changes handler compares against its request-open value.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatSnap {
    pub ino: u64,
    pub size: u64,
    /// st_mtime in nanoseconds since the epoch (sec*1e9 + nsec).
    pub mtime_ns: i64,
    /// st_ctime in nanoseconds since the epoch.
    pub ctime_ns: i64,
}

/// Outcome of a ranged `pread` (RFC-0004-b-endpoints §7.6).
///
/// `Short` means the requested span ran past the file's real end — the only
/// way that happens on a pinned fd is an in-place truncation, so the caller
/// must treat it exactly like an fstat-recheck mismatch: discard the whole
/// round and retry from `locate` (§4.1). `available` is reported rather than
/// discarded so the caller can log/measure; it is never a usable payload.
#[doc(hidden)]
#[derive(Debug)]
pub enum ReadSpan {
    Full(Vec<u8>),
    Short { available: Vec<u8> },
}

/// One remembered `(offset, hash)` plus the fstat tuple that produced it.
/// "Single slot" per RFC-0004-b-endpoints §6.2: only the most recent nextCursor pair is
/// kept — the wire's steady state (`off == slot.offset`) hits O(1), and a
/// rare off-slot cursor just pays one from-scratch recompute.
struct FpSlot {
    ino: u64,
    size: u64,
    mtime_ns: i64,
    ctime_ns: i64,
    /// Byte count the last minted nextCursor covered (the boundary then).
    offset: u64,
    /// Fingerprint of `[0, offset)` — final value, not continuable state.
    hash: [u8; 16],
    /// LRU clock.
    tick: u64,
}

/// `Mutex<LruMap<sid → Slot>>` from RFC-0004-b-endpoints §6.2, hand-rolled over a
/// HashMap + monotonically increasing tick (no `lru` crate per tech-spec
/// §5's third-party freeze). Eviction is an O(cap) min-tick scan on insert
/// only; cap is 256 sessions.
#[derive(Default)]
struct FpCache {
    map: HashMap<String, FpSlot>,
    tick: u64,
}

impl FpCache {
    /// A hit asserts "this fstat tuple still describes the file instance the
    /// slot was computed from" — wrong belief costs one recompute at most,
    /// never a wrong verdict (§6.4). A stale slot is dropped on sight.
    /// Hits refresh the tick — the cache is LRU (§6.2), not FIFO.
    fn lookup(&mut self, sid: &str, st: &StatSnap) -> Option<(u64, [u8; 16])> {
        match self.map.get_mut(sid) {
            Some(slot)
                if slot.ino == st.ino
                    && slot.size == st.size
                    && slot.mtime_ns == st.mtime_ns
                    && slot.ctime_ns == st.ctime_ns =>
            {
                self.tick += 1;
                slot.tick = self.tick;
                Some((slot.offset, slot.hash))
            }
            Some(_) => {
                self.map.remove(sid);
                None
            }
            None => None,
        }
    }

    fn store(&mut self, sid: &str, st: &StatSnap, offset: u64, hash: [u8; 16]) {
        self.tick += 1;
        let tick = self.tick;
        if !self.map.contains_key(sid) && self.map.len() >= FP_CACHE_CAP {
            if let Some(oldest) = self
                .map
                .iter()
                .min_by_key(|(_, slot)| slot.tick)
                .map(|(key, _)| key.clone())
            {
                self.map.remove(&oldest);
            }
        }
        self.map.insert(
            sid.to_string(),
            FpSlot {
                ino: st.ino,
                size: st.size,
                mtime_ns: st.mtime_ns,
                ctime_ns: st.ctime_ns,
                offset,
                hash,
                tick,
            },
        );
    }
}

impl Store {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            lock: Mutex::new(()),
            fp: Mutex::new(FpCache::default()),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Path-traversal defence (RFC-0004-b-endpoints §4.2): a session id must be a bare
    /// file stem — empty, `/`, `..` or NUL are refused outright before any
    /// id reaches a `dir.join()` call.
    fn validate_sid(session: &str) -> Result<()> {
        if session.is_empty()
            || session.contains('/')
            || session.contains("..")
            || session.contains('\0')
        {
            bail!("invalid session id {session:?}");
        }
        Ok(())
    }

    /// Exact-name recursive scan for `<session>.jsonl` under the root,
    /// returning every regular-file hit ordered so `hits[0]` is the winner
    /// every consumer takes: lexicographic by path, then — when more than
    /// one hit exists — re-ranked by newest mtime (path order is only the
    /// tie-break; a hit whose metadata fails, e.g. deleted mid-scan, sorts
    /// last).
    ///
    /// `DirEntry::file_type` does not follow symlinks, so symlinked
    /// directories are never descended (a self-referencing link cannot loop
    /// us) and symlinks named like a session file never match — only regular
    /// files answer (§4.2). Crash-residue `*.tmp.*`/`*.zp*` staging names can
    /// never equal the exact target name, so they need no explicit filter
    /// here; `list` applies the glob filter on its own stem collection.
    ///
    /// Duplicate `<sid>.jsonl` files under different subtrees are an
    /// abnormal-but-real state (§4.2): claude writes one `<sid>.jsonl` per
    /// cwd-mangled project dir, so resuming the same session from a second
    /// directory leaves same-sid files in two dirs — the live copy is the
    /// one still being appended, hence the newest mtime wins.
    fn scan(&self, session: &str) -> Vec<PathBuf> {
        let target = OsString::from(format!("{session}.jsonl"));
        let mut hits = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let entries = match fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            for entry in entries.flatten() {
                let Ok(ft) = entry.file_type() else {
                    continue;
                };
                if ft.is_dir() {
                    stack.push(entry.path());
                } else if ft.is_file() && entry.file_name() == target {
                    hits.push(entry.path());
                }
            }
        }
        hits.sort();
        if hits.len() > 1 {
            // Newest mtime wins; the pre-sort's lexicographic path order is
            // the tie-break, and a hit whose metadata fails (deleted
            // mid-scan) sorts last — None < Some(_).
            let mtime = |p: &PathBuf| fs::metadata(p).and_then(|m| m.modified()).ok();
            hits.sort_by(|a, b| mtime(b).cmp(&mtime(a)).then_with(|| a.cmp(b)));
            eprintln!(
                "store: duplicate {session}.jsonl hits; using newest-mtime winner {}",
                hits[0].display()
            );
        }
        hits
    }

    fn find_sync(&self, session: &str) -> Option<PathBuf> {
        self.scan(session).into_iter().next()
    }

    fn require(&self, session: &str) -> Result<PathBuf> {
        self.find_sync(session)
            .with_context(|| format!("session {session} not found under {}", self.root.display()))
    }

    fn read_lines(path: &Path) -> Result<Vec<Value>> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        Self::parse_lines(&raw)
    }

    fn parse_lines(raw: &str) -> Result<Vec<Value>> {
        raw.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).context("store line is not JSON"))
            .collect()
    }

    /// RFC-0004-b-endpoints §8.1: never truncate the live file — write a same-directory
    /// `*.tmp.<pid>.<seq>` file, fsync it, atomically rename over the target,
    /// then fsync the directory. Atomic rename is what makes lock-free reads
    /// sound: a reader's open fd can only ever see the old file or the new
    /// file. Declared side effect: merge/fork now swap inodes, which only
    /// shows up in the cache slot's revalidation fields (§6.2).
    fn write_lines(path: &Path, lines: &[Value]) -> Result<()> {
        let mut out = String::new();
        for line in lines {
            out.push_str(&line.to_string());
            out.push('\n');
        }
        let dir = path.parent().unwrap_or_else(|| Path::new("."));
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("store path has no utf-8 file name")?;
        let tmp = dir.join(tmp_name(name));
        let result = (|| -> Result<()> {
            write_tmp(&tmp, out.as_bytes())?;
            fs::rename(&tmp, path)
                .with_context(|| format!("renaming {}", tmp.display()))?;
            fsync_dir(dir)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&tmp);
        }
        result
    }

    // ---- read path: lock-free, fd-pinned (RFC-0004-b-endpoints §4–§7) ------------------

    pub async fn exists(&self, session: &str) -> bool {
        self.find_sync(session).is_some()
    }

    /// Lock-free full read pinned on one open fd: a concurrent atomic rename
    /// can only place the locate before or after the swap — the bytes read
    /// are always a complete old or complete new file.
    pub async fn messages(&self, session: &str) -> Result<Vec<Value>> {
        let Some(file) = self.locate(session)? else {
            bail!("session {session} not found under {}", self.root.display())
        };
        let bytes = pread_collect(&file, 0, u64::MAX)?;
        let raw = String::from_utf8(bytes).context("store file is not UTF-8")?;
        Self::parse_lines(&raw)
    }

    /// All session ids found under the root (file stems of *.jsonl),
    /// excluding `*.tmp.*`/`*.zp*` residue so crash leftovers never surface
    /// as sessions. Lock-free: directory enumeration is rename-safe.
    pub async fn list(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let entries = match fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            for entry in entries.flatten() {
                let Ok(ft) = entry.file_type() else {
                    continue;
                };
                if ft.is_dir() {
                    stack.push(entry.path());
                    continue;
                }
                if !ft.is_file() {
                    continue; // symlinks and oddities are not sessions
                }
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                if !name.ends_with(".jsonl") || is_temp_name(name) {
                    continue;
                }
                out.push(name[..name.len() - ".jsonl".len()].to_string());
            }
        }
        out.sort();
        Ok(out)
    }

    // ---- changes-pipeline primitives (RFC-0004-b-endpoints §7) ------------------------
    //
    // These are #[doc(hidden)] internals: the only public surface of the sync
    // protocol is the pair of HTTP endpoints — callers learn `changes` and
    // `publish`, not locate/fingerprint mechanics. They are `pub` only so the
    // integration tests in tests/store.rs and the T-04 sync handler can reach
    // them.

    /// RFC-0004-b-endpoints §4.2: resolve a session id to an OPEN FILE, not a path —
    /// pinning locate, fstat and reads on one file snapshot so a concurrent
    /// atomic rename can only be observed before or after the open, never
    /// torn. `Ok(None)` is a clean miss (including losing the open race to a
    /// delete/rename); an id with `/`, `..` or empty is refused outright.
    #[doc(hidden)]
    pub fn locate(&self, session: &str) -> Result<Option<File>> {
        Self::validate_sid(session)?;
        let Some(path) = self.scan(session).into_iter().next() else {
            return Ok(None);
        };
        match File::open(&path) {
            Ok(file) => Ok(Some(file)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("opening {}", path.display())),
        }
    }

    /// RFC-0004-b-endpoints §7.2: file identity of the pinned snapshot — see StatSnap.
    #[doc(hidden)]
    pub fn fstat(file: &File) -> Result<StatSnap> {
        let m = file.metadata().context("fstat session file")?;
        let (ino, size, mtime_ns, ctime_ns) = file_stat(&m);
        Ok(StatSnap {
            ino,
            size,
            mtime_ns,
            ctime_ns,
        })
    }

    /// RFC-0004-b-endpoints §5: offset just past the last `\n` in the file. A
    /// `\n`-terminated file yields `size`; an unterminated tail is excluded
    /// (a half-written line never crosses the boundary); a file with no `\n`
    /// or an empty file yields 0. Scans the last 64KiB window first and
    /// pages back toward the head on a miss. `\r\n` endings need no special
    /// case — only the `\n` byte is searched.
    ///
    /// The RFC's incremental-rescan clause (only scan `[S, size')` when a
    /// size-S scan found no newline) is a cost clause for pathological
    /// newline-less growth, not correctness; the fingerprint slot already
    /// makes the steady-state case O(1), so this implementation always
    /// backscans.
    #[doc(hidden)]
    pub fn stable_boundary(file: &File, size: u64) -> Result<u64> {
        let mut end = size;
        while end > 0 {
            let start = end.saturating_sub(BOUNDARY_WINDOW);
            let window = pread_collect(file, start, end - start)?;
            if let Some(i) = window.iter().rposition(|&b| b == b'\n') {
                return Ok(start + i as u64 + 1);
            }
            end = start;
        }
        Ok(0)
    }

    /// RFC-0004-b-endpoints §7.6: `pread` `[offset, boundary)` on the pinned fd. `Short`
    /// is the distinguishable truncation signal — see ReadSpan.
    #[doc(hidden)]
    pub fn read_range(file: &File, offset: u64, boundary: u64) -> Result<ReadSpan> {
        debug_assert!(boundary >= offset);
        let buf = pread_collect(file, offset, boundary.saturating_sub(offset))?;
        if buf.len() as u64 == boundary.saturating_sub(offset) {
            Ok(ReadSpan::Full(buf))
        } else {
            Ok(ReadSpan::Short { available: buf })
        }
    }

    /// RFC-0004-b-endpoints §6.3 verify path: fingerprint of `[0, offset)` on the pinned
    /// fd. A slot hit whose `(ino,size,mtime_ns,ctime_ns)` tuple still
    /// verifies *and* `offset == slot.offset` answers O(1) with zero reads —
    /// that is the steady-state `unchanged` poll. Every other case
    /// recomputes from byte 0; the slot is never written here (only cursor
    /// generation updates it).
    ///
    /// The verdict never depends on the cache: on a miss this is always the
    /// hash of bytes actually read, so a mid-computation truncation can only
    /// yield a *mismatch* (→ reset), never a false match.
    #[doc(hidden)]
    pub fn prefix_hash(
        &self,
        session: &str,
        file: &File,
        st: &StatSnap,
        offset: u64,
    ) -> Result<[u8; 16]> {
        {
            let mut cache = self.fp.lock();
            if let Some((slot_off, slot_hash)) = cache.lookup(session, st) {
                if slot_off == offset {
                    return Ok(slot_hash);
                }
            }
        }
        let bytes = pread_collect(file, 0, offset)?;
        Ok(prefix_hash(&bytes))
    }

    /// RFC-0004-b-endpoints §6.3 generate path: fingerprint of `[0, offset)` for a
    /// nextCursor being minted. Recomputes unconditionally and seals the
    /// session's single slot so the next poll's `off == slot.offset` check
    /// stays O(1).
    #[doc(hidden)]
    pub fn seal_prefix(
        &self,
        session: &str,
        file: &File,
        st: &StatSnap,
        offset: u64,
    ) -> Result<[u8; 16]> {
        let bytes = pread_collect(file, 0, offset)?;
        let hash = prefix_hash(&bytes);
        self.fp.lock().store(session, st, offset, hash);
        Ok(hash)
    }

    /// RFC-0004-b-endpoints §5 cost clause ① + §6.3: a verified slot means the file
    /// bytes did not change, so the stable boundary may be reused as
    /// `slot.offset` without rescanning the tail — the true steady-state
    /// `unchanged` cost.
    #[doc(hidden)]
    pub fn cached_cursor(&self, session: &str, st: &StatSnap) -> Option<(u64, [u8; 16])> {
        self.fp.lock().lookup(session, st)
    }

    /// RFC-0004-b-endpoints §9.1 (the primitive T-04's publish handler will call):
    /// durably create `<dir>/<sid>.jsonl` with `content`, refusing to
    /// clobber. A same-directory temp file is written and fsynced first,
    /// then hard-linked to the target name — a bare rename would silently
    /// overwrite an external creator (claude self-creating a session, an
    /// operator dropping a file) that landed inside the fsync window. On
    /// `EEXIST` the temp is unlinked and `Ok(false)` returns so the caller
    /// falls through to the §9.2 sha comparison instead. Success finishes
    /// with a directory fsync; leftover `*.tmp.<pid>.<seq>` names are pure
    /// garbage that `locate`'s exact match never sees.
    #[doc(hidden)]
    pub fn create_exclusive(&self, dir: &Path, session: &str, content: &[u8]) -> Result<bool> {
        Self::validate_sid(session)?;
        fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let name = format!("{session}.jsonl");
        let tmp = dir.join(tmp_name(&name));
        let target = dir.join(&name);
        let result = (|| -> Result<bool> {
            write_tmp(&tmp, content)?;
            match fs::hard_link(&tmp, &target) {
                Ok(()) => {
                    fsync_dir(dir)?;
                    Ok(true)
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
                Err(e) => Err(e).with_context(|| format!("linking {}", target.display())),
            }
        })();
        let _ = fs::remove_file(&tmp);
        result
    }

    /// RFC-0004-b-endpoints §9.3 (the lock-scoped half of T-04's publish handler):
    /// "check existence → write" runs inside the same global write lock as
    /// merge/fork/delete, so a concurrent write's rename can never land
    /// between the existence check and the no-clobber create. Returns
    /// `Ok(true)` when the file was created with `content`, `Ok(false)`
    /// whenever a file exists — either it was already there on entry, or an
    /// out-of-lock external creator (claude self-creating a session, an
    /// operator dropping a file) won the link window inside §9.1's
    /// `create_exclusive`. `Ok(false)` is the caller's cue to fall back to
    /// the §9.2 sha comparison, which resolves already vs conflict.
    #[doc(hidden)]
    pub fn publish_create(&self, dir: &Path, session: &str, content: &[u8]) -> Result<bool> {
        let _guard = self.lock.lock();
        if self.locate(session)?.is_some() {
            return Ok(false);
        }
        self.create_exclusive(dir, session, content)
    }

    // ---- write path: one global mutex, atomic renames ----------------------

    pub async fn fork(&self, session: &str, up_to: &str) -> Result<ForkOut> {
        Self::validate_sid(session)?;
        let _guard = self.lock.lock();
        let source = self.require(session)?;
        let lines = Self::read_lines(&source)?;

        let cut = lines
            .iter()
            .position(|l| l.get("uuid").and_then(Value::as_str) == Some(up_to))
            .with_context(|| format!("message {up_to} not found in session {session}"))?;

        let new_id = Uuid::new_v4().to_string();
        let mut pairs = Vec::with_capacity(cut + 1);
        let mut messages = Vec::with_capacity(cut + 1);
        let mut prev_new: Option<String> = None;
        let mut staged: Vec<Value> = Vec::with_capacity(cut + 1);
        for line in &lines[..=cut] {
            // Real stores interleave uuid-less bookkeeping lines
            // (queue-operation, last-prompt, ...). Forks copy the transcript
            // only; claude regenerates bookkeeping itself.
            let Some(old) = line.get("uuid").and_then(Value::as_str) else {
                continue;
            };
            let mut obj = line.clone();
            let old = old.to_string();
            let new = Uuid::new_v4().to_string();
            obj["sessionId"] = json!(new_id);
            obj["uuid"] = json!(new);
            match &prev_new {
                Some(parent) => obj["parentUuid"] = json!(parent),
                // First copied line keeps its original parent (usually null):
                // the fork is a fresh chain root, not a continuation.
                None => {}
            }
            prev_new = Some(new.clone());
            pairs.push((old, new));
            messages.push(obj.clone());
            staged.push(obj);
        }

        let dest = source.with_file_name(format!("{new_id}.jsonl"));
        Self::write_lines(&dest, &staged)?;
        Ok(ForkOut { session_id: new_id, pairs, messages })
    }

    pub async fn delete(&self, session: &str) -> Result<bool> {
        Self::validate_sid(session)?;
        let _guard = self.lock.lock();
        match self.find_sync(session) {
            Some(path) => Ok(std::fs::remove_file(path).is_ok()),
            None => Ok(false),
        }
    }

    /// Append-only union by uuid, the sync primitive behind the A-side store
    /// mirror (ADR-0004): base order = the existing file, then incoming lines
    /// whose uuid is new, in incoming order. Incoming uuid-less lines are
    /// dropped — bookkeeping lines are regenerated by claude and carry no
    /// cross-machine identity. Both sides only ever append, so the union is
    /// order-stable for the fork/resume prefix checks the Agent SDK makes.
    pub async fn merge(&self, session: &str, incoming: &[Value]) -> Result<MergeOut> {
        Self::validate_sid(session)?;
        let _guard = self.lock.lock();
        let path = match self.find_sync(session) {
            Some(path) => path,
            None => {
                let dir = self.root.join("projects").join("lab");
                std::fs::create_dir_all(&dir)
                    .with_context(|| format!("creating {}", dir.display()))?;
                dir.join(format!("{session}.jsonl"))
            }
        };
        let mut lines = if path.exists() { Self::read_lines(&path)? } else { Vec::new() };
        let mut seen: std::collections::HashSet<String> = lines
            .iter()
            .filter_map(|l| l.get("uuid").and_then(Value::as_str).map(str::to_string))
            .collect();
        let mut added = 0usize;
        for line in incoming {
            let Some(uuid) = line.get("uuid").and_then(Value::as_str) else {
                continue;
            };
            if seen.insert(uuid.to_string()) {
                let mut obj = line.clone();
                obj["sessionId"] = json!(session);
                lines.push(obj);
                added += 1;
            }
        }
        Self::write_lines(&path, &lines)?;
        Ok(MergeOut { added, total: lines.len() })
    }
}

/// Temp names live in the target's own directory (rename/link stay on one
/// filesystem) and always match the `*.tmp.*` skip glob so `locate` never
/// sees them and `list` filters them (RFC-0004-b-endpoints §8.1/§4.2). A per-process
/// counter keeps concurrent merges of the same target distinct.
fn tmp_name(file_name: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "{file_name}.tmp.{}.{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// Write `content` to a fresh temp path and fsync it — the "data hits disk
/// before the rename" half of crash-consistent writes.
fn write_tmp(path: &Path, content: &[u8]) -> Result<()> {
    let mut file =
        File::create(path).with_context(|| format!("creating {}", path.display()))?;
    file.write_all(content)
        .with_context(|| format!("writing {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("fsyncing {}", path.display()))?;
    Ok(())
}

/// fsync the directory so the rename/link itself is durable (without this a
/// crash can resurrect the old name even though the new file's data landed).
fn fsync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("fsyncing dir {}", dir.display()))
}

/// Residue glob used by `list` (locate skips these for free via its
/// exact-name match). Matches only the two staging shapes this codebase can
/// actually produce — `*.tmp.<pid>.<seq>` (store writes) and `*.zp<pid>`
/// (mirror-side staging residue that can be copied in) — not the bare
/// substrings: a session id containing ".zp" or ".tmp." (`a.zp1.jsonl`) is
/// a legal name and must stay visible (review M4).
fn is_temp_name(name: &str) -> bool {
    // `*.tmp.<digits>.<digits>` — tmp_name() emits `<file>.tmp.<pid>.<seq>`.
    if let Some(tail) = name.rsplit(".tmp.").next() {
        let mut parts = tail.split('.');
        if matches!(parts.next(), Some(p) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
            && matches!(parts.next(), Some(p) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
        {
            return true;
        }
    }
    // `*.zp<digits>` — `.zp` followed by a numeric tail only.
    if let Some(tail) = name.rsplit(".zp").next() {
        if !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_digit()) {
            return true;
        }
    }
    false
}

/// Loop `pread` until `want` bytes are gathered or the file yields EOF.
/// Unlike `read_exact_at`, a short answer reports how much was actually
/// there — exactly the in-place-truncation signal the read primitives need.
fn pread_collect(file: &File, offset: u64, want: u64) -> Result<Vec<u8>> {
    let end = offset.saturating_add(want);
    let mut buf = Vec::with_capacity((end - offset).min(1 << 20) as usize);
    let mut pos = offset;
    let mut chunk = [0u8; PREAD_CHUNK];
    while pos < end {
        let take = ((end - pos) as usize).min(PREAD_CHUNK);
        let n = read_at(file, &mut chunk[..take], pos).context("pread session file")?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        pos += n as u64;
    }
    Ok(buf)
}
