//! `<mirror>/.sync/state.json` — the single authoritative file (RFC-0002-a-state §3,
//! §8). One v3 `sessions` table, eight fields per record, no "status" field:
//! a session's class is *derived* by `classify.rs` from the field
//! combination. A commit is one whole-file transaction
//! (`temp → fsync → rename → dir fsync`); parsing is lenient about unknown
//! fields and ruthless about known-field schema violations — any violation
//! quarantines the entire file as `state.corrupt-<unix_ms>` and latches the
//! fail-closed mode (§8), never silently zeroing `publishedAt`/tombstones.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

pub(crate) const VERSION: u32 = 3;
const STATE_FILE: &str = "state.json";
const CORRUPT_PREFIX: &str = "state.corrupt-";

/// `zpc1` cursor re-export for callers that only need the byte count.
pub(crate) use crate::proto::cursor::parse as parse_cursor;

/// The fs primitives live in `sync::fs`; re-exported here because
/// `state::ensure_dir`/`state::write_atomic`/`state::fsync_dir` are the
/// established call paths of callers outside `sync` (cli/daemon).
pub(crate) use super::fs::{ensure_dir, fsync_dir, write_atomic};

/// `now_rfc3339` lives in `sync::time`; re-exported for the e2e harness
/// and cli/daemon callers that mint `*At` stamps through `state::`.
pub use super::time::now_rfc3339;

// ---------------------------------------------------------------- schema ---

/// `task.phase` — the four-phase publish slot (RFC-0002-a-state §4.1). `degraded`
/// and `published` are the durable terminals.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    Writing,
    Validated,
    Published,
    Degraded,
}

impl Phase {
    /// RFC-0003-zonepaird §4.3-3: replayed `ensure-task` intents may only advance, and
    /// degraded/published never regress. A `validated` intent *does* reset a
    /// `degraded` slot — that is the only documented recovery path (§6.3:
    /// "a new intent entering the queue explicitly resets the task row
    /// back to validated").
    pub fn may_advance_to(self, next: Phase) -> bool {
        use Phase::*;
        match next {
            Writing => false, // no regression ever lands back on writing
            Validated => matches!(self, Writing | Degraded),
            Published => self != Published,
            Degraded => self != Published,
        }
    }
}

/// `materialized` — where this session's bytes live (RFC-0002-a-state §3.3).
/// Absent in the record is equivalent to `Full` (v2 upgrade needs no
/// rewrite); a fourth value is a schema violation that quarantines the file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Materialized {
    /// `[0, cursor)` is a byte-exact local copy.
    #[default]
    Full,
    /// mirror holds a deliberately generated sentinel shell, not a prefix.
    Stub,
    /// file deliberately removed; only the record remains.
    Unloaded,
}

impl Materialized {
    /// `materialized ∈ {full, absent}` — the RFC's non-pruned equivalence.
    pub fn is_full(self) -> bool {
        self == Materialized::Full
    }
}

/// `pull` — the only evidence a session is B-owned (§3.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pull {
    /// `zpc1.<bytes>.<fp>` — opaque; A reads only the byte count.
    pub cursor: String,
    /// mirror-relative path the copy lands at.
    pub file: String,
}

/// `pullPending` — queued pull intent; a *goal*, never progress evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullPending {
    /// `"latest"` or a `zpc1` cursor string.
    pub target: String,
    pub enqueued_at: String,
    /// Provenance: the journal record filename that produced (or last
    /// merged into) this pending — `<unix_ms>-<pid>-<seq>.json`. Journal
    /// filename order IS consume order, so `commit_materialized` can tell
    /// a pre-evict pending (cleared by §4.3-4②) from a post-evict one
    /// (fresh traffic, survives) without any wall-clock comparison.
    /// Absent on pre-provenance files: treated as "predates everything".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<String>,
}

impl PullPending {
    /// Journal-order provenance check (§4.3-4②): does this pending's
    /// `record` order BEFORE `evict_record` in journal filename order —
    /// which IS consume order? `None` (a pre-provenance file) counts as
    /// predating. The evict read-in arm and `commit_materialized` share
    /// this one rule so a pending is cleared by exactly the evicts that
    /// precede the pull intent which stamped it.
    pub fn predates(&self, evict_record: &str) -> bool {
        self.record.as_deref().unwrap_or("") < evict_record
    }
}

/// `task` — the publish-only slot (§3.3). Its mere existence means "a
/// publish task is pending"; pull work never lands here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub file: String,
    pub phase: Phase,
    pub first_seen: String,
    pub enqueued_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// `deleted` — the tombstone. `file_id` binds the *file instance*, not the
/// session id: `null` means "this id must never carry a file again".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Deleted {
    pub at: String,
    /// Whether the session was published at delete time (folded evidence).
    pub published_at_delete: bool,
    /// `sha256:<32hex>` fingerprint of the deleted content, or `null` when
    /// the file was absent/stub/unloaded at delete time.
    pub file_id: Option<String>,
}

/// `registered` — `--session-id` registered on B, no data yet (§3.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Registered {
    /// Always `"b-owned"` — any other value is a schema violation.
    pub kind: String,
    pub at: String,
}

/// One session's record: any subset of the eight fields; the classification
/// is read *out of* the combination, never stored.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull: Option<Pull>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_pending: Option<PullPending>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<Task>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted: Option<Deleted>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registered: Option<Registered>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialized: Option<Materialized>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_unchanged_at: Option<String>,
}

impl SessionRecord {
    /// `materialized` absent ≡ `full` (v2 records upgrade in place).
    pub fn materialization(&self) -> Materialized {
        self.materialized.unwrap_or_default()
    }

    /// Tombstone commit (§3.3): `deleted{}` lands in the SAME transaction
    /// that clears `pull`/`pullPending`/`task`/`publishedAt`/`registered`/
    /// `materialized`; `publishedAt` folds into `publishedAtDelete`.
    /// `file_id` is `None` (sealed) whenever the content fingerprint cannot
    /// be taken — file absent, `stub` shell, or `unloaded`.
    pub fn bury(&mut self, at: impl Into<String>, file_id: Option<String>) {
        let published_at_delete = self.published_at.is_some();
        *self = SessionRecord {
            deleted: Some(Deleted {
                at: at.into(),
                published_at_delete,
                file_id,
            }),
            // §3.3 lists exactly six fields to clear; `lastUnchangedAt` is
            // not among them, so it survives the bury.
            last_unchanged_at: self.last_unchanged_at.clone(),
            ..SessionRecord::default()
        };
    }

    /// First-pull commit (§3.3): `pull` lands in the same transaction that
    /// clears `registered` — `registered ∧ pull` is unreachable afterwards.
    pub fn begin_pull(&mut self, pull: Pull) {
        self.pull = Some(pull);
        self.registered = None;
    }

    /// `pullPending` satisfied ⇒ same-transaction delete (§9).
    pub fn satisfy_pull_pending(&mut self) {
        self.pull_pending = None;
    }
}

/// `deleted.fileId` form: `sha256:` + lowercase-hex truncated SHA-256 — the
/// same algorithm as the cursor fingerprint segment (§3.3).
pub fn is_file_id(s: &str) -> bool {
    let Some(hex) = s.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 32 && hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Mirror-relative path confinement: `task.file`/`pull.file` are joined
/// against the mirror root by every consumer, so a record carrying an
/// absolute path or a `..` escape would read/write outside the trust
/// domain. Reject those shapes (and the empty string) — anything else is
/// a legal relative path by construction.
pub fn confined_rel_path(s: &str) -> bool {
    !s.is_empty()
        && !Path::new(s)
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
}

/// `deleted.fileId` form: `sha256:` + lowercase-hex truncated SHA-256 — the
/// same algorithm as the cursor fingerprint segment (§3.3).
pub fn file_id_of(bytes: &[u8]) -> String {
    let h = crate::proto::cursor::prefix_hash(bytes);
    format!("sha256:{}", crate::proto::cursor::hex_encode(&h))
}

// ------------------------------------------------------------- the file ---

/// The authoritative table as loaded/committed. Nothing outside `sessions`
/// is persisted (except the wire `version`), so runtime concerns never leak
/// into the schema.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct State {
    pub sessions: BTreeMap<String, SessionRecord>,
}

impl State {
    /// An empty v3 table — the degraded-table shape after quarantine too:
    /// rebuildable fields (`pull`/`pullPending`/`materialized`/
    /// `lastUnchangedAt`) start empty and re-pull from scratch (§8-3).
    pub fn empty_v3() -> State {
        State::default()
    }
}

#[derive(Serialize)]
struct Wire<'a> {
    version: u32,
    sessions: &'a BTreeMap<String, SessionRecord>,
}

/// Why the fail-closed mode latched: the file quarantined and the reason it
/// failed schema checks. `ensure_published` answers "state corrupt,
/// suspended" and `sync` reports this path (§8-2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Corruption {
    pub quarantined: PathBuf,
    pub reason: String,
}

impl fmt::Display for Corruption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "state.json quarantined to {}: {}",
            self.quarantined.display(),
            self.reason
        )
    }
}

pub struct LoadOutcome {
    /// Always usable: on corruption this is the empty v3 table.
    pub state: State,
    /// `Some` when the fail-closed mode is latched — either this load just
    /// quarantined a bad `state.json`, or a `state.corrupt-*` residue is
    /// still present (the persistent latch, §9.1-4; removed only by a human).
    pub corruption: Option<Corruption>,
}

pub fn sync_dir(mirror: &Path) -> PathBuf {
    mirror.join(".sync")
}

pub fn state_path(sync_dir: &Path) -> PathBuf {
    sync_dir.join(STATE_FILE)
}

/// Ensure `<mirror>/.sync` exists with mode 0700 (RFC-0002-a-state §3.1).
pub fn ensure_sync_dir(mirror: &Path) -> Result<PathBuf> {
    let dir = sync_dir(mirror);
    ensure_dir(&dir, 0o700)?;
    Ok(dir)
}

/// `state.corrupt-*` residues currently latching the failure mode, sorted
/// for a deterministic diagnostic surface.
pub fn corrupt_files(sync_dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(sync_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    for entry in entries.flatten() {
        if entry
            .file_name()
            .to_str()
            .is_some_and(|n| n.starts_with(CORRUPT_PREFIX))
        {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

/// Load `state.json`. Missing file ⇒ empty v3 (corrupt residues still
/// latch). Unreadable/invalid file ⇒ quarantine + fail-closed signal +
/// empty table; the rebuildable-fields reset is just "start empty and
/// re-pull" (§8-3), the loud part is the `Corruption` the caller reports.
pub fn load(sync_dir: &Path) -> Result<LoadOutcome> {
    load_impl(sync_dir, true)
}

/// `load` minus the writer's privileges: never quarantines, never creates
/// directories, never writes a byte. Short-lived readers (the adapter's
/// `wait_for_publish`, CLI peeks) use this — the schema checks still run
/// and a violation surfaces as `Corruption` (so the caller can fail-closed
/// loudly), but the file itself stays exactly where it was: quarantine is
/// the writer's startup duty (§9.1-3), not a reader's (RFC-0003-zonepaird §8.3
/// "never writes state.json").
pub fn load_readonly(sync_dir: &Path) -> Result<LoadOutcome> {
    load_impl(sync_dir, false)
}

/// The load skeleton both entry points share. `writable` selects the one
/// writer's privilege: on a schema violation the writer renames the file
/// aside (`state.corrupt-*`) while the read-only caller reports the same
/// `Corruption` with `quarantined` pointing at the file *in place* —
/// diagnostics only, the rename is left for the next writer's startup.
fn load_impl(sync_dir: &Path, writable: bool) -> Result<LoadOutcome> {
    let path = state_path(sync_dir);
    let raw = match std::fs::read(&path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(LoadOutcome {
                state: State::empty_v3(),
                corruption: latched(sync_dir)?,
            });
        }
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
        Ok(raw) => raw,
    };
    match parse_state(&raw) {
        Ok(state) => Ok(LoadOutcome {
            state,
            corruption: latched(sync_dir)?,
        }),
        Err(reason) => Ok(LoadOutcome {
            state: State::empty_v3(),
            corruption: Some(Corruption {
                quarantined: if writable {
                    quarantine(sync_dir, &path, &reason)?
                } else {
                    // The file was NOT renamed — it names the file that
                    // failed schema checks, for diagnostics only.
                    path
                },
                reason,
            }),
        }),
    }
}

/// Commit the whole table in one transaction (§3.1): temp file → write →
/// fsync → atomic rename → directory fsync. The transaction unit IS one
/// file write; multi-field updates (e.g. `phase=published` + `publishedAt`
/// + `pull`) land atomically because they are one `State`.
pub fn commit(sync_dir: &Path, state: &State) -> Result<()> {
    ensure_dir(sync_dir, 0o700)?;
    let wire = Wire {
        version: VERSION,
        sessions: &state.sessions,
    };
    let bytes = serde_json::to_vec_pretty(&wire).context("serializing state.json")?;
    let tmp = sync_dir.join(".state.json.tmp");
    let target = state_path(sync_dir);
    write_atomic(&tmp, &target, &bytes)
        .with_context(|| format!("committing {}", target.display()))
}

// --------------------------------------------------------- parse/validate ---

/// §8 version set, nailed down: integers `2` and `3` are the only readable
/// files. Absent / non-integer / string / `4` / `1` all quarantine — v4 is
/// quarantined so an older binary cannot destroy a newer file.
fn parse_state(raw: &[u8]) -> std::result::Result<State, String> {
    let value: serde_json::Value = serde_json::from_slice(raw)
        .map_err(|e| format!("not valid JSON: {e}"))?;
    let obj = value
        .as_object()
        .ok_or_else(|| "root is not an object".to_string())?;
    match obj.get("version") {
        Some(serde_json::Value::Number(n)) if n.as_u64() == Some(2) || n.as_u64() == Some(3) => {}
        other => return Err(format!("unrecognized version: {other:?}")),
    }
    let mut sessions = BTreeMap::new();
    if let Some(sv) = obj.get("sessions") {
        let table = sv
            .as_object()
            .ok_or_else(|| "\"sessions\" is not an object".to_string())?;
        for (sid, rv) in table {
            sessions.insert(
                sid.clone(),
                parse_record(rv).map_err(|e| format!("session {sid}: {e}"))?,
            );
        }
    }
    Ok(State { sessions })
}

/// The eight known `SessionRecord` wire keys — the single enumeration
/// shared by the null pre-check below and the wire-shape pin test (the
/// serde derive can't name its own fields).
const RECORD_FIELDS: [&str; 8] = [
    "pull",
    "pullPending",
    "task",
    "publishedAt",
    "deleted",
    "registered",
    "materialized",
    "lastUnchangedAt",
];

/// Per-record parse: **unknown fields tolerated, known-field violations
/// fail the whole file** (§8). A field present as `null` is a violation for
/// object-typed fields (they are either absent or an object) — serde's
/// `Option` reads `null` as absent, so the five object-typed fields plus
/// `materialized` get a null pre-check before the derive runs.
fn parse_record(value: &serde_json::Value) -> std::result::Result<SessionRecord, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "record is not an object".to_string())?;
    for key in RECORD_FIELDS {
        if obj.get(key).is_some_and(serde_json::Value::is_null) {
            return Err(format!("field {key}: null is not a valid value"));
        }
    }
    // `deleted.fileId` is required-but-nullable (§3.3): the KEY must be
    // present — a missing key on a tombstone is schema corruption, not a
    // sealed sid — and a present value is either `null` or a well-formed
    // `sha256:<32hex>`. A malformed fingerprint can never match a file
    // instance, which would let the deleted session resurrect through
    // the "fingerprint mismatch → new instance" door.
    if let Some(d) = obj.get("deleted") {
        if let Some(dobj) = d.as_object() {
            match dobj.get("fileId") {
                None => return Err("deleted.fileId: required key missing".to_string()),
                Some(serde_json::Value::Null) => {}
                Some(serde_json::Value::String(s)) if is_file_id(s) => {}
                Some(other) => {
                    return Err(format!("deleted.fileId: malformed value {other:?}"))
                }
            }
        }
    }
    let rec: SessionRecord =
        serde_json::from_value(value.clone()).map_err(|e| format!("record: {e}"))?;
    if let Some(r) = &rec.registered {
        if r.kind != "b-owned" {
            return Err(format!(
                "registered.kind must be \"b-owned\", got {:?}",
                r.kind
            ));
        }
    }
    // `file` fields name mirror-relative paths — `..` or an absolute
    // path would make workers touch files outside the mirror.
    for (field, file) in [
        ("pull.file", rec.pull.as_ref().map(|p| p.file.as_str())),
        ("task.file", rec.task.as_ref().map(|t| t.file.as_str())),
    ] {
        if let Some(f) = file {
            if !confined_rel_path(f) {
                return Err(format!("{field} escapes the mirror: {f:?}"));
            }
        }
    }
    Ok(rec)
}

/// Step 1 of §8: rename the broken file aside atomically, keeping it
/// visible for manual recovery. `state.corrupt-<unix_ms>` (+ `-n` on the
/// astronomically unlikely same-ms collision).
fn quarantine(sync_dir: &Path, path: &Path, reason: &str) -> Result<PathBuf> {
    let stamp = super::time::unix_ms();
    let mut candidate = sync_dir.join(format!("{CORRUPT_PREFIX}{stamp}"));
    for n in 1..1000 {
        if !candidate.exists() {
            break;
        }
        candidate = sync_dir.join(format!("{CORRUPT_PREFIX}{stamp}-{n}"));
    }
    std::fs::rename(path, &candidate).with_context(|| {
        format!("quarantining corrupt {} ({reason})", path.display())
    })?;
    fsync_dir(sync_dir)?;
    eprintln!(
        "zonepair: state.json corrupt ({reason}); quarantined to {} — \
         rebuildable fields (pull/pullPending/materialized/lastUnchangedAt) \
         reset and will re-pull from B; prune facts are LOST, all sessions \
         re-pull as full",
        candidate.display()
    );
    Ok(candidate)
}

/// §9.1-4: a `state.corrupt-*` residue is the persistent failure latch —
/// every start re-enters the failed mode until a human removes it.
fn latched(sync_dir: &Path) -> Result<Option<Corruption>> {
    Ok(corrupt_files(sync_dir)?.into_iter().next().map(|quarantined| {
        Corruption {
            quarantined,
            reason: "state.corrupt-* residue present".to_string(),
        }
    }))
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::persist::fs::testkit::tmp;
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;

    fn full_record() -> SessionRecord {
        SessionRecord {
            pull: Some(Pull {
                cursor: "zpc1.42.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                file: "projects/lab/s1.jsonl".into(),
            }),
            pull_pending: Some(PullPending {
                target: "latest".into(),
                enqueued_at: "2026-11-18T01:00:00Z".into(),
                record: None,
            }),
            task: Some(Task {
                file: "projects/lab/s1.jsonl".into(),
                phase: Phase::Validated,
                first_seen: "2026-11-18T00:59:00Z".into(),
                enqueued_at: "2026-11-18T01:00:00Z".into(),
                last_error: Some("boom".into()),
            }),
            published_at: Some("2026-11-18T00:58:00Z".into()),
            deleted: Some(Deleted {
                at: "2026-11-18T00:57:00Z".into(),
                published_at_delete: true,
                file_id: Some("sha256:00112233445566778899aabbccddeeff".into()),
            }),
            registered: Some(Registered {
                kind: "b-owned".into(),
                at: "2026-11-18T00:56:00Z".into(),
            }),
            materialized: Some(Materialized::Stub),
            last_unchanged_at: Some("2026-11-18T00:55:00Z".into()),
        }
    }

    // T-410: v3 record round-trips all eight fields; `materialized` absent
    // reads as `full`; the max publish-confirmation transaction lands
    // `phase=published` + `publishedAt` + `pull` in one commit.
    #[test]
    fn t410_schema_roundtrip_and_materialized_default() {
        let dir = tmp("state", "t410");
        ensure_dir(&dir, 0o700).unwrap();
        let mut state = State::empty_v3();
        state.sessions.insert("s1".into(), full_record());
        // The max single transaction: three fields written together.
        state.sessions.insert(
            "s2".into(),
            SessionRecord {
                task: Some(Task {
                    file: "projects/lab/s2.jsonl".into(),
                    phase: Phase::Published,
                    first_seen: "2026-11-18T00:00:00Z".into(),
                    enqueued_at: "2026-11-18T00:01:00Z".into(),
                    last_error: None,
                }),
                published_at: Some("2026-11-18T00:02:00Z".into()),
                pull: Some(Pull {
                    cursor: "zpc1.7.bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                    file: "projects/lab/s2.jsonl".into(),
                }),
                ..SessionRecord::default()
            },
        );
        commit(&dir, &state).unwrap();
        let loaded = load(&dir).unwrap();
        assert!(loaded.corruption.is_none());
        assert_eq!(loaded.state, state);

        // Wire shape pins: version + camelCase keys.
        let raw: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(state_path(&dir)).unwrap()).unwrap();
        assert_eq!(raw["version"], json!(3));
        let rec = &raw["sessions"]["s1"];
        for key in RECORD_FIELDS {
            assert!(rec.get(key).is_some(), "missing wire key {key}");
        }

        // `materialized` absent ≡ full — a v2-shaped record needs no rewrite.
        let rec2 = SessionRecord {
            pull: Some(Pull {
                cursor: crate::proto::cursor::ZERO.into(),
                file: "projects/lab/s9.jsonl".into(),
            }),
            ..SessionRecord::default()
        };
        assert_eq!(rec2.materialization(), Materialized::Full);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // T-412: a commit is one temp→fsync→rename→dir-fsync transaction; the
    // .sync dir is 0700 and state.json 0600; no temp residue is left
    // visible and the file is parseable JSON.
    #[test]
    fn t412_commit_is_one_atomic_transaction() {
        let mirror = tmp("state", "t412");
        let sync = ensure_sync_dir(&mirror).unwrap();
        assert_eq!(sync.metadata().unwrap().permissions().mode() & 0o777, 0o700);
        let state = State::empty_v3();
        commit(&sync, &state).unwrap();
        let mode = state_path(&sync)
            .metadata()
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        // No temp residue and parseable content.
        let leftovers: Vec<_> = std::fs::read_dir(&sync)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_str().is_some_and(|n| n.contains("tmp")))
            .collect();
        assert!(leftovers.is_empty());
        let parsed: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(state_path(&sync)).unwrap()).unwrap();
        assert_eq!(parsed["sessions"], json!({}));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-413: version recognition set is exactly {2,3} — absent / string /
    // non-integer / 1 / 4 all quarantine; a v2 file upgrades in place
    // (unknown fields tolerated, materialized absent ⇒ full).
    #[test]
    fn t413_version_recognition_set() {
        for (name, doc) in [
            ("absent", json!({"sessions": {}})),
            ("string", json!({"version": "3", "sessions": {}})),
            ("float", json!({"version": 2.5, "sessions": {}})),
            ("one", json!({"version": 1, "sessions": {}})),
            ("four", json!({"version": 4, "sessions": {}})),
        ] {
            let dir = tmp("state", "t413");
            ensure_dir(&dir, 0o700).unwrap();
            std::fs::write(state_path(&dir), doc.to_string()).unwrap();
            let out = load(&dir).unwrap();
            let c = out.corruption.expect(&format!("version case {name}"));
            assert!(c.quarantined.exists(), "{name}");
            assert!(!state_path(&dir).exists(), "{name}");
            assert_eq!(out.state, State::empty_v3(), "{name}");
            std::fs::remove_dir_all(&dir).unwrap();
        }

        // v2 upgrades in place: unknown top-level + record fields tolerated.
        let dir = tmp("state", "t413v2");
        ensure_dir(&dir, 0o700).unwrap();
        std::fs::write(
            state_path(&dir),
            json!({
                "version": 2,
                "futureField": {"nested": true},
                "sessions": {
                    "s1": {
                        "pull": {"cursor": "zpc1.5.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                 "file": "projects/lab/s1.jsonl", "extra": 1},
                        "unknownRecordField": [1, 2, 3]
                    }
                }
            })
            .to_string(),
        )
        .unwrap();
        let out = load(&dir).unwrap();
        assert!(out.corruption.is_none(), "v2 must upgrade, not quarantine");
        let rec = &out.state.sessions["s1"];
        assert!(rec.pull.is_some());
        assert_eq!(rec.materialization(), Materialized::Full);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // T-415: any record schema violation — `materialized` fourth enum
    // value, `deleted` missing `at` — quarantines the WHOLE file.
    #[test]
    fn t415_record_schema_violation_quarantines_whole_file() {
        for (name, record) in [
            ("mat4", json!({"materialized": "ghost"})),
            ("del-no-at", json!({"deleted": {"fileId": null, "publishedAtDelete": false}})),
            ("pull-null", json!({"pull": null})),
            ("pull-bad", json!({"pull": {"cursor": 5}})),
            ("kind", json!({"registered": {"kind": "a-owned", "at": "t"}})),
            ("phase", json!({"task": {"file": "f", "phase": "limbo",
                "firstSeen": "t", "enqueuedAt": "t"}})),
        ] {
            let dir = tmp("state", "t415");
            ensure_dir(&dir, 0o700).unwrap();
            std::fs::write(
                state_path(&dir),
                json!({"version": 3, "sessions": {"sx": record}}).to_string(),
            )
            .unwrap();
            let out = load(&dir).unwrap();
            assert!(out.corruption.is_some(), "{name} must quarantine");
            assert_eq!(out.state, State::empty_v3(), "{name}");
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    // R2-I4: `deleted.fileId` is required-but-nullable — a MISSING key
    // quarantines (a truncated tombstone must not silently seal a sid),
    // and a malformed fingerprint quarantines (it can never match an
    // instance, which would let the session resurrect through the
    // fingerprint-mismatch door). Null and a well-formed id are legal.
    #[test]
    fn tombstone_fileid_presence_and_shape_are_schema() {
        for (name, rec) in [
            ("missing", json!({"deleted": {"at": "t", "publishedAtDelete": false}})),
            ("malformed", json!({"deleted": {"at": "t", "publishedAtDelete": false, "fileId": "garbage"}})),
            ("uppercase", json!({"deleted": {"at": "t", "publishedAtDelete": false,
                "fileId": "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}})),
        ] {
            let dir = tmp("state", "fid");
            ensure_dir(&dir, 0o700).unwrap();
            std::fs::write(
                state_path(&dir),
                json!({"version": 3, "sessions": {"sx": rec}}).to_string(),
            )
            .unwrap();
            let out = load(&dir).unwrap();
            assert!(out.corruption.is_some(), "{name} must quarantine");
            std::fs::remove_dir_all(&dir).unwrap();
        }
        // Legal shapes: fileId null (sealed) and a well-formed fingerprint.
        for (name, file_id) in [
            ("null", serde_json::Value::Null),
            ("valid", json!("sha256:ba7816bf8f01cfea414140de5dae2223")),
        ] {
            let dir = tmp("state", "fid-ok");
            ensure_dir(&dir, 0o700).unwrap();
            std::fs::write(
                state_path(&dir),
                json!({"version": 3, "sessions": {"sx":
                    {"deleted": {"at": "t", "publishedAtDelete": false, "fileId": file_id}}}})
                .to_string(),
            )
            .unwrap();
            let out = load(&dir).unwrap();
            assert!(out.corruption.is_none(), "{name} must parse");
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    // Scalar `*At` fields: `null` is a violation like every other known
    // field — absent-or-string only (RFC field table, no tolerated nulls).
    #[test]
    fn scalar_at_fields_reject_null() {
        for key in ["publishedAt", "lastUnchangedAt"] {
            let dir = tmp("state", "atnull");
            ensure_dir(&dir, 0o700).unwrap();
            std::fs::write(
                state_path(&dir),
                json!({"version": 3, "sessions": {"sx": {key: serde_json::Value::Null}}})
                    .to_string(),
            )
            .unwrap();
            let out = load(&dir).unwrap();
            assert!(out.corruption.is_some(), "{key}:null must quarantine");
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    // `pull.file`/`task.file` confinement: `..` and absolute paths
    // quarantine — a record must never make workers touch files outside
    // the mirror root.
    #[test]
    fn file_fields_are_confined_to_the_mirror() {
        for (name, rec) in [
            ("pull-escape", json!({"pull": {"cursor": "zpc1.0.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "file": "../x.jsonl"}})),
            ("task-absolute", json!({"task": {"file": "/etc/passwd", "phase": "writing",
                "firstSeen": "t", "enqueuedAt": "t"}})),
        ] {
            let dir = tmp("state", "escape");
            ensure_dir(&dir, 0o700).unwrap();
            std::fs::write(
                state_path(&dir),
                json!({"version": 3, "sessions": {"sx": rec}}).to_string(),
            )
            .unwrap();
            let out = load(&dir).unwrap();
            assert!(out.corruption.is_some(), "{name} must quarantine");
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    // T-416/T-417: the quarantine lifecycle — bad file renamed to
    // `state.corrupt-*` (bytes preserved), fail-closed latches via the
    // residue on every load, rebuildable fields come back empty; a human
    // deleting the residue is the only exit.
    #[test]
    fn t416_quarantine_lifecycle_latches_until_residue_removed() {
        let dir = tmp("state", "t416");
        ensure_dir(&dir, 0o700).unwrap();
        std::fs::write(state_path(&dir), "{not json").unwrap();
        let out = load(&dir).unwrap();
        let c = out.corruption.clone().unwrap();
        assert!(c.quarantined.file_name().unwrap().to_str().unwrap().starts_with(CORRUPT_PREFIX));
        // Bytes preserved for manual recovery.
        assert_eq!(std::fs::read_to_string(&c.quarantined).unwrap(), "{not json");
        assert_eq!(out.state, State::empty_v3());

        // The latch: every load re-enters failure mode while residue lives.
        let out2 = load(&dir).unwrap();
        assert!(out2.corruption.is_some(), "residue must latch fail-closed");
        assert_eq!(corrupt_files(&dir).unwrap().len(), 1);

        // Human removes the residue → next load exits the failure mode.
        std::fs::remove_file(&c.quarantined).unwrap();
        let out3 = load(&dir).unwrap();
        assert!(out3.corruption.is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // T-418: the tombstone commit clears every other field in the same
    // transaction and folds `publishedAt` into `publishedAtDelete`.
    #[test]
    fn t418_tombstone_single_transaction_clears_fields() {
        let mut rec = full_record();
        rec.bury(
            "2026-11-18T02:00:00Z",
            Some("sha256:ffffffffffffffffffffffffffffffff".into()),
        );
        assert!(rec.pull.is_none() && rec.pull_pending.is_none());
        assert!(rec.task.is_none() && rec.registered.is_none());
        assert!(rec.published_at.is_none() && rec.materialized.is_none());
        let d = rec.deleted.unwrap();
        assert!(d.published_at_delete, "publishedAt folds into the tombstone");
        assert_eq!(d.file_id.as_deref(), Some("sha256:ffffffffffffffffffffffffffffffff"));

        let mut never_published = SessionRecord {
            pull: Some(Pull {
                cursor: crate::proto::cursor::ZERO.into(),
                file: "f".into(),
            }),
            ..SessionRecord::default()
        };
        never_published.bury("t", None);
        assert!(!never_published.deleted.unwrap().published_at_delete);
    }

    // T-419: `pullPending` deletes same-transaction on satisfaction and is
    // never `pull` evidence; first pull clears `registered` atomically.
    #[test]
    fn t419_pull_pending_and_registered_lifecycles() {
        let mut rec = SessionRecord {
            pull_pending: Some(PullPending {
                target: "latest".into(),
                enqueued_at: "t".into(),
                record: None,
            }),
            ..SessionRecord::default()
        };
        assert!(rec.pull.is_none(), "pullPending is not pull evidence");
        rec.satisfy_pull_pending();
        assert!(rec.pull_pending.is_none());

        let mut reg = SessionRecord {
            registered: Some(Registered {
                kind: "b-owned".into(),
                at: "t".into(),
            }),
            ..SessionRecord::default()
        };
        reg.begin_pull(Pull {
            cursor: "zpc1.1.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            file: "projects/lab/s.jsonl".into(),
        });
        assert!(reg.registered.is_none(), "registered ∧ pull is unreachable");
        assert!(reg.pull.is_some());
    }

    // R3-I4: `load_readonly` runs the same schema checks but NEVER writes —
    // a corrupt file is reported, not renamed, and residue still latches.
    #[test]
    fn load_readonly_reports_without_quarantining() {
        let dir = tmp("state", "readonly");
        ensure_dir(&dir, 0o700).unwrap();

        // Corrupt file: corruption surfaces, file stays put, table is empty.
        std::fs::write(state_path(&dir), "{not json").unwrap();
        let out = load_readonly(&dir).unwrap();
        let c = out.corruption.expect("corruption must be reported");
        assert_eq!(c.quarantined, state_path(&dir));
        assert!(state_path(&dir).exists(), "readonly load never renames");
        assert!(corrupt_files(&dir).unwrap().is_empty());
        assert_eq!(out.state, State::empty_v3());

        // Valid file: loads identically to `load`.
        let mut st = State::empty_v3();
        st.sessions.insert("s".into(), full_record());
        commit(&dir, &st).unwrap();
        let out = load_readonly(&dir).unwrap();
        assert!(out.corruption.is_none());
        assert_eq!(out.state, st);

        // Missing file: empty v3, and residue still latches.
        std::fs::remove_file(state_path(&dir)).unwrap();
        std::fs::write(dir.join("state.corrupt-1"), "x").unwrap();
        let out = load_readonly(&dir).unwrap();
        assert!(out.corruption.is_some(), "residue latches readonly too");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // `file_id_of` shares the cursor's truncated-SHA256 algorithm.
    #[test]
    fn file_id_format_matches_cursor_fingerprint() {
        let id = file_id_of(b"abc");
        assert_eq!(id.len(), 7 + 32);
        assert!(id.starts_with("sha256:"));
        assert_eq!(&id[7..], "ba7816bf8f01cfea414140de5dae2223");
    }
}
