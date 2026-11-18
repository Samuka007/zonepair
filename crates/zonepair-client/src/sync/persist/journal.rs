//! `<mirror>/.sync/journal/` — the intent channel, the system's only
//! multi-writer point (RFC-0003-zonepaird §4). Any short-lived process appends one
//! record `<unix_ms>-<pid>-<seq>.json` through the five-step write
//! (`create_new *.tmp` → write → file fsync → **rename to final name** →
//! dir fsync); the sole writer consumes them in filename order, committing
//! each record into `state.json` *before* deleting the journal file
//! (commit-then-delete — the reverse pair would lose a consumed intent to
//! a crash landing in between).
//!
//! `*.tmp` files are never consumed: a crash leaves "created, half-written"
//! residue that startup cleanup reclaims. Anything that is neither a
//! well-formed name nor a parseable record is debris, reported separately
//! for the cleanup step to remove.

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::fs::{ensure_dir, fsync_dir, open_new};
use super::state::{self, Phase, PullPending, Registered, SessionRecord, State};
use super::time::unix_ms;

/// RFC-0003-zonepaird §4.2-2: more than this many unconsumed records means the writer
/// is missing — fail loudly at append time instead of queuing forever.
pub(crate) const MAX_PENDING: usize = 4096;

/// Per-process append sequence: the third leg of the filename triple. Two
/// appends in the same millisecond still order deterministically, and two
/// processes collide on neither name (`pid` separates them).
static SEQ: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------- intents ---

/// One journal record: the `op` tag plus its payload. Serde shape is the
/// disk contract — records are written by OTHER processes, field names
/// must match RFC-0003-zonepaird §4.1 exactly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Intent {
    /// `--session-id` — register a B-owned session id, no data yet.
    Register { sid: String, enqueued_at: String },
    /// Pull goal: `"latest"` or a `zpc1` cursor — lands as `pullPending`.
    Pull {
        sid: String,
        target: String,
        enqueued_at: String,
    },
    /// Publish task ensure — `validated` is the only admitted wire phase;
    /// orphan `writing` rows are reconcile commits, not intents.
    EnsureTask {
        sid: String,
        file: String,
        phase: Phase,
        enqueued_at: String,
    },
    /// `sess delete` notice → lands the tombstone.
    NoteRemoteDelete { sid: String, enqueued_at: String },
    /// Manual `sync` → one whole-mirror reconcile pass. Carries no record
    /// fields; the consumer flags it and reconciles before deleting.
    Reconcile { enqueued_at: String },
    /// `prune` — `mode: "stub" | "drop"`. Parsed; consumption is the
    /// materialize layer's execution (deferred per brief).
    Evict {
        sid: String,
        mode: EvictMode,
        enqueued_at: String,
    },
    /// `hydrate` — pull bytes back to `full`. Parsed; consumption deferred.
    Hydrate { sid: String, enqueued_at: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EvictMode {
    Stub,
    Drop,
}

// Wire payloads — private serde views, one per op. Unknown fields are
// tolerated; missing or mistyped required fields make the record invalid.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegisterWire {
    sid: String,
    kind: String,
    enqueued_at: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PullWire {
    sid: String,
    target: String,
    enqueued_at: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EnsureTaskWire {
    kind: String,
    sid: String,
    file: String,
    phase: Phase,
    enqueued_at: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SidWire {
    sid: String,
    enqueued_at: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReconcileWire {
    scope: String,
    enqueued_at: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvictWire {
    sid: String,
    mode: EvictMode,
    enqueued_at: String,
}

fn take<T: for<'de> Deserialize<'de>>(body: serde_json::Value, op: &str) -> Result<T> {
    serde_json::from_value(body).map_err(|e| anyhow::anyhow!("{op} record: {e}"))
}

/// Parse one record body (`{"op": …, …}`). Errors are per-record: the
/// caller decides quarantine, the parser never touches the filesystem.
pub(crate) fn parse_intent(body: serde_json::Value) -> Result<Intent> {
    let op = body
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("record lacks a string \"op\""))?
        .to_string();
    match op.as_str() {
        "register" => {
            let w: RegisterWire = take(body, &op)?;
            if w.kind != "b-owned" {
                bail!("register.kind must be \"b-owned\", got {:?}", w.kind);
            }
            Ok(Intent::Register {
                sid: w.sid,
                enqueued_at: w.enqueued_at,
            })
        }
        "pull" => {
            let w: PullWire = take(body, &op)?;
            Ok(Intent::Pull {
                sid: w.sid,
                target: w.target,
                enqueued_at: w.enqueued_at,
            })
        }
        "ensure-task" => {
            let w: EnsureTaskWire = take(body, &op)?;
            if w.kind != "publish" {
                bail!("ensure-task.kind must be \"publish\", got {:?}", w.kind);
            }
            // The wire admits `validated` only (RFC-0003-zonepaird §4.1's literal):
            // `writing` would mint a fresh task row with no forward path
            // (`may_advance_to(Writing)` is always false) — orphan
            // registration lives inside reconcile's own commits, never
            // on the wire. Terminal phases (`published` most of all)
            // would mint a fake publish fact with zero publish RPCs:
            // "corruption never masquerades as success" applies at the
            // trust boundary.
            if !matches!(w.phase, Phase::Validated) {
                bail!(
                    "ensure-task.phase must be \"validated\", got {:?}",
                    w.phase
                );
            }
            // `file` joins the mirror root at execution — `..`/absolute
            // shapes are rejected at ingest, not discovered mid-publish.
            if !state::confined_rel_path(&w.file) {
                bail!("ensure-task.file escapes the mirror: {:?}", w.file);
            }
            Ok(Intent::EnsureTask {
                sid: w.sid,
                file: w.file,
                phase: w.phase,
                enqueued_at: w.enqueued_at,
            })
        }
        "note-remote-delete" => {
            let w: SidWire = take(body, &op)?;
            Ok(Intent::NoteRemoteDelete {
                sid: w.sid,
                enqueued_at: w.enqueued_at,
            })
        }
        "reconcile" => {
            let w: ReconcileWire = take(body, &op)?;
            if w.scope != "all" {
                bail!("reconcile.scope must be \"all\", got {:?}", w.scope);
            }
            Ok(Intent::Reconcile {
                enqueued_at: w.enqueued_at,
            })
        }
        "evict" => {
            let w: EvictWire = take(body, &op)?;
            Ok(Intent::Evict {
                sid: w.sid,
                mode: w.mode,
                enqueued_at: w.enqueued_at,
            })
        }
        "hydrate" => {
            let w: SidWire = take(body, &op)?;
            Ok(Intent::Hydrate {
                sid: w.sid,
                enqueued_at: w.enqueued_at,
            })
        }
        _ => bail!("unknown op {op:?}"),
    }
}

// ------------------------------------------------------------ the writer ---

/// Count unconsumed intents (formally-named `*.json` records only; debris
/// is not backlog). Cheap directory scan, no lock — §4.2 allows an
/// approximate check because the hard rule is only ">4096 fails loudly".
pub fn pending_count(dir: &Path) -> usize {
    list_records(dir).map(|(r, _)| r.len()).unwrap_or(0)
}

/// Append one intent — the five-step write of §4.2. Fails loudly at any
/// step: an unconfirmed intent is worse than an immediate error to the
/// caller (§3.4). `ensure_owner` is the CALLER's preceding step, not this
/// function's business.
pub fn append(dir: &Path, intent: &Intent) -> Result<PathBuf> {
    ensure_dir(dir, 0o700)?;
    if pending_count(dir) >= MAX_PENDING {
        bail!(
            "journal backlog: {} unconsumed records in {} (writer missing?)",
            MAX_PENDING,
            dir.display()
        );
    }
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let stem = record_stem(unix_ms(), std::process::id(), seq);
    let tmp = dir.join(format!("{stem}.tmp"));
    let target = dir.join(format!("{stem}.json"));
    let body = intent_to_json(intent);

    // Steps 3a–c: fresh `*.tmp`, content, file fsync.
    let mut file = open_new(&tmp, 0o600)
        .with_context(|| format!("creating {}", tmp.display()))?;
    let result = (|| {
        file.write_all(body.as_bytes())?;
        file.sync_all()?;
        // Steps 3d–e: rename to the formal name, fsync the directory so the
        // *name* survives power loss, not just the bytes.
        std::fs::rename(&tmp, &target)?;
        fsync_dir(dir)?;
        Ok(())
    })();
    if result.is_err() {
        drop(file);
        let _ = std::fs::remove_file(&tmp);
    }
    result.map(|()| target)
}

fn intent_to_json(intent: &Intent) -> String {
    use serde_json::json;
    let v = match intent {
        Intent::Register { sid, enqueued_at } => json!({
            "op": "register", "sid": sid, "kind": "b-owned",
            "enqueuedAt": enqueued_at,
        }),
        Intent::Pull {
            sid,
            target,
            enqueued_at,
        } => json!({
            "op": "pull", "sid": sid, "target": target,
            "enqueuedAt": enqueued_at,
        }),
        Intent::EnsureTask {
            sid,
            file,
            phase,
            enqueued_at,
        } => json!({
            "op": "ensure-task", "kind": "publish", "sid": sid,
            "file": file, "phase": phase, "enqueuedAt": enqueued_at,
        }),
        Intent::NoteRemoteDelete { sid, enqueued_at } => json!({
            "op": "note-remote-delete", "sid": sid,
            "enqueuedAt": enqueued_at,
        }),
        Intent::Reconcile { enqueued_at } => json!({
            "op": "reconcile", "scope": "all",
            "enqueuedAt": enqueued_at,
        }),
        Intent::Evict {
            sid,
            mode,
            enqueued_at,
        } => json!({
            "op": "evict", "sid": sid,
            "mode": match mode { EvictMode::Stub => "stub", EvictMode::Drop => "drop" },
            "enqueuedAt": enqueued_at,
        }),
        Intent::Hydrate { sid, enqueued_at } => json!({
            "op": "hydrate", "sid": sid, "enqueuedAt": enqueued_at,
        }),
    };
    serde_json::to_string(&v).unwrap()
}

// ------------------------------------------------------------ the reader ---

/// A formally-named journal file: `record` parses to an intent; `invalid`
/// carried the reason (still in sort order — the cleanup step removes it,
/// a bad record must not wedge the channel).
pub struct Entry {
    pub name: String,
    pub record: std::result::Result<Intent, String>,
}

/// Files that are neither `*.tmp` residue nor `<digits>-<digits>-<digits>.json`:
/// they are debris the startup cleanup removes (§9.1-2).
#[derive(Debug)]
pub struct Debris {
    pub name: String,
}

/// The `<unix_ms>-<pid>-<seq>` stem a journal record is written under —
/// the one definition of the name; `is_formal_name` validates the same
/// shape from the other end.
fn record_stem(ms: u64, pid: u32, seq: u64) -> String {
    format!("{ms}-{pid}-{seq}")
}

/// `<digits>-<digits>-<digits>.json`, the name `record_stem` produces —
/// checked by shape (not by `u64` parse) so an oversized leg still scans
/// as a formal record rather than debris.
fn is_formal_name(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".json") else {
        return false;
    };
    let mut parts = stem.split('-');
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(a), Some(b), Some(c), None)
            if !a.is_empty()
                && a.bytes().all(|b| b.is_ascii_digit())
                && !b.is_empty()
                && b.bytes().all(|b| b.is_ascii_digit())
                && !c.is_empty()
                && c.bytes().all(|b| b.is_ascii_digit())
    )
}

/// Directory scan split into three classes: formal records in **filename
/// order** (the deterministic consume order — plain lexicographic, matching
/// the RFC, so `100-…` sorts before `99-…` by design), `*.tmp` residue and
/// debris. `*.tmp` is residue by construction (half-written append), other
/// odd names are debris for the startup cleanup.
fn list_records(dir: &Path) -> io::Result<(Vec<String>, Vec<Debris>)> {
    let mut formal = Vec::new();
    let mut debris = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((formal, debris)),
        Err(e) => return Err(e),
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let is_dir = entry.file_type().is_ok_and(|t| t.is_dir());
        // `*.tmp` residue (half-written append), directories, and odd names
        // are all debris — never consumed, swept by the startup cleanup.
        if !is_dir && is_formal_name(&name) {
            formal.push(name);
        } else {
            debris.push(Debris { name });
        }
    }
    formal.sort_unstable();
    debris.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    Ok((formal, debris))
}

/// Read the channel: formal records parsed in filename order, debris
/// listed for the caller to remove. Zero writes.
pub fn read_pending(dir: &Path) -> Result<(Vec<Entry>, Vec<Debris>)> {
    let (names, debris) = list_records(dir).with_context(|| {
        format!("scanning journal {}", dir.display())
    })?;
    let mut entries = Vec::with_capacity(names.len());
    for name in names {
        let path = dir.join(&name);
        let record = match std::fs::read(&path) {
            Ok(raw) => match serde_json::from_slice(&raw) {
                Ok(body) => parse_intent(body).map_err(|e| e.to_string()),
                Err(e) => Err(format!("not valid JSON: {e}")),
            },
            Err(e) => Err(format!("unreadable: {e}")),
        };
        entries.push(Entry { name, record });
    }
    Ok((entries, debris))
}

// ------------------------------------------------------------- consume ----

/// What applying one intent does to the state. `RunReconcile` and
/// `Deferred` change *when the record may be deleted*: intents that need
/// out-of-transaction work are handed back to the caller deferred instead
/// of silently consumed — and `Deferred` may still mutate in-memory state
/// (evict's `pullPending` clear), durable only via the executor's commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Effect {
    /// Record fields landed; the journal file may be deleted post-commit.
    Applied,
    /// Intent discarded without touching fields (tombstone suppression).
    Dropped,
    /// `reconcile` — the caller runs a full reconcile pass, then deletes
    /// the file. Nothing to commit, so consume reports it instead.
    RunReconcile,
    /// `evict`/`hydrate` — delivered for the materialize executor; the
    /// record file is deleted only after the `materialized` commit lands
    /// (§4.3-4③), so read-in itself commits nothing for them.
    Deferred,
}

/// Apply one intent to the in-memory record for `sid` (idempotent by
/// construction — replaying a consumed record must be a no-op, §4.3-3).
/// `tombstone_file_id` is the `deleted.fileId` the caller computed for
/// `note-remote-delete` (content fingerprint or `None` = sealed).
/// `record_name` is the journal filename being consumed — `pull` stamps
/// it onto `pullPending.record` so `commit_materialized` can order a
/// pending against the evict record without any clock comparison
/// (filename order IS journal order). `""` for direct test calls.
pub(crate) fn apply(
    state: &mut State,
    intent: &Intent,
    tombstone_file_id: Option<String>,
    record_name: &str,
) -> Effect {
    match intent {
        Intent::Register { sid, enqueued_at } => {
            let rec = state.sessions.entry(sid.clone()).or_default();
            // Registration only fills a blank slot — never overwrites pull
            // evidence or a tombstone (idempotent replay).
            if rec.pull.is_none()
                && rec.task.is_none()
                && rec.published_at.is_none()
                && rec.deleted.is_none()
                && rec.registered.is_none()
            {
                rec.registered = Some(Registered {
                    kind: "b-owned".into(),
                    at: enqueued_at.clone(),
                });
            }
            Effect::Applied
        }
        Intent::Pull {
            sid,
            target,
            enqueued_at,
        } => {
            let rec = state.sessions.entry(sid.clone()).or_default();
            // §4.3-4①, extended: a pull intent on a tombstone is dropped
            // at read-in — `pullPending` there could never be satisfied
            // (`pick` skips deleted records), so landing it would wedge
            // `sync --wait` on an unsatisfiable field forever.
            if rec.deleted.is_some() {
                return Effect::Dropped;
            }
            merge_pull_pending(rec, target, enqueued_at, record_name);
            Effect::Applied
        }
        Intent::EnsureTask {
            sid,
            file,
            phase,
            enqueued_at,
        } => {
            let rec = state.sessions.entry(sid.clone()).or_default();
            // Same tombstone drop as `pull`: a task row on a buried
            // record can never be picked — and re-landing one would
            // break replay idempotency (§4.3-1) the moment a replayed
            // `note-remote-delete` no longer wipes the record.
            if rec.deleted.is_some() {
                return Effect::Dropped;
            }
            ensure_task(rec, file, *phase, enqueued_at);
            Effect::Applied
        }
        Intent::NoteRemoteDelete { sid, enqueued_at } => {
            let rec = state.sessions.entry(sid.clone()).or_default();
            // §4.3-1: replaying against an existing tombstone must be a
            // no-op — `bury` would recompute `publishedAtDelete` from
            // the already-cleared `publishedAt` (true→false) and
            // overwrite `fileId` with whatever the probe returns now.
            if rec.deleted.is_some() {
                return Effect::Dropped;
            }
            rec.bury(enqueued_at.clone(), tombstone_file_id);
            Effect::Applied
        }
        Intent::Reconcile { .. } => Effect::RunReconcile,
        // §4.3-4①: the tombstone outranks any materialize intent — drop it.
        // ② `evict` clears the sid's `pullPending` in the same transaction
        // as its read-in (the durable commit is the consume loop's arm);
        // "reclaim the bytes" outranks every in-flight pull goal, else a queued
        // pull would resurrect the file right after evict lands. An
        // unknown sid consumes silently WITHOUT minting a record — an
        // all-empty row would park itself as a row-26 phantom suspend.
        Intent::Evict { sid, .. } => {
            let Some(rec) = state.sessions.get_mut(sid) else {
                return Effect::Dropped;
            };
            if rec.deleted.is_some() {
                return Effect::Dropped;
            }
            // ②'s clear carries the same provenance guard
            // `commit_materialized` applies: wipe only a pending stamped
            // BEFORE this evict's record name. A restart re-runs `apply`
            // on every still-parked record (the parked set is rebuilt
            // empty) — the guard is what stops that replay from
            // durably erasing a post-evict pull intent whose own
            // `pullPending` commit already landed on disk.
            if rec
                .pull_pending
                .as_ref()
                .is_none_or(|p| p.predates(record_name))
            {
                rec.pull_pending = None;
            }
            Effect::Deferred
        }
        Intent::Hydrate { sid, .. } => {
            let tombstoned = state
                .sessions
                .get(sid)
                .is_some_and(|r| r.deleted.is_some());
            if tombstoned {
                Effect::Dropped
            } else {
                Effect::Deferred
            }
        }
    }
}

/// Same-sid pull merge (§4.3-2): `pullPending.target` takes `max` under a
/// commutative order — `"latest"` outranks every pinned cursor; two pinned
/// cursors compare by byte count, with string order as the deterministic
/// fallback (a corrupt target still converges to one value).
/// `record_name` is the provenance stamp: the newest record contributing
/// to the pending owns it — a pending merged by a post-evict pull orders
/// AFTER that evict and survives `commit_materialized`'s conditional clear.
fn merge_pull_pending(rec: &mut SessionRecord, target: &str, enqueued_at: &str, record_name: &str) {
    match &mut rec.pull_pending {
        None => {
            rec.pull_pending = Some(PullPending {
                target: target.to_string(),
                enqueued_at: enqueued_at.to_string(),
                record: Some(record_name.to_string()),
            });
        }
        Some(pending) => {
            let winner = target_max(&pending.target, target);
            pending.target = winner.to_string();
            pending.record = Some(record_name.to_string());
            if enqueued_at > pending.enqueued_at.as_str() {
                pending.enqueued_at = enqueued_at.to_string();
            }
        }
    }
}

fn target_max<'a>(a: &'a str, b: &'a str) -> &'a str {
    if a == "latest" {
        return a;
    }
    if b == "latest" {
        return b;
    }
    match (
        state::parse_cursor(a).map(|c| c.offset),
        state::parse_cursor(b).map(|c| c.offset),
    ) {
        (Ok(x), Ok(y)) => {
            if x >= y {
                a
            } else {
                b
            }
        }
        // Unparseable targets degrade to string order — still commutative.
        _ => a.max(b),
    }
}

/// `ensure-task` read-in: phase only advances (§4.3-3 — a replayed
/// `validated` meeting an already-`published` slot is a no-op; a fresh
/// `validated` against `degraded` is the documented reset path).
fn ensure_task(rec: &mut SessionRecord, file: &str, phase: Phase, enqueued_at: &str) {
    match &mut rec.task {
        None => {
            rec.task = Some(state::Task {
                file: file.to_string(),
                phase,
                first_seen: enqueued_at.to_string(),
                enqueued_at: enqueued_at.to_string(),
                last_error: None,
            });
        }
        Some(task) => {
            if task.phase.may_advance_to(phase) {
                task.phase = phase;
                task.enqueued_at = enqueued_at.to_string();
            }
            // else: replay or regression — no-op by the phase machine.
        }
    }
}

/// One consume step's outcome, returned per entry so the caller sees what
/// replay did — including the deferred kinds it must handle later.
#[derive(Debug)]
pub(crate) struct Consumed {
    pub name: String,
    /// The parsed intent (`None` only for unparseable records) — deferred
    /// executors (`evict`/`hydrate`) and recovery bookkeeping (a fresh pull
    /// intent re-arms a pull-degraded sid) read it back from here.
    pub intent: Option<Intent>,
    pub effect: std::result::Result<Effect, String>,
}

/// Consume the channel (sole writer only): read in filename order, apply
/// each record, commit state, THEN delete the file — commit-then-delete
/// (§4.3-1). Returns the per-entry effects; entries whose effect needs
/// out-of-transaction work (`RunReconcile`/`Deferred`) are *not*
/// deleted and are reported so the caller executes them, then deletes.
///
/// `tombstone_fp` computes the `deleted.fileId` for `note-remote-delete`:
/// the caller supplies a probe closure (record → fingerprint or `None`),
/// keeping this module free of mirror-file IO specifics. It runs ONLY
/// for `note-remote-delete` records — probing the whole session file
/// per intent would put O(records × file size) disk IO inside the
/// state lock for no consumer.
///
/// `parked` names deferred records already represented as work (the
/// pool's `deferred_names`): they persist on disk until their commit
/// lands and re-appear in every drain, but `apply()` must run only on a
/// record's FIRST sighting — re-applying an `evict` would re-fire the
/// `pullPending` wipe and erase pull intents that legitimately landed
/// *after* the evict was consumed (§4.3-4② "in-flight" means in-flight at
/// read-in, exactly once). A parked record still reports its deferred
/// effect so the caller keeps its ownership bookkeeping; it never
/// re-commits and never deletes.
pub(crate) fn consume_ordered_parked(
    dir: &Path,
    sync_dir: &Path,
    state: &mut State,
    mut tombstone_fp: impl FnMut(&State, &str) -> Option<String>,
    parked: &std::collections::HashSet<String>,
) -> Result<Vec<Consumed>> {
    let (entries, _debris) = read_pending(dir)?;
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        match entry.record {
            Err(e) => {
                // Invalid record: not consumable. Startup cleanup (§9.1-2)
                // removes it — leave it in place and report.
                out.push(Consumed {
                    name: entry.name,
                    intent: None,
                    effect: Err(e),
                });
                continue;
            }
            Ok(intent) => {
                if parked.contains(&entry.name) {
                    // Already read in once: re-report the deferred shape
                    // without re-running `apply` — first-sighting only.
                    let effect = if matches!(intent, Intent::Reconcile { .. }) {
                        Effect::RunReconcile
                    } else {
                        Effect::Deferred
                    };
                    out.push(Consumed {
                        name: entry.name,
                        intent: Some(intent),
                        effect: Ok(effect),
                    });
                    continue;
                }
                let fp = if matches!(intent, Intent::NoteRemoteDelete { .. }) {
                    tombstone_fp(state, sid_of(&intent).unwrap_or_default())
                } else {
                    None
                };
                let effect = apply(state, &intent, fp, &entry.name);
                let deferred =
                    matches!(effect, Effect::RunReconcile | Effect::Deferred);
                if deferred {
                    // §4.3-4② durable arm: `evict` clears `pullPending`
                    // IN ITS READ-IN TRANSACTION — the in-memory clear
                    // `apply` just did must land on disk now. The
                    // journal's own order is the ordering authority: a
                    // pull intent enqueued after this evict reads in
                    // later and lands a fresh `pullPending` (which
                    // survives), while anything queued before it is
                    // already wiped. No timestamp comparison can express
                    // that order — RFC3339 has only second resolution.
                    // The record file itself is NOT deleted — execution
                    // and its `materialized` commit still come first
                    // (§4.3-4③).
                    if matches!(intent, Intent::Evict { .. }) {
                        state::commit(sync_dir, state)?;
                    }
                } else {
                    // Commit-then-delete: crash between the two replays
                    // idempotently instead of losing the intent.
                    state::commit(sync_dir, state)?;
                    std::fs::remove_file(dir.join(&entry.name)).with_context(|| {
                        format!("deleting consumed journal record {}", entry.name)
                    })?;
                    fsync_dir(dir)?;
                }
                out.push(Consumed {
                    name: entry.name,
                    intent: Some(intent),
                    effect: Ok(effect),
                });
            }
        }
    }
    Ok(out)
}

/// `consume_ordered` without a parked set — every deferred record is a
/// first sighting. Test-only entry point: the production consumer is
/// `consume_ordered_parked` via the pool's `deferred_names`.
#[cfg(test)]
pub(crate) fn consume_ordered(
    dir: &Path,
    sync_dir: &Path,
    state: &mut State,
    tombstone_fp: impl FnMut(&State, &str) -> Option<String>,
) -> Result<Vec<Consumed>> {
    consume_ordered_parked(
        dir,
        sync_dir,
        state,
        tombstone_fp,
        &std::collections::HashSet::new(),
    )
}

pub(crate) fn sid_of(intent: &Intent) -> Option<&str> {
    match intent {
        Intent::Register { sid, .. }
        | Intent::Pull { sid, .. }
        | Intent::EnsureTask { sid, .. }
        | Intent::NoteRemoteDelete { sid, .. }
        | Intent::Evict { sid, .. }
        | Intent::Hydrate { sid, .. } => Some(sid),
        Intent::Reconcile { .. } => None,
    }
}

/// Delete debris and `*.tmp` residue files — the journal half of startup
/// cleanup step 2 (§9.1-2). Returns the number removed. Non-regular
/// entries (directories, vanished paths) are SKIPPED — `remove_file`
/// fails on a directory and a single bad entry must never wedge boot
/// cleanup.
pub fn remove_debris(dir: &Path, debris: &[Debris]) -> Result<usize> {
    let mut removed = 0;
    for d in debris {
        let path = dir.join(&d.name);
        if !path.is_file() {
            continue;
        }
        std::fs::remove_file(&path)
            .with_context(|| format!("removing journal debris {}", d.name))?;
        removed += 1;
    }
    if removed > 0 {
        fsync_dir(dir)?;
    }
    Ok(removed)
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::persist::fs::testkit::tmp;
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    fn setup(name: &str) -> (PathBuf, PathBuf, PathBuf) {
        let mirror = tmp("journal", name);
        let sync = state::ensure_sync_dir(&mirror).unwrap();
        let journal = sync.join("journal");
        ensure_dir(&journal, 0o700).unwrap();
        (mirror, sync, journal)
    }

    fn no_fp(_: &State, _: &str) -> Option<String> {
        None
    }

    fn write_intent(dir: &Path, name: &str, body: serde_json::Value) {
        std::fs::write(dir.join(name), body.to_string()).unwrap();
    }

    // T-510: five-step write — the formal name appears only via rename, the
    // record file is 0600, `*.tmp` residue never enters the consume set.
    #[test]
    fn t510_five_step_append_and_tmp_never_consumed() {
        let (mirror, _sync, journal) = setup("t510");
        let path = append(
            &journal,
            &Intent::Reconcile {
                enqueued_at: "2026-11-18T03:00:00Z".into(),
            },
        )
        .unwrap();
        assert_eq!(
            path.metadata().unwrap().permissions().mode() & 0o777,
            0o600
        );
        // No leftover *.tmp from the successful append.
        let (entries, debris) = read_pending(&journal).unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].record.is_ok());
        assert!(debris.is_empty(), "no tmp residue after rename: {debris:?}");

        // A manually-planted half-written tmp is residue, not a record.
        std::fs::write(journal.join("9999-9-9.tmp"), b"{\"op\":").unwrap();
        // And odd junk is debris too.
        std::fs::write(journal.join("README"), b"hi").unwrap();
        let (entries, debris) = read_pending(&journal).unwrap();
        assert_eq!(entries.len(), 1, "tmp must not enter the consume set");
        assert_eq!(debris.len(), 2);
        assert_eq!(
            remove_debris(&journal, &debris).unwrap(),
            2,
            "startup cleanup removes residue"
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-511: 4096 unconsumed records is a loud append failure.
    #[test]
    fn t511_backlog_limit_fails_loudly() {
        let (mirror, _sync, journal) = setup("t511");
        for i in 0..MAX_PENDING {
            std::fs::write(
                journal.join(format!("{:013}-1-1.json", i as u64)),
                r#"{"op":"reconcile","scope":"all","enqueuedAt":"t"}"#,
            )
            .unwrap();
        }
        let err = append(
            &journal,
            &Intent::Reconcile {
                enqueued_at: "t".into(),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("4096"), "{err}");
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-501/T-512: intents read in filename order; register lands
    // `registered{kind:"b-owned",at}` through commit; consume deletes the
    // file only after the commit.
    #[test]
    fn t501_register_intent_lands_and_orders() {
        let (mirror, sync, journal) = setup("t501");
        let mut state = State::empty_v3();
        // Later filename consumed second regardless of write order.
        write_intent(
            &journal,
            "100-1-1.json",
            json!({"op":"pull","sid":"s2","target":"latest","enqueuedAt":"t2"}),
        );
        write_intent(
            &journal,
            "99-1-1.json",
            json!({"op":"register","sid":"s1","kind":"b-owned","enqueuedAt":"t1"}),
        );
        let consumed = consume_ordered(&journal, &sync, &mut state, no_fp).unwrap();
        assert_eq!(consumed.len(), 2);
        // RFC §4.3-1 orders by FILE NAME — plain lexicographic. Real
        // names carry 13-digit unix-ms so lexicographic == numeric there;
        // these names deliberately cross a digit boundary to pin the rule.
        assert_eq!(consumed[0].name, "100-1-1.json", "lexicographic order");
        assert_eq!(consumed[1].name, "99-1-1.json");
        let s1 = &state.sessions["s1"];
        assert_eq!(s1.registered.as_ref().unwrap().kind, "b-owned");
        assert!(state.sessions["s2"].pull_pending.is_some());
        // Both records committed then deleted.
        assert!(std::fs::read_dir(&journal).unwrap().next().is_none());
        // On-disk state.json reflects the committed records.
        let disk = state::load(&sync).unwrap();
        assert!(disk.corruption.is_none());
        assert_eq!(disk.state.sessions.len(), 2);
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-502: same-sid pull intents merge — `target` takes max; two pinned
    // cursors compare by byte count; `latest` outranks any cursor.
    #[test]
    fn t502_pull_target_max_merge() {
        let (mirror, _sync, journal) = setup("t502");
        let cur = |off: u64| format!("zpc1.{off}.{}", "a".repeat(32));
        let mut state = State::empty_v3();
        for (name, target) in [
            ("1-1-1.json", cur(500)),
            ("2-1-1.json", cur(200)),
            ("3-1-1.json", cur(900)),
        ] {
            write_intent(
                &journal,
                name,
                json!({"op":"pull","sid":"s","target":target,"enqueuedAt":"t"}),
            );
        }
        consume_ordered(&journal, &state::ensure_sync_dir(&mirror).unwrap(), &mut state, no_fp)
            .unwrap();
        let pending = state.sessions["s"].pull_pending.as_ref().unwrap();
        assert_eq!(pending.target, cur(900), "max by byte count");

        // `latest` always wins; replaying a smaller target after it is a no-op.
        let mut st2 = State::empty_v3();
        for t in ["latest", cur(999).as_str(), cur(1).as_str()] {
            apply(
                &mut st2,
                &Intent::Pull {
                    sid: "s".into(),
                    target: t.into(),
                    enqueued_at: "t".into(),
                },
                None,
                "",
            );
        }
        assert_eq!(st2.sessions["s"].pull_pending.as_ref().unwrap().target, "latest");
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-503/T-513: ensure-task lands `validated` directly; phase only
    // advances (replayed validated under published is a no-op; validated
    // DOES reset degraded — the §6.3 recovery path).
    #[test]
    fn t503_ensure_task_phase_only_advances() {
        let mut state = State::empty_v3();
        let intent = |phase: Phase| Intent::EnsureTask {
            sid: "s".into(),
            file: "projects/lab/s.jsonl".into(),
            phase,
            enqueued_at: "t".into(),
        };
        apply(&mut state, &intent(Phase::Validated), None, "9-1-1.json");
        let task = state.sessions["s"].task.as_ref().unwrap();
        assert_eq!(task.phase, Phase::Validated);
        assert_eq!(task.file, "projects/lab/s.jsonl");

        // Forward: validated → published (worker confirmation path).
        apply(&mut state, &intent(Phase::Published), None, "9-1-1.json");
        assert_eq!(state.sessions["s"].task.as_ref().unwrap().phase, Phase::Published);
        // Replay: validated under published is a no-op (idempotency).
        apply(&mut state, &intent(Phase::Validated), None, "9-1-1.json");
        assert_eq!(state.sessions["s"].task.as_ref().unwrap().phase, Phase::Published);
        // Degraded terminal: a replayed validated cannot regress the slot —
        // but the DOCUMENTED recovery is a fresh validated intent resetting
        // degraded (§6.3); may_advance_to encodes exactly that edge.
        let mut st3 = State::empty_v3();
        st3.sessions.insert(
            "s".into(),
            SessionRecord {
                task: Some(state::Task {
                    file: "f".into(),
                    phase: Phase::Degraded,
                    first_seen: "t".into(),
                    enqueued_at: "t".into(),
                    last_error: Some("x".into()),
                }),
                ..SessionRecord::default()
            },
        );
        apply(&mut st3, &intent(Phase::Validated), None, "9-1-1.json");
        assert_eq!(st3.sessions["s"].task.as_ref().unwrap().phase, Phase::Validated);
    }

    // T-504: note-remote-delete lands the tombstone (fields cleared,
    // publishedAtDelete folded, caller-supplied fileId); reconcile is
    // reported deferred — a full pass runs before its file is deleted.
    #[test]
    fn t504_note_remote_delete_and_reconcile_deferral() {
        let (mirror, sync, journal) = setup("t504");
        let mut state = State::empty_v3();
        state.sessions.insert(
            "s".into(),
            SessionRecord {
                pull: Some(state::Pull {
                    cursor: "zpc1.9.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                    file: "projects/lab/s.jsonl".into(),
                }),
                published_at: Some("t0".into()),
                ..SessionRecord::default()
            },
        );
        write_intent(
            &journal,
            "1-1-1.json",
            json!({"op":"note-remote-delete","sid":"s","enqueuedAt":"t9"}),
        );
        write_intent(
            &journal,
            "2-1-1.json",
            json!({"op":"reconcile","scope":"all","enqueuedAt":"t10"}),
        );
        let consumed = consume_ordered(&journal, &sync, &mut state, |_st, sid| {
            // The probe only supplies fileId for tombstone intents; other
            // ops (like reconcile, which carries no sid) pass "".
            if sid == "s" {
                Some("sha256:deadbeef".into())
            } else {
                None
            }
        })
        .unwrap();
        let d = state.sessions["s"].deleted.as_ref().unwrap();
        assert!(d.published_at_delete);
        assert_eq!(d.file_id.as_deref(), Some("sha256:deadbeef"));
        assert!(state.sessions["s"].pull.is_none(), "tombstone clears pull");
        assert_eq!(consumed[1].effect.as_ref().unwrap(), &Effect::RunReconcile);
        // Deferred record stays on disk for the caller to finish.
        assert!(journal.join("2-1-1.json").exists());
        assert!(!journal.join("1-1-1.json").exists());
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-11 #2 (§4.3-1): replaying `note-remote-delete` against an
    // existing tombstone is Dropped, not re-buried — re-burying would
    // reset `publishedAtDelete` to false and overwrite `fileId` with a
    // probe answer taken after the fact.
    #[test]
    fn note_remote_delete_replay_on_tombstone_is_dropped() {
        let mut state = State::empty_v3();
        let delete = Intent::NoteRemoteDelete {
            sid: "s".into(),
            enqueued_at: "t9".into(),
        };
        // First application on a published record: the tombstone folds
        // `publishedAt` into `publishedAtDelete` and keeps the probe's
        // content fingerprint.
        apply(
            &mut state,
            &Intent::Pull {
                sid: "s".into(),
                target: "latest".into(),
                enqueued_at: "t0".into(),
            },
            None,
            "",
        );
        state
            .sessions
            .get_mut("s")
            .unwrap()
            .published_at = Some("t0".into());
        assert_eq!(
            apply(&mut state, &delete, Some("sha256:aa".into()), "9-1-1.json"),
            Effect::Applied
        );
        let tombstoned = state.sessions["s"].clone();
        assert!(tombstoned.deleted.as_ref().unwrap().published_at_delete);
        assert_eq!(
            tombstoned.deleted.as_ref().unwrap().file_id.as_deref(),
            Some("sha256:aa")
        );
        // Replay (probe now sees the file gone → None): every field of
        // the tombstone survives verbatim — the pre-fix replay folded a
        // cleared `publishedAt` into `publishedAtDelete=false` and
        // wiped `fileId` back to null.
        assert_eq!(apply(&mut state, &delete, None, "9-1-1.json"), Effect::Dropped);
        assert_eq!(state.sessions["s"], tombstoned, "replay must not rebury");
        let d = state.sessions["s"].deleted.as_ref().unwrap();
        assert!(d.published_at_delete);
        assert_eq!(d.file_id.as_deref(), Some("sha256:aa"));
    }

    // T-11 #3: a `pull` intent on a tombstoned record is Dropped at
    // read-in — `pullPending` there could never be satisfied (`pick`
    // skips `deleted` records), so landing it wedged `sync --wait` on an
    // unsatisfiable field. `ensure-task` drops the same way: a task row
    // on a tombstone is equally unpickable residue.
    #[test]
    fn pull_and_ensure_task_intents_drop_on_tombstone() {
        let mut state = State::empty_v3();
        state
            .sessions
            .entry("s".into())
            .or_default()
            .bury("t", Some("sha256:aa".into()));
        let tombstoned = state.sessions["s"].clone();
        let pull = Intent::Pull {
            sid: "s".into(),
            target: "latest".into(),
            enqueued_at: "t2".into(),
        };
        assert_eq!(apply(&mut state, &pull, None, "9-1-1.json"), Effect::Dropped);
        assert_eq!(
            state.sessions["s"], tombstoned,
            "drop must leave the tombstone verbatim"
        );
        assert!(state.sessions["s"].pull_pending.is_none());
        let ensure = Intent::EnsureTask {
            sid: "s".into(),
            file: "projects/lab/s.jsonl".into(),
            phase: Phase::Validated,
            enqueued_at: "t3".into(),
        };
        assert_eq!(apply(&mut state, &ensure, None, "9-1-1.json"), Effect::Dropped);
        assert_eq!(state.sessions["s"], tombstoned);
        assert!(state.sessions["s"].task.is_none());
        // The guard only fires on tombstones — a live record still lands.
        let live_pull = Intent::Pull {
            sid: "s2".into(),
            target: "latest".into(),
            enqueued_at: "t2".into(),
        };
        assert_eq!(apply(&mut state, &live_pull, None, "9-1-1.json"), Effect::Applied);
        assert!(state.sessions["s2"].pull_pending.is_some());
    }

    // T-513 (other ops): replaying a consumed intent stream yields the same
    // state — every op's second application equals its first.
    #[test]
    fn t513_replay_is_idempotent_for_all_applied_ops() {
        let mut a = State::empty_v3();
        let stream = [
            Intent::Register {
                sid: "s".into(),
                enqueued_at: "t".into(),
            },
            Intent::Pull {
                sid: "s".into(),
                target: "latest".into(),
                enqueued_at: "t".into(),
            },
            Intent::EnsureTask {
                sid: "s".into(),
                file: "f".into(),
                phase: Phase::Validated,
                enqueued_at: "t".into(),
            },
            Intent::NoteRemoteDelete {
                sid: "s".into(),
                enqueued_at: "t".into(),
            },
        ];
        for i in &stream {
            apply(&mut a, i, Some("sha256:aa".into()), "9-1-1.json");
        }
        let mut b = a.clone();
        for i in &stream {
            apply(&mut b, i, Some("sha256:aa".into()), "9-1-1.json");
        }
        assert_eq!(a, b, "second replay must be a no-op");
    }

    // evict/hydrate parse; tombstone hit drops them, otherwise they are
    // reported deferred — the materialize executor runs them and the
    // record file survives until its `materialized` commit lands.
    #[test]
    fn evict_hydrate_parse_and_defer() {
        let evict: Intent = parse_intent(
            json!({"op":"evict","sid":"s","mode":"drop","enqueuedAt":"t"}),
        )
        .unwrap();
        assert_eq!(
            evict,
            Intent::Evict {
                sid: "s".into(),
                mode: EvictMode::Drop,
                enqueued_at: "t".into()
            }
        );
        let mut state = State::empty_v3();
        // A registered sid defers; an unknown sid drops silently (apply
        // must not mint an all-empty record row for it).
        state.sessions.entry("s".into()).or_default();
        assert_eq!(apply(&mut state, &evict, None, "9-1-1.json"), Effect::Deferred);
        state
            .sessions
            .entry("s".into())
            .or_default()
            .bury("t", None);
        assert_eq!(apply(&mut state, &evict, None, "9-1-1.json"), Effect::Dropped);
        // Unknown op / malformed record are per-record errors.
        assert!(parse_intent(json!({"op":"frob"})).is_err());
        assert!(parse_intent(json!({"op":"pull","sid":"s"})).is_err());
    }

    // T-804, read-in rules (§4.3-4): ① tombstone drops the materialize
    // intent; ② evict clears pullPending in the same transaction;
    // ③ deferred records stay on disk until their materialized commit.
    #[test]
    fn t804_evict_read_in_rules() {
        let (mirror, sync, journal) = setup("t804");
        let mut state = State::empty_v3();
        state.sessions.insert(
            "s1".into(),
            SessionRecord {
                pull: Some(state::Pull {
                    cursor: "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                    file: "projects/lab/s1.jsonl".into(),
                }),
                pull_pending: Some(PullPending {
                    target: "latest".into(),
                    enqueued_at: "t".into(),
                    record: None,
                }),
                ..SessionRecord::default()
            },
        );
        write_intent(
            &journal,
            "1-1-1.json",
            json!({"op":"evict","sid":"s1","mode":"stub","enqueuedAt":"t9"}),
        );
        let consumed = consume_ordered(&journal, &sync, &mut state, no_fp).unwrap();
        assert_eq!(consumed.len(), 1);
        assert_eq!(consumed[0].effect, Ok(Effect::Deferred));
        // ② the pending pull goal is cleared at read-in — "reclaim the bytes"
        // outranks in-flight pulls. M-4: the clear lands DURABLY in the
        // read-in transaction (§4.3-4② literal), while ③ keeps the
        // record file for the executor.
        assert!(
            state.sessions["s1"].pull_pending.is_none(),
            "evict read-in clears pullPending"
        );
        let disk = state::load(&sync).unwrap();
        assert!(
            disk.state.sessions["s1"].pull_pending.is_none(),
            "read-in transaction commits the clear to disk"
        );
        assert!(disk.state.sessions["s1"].materialization().is_full());
        // ③ deferred: the record survives — its file deletes only after
        // the executor commits `materialized`.
        assert!(journal.join("1-1-1.json").exists(),
            "deferred record not deleted at read-in");

        // ① tombstone drops the intent outright — file deleted at once.
        write_intent(
            &journal,
            "2-1-1.json",
            json!({"op":"hydrate","sid":"s1","enqueuedAt":"t10"}),
        );
        write_intent(
            &journal,
            "3-1-1.json",
            json!({"op":"evict","sid":"s1","mode":"drop","enqueuedAt":"t11"}),
        );
        write_intent(
            &journal,
            "4-1-1.json",
            json!({"op":"note-remote-delete","sid":"s1","enqueuedAt":"t12"}),
        );
        let consumed = consume_ordered(&journal, &sync, &mut state, no_fp).unwrap();
        // `1-1-1` is still parked from the first drain — deferred records
        // re-appear in every consume until their commit lands.
        assert_eq!(consumed[0].name, "1-1-1.json");
        assert_eq!(consumed[0].effect, Ok(Effect::Deferred));
        assert_eq!(consumed[1].effect, Ok(Effect::Deferred), "hydrate deferred");
        assert_eq!(consumed[2].effect, Ok(Effect::Deferred), "evict before delete");
        assert_eq!(consumed[3].effect, Ok(Effect::Applied), "tombstone lands");
        // Replay after the tombstone: both intents now drop.
        write_intent(
            &journal,
            "5-1-1.json",
            json!({"op":"evict","sid":"s1","mode":"stub","enqueuedAt":"t13"}),
        );
        let consumed = consume_ordered(&journal, &sync, &mut state, no_fp).unwrap();
        let evict = consumed
            .iter()
            .find(|c| c.name == "5-1-1.json")
            .unwrap();
        assert_eq!(evict.effect, Ok(Effect::Dropped),
            "tombstone outranks materialize intents");
        assert!(!journal.join("5-1-1.json").exists(),
            "dropped record consumed normally");
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // R2-C1 core invariant: a deferred record re-appears in every drain
    // until its commit lands, but `apply` runs only on FIRST sighting —
    // the parked set must stop `pull_pending = None` re-firing on
    // records that arrived after the evict was consumed.
    #[test]
    fn parked_deferred_record_reemits_without_reapply() {
        let (mirror, sync, journal) = setup("parked");
        let mut state = State::empty_v3();
        state.sessions.insert(
            "s1".into(),
            SessionRecord {
                pull: Some(state::Pull {
                    cursor: "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                    file: "projects/lab/s1.jsonl".into(),
                }),
                ..SessionRecord::default()
            },
        );
        write_intent(
            &journal,
            "1-1-1.json",
            json!({"op":"evict","sid":"s1","mode":"drop","enqueuedAt":"t9"}),
        );
        write_intent(
            &journal,
            "2-1-1.json",
            json!({"op":"pull","sid":"s1","target":"latest","enqueuedAt":"t10"}),
        );
        // Same batch: evict clears nothing (no pending yet), the pull
        // lands `pullPending` stamped with its own record name.
        let consumed = consume_ordered_parked(
            &journal, &sync, &mut state, no_fp, &Default::default(),
        ).unwrap();
        assert_eq!(consumed[0].effect, Ok(Effect::Deferred));
        assert_eq!(consumed[1].effect, Ok(Effect::Applied));
        let pending = state.sessions["s1"].pull_pending.as_ref().unwrap();
        assert_eq!(pending.record.as_deref(), Some("2-1-1.json"));
        assert!(!journal.join("2-1-1.json").exists());

        // Re-drain with the evict parked: re-emitted Deferred, apply NOT
        // re-run — the post-evict pending survives verbatim and no
        // re-commit happens (the record would otherwise re-clear it).
        let parked: std::collections::HashSet<String> =
            ["1-1-1.json".to_string()].into_iter().collect();
        let consumed = consume_ordered_parked(
            &journal, &sync, &mut state, no_fp, &parked,
        ).unwrap();
        assert_eq!(consumed.len(), 1);
        assert_eq!(consumed[0].name, "1-1-1.json");
        assert_eq!(consumed[0].effect, Ok(Effect::Deferred),
            "parked record re-reports deferred for bookkeeping");
        assert!(
            state.sessions["s1"].pull_pending.is_some(),
            "parked replay must NOT re-fire the pullPending wipe"
        );
        assert!(journal.join("1-1-1.json").exists(),
            "parked record is never deleted by a drain");
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // R2-C1 restart arm: the parked set only lives in memory — a daemon
    // restart drains again with it EMPTY, so a still-parked evict record
    // re-runs `apply`. The provenance guard must stop that replay from
    // wiping a post-evict `pullPending` whose own commit landed durably
    // before the restart. Scenario: drain commits the pending → process
    // dies → state reloads from disk → the evict record (still on disk)
    // reads in a second time.
    #[test]
    fn restart_replay_of_parked_evict_keeps_post_evict_pending() {
        let (mirror, sync, journal) = setup("restart-evict");
        let mut state = State::empty_v3();
        state.sessions.insert(
            "s1".into(),
            SessionRecord {
                pull: Some(state::Pull {
                    cursor: "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                    file: "projects/lab/s1.jsonl".into(),
                }),
                ..SessionRecord::default()
            },
        );
        write_intent(
            &journal,
            "1-1-1.json",
            json!({"op":"evict","sid":"s1","mode":"drop","enqueuedAt":"t9"}),
        );
        write_intent(
            &journal,
            "2-1-1.json",
            json!({"op":"pull","sid":"s1","target":"latest","enqueuedAt":"t10"}),
        );
        // First drain: evict defers, pull lands `pullPending` stamped
        // `2-1-1.json` — and the durable commit carries both changes.
        consume_ordered_parked(
            &journal, &sync, &mut state, no_fp, &Default::default(),
        ).unwrap();
        assert!(journal.join("1-1-1.json").exists(), "evict still parked");

        // Simulate the restart: the in-memory state is discarded and
        // reloaded from disk; the parked set is empty again, so the
        // drain re-applies `1-1-1.json` to a state that already carries
        // the post-evict pending.
        let mut state = state::load(&sync).unwrap().state;
        let pending = state.sessions["s1"].pull_pending.as_ref().unwrap();
        assert_eq!(pending.record.as_deref(), Some("2-1-1.json"));

        let consumed = consume_ordered_parked(
            &journal, &sync, &mut state, no_fp, &Default::default(),
        ).unwrap();
        assert_eq!(consumed[0].effect, Ok(Effect::Deferred));
        let pending = state.sessions["s1"].pull_pending.as_ref().unwrap();
        assert_eq!(
            pending.record.as_deref(),
            Some("2-1-1.json"),
            "post-evict pending survives the restart re-apply"
        );

        // The converse still holds: a pending stamped BEFORE the evict
        // is wiped by replay — provenance-less counts as predating.
        state.sessions.get_mut("s1").unwrap().pull_pending =
            Some(PullPending {
                target: "latest".into(),
                enqueued_at: "t8".into(),
                record: Some("0-1-1.json".into()),
            });
        let consumed = consume_ordered_parked(
            &journal, &sync, &mut state, no_fp, &Default::default(),
        ).unwrap();
        assert_eq!(consumed[0].effect, Ok(Effect::Deferred));
        assert!(
            state.sessions["s1"].pull_pending.is_none(),
            "pre-evict pending is still wiped on replay"
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Evict on an unknown sid is `Dropped` at read-in: the record file is
    // consumed and deleted like any applied intent, and NO record is
    // minted — an empty row would otherwise suspend forever as row 26.
    #[test]
    fn evict_on_unknown_sid_drops_without_minting_record() {
        let (mirror, sync, journal) = setup("evict-ghost");
        let mut state = State::empty_v3();
        write_intent(
            &journal,
            "1-1-1.json",
            json!({"op":"evict","sid":"ghost","mode":"drop","enqueuedAt":"t"}),
        );
        let consumed = consume_ordered(&journal, &sync, &mut state, no_fp).unwrap();
        assert_eq!(consumed[0].effect, Ok(Effect::Dropped));
        assert!(!journal.join("1-1-1.json").exists());
        assert!(!state.sessions.contains_key("ghost"));
        let disk = state::load(&sync).unwrap();
        assert!(!disk.state.sessions.contains_key("ghost"));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // R2-I2: `ensure-task` rejects terminal phases at the wire — a
    // `published` record would mint a fake publish fact with zero RPCs.
    #[test]
    fn ensure_task_rejects_terminal_phases() {
        assert!(parse_intent(json!({
            "op":"ensure-task","kind":"publish","sid":"s",
            "file":"projects/lab/s.jsonl","phase":"published","enqueuedAt":"t"
        })).is_err());
        assert!(parse_intent(json!({
            "op":"ensure-task","kind":"publish","sid":"s",
            "file":"projects/lab/s.jsonl","phase":"degraded","enqueuedAt":"t"
        })).is_err());
        for phase in ["validated"] {
            assert!(
                parse_intent(json!({
                    "op":"ensure-task","kind":"publish","sid":"s",
                    "file":"projects/lab/s.jsonl","phase":phase,"enqueuedAt":"t"
                })).is_ok(),
                "phase {phase} is the only legitimate wire entry"
            );
        }
        // `writing` is rejected at the wire — RFC §4.1 admits `validated`
        // only; orphan `task(writing)` rows come from reconcile's own
        // commits, never through a journal intent.
        assert!(parse_intent(json!({
            "op":"ensure-task","kind":"publish","sid":"s",
            "file":"projects/lab/s.jsonl","phase":"writing","enqueuedAt":"t"
        })).is_err());
        // Path escape is rejected at ingest, not discovered mid-publish.
        assert!(parse_intent(json!({
            "op":"ensure-task","kind":"publish","sid":"s",
            "file":"../outside.jsonl","phase":"validated","enqueuedAt":"t"
        })).is_err());
        assert!(parse_intent(json!({
            "op":"ensure-task","kind":"publish","sid":"s",
            "file":"/etc/passwd","phase":"validated","enqueuedAt":"t"
        })).is_err());
    }

    // Debris cleanup skips non-regular entries — a directory inside
    // `journal/` is listed as debris but must not wedge the sweep.
    #[test]
    fn remove_debris_skips_directories() {
        let (mirror, _sync, journal) = setup("debris-dir");
        std::fs::create_dir(journal.join("oddball")).unwrap();
        std::fs::write(journal.join("stray"), b"x").unwrap();
        let (_, debris) = read_pending(&journal).unwrap();
        assert_eq!(debris.len(), 2);
        assert_eq!(remove_debris(&journal, &debris).unwrap(), 1);
        assert!(journal.join("oddball").is_dir());
        assert!(!journal.join("stray").exists());
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-514: journal append does not depend on a live writer — the intent
    // is durable the moment append returns.
    #[test]
    fn t514_append_persists_without_owner() {
        let (mirror, _sync, journal) = setup("t514");
        append(
            &journal,
            &Intent::Register {
                sid: "s".into(),
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        let (entries, _) = read_pending(&journal).unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].record.is_ok());
        std::fs::remove_dir_all(&mirror).unwrap();
    }
}
