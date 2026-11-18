//! Session classification — a pure function over the record's field
//! combination plus three content probes (RFC-0002-a-state §5–§7, §9). No "status"
//! is ever stored: every reconcile pass re-derives the class from
//! `{file?, pull?, task?, publishedAt?, registered?, materialized?} ×
//! {B file?}`.
//!
//! Fixed rule order: the tombstone filter ALWAYS runs first (§5) — a
//! `fileId` instance hit suppresses everything; then the 26-row joint
//! decision table of §6.1, first match wins (more specific rows ahead of
//! steady rows, exactly as the RFC numbers them). Rows that depend on
//! `file present on B?` evaluate only when the probe answered — an unreachable B
//! is "this round is indeterminate" for those rows, never "B has nothing".
//!
//! The module is deliberately IO-free: `FileProbe`/`BProbe` are injected
//! parameters so the whole table is table-driven unit-testable.

use super::persist::state::{Deleted, Materialized, Phase, SessionRecord, State};

// ----------------------------------------------------------------- probes ---

/// Local file evidence, gathered by the CALLER (reconcile / argv layer).
/// Fields that need content reads are `Option`s — absent means "no such
/// file", never guessed.
#[derive(Clone, Debug, Default)]
pub struct FileProbe {
    /// The `<sid>.jsonl` copy exists under `projects/**`.
    pub exists: bool,
    /// Byte length (append pre-check / length predicate input).
    pub len: u64,
    /// `sha256:<32hex>` of the full content — the tombstone `fileId`
    /// fingerprint and cursor-fingerprint algorithm (state::file_id_of).
    pub fingerprint: Option<String>,
    /// Any complete line carries `forkedFrom.sessionId` (§8.3 read rule).
    pub forked_from: bool,
    /// File is a generated stub shell: the single-line `type ==
    /// "zonepair-stub"` sentinel (tech-spec-p2 §3.2) — the probe itself
    /// is `materialize::is_stub_shell`, strict whole-file.
    pub is_stub_shell: bool,
    /// The path exists but could not be read — NOT absence. Callers
    /// must skip the sid this round; classifying an unreadable file as
    /// gone feeds destructive edges (a transient EACCES could tombstone
    /// a live publish task).
    pub unreadable: bool,
}

impl FileProbe {
    /// Nothing on disk.
    pub fn absent() -> Self {
        Self::default()
    }

    /// Probe from content the caller already read — pure, no IO. All
    /// content probes derive from the same byte view the writer sees.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut forked_from = false;
        for line in bytes.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(line) {
                if v
                    .get("forkedFrom")
                    .and_then(|f| f.get("sessionId"))
                    .is_some()
                {
                    forked_from = true;
                }
            }
        }
        FileProbe {
            exists: true,
            len: bytes.len() as u64,
            fingerprint: Some(super::persist::state::file_id_of(bytes)),
            forked_from,
            // The shell definition lives with the recipe that mints it.
            is_stub_shell: super::engine::materialize::is_stub_shell(bytes),
            // These bytes were read — that is the opposite of unreadable.
            unreadable: false,
        }
    }
}

/// The B-side existence answer. `Unknown` (unreachable / probe failed) is
/// NOT "absent": rows depending on it skip this round rather than act on a
/// guess (§6 preamble).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BProbe {
    /// B confirms the session exists (`B has data` / `B has the session`).
    Present,
    /// B answered 404.
    Absent,
    /// Probe failed — no answer this round.
    Unknown,
}

// ---------------------------------------------------------------- verdict ---

/// What the matched row tells the executor to do. This is the
/// classification output, not an execution plan — worker/reconcile turn it
/// into requests and commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Normal state — nothing to do.
    Steady,
    /// Wait legitimately: for an intent, for B to produce data, or for the
    /// reconcile pass to clean up. NOT suspended — `ensure_published`
    /// keeps waiting through this.
    Hold,
    /// `task{writing}` — hold awaiting `ensure-task`, or the orphan
    /// permanently-suspended registration (§4.2): registered `writing`
    /// with no spawn evidence, no forward path.
    WritingHold { orphan: bool },
    /// `task{validated}` — re-enter the worker and drive the publish.
    Publish,
    /// The publish product vanished: terminate the task and commit the
    /// tombstone (§4.1 down-edge, table-internalized).
    TerminateWithTombstone,
    /// Published artifact deleted locally: tombstone
    /// `{publishedAtDelete: true, fileId: null}`; pull is void. The flag
    /// is constant at construction — the executor recomputes it inside
    /// `bury()`.
    Tombstone,
    /// Tombstone instance hit: the file "should be gone but is there" —
    /// suppress everything, delete it again.
    SuppressAndDelete,
    /// Suspended: a diagnosable failure — reconcile marks it, workers see
    /// `Degraded(suspended)`. The reason distinguishes "waiting for a
    /// human" from "field corruption" for reporting.
    Suspend(SuspendReason),
    /// `pull∧unloaded∧file` — hydrate crash window or external write:
    /// re-pull the whole file and commit `materialized="full"` in the same
    /// transaction.
    HydrateReset,
    /// First-pull family: reset-pull `[0,boundary)` and commit `pull`.
    /// `overwrite_local` covers the crash window (row 19 B-present): the
    /// local file is not trusted and gets replaced.
    FirstPull { overwrite_local: bool },
    /// No record + B has the session: backfill `registered` (after the
    /// tombstone filter, which already ran).
    BackfillRegistered,
}

/// Why a row suspended — reported to workers and `ensure_published`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuspendReason {
    /// Record field combination the normal transition graph cannot reach
    /// (row 9 `publishedAt∧no pull`, row 25 `materialized≠full` off-pull,
    /// row 15 non-shell under `stub`).
    FieldCorruption,
    /// Something a human must decide (B lost a published artifact, B-side
    /// delete vs local loss, orphan files, total unknown).
    ManualDecision,
    /// B-side file lost but record says we pulled it (row 18).
    BLostRecovery,
}

/// One verdict: the RFC row that fired (`0` = tombstone filter, which is
/// always evaluated before the numbered rows) and its action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Verdict {
    pub row: u8,
    pub action: Action,
}

/// Tombstone filter (§5), first always. `fileId == null` hits
/// unconditionally — it records "this id must never carry a file again",
/// a seal stronger than binding one old instance. A non-null `fileId`
/// binds the deleted file's instance: matching fingerprint suppresses;
/// mismatching means the tombstone belongs to an older instance and the
/// new file classifies normally.
pub fn tombstone_hit(tombstone: Option<&Deleted>, file: &FileProbe) -> bool {
    match tombstone {
        None => false,
        Some(d) => match &d.file_id {
            None => true, // sealed sid — always hit
            Some(id) => file.fingerprint.as_deref() == Some(id.as_str()),
        },
    }
}

// ------------------------------------------------------------- the table ---

struct Ctx<'a> {
    rec: Option<&'a SessionRecord>,
    file: &'a FileProbe,
    b: BProbe,
}

impl<'a> Ctx<'a> {
    fn r(&self) -> &'a SessionRecord {
        self.rec.unwrap_or(&EMPTY_RECORD)
    }
    fn has_task(&self) -> bool {
        self.r().task.is_some()
    }
    fn phase_is(&self, p: Phase) -> bool {
        self.r().task.as_ref().is_some_and(|t| t.phase == p)
    }
    fn full(&self) -> bool {
        self.r().materialization().is_full()
    }
    fn mat(&self) -> Materialized {
        self.r().materialization()
    }
    /// §3.3: `no pull` means no `pull` FIELD — `pullPending` never counts.
    fn has_pull(&self) -> bool {
        self.r().pull.is_some()
    }
    /// Rows 23/24/26's "no record/all-empty": no fields at all (pullPending counts
    /// as a field even though it never classifies, so a record holding
    /// only a pending intent is not "empty").
    fn no_record_fields(&self) -> bool {
        let r = self.r();
        r.pull.is_none()
            && r.pull_pending.is_none()
            && r.task.is_none()
            && r.published_at.is_none()
            && r.deleted.is_none()
            && r.registered.is_none()
            && r.materialized.is_none()
    }
    fn needs_b(&self) -> bool {
        self.b == BProbe::Unknown
    }
}

static EMPTY_RECORD: SessionRecord = SessionRecord {
    pull: None,
    pull_pending: None,
    task: None,
    published_at: None,
    deleted: None,
    registered: None,
    materialized: None,
    last_unchanged_at: None,
};

struct Row {
    /// RFC-0002-a-state §6.1 row number — the source of truth for the order.
    n: u8,
    /// `true` when the predicate reads `file present on B?` — with `BProbe::Unknown`
    /// the row is skipped entirely (evaluated as "cannot judge"), letting a
    /// later B-independent row still act.
    needs_b: bool,
    test: fn(&Ctx) -> bool,
    act: fn(&Ctx) -> Action,
}

/// The 26 rows in RFC order — read alongside `docs/rfc/rfc-0002-a-state.md`
/// §6.1; every `n` is that table's row number.
static ROWS: &[Row] = &[
    // 1 — degraded terminal: never auto-reset (§4.1: no out-edges).
    Row {
        n: 1,
        needs_b: false,
        test: |c| c.phase_is(Phase::Degraded),
        act: |_| Action::Hold,
    },
    // 2 — writing ∧ file: hold for `ensure-task`; spawn evidence is the
    // only advance.
    Row {
        n: 2,
        needs_b: false,
        test: |c| c.has_task() && c.phase_is(Phase::Writing) && c.file.exists,
        act: |_| Action::WritingHold { orphan: false },
    },
    // 3 — validated ∧ file: re-enter the worker to publish.
    Row {
        n: 3,
        needs_b: false,
        test: |c| c.has_task() && c.phase_is(Phase::Validated) && c.file.exists,
        act: |_| Action::Publish,
    },
    // 4 — task ∧ no file (tombstone already missed): product vanished →
    // terminate + tombstone.
    Row {
        n: 4,
        needs_b: false,
        test: |c| c.has_task() && !c.file.exists,
        act: |_| Action::TerminateWithTombstone,
    },
    // 5 — published phase. The RFC calls this a pre-cleanup window with
    // reconcile removing the task row "next"; under first-match ordering
    // no row ever does, so the row is a persistent terminal — harmless:
    // `publishedAt`/`pull` are authoritative and wait predicates treat a
    // `published` task as terminal evidence.
    Row {
        n: 5,
        needs_b: false,
        test: |c| c.phase_is(Phase::Published),
        act: |_| Action::Steady,
    },
    // 6 — task ∧ pull handoff window (confirmation transaction landed,
    // cleanup pending): advance by the task's phase. Shadowed by rows 1–5
    // (see t456): every `task` shape classifies before this row can match,
    // so the RFC's `TaskPhase` action is documentation-only — the variant
    // is gone and no table entry emits it.
    // 7 — pull ∧ publishedAt ∧ {stub,unloaded}: published & pruned — the
    // `prune`/`sess delete` fork: normal, never a tombstone.
    Row {
        n: 7,
        needs_b: false,
        test: |c| {
            c.has_pull()
                && c.r().published_at.is_some()
                && !c.full()
                && c.b != BProbe::Absent
        },
        act: |_| Action::Steady,
    },
    // 8 — pull ∧ publishedAt ∧ file: the published-then-pulled steady row.
    Row {
        n: 8,
        needs_b: false,
        test: |c| {
            c.has_pull()
                && c.r().published_at.is_some()
                && c.file.exists
                && c.b != BProbe::Absent
        },
        act: |_| Action::Steady,
    },
    // 9 — publishedAt ∧ no pull ∧ file: unreachable by the normal
    // transition graph (`pull` and `publishedAt` commit together) — field
    // corruption, suspend.
    Row {
        n: 9,
        needs_b: false,
        test: |c| {
            c.r().published_at.is_some() && !c.has_pull() && c.file.exists
        },
        act: |_| Action::Suspend(SuspendReason::FieldCorruption),
    },
    // 10 — publishedAt ∧ no file ∧ {full,absent}: deleted after publish →
    // tombstone {publishedAtDelete:true, fileId:null}.
    Row {
        n: 10,
        needs_b: false,
        test: |c| {
            c.r().published_at.is_some() && !c.file.exists && c.full()
        },
        act: |_| Action::Tombstone,
    },
    // 11 — publishedAt ∧ file ∧ B 404: published artifact lost on B —
    // suspend, never silently republish.
    Row {
        n: 11,
        needs_b: true,
        test: |c| {
            c.r().published_at.is_some() && c.file.exists && c.b == BProbe::Absent
        },
        act: |_| Action::Suspend(SuspendReason::ManualDecision),
    },
    // 12 — pull ∧ stub ∧ file-is-shell: pruned B-owned — normal; no length
    // check, no auto re-pull.
    Row {
        n: 12,
        needs_b: false,
        test: |c| {
            c.has_pull()
                && c.mat() == Materialized::Stub
                && c.file.exists
                && c.file.is_stub_shell
                && c.b != BProbe::Absent
        },
        act: |_| Action::Steady,
    },
    // 13 — pull ∧ {stub,unloaded} ∧ no file: unloaded steady state — wait
    // for hydrate or traffic (a bare-`rm`ed stub lands here too). Yields
    // to B-404 so a B-loss still surfaces on pruned records (§6 exemption:
    // `pull ∧ B 404` judgments are NOT exempted).
    Row {
        n: 13,
        needs_b: false,
        test: |c| c.has_pull() && !c.full() && !c.file.exists && c.b != BProbe::Absent,
        act: |_| Action::Steady,
    },
    // 14 — pull ∧ unloaded ∧ file: hydrate crash window or external
    // write — whole-file re-pull, same-transaction `full`.
    Row {
        n: 14,
        needs_b: false,
        test: |c| {
            c.has_pull()
                && c.mat() == Materialized::Unloaded
                && c.file.exists
                && c.b != BProbe::Absent
        },
        act: |_| Action::HydrateReset,
    },
    // 15 — pull ∧ stub ∧ file-not-shell: external write or tampered
    // record — the shell is recognizable, anything else is suspect.
    Row {
        n: 15,
        needs_b: false,
        test: |c| {
            c.has_pull()
                && c.mat() == Materialized::Stub
                && c.file.exists
                && !c.file.is_stub_shell
        },
        act: |_| Action::Suspend(SuspendReason::FieldCorruption),
    },
    // 16 — pull ∧ file ∧ {full,absent}, no task/publishedAt: the B-owned
    // steady row (length predicate §7.2 applies on top).
    Row {
        n: 16,
        needs_b: false,
        test: |c| {
            c.has_pull() && c.file.exists && c.full() && c.b != BProbe::Absent
        },
        act: |_| Action::Steady,
    },
    // 17 — pull ∧ no file ∧ {full,absent}, no publishedAt: B-side delete
    // or local loss — suspend for a human; don't rebuild another's delete.
    Row {
        n: 17,
        needs_b: false,
        test: |c| c.has_pull() && !c.file.exists && c.full(),
        act: |_| Action::Suspend(SuspendReason::ManualDecision),
    },
    // 18 — pull ∧ B 404: B lost it — suspend (mark + log, no auto
    // re-upload, no local delete). Reachable only when rows 13/17 did not
    // already suspend a file-absent record; the exemption (§6) explicitly
    // keeps this B-independent verdict reachable for materialized states.
    Row {
        n: 18,
        needs_b: true,
        test: |c| c.has_pull() && c.b == BProbe::Absent,
        act: |_| Action::Suspend(SuspendReason::BLostRecovery),
    },
    // 19 — registered ∧ no pull ∧ file: first-pull crash window — B has
    // data ⇒ reset-pull (overwrite local); B 404 ⇒ orphan suspend.
    Row {
        n: 19,
        needs_b: true,
        test: |c| {
            c.r().registered.is_some() && !c.has_pull() && c.file.exists
        },
        act: |c| {
            if c.b == BProbe::Present {
                Action::FirstPull {
                    overwrite_local: true,
                }
            } else {
                Action::WritingHold { orphan: true }
            }
        },
    },
    // 20 — registered ∧ no pull ∧ B 404 ∧ no file: registered, not yet
    // produced — the normal pre-state, wait for B.
    Row {
        n: 20,
        needs_b: true,
        test: |c| {
            c.r().registered.is_some()
                && !c.has_pull()
                && !c.file.exists
                && c.b == BProbe::Absent
        },
        act: |_| Action::Hold,
    },
    // 21 — registered ∧ no pull ∧ B has data ∧ no file: first pull builds
    // `pull` (`projects/lab/` convention).
    Row {
        n: 21,
        needs_b: true,
        test: |c| {
            c.r().registered.is_some()
                && !c.has_pull()
                && !c.file.exists
                && c.b == BProbe::Present
        },
        act: |_| Action::FirstPull {
            overwrite_local: false,
        },
    },
    // 22 — file ∧ forkedFrom ∧ no pull/task/registered/publishedAt: fork
    // candidate — register task(writing), do NOT publish.
    Row {
        n: 22,
        needs_b: false,
        test: |c| {
            c.file.exists
                && c.file.forked_from
                && !c.has_pull()
                && !c.has_task()
                && c.r().registered.is_none()
                && c.r().published_at.is_none()
        },
        act: |_| Action::WritingHold { orphan: false },
    },
    // 23 — no record ∧ B has the session: B-side backfill `registered`
    // (tombstone already ran — a sealed sid never reaches here).
    Row {
        n: 23,
        needs_b: true,
        test: |c| c.rec.is_none() && c.b == BProbe::Present,
        act: |_| Action::BackfillRegistered,
    },
    // 24 — no record fields ∧ file ∧ B 404: a hand-dropped non-fork file —
    // orphan: register `writing` + permanently suspend.
    Row {
        n: 24,
        needs_b: true,
        test: |c| {
            c.no_record_fields()
                && c.file.exists
                && !c.file.forked_from
                && c.b == BProbe::Absent
        },
        act: |_| Action::WritingHold { orphan: true },
    },
    // 25 — materialized≠full on a non-pull record: `materialized` only
    // means something on pull sessions — field corruption, suspend.
    Row {
        n: 25,
        needs_b: false,
        test: |c| !c.has_pull() && !c.full(),
        act: |_| Action::Suspend(SuspendReason::FieldCorruption),
    },
    // 26 — fully empty (no file, no fields): unknown — conservative
    // suspend; better stopped than wrong.
    Row {
        n: 26,
        needs_b: false,
        test: |c| c.rec.is_some() && c.no_record_fields() && !c.file.exists,
        act: |_| Action::Suspend(SuspendReason::ManualDecision),
    },
];

/// The joint classification: tombstone filter first (row `0`), then the
/// §6.1 table top-down. `record` is `None` when the sid has no entry at
/// all — needed to distinguish row 23 (`no record`) from row 26 (`all-empty`).
pub fn classify(
    record: Option<&SessionRecord>,
    file: &FileProbe,
    b: BProbe,
) -> Verdict {
    // §5 — the tombstone filter always precedes the table. `fileId` null
    // is "sealed, file must never exist"; a fingerprint hit binds the
    // instance. Either way: suppress + re-delete.
    if tombstone_hit(record.and_then(|r| r.deleted.as_ref()), file) {
        return Verdict {
            row: 0,
            action: Action::SuppressAndDelete,
        };
    }
    let ctx = Ctx {
        rec: record,
        file,
        b,
    };
    for row in ROWS {
        if row.needs_b && ctx.needs_b() {
            continue; // B unanswerable — skip the row, judge next round
        }
        if (row.test)(&ctx) {
            return Verdict {
                row: row.n,
                action: (row.act)(&ctx),
            };
        }
    }
    // The table is not meant to be left un-hit; a record carrying only
    // `pullPending` (or a hand-dropped file with B still silent) lands
    // here — wait a round rather than suspend a legitimate state.
    Verdict {
        row: u8::MAX,
        action: Action::Hold,
    }
}

/// §6.2 — the continuation-fork predicate: all four conditions ⇒ the file
/// is a fork product and `--resume` must wait for publish. `pull` or
/// `publishedAt` alone makes it an ordinary resume (a published fork's
/// ORIGIN session hits this too — same file, same marker; the persisted
/// state is the only thing telling the two apart).
pub fn continuation_fork(record: Option<&SessionRecord>, file: &FileProbe) -> bool {
    let r = record.unwrap_or(&EMPTY_RECORD);
    file.forked_from
        && r.pull.is_none()
        && r.published_at.is_none()
        && !tombstone_hit(r.deleted.as_ref(), file)
}

/// §7.2 — the length predicate, partitioned by caller. Startup reconcile
/// trusts "I wrote a torn tail" (`> bytes` ⇒ truncate); steady-state
/// reconcile distrusts ANY mismatch (an unknown external writer touched a
/// B-owned file — degrade and re-pull whole).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LengthSource {
    /// Crash recovery — own torn-tail writes are the expected mismatch.
    Startup,
    /// Periodic / manual sync — no benign explanation for a mismatch.
    Steady,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LengthVerdict {
    /// Local length equals the cursor byte count — clean.
    Clean,
    /// `local > bytes` at startup: truncate to the cursor offset, then
    /// resume the normal incremental pull.
    Truncate { to: u64 },
    /// `local < bytes` at startup, or ANY mismatch at steady state:
    /// `degraded` → whole-file re-pull.
    Degraded,
}

pub fn length_verdict(source: LengthSource, local_len: u64, cursor_bytes: u64) -> LengthVerdict {
    match source {
        LengthSource::Startup => {
            if local_len == cursor_bytes {
                LengthVerdict::Clean
            } else if local_len > cursor_bytes {
                LengthVerdict::Truncate {
                    to: cursor_bytes,
                }
            } else {
                LengthVerdict::Degraded
            }
        }
        LengthSource::Steady => {
            if local_len == cursor_bytes {
                LengthVerdict::Clean
            } else {
                LengthVerdict::Degraded
            }
        }
    }
}

/// §9 — `sync --wait`'s structural predicate (pure, no clocks): journal
/// empty ∧ no `pullPending` ∧ every `task` terminal, absent, or a
/// spawn-evidence-less suspended `writing` orphan. A `pull`-absent session
/// is not a debt — registered-not-yet-produced and unloaded sessions are
/// not waited on. `journal_empty` is the caller's directory scan.
pub fn sync_settled(state: &State, journal_empty: bool) -> bool {
    if !journal_empty {
        return false;
    }
    state.sessions.values().all(|r| {
        r.pull_pending.is_none()
            && r.task.as_ref().is_none_or(|t| match t.phase {
                Phase::Published | Phase::Degraded => true,
                // writing with no spawn evidence = the orphan permanent
                // hold (§4.2): never terminal, but also not "work queued".
                Phase::Writing => true,
                Phase::Validated => false, // in-flight publish — keep waiting
            })
    })
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::cursor::ZERO;

    fn rec() -> SessionRecord {
        SessionRecord::default()
    }

    fn with_pull(r: &mut SessionRecord) -> &mut SessionRecord {
        r.pull = Some(super::super::persist::state::Pull {
            cursor: "zpc1.100.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            file: "projects/lab/s.jsonl".into(),
        });
        r
    }

    fn task(phase: Phase) -> SessionRecord {
        SessionRecord {
            task: Some(super::super::persist::state::Task {
                file: "f".into(),
                phase,
                first_seen: "t".into(),
                enqueued_at: "t".into(),
                last_error: None,
            }),
            ..rec()
        }
    }

    fn file(bytes: &[u8]) -> FileProbe {
        FileProbe::from_bytes(bytes)
    }

    const B_OFF: BProbe = BProbe::Present;

    // -- tombstone filter (T-421/422/423) ----------------------------------

    // T-421: non-null fileId + matching fingerprint ⇒ instance hit —
    // suppress + delete again.
    #[test]
    fn t421_tombstone_instance_hit_suppresses() {
        let bytes = b"{\"x\":1}\n";
        let mut r = rec();
        r.bury("t", Some(super::super::persist::state::file_id_of(bytes)));
        let v = classify(Some(&r), &file(bytes), B_OFF);
        assert_eq!((v.row, v.action), (0, Action::SuppressAndDelete));
    }

    // T-422: non-null fileId + mismatch ⇒ old-instance tombstone; the new
    // file classifies normally through the table.
    #[test]
    fn t422_tombstone_mismatch_is_old_instance() {
        let bytes = b"{\"x\":2}\n";
        let mut r = rec();
        r.bury("t", Some("sha256:00000000000000000000000000000000".into()));
        let v = classify(Some(&r), &file(bytes), B_OFF);
        assert_ne!(v.row, 0);
        // Fork marker + no fields ⇒ row 22 candidate.
        let forked = file(b"{\"forkedFrom\":{\"sessionId\":\"x\"}}\n");
        let v2 = classify(Some(&r), &forked, B_OFF);
        assert_eq!(v2.row, 22);
    }

    // T-423: ORDER counterexample — tombstone + pull + file + B 404 must
    // hit the tombstone first, not "B lost it" (row 18).
    #[test]
    fn t423_tombstone_precedes_b_lost() {
        let bytes = b"{\"x\":3}\n";
        let mut r = rec();
        with_pull(&mut r);
        r.bury("t", Some(super::super::persist::state::file_id_of(bytes)));
        let v = classify(Some(&r), &file(bytes), BProbe::Absent);
        assert_eq!((v.row, v.action), (0, Action::SuppressAndDelete));
        // fileId null: sealed sid — hits even with a DIFFERENT file.
        let mut sealed = rec();
        sealed.bury("t", None);
        let v2 = classify(Some(&sealed), &file(b"other\n"), BProbe::Absent);
        assert_eq!((v2.row, v2.action), (0, Action::SuppressAndDelete));
        // ...and hits even with no file at all.
        let v3 = classify(Some(&sealed), &FileProbe::absent(), BProbe::Absent);
        assert_eq!(v3.action, Action::SuppressAndDelete);
    }

    // -- the 26 rows (T-451–T-476, non-stub rows per brief) -----------------

    #[test]
    fn t451_row1_degraded_is_terminal() {
        let r = task(Phase::Degraded);
        let f = file(b"a\n");
        let v = classify(Some(&r), &f, B_OFF);
        assert_eq!((v.row, v.action), (1, Action::Hold));
        // Even with a pull riding along, degraded is evaluated first.
        let mut r2 = task(Phase::Degraded);
        with_pull(&mut r2);
        assert_eq!(classify(Some(&r2), &f, B_OFF).row, 1);
    }

    #[test]
    fn t452_row2_writing_holds() {
        let r = task(Phase::Writing);
        let v = classify(Some(&r), &file(b"a\n"), B_OFF);
        assert_eq!(
            (v.row, v.action),
            (2, Action::WritingHold { orphan: false })
        );
    }

    #[test]
    fn t453_row3_validated_publishes() {
        let r = task(Phase::Validated);
        let v = classify(Some(&r), &file(b"a\n"), B_OFF);
        assert_eq!((v.row, v.action), (3, Action::Publish));
    }

    #[test]
    fn t454_row4_task_no_file_terminates() {
        let r = task(Phase::Validated);
        let v = classify(Some(&r), &FileProbe::absent(), B_OFF);
        assert_eq!((v.row, v.action), (4, Action::TerminateWithTombstone));
    }

    #[test]
    fn t455_row5_published_window_is_steady() {
        let r = task(Phase::Published);
        let v = classify(Some(&r), &file(b"a\n"), B_OFF);
        assert_eq!((v.row, v.action), (5, Action::Steady));
    }

    // T-456: `task ∧ pull` (publish handoff). RFC row 6 IS in the table,
    // but under first-match it is shadowed — rows 1–5 already cover every
    // task×file combination, and their actions ARE "advance by the task's
    // phase". The test pins the shadowing rows so the equivalence is
    // documented rather than the dead row pretending to fire.
    #[test]
    fn t456_row6_task_plus_pull_is_handoff() {
        let mut r = task(Phase::Validated);
        with_pull(&mut r);
        let v = classify(Some(&r), &file(b"a\n"), B_OFF);
        assert_eq!(
            (v.row, v.action),
            (3, Action::Publish),
            "task∧pull advances by task phase — first-match hits row 3"
        );
        let mut p = task(Phase::Published);
        with_pull(&mut p);
        assert_eq!(classify(Some(&p), &file(b"a\n"), B_OFF).row, 5);
    }

    // T-801 §6.1 rows 7/12–15, the pruned-materialization family: published
    // ∧ pruned is normal; stub ∧ shell is normal and never re-pulled;
    // {stub,unloaded} ∧ no file is normal; unloaded ∧ file re-pulls to
    // full; stub ∧ non-shell suspends.
    #[test]
    fn t801_materialization_rows_table() {
        let shell = file(b"{\"type\":\"zonepair-stub\",\"stubVersion\":1}\n");
        let real = file(b"{\"x\":1}\n");
        let pull = || {
            let mut r = rec();
            with_pull(&mut r);
            r
        };
        let cases: Vec<(SessionRecord, FileProbe, u8, Action)> = {
            let mut v: Vec<(SessionRecord, FileProbe, u8, Action)> = Vec::new();
            // 7 — pull ∧ publishedAt ∧ pruned: the `prune`/`sess delete`
            // fork is normal, never a tombstone.
            for mat in [Materialized::Unloaded, Materialized::Stub] {
                let mut r = pull();
                r.published_at = Some("t".into());
                r.materialized = Some(mat);
                v.push((r, FileProbe::absent(), 7, Action::Steady));
            }
            // 12 — pull ∧ stub ∧ shell: steady, no length check, no re-pull.
            let mut r = pull();
            r.materialized = Some(Materialized::Stub);
            v.push((r.clone(), shell.clone(), 12, Action::Steady));
            // 13 — pull ∧ stub/unloaded ∧ no file: the pruned steady rows.
            v.push((r.clone(), FileProbe::absent(), 13, Action::Steady));
            let mut u = pull();
            u.materialized = Some(Materialized::Unloaded);
            v.push((u.clone(), FileProbe::absent(), 13, Action::Steady));
            // 14 — pull ∧ unloaded ∧ file: whole re-pull back to full.
            v.push((u, real.clone(), 14, Action::HydrateReset));
            // 15 — pull ∧ stub ∧ non-shell: suspend.
            v.push((
                r,
                real.clone(),
                15,
                Action::Suspend(SuspendReason::FieldCorruption),
            ));
            v
        };
        for (i, (r, f, row, action)) in cases.into_iter().enumerate() {
            let v = classify(Some(&r), &f, B_OFF);
            assert_eq!((v.row, v.action), (row, action), "case {i}");
        }
        // …but B-404 beats "file exists" for the SAME pull record (row 18
        // is B-independent and deliberately not exempted).
        let mut r = pull();
        r.materialized = Some(Materialized::Stub);
        assert_eq!(classify(Some(&r), &shell, BProbe::Absent).row, 18);
        // And B-404 on an unloaded record surfaces too — same as full's
        // file-absent suspension, B loss is never exempted.
        let mut u = pull();
        u.materialized = Some(Materialized::Unloaded);
        assert_eq!(
            classify(Some(&u), &FileProbe::absent(), BProbe::Absent).row,
            18
        );
    }

    #[test]
    fn t458_row8_pull_published_file_steady() {
        let mut r = rec();
        with_pull(&mut r);
        r.published_at = Some("t".into());
        let v = classify(Some(&r), &file(b"a\n"), B_OFF);
        assert_eq!((v.row, v.action), (8, Action::Steady));
    }

    #[test]
    fn t459_row9_published_without_pull_is_corrupt() {
        let mut r = rec();
        r.published_at = Some("t".into());
        let v = classify(Some(&r), &file(b"a\n"), B_OFF);
        assert_eq!(
            (v.row, v.action),
            (9, Action::Suspend(SuspendReason::FieldCorruption))
        );
    }

    #[test]
    fn t460_row10_published_deleted_tombstones() {
        let mut r = rec();
        r.published_at = Some("t".into());
        let v = classify(Some(&r), &FileProbe::absent(), B_OFF);
        assert_eq!((v.row, v.action), (10, Action::Tombstone));
    }

    #[test]
    fn t461_row11_published_b_lost_suspends() {
        // The row-11 shape is `pull ∧ publishedAt ∧ file ∧ B404`: without
        // `pull` the record is field-corrupt and row 9 wins first.
        let mut r = rec();
        r.published_at = Some("t".into());
        assert_eq!(classify(Some(&r), &file(b"a\n"), BProbe::Absent).row, 9);
        with_pull(&mut r);
        let v = classify(Some(&r), &file(b"a\n"), BProbe::Absent);
        assert_eq!(
            (v.row, v.action),
            (11, Action::Suspend(SuspendReason::ManualDecision))
        );
    }

    #[test]
    fn t466_row16_pull_file_full_is_steady() {
        let mut r = rec();
        with_pull(&mut r);
        let v = classify(Some(&r), &file(b"a\n"), B_OFF);
        assert_eq!((v.row, v.action), (16, Action::Steady));
    }

    #[test]
    fn t467_row17_pull_no_file_suspends() {
        let mut r = rec();
        with_pull(&mut r);
        let v = classify(Some(&r), &FileProbe::absent(), B_OFF);
        assert_eq!(
            (v.row, v.action),
            (17, Action::Suspend(SuspendReason::ManualDecision))
        );
    }

    #[test]
    fn t468_row18_pull_b404_suspends() {
        let mut r = rec();
        with_pull(&mut r);
        // pull ∧ file ∧ B404 → row 18's explicit suspend.
        let v = classify(Some(&r), &file(b"a\n"), BProbe::Absent);
        assert_eq!(
            (v.row, v.action),
            (18, Action::Suspend(SuspendReason::BLostRecovery))
        );
        // pull ∧ no file ∧ B404 suspends identically — via row 17 (file
        // absence is checked first, but the outcome is the same
        // "B-side loss — suspend for a human").
        let v = classify(Some(&r), &FileProbe::absent(), BProbe::Absent);
        assert_eq!(v.row, 17);
        assert!(matches!(v.action, Action::Suspend(_)));
    }

    #[test]
    fn t469_row19_registered_crash_window() {
        let mut r = rec();
        r.registered = Some(super::super::persist::state::Registered {
            kind: "b-owned".into(),
            at: "t".into(),
        });
        let f = file(b"a\n");
        // B has data → reset-pull overwriting the untrusted local file.
        let v = classify(Some(&r), &f, BProbe::Present);
        assert_eq!(
            (v.row, v.action),
            (
                19,
                Action::FirstPull {
                    overwrite_local: true
                }
            )
        );
        // B 404 → orphan suspend, NOT "publish it" — never misclassify a
        // crash-window file as A-owned.
        let v = classify(Some(&r), &f, BProbe::Absent);
        assert_eq!(
            (v.row, v.action),
            (19, Action::WritingHold { orphan: true })
        );
    }

    #[test]
    fn t470_row20_registered_no_file_b404_waits() {
        let mut r = rec();
        r.registered = Some(super::super::persist::state::Registered {
            kind: "b-owned".into(),
            at: "t".into(),
        });
        let v = classify(Some(&r), &FileProbe::absent(), BProbe::Absent);
        assert_eq!((v.row, v.action), (20, Action::Hold));
    }

    #[test]
    fn t471_row21_registered_no_file_b_data_pulls() {
        let mut r = rec();
        r.registered = Some(super::super::persist::state::Registered {
            kind: "b-owned".into(),
            at: "t".into(),
        });
        let v = classify(Some(&r), &FileProbe::absent(), BProbe::Present);
        assert_eq!(
            (v.row, v.action),
            (
                21,
                Action::FirstPull {
                    overwrite_local: false
                }
            )
        );
    }

    #[test]
    fn t472_row22_fork_candidate_registers_writing() {
        let f = file(b"{\"forkedFrom\":{\"sessionId\":\"parent\"}}\n");
        let v = classify(Some(&rec()), &f, B_OFF);
        assert_eq!(
            (v.row, v.action),
            (22, Action::WritingHold { orphan: false })
        );
    }

    #[test]
    fn t473_row23_no_record_b_present_backfills() {
        let v = classify(None, &FileProbe::absent(), BProbe::Present);
        assert_eq!((v.row, v.action), (23, Action::BackfillRegistered));
    }

    #[test]
    fn t474_row24_no_record_file_b404_orphan() {
        let v = classify(Some(&rec()), &file(b"a\n"), BProbe::Absent);
        assert_eq!(
            (v.row, v.action),
            (24, Action::WritingHold { orphan: true })
        );
    }

    #[test]
    fn t475_row25_materialized_off_pull_is_corrupt() {
        let mut r = rec();
        r.materialized = Some(Materialized::Unloaded);
        // no pull — materialized on a non-pull record is write-corruption.
        let v = classify(Some(&r), &FileProbe::absent(), B_OFF);
        assert_eq!(
            (v.row, v.action),
            (25, Action::Suspend(SuspendReason::FieldCorruption))
        );
        // Same field on a pull record is legitimate (row 13 catches it).
        with_pull(&mut r);
        assert_eq!(classify(Some(&r), &FileProbe::absent(), B_OFF).row, 13);
    }

    #[test]
    fn t476_row26_fully_empty_suspends() {
        let v = classify(Some(&rec()), &FileProbe::absent(), B_OFF);
        assert_eq!(
            (v.row, v.action),
            (26, Action::Suspend(SuspendReason::ManualDecision))
        );
    }

    // T-477: unreachable B never acts — B-dependent rows skip, and a
    // B-independent fallback may still answer this round.
    #[test]
    fn t477_b_probe_unknown_skips_b_rows() {
        let mut r = rec();
        with_pull(&mut r);
        // pull ∧ file ∧ full: row 16 is B-independent → still Steady.
        assert_eq!(
            classify(Some(&r), &file(b"a\n"), BProbe::Unknown).row,
            16
        );
        // pull ∧ no file: row 17 (no B read) suspends as usual.
        assert_eq!(classify(Some(&r), &FileProbe::absent(), BProbe::Unknown).row, 17);
        // registered ∧ file: row 19 needs B → skip → no earlier row ⇒
        // falls through past the table to the wait-a-round verdict.
        let mut reg = rec();
        reg.registered = Some(super::super::persist::state::Registered {
            kind: "b-owned".into(),
            at: "t".into(),
        });
        let v = classify(Some(&reg), &file(b"a\n"), BProbe::Unknown);
        assert_eq!((v.row, v.action), (u8::MAX, Action::Hold));
    }

    // -- §6.2 continuation-fork predicate (T-480–T-483) ---------------------

    #[test]
    fn t480_all_four_conditions_is_fork() {
        let f = file(b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n");
        assert!(continuation_fork(Some(&rec()), &f));
    }

    #[test]
    fn t481_pull_or_published_is_plain_resume() {
        let f = file(b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n");
        let mut r = rec();
        with_pull(&mut r);
        assert!(!continuation_fork(Some(&r), &f));
        r.pull = None;
        r.published_at = Some("t".into());
        assert!(!continuation_fork(Some(&r), &f));
    }

    #[test]
    fn t482_tombstone_hit_blocks_resume() {
        let bytes = b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n";
        let f = file(bytes);
        let mut r = rec();
        r.bury("t", Some(super::super::persist::state::file_id_of(bytes)));
        assert!(!continuation_fork(Some(&r), &f));
        // Sealed sid (fileId null) also blocks.
        r.bury("t", None);
        assert!(!continuation_fork(Some(&r), &f));
    }

    #[test]
    fn t483_zero_complete_lines_no_record_is_plain() {
        // A file with only a torn tail cannot carry forkedFrom — predicate
        // false ⇒ plain resume; B's loud resume-miss is the backstop.
        let f = file(b"{\"forkedFrom\":{\"sessionId\":\"p\"}"); // no \n
        assert!(!continuation_fork(Some(&rec()), &f));
        assert!(!continuation_fork(None, &f));
    }

    // -- §7.2 length predicate (T-490–T-492) --------------------------------

    #[test]
    fn t490_startup_longer_truncates() {
        assert_eq!(
            length_verdict(LengthSource::Startup, 150, 100),
            LengthVerdict::Truncate { to: 100 }
        );
    }

    #[test]
    fn t491_startup_shorter_degrades() {
        assert_eq!(
            length_verdict(LengthSource::Startup, 50, 100),
            LengthVerdict::Degraded
        );
        // Equal is clean.
        assert_eq!(
            length_verdict(LengthSource::Startup, 100, 100),
            LengthVerdict::Clean
        );
    }

    #[test]
    fn t492_steady_any_mismatch_degrades() {
        for len in [50, 150] {
            assert_eq!(
                length_verdict(LengthSource::Steady, len, 100),
                LengthVerdict::Degraded,
                "len={len}"
            );
        }
        assert_eq!(
            length_verdict(LengthSource::Steady, 100, 100),
            LengthVerdict::Clean
        );
    }

    // -- §9 sync --wait structural predicate (T-495) -------------------------

    #[test]
    fn t495_sync_wait_structural() {
        let mut st = State::empty_v3();
        assert!(sync_settled(&st, true));

        // pullPending blocks settle.
        st.sessions.insert(
            "a".into(),
            SessionRecord {
                pull_pending: Some(super::super::persist::state::PullPending {
                    target: "latest".into(),
                    enqueued_at: "t".into(),
                    record: None,
                }),
                ..rec()
            },
        );
        assert!(!sync_settled(&st, true));

        // validated task blocks; writing-orphan does not.
        st.sessions.clear();
        st.sessions.insert("b".into(), task(Phase::Validated));
        assert!(!sync_settled(&st, true));
        st.sessions.clear();
        st.sessions.insert("b".into(), task(Phase::Writing));
        st.sessions.insert("c".into(), task(Phase::Published));
        st.sessions.insert("d".into(), task(Phase::Degraded));
        assert!(sync_settled(&st, true));

        // A pull-absent session is not a debt; non-empty journal blocks.
        st.sessions.clear();
        st.sessions.insert(
            "e".into(),
            SessionRecord {
                registered: Some(super::super::persist::state::Registered {
                    kind: "b-owned".into(),
                    at: "t".into(),
                }),
                ..rec()
            },
        );
        assert!(sync_settled(&st, true));
        assert!(!sync_settled(&st, false));
    }

    // FileProbe::from_bytes is pure — no IO, all probes from one view.
    #[test]
    fn file_probe_from_bytes_is_pure() {
        let f = file(b"{\"a\":1}\n{\"forkedFrom\":{\"sessionId\":\"p\"}}\n");
        assert!(f.exists && f.forked_from);
        assert_eq!(f.len, 41);
        assert!(!f.is_stub_shell);
        let shell = file(b"{\"type\":\"zonepair-stub\",\"stubVersion\":1}\n");
        assert!(shell.is_stub_shell);
        // Zero cursor is fine as a "no pull yet" record, unused here.
        assert_eq!(ZERO, "zpc1.0.00000000000000000000000000000000");
    }
}
