//! The nine-step startup sequence (RFC-0003-zonepaird §9.1), run in strict order
//! inside `boot()`. Each step exists because a crash edge needs it:
//!
//! 1. **take the lock** — `.sync/` 0700 ensured, `flock(LOCK_EX|LOCK_NB)` on
//!    `owner.lock`; held → `AlreadyOwned` and the process exits
//!    immediately (the §3.2 self-convergence rule — the only thing
//!    keeping two zonepairds from double-writing).
//! 2. **clear half-finished work** — `*.zp*` residue under `projects/**` (crashed
//!    reset/evict temp files), `.sync/*.tmp` residue, journal `*.tmp`
//!    and non-formal debris, plus formal-but-unparseable journal
//!    records — all deleted before they can poison the consume set.
//! 3. **read the authoritative state** — `state::load`: missing → empty v3; v2 → in-place
//!    upgrade; corrupt/unknown-version/schema-violation → rename to
//!    `state.corrupt-<ts>` + empty table (load does the quarantine).
//! 4. **inherit the failure state** — a `state.corrupt-*` residue latches fail-closed:
//!    `Shared::fail_closed` is set, the loud log line names the
//!    quarantined file and the prune-fact loss, and every executor
//!    suspends while the latch lives.
//! 5. **rebuild the notify table** — REMOVED with `wait_cursor` (impl/mirror-sync):
//!    `Notify` survives as the in-process wake hint for `Waits::wake` —
//!    `subscribe` creates the sender lazily on first subscription.
//! 6. **replay the journal** — `drain_journal` consumes every pending record
//!    (commit-then-delete per record); under the fail-closed latch the
//!    replay still lands intents — execution is what suspends.
//! 7. **recover in-flight work** — `task{validated}` re-enters the publish queue
//!    (publish is idempotent); `pull` records resume from their
//!    committed cursor; `pullPending` re-queues itself (the pick's
//!    state scan covers it — no explicit enqueue needed).
//! 8. **startup reconcile** — `reconcile::run(PassKind::Startup)`: tombstone
//!    filter first, then the joint table; the startup length partition
//!    trusts `len > bytes` as our own torn tail and truncates.
//! 9. **main loop** — `boot` returns a `Booted`; `run()` hands it to the
//!    main loop (journal poll + E4 active set + periodic reconcile).

use anyhow::Result;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::sync::state_machine::BProbe;
use crate::sync::persist::journal::{self};
use crate::sync::engine::reconcile::{self, PassKind, ReconcileReport};
use crate::sync::persist::state::{self, Corruption, Phase};
use crate::sync::engine::{ExecCtx, Pool, Shared, Work};
use crate::transport::Base;

use super::owner::{self, Owner};

/// Everything `boot` produces: the held lock, the disk triple, the
/// scheduler, and what the sequence saw — the report tests assert on.
pub struct Booted {
    /// The held flock — must outlive the daemon (drop = release).
    pub owner: Owner,
    /// `<mirror>/.sync` — kept for `stop`/`owner.pid`/heartbeat paths.
    pub sync_dir: PathBuf,
    pub shared: Arc<Shared>,
    /// `Pool` owns the executor bundle (`ExecCtx`) — `boot`'s local
    /// `ctx` is consumed by step 8 and step 7's sweep enqueue, nothing
    /// downstream needs a second handle on it.
    pub pool: Arc<Pool>,
    pub report: StartupReport,
}

#[derive(Debug, Default)]
pub struct CleanupReport {
    /// `*.zp*` residue under `projects/**`.
    pub zp_removed: usize,
    /// journal debris (`*.tmp`, odd names, directories) removed.
    pub journal_debris_removed: usize,
    /// Formal-but-unparseable journal records removed (step 2's
    /// "empty/bad records → quarantine-delete" arm — removal, no quarantine).
    pub bad_records_removed: usize,
    /// `.sync/*.tmp` residue removed (crashed state-commit temp).
    pub sync_tmp_removed: usize,
}

impl CleanupReport {
    pub fn total(&self) -> usize {
        self.zp_removed + self.journal_debris_removed + self.bad_records_removed
            + self.sync_tmp_removed
    }
}

#[derive(Debug)]
pub struct StartupReport {
    pub cleanup: CleanupReport,
    /// The fail-closed latch, `Some` when step 3/4 quarantined or found
    /// a `state.corrupt-*` residue.
    pub corruption: Option<Corruption>,
    /// Journal records consumed at step 6 (applied + deferred + errors).
    pub replayed: usize,
    /// In-flight work re-queued at step 7 (validated tasks + pull rows).
    pub recovered: usize,
    /// The step-8 startup reconcile report.
    pub reconcile: ReconcileReport,
}

/// Why `boot` did not produce a running daemon.
#[derive(Debug)]
pub enum BootBlocked {
    /// §9.1-1: another writer holds `owner.lock` — exit at once.
    AlreadyOwned,
}

/// The endpoint bundle `run()` supplies and tests stub out: wire base +
/// token + the B-side existence probe (production = `store_exists` over
/// HTTP, tests = a constant `Unknown`/`Present` map).
#[derive(Clone, Default)]
pub struct ExecParts {
    pub base: Option<Base>,
    pub token: Option<String>,
    /// `file present on B?` — `Unknown` must be answered on probe failure.
    pub b_probe: Option<Arc<dyn Fn(&str) -> BProbe + Send + Sync>>,
}

impl ExecParts {
    fn into_ctx(self, shared: Arc<Shared>) -> ExecCtx {
        ExecCtx {
            shared,
            base: self.base,
            token: self.token,
            b_probe: self
                .b_probe
                .unwrap_or_else(|| Arc::new(|_| BProbe::Unknown)),
        }
    }
}

/// Steps 1–8 of §9.1. `Ok(Err(BootBlocked))` on a held lock; a real error
/// is `Err` — startup failures are loud, the daemon does not limp.
pub fn boot(mirror: &Path, parts: ExecParts) -> Result<std::result::Result<Booted, BootBlocked>> {
    // ── 1. take the lock ────────────────────────────────────────────
    let sync_dir = state::ensure_sync_dir(mirror)?;
    let Some(owner) = owner::acquire(&sync_dir)? else {
        return Ok(Err(BootBlocked::AlreadyOwned));
    };
    // The SIGTERM target rides next to the lock — best-effort; a stale
    // pid file is harmless (stop probes the lock anyway).
    let _ = owner.write_pid(&sync_dir);

    // ── 2. clear half-finished work ─────────────────────────────────
    let cleanup = cleanup(mirror, &sync_dir)?;

    // ── 3. read authority ── 4. inherit failure state ────────────────
    // `Shared::open` runs `state::load`: missing → empty v3, v2 →
    // upgrade, corrupt → quarantine + empty table, and any
    // `state.corrupt-*` residue latches `fail_closed` — steps 3 and 4
    // land together by construction.
    let (shared, corruption) = Shared::open(mirror)?;
    if let Some(c) = &corruption {
        eprintln!(
            "zonepaird: FAIL-CLOSED — {c}; all tasks suspend, journal replay \
             continues, orphaned files register task(writing) permanently held; \
             recover by fixing or deleting the state.corrupt-* file"
        );
    }

    let ctx = parts.into_ctx(shared.clone());
    let pool = Pool::new(ctx.clone());

    // ── 5. (removed) rebuild notify table — the §9.1-5 wake-cache rebuild retired
    // with `wait_cursor`: receivers it primed would never exist, and the
    // sender GC (`Notify::signal` drops receiver-less channels) made the
    // ritual a no-op. Step numbering preserved for §9.1 parity.

    // ── 6. replay the journal ───────────────────────────────────────
    // Fail-closed does not gate replay: intents land, execution suspends.
    let report = pool.drain_journal()?;
    let replayed = report.consumed.len();
    for (name, err) in &report.errors {
        eprintln!("zonepaird: journal record {name} unparseable after cleanup: {err}");
    }

    // ── 7. recover in-flight work ───────────────────────────────────
    // `validated` re-enters the publish queue — idempotent, safe to
    // redo. `pull` records re-enqueue to resume from `pull.cursor`;
    // `pullPending` needs no explicit enqueue (pick()'s state scan
    // resurfaces it every round — that IS its queue). `writing` holds:
    // the row itself is the queued state, only ensure-task advances it.
    let mut recovered = 0;
    {
        let susp = shared.suspend.lock().unwrap();
        let manual = susp.manual.clone();
        drop(susp);
        shared.with_state(|st| {
            for (sid, rec) in &st.sessions {
                if rec.deleted.is_some() {
                    continue;
                }
                if rec.task.as_ref().is_some_and(|t| t.phase == Phase::Validated)
                    && !manual.contains(sid)
                {
                    pool.enqueue(Work::Publish { sid: sid.clone() });
                    recovered += 1;
                }
                if rec.pull.is_some() && rec.materialization().is_full() {
                    pool.enqueue(Work::Pull {
                        sid: sid.clone(),
                        force_reset: false,
                    });
                    recovered += 1;
                }
            }
        });
    }

    // ── 8. startup reconcile ────────────────────────────────────────
    // Startup partition: `len > cursor bytes` truncates our own torn
    // tail; the steady partition (any mismatch → degraded) is wrong
    // here — the crash window guarantees benign residue exists.
    let reconcile = run_reconcile_pass(&ctx, &pool, PassKind::Startup)?;

    // ── 9. → main loop (caller) ─────────────────────────────────────
    let report = StartupReport {
        cleanup,
        corruption,
        replayed,
        recovered,
        reconcile,
    };
    Ok(Ok(Booted {
        owner,
        sync_dir,
        shared,
        pool,
        report,
    }))
}

/// One `reconcile::run` pass and the pool-side folding it produces —
/// `suspend.manual` rebuilt, recovered sids' pull overlay lifted,
/// produced work enqueued, committed sids woken. Sole caller: step 8's
/// Startup pass — the steady sweep rides `Work::Reconcile` inside the
/// pool instead (daemon/mod.rs).
fn run_reconcile_pass(
    ctx: &ExecCtx,
    pool: &Pool,
    kind: PassKind,
) -> Result<ReconcileReport> {
    let shared = &ctx.shared;
    let probe = Arc::clone(&ctx.b_probe);
    let prev: Vec<String> = shared
        .suspend
        .lock()
        .unwrap()
        .manual
        .iter()
        .cloned()
        .collect();
    let mut store = shared.clone();
    let report = reconcile::run(
        &shared.mirror,
        &shared.sync_dir,
        &mut store,
        kind,
        probe.as_ref(),
        &|sid: &str| prev.iter().any(|s| s == sid),
    )?;
    {
        let mut s = shared.suspend.lock().unwrap();
        s.manual = report.suspended.iter().cloned().collect();
        for sid in &report.unsuspended {
            s.pull.remove(sid);
        }
    }
    for w in &report.work {
        let work = match w {
            reconcile::Work::Publish { sid } => Work::Publish { sid: sid.clone() },
            reconcile::Work::Pull { sid, force_reset } => Work::Pull {
                sid: sid.clone(),
                force_reset: *force_reset,
            },
        };
        pool.enqueue(work);
    }
    for sid in &report.committed {
        shared.notify.signal(sid);
    }
    for note in &report.notes {
        eprintln!("zonepaird: reconcile: {note}");
    }
    Ok(report)
}

/// §9.1-2 — half-written artifacts of a crashed writer: `*.zp*` under
/// `projects/**`, `.sync/*.tmp`, journal debris (`*.tmp`, odd names,
/// directories) and formal-but-unparseable journal records. All of it
/// deletes — none of it is evidence a caller needs. Sole caller: `boot`.
fn cleanup(mirror: &Path, sync_dir: &Path) -> Result<CleanupReport> {
    let mut report = CleanupReport::default();

    // `*.zp*` residue under `projects/**` — crashed apply::reset /
    // evict temp files. The scan set already excludes them; this pass
    // deletes them so they never accumulate.
    let projects = mirror.join("projects");
    if projects.is_dir() {
        let mut stack = vec![projects];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir)?.flatten() {
                let path = entry.path();
                if entry.file_type().is_ok_and(|t| t.is_dir()) {
                    stack.push(path);
                    continue;
                }
                if entry
                    .file_name()
                    .to_str()
                    .is_some_and(|n| n.contains(".zp"))
                {
                    std::fs::remove_file(&path)?;
                    report.zp_removed += 1;
                }
            }
        }
    }

    // `.sync/*.tmp` — a crashed state-commit temp; never loadable, never
    // consumed. `owner.pid`'s `.tmp` sibling is swept by the same glob.
    if sync_dir.is_dir() {
        for entry in std::fs::read_dir(sync_dir)?.flatten() {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.ends_with(".tmp"))
                && entry.file_type().is_ok_and(|t| t.is_file())
            {
                std::fs::remove_file(entry.path())?;
                report.sync_tmp_removed += 1;
            }
        }
    }

    // Journal: debris (tmp residue, directories, odd names) plus
    // formal-named-but-unparseable records — both poison the consume
    // set if left; both delete outright (no quarantine for debris).
    let journal_dir = sync_dir.join("journal");
    let (entries, debris) = journal::read_pending(&journal_dir)?;
    report.journal_debris_removed += journal::remove_debris(&journal_dir, &debris)?;
    for entry in entries {
        if entry.record.is_err() {
            std::fs::remove_file(journal_dir.join(&entry.name))?;
            report.bad_records_removed += 1;
            eprintln!(
                "zonepaird: startup cleanup removed invalid journal record {}",
                entry.name
            );
        }
    }
    if report.total() > 0 {
        let _ = state::fsync_dir(sync_dir);
    }
    Ok(report)
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::persist::state::{Pull, PullPending, SessionRecord, State, Task};
    use crate::sync::engine::TaskOutcome;

    fn tmp(name: &str) -> PathBuf {
        crate::sync::persist::fs::testkit::tmp("startup", name)
    }

    fn unknown_b_probe() -> ExecParts {
        ExecParts {
            b_probe: Some(Arc::new(|_| BProbe::Unknown)),
            ..Default::default()
        }
    }

    fn write_file(mirror: &Path, rel: &str, bytes: &[u8]) {
        crate::sync::persist::fs::testkit::write_file(mirror, rel, bytes)
    }

    // T-630: boot step 1 — a held owner.lock makes the second boot
    // AlreadyOwned (the losing spawn's immediate exit).
    #[test]
    fn t630_held_lock_blocks_second_boot() {
        let mirror = tmp("t630-boot");
        let first = boot(&mirror, unknown_b_probe()).unwrap().unwrap();
        // `.sync/` + owner.lock materialized at step 1.
        assert!(first.sync_dir.join("owner.lock").exists());
        let second = boot(&mirror, unknown_b_probe()).unwrap();
        assert!(
            matches!(second, Err(BootBlocked::AlreadyOwned)),
            "held lock must block the second boot — self-convergence"
        );
        drop(first);
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-631: boot step 2 — `*.zp*` mirror residue, `.sync/*.tmp`,
    // journal `*.tmp` + odd debris, and a formal-but-unparseable record
    // all disappear; the VALID record survives for step 6 to consume.
    #[test]
    fn t631_cleanup_reclaims_half_written_artifacts() {
        let mirror = tmp("t631");
        let sync = state::ensure_sync_dir(&mirror).unwrap();
        let journal_dir = sync.join("journal");
        state::ensure_dir(&journal_dir, 0o700).unwrap();
        // Mirror *.zp residue (crashed reset temp).
        write_file(&mirror, "projects/lab/s1.jsonl.zp123", b"partial");
        write_file(&mirror, "projects/x/s2.jsonl.zp1", b"partial");
        // .sync/*.tmp (crashed state commit).
        std::fs::write(sync.join(".state.json.tmp"), b"{").unwrap();
        // Journal debris: a half-written append tmp + an odd name. Both
        // must read as ORPHANS of a dead writer, so age them past the
        // residue gate — a fresh *.tmp could be a live append in flight.
        std::fs::write(journal_dir.join("5-5-5.tmp"), b"{\"op\":").unwrap();
        std::fs::write(journal_dir.join("README"), b"junk").unwrap();
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(30);
        for name in ["5-5-5.tmp", "README"] {
            std::fs::File::options()
                .write(true)
                .open(journal_dir.join(name))
                .unwrap()
                .set_modified(old)
                .unwrap();
        }
        // Formal-but-unparseable record (empty + bad JSON).
        std::fs::write(journal_dir.join("6-1-1.json"), b"").unwrap();
        std::fs::write(journal_dir.join("7-1-1.json"), b"{not json").unwrap();
        // One valid record — step 6 must still consume it.
        std::fs::write(
            journal_dir.join("9-1-1.json"),
            r#"{"op":"register","sid":"s1","kind":"b-owned","enqueuedAt":"t"}"#,
        )
        .unwrap();

        let booted = boot(&mirror, unknown_b_probe()).unwrap().unwrap();
        let c = &booted.report.cleanup;
        assert_eq!(c.zp_removed, 2);
        assert_eq!(c.sync_tmp_removed, 1);
        assert_eq!(c.journal_debris_removed, 2, "tmp + odd name");
        assert_eq!(c.bad_records_removed, 2, "empty + unparseable");
        assert_eq!(booted.report.replayed, 1, "the valid record replays");
        assert!(
            booted
                .shared
                .with_state(|st| st.sessions["s1"].registered.is_some())
        );
        // The valid record is gone (consumed); nothing left but state.json.
        let left: Vec<_> = std::fs::read_dir(&journal_dir).unwrap().flatten().collect();
        assert!(left.is_empty(), "journal drains clean: {left:?}");
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-632: boot step 3 — missing state.json → empty v3; v2 upgrades;
    // corrupt → quarantine + fail-closed latch.
    #[test]
    fn t632_load_paths_missing_v2_corrupt() {
        // Missing → empty table.
        let mirror = tmp("t632a");
        let booted = boot(&mirror, unknown_b_probe()).unwrap().unwrap();
        assert!(booted.report.corruption.is_none());
        assert!(booted.shared.with_state(|st| st.sessions.is_empty()));
        drop(booted);

        // v2 upgrades in place (materialized absent ⇒ full).
        let mirror = tmp("t632b");
        let sync = state::ensure_sync_dir(&mirror).unwrap();
        std::fs::write(
            sync.join("state.json"),
            serde_json::json!({
                "version": 2,
                "sessions": {"s1": {"pull": {"cursor": "zpc1.5.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                            "file": "projects/lab/s1.jsonl"}}}
            })
            .to_string(),
        )
        .unwrap();
        let booted = boot(&mirror, unknown_b_probe()).unwrap().unwrap();
        assert!(booted.report.corruption.is_none());
        let rec = booted.shared.with_state(|st| st.sessions["s1"].clone());
        assert_eq!(rec.materialization(), state::Materialized::Full);
        drop(booted);

        // Corrupt → quarantined + fail-closed.
        let mirror = tmp("t632c");
        let sync = state::ensure_sync_dir(&mirror).unwrap();
        std::fs::write(sync.join("state.json"), b"{not json").unwrap();
        let booted = boot(&mirror, unknown_b_probe()).unwrap().unwrap();
        let c = booted.report.corruption.expect("corruption latches");
        assert!(c.quarantined.exists());
        assert!(!sync.join("state.json").exists());
        assert!(booted.shared.fail_closed.load(std::sync::atomic::Ordering::Relaxed));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-633: under the fail-closed latch the journal still replays —
    // the intent lands in state — but execution suspends: the pooled
    // work's run_one answers Pending, never executes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t633_fail_closed_replays_but_suspends_execution() {
        let mirror = tmp("t633");
        let sync = state::ensure_sync_dir(&mirror).unwrap();
        let journal_dir = sync.join("journal");
        state::ensure_dir(&journal_dir, 0o700).unwrap();
        // Latch fail-closed via a state.corrupt-* residue.
        std::fs::write(sync.join("state.corrupt-1"), b"junk").unwrap();
        // The file must exist — a task row over an absent file is a
        // legitimately-tombstoned "disappeared task" (T-654), which
        // would mask the fail-closed gate this test actually checks.
        write_file(&mirror, "projects/lab/s2.jsonl", b"{\"a\":1}\n");
        // Queue a publish intent — under fail-closed it must land in
        // state but never reach a worker.
        journal::append(
            &journal_dir,
            &journal::Intent::EnsureTask {
                sid: "s2".into(),
                file: "projects/lab/s2.jsonl".into(),
                phase: Phase::Validated,
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        let booted = boot(&mirror, unknown_b_probe()).unwrap().unwrap();
        // Replay landed: task row committed as validated.
        let task = booted
            .shared
            .with_state(|st| st.sessions["s2"].task.clone())
            .unwrap();
        assert_eq!(task.phase, Phase::Validated, "replay lands intents");
        // Step 7 even re-queues it — but execution is gated.
        let work = booted.pool.pick();
        assert!(matches!(work, Some(Work::Publish { .. })), "{work:?}");
        let outcome = booted
            .pool
            .run_one(&work.unwrap())
            .await;
        assert!(
            matches!(outcome, TaskOutcome::Pending(ref r) if r.contains("corrupt")),
            "fail-closed suspends execution: {outcome:?}"
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-634: step 7 re-queues in-flight work — a `validated` task
    // resurfaces Publish; a live `pull` record resumes (pullPending
    // self-resurfaces through the state scan).
    #[test]
    fn t634_inflight_recovery_requeues_validated_and_pull() {
        let mirror = tmp("t634");
        let sync = state::ensure_sync_dir(&mirror).unwrap();
        // Files the classify table needs to see (a `validated` row +
        // a steady pull row both probe `projects/**`).
        write_file(&mirror, "projects/lab/pub.jsonl", b"{\"forkedFrom\":{\"sessionId\":\"x\"}}\n");
        write_file(&mirror, "projects/lab/s1.jsonl", b"{\"a\":1}\n");
        let mut st = State::empty_v3();
        st.sessions.insert(
            "pub".into(),
            SessionRecord {
                task: Some(Task {
                    file: "projects/lab/pub.jsonl".into(),
                    phase: Phase::Validated,
                    first_seen: "t".into(),
                    enqueued_at: "t".into(),
                    last_error: None,
                }),
                ..Default::default()
            },
        );
        st.sessions.insert(
            "s1".into(),
            SessionRecord {
                pull: Some(Pull {
                    // Local file is exactly cursor bytes — steady row.
                    cursor: "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                    file: "projects/lab/s1.jsonl".into(),
                }),
                ..Default::default()
            },
        );
        st.sessions.insert(
            "pp".into(),
            SessionRecord {
                pull_pending: Some(PullPending {
                    target: "latest".into(),
                    enqueued_at: "t".into(),
                    record: None,
                }),
                ..Default::default()
            },
        );
        state::commit(&sync, &st).unwrap();

        let booted = boot(&mirror, unknown_b_probe()).unwrap().unwrap();
        assert!(booted.report.recovered >= 2);
        // Validated → Publish is the top pick (rank 0 beats pull).
        let first = booted.pool.pick().unwrap();
        assert_eq!(first, Work::Publish { sid: "pub".into() });
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-635 + step 8: the startup pass runs the tombstone filter first
    // — a tombstoned sid whose file lingers gets it re-deleted, never
    // re-classified as "B lost it".
    #[test]
    fn t635_startup_reconcile_tombstone_filter_first() {
        let mirror = tmp("t635");
        let sync = state::ensure_sync_dir(&mirror).unwrap();
        let bytes = b"{\"x\":1}\n";
        write_file(&mirror, "projects/lab/s7.jsonl", bytes);
        let mut st = State::empty_v3();
        let mut rec = SessionRecord {
            pull: Some(Pull {
                cursor: "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                file: "projects/lab/s7.jsonl".into(),
            }),
            ..Default::default()
        };
        rec.bury("t", Some(state::file_id_of(bytes)));
        st.sessions.insert("s7".into(), rec);
        state::commit(&sync, &st).unwrap();

        let booted = boot(&mirror, unknown_b_probe()).unwrap().unwrap();
        assert!(
            !mirror.join("projects/lab/s7.jsonl").exists(),
            "tombstoned file re-deleted at startup"
        );
        assert!(booted
            .report
            .reconcile
            .verdicts
            .iter()
            .any(|(sid, v)| sid == "s7" && v.row == 0));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Step 8's startup partition: a torn tail (`len > cursor bytes`)
    // truncates to the cursor — Steady would degrade the same shape.
    #[test]
    fn startup_pass_truncates_torn_tail() {
        let mirror = tmp("startup-trunc");
        let sync = state::ensure_sync_dir(&mirror).unwrap();
        // Cursor covers 7 bytes; file has 12 — our own torn tail.
        write_file(&mirror, "projects/lab/s1.jsonl", b"{'a':1}\nEXTR");
        let mut st = State::empty_v3();
        st.sessions.insert(
            "s1".into(),
            SessionRecord {
                pull: Some(Pull {
                    cursor: "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                    file: "projects/lab/s1.jsonl".into(),
                }),
                ..Default::default()
            },
        );
        state::commit(&sync, &st).unwrap();

        let booted = boot(&mirror, unknown_b_probe()).unwrap().unwrap();
        assert_eq!(
            std::fs::metadata(mirror.join("projects/lab/s1.jsonl"))
                .unwrap()
                .len(),
            7,
            "startup partition truncates, steady would degrade"
        );
        assert!(booted.report.reconcile.suspended.is_empty());
        std::fs::remove_dir_all(&mirror).unwrap();
    }
}
