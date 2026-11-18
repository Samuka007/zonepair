//! The wait predicate (RFC-0003-zonepaird §7): `ensure_published`.
//!
//! `wait_cursor` (§7.2's pinned-cursor waiter) shipped without a
//! consumer and was removed (impl/mirror-sync) — the §7.2 comparison
//! semantics survive in `worker::pending_satisfied`, which the pull
//! executor applies inside its commit transaction.
//!
//! **Authority is always the disk `state.json`** — a wait is a read loop
//! over committed records, never over the executor's memory. The
//! `watch::Receiver` an in-process caller may pass in `Waits::wake`
//! shortens the poll for same-process waiters; it is a cache, never a
//! truth source, and its absence changes semantics not at all. A
//! cross-process waiter (the transport argv layer, `sync --wait`) passes
//! `Waits::default()` and rides the ≤50 ms poll alone.
//!
//! `ensure_published`'s terminal branches evaluate in strict order
//! (§7.1), every one re-checked on every poll — they are verdicts, not
//! a state machine:
//!
//! 1. `task.phase == published`, or the task row is gone while
//!    `publishedAt` persists → `Published` (row cleanup races are
//!    invisible — the fact outlives the slot).
//! 2. a `deleted` tombstone → `Superseded` (the product vanished; resume
//!    must fail loudly, §8.1).
//! 3. `task.phase == degraded` → `Degraded(lastError)` — the persistent
//!    terminal, diagnosable.
//! 4. the session is suspended → `Degraded(suspended)`: in-process the
//!    pool's overlay answers directly (`Waits::suspend`); cross-process
//!    the verdict is re-derived with the same `classify` table the
//!    reconcile pass uses — a suspension reconcile would reach is never
//!    hidden from the waiter.
//! 5. only after every terminal branch misses does the wait continue —
//!    `task` absent with neither `publishedAt` nor `deleted` is a
//!    *pre-state* (the intent may not be read in yet), not a verdict.
//!    A session needing no publish at all — `pull` or `registered`
//!    present and never a task — answers `Ready` immediately.
//!
//! The waiter's revive obligation (§7.1 tail): every `revive_interval`
//! the loop probes `<mirror>/.sync/owner.lock` — free means the writer
//! died mid-wait, so the caller-supplied `revive` (production:
//! `ensure_owner()`) backstops a new one. Without it the waiter burns
//! its whole budget sitting on a consumed intent.
//!
//! The task budget (300 s) caps the wait; expiry is `Degraded`, never
//! a panic or a lie.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

use super::state_machine::{self, BProbe};
use super::engine::reconcile;
use super::engine::shared::{Shared, TASK_BUDGET};
use super::persist::state::{Phase, SessionRecord};
// "Writer died" probe — the one owner-lock try-lock implementation.
// Callers here spawn a daemon bound to that dir anyway, so the probe's
// create-on-peek side effect is benign.
use crate::daemon::owner::lock_free;

/// `ensure_published`'s answer set (§7.1 signature).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishWait {
    /// The session needs no publish: already B-owned (`pull`/`registered`
    /// present) and never a task — nothing to wait on.
    Ready,
    /// `phase=published` committed, or the task row is gone while
    /// `publishedAt` persists — the fact outlives the slot.
    Published,
    /// The product is gone (tombstone) — resume must not start.
    Superseded,
    /// Terminal failure: `degraded` carries `lastError`; a suspension
    /// reads `"suspended"`; budget exhaustion reads a timeout reason.
    Degraded(String),
}

/// Injectable knobs (T-611's budget edge, T-614's revive cadence): the
/// production defaults are the RFC constants; tests shrink them to
/// milliseconds instead of sleeping real seconds.
#[derive(Clone, Debug)]
pub struct WaitPolicy {
    /// Total wall budget — the §6.2 task budget (300 s) in production:
    /// publish upload time scales with fork size, so the wait budget is
    /// the task budget, not a small fixed cap.
    pub budget: Duration,
    /// Poll interval — §7's ≤50 ms cross-process poll.
    pub poll: Duration,
    /// Slow-tick cadence: owner-lock probe (+ revive) and the
    /// cross-process suspension re-derivation (a full file probe —
    /// page-cache cheap but not free, so it rides the slow tick).
    pub revive_interval: Duration,
}

impl Default for WaitPolicy {
    fn default() -> Self {
        Self {
            budget: TASK_BUDGET,
            poll: Duration::from_millis(50),
            revive_interval: Duration::from_secs(1),
        }
    }
}

/// Optional waiter inputs — every one absent keeps the poll-only
/// cross-process semantics (`Waits::default()`).
#[derive(Default)]
pub struct Waits {
    /// In-process wake-up cache (`Shared::notify`): receivers are woken
    /// on every commit touching the sid — never a truth source.
    pub wake: Option<watch::Receiver<()>>,
    /// In-process suspension answer (the pool's overlay): `true` means
    /// the sid is suspended right now. Cross-process callers leave this
    /// `None` and ride the classify re-derivation below.
    pub suspend: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    /// "Does B still hold this session" for the re-derivation —
    /// production wires `store_exists`; `None` answers `Unknown` (rows
    /// needing B skip — never guessed).
    pub b_probe: Option<Arc<dyn Fn(&str) -> BProbe + Send + Sync>>,
    /// The revive callback (production: `ensure_owner()`). Invoked at
    /// most once per `revive_interval`, only while `owner.lock` is free.
    pub revive: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Waits {
    /// Same-process wiring: subscribe to the sid's `watch` channel and
    /// read the pool's suspension overlay — the in-process fast path.
    /// `b_probe`/`revive` stay caller-supplied.
    pub fn in_process(shared: &Arc<Shared>, sid: &str) -> Self {
        let sh = Arc::clone(shared);
        let s = sid.to_string();
        Self {
            wake: Some(shared.notify.subscribe(sid)),
            suspend: Some(Arc::new(move || {
                sh.suspend.lock().unwrap().contains(&s)
            })),
            ..Default::default()
        }
    }
}


/// The cross-process suspension answer: re-run the §6.1 table exactly as
/// the reconcile pass would. `Action::Suspend` is the only verdict the
/// pool's overlay records — kept in parity so an in-process and a
/// cross-process waiter see the same failure. An unreadable file never
/// answers suspend — the reconcile pass skips the sid rather than
/// classifying absence it cannot prove.
fn suspend_rederived(shared: &Shared, sid: &str, rec: &SessionRecord, b: BProbe) -> bool {
    let file_rel = rec
        .task
        .as_ref()
        .map(|t| t.file.as_str())
        .or_else(|| rec.pull.as_ref().map(|p| p.file.as_str()));
    let (probe, _) = reconcile::probe_session_file(&shared.mirror, file_rel, sid);
    if probe.unreadable {
        return false;
    }
    matches!(
        state_machine::classify(Some(rec), &probe, b).action,
        state_machine::Action::Suspend(_)
    )
}

/// Cross-process suspension probe shared by both predicates — `None`
/// when an in-process overlay already answers (its check runs earlier
/// in the loop), or when the slow tick hasn't come due; the verdict is
/// the same re-derived `Suspend` classify the reconcile pass computes.
fn cross_process_suspended(
    shared: &Shared,
    sid: &str,
    rec: &SessionRecord,
    waits: &Waits,
    ctl: &LoopCtl,
) -> bool {
    waits.suspend.is_none()
        && ctl.slow_due()
        && suspend_rederived(shared, sid, rec, b_probe(waits, sid))
}

fn b_probe(waits: &Waits, sid: &str) -> BProbe {
    waits
        .b_probe
        .as_ref()
        .map(|p| p(sid))
        .unwrap_or(BProbe::Unknown)
}

/// Snapshot one record; the lock hold is one clone.
fn record_of(shared: &Shared, sid: &str) -> Option<SessionRecord> {
    shared.with_state(|st| st.sessions.get(sid).cloned())
}

/// The shared ticking step for both predicates: revive duty on the slow
/// tick, then sleep for `poll` (or until the wake cache fires, or the
/// budget runs out). `rest` returns `Some(())` on budget exhaustion.
struct LoopCtl {
    deadline: Instant,
    next_slow: Instant,
    poll: Duration,
    revive_interval: Duration,
}

impl LoopCtl {
    fn new(policy: &WaitPolicy) -> Self {
        Self {
            deadline: Instant::now() + policy.budget,
            // First slow tick is immediate — "who notices absence first
            // revives" applies to the waiter that just started.
            next_slow: Instant::now(),
            poll: policy.poll,
            revive_interval: policy.revive_interval,
        }
    }

    /// Slow tick due right now? Read-only — `rest` advances the meter.
    fn slow_due(&self) -> bool {
        Instant::now() >= self.next_slow
    }

    /// Run the revive duty + advance the slow meter, then sleep. Returns
    /// `Some(())` once the deadline has passed.
    async fn rest(
        &mut self,
        sync_dir: &Path,
        waits: &Waits,
        wake: &mut Option<watch::Receiver<()>>,
    ) -> Option<()> {
        if self.slow_due() {
            self.next_slow = Instant::now() + self.revive_interval;
            if let Some(revive) = &waits.revive {
                if lock_free(sync_dir) {
                    revive();
                }
            }
        }
        if Instant::now() >= self.deadline {
            return Some(());
        }
        let nap = self
            .poll
            .min(self.deadline.saturating_duration_since(Instant::now()));
        match wake {
            Some(rx) => {
                tokio::select! {
                    _ = tokio::time::sleep(nap) => {}
                    _ = rx.changed() => {}
                }
            }
            None => tokio::time::sleep(nap).await,
        }
        if Instant::now() >= self.deadline {
            Some(())
        } else {
            None
        }
    }
}

/// The waiter's revive duty, exposed for drain-style wait loops that
/// don't ride `LoopCtl` (`sync --wait` polls the §9 structural predicate
/// in `cli/`): probe the lock; when no writer holds it, spawn one
/// through `ensure_owner` — same rule as every other "who notices
/// absence first revives" path (§3.2/§3.4).
pub fn revive_dead_writer(sync_dir: &Path, mirror: &Path) {
    if lock_free(sync_dir) {
        let _ = crate::daemon::owner::ensure_owner_daemon(sync_dir, mirror);
    }
}

/// Block until `ensure_published`'s terminal branches close (§7.1).
/// `state.json` under `shared` is the only input; `waits` supplies the
/// optional in-process fast path and the revive hook.
pub async fn ensure_published(
    shared: &Shared,
    sid: &str,
    waits: &Waits,
    policy: &WaitPolicy,
) -> PublishWait {
    let mut ctl = LoopCtl::new(policy);
    let mut wake = waits.wake.clone();
    loop {
        // In-process suspension (a cheap overlay read) — every round,
        // ahead of the disk branches it would mask.
        if waits.suspend.as_ref().is_some_and(|s| s()) {
            return PublishWait::Degraded("suspended".into());
        }
        if let Some(rec) = record_of(shared, sid) {
            let phase = rec.task.as_ref().map(|t| t.phase);
            // ① the published fact — phase, or surviving `publishedAt`.
            if phase == Some(Phase::Published)
                || (rec.task.is_none() && rec.published_at.is_some())
            {
                return PublishWait::Published;
            }
            // ② tombstone — the product vanished.
            if rec.deleted.is_some() {
                return PublishWait::Superseded;
            }
            // ③ the persistent degraded terminal — diagnosable.
            if phase == Some(Phase::Degraded) {
                return PublishWait::Degraded(
                    rec.task
                        .as_ref()
                        .and_then(|t| t.last_error.clone())
                        .unwrap_or_else(|| "degraded".into()),
                );
            }
            if rec.task.is_none() && (rec.pull.is_some() || rec.registered.is_some()) {
                // Ready — unless a cross-process suspension masks it
                // (an in-process overlay already answered above).
                if cross_process_suspended(shared, sid, &rec, waits, &ctl) {
                    return PublishWait::Degraded("suspended".into());
                }
                return PublishWait::Ready;
            }
            // ⑤ keep waiting: task absent with neither `publishedAt`
            // nor a tombstone is a pre-state — but a cross-process
            // suspension (slow tick) still degrades rather than wait
            // out the whole budget.
            if cross_process_suspended(shared, sid, &rec, waits, &ctl) {
                return PublishWait::Degraded("suspended".into());
            }
        }
        // No record at all → the intent may not be read in yet: wait.
        if ctl.rest(&shared.sync_dir, waits, &mut wake).await.is_some() {
            return PublishWait::Degraded("wait budget exhausted".into());
        }
    }
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::state;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering as AOrd};

    fn setup(name: &str) -> (PathBuf, Arc<Shared>) {
        crate::sync::persist::fs::testkit::setup_shared("wait", name)
    }

    fn fast() -> WaitPolicy {
        WaitPolicy {
            budget: Duration::from_millis(80),
            poll: Duration::from_millis(2),
            revive_interval: Duration::from_millis(10),
        }
    }

    use crate::sync::persist::fs::testkit::write_file;

    // T-611: the five-branch order, one scenario per arm — published fact
    // (either shape), tombstone, degraded, suspended, and the
    // double-absent "keep waiting" that expires on budget.
    #[tokio::test]
    async fn t611_five_branch_order() {
        let (mirror, shared) = setup("t611");
        let waits = Waits::default();
        let pol = fast();

        // ① task absent ∧ publishedAt → Published (row already cleaned).
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "pubd".into(),
                    SessionRecord {
                        published_at: Some("t".into()),
                        pull: Some(state::Pull {
                            cursor: "zpc1.5.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                            file: "projects/lab/pubd.jsonl".into(),
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        write_file(&mirror, "projects/lab/pubd.jsonl", b"{'a':1}\n");
        assert_eq!(
            ensure_published(&shared, "pubd", &waits, &pol).await,
            PublishWait::Published
        );

        // ② tombstone → Superseded.
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "dead".into(),
                    SessionRecord {
                        deleted: Some(state::Deleted {
                            at: "t".into(),
                            published_at_delete: true,
                            file_id: None,
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        assert_eq!(
            ensure_published(&shared, "dead", &waits, &pol).await,
            PublishWait::Superseded
        );

        // ③ phase=degraded → Degraded(lastError).
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "bad".into(),
                    SessionRecord {
                        task: Some(state::Task {
                            file: "projects/lab/bad.jsonl".into(),
                            phase: Phase::Degraded,
                            first_seen: "t".into(),
                            enqueued_at: "t".into(),
                            last_error: Some("conflict".into()),
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        assert_eq!(
            ensure_published(&shared, "bad", &waits, &pol).await,
            PublishWait::Degraded("conflict".into())
        );

        // ④ suspended (in-process overlay) → Degraded(suspended) — a
        // validated task underneath must NOT mask it.
        shared.suspend.lock().unwrap().manual.insert("susp".into());
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "susp".into(),
                    SessionRecord {
                        task: Some(state::Task {
                            file: "projects/lab/susp.jsonl".into(),
                            phase: Phase::Validated,
                            first_seen: "t".into(),
                            enqueued_at: "t".into(),
                            last_error: None,
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        let inproc = Waits::in_process(&shared, "susp");
        assert_eq!(
            ensure_published(&shared, "susp", &inproc, &pol).await,
            PublishWait::Degraded("suspended".into())
        );

        // ⑤ no terminal branch ∧ no suspension → keep waiting until
        // budget → Degraded(timeout). (A `validated` task mid-flight is
        // the honest "not yet" shape — an all-empty record would hit
        // classify row 26 and suspend instead.)
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "quiet".into(),
                    SessionRecord {
                        task: Some(state::Task {
                            file: "projects/lab/quiet.jsonl".into(),
                            phase: Phase::Validated,
                            first_seen: "t".into(),
                            enqueued_at: "t".into(),
                            last_error: None,
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        write_file(&mirror, "projects/lab/quiet.jsonl", b"{\"forkedFrom\":{}}\n");
        let v = ensure_published(&shared, "quiet", &waits, &pol).await;
        assert!(
            matches!(&v, PublishWait::Degraded(r) if r.contains("budget")),
            "in-flight waits then degrades: {v:?}"
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // A mid-wait commit reaches the waiter: land `published` +
    // `publishedAt` + `pull` while ensure_published blocks.
    #[tokio::test]
    async fn published_mid_wait_wakes_and_returns() {
        let (mirror, shared) = setup("midwait");
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "s2".into(),
                    SessionRecord {
                        task: Some(state::Task {
                            file: "projects/lab/s2.jsonl".into(),
                            phase: Phase::Validated,
                            first_seen: "t".into(),
                            enqueued_at: "t".into(),
                            last_error: None,
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        write_file(&mirror, "projects/lab/s2.jsonl", b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n");
        let sh = shared.clone();
        let committer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(15)).await;
            sh.commit_state(|st| {
                let rec = st.sessions.get_mut("s2").unwrap();
                rec.task.as_mut().unwrap().phase = Phase::Published;
                rec.published_at = Some(crate::sync::persist::time::now_rfc3339());
                rec.begin_pull(state::Pull {
                    cursor: "zpc1.40.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                    file: "projects/lab/s2.jsonl".into(),
                });
            })
            .unwrap();
            sh.notify.signal("s2");
        });
        let waits = Waits::in_process(&shared, "s2");
        let v = ensure_published(&shared, "s2", &waits, &fast()).await;
        assert_eq!(v, PublishWait::Published);
        committer.await.unwrap();
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Ready: a pull session (or registered-only) with no task answers
    // immediately — nothing to publish.
    #[tokio::test]
    async fn ready_when_nothing_to_publish() {
        let (mirror, shared) = setup("ready");
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "bown".into(),
                    SessionRecord {
                        pull: Some(state::Pull {
                            cursor: "zpc1.5.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                            file: "projects/lab/bown.jsonl".into(),
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        write_file(&mirror, "projects/lab/bown.jsonl", b"{'a':1}\n");
        assert_eq!(
            ensure_published(&shared, "bown", &Waits::default(), &fast()).await,
            PublishWait::Ready
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Cross-process suspension: no overlay hook — the classify
    // re-derivation still catches a row-17 shape (pull ∧ file absent).
    #[tokio::test]
    async fn suspended_rederived_without_overlay() {
        let (mirror, shared) = setup("susp-derive");
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "gone-file".into(),
                    SessionRecord {
                        pull: Some(state::Pull {
                            cursor: "zpc1.5.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                            file: "projects/lab/gone-file.jsonl".into(),
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        // File deliberately absent → row 17 Suspend(ManualDecision).
        assert_eq!(
            ensure_published(&shared, "gone-file", &Waits::default(), &fast()).await,
            PublishWait::Degraded("suspended".into())
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-614: a dead writer must not strand the waiter — with `owner.lock`
    // free, the revive hook fires on the slow tick. With the lock held
    // (a live writer exists) it must NOT fire.
    #[tokio::test]
    async fn t614_waiter_revives_dead_owner() {
        let (mirror, shared) = setup("t614");
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "s".into(),
                    SessionRecord {
                        task: Some(state::Task {
                            file: "projects/lab/s.jsonl".into(),
                            phase: Phase::Validated,
                            first_seen: "t".into(),
                            enqueued_at: "t".into(),
                            last_error: None,
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        write_file(&mirror, "projects/lab/s.jsonl", b"{\"forkedFrom\":{}}\n");
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = calls.clone();
        let waits = Waits {
            revive: Some(Arc::new(move || {
                calls_c.fetch_add(1, AOrd::SeqCst);
            })),
            ..Default::default()
        };
        let _v = ensure_published(&shared, "s", &waits, &fast()).await;
        assert!(
            calls.load(AOrd::SeqCst) >= 1,
            "free owner.lock must trigger revive"
        );

        // Held lock → revive stays silent.
        let held = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(shared.sync_dir.join("owner.lock"))
            .unwrap();
        held.try_lock().unwrap();
        assert!(!lock_free(&shared.sync_dir));
        let calls2 = Arc::new(AtomicUsize::new(0));
        let calls2_c = calls2.clone();
        let waits2 = Waits {
            revive: Some(Arc::new(move || {
                calls2_c.fetch_add(1, AOrd::SeqCst);
            })),
            ..Default::default()
        };
        let _v = ensure_published(&shared, "s", &waits2, &fast()).await;
        assert_eq!(calls2.load(AOrd::SeqCst), 0, "held lock → no revive");
        held.unlock().unwrap();
        assert!(lock_free(&shared.sync_dir));
        std::fs::remove_dir_all(&mirror).unwrap();
    }
}
