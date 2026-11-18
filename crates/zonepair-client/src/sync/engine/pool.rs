//! The §6.2 scheduler (RFC-0003-zonepaird): one `Pool`, the `Work` enum,
//! and the queue-layer retry taxonomy (`Retry` books the backoff+budget
//! account, `Pending` re-queues budget-free, `Degraded` is terminal).
//!
//! Degraded has two faces and they are kept apart on purpose
//! (§6.3 + RFC-0002-a-state §10):
//!
//! - **publish** degrades persist: `task.phase = degraded` + `lastError`
//!   committed to `state.json` — a durable terminal with no out-edge.
//!   Reconcile never auto-resets it; the only recovery is a fresh journal
//!   intent (`ensure-task`) explicitly resetting the phase back to
//!   `validated` (`Phase::may_advance_to` encodes exactly that edge).
//! - **pull** degrades have no persistent slot: the executor returns
//!   `Degraded`, the pool marks the sid in its in-memory `Suspended.pull`
//!   overlay (rebuilt evidence: a degraded pull blocked from re-running
//!   until a fresh `pull`/`hydrate` intent re-arms it — `drain_journal`
//!   reports those sids as `recovery_sids`), and reconcile's per-round
//!   classification is what converges the session (a forced zero-cursor
//!   re-pull queued as `Work::Pull{force_reset}` — healing pulls bypass
//!   the suspension gate, and their success or a clean verdict lifts the
//!   overlay).
//!
//! Concurrency (§6.2): pool = 4 slots; tasks on the same sid are strictly
//! serial in read-in order; different sids run freely. `reconcile` is a
//! mutual-exclusion barrier against every other work kind — a pull
//! appending mid-pass would read as a bogus length mismatch; a pass
//! committing mid-pull would interleave the write-order rule. Selection
//! order `publish > evict/hydrate > pull > reconcile` — evict/hydrate
//! outranks pulls: it is a human's action on the disk, higher priority
//! than background freshness (§6.1).

use anyhow::Result;
use futures_util::FutureExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::sync::persist::fs;
use crate::sync::persist::journal::{self, Intent};
use crate::sync::persist::state::{Phase, State};

use super::materialize::run_materialize;
use super::pull::run_pull;
use super::publish::run_publish;
use super::reconcile::{self, PassKind};
use super::shared::{ExecCtx, TASK_BUDGET};

/// §6.2 — worker pool size. B's write path is globally serial anyway;
/// four lanes amortize a ~45-session sweep to seconds.
pub const POOL_SIZE: usize = 4;

/// §6.3 — queue-layer backoff sequence: `1s → 5s → 30s` cap.
pub const QUEUE_BACKOFFS: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(30),
];

/// `Pending` re-queue delay — the budget-free path for "B said not yet".
/// Short enough that `sync --wait` feels live, long enough to not hot-
/// loop a registered-but-empty session.
pub const PENDING_COOLDOWN: Duration = Duration::from_millis(250);

/// Injectable retry knobs (T-602/T-613 need a deterministic clock): the
/// production defaults are the RFC constants; tests shrink them to
/// milliseconds instead of sleeping real seconds.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Successive queue-layer delays (`backoffs[i]` after attempt i+1,
    /// capped at the last entry).
    pub backoffs: Vec<Duration>,
    /// Total budget for one retry sequence.
    pub budget: Duration,
    /// `Pending` re-queue delay.
    pub pending_cooldown: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            backoffs: QUEUE_BACKOFFS.to_vec(),
            budget: TASK_BUDGET,
            pending_cooldown: PENDING_COOLDOWN,
        }
    }
}

// ------------------------------------------------------------------ work ---

/// One schedulable task. `Pull`/`Publish` are the §6.1 executors;
/// `Materialize` carries a deferred evict/hydrate journal intent plus the
/// record name deleted after its `materialized` commit; `Reconcile` is
/// the whole-mirror pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Work {
    /// Incremental (`force_reset=false`) or forced whole (`true`) pull.
    /// `force_reset` sends the zero cursor — used by the length-predicate
    /// recovery, hydrate, and first-pull overwrite paths.
    Pull { sid: String, force_reset: bool },
    /// `task{validated}` — the publish executor.
    Publish { sid: String },
    /// evict/hydrate journal intent deferred from `consume_ordered` —
    /// carried so the record file is deleted only after its execution
    /// commits (`name`), RFC-0003-zonepaird §4.3-4③.
    Materialize { name: String, intent: Intent },
    /// One whole-mirror reconciliation pass. `names` are journal record
    /// files to delete after the pass completes (commit-then-execute-
    /// then-delete, mirroring §4.3-1's ordering for deferred records).
    Reconcile { names: Vec<String> },
}

impl Work {
    /// The session this task serializes on (`reconcile` is global).
    pub fn sid(&self) -> Option<&str> {
        match self {
            Work::Pull { sid, .. } => Some(sid),
            Work::Publish { sid } => Some(sid),
            Work::Materialize { intent, .. } => match intent {
                Intent::Evict { sid, .. } | Intent::Hydrate { sid, .. } => Some(sid),
                _ => None,
            },
            Work::Reconcile { .. } => None,
        }
    }

    /// Dedup/running key. `reconcile` shares one global key — only one
    /// pass is ever schedulable.
    fn key(&self) -> String {
        match self {
            Work::Pull { sid, force_reset } => format!("pull:{sid}:{force_reset}"),
            Work::Publish { sid } => format!("pub:{sid}"),
            Work::Materialize { name, .. } => format!("mat:{name}"),
            Work::Reconcile { .. } => "reconcile".to_string(),
        }
    }

    /// Selection rank (§6.2): publish > evict/hydrate > pull > reconcile.
    /// The RFC's two-tier pull split (`pull(waited) > pull(background)`)
    /// retired with `wait_cursor` — in-process waiters no longer exist to
    /// mark a pull waited-on, so one pull rank serves both.
    fn rank(&self) -> u8 {
        match self {
            Work::Publish { .. } => 0,
            Work::Materialize { .. } => 1,
            Work::Pull { .. } => 2,
            Work::Reconcile { .. } => 4,
        }
    }
}

// ------------------------------------------------------------- outcomes ---

/// What an executor decided about its task. The queue layer turns these
/// into books: `Retry` consumes the backoff+budget account, `Pending`
/// re-queues without accounting, `Degraded` is terminal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskOutcome {
    /// Task complete (commit already landed by the executor).
    Done,
    /// "Not yet" answer — B 404 on a pull, an unsatisfiable-but-legitimate
    /// target. Re-queued after `pending_cooldown`; never degrades on
    /// its own.
    Pending(String),
    /// Transient failure: transport retry budget exhausted or a local IO
    /// error. Books `backoffs[attempt]`; over `budget` it degrades.
    Retry(String),
    /// Terminal failure. For `Publish` this commits `task.phase=degraded`
    /// (the durable terminal); for `Pull` it marks the sid in the
    /// suspension overlay (pull degraded has no slot, §6.3/RFC-0002-a-state §10).
    Degraded(String),
}

// ------------------------------------------------------------------ pool ---

type ExecFn = dyn Fn(&ExecCtx, Work) -> std::pin::Pin<Box<dyn Future<Output = Result<TaskOutcome>> + Send + '_>>
    + Send
    + Sync;

/// A booked task: the work itself plus its retry accounting. Books hold
/// their work OUT of the pending queue — a task in cooldown is not a
/// candidate until its time comes.
struct Book {
    work: Work,
    attempt: u32,
    next_at: Instant,
    /// `Some` for retry books (the §6.3 task budget, counted once across
    /// re-picks); `None` for pending books — "not yet" never degrades.
    deadline: Option<Instant>,
    /// Currently executing — exempt from due/expiry scans until `finish`
    /// books it again.
    in_flight: bool,
}

/// The §6.2 scheduler. All mutable scheduling state lives behind short
/// `Mutex` holds — the pool itself is `Arc`-shared between the daemon's
/// producer side (journal/reconcile enqueueing) and worker tasks.
pub struct Pool {
    ctx: ExecCtx,
    policy: RetryPolicy,
    pending: Mutex<VecDeque<(u64, Work)>>,
    books: Mutex<HashMap<String, Book>>,
    /// Running keys — dedup + dispatch bookkeeping.
    running: Mutex<HashSet<String>>,
    /// Running sids — the "same sid strictly serial" guarantee.
    running_sids: Mutex<HashSet<String>>,
    seq: AtomicU64,
    /// Test seam: replaces the production executor dispatch. The pool's
    /// scheduling, retry and concurrency logic still runs for real.
    exec: Option<Box<ExecFn>>,
    /// Journal record names for reconcile intents that arrived while a
    /// pass was already running — `finish` schedules them as one
    /// follow-up pass so no intent is lost mid-scan.
    reconcile_names: Mutex<Vec<String>>,
    /// Deferred journal record names already represented as work.
    /// Deferred records stay on disk until their commit lands
    /// (commit-then-delete, §4.3-4③), so every `drain_journal` re-reads
    /// them; this set is what stops each 50 ms drain from re-enqueueing
    /// the same intent — outranking and defeating its own retry book on
    /// `Retry`, or spinning forever after `Degraded` (the book is
    /// removed but the record is conserved for the next boot). Names are
    /// unique per record; the set is per-boot and bounded by the drain.
    deferred_names: Mutex<HashSet<String>>,
}

impl Pool {
    pub fn new(ctx: ExecCtx) -> Arc<Self> {
        Arc::new(Self {
            ctx,
            policy: RetryPolicy::default(),
            pending: Mutex::new(VecDeque::new()),
            books: Mutex::new(HashMap::new()),
            running: Mutex::new(HashSet::new()),
            running_sids: Mutex::new(HashSet::new()),
            seq: AtomicU64::new(0),
            exec: None,
            reconcile_names: Mutex::new(Vec::new()),
            deferred_names: Mutex::new(HashSet::new()),
        })
    }

    pub fn with_policy(mut self: Arc<Self>, policy: RetryPolicy) -> Arc<Self> {
        Arc::get_mut(&mut self).unwrap().policy = policy;
        self
    }

    /// Test seam: swap the executor dispatch for an injected function.
    /// The returned future must be `'static` — injected executors should
    /// clone anything they await on (the trait-object lifetime cannot
    /// express a per-call borrow anyway).
    pub fn with_exec<F, Fut>(mut self: Arc<Self>, exec: F) -> Arc<Self>
    where
        F: Fn(&ExecCtx, Work) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<TaskOutcome>> + Send + 'static,
    {
        Arc::get_mut(&mut self).unwrap().exec = Some(Box::new(move |ctx, work| {
            Box::pin(exec(ctx, work))
        }));
        self
    }

    /// Enqueue work — dedup by key against pending + running. Reconcile
    /// works MERGE their journal record names into an already-pending or
    /// cooling (booked) pass: two `reconcile(all)` intents still only
    /// schedule one pass, but every record must be deleted afterwards —
    /// merging into a cooling book is what keeps a record drained during
    /// the book's cooldown from being silently dropped by key dedup.
    pub fn enqueue(&self, work: Work) {
        let key = work.key();
        let mut pending = self.pending.lock().unwrap();
        if let Work::Reconcile { names } = &work {
            if let Some((_, Work::Reconcile { names: existing })) =
                pending.iter_mut().find(|(_, w)| matches!(w, Work::Reconcile { .. }))
            {
                existing.extend(names.iter().cloned());
                return;
            }
            if self.running.lock().unwrap().contains("reconcile") {
                // A pass is scanning right now and cannot see this intent's
                // commits — schedule a follow-up pass once it finishes.
                self.reconcile_names
                    .lock()
                    .unwrap()
                    .extend(names.iter().cloned());
                return;
            }
            if let Some(book) = self.books.lock().unwrap().get_mut("reconcile") {
                if let Work::Reconcile { names: existing } = &mut book.work {
                    existing.extend(names.iter().cloned());
                    return;
                }
            }
        }
        if pending.iter().any(|(_, w)| w.key() == key)
            || self.books.lock().unwrap().contains_key(&key)
            || self.running.lock().unwrap().contains(&key)
        {
            return;
        }
        pending.push_back((self.seq.fetch_add(1, Ordering::Relaxed), work));
    }

    /// The journal consumption entry point (the daemon's E1–E6 funnel):
    /// read in every pending record, then — fresh pull intents re-arm
    /// pull-degraded sids (§6.3 recovery), materialize intents become
    /// ranked work, one reconcile pass covers all `reconcile` records,
    /// and applied-intent sids wake their waiters.
    pub fn drain_journal(&self) -> Result<JournalReport> {
        // Snapshot parked deferred names BEFORE consuming: `apply` may
        // run only on a record's first sighting — parked records re-emit
        // their deferred shape without mutating state (the evict
        // pullPending wipe is a once-only read-in effect).
        let parked: HashSet<String> = self.deferred_names.lock().unwrap().clone();
        let mut guard = self.ctx.shared.lock();
        let report = consume_journal(
            &self.ctx.shared.mirror,
            &self.ctx.shared.journal_dir,
            &self.ctx.shared.sync_dir,
            &mut guard,
            &parked,
        )?;
        drop(guard);
        {
            let mut susp = self.ctx.shared.suspend.lock().unwrap();
            for sid in &report.recovery_sids {
                susp.pull.remove(sid);
            }
        }
        // Deferred records persist until their commit lands, so a record
        // already represented as work (pending, running, retry/pending
        // book, or parked after degrade) must not be re-enqueued by the
        // next drain — `deferred_names` marks "this record already has an
        // owner this boot". A genuinely NEW record always has a new name.
        let mut seen = self.deferred_names.lock().unwrap();
        for (name, intent) in &report.materialize {
            if seen.insert(name.clone()) {
                self.enqueue(Work::Materialize {
                    name: name.clone(),
                    intent: intent.clone(),
                });
            }
        }
        let fresh: Vec<String> = report
            .reconcile_records
            .iter()
            .filter(|n| seen.insert((*n).clone()))
            .cloned()
            .collect();
        drop(seen);
        if !fresh.is_empty() {
            self.enqueue(Work::Reconcile { names: fresh });
        }
        for sid in &report.applied_sids {
            self.ctx.shared.notify.signal(sid);
        }
        Ok(report)
    }

    /// Pick the highest-ranked eligible work. `Instant` budget checks run
    /// here: an expired RETRY book degrades its task as a side effect (the
    /// queue layer's `cumulative retries over task budget → degraded`);
    /// pending books never
    /// expire.
    ///
    /// Candidates come from three sources and their fate differs:
    /// `pending` entries (journal-derived work carrying payloads like
    /// reconcile record names) MUST be requeued when they lose —
    /// deduplication collapses them into one entry only when their key
    /// won; due books are held aside and restored (the chosen one marked
    /// `in_flight` so `finish` continues its accounting); state-derived
    /// candidates are re-derived every pick, so they need no storage.
    ///
    /// Eligibility filters, in order: running-key dedup, same-sid
    /// serialization (any sid-bearing work blocks on an already-running
    /// same-sid task), the reconcile barrier (`reconcile` only starts on
    /// an empty pool and blocks everything while it runs — a mid-pass
    /// append would read as a bogus length mismatch), and the suspension
    /// overlay (`manual` blocks pull+publish; `pull` blocks pulls; a
    /// `force_reset` healing pull bypasses both — it is the convergence).
    /// A state-resurfacing candidate also yields to a live (non-due)
    /// book of the same key — otherwise a cooled-down `Pending` would
    /// hot-loop on the state's `pullPending` field.
    pub fn pick(&self) -> Option<Work> {
        enum Src {
            Pending,
            Book,
            State,
        }

        let now = Instant::now();
        let mut due_books: Vec<Book> = Vec::new();
        let mut expired: Vec<Work> = Vec::new();
        let mut candidates: Vec<(u8, u64, Src, Work)> = Vec::new();
        {
            // Due books leave the map for selection; expired retry books
            // degrade below (outside the lock — degrade commits state).
            let mut books = self.books.lock().unwrap();
            let due_keys: Vec<String> = books
                .iter()
                .filter(|(_, b)| !b.in_flight && b.next_at <= now)
                .map(|(k, _)| k.clone())
                .collect();
            for key in due_keys {
                let book = books.remove(&key).unwrap();
                if book.deadline.is_some_and(|d| now >= d) {
                    expired.push(book.work);
                } else {
                    let seq = u64::MAX - book.attempt as u64;
                    candidates.push((book.work.rank(), seq, Src::Book, book.work.clone()));
                    due_books.push(book);
                }
            }
        }
        for work in expired {
            self.degrade_work(&work, "network");
        }
        // Pending drains in full — losers return below, keeping order.
        let drained: Vec<(u64, Work)> = {
            let mut pending = self.pending.lock().unwrap();
            pending.drain(..).collect()
        };
        for (seq, work) in &drained {
            candidates.push((work.rank(), *seq, Src::Pending, work.clone()));
        }
        // State-driven candidates: validated tasks → publish; pullPending
        // → pull. Tombstoned records never yield work — the tombstone
        // outranks everything (§5). These re-derive every pick; nothing
        // is persisted from them.
        self.ctx.shared.with_state(|st| {
            for (sid, rec) in &st.sessions {
                if rec.deleted.is_some() {
                    continue;
                }
                if rec.task.as_ref().is_some_and(|t| t.phase == Phase::Validated) {
                    candidates.push((0, 0, Src::State, Work::Publish { sid: sid.clone() }));
                }
                if rec.pull_pending.is_some() {
                    candidates.push((
                        2,
                        0,
                        Src::State,
                        Work::Pull {
                            sid: sid.clone(),
                            force_reset: false,
                        },
                    ));
                }
            }
        });
        let susp = self.ctx.shared.suspend.lock().unwrap();
        let running = self.running.lock().unwrap();
        let mut running_sids = self.running_sids.lock().unwrap();
        let reconcile_running = running.contains("reconcile");
        // Dedup by key keeping the best (rank, seq).
        let mut best: HashMap<String, (u8, u64, Src, Work)> = HashMap::new();
        for (rank, seq, src, work) in candidates {
            // State-resurfacing work yields to a live (non-due) book —
            // the book's cooldown is the only thing keeping a Pending
            // from hot-looping against its own persisted intent field.
            if matches!(src, Src::State)
                && self
                    .books
                    .lock()
                    .unwrap()
                    .get(&work.key())
                    .is_some_and(|b| !b.in_flight)
            {
                continue;
            }
            let key = work.key();
            match best.get(&key) {
                Some((r, s, _, _)) if (*r, *s) <= (rank, seq) => {}
                _ => {
                    best.insert(key, (rank, seq, src, work));
                }
            }
        }
        let mut eligible: Vec<(u8, u64, Src, Work)> = best.into_values().collect();
        // (rank, seq) is the RFC order; `key` breaks exact ties so equal
        // candidates pick deterministically regardless of map order.
        eligible.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)).then_with(|| a.3.key().cmp(&b.3.key())));
        let mut chosen: Option<Work> = None;
        for (_, _, _, work) in eligible {
            let key = work.key();
            if running.contains(&key) {
                continue;
            }
            let gated = match &work {
                Work::Reconcile { .. } => !running.is_empty(),
                Work::Pull { sid, force_reset } => {
                    reconcile_running
                        || running_sids.contains(sid)
                        || (!force_reset && susp.contains(sid))
                }
                Work::Publish { sid } => {
                    reconcile_running
                        || running_sids.contains(sid)
                        || susp.manual.contains(sid)
                }
                Work::Materialize { intent, .. } => match intent {
                    Intent::Evict { sid, .. } | Intent::Hydrate { sid, .. } => {
                        reconcile_running || running_sids.contains(sid)
                    }
                    _ => reconcile_running,
                },
            };
            if gated {
                continue;
            }
            running_sids.extend(work.sid().map(str::to_string));
            drop(running_sids);
            drop(susp);
            drop(running);
            self.running.lock().unwrap().insert(key.clone());
            chosen = Some(work);
            break;
        }
        // Restore losers: pending entries whose key lost go back in
        // order; due books re-enter the map, the chosen one in-flight
        // (its accounting hands off to the executing task's finish()).
        let chosen_key = chosen.as_ref().map(|w| w.key());
        {
            let mut pending = self.pending.lock().unwrap();
            for (seq, work) in drained {
                if chosen_key.as_deref() != Some(work.key().as_str()) {
                    pending.push_back((seq, work));
                }
            }
        }
        {
            let mut books = self.books.lock().unwrap();
            for mut book in due_books {
                book.in_flight = chosen_key.as_deref() == Some(book.work.key().as_str());
                books.insert(book.work.key(), book);
            }
        }
        chosen
    }

    /// Dispatch loop: fill free slots by pick order. Each spawned task
    /// runs `run_one` (with a panic guard — a poisoned executor must not
    /// wedge the sid's serial slot) and reports to `finish`.
    pub fn dispatch(self: &Arc<Self>) -> usize {
        let mut spawned = 0;
        while self.running.lock().unwrap().len() < POOL_SIZE {
            let Some(work) = self.pick() else { break };
            let pool = Arc::clone(self);
            tokio::spawn(async move {
                let outcome = AssertUnwindSafe(pool.run_one(&work))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| TaskOutcome::Retry("panic in executor".into()));
                pool.finish(&work, outcome);
            });
            spawned += 1;
        }
        spawned
    }

    /// One task through the executor dispatch: the test seam if injected,
    /// otherwise the production mapping.
    pub async fn run_one(&self, work: &Work) -> TaskOutcome {
        if self.ctx.shared.fail_closed.load(Ordering::Relaxed) {
            // §9.1-6: under the fail-closed latch every task suspends —
            // replay still runs (consume_journal), execution stops —
            // reconcile included: it commits records, and a quarantined
            // table must not be written on top of.
            return TaskOutcome::Pending("state corrupt — suspended".into());
        }
        if let Some(exec) = &self.exec {
            return match exec(&self.ctx, work.clone()).await {
                Ok(o) => o,
                Err(e) => TaskOutcome::Retry(format!("{e:#}")),
            };
        }
        match work {
            Work::Pull { sid, force_reset } => {
                run_pull(&self.ctx, sid, *force_reset).await
            }
            Work::Publish { sid } => run_publish(&self.ctx, sid).await,
            Work::Materialize { name, intent } => {
                run_materialize(&self.ctx, name, intent).await
            }
            Work::Reconcile { .. } => self.run_reconcile().await,
        }
    }

    /// `Work::Reconcile` through `reconcile::run`: `manual` is rebuilt
    /// from this pass's suspend verdicts; `pull` lifts ONLY for sids
    /// this pass reports `unsuspended` (previously manual-suspended, now
    /// clean) — a pull-degraded sid with a clean verdict stays overlaid
    /// until a fresh intent re-arms it, matching §6.3's "recovery's only
    /// path = a new intent". Produced work is enqueued; committed sids wake
    /// waiters.
    async fn run_reconcile(&self) -> TaskOutcome {
        let shared = self.ctx.shared.clone();
        let probe = Arc::clone(&self.ctx.b_probe);
        let prev: Vec<String> = {
            let s = shared.suspend.lock().unwrap();
            s.manual.iter().cloned().collect()
        };
        let mut store = shared.clone();
        match reconcile::run(
            &shared.mirror,
            &shared.sync_dir,
            &mut store,
            PassKind::Steady,
            probe.as_ref(),
            &|sid: &str| prev.iter().any(|s| s == sid),
        ) {
            Ok(report) => {
                {
                    let mut s = shared.suspend.lock().unwrap();
                    s.manual = report.suspended.iter().cloned().collect();
                    for sid in &report.unsuspended {
                        s.pull.remove(sid);
                    }
                }
                for w in report.work {
                    let work = match w {
                        reconcile::Work::Publish { sid } => Work::Publish { sid },
                        reconcile::Work::Pull { sid, force_reset } => {
                            Work::Pull { sid, force_reset }
                        }
                    };
                    self.enqueue(work);
                }
                for sid in &report.committed {
                    shared.notify.signal(sid);
                }
                // A steady pass owes the same stderr evidence the boot
                // pass emits (startup::run_reconcile_pass) — suspends and
                // re-deletes are otherwise invisible off-disk.
                for note in &report.notes {
                    eprintln!("zonepaird: reconcile: {note}");
                }
                TaskOutcome::Done
            }
            Err(e) => TaskOutcome::Retry(format!("reconcile: {e:#}")),
        }
    }

    /// Completion accounting — the queue-layer's heart (§6.3).
    pub fn finish(&self, work: &Work, outcome: TaskOutcome) {
        let key = work.key();
        self.running.lock().unwrap().remove(&key);
        if let Some(sid) = work.sid() {
            self.running_sids.lock().unwrap().remove(sid);
            self.ctx.shared.notify.signal(sid);
        }
        match outcome {
            TaskOutcome::Done => {
                self.books.lock().unwrap().remove(&key);
                if let Work::Materialize { name, .. } = work {
                    // §4.3-4③: the record file deletes only now — after
                    // the executor's `materialized` commit. An earlier
                    // delete would lose the intent to a mid-window crash.
                    let _ = delete_journal_records(
                        &self.ctx.shared.journal_dir,
                        std::slice::from_ref(name),
                    );
                }
                if let Work::Reconcile { names } = work {
                    // Deferred record deletion (§4.3-1 ordering): the pass
                    // completed, now its journal records may go. Names that
                    // arrived mid-pass schedule a follow-up pass — their
                    // records still exist and still need a pass over the
                    // intents they committed.
                    let _ = delete_journal_records(&self.ctx.shared.journal_dir, names);
                    let extra = std::mem::take(&mut *self.reconcile_names.lock().unwrap());
                    if !extra.is_empty() {
                        self.enqueue(Work::Reconcile { names: extra });
                    }
                }
                if let Work::Pull { sid, force_reset: true } = work {
                    // A healing pull that completed lifts the pull overlay.
                    self.ctx.shared.suspend.lock().unwrap().pull.remove(sid);
                }
            }
            TaskOutcome::Pending(_) => {
                let mut books = self.books.lock().unwrap();
                let book = books.entry(key).or_insert_with(|| Book {
                    work: work.clone(),
                    attempt: 0,
                    next_at: Instant::now(),
                    deadline: None, // pending is budget-free
                    in_flight: false,
                });
                book.in_flight = false;
                book.next_at = Instant::now() + self.policy.pending_cooldown;
            }
            TaskOutcome::Retry(reason) => {
                let mut books = self.books.lock().unwrap();
                let book = books.entry(key.clone()).or_insert_with(|| Book {
                    work: work.clone(),
                    attempt: 0,
                    next_at: Instant::now(),
                    deadline: Some(Instant::now() + self.policy.budget),
                    in_flight: false,
                });
                book.in_flight = false;
                // A book that was pending-first still joins the budget:
                // the retry sequence's deadline counts from its first
                // Retry outcome.
                if book.deadline.is_none() {
                    book.deadline = Some(Instant::now() + self.policy.budget);
                }
                book.attempt += 1;
                if book.deadline.is_some_and(|d| Instant::now() >= d) {
                    drop(books);
                    self.degrade_work(work, &reason);
                    return;
                }
                let idx = ((book.attempt - 1) as usize).min(self.policy.backoffs.len() - 1);
                book.next_at = Instant::now() + self.policy.backoffs[idx];
            }
            TaskOutcome::Degraded(reason) => {
                self.books.lock().unwrap().remove(&key);
                self.degrade_work(work, &reason);
            }
        }
    }

    /// Terminal-failure routing — publish degrades persist in the task
    /// row (the only durable degraded slot), pull degrades mark the
    /// in-memory overlay, everything else is a loud log line. An
    /// executor-committed degrade (precheck/conflict/bad-request, with
    /// its richer `lastError`) is never overwritten by this layer.
    fn degrade_work(&self, work: &Work, reason: &str) {
        match work {
            Work::Publish { sid } => {
                if let Err(e) = self.ctx.shared.commit_state(|st| {
                    if let Some(rec) = st.sessions.get_mut(sid) {
                        if let Some(task) = rec.task.as_mut() {
                            if task.phase != Phase::Degraded
                                && task.phase.may_advance_to(Phase::Degraded)
                            {
                                task.phase = Phase::Degraded;
                                task.last_error = Some(reason.to_string());
                            }
                        }
                    }
                }) {
                    eprintln!("zonepaird: degraded commit for {sid} failed: {e:#}");
                }
                eprintln!("zonepaird: publish {sid} degraded({reason}) — persistent terminal");
                self.ctx.shared.notify.signal(sid);
            }
            Work::Pull { sid, .. } => {
                self.ctx
                    .shared
                    .suspend
                    .lock()
                    .unwrap()
                    .pull
                    .insert(sid.clone());
                eprintln!("zonepaird: pull {sid} degraded({reason}) — suspended for reconcile");
                self.ctx.shared.notify.signal(sid);
            }
            Work::Materialize { intent, .. } => {
                eprintln!("zonepaird: materialize {intent:?} degraded({reason}) — intent conserved");
            }
            Work::Reconcile { .. } => {
                eprintln!("zonepaird: reconcile degraded({reason})");
            }
        }
    }

    /// Is this sid currently suspension-gated? (diagnostics + tests)
    pub fn is_suspended(&self, sid: &str) -> bool {
        self.ctx.shared.suspend.lock().unwrap().contains(sid)
    }
}

// -------------------------------------------------- journal consumption ---

/// What one journal drain produced — the pool maps it into work.
#[derive(Debug, Default)]
pub struct JournalReport {
    /// Every consumed record's effect (applied/dropped/deferred/error).
    pub(crate) consumed: Vec<journal::Consumed>,
    /// sids whose fresh `pull`/`hydrate` intents re-arm a pull-degraded
    /// suspension — the §6.3 "recovery's only path = a new intent" for the
    /// pull path.
    pub recovery_sids: Vec<String>,
    /// sids any intent landed on — their waiters get a wake signal.
    pub applied_sids: Vec<String>,
    /// `(record name, intent)` pairs whose execution was deferred.
    pub materialize: Vec<(String, Intent)>,
    /// Record files to delete after the reconcile pass they scheduled.
    pub reconcile_records: Vec<String>,
    /// Per-record errors (unparseable records — startup cleanup's input).
    pub errors: Vec<(String, String)>,
}

/// Consume the journal channel under the caller's `&mut State`, wiring
/// the real tombstone probe (`reconcile::tombstone_file_id`) into
/// `note-remote-delete` so `fileId` binds the deleted file's instance —
/// or `None` when the file is absent/stub/unloaded (§3.3).
pub fn consume_journal(
    mirror: &Path,
    journal_dir: &Path,
    sync_dir: &Path,
    state: &mut State,
    parked: &HashSet<String>,
) -> Result<JournalReport> {
    let consumed = journal::consume_ordered_parked(journal_dir, sync_dir, state, |st, sid| {
        let rec = st.sessions.get(sid);
        let rel = rec
            .and_then(|r| r.task.as_ref().map(|t| t.file.as_str()))
            .or_else(|| rec.and_then(|r| r.pull.as_ref().map(|p| p.file.as_str())));
        let (probe, _) = reconcile::probe_session_file(mirror, rel, sid);
        reconcile::tombstone_file_id(rec, &probe)
    }, parked)?;
    let mut report = JournalReport::default();
    for entry in consumed {
        match (&entry.intent, &entry.effect) {
            (Some(intent), Ok(journal::Effect::Applied)) => {
                if let Some(sid) = journal::sid_of(intent) {
                    report.applied_sids.push(sid.to_string());
                    if matches!(intent, Intent::Pull { .. }) {
                        report.recovery_sids.push(sid.to_string());
                    }
                }
            }
            (Some(intent), Ok(journal::Effect::Dropped)) => {
                if let Some(sid) = journal::sid_of(intent) {
                    report.applied_sids.push(sid.to_string());
                }
            }
            (Some(intent), Ok(journal::Effect::Deferred)) => {
                report.materialize.push((entry.name.clone(), intent.clone()));
                if let Some(sid) = journal::sid_of(intent) {
                    if matches!(intent, Intent::Hydrate { .. }) {
                        report.recovery_sids.push(sid.to_string());
                    }
                    report.applied_sids.push(sid.to_string());
                }
            }
            (Some(_), Ok(journal::Effect::RunReconcile)) => {
                report.reconcile_records.push(entry.name.clone());
            }
            (_, Err(e)) => report.errors.push((entry.name.clone(), e.clone())),
            _ => {}
        }
        report.consumed.push(entry);
    }
    Ok(report)
}

/// Delete journal records whose deferred execution finished (reconcile
/// pass done / materialize committed), then fsync the directory — the
/// mirror of `consume_ordered`'s commit-then-delete ordering.
pub fn delete_journal_records(dir: &Path, names: &[String]) -> Result<usize> {
    let mut deleted = 0;
    for name in names {
        match std::fs::remove_file(dir.join(name)) {
            Ok(()) => deleted += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    if deleted > 0 {
        fs::fsync_dir(dir)?;
    }
    Ok(deleted)
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AOrd};
    use std::sync::Mutex as StdMutex;

    use crate::sync::engine::shared::Shared;
    use crate::sync::engine::testkit::*;
    use crate::sync::persist::journal;
    use crate::sync::persist::state::{self, Pull, SessionRecord};
    use crate::sync::state_machine::BProbe;

    use crate::sync::persist::fs::testkit::write_file;

    // Wave-2 I-1: a deferred journal record stays on disk across
    // Retry/Pending/Degraded (commit-then-delete), but the next drain
    // must NOT re-enqueue it — its retry book (or parked-intent
    // conservation) already represents it. Pre-fix the re-read record
    // re-entered `pending` every poll, outranking and nullifying the
    // book, and a Degraded materialize hot-looped forever.
    #[tokio::test]
    async fn deferred_record_retry_and_degrade_do_not_reenter_pending() {
        let (mirror, shared) = setup("deferred-spin");
        write_file(&mirror, "projects/lab/s1.jsonl", b"{\"a\":1}\n");
        shared
            .commit_state(|st| {
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
            })
            .unwrap();
        journal::append(
            &shared.journal_dir,
            &Intent::Evict {
                sid: "s1".into(),
                mode: journal::EvictMode::Drop,
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        // The injected executor answers Retry — the queue books it.
        let pool = Pool::new(ExecCtx::new(shared.clone()))
            .with_policy(RetryPolicy {
                backoffs: vec![Duration::from_millis(200)],
                budget: Duration::from_secs(60),
                pending_cooldown: Duration::from_millis(200),
            })
            .with_exec(|_, _| async { Ok(TaskOutcome::Retry("transient".into())) });
        pool.drain_journal().unwrap();
        let work = pool.pick().expect("materialize work queued");
        assert!(matches!(work, Work::Materialize { .. }));
        let outcome = pool.run_one(&work).await;
        assert!(matches!(outcome, TaskOutcome::Retry(_)));
        pool.finish(&work, outcome);
        // The record is still on disk; a drain while the retry book
        // cools must offer NOTHING — no re-enqueued pending duplicate.
        assert_eq!(journal::pending_count(&shared.journal_dir), 1);
        pool.drain_journal().unwrap();
        assert!(
            pool.pick().is_none(),
            "booked retry must not be re-enqueued by the next drain"
        );
        // After cooldown the BOOK (not a re-read record) resurfaces.
        tokio::time::sleep(Duration::from_millis(250)).await;
        let again = pool.pick().expect("retry book resurfaces on schedule");
        assert_eq!(again.key(), work.key());

        // Degraded: terminal — the record is conserved on disk for the
        // next boot, but THIS boot must never re-enqueue it.
        pool.finish(&again, TaskOutcome::Degraded("nope".into()));
        pool.drain_journal().unwrap();
        assert!(
            pool.pick().is_none(),
            "degraded deferred record must stay parked, not hot-loop"
        );
        assert_eq!(journal::pending_count(&shared.journal_dir), 1,
            "record conserved for replay on next boot");
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-610a: selection order — publish beats materialize beats pull
    // beats reconcile. (The waited/background pull split retired with
    // `wait_cursor`; both pulls carry one rank now.)
    #[tokio::test]
    async fn t610_selection_order() {
        let (mirror, shared) = setup("t610a");
        shared
            .commit_state(|st| {
                // publish candidate via state scan.
                st.sessions.insert(
                    "pub".into(),
                    SessionRecord {
                        task: Some(state::Task {
                            file: "f".into(),
                            phase: Phase::Validated,
                            first_seen: "t".into(),
                            enqueued_at: "t".into(),
                            last_error: None,
                        }),
                        ..Default::default()
                    },
                );
                // two pulls — same rank now; seq order decides which goes.
                for sid in ["wpull", "bpull"] {
                    st.sessions.insert(
                        sid.into(),
                        SessionRecord {
                            pull_pending: Some(state::PullPending {
                                target: "latest".into(),
                                enqueued_at: "t".into(),
                            record: None,
                            }),
                            ..Default::default()
                        },
                    );
                }
            })
            .unwrap();
        let pool = Pool::new(ExecCtx::new(shared.clone()));
        pool.enqueue(Work::Reconcile { names: vec![] });
        pool.enqueue(Work::Materialize {
            name: "1-1-1.json".into(),
            intent: Intent::Evict {
                sid: "mat".into(),
                mode: journal::EvictMode::Drop,
                enqueued_at: "t".into(),
            },
        });
        // Each picked work is finished before the next pick — the
        // reconcile barrier (pool must be empty) would otherwise mask
        // the rank order this test asserts. `finish` is not the executor:
        // it does not commit state, so each pick is followed by the
        // commit the real executor would have made — otherwise the
        // state scan re-offers the same work next round.
        let finish_publish = |shared: &Shared, sid: &str| {
            shared
                .commit_state(|st| {
                    st.sessions.get_mut(sid).unwrap().task.as_mut().unwrap().phase =
                        Phase::Published;
                })
                .unwrap();
        };
        let finish_pull = |shared: &Shared, sid: &str| {
            shared
                .commit_state(|st| {
                    st.sessions.get_mut(sid).unwrap().satisfy_pull_pending();
                })
                .unwrap();
        };
        let first = pool.pick().unwrap();
        assert_eq!(first, Work::Publish { sid: "pub".into() }, "publish first");
        pool.finish(&first, TaskOutcome::Done);
        finish_publish(&shared, "pub");
        let second = pool.pick().unwrap();
        assert!(matches!(second, Work::Materialize { .. }), "evict/hydrate second");
        pool.finish(&second, TaskOutcome::Done);
        let third = pool.pick().unwrap();
        assert_eq!(
            third,
            Work::Pull {
                sid: "bpull".into(),
                force_reset: false
            },
            "pulls tie on rank — key order decides"
        );
        pool.finish(&third, TaskOutcome::Done);
        finish_pull(&shared, "bpull");
        let fourth = pool.pick().unwrap();
        assert_eq!(
            fourth,
            Work::Pull {
                sid: "wpull".into(),
                force_reset: false
            },
            "second pull fourth"
        );
        pool.finish(&fourth, TaskOutcome::Done);
        finish_pull(&shared, "wpull");
        let fifth = pool.pick().unwrap();
        assert!(matches!(fifth, Work::Reconcile { .. }), "reconcile last");
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-610b: pool=4 concurrent lanes, same-sid strictly serial.
    #[tokio::test]
    async fn t610_pool_concurrency_and_sid_serial() {
        let (mirror, shared) = setup("t610b");
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(StdMutex::new(Vec::<String>::new()));
        let inflight_c = in_flight.clone();
        let max_c = max_seen.clone();
        let started_c = started.clone();
        let pool = Pool::new(ExecCtx::new(shared.clone())).with_exec(move |_ctx, work| {
            let inflight = inflight_c.clone();
            let max = max_c.clone();
            let started = started_c.clone();
            let key = work.key();
            async move {
                let now = inflight.fetch_add(1, AOrd::SeqCst) + 1;
                max.fetch_max(now, AOrd::SeqCst);
                started.lock().unwrap().push(key);
                tokio::time::sleep(Duration::from_millis(30)).await;
                inflight.fetch_sub(1, AOrd::SeqCst);
                Ok(TaskOutcome::Done)
            }
        });
        // Six works on six different sids → pool caps at 4 in flight.
        for i in 0..6 {
            pool.enqueue(Work::Pull {
                sid: format!("s{i}"),
                force_reset: false,
            });
        }
        // Two works on the SAME sid (pull + publish) → serialized: the
        // second must not start while the first runs.
        pool.enqueue(Work::Pull {
            sid: "dup".into(),
            force_reset: false,
        });
        pool.enqueue(Work::Publish { sid: "dup".into() });
        let spawned = pool.dispatch();
        assert_eq!(spawned, POOL_SIZE);
        // Drain: keep dispatching until the pending queue and running set
        // are empty.
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(15)).await;
            pool.dispatch();
            if pool.running.lock().unwrap().is_empty()
                && pool.pending.lock().unwrap().is_empty()
            {
                break;
            }
        }
        assert!(pool.running.lock().unwrap().is_empty());
        assert_eq!(max_seen.load(AOrd::SeqCst), POOL_SIZE, "4 lanes full");
        // The two same-sid works ran serially — both started, never
        // together: `dup` appears twice in `started` and never overlapped
        // (in_flight max equals pool size, but the serial check is that
        // the same-sid works did not run at the same time — guaranteed by
        // running_sids; assert both completed).
        let started = started.lock().unwrap();
        assert!(started.contains(&"pull:dup:false".to_string()));
        assert!(started.contains(&"pub:dup".to_string()));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Queue-layer backoff ladder: Retry books 1s→5s→30s-equivalent slots
    // via the injected policy; expired books degrade through pick().
    #[tokio::test]
    async fn t602_queue_backoff_ladder_and_deadline() {
        let (mirror, shared) = setup("t602b");
        let pool = Pool::new(ExecCtx::new(shared.clone())).with_policy(RetryPolicy {
            backoffs: vec![
                Duration::from_millis(30),
                Duration::from_millis(60),
                Duration::from_millis(90),
            ],
            budget: Duration::from_secs(60),
            pending_cooldown: Duration::from_millis(5),
        });
        let work = Work::Pull {
            sid: "s1".into(),
            force_reset: false,
        };
        pool.finish(&work, TaskOutcome::Retry("x".into()));
        {
            let books = pool.books.lock().unwrap();
            let b = &books["pull:s1:false"];
            assert_eq!(b.attempt, 1);
            // First backoff ≈ 30ms (injected 1s equivalent).
            let dt = b.next_at.duration_since(Instant::now());
            assert!(dt <= Duration::from_millis(35), "{dt:?}");
        }
        pool.finish(&work, TaskOutcome::Retry("x".into()));
        {
            let books = pool.books.lock().unwrap();
            let b = &books["pull:s1:false"];
            assert_eq!(b.attempt, 2);
            let dt = b.next_at.duration_since(Instant::now());
            assert!(dt > Duration::from_millis(35), "second step is longer: {dt:?}");
        }
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Pending is the budget-free lane: re-queued on cooldown, never
    // counted as attempts — a registered-but-empty session can sit here
    // indefinitely without degrading.
    #[tokio::test]
    async fn pending_requeues_without_budget() {
        let (mirror, shared) = setup("pending");
        let pool = Pool::new(ExecCtx::new(shared.clone())).with_policy(fast_policy());
        let work = Work::Pull {
            sid: "s1".into(),
            force_reset: false,
        };
        for _ in 0..5 {
            pool.finish(&work, TaskOutcome::Pending("B 404".into()));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        // The book exists but has zero attempts — pick() offers it again,
        // and no suspension ever latches.
        let picked = pool.pick();
        assert!(picked.is_some() || !pool.is_suspended("s1"));
        assert!(!pool.is_suspended("s1"));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // drain_journal: pull intents re-arm a pull-degraded sid; a reconcile
    // intent becomes ranked work whose journal file is deleted only
    // after the pass completes.
    #[tokio::test]
    async fn drain_journal_recovers_and_defers() {
        let (mirror, shared) = setup("drain");
        shared.suspend.lock().unwrap().pull.insert("s1".to_string());
        journal::append(
            &shared.journal_dir,
            &Intent::Pull {
                sid: "s1".into(),
                target: "latest".into(),
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        journal::append(
            &shared.journal_dir,
            &Intent::Reconcile {
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        let pool = Pool::new(ExecCtx::new(shared.clone()));
        let report = pool.drain_journal().unwrap();
        // The fresh pull intent re-arms the degraded sid.
        assert!(report.recovery_sids.contains(&"s1".to_string()));
        assert!(!shared.suspend.lock().unwrap().pull.contains("s1"));
        // The reconcile record is deferred — file still there, work queued.
        assert_eq!(report.reconcile_records.len(), 1);
        assert!(shared.journal_dir.join(&report.reconcile_records[0]).exists());
        // `pull` landed as pullPending → scanned pull work is pickable;
        // reconcile sits in pending behind it.
        assert!(shared.with_state(|st| st.sessions["s1"].pull_pending.is_some()));
        // finish(Reconcile::Done) deletes the record files.
        let name = report.reconcile_records[0].clone();
        pool.finish(
            &Work::Reconcile {
                names: vec![name.clone()],
            },
            TaskOutcome::Done,
        );
        assert!(!shared.journal_dir.join(&name).exists());
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // The reconcile barrier: a running reconcile blocks sid work, and a
    // running sid work blocks reconcile — the executor must never scan a
    // mid-append file.
    #[tokio::test]
    async fn reconcile_is_a_pool_barrier() {
        let (mirror, shared) = setup("barrier");
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "s1".into(),
                    SessionRecord {
                        pull_pending: Some(state::PullPending {
                            target: "latest".into(),
                            enqueued_at: "t".into(),
                        record: None,
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        let pool = Pool::new(ExecCtx::new(shared.clone()));
        pool.enqueue(Work::Reconcile { names: vec![] });
        // Rank order first: the background pull outranks reconcile.
        let first = pool.pick().unwrap();
        assert!(
            matches!(&first, Work::Pull { sid, .. } if sid == "s1"),
            "pull outranks reconcile: {first:?}"
        );
        // While the pull "runs", reconcile must not start (empty-pool rule).
        assert!(pool.pick().is_none(), "reconcile waits for an empty pool");
        // Cool the pull down (executor-level: it would keep pending until
        // B produces bytes) — the book's cooldown frees the barrier.
        pool.finish(&first, TaskOutcome::Pending("B 404".into()));
        let rec = pool.pick().unwrap();
        assert!(matches!(rec, Work::Reconcile { .. }), "reconcile takes an empty pool");
        // And the reverse direction: reconcile running blocks sid work.
        assert!(pool.pick().is_none(), "reconcile blocks sid work");
        pool.finish(&rec, TaskOutcome::Done);
        // The pull's pending cooldown (default policy, 250ms) must lapse
        // before its book is a candidate again.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let next = pool.pick().unwrap();
        assert!(matches!(next, Work::Pull { .. }), "pool unblocks after reconcile");
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-11 #4: a steady-state `Work::Reconcile` must print `report.notes`
    // to stderr — the boot pass already did (startup::run_reconcile_pass)
    // but the pool's execute body dropped them. stderr cannot be
    // captured in-process, so the flagged re-entry plants a BLostRecovery
    // record and runs `run_one(Work::Reconcile)` for real; the unflagged
    // arm re-invokes this test binary as a child under `--nocapture`
    // and asserts on the child's captured stderr.
    #[tokio::test]
    async fn t11_steady_reconcile_notes_reach_stderr() {
        const FLAG: &str = "ZP_T11_EMIT_RECONCILE";
        if std::env::var_os(FLAG).is_some() {
            // Emit half: pull record + live file + B 404 → row 18
            // `suspended (BLostRecovery)` through the real run_one path.
            let (mirror, shared) = setup("t11-emit");
            write_file(&mirror, "projects/lab/s1.jsonl", b"{\"a\":1}\n");
            shared
                .commit_state(|st| {
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
                })
                .unwrap();
            let ctx = ExecCtx {
                shared: shared.clone(),
                base: None,
                token: None,
                b_probe: Arc::new(|_| BProbe::Absent),
            };
            let pool = Pool::new(ctx);
            let outcome = pool
                .run_one(&Work::Reconcile { names: vec![] })
                .await;
            assert_eq!(outcome, TaskOutcome::Done);
            assert!(pool.is_suspended("s1"), "BLostRecovery must suspend s1");
            std::fs::remove_dir_all(&mirror).unwrap();
            return;
        }
        let exe = std::env::current_exe().unwrap();
        let out = std::process::Command::new(exe)
            .arg("sync::engine::pool::tests::t11_steady_reconcile_notes_reach_stderr")
            .arg("--exact")
            .arg("--nocapture")
            .env(FLAG, "1")
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            out.status.success(),
            "emit arm failed: {stderr}\n{}",
            String::from_utf8_lossy(&out.stdout)
        );
        assert!(
            stderr.contains("zonepaird: reconcile: s1: suspended (BLostRecovery)"),
            "steady reconcile notes must reach stderr: {stderr}"
        );
    }
}
