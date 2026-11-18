//! The executor's shared context (RFC-0003-zonepaird §4.2): `Shared` carries the
//! mirror layout, the authoritative `state.json` table behind a lock, the
//! per-sid wake-up cache (`Notify`), the runtime suspension overlay
//! (`Suspended`), and the fail-closed latch from `state.corrupt-*`. It has
//! no executor knowledge of its own — workers, waiters and the materialize
//! executor all import this layer.

use anyhow::Result;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crate::sync::persist::fs;
use crate::sync::persist::state::{self, Corruption, SessionRecord, State};
use crate::sync::state_machine::BProbe;
use crate::transport::Base;

/// §6.2 — single-task logical budget. One retry sequence may span this
/// much wall time before the task degrades (`network` for pulls, the
/// attempt's reason class for publishes). Owned here (the shared
/// executor context) so `wait` and `pool` both draw it without
/// depending on each other.
pub const TASK_BUDGET: Duration = Duration::from_secs(300);

/// Where a reconcile pass reads and mutates session records. `State`
/// implements it in tests (single-writer ownership); `Arc<Shared>`
/// implements it below so a pass commits one record at a time without
/// holding the state lock across file IO.
pub trait SessionStore {
    /// All sids the record table knows — unioned with the mirror scan.
    fn sids(&self) -> std::collections::BTreeSet<String>;
    /// One record, cloned (`None` = sid unknown to the table).
    fn get(&self, sid: &str) -> Option<SessionRecord>;
    /// Mutate one record and commit the whole table in the same
    /// transaction: the crash window stays one-record-wide.
    fn mutate_commit(
        &mut self,
        sync_dir: &Path,
        sid: &str,
        f: &mut dyn FnMut(&mut SessionRecord),
    ) -> Result<()>;
}

/// `watch` wake-up cache per sid (§7): a convenience for in-process
/// waiters (`Waits::wake`), a cache never a truth source — dropped and
/// rebuilt freely.
#[derive(Default)]
pub struct Notify {
    map: Mutex<HashMap<String, tokio::sync::watch::Sender<()>>>,
}

impl Notify {
    /// Wake every receiver parked on `sid`. GC's the channel when no
    /// receiver is listening (a stale sender costs nothing anyway — this
    /// just keeps the map bounded).
    pub fn signal(&self, sid: &str) {
        let mut map = self.map.lock().unwrap();
        if let Some(tx) = map.get(sid) {
            if tx.receiver_count() == 0 {
                map.remove(sid);
            } else {
                let _ = tx.send(());
            }
        }
    }

    /// A receiver for in-process waits. `wait.rs` polls disk as the
    /// authority; this just shortens the polling interval.
    pub fn subscribe(&self, sid: &str) -> tokio::sync::watch::Receiver<()> {
        self.map
            .lock()
            .unwrap()
            .entry(sid.to_string())
            .or_insert_with(|| tokio::sync::watch::channel(()).0)
            .subscribe()
    }

}

/// Runtime suspension overlay — in-memory only, exactly like the RFC
/// wants it: neither kind of suspension is a `state.json` field.
/// `manual` is rebuilt from reconcile verdicts each pass; `pull` holds
/// pull-path degradations until a fresh pull/hydrate intent re-arms the
/// sid or a healing pull / clean verdict converges it.
#[derive(Default)]
pub struct Suspended {
    /// Reconcile-derive suspensions (orphans, B-loss, field corruption).
    pub manual: HashSet<String>,
    /// Pull-path degradations (§6.3 — no persistent slot).
    pub pull: HashSet<String>,
}

impl Suspended {
    pub fn contains(&self, sid: &str) -> bool {
        self.manual.contains(sid) || self.pull.contains(sid)
    }
}

/// Everything an executor needs that is not the task itself: the mirror
/// layout, the authoritative table behind a lock, the wake-up cache, the
/// suspension overlay, and the fail-closed latch from `state.corrupt-*`.
pub struct Shared {
    pub mirror: PathBuf,
    pub sync_dir: PathBuf,
    pub journal_dir: PathBuf,
    /// The authoritative table — commits go through `commit_state`, which
    /// is the only place `state::commit` is invoked from this layer.
    pub state: Mutex<State>,
    /// Per-sid `watch` wake-up cache.
    pub notify: Notify,
    /// Runtime suspension overlay.
    pub suspend: Mutex<Suspended>,
    /// `state.corrupt-*` latch (§8 fail-closed): executors suspend.
    pub fail_closed: std::sync::atomic::AtomicBool,
}

impl Shared {
    /// Open `<mirror>/.sync/` (0700) and load the authoritative table.
    /// Corruption latches `fail_closed` and is returned for the caller's
    /// loud log line.
    pub fn open(mirror: &Path) -> Result<(Arc<Shared>, Option<Corruption>)> {
        let sync_dir = state::ensure_sync_dir(mirror)?;
        fs::ensure_dir(&sync_dir.join("journal"), 0o700)?;
        let out = state::load(&sync_dir)?;
        shared_of(mirror, sync_dir, out)
    }

    /// `open` for short-lived read-only callers (`wait_for_publish` in the
    /// adapter): the `.sync` tree is never created and `state.json` is read
    /// through `load_readonly` — a schema violation still latches
    /// `fail_closed` and reports a `Corruption`, but the file is never
    /// renamed; quarantine is the writer's privilege (RFC-0003-zonepaird §8.3).
    pub fn open_readonly(mirror: &Path) -> Result<(Arc<Shared>, Option<Corruption>)> {
        let sync_dir = state::sync_dir(mirror);
        let out = state::load_readonly(&sync_dir)?;
        shared_of(mirror, sync_dir, out)
    }

    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Read the table under the lock — short critical sections only.
    pub fn with_state<R>(&self, f: impl FnOnce(&State) -> R) -> R {
        f(&self.lock())
    }

    /// Mutate + commit in one lock hold: the transaction unit is one file
    /// write, and holding the lock through it keeps concurrent workers'
    /// commits from interleaving their read-modify cycles.
    pub fn commit_state(&self, f: impl FnOnce(&mut State)) -> Result<()> {
        let mut guard = self.lock();
        f(&mut guard);
        state::commit(&self.sync_dir, &guard)
    }
}

/// The `Shared` construction `open`/`open_readonly` share — the only
/// difference between them is upstream (dir creation, which `load` runs).
fn shared_of(
    mirror: &Path,
    sync_dir: PathBuf,
    out: state::LoadOutcome,
) -> Result<(Arc<Shared>, Option<Corruption>)> {
    let shared = Arc::new(Shared {
        mirror: mirror.to_path_buf(),
        journal_dir: sync_dir.join("journal"),
        sync_dir,
        state: Mutex::new(out.state),
        notify: Notify::default(),
        suspend: Mutex::new(Suspended::default()),
        fail_closed: std::sync::atomic::AtomicBool::new(out.corruption.is_some()),
    });
    Ok((shared, out.corruption))
}

/// Everything shared by executors: the disk triple (`Shared`), the wire
/// endpoint (`None` in tests that never reach the network), and the B
/// probe reconcile uses.
#[derive(Clone)]
pub struct ExecCtx {
    pub shared: Arc<Shared>,
    pub base: Option<Base>,
    pub token: Option<String>,
    /// `file present on B?` — production wires `store_exists`; tests inject a
    /// static answer. `Unknown` must be returned on probe failure.
    pub b_probe: Arc<dyn Fn(&str) -> BProbe + Send + Sync>,
}

impl ExecCtx {
    /// Test convenience — production builds `ExecCtx` through
    /// `ExecParts::into_ctx` (daemon startup); unit tests need a
    /// network-free context three fields shorter.
    #[cfg(test)]
    pub fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            base: None,
            token: None,
            b_probe: Arc::new(|_| BProbe::Unknown),
        }
    }
}

/// `Arc<Shared>` IS the `SessionStore` the reconcile seam asks for: it
/// clones records out under the lock and commits one record at a time —
/// the pass never holds the lock across file IO.
impl SessionStore for Arc<Shared> {
    fn sids(&self) -> BTreeSet<String> {
        self.lock().sessions.keys().cloned().collect()
    }
    fn get(&self, sid: &str) -> Option<SessionRecord> {
        self.lock().sessions.get(sid).cloned()
    }
    fn mutate_commit(
        &mut self,
        sync_dir: &Path,
        sid: &str,
        f: &mut dyn FnMut(&mut SessionRecord),
    ) -> Result<()> {
        let mut guard = self.lock();
        f(guard.sessions.entry(sid.to_string()).or_default());
        state::commit(sync_dir, &guard)
    }
}
