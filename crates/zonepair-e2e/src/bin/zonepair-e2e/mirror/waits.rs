use crate::fixtures::{fail, Server};
use crate::mirror::files::mirror_bytes;
use crate::mirror::journal::{journal_empty, read_state};
use crate::mirror::MirrorHome;
use std::time::{Duration, Instant};
use zonepair_client::{
    daemon::owner,
    sync::persist::state::{Phase, State},
};

// ---- convergence waiting ------------------------------------------------------

/// Poll `pred` at the RFC's ≤50 ms cadence; `fail!` with `what` on timeout.
/// The convergence primitive behind every scenario assertion — waits ride
/// committed `state.json`, mirror bytes, lock probes and the journal,
/// never wall-clock sleeps.
pub(crate) fn wait_for(what: &str, timeout: Duration, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    fail(&format!("{what} did not hold within {timeout:?}"));
}

pub(crate) fn wait_state(what: &str, home: &MirrorHome, timeout: Duration, mut pred: impl FnMut(&State) -> bool) {
    wait_for(what, timeout, || pred(&read_state(home)));
}

pub(crate) fn wait_lock_free(home: &MirrorHome, timeout: Duration) {
    let sync_dir = home.sync_dir();
    wait_for("owner.lock to go free", timeout, || owner::lock_free(&sync_dir));
}

/// `sync --wait`'s three conditions, recomputed observer-side: journal
/// drained ∧ no pullPending ∧ every task terminal/absent/orphan-writing.
pub(crate) fn wait_journal_drained(home: &MirrorHome, timeout: Duration) {
    wait_state("journal drained + state settled", home, timeout, |st| {
        journal_empty(home)
            && st.sessions.values().all(|r| {
                r.pull_pending.is_none()
                    && r.task
                        .as_ref()
                        .is_none_or(|t| t.phase != Phase::Validated)
            })
    });
}

pub(crate) fn wait_mirror_bytes(what: &str, home: &MirrorHome, sid: &str, want: &[u8], timeout: Duration) {
    wait_for(what, timeout, || {
        mirror_bytes(home, sid).is_some_and(|b| b == want)
    });
}

/// stderr-*.log files on the server — one exists per spawned claude child,
/// i.e. per established stdio-v1 WS. The T-715 order probe.
pub(crate) fn stderr_log_count(server: &Server) -> usize {
    let logs = server.workdir.join("logs");
    std::fs::read_dir(&logs)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().starts_with("stderr-"))
                .count()
        })
        .unwrap_or(0)
}
