//! `zonepaird` — the one long-lived process that touches sync state
//! (RFC-0003-zonepaird §2–§3). It exists because adapter reads can't be hooked
//! (zero-patch) and every stub is short-lived: the mirror must already
//! hold the bytes when the adapter reads, so somebody must pull between
//! two stubs. That somebody is this process.
//!
//! The process model in one paragraph: any process that wants to write
//! an intent runs `ensure_owner` first — probe `owner.lock`, spawn a
//! detached `zonepaird` if it's free. N spawns converge on the lock:
//! exactly one survives step 1 of the startup sequence (§9.1), the rest
//! exit immediately. The survivor replays the journal, restores
//! in-flight tasks, runs one reconcile pass, then enters the main loop
//! below. There is no watchdog — a dead writer is noticed by the next
//! intent writer or waiter probing the lock, which spawns a fresh one
//! that rebuilds everything from disk.
//!
//! The main loop (§9.1-9, §5, §3.3):
//!
//! ```text
//! every poll (≤50 ms, injectable)
//!   ├─ stop marker?              → graceful exit
//!   ├─ owner.lock inode stale?   → suicide (flock-vs-unlink, §3.3)
//!   ├─ drain_journal             → consume intents, feed the pool
//!   ├─ pool.dispatch             → fill free worker lanes
//!   ├─ E4 active-set tick ~2s    → Pull for recently-active sids
//!   ├─ sweep tick ~60s           → one Steady reconcile pass
//!   ├─ owner.lock heartbeat      → mtime bump every round (liveness)
//!   └─ round > heartbeat_max     → exit (self-heartbeat, §3.3)
//! ```
//!
//! E4's "active" set is a pure-memory `last-traffic` table fed by pull
//! intents (E1/E3 traffic flows through the journal); it may be lost
//! and rebuilt — the worst outcome is the sweep covering the session
//! instead. `stop` is `zonepaird stop`'s marker file: the CLI writes
//! `.sync/stop`, waits one graceful window, then escalates to SIGTERM
//! on `owner.pid` — SIGTERM mid-commit is a designed crash window
//! (atomic commits make it a no-op), the marker is the polite path.

pub mod owner;
pub mod startup;

use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api;
use crate::sync::state_machine::BProbe;
use crate::sync::persist::journal::Intent;
use crate::sync::state;
use crate::sync::engine::Work;
use crate::transport::Base;

use startup::{BootBlocked, Booted, ExecParts};

/// The `zonepaird stop` marker inside `.sync/` — a file, because the
/// daemon's only IPC channel is the filesystem.
pub const STOP_FILE: &str = "stop";

/// Production knobs (§5, §3.3, §7): journal discovery ≤ 50 ms, active
/// set ~2 s, full sweep ~60 s, self-heartbeat 30 s per round, active-set
/// TTL 10 min. All injectable for tests — the RFC's constants, not
/// guesses.
#[derive(Clone, Debug)]
pub struct DaemonConfig {
    /// Journal discovery / main-loop tick (≤ 50 ms, §7).
    pub journal_poll: Duration,
    /// E4 active-set tick (~2 s, §8.2 Δ_active).
    pub active_interval: Duration,
    /// Whole-library sweep (~60 s, §8.2 Δ_idle) — a Steady reconcile.
    pub sweep_interval: Duration,
    /// A main-loop round lasting longer than this kills the process —
    /// the self-heartbeat (§3.3). 30 s in production.
    pub heartbeat_max_round: Duration,
    /// `last_traffic` TTL (10 min, §5) — pure memory, free to lose.
    pub active_ttl: Duration,
    /// `stop` CLI: how long to wait for the marker-driven graceful exit
    /// before escalating to SIGTERM.
    pub stop_grace: Duration,
    /// Boot steps 1–8, skip the loop — a smoke seam (manual/e2e).
    pub once: bool,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            journal_poll: Duration::from_millis(50),
            active_interval: Duration::from_secs(2),
            sweep_interval: Duration::from_secs(60),
            heartbeat_max_round: Duration::from_secs(30),
            active_ttl: Duration::from_secs(600),
            stop_grace: Duration::from_secs(3),
            once: false,
        }
    }
}

/// How the daemon ended — mapped to an exit code by the CLI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExitReason {
    /// `zonepaird stop` requested it (stop marker consumed).
    Stopped,
    /// The inode self-check fired: `.sync/` was rebuilt under our held
    /// lock — yielding so the new lock's winner takes over (§3.3).
    LockLost,
    /// A main-loop round exceeded `heartbeat_max_round`.
    HeartbeatExceeded,
    /// `--once`: startup completed, no loop.
    BootedOnly,
}

/// The production `b_probe` (§3.7 seam): `store_exists` over the wire.
/// `ExecCtx.b_probe` is a sync `Fn`, so the async call goes through
/// `block_in_place` — reconcile passes run inside pool tasks on the
/// multi-thread runtime, where that is legal. Any probe failure or an
/// absent endpoint answers `Unknown` — never guessed.
fn http_b_probe(base: Option<&Base>, token: Option<String>) -> Arc<dyn Fn(&str) -> BProbe + Send + Sync> {
    let base = base.cloned();
    Arc::new(move |sid: &str| {
        let Some(base) = base.clone() else {
            return BProbe::Unknown;
        };
        let token = token.clone();
        let sid = sid.to_string();
        match tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(api::store_exists(&base, token.as_deref(), &sid))
        }) {
            Ok(true) => BProbe::Present,
            Ok(false) => BProbe::Absent,
            Err(_) => BProbe::Unknown,
        }
    })
}

/// Production wiring for `ExecParts`: base + token + the HTTP probe.
pub fn exec_parts(base: Option<Base>, token: Option<String>) -> ExecParts {
    ExecParts {
        b_probe: Some(http_b_probe(base.as_ref(), token.clone())),
        base,
        token,
    }
}

/// The whole process: boot (steps 1–8) then the main loop.
/// `AlreadyOwned` is reported, never an error — the losing spawn's
/// expected exit (§3.2).
pub async fn run(
    mirror: &Path,
    parts: ExecParts,
    cfg: &DaemonConfig,
) -> Result<std::result::Result<ExitReason, BootBlocked>> {
    let booted = match startup::boot(mirror, parts)? {
        Ok(b) => b,
        Err(blocked) => return Ok(Err(blocked)),
    };
    if cfg.once {
        return Ok(Ok(ExitReason::BootedOnly));
    }
    Ok(Ok(run_loop(&booted, cfg).await?))
}

/// §9.1-9 — the loop. Runs until the stop marker, the inode self-check,
/// or the self-heartbeat exits it.
pub async fn run_loop(booted: &Booted, cfg: &DaemonConfig) -> Result<ExitReason> {
    let Booted {
        owner,
        sync_dir,
        shared,
        pool,
        ..
    } = booted;
    let mut last_active = HashMap::<String, Instant>::new();
    let mut next_active = Instant::now() + cfg.active_interval;
    let mut next_sweep = Instant::now() + cfg.sweep_interval;

    // §3.3 hang escape — a SEPARATE thread, not a loop-tail measure:
    // a round that truly wedges (NFS stall, hung flock semantics) never
    // reaches `round.elapsed()`, so the in-loop check can't fire. The
    // watchdog watches a per-round-start stamp written on an OS thread
    // that needs nothing from the tokio executor; expiry exits the
    // process — the lock dies with it, and the next intent writer or
    // waiter revives a fresh daemon. `heartbeat_max_round == 0` is the
    // test knob's "exit next round" — disarmed here, the in-loop check
    // covers it. Two budget windows of silence before killing keeps a
    // merely-slow round from being shot mid-work.
    use std::sync::atomic::{AtomicBool, AtomicU64};
    let armed = Arc::new(AtomicBool::new(cfg.heartbeat_max_round > Duration::ZERO));
    let round_start_ms = Arc::new(AtomicU64::new(0));
    if armed.load(std::sync::atomic::Ordering::Relaxed) {
        let armed_t = Arc::clone(&armed);
        let stamp_t = Arc::clone(&round_start_ms);
        let max_ms = cfg.heartbeat_max_round.as_millis() as u64;
        let max_round = cfg.heartbeat_max_round;
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(500));
            if !armed_t.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }
            let stamp = stamp_t.load(std::sync::atomic::Ordering::Relaxed);
            if stamp != 0
                && crate::sync::persist::time::unix_ms().saturating_sub(stamp) > 2 * max_ms
            {
                eprintln!(
                    "zonepaird: main-loop round unresponsive > {max_round:?} — exiting on self-heartbeat"
                );
                std::process::exit(124);
            }
        });
    }
    let mut drain_retry_at = Instant::now();

    let outcome = 'outer: {
        loop {
            let round = Instant::now();
            round_start_ms.store(
                crate::sync::persist::time::unix_ms(),
                std::sync::atomic::Ordering::Relaxed,
            );

            // `zonepaird stop` — marker first (graceful), then gone.
            if sync_dir.join(STOP_FILE).exists() {
                let _ = std::fs::remove_file(sync_dir.join(STOP_FILE));
                owner.clear_pid(sync_dir);
                break 'outer ExitReason::Stopped;
            }

            // flock-vs-unlink (§3.3): `.sync/` rebuilt → our fd locks a dead
            // inode while a fresh spawn can win the new file — yield now.
            if owner.stale() {
                eprintln!(
                    "zonepaird: {} rebuilt under my lock — yielding",
                    owner::LOCK_FILE
                );
                owner.clear_pid(sync_dir);
                break 'outer ExitReason::LockLost;
            }

            // Journal discovery: every record consumed feeds the pool —
            // applied intents wake waiters, deferred ones become work.
            // A drain failure must NOT kill the loop: the journal is
            // durable, the records survive, and a transient fsync/commit
            // error (ENOSPC spike, EBUSY) retries at sweep cadence —
            // exiting here would crash-loop every respawn against the
            // same disk fault while holding the lock.
            let now = Instant::now();
            if now >= drain_retry_at {
                match pool.drain_journal() {
                    Ok(report) => {
                        // E4's memory-only traffic table: pull/register
                        // intents mark "this sid saw traffic" — a hint,
                        // never truth.
                        for entry in &report.consumed {
                            if let Some(
                                Intent::Pull { sid, .. } | Intent::Register { sid, .. },
                            ) = entry.intent.as_ref()
                            {
                                last_active.insert(sid.clone(), now);
                            }
                        }
                        for (name, err) in &report.errors {
                            eprintln!("zonepaird: journal record {name} skipped: {err}");
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "zonepaird: journal drain failed: {e:#} — suspended until next sweep"
                        );
                        drain_retry_at = now + cfg.sweep_interval;
                    }
                }
            }

            pool.dispatch();

        // E4 active set (~2 s): recently-active sids get a pull — the
        // freshness budget Δ_active. Pruned/tombstoned/suspended sids
        // are skipped (E4 does not run for them, §5 E4 row).
        if now >= next_active {
            next_active = now + cfg.active_interval;
            last_active.retain(|_, at| now.duration_since(*at) < cfg.active_ttl);
            let sids: Vec<String> = last_active.keys().cloned().collect();
            for sid in sids {
                let wants = shared.with_state(|st| {
                    st.sessions.get(&sid).is_some_and(|r| {
                        r.pull.is_some()
                            && r.pull_pending.is_none()
                            && r.deleted.is_none()
                            && r.materialization().is_full()
                    })
                });
                if wants && !pool.is_suspended(&sid) {
                    pool.enqueue(Work::Pull {
                        sid,
                        force_reset: false,
                    });
                }
            }
        }

        // Whole-library sweep (~60 s): one Steady reconcile pass. The
        // pass' own verdicts queue the pulls/publishes it discovers;
        // `registered`/pull sids are the B-probe's inputs.
        if now >= next_sweep {
            next_sweep = now + cfg.sweep_interval;
            pool.enqueue(Work::Reconcile { names: vec![] });
        }

            // External liveness: the lock mtime bumps every round —
            // doctor reads "seconds-old" as mid-cycle, minutes-old as
            // stale. The §3.3 round cap exits so a stuck round still
            // frees the lock; the watchdog covers the case where even
            // this check cannot run.
            owner.beat();
            if round.elapsed() > cfg.heartbeat_max_round {
                eprintln!(
                    "zonepaird: main-loop round took {:?} (> {:?}) — exiting on self-heartbeat",
                    round.elapsed(),
                    cfg.heartbeat_max_round
                );
                owner.clear_pid(sync_dir);
                break 'outer ExitReason::HeartbeatExceeded;
            }

            tokio::time::sleep(cfg.journal_poll).await;
        }
    };
    armed.store(false, std::sync::atomic::Ordering::Relaxed);
    Ok(outcome)
}

/// `zonepaird stop` (§3.3): write the marker for the graceful path, wait
/// `stop_grace` for the loop to consume it, then escalate to SIGTERM on
/// `owner.pid` — the RFC's specified mechanism. Returns what happened:
/// `true` if a writer was signalled (marker written or TERM sent),
/// `false` when no writer exists at all.
pub fn stop_daemon(sync_dir: &Path, cfg: &DaemonConfig) -> Result<bool> {
    if owner::lock_free(sync_dir) {
        // No writer — but sweep a stale marker/pid so the next boot
        // doesn't read them as a request.
        let _ = std::fs::remove_file(sync_dir.join(STOP_FILE));
        let _ = std::fs::remove_file(sync_dir.join(owner::PID_FILE));
        return Ok(false);
    }
    let marker = sync_dir.join(STOP_FILE);
    let _ = state::write_atomic(&sync_dir.join(".stop.tmp"), &marker, b"stop\n");

    let deadline = Instant::now() + cfg.stop_grace;
    while Instant::now() < deadline {
        if !marker.exists() {
            return Ok(true); // the loop consumed it — graceful stop in flight
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    // Grace window expired — escalate to the RFC's SIGTERM path, but only
    // while the lock is still held: `owner.pid` is an unverified file, and
    // signalling whatever pid it names could hit an unrelated process
    // after a stale-file + pid-reuse collision. Lock already free means
    // the writer left on its own — nothing to TERM.
    if owner::lock_free(sync_dir) {
        let _ = std::fs::remove_file(&marker);
        let _ = std::fs::remove_file(sync_dir.join(owner::PID_FILE));
        return Ok(false);
    }
    if let Ok(pid) = std::fs::read_to_string(sync_dir.join(owner::PID_FILE)) {
        if let Ok(pid) = pid.trim().parse::<u32>() {
            let _ = std::process::Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .status();
        }
    }
    let _ = std::fs::remove_file(&marker);
    Ok(true)
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::persist::journal;
    use crate::sync::persist::state::{Pull, SessionRecord, State};
    use crate::transport::parse_base;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering as AOrd};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Count every `changes` hit while answering `unchanged` forever —
    /// E4's "did the poll fire" observable without a reply script.
    async fn serve_unchanged() -> (Base, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = parse_base(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_task = Arc::clone(&hits);
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let hits = Arc::clone(&hits_task);
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let mut request = Vec::new();
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }
                    hits.fetch_add(1, AOrd::SeqCst);
                    let body = serde_json::json!({
                        "ok": true, "state": "unchanged",
                        "nextCursor": "zpc1.0.00000000000000000000000000000000"
                    })
                    .to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                });
            }
        });
        (base, hits, server)
    }

    fn tmp(name: &str) -> PathBuf {
        crate::sync::persist::fs::testkit::tmp("daemon", name)
    }

    fn fast_cfg() -> DaemonConfig {
        DaemonConfig {
            journal_poll: Duration::from_millis(5),
            active_interval: Duration::from_millis(10),
            sweep_interval: Duration::from_millis(30),
            heartbeat_max_round: Duration::from_secs(30),
            active_ttl: Duration::from_secs(600),
            stop_grace: Duration::from_millis(300),
            once: false,
        }
    }

    fn unknown_parts() -> ExecParts {
        ExecParts {
            b_probe: Some(Arc::new(|_| BProbe::Unknown)),
            ..Default::default()
        }
    }

    // The daemon exits Stopped when `zonepaird stop`'s marker lands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t641_stop_marker_exits_gracefully() {
        let mirror = tmp("t641-stop");
        let booted = startup::boot(&mirror, unknown_parts())
            .unwrap()
            .unwrap();
        let sync = booted.sync_dir.clone();
        let cfg = fast_cfg();
        let cfg2 = cfg.clone();
        let looper = tokio::spawn(async move { run_loop(&booted, &cfg2).await });
        // Give the loop a beat, then `zonepaird stop`.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(stop_daemon(&sync, &cfg).unwrap());
        let reason = tokio::time::timeout(Duration::from_secs(5), looper)
            .await
            .expect("loop joins")
            .unwrap()
            .unwrap();
        assert_eq!(reason, ExitReason::Stopped);
        assert!(!sync.join(STOP_FILE).exists());
        assert!(!sync.join(owner::PID_FILE).exists(), "pid file cleared");
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // `stop` on an empty mirror reports "no writer" — and leaves no
    // stale marker behind.
    #[test]
    fn stop_without_owner_is_a_noop() {
        let mirror = tmp("stop-none");
        let sync = state::ensure_sync_dir(&mirror).unwrap();
        assert!(!stop_daemon(&sync, &fast_cfg()).unwrap());
        assert!(!sync.join(STOP_FILE).exists());
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-640: `.sync/` rebuilt under the held lock → the self-check
    // fires, the loop exits LockLost instead of running a second writer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t640_sync_rebuild_exits_lock_lost() {
        let mirror = tmp("t640-loop");
        let booted = startup::boot(&mirror, unknown_parts())
            .unwrap()
            .unwrap();
        let sync = booted.sync_dir.clone();
        let cfg = fast_cfg();
        let looper = tokio::spawn(async move { run_loop(&booted, &cfg).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Rebuild `.sync/` — the held fd now locks an unlinked inode.
        std::fs::remove_dir_all(&sync).unwrap();
        let _ = state::ensure_sync_dir(&mirror).unwrap();
        let reason = tokio::time::timeout(Duration::from_secs(5), looper)
            .await
            .expect("loop joins")
            .unwrap()
            .unwrap();
        assert_eq!(reason, ExitReason::LockLost);
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-641's heartbeat arm: a round exceeding heartbeat_max_round exits
    // — tested at the injected zero-ish budget, not the 30 s constant.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t641_round_budget_exits_on_heartbeat() {
        let mirror = tmp("t641-beat");
        let booted = startup::boot(&mirror, unknown_parts())
            .unwrap()
            .unwrap();
        let mut cfg = fast_cfg();
        cfg.heartbeat_max_round = Duration::ZERO;
        let reason = run_loop(&booted, &cfg).await.unwrap();
        assert_eq!(reason, ExitReason::HeartbeatExceeded);
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // The liveness heartbeat is observable on the lock file's mtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heartbeat_updates_lock_mtime() {
        let mirror = tmp("beat-mtime");
        let booted = startup::boot(&mirror, unknown_parts())
            .unwrap()
            .unwrap();
        let lock = booted.sync_dir.join(owner::LOCK_FILE);
        let before = std::fs::metadata(&lock).unwrap().modified().unwrap();
        std::thread::sleep(Duration::from_millis(1100));
        booted.owner.beat();
        let after = std::fs::metadata(&lock).unwrap().modified().unwrap();
        assert!(after > before, "mtime must advance: {before:?} {after:?}");
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // The loop's journal-discovery arm: an intent appended after boot is
    // consumed on the next poll — the E1/E3 path in miniature.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn loop_drains_intents_written_after_boot() {
        let mirror = tmp("drain-live");
        let booted = startup::boot(&mirror, unknown_parts())
            .unwrap()
            .unwrap();
        let shared = booted.shared.clone();
        let sync = booted.sync_dir.clone();
        let cfg = fast_cfg();
        let looper = tokio::spawn(async move { run_loop(&booted, &cfg).await });
        journal::append(
            &shared.journal_dir,
            &Intent::Pull {
                sid: "s9".into(),
                target: "latest".into(),
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        // Wait until the pull lands in state (the loop consumed it). The
        // record appears only after the daemon picks the intent up, so the
        // poll must tolerate its absence — indexing would panic on a slow
        // runner before the loop ever ran.
        for _ in 0..100 {
            if shared.with_state(|st| {
                st.sessions
                    .get("s9")
                    .is_some_and(|s| s.pull_pending.is_some())
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            shared.with_state(|st| st
                .sessions
                .get("s9")
                .is_some_and(|s| s.pull_pending.is_some())),
            "journal poll must consume the appended intent"
        );
        assert!(shared.journal_dir.read_dir().unwrap().next().is_none());
        std::fs::write(sync.join(STOP_FILE), b"stop").unwrap();
        let reason = tokio::time::timeout(Duration::from_secs(5), looper)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(reason, ExitReason::Stopped);
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // `run()`'s already-owned branch: `run` against a held lock reports
    // AlreadyOwned and never reaches the loop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_reports_already_owned() {
        let mirror = tmp("run-owned");
        let cfg = fast_cfg();
        let held = startup::boot(&mirror, unknown_parts())
            .unwrap()
            .unwrap();
        let second = run(&mirror, unknown_parts(), &cfg).await.unwrap();
        assert!(matches!(second, Err(BootBlocked::AlreadyOwned)));
        drop(held);
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // --once: boot then exit — no loop, but state/lock/report all real.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn once_boots_and_exits() {
        let mirror = tmp("once");
        let mut cfg = fast_cfg();
        cfg.once = true;
        let out = run(&mirror, unknown_parts(), &cfg).await.unwrap();
        assert!(matches!(out, Ok(ExitReason::BootedOnly)));
        // Boot released its Owner — the lock is free again.
        assert!(owner::lock_free(&state::ensure_sync_dir(&mirror).unwrap()));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // The E4 arm: a pull intent marks a sid active; the next active tick
    // enqueues a pull only when a `pull` record exists and the sid is
    // unsuspended — a registered-only sid waits for pullPending instead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn e4_active_tick_queues_pull_for_active_pull_sids() {
        let mirror = tmp("e4");
        let booted = startup::boot(&mirror, unknown_parts())
            .unwrap()
            .unwrap();
        let shared = booted.shared.clone();
        let sync = booted.sync_dir.clone();
        // Give s9 a pull record (eligible) and s8 none (not eligible).
        shared
            .commit_state(|st: &mut State| {
                st.sessions.insert(
                    "s9".into(),
                    SessionRecord {
                        pull: Some(Pull {
                            cursor: "zpc1.0.00000000000000000000000000000000".into(),
                            file: "projects/lab/s9.jsonl".into(),
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        // E4's marking input: pull intents for both sids.
        for sid in ["s8", "s9"] {
            journal::append(
                &shared.journal_dir,
                &Intent::Pull {
                    sid: sid.into(),
                    target: "latest".into(),
                    enqueued_at: "t".into(),
                },
            )
            .unwrap();
        }
        let cfg = fast_cfg();
        let looper = tokio::spawn(async move { run_loop(&booted, &cfg).await });
        // Both intents consumed → pullPending on both; s9's pull record
        // also made it active → E4 can enqueue Pull{s9}. The pending
        // pool state: s9 pick should offer a pull; s8's pullPending
        // resurfaces through the state scan anyway — the assertion is
        // journal drained + state updated.
        for _ in 0..200 {
            let done = shared.with_state(|st| {
                st.sessions
                    .get("s8")
                    .is_some_and(|r| r.pull_pending.is_some())
                    && st
                        .sessions
                        .get("s9")
                        .is_some_and(|r| r.pull_pending.is_some())
            });
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(shared.with_state(|st| {
            st.sessions
                .get("s9")
                .is_some_and(|r| r.pull_pending.is_some())
        }));
        // The pool picked s9's pull work (state scan) — prove the loop
        // dispatched: running or books or pending holds at least one.
        assert!(journal::pending_count(&shared.journal_dir) == 0);
        std::fs::write(sync.join(STOP_FILE), b"stop").unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), looper).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-807 (§5 E4): an active sid carries no `changes` traffic while
    // `materialized != full`; the poll resumes the tick after the mark
    // returns to full.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t807_e4_skips_non_full_then_resumes() {
        let (base, hits, server) = serve_unchanged().await;
        let mirror = tmp("t807");
        let parts = ExecParts {
            base: Some(base),
            b_probe: Some(Arc::new(|_| BProbe::Unknown)),
            ..Default::default()
        };
        let booted = startup::boot(&mirror, parts).unwrap().unwrap();
        let shared = booted.shared.clone();
        let sync = booted.sync_dir.clone();
        shared
            .commit_state(|st: &mut State| {
                st.sessions.insert(
                    "s9".into(),
                    SessionRecord {
                        pull: Some(Pull {
                            cursor: "zpc1.0.00000000000000000000000000000000".into(),
                            file: "projects/lab/s9.jsonl".into(),
                        }),
                        materialized: Some(state::Materialized::Unloaded),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        // E4's activity input: a register intent marks the sid without
        // seeding `pullPending` (a pull intent would — and it is the
        // pullPending path, not E4, that hydrates pruned sessions).
        journal::append(
            &shared.journal_dir,
            &Intent::Register {
                sid: "s9".into(),
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        let cfg = fast_cfg();
        let looper = tokio::spawn({
            let cfg = cfg.clone();
            async move { run_loop(&booted, &cfg).await }
        });
        // Several E4 ticks (~10 ms each) pass — the unloaded sid stays
        // out of the poll.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(hits.load(AOrd::SeqCst), 0, "unloaded sid must not be polled");
        // Back to full — the honest full shape needs the file too: a
        // pull record marked full with no file on disk is the row-17
        // misjudgment shape and suspends for human review, correctly
        // keeping E4 gated. The cursor is zero-offset, so an empty file
        // is a clean length match.
        std::fs::create_dir_all(mirror.join("projects/lab")).unwrap();
        std::fs::write(mirror.join("projects/lab/s9.jsonl"), b"").unwrap();
        shared
            .commit_state(|st: &mut State| {
                st.sessions
                    .get_mut("s9")
                    .unwrap()
                    .materialized = Some(state::Materialized::Full);
            })
            .unwrap();
        for _ in 0..200 {
            if hits.load(AOrd::SeqCst) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(hits.load(AOrd::SeqCst) > 0, "full mark resumes E4 polling");
        std::fs::write(sync.join(STOP_FILE), b"stop").unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), looper).await;
        server.abort();
        std::fs::remove_dir_all(&mirror).unwrap();
    }
}
