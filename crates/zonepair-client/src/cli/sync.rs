//! sync: the manual E5 trigger — enqueue `reconcile{scope:"all"}` into the
//! journal; `--wait` then polls the RFC-0002-a-state §9 structural predicate
//! (journal drained ∧ no `pullPending` ∧ every task terminal/absent/orphan
//! `writing`) instead of measuring clock freshness.
//!
//! Short-lived process rule: this command never mutates `state.json` —
//! the read-only decoder answers "is the backlog empty" from disk while
//! the writer (zonepaird, spawned by `ensure_owner`) does the work.

use super::transport::read_state_ro;
use super::zonepaird::enqueue;
use super::{parse_cli, resolve_mirror_dir};
use crate::sync::state_machine as classify;
use crate::sync::persist::journal::{self, Intent};
use crate::sync::state;
use crate::sync::wait;
use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// The §9 drain loop's poll cadence — same ≤50 ms interval the wait
/// predicates use cross-process.
const POLL: Duration = Duration::from_millis(50);
/// `--wait`'s budget = the §6.2 task budget (300 s): a full-library
/// reconcile over a cold mirror can legitimately take minutes.
const WAIT_BUDGET: Duration = Duration::from_secs(300);
/// R4-M1: the drain poll doubles as the waiter's revive duty (§7.1 —
/// "who notices absence first revives"), but `revive_dead_writer`
/// probes `owner.lock` itself, so calls are throttled to once per few
/// seconds rather than every 50 ms poll tick.
const REVIVE_PROBE: Duration = Duration::from_secs(2);

#[derive(clap::Parser)]
#[command(name = "sync")]
struct SyncCli {
    /// After enqueuing, poll until the journal is drained and state settled
    #[arg(long)]
    wait: bool,
}

pub(crate) async fn sync_cli(args: &[String]) -> Result<()> {
    let cli: SyncCli = parse_cli("sync", args);
    let wait_flag = cli.wait;
    let mirror = PathBuf::from(resolve_mirror_dir(None));
    enqueue(
        &mirror,
        Intent::Reconcile {
            enqueued_at: state::now_rfc3339(),
        },
    )
    .context("enqueuing reconcile intent")?;

    if !wait_flag {
        println!("sync: reconcile intent enqueued");
        return Ok(());
    }

    let sync_dir = state::sync_dir(&mirror);
    match wait_drained(&sync_dir, WAIT_BUDGET, POLL).await {
        Ok(()) => {
            println!("sync ok: journal drained, state settled");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// §9's three conditions, read straight off disk. `journal_empty` counts
/// only formally-named records — `*.tmp` residue and debris are cleanup
/// artifacts, not backlog.
fn drained(sync_dir: &Path) -> bool {
    if state::corrupt_files(sync_dir)
        .map(|v| !v.is_empty())
        .unwrap_or(false)
    {
        // Fail-closed (§8): the failure mode never "settles" — report
        // through the caller's corruption check instead.
        return true;
    }
    let journal_dir = sync_dir.join("journal");
    let journal_empty = journal::pending_count(&journal_dir) == 0;
    let state = read_state_ro(sync_dir);
    classify::sync_settled(&state, journal_empty)
}

async fn wait_drained(sync_dir: &Path, budget: Duration, poll: Duration) -> Result<()> {
    let deadline = Instant::now() + budget;
    // `<mirror>/.sync`'s parent is the mirror `revive_dead_writer` binds
    // a respawned zonepaird to.
    let mirror = sync_dir.parent().unwrap_or(sync_dir).to_path_buf();
    let mut next_revive = Instant::now();
    loop {
        if let Some(corrupt) = state::corrupt_files(sync_dir)?.into_iter().next() {
            bail!(
                "sync --wait: state corrupt — resolve {} manually (§8 fail-closed)",
                corrupt.display()
            );
        }
        if drained(sync_dir) {
            return Ok(());
        }
        // R4-M1: a dead writer (owner.lock free) gets revived while we
        // wait — the same §3.2/§3.4 duty every other waiter carries.
        // Throttled: the probe is a flock try-lock, cheap but not free.
        if Instant::now() >= next_revive {
            next_revive = Instant::now() + REVIVE_PROBE;
            wait::revive_dead_writer(sync_dir, &mirror);
        }
        if Instant::now() >= deadline {
            bail!("sync --wait: backlog did not settle within {budget:?}");
        }
        tokio::time::sleep(poll).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::testkit::hold_owner;
    use crate::cli::try_parse_cli;
    use crate::sync::persist::fs::testkit::tmp;
    use crate::sync::persist::state::{Phase, PullPending, SessionRecord, Task};

    fn s(v: &str) -> String {
        v.to_string()
    }

    #[test]
    fn t14_sync_flag_equals() {
        // --flag=value gain: bool flags don't take values in clap either,
        // so the pinned form here is the bare flag plus rejection of junk.
        let cli = try_parse_cli::<SyncCli>("sync", &[s("--wait")]).unwrap();
        assert!(cli.wait);
        let cli = try_parse_cli::<SyncCli>("sync", &[]).unwrap();
        assert!(!cli.wait);
        assert!(try_parse_cli::<SyncCli>("sync", &[s("bogus")]).is_err());
        assert!(try_parse_cli::<SyncCli>("sync", &[s("--wait=true")]).is_err());
    }

    // T-652: `sync` lands a `reconcile{scope:"all"}` intent — nothing
    // else, and the record is a formal journal file.
    #[test]
    fn t652_sync_enqueues_reconcile_all() {
        let mirror = tmp("synccli", "t652");
        // Hold the owner lock: `enqueue` sees the writer-present branch
        // and never spawns a daemon from the test.
        let _owner = {
            let sync = state::ensure_sync_dir(&mirror).unwrap();
            crate::daemon::owner::acquire(&sync)
                .unwrap()
                .expect("test must own the lock")
        };
        enqueue(
            &mirror,
            Intent::Reconcile {
                enqueued_at: state::now_rfc3339(),
            },
        )
        .unwrap();
        let dir = state::sync_dir(&mirror).join("journal");
        let (entries, debris) = journal::read_pending(&dir).unwrap();
        assert_eq!(entries.len(), 1);
        assert!(debris.is_empty());
        assert!(matches!(
            entries[0].record,
            Ok(Intent::Reconcile { .. })
        ));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-709/T-495: `--wait`'s structural predicate — drains only when the
    // journal is empty AND every task is terminal/absent/orphan-writing
    // AND no pullPending owes. A `pull`-absent session is not debt.
    #[test]
    fn t709_drained_predicate_three_conditions() {
        let mirror = tmp("synccli", "t709");
        let sync_dir = state::ensure_sync_dir(&mirror).unwrap();
        let journal_dir = sync_dir.join("journal");
        state::ensure_dir(&journal_dir, 0o700).unwrap();

        // Clean mirror: nothing owed → drained immediately.
        assert!(drained(&sync_dir));

        // An unconsumed intent blocks draining.
        journal::append(
            &journal_dir,
            &Intent::Reconcile {
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        assert!(!drained(&sync_dir));

        // Consume it (commit + delete) — drained again.
        let mut st = state::State::empty_v3();
        journal::consume_ordered(&journal_dir, &sync_dir, &mut st, |_, _| None).unwrap();
        // `reconcile` is deferred: the record survives until the caller's
        // pass deletes it — still backlog.
        assert!(!drained(&sync_dir));
        let (entries, _) = journal::read_pending(&journal_dir).unwrap();
        for e in &entries {
            std::fs::remove_file(journal_dir.join(&e.name)).unwrap();
        }
        assert!(drained(&sync_dir));

        // pullPending owes → not settled.
        let mut st = state::State::empty_v3();
        st.sessions.insert(
            "s".into(),
            SessionRecord {
                pull_pending: Some(PullPending {
                    target: "latest".into(),
                    enqueued_at: "t".into(),
                    record: None,
                }),
                ..SessionRecord::default()
            },
        );
        state::commit(&sync_dir, &st).unwrap();
        assert!(!drained(&sync_dir));

        // A `validated` publish in flight owes; `published`/`degraded`
        // and orphan `writing` do not.
        for (phase, settled) in [
            (Phase::Validated, false),
            (Phase::Published, true),
            (Phase::Degraded, true),
            (Phase::Writing, true),
        ] {
            let mut st = state::State::empty_v3();
            st.sessions.insert(
                "s".into(),
                SessionRecord {
                    task: Some(Task {
                        file: "projects/lab/s.jsonl".into(),
                        phase,
                        first_seen: "t".into(),
                        enqueued_at: "t".into(),
                        last_error: None,
                    }),
                    ..SessionRecord::default()
                },
            );
            state::commit(&sync_dir, &st).unwrap();
            assert_eq!(drained(&sync_dir), settled, "phase {phase:?}");
        }
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // wait_drained returns once a backlog disappears — deterministic
    // state mutation drives it, no sleeps on the assertion side.
    #[tokio::test]
    async fn wait_drained_returns_when_settled() {
        let mirror = tmp("synccli", "drain");
        // Hold the owner lock: wait_drained's revive duty (R4-M1) must
        // see a live writer — a free lock would spawn current_exe as
        // `zonepaird`, i.e. re-run the whole test binary.
        let _owner = hold_owner(&mirror);
        let sync_dir = state::sync_dir(&mirror);
        let journal_dir = sync_dir.join("journal");
        journal::append(
            &journal_dir,
            &Intent::Pull {
                sid: "s".into(),
                target: "latest".into(),
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        let sd = sync_dir.clone();
        let drainer = tokio::spawn(async move {
            // Simulate the writer consuming the intent + satisfying the
            // pending pull mid-wait.
            tokio::time::sleep(Duration::from_millis(60)).await;
            let mut st = state::State::empty_v3();
            journal::consume_ordered(&journal_dir, &sd, &mut st, |_, _| None).unwrap();
            st.sessions.get_mut("s").unwrap().satisfy_pull_pending();
            state::commit(&sd, &st).unwrap();
        });
        wait_drained(&sync_dir, Duration::from_secs(5), Duration::from_millis(5))
            .await
            .unwrap();
        drainer.await.unwrap();
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Corruption is a loud failure, never a settle.
    #[tokio::test]
    async fn wait_drained_fails_loud_on_corruption() {
        let mirror = tmp("synccli", "corrupt");
        let _owner = hold_owner(&mirror);
        let sync_dir = state::sync_dir(&mirror);
        std::fs::write(sync_dir.join("state.corrupt-1"), "{}").unwrap();
        let err = wait_drained(&sync_dir, Duration::from_secs(1), Duration::from_millis(5))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("state.corrupt"), "{err:#}");
        std::fs::remove_dir_all(&mirror).unwrap();
    }
}
