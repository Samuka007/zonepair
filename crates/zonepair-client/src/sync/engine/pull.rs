//! The pull executor (RFC-0003-zonepaird §6.1): `changes` → `apply` →
//! commit, looping until `unchanged` (the "latest" convergence) or until
//! the pinned `pullPending.target` is reached. The wire-failure
//! classifier both request-facing executors share also lives here.

use anyhow::Result;
use std::path::Path;
use std::sync::atomic::Ordering;

use crate::api;
use crate::proto::cursor;
use crate::transport::{StatusError, TransportFault};

use crate::sync::persist::fs;
use crate::sync::persist::state::{Materialized, Pull, SessionRecord};
use crate::sync::persist::time::now_rfc3339;

use super::apply::{self, ChangesReply as ApplyReply, Outcome};
use super::pool::TaskOutcome;
use super::reconcile;
use super::shared::ExecCtx;

// ------------------------------------------------------------ executors ---

/// The pull executor: `changes` → `apply` → commit, looping until
/// `unchanged` (the "latest" convergence) or until the pinned
/// `pullPending.target` is reached. Every cursor commit lands only after
/// the bytes it names are fsync-complete — the write-order rule is
/// `apply.rs`'s; this loop only sequences requests around it.
pub async fn run_pull(ctx: &ExecCtx, sid: &str, force_reset: bool) -> TaskOutcome {
    // Snapshot the starting position FIRST: a tombstoned or unknown
    // session exits without needing an endpoint at all (the tombstone
    // suppresses every materializing action, §4.3-4①).
    let Some((mut cursor, file_rel, mut hydrate, had_pending)) = ctx.shared.with_state(|st| {
        let rec = st.sessions.get(sid)?;
        if rec.deleted.is_some() {
            return None;
        }
        let hydrate = !rec.materialization().is_full();
        let cursor = rec.pull.as_ref().map(|p| p.cursor.clone()).unwrap_or_default();
        let file_rel = rec
            .pull
            .as_ref()
            .map(|p| p.file.clone())
            .or_else(|| default_session_file_rel(&ctx.shared.mirror, sid));
        Some((cursor, file_rel, hydrate, rec.pull_pending.is_some()))
    }) else {
        return TaskOutcome::Done;
    };
    // §4.3-4② ordering gate: a `Work::Pull` booked BEFORE an evict must
    // not resurrect the session the evict just pruned. The surviving
    // `pullPending` field is the evidence of post-evict traffic — every
    // legal hydrate channel carries it (E1/E3 land as `pull` intents →
    // `pullPending`; a forced reset from reconcile's HydrateReset row is
    // the same whole-pull shape and is unaffected by this gate because
    // `force_reset` skips here). A stale pull on a pruned record has no
    // such evidence: it is consumed, never diverted.
    if !force_reset && hydrate && !had_pending {
        return TaskOutcome::Done;
    }
    let Some(base) = ctx.base.clone() else {
        return TaskOutcome::Degraded("no sync endpoint configured".into());
    };
    if ctx.shared.fail_closed.load(Ordering::Relaxed) {
        return TaskOutcome::Pending("state corrupt — suspended".into());
    }
    let Some(rel) = file_rel else {
        return TaskOutcome::Degraded(format!("bad-sid: {sid:?} cannot name a mirror file"));
    };
    // The hydrate path is a forced whole pull (§4.5): `materialized!=full`
    // bytes are not a legal prefix, so the cursor zeroes exactly like
    // `force_reset` — B answers `reset` and the same-transaction commit
    // returns `materialized="full"`.
    if force_reset || hydrate {
        cursor.clear();
    }
    let mut diverged_once = false;
    loop {
        let reply = match api::sync::changes(&base, ctx.token.as_deref(), sid, &cursor).await {
            Ok(r) => r,
            Err(e) => return classify_failure(&e, "bad-cursor"),
        };
        let (apply_reply, what) = match reply {
            api::sync::ChangesReply::Unchanged { .. } => {
                // A registered-but-empty first pull leaves no bytes: create
                // the empty file so `pull ∧ no file` does not suspend the
                // session next reconcile pass. The same holds on the
                // hydrate path — `unchanged` against a zero cursor means
                // B's whole file is empty, so a stub/unloaded shell is
                // legitimately replaced by zero bytes, not "diverged".
                let file = ctx.shared.mirror.join(&rel);
                if hydrate || !file.exists() {
                    if let Err(e) = force_empty(&file) {
                        return TaskOutcome::Retry(format!("touch {}: {e}", file.display()));
                    }
                }
                if let Err(e) = ctx.shared.commit_state(|st| {
                    let rec = st.sessions.entry(sid.to_string()).or_default();
                    if rec.deleted.is_some() {
                        return;
                    }
                    // First pull of an empty session still lands `pull` —
                    // `[0,0)` IS a legal prefix.
                    if rec.pull.is_none() {
                        rec.begin_pull(Pull {
                            cursor: cursor::ZERO.into(),
                            file: rel.clone(),
                        });
                    }
                    if hydrate {
                        rec.materialized = Some(Materialized::Full);
                        // B said unchanged against the ZERO cursor — the
                        // file is legitimately empty, so the cursor must
                        // zero too: keeping the pre-evict cursor would
                        // make the next pull request a stale offset and
                        // pay one LocalDiverged + re-reset round-trip.
                        rec.pull = Some(Pull {
                            cursor: cursor::ZERO.into(),
                            file: rel.clone(),
                        });
                    }
                    rec.last_unchanged_at = Some(now_rfc3339());
                    if pending_satisfied(rec, true) {
                        rec.satisfy_pull_pending();
                    }
                }) {
                    return TaskOutcome::Retry(format!("commit unchanged: {e:#}"));
                }
                ctx.shared.notify.signal(sid);
                return TaskOutcome::Done;
            }
            api::sync::ChangesReply::Missing => {
                // §6.3: 404 does NOT consume `pullPending` — the intent
                // persists until B produces the file; reconcile classifies
                // the session meantime.
                return TaskOutcome::Pending("B 404 — intent conserved".into());
            }
            api::sync::ChangesReply::Unsupported => {
                return TaskOutcome::Degraded("unsupported".into());
            }
            api::sync::ChangesReply::Append { body, next_cursor } => (
                ApplyReply::Append {
                    request_offset: cursor::parse(&cursor).map(|c| c.offset).unwrap_or(0),
                    body,
                    next_cursor,
                },
                "append",
            ),
            api::sync::ChangesReply::Reset { body, next_cursor } => {
                (ApplyReply::Reset { body, next_cursor }, "reset")
            }
        };
        // Append and Reset share one outcome handling — the variants
        // differ only in the request (`request_offset` is append's
        // prefix-proof input); what landed and what comes next is the
        // same transaction either way.
        match apply::apply(
            &apply_reply,
            &ctx.shared.mirror.join(&rel),
            ctx.shared
                .with_state(|st| st.sessions.get(sid).and_then(|r| r.deleted.clone()))
                .as_ref(),
        ) {
            Ok(Outcome::Advanced { next_cursor }) => {
                if let Err(e) = commit_pull(ctx, sid, &next_cursor, &rel, hydrate) {
                    return TaskOutcome::Retry(format!("commit {what}: {e:#}"));
                }
                hydrate = false;
                cursor = next_cursor;
                // §7.2 convergence: a pinned pullPending satisfied
                // same-transaction means the pull's goal is met —
                // stop here instead of draining to `unchanged`
                // behind a still-growing session.
                if had_pending
                    && ctx.shared.with_state(|st| {
                        st.sessions.get(sid).is_none_or(|r| r.pull_pending.is_none())
                    })
                {
                    return TaskOutcome::Done;
                }
            }
            Ok(Outcome::Unchanged) => {}
            Ok(Outcome::LocalDiverged { .. }) => {
                // §6.3: local prefix can no longer prove itself —
                // re-request with the zero cursor once; a second
                // divergence is the degraded terminal.
                if diverged_once {
                    return TaskOutcome::Degraded("local-diverged".into());
                }
                diverged_once = true;
                cursor.clear();
            }
            Ok(Outcome::TombstoneSuppressed) => return TaskOutcome::Done,
            Err(e) => return TaskOutcome::Retry(format!("apply {what}: {e:#}")),
        }
    }
}
/// `pull.cursor` commit — after `apply` has already fsync'd the bytes.
/// `hydrate` marks the `materialized != full → full` same-transaction
/// return (§4.5 hydrate sequence); `registered` clears through
/// `begin_pull`; `pullPending` is satisfied same-transaction when its
/// target was reached (§9 contract for `sync --wait`).
fn commit_pull(ctx: &ExecCtx, sid: &str, next_cursor: &str, rel: &str, hydrate: bool) -> Result<()> {
    ctx.shared.commit_state(|st| {
        let rec = st.sessions.entry(sid.to_string()).or_default();
        if rec.deleted.is_some() {
            return; // tombstone committed mid-pull — never resurrect
        }
        rec.begin_pull(Pull {
            cursor: next_cursor.to_string(),
            file: rel.to_string(),
        });
        if hydrate {
            rec.materialized = Some(Materialized::Full);
        }
        if pending_satisfied(rec, false) {
            rec.satisfy_pull_pending();
        }
    })?;
    ctx.shared.notify.signal(sid);
    Ok(())
}

/// `pullPending` satisfaction (§3.3 + §9): `latest` is satisfied by an
/// `unchanged` verdict; a pinned cursor is satisfied when the pull
/// cursor's fingerprint segment matches AND byte count reached it. A
/// fingerprint mismatch means the target attests a different file —
/// conserved forever (the §7.2 divergence rule `wait_cursor` used to
/// report lives here now).
fn pending_satisfied(rec: &SessionRecord, on_unchanged: bool) -> bool {
    let Some(pp) = &rec.pull_pending else {
        return false;
    };
    if pp.target == "latest" {
        return on_unchanged;
    }
    let (Some(pull), Ok(target)) = (rec.pull.as_ref(), cursor::parse(&pp.target)) else {
        return false;
    };
    cursor::parse(&pull.cursor)
        .map(|c| c.prefix_hash == target.prefix_hash && c.offset >= target.offset)
        .unwrap_or(false)
}
/// Failure classification shared by both executors: `StatusError` 4xx is
/// the caller-error family (`degraded_reason` names which), 5xx is
/// transient after the transport budget, `TransportFault`/`reqwest`/`Elapsed`
/// (the request-phase and body-idle deadlines in `request_raw`) is the
/// network class, and anything else (fsync, serde, sid validation)
/// books a queue retry — the budget turns a persistent one into
/// `degraded(network)` instead of guessing a terminal taxonomy for it.
pub(crate) fn classify_failure(e: &anyhow::Error, degraded_reason: &'static str) -> TaskOutcome {
    if let Some(se) = e.downcast_ref::<StatusError>() {
        if (400..500).contains(&se.status) {
            return TaskOutcome::Degraded(degraded_reason.to_string());
        }
        return TaskOutcome::Retry(format!("http {}", se.status));
    }
    if e.is::<TransportFault>()
        || e.is::<tokio::time::error::Elapsed>()
        || e.downcast_ref::<reqwest::Error>().is_some()
    {
        return TaskOutcome::Retry(format!("transport: {e:#}"));
    }
    TaskOutcome::Retry(format!("{e:#}"))
}
// ------------------------------------------------------------ utilities ---

/// `projects/lab/<sid>.jsonl` is the landing convention; an already-
/// discovered file elsewhere under `projects/**` wins (record binding
/// will store whatever path we actually used). A sid that cannot name a
/// safe relative path returns `None`.
fn default_session_file_rel(mirror: &Path, sid: &str) -> Option<String> {
    if sid.is_empty()
        || sid.bytes().any(|b| matches!(b, b'/' | b'\\' | 0))
        || sid.contains("..")
    {
        return None;
    }
    if let Some(found) = reconcile::session_file(mirror, sid) {
        if let Ok(rel) = found.strip_prefix(mirror) {
            return Some(rel.to_string_lossy().into_owned());
        }
    }
    Some(format!("projects/lab/{sid}.jsonl"))
}

/// Make `file` a durable zero-byte session file (first pull of an empty
/// session, or `unchanged` on the hydrate path): parent dirs, 0600, the
/// same `.zp<PID>`-temp atomic write as reset bodies — a stub shell is
/// *replaced* (rename), an absent file is created.
fn force_empty(file: &Path) -> Result<()> {
    fs::write_zp_atomic(file, b"")
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use serde_json::json;

    use crate::sync::engine::pool::{Pool, Work};
    use crate::sync::engine::testkit::*;
    use crate::sync::persist::journal::{self, Intent};
    use crate::sync::persist::state;

    use crate::sync::persist::fs::testkit::write_file;

    // -- pull executor -------------------------------------------------

    // T-601/602: transport layer burns its own 3 attempts per request;
    // the queue layer then books a Retry. An expired budget lands the
    // pull-path degrade in the runtime overlay — `degraded(network)` —
    // never a task row, never auto-reset.
    #[tokio::test]
    async fn t602_pull_transport_retry_budget_degrades_network() {
        let script = vec![
            vec![Scripted::Drop],
            vec![Scripted::Drop],
            vec![Scripted::Drop],
        ];
        let (base, seen, server) = serve_script(script).await;
        let (mirror, shared) = setup("t602");
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
        let ctx = ctx_with(&base, &shared);
        let outcome = run_pull(&ctx, "s1", false).await;
        assert!(matches!(outcome, TaskOutcome::Retry(_)), "{outcome:?}");
        assert_eq!(
            lines(&seen).len(),
            3,
            "transport layer consumed its 3-attempt budget"
        );

        // Queue layer: book it, let the (injected, tiny) budget expire,
        // then pick() performs the degrade — suspend.pull carries the sid
        // and NO task row ever appears.
        let pool = Pool::new(ctx.clone()).with_policy(fast_policy());
        let work = Work::Pull {
            sid: "s1".into(),
            force_reset: false,
        };
        pool.finish(&work, outcome);
        tokio::time::sleep(Duration::from_millis(10)).await;
        // pick() drains the expired book → degrade.
        let _ = pool.pick();
        assert!(
            pool.is_suspended("s1"),
            "pull-degraded sid must sit in the overlay"
        );
        assert!(shared.with_state(|st| st.sessions["s1"].task.is_none()),
            "pull degraded must not mint a task row");
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-604: changes 404 `code:"missing"` → Pending (never degraded, and
    // `pullPending` stays put — the intent outlives the 404).
    #[tokio::test]
    async fn t604_changes_missing_pends_and_conserves_intent() {
        let script = vec![vec![Scripted::Reply(http_json(
            404,
            &json!({"ok": false, "error": "nope", "code": "missing"}).to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let (mirror, shared) = setup("t604");
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "s1".into(),
                    SessionRecord {
                        registered: Some(state::Registered {
                            kind: "b-owned".into(),
                            at: "t".into(),
                        }),
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
        let ctx = ctx_with(&base, &shared);
        let outcome = run_pull(&ctx, "s1", false).await;
        assert!(matches!(outcome, TaskOutcome::Pending(_)), "{outcome:?}");
        assert!(shared.with_state(|st| st.sessions["s1"].pull_pending.is_some()),
            "404 must not consume pullPending");
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-608: 404 with an empty body is "server too old" — a definitive
    // degraded(unsupported), never a silent full pull.
    #[tokio::test]
    async fn t608_old_server_404_is_degraded_unsupported() {
        let script = vec![vec![Scripted::Reply(
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
        )]];
        let (base, seen, server) = serve_script(script).await;
        let (mirror, shared) = setup("t608");
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
        let ctx = ctx_with(&base, &shared);
        let outcome = run_pull(&ctx, "s1", false).await;
        assert_eq!(outcome, TaskOutcome::Degraded("unsupported".into()));
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-603: changes 400 (bad cursor) → degraded(bad-cursor), no repull.
    #[tokio::test]
    async fn t603_changes_400_is_degraded_bad_cursor() {
        let script = vec![vec![Scripted::Reply(http_json(
            400,
            &json!({"ok": false, "error": "bad cursor"}).to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let (mirror, shared) = setup("t603");
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
        write_file(&mirror, "projects/lab/s1.jsonl", b"{\"a\":1}\n");
        let ctx = ctx_with(&base, &shared);
        let outcome = run_pull(&ctx, "s1", false).await;
        assert_eq!(outcome, TaskOutcome::Degraded("bad-cursor".into()));
        assert_eq!(lines(&seen).len(), 1);
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-605 + apply-order: an append verdict whose `p.offset` disagrees
    // with the local length first re-requests with the ZERO cursor (the
    // reset converges); a second divergence is degraded(local-diverged).
    #[tokio::test]
    async fn t605_local_diverged_zero_cursor_reset_converges() {
        let reset_body = b"{\"full\":1}\n";
        let next = "zpc1.11.bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let script = vec![
            // Request carries the stale cursor → server says append at p.offset=8.
            vec![Scripted::Reply(http_octets("append", "zpc1.12.cccccccccccccccccccccccccccccccc", b"xx"))],
            // Zero-cursor re-request → reset with the whole stable prefix.
            vec![Scripted::Reply(http_octets("reset", next, reset_body))],
            // Follow-up → unchanged.
            vec![Scripted::Reply(http_json(
                200,
                &json!({"ok": true, "state": "unchanged", "nextCursor": next}).to_string(),
            ))],
        ];
        let (base, seen, server) = serve_script(script).await;
        let (mirror, shared) = setup("t605");
        write_file(&mirror, "projects/lab/s1.jsonl", b"{\"a\":1}\nEXTRA-GROWTH");
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "s1".into(),
                    SessionRecord {
                        pull: Some(Pull {
                            cursor: "zpc1.8.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                            file: "projects/lab/s1.jsonl".into(),
                        }),
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
        let ctx = ctx_with(&base, &shared);
        let outcome = run_pull(&ctx, "s1", false).await;
        assert_eq!(outcome, TaskOutcome::Done, "{outcome:?}");
        let reqs = lines(&seen);
        assert_eq!(reqs.len(), 3, "{reqs:?}");
        assert!(
            reqs[0].contains("cursor=zpc1.8"),
            "first request carries the stale cursor: {reqs:?}"
        );
        assert!(
            !reqs[1].contains("cursor="),
            "divergence re-requests with the zero cursor: {reqs:?}"
        );
        let file = mirror.join("projects/lab/s1.jsonl");
        assert_eq!(std::fs::read(&file).unwrap(), reset_body);
        let pull = pull_state(&shared, "s1").unwrap();
        assert_eq!(pull.cursor, next);
        assert!(shared.with_state(|st| st.sessions["s1"].pull_pending.is_none()),
            "unchanged consumes a latest pullPending");
        assert!(shared.with_state(|st| st.sessions["s1"].last_unchanged_at.is_some()));
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Pull-until-unchanged convergence: append, append, unchanged →
    // cursor lands at the last commit, file holds all bytes.
    #[tokio::test]
    async fn pull_loops_until_unchanged() {
        let c1 = "zpc1.3.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let c2 = "zpc1.6.bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let script = vec![
            vec![Scripted::Reply(http_octets("append", c1, b"one"))],
            vec![Scripted::Reply(http_octets("append", c2, b"two"))],
            vec![Scripted::Reply(http_json(
                200,
                &json!({"ok": true, "state": "unchanged", "nextCursor": c2}).to_string(),
            ))],
        ];
        let (base, seen, server) = serve_script(script).await;
        let (mirror, shared) = setup("loop");
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
        let ctx = ctx_with(&base, &shared);
        // First pull: empty cursor → B answers append anyway (mock).
        let outcome = run_pull(&ctx, "s1", false).await;
        assert_eq!(outcome, TaskOutcome::Done);
        let file = mirror.join("projects/lab/s1.jsonl");
        assert_eq!(std::fs::read(&file).unwrap(), b"onetwo");
        assert_eq!(pull_state(&shared, "s1").unwrap().cursor, c2);
        assert_eq!(lines(&seen).len(), 3);
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-805's worker half: `materialized != full` converts the pull into
    // the hydrate path — zero cursor out, `reset` in, and the same
    // transaction commits `materialized="full"` + the new cursor.
    #[tokio::test]
    async fn t805_pull_on_pruned_record_hydrates_to_full() {
        let body = b"{\"x\":1}\n";
        let next = "zpc1.7.bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let script = vec![
            vec![Scripted::Reply(http_octets("reset", next, body))],
            vec![Scripted::Reply(http_json(
                200,
                &json!({"ok": true, "state": "unchanged", "nextCursor": next}).to_string(),
            ))],
        ];
        let (base, seen, server) = serve_script(script).await;
        let (mirror, shared) = setup("t805");
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "s1".into(),
                    SessionRecord {
                        pull: Some(Pull {
                            cursor: "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                            file: "projects/lab/s1.jsonl".into(),
                        }),
                        materialized: Some(Materialized::Unloaded),
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
        let ctx = ctx_with(&base, &shared);
        let outcome = run_pull(&ctx, "s1", false).await;
        assert_eq!(outcome, TaskOutcome::Done);
        let reqs = lines(&seen);
        assert!(!reqs[0].contains("cursor="), "hydrate starts from zero: {reqs:?}");
        let rec = shared.with_state(|st| st.sessions["s1"].clone());
        assert_eq!(rec.materialization(), Materialized::Full,
            "hydrate commit returns materialized to full same-transaction");
        assert_eq!(rec.pull.as_ref().unwrap().cursor, next);
        assert_eq!(std::fs::read(mirror.join("projects/lab/s1.jsonl")).unwrap(), body);
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Post-review NEW defect: a `Work::Pull` booked BEFORE an evict
    // commits (boot recovery queue, E4 tick) must not resurrect the
    // pruned session — it predates the evict, so the §4.3-4② gate drops
    // it: `materialized != full` with no surviving `pullPending` = stale
    // work, not traffic. A pull intent arriving AFTER the evict is the
    // opposite case (pullPending present) and does hydrate.
    #[tokio::test]
    async fn queued_pull_predating_evict_does_not_resurrect() {
        let (mirror, shared) = setup("pull-pre-evict");
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
        // The pull is queued BEFORE the evict is even drained — the
        // E4/boot-recovery shape (no pullPending anywhere).
        let pool = Pool::new(ExecCtx::new(shared.clone()));
        pool.enqueue(Work::Pull {
            sid: "s1".into(),
            force_reset: false,
        });
        journal::append(
            &shared.journal_dir,
            &Intent::Evict {
                sid: "s1".into(),
                mode: journal::EvictMode::Drop,
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        pool.drain_journal().unwrap();
        // Materialize outranks pull; run them in pick order (serial sid).
        let first = pool.pick().unwrap();
        assert!(matches!(first, Work::Materialize { .. }));
        assert_eq!(pool.run_one(&first).await, TaskOutcome::Done);
        pool.finish(&first, TaskOutcome::Done);
        assert!(!mirror.join("projects/lab/s1.jsonl").exists());
        // The stale pull runs next — ExecCtx has no base: a divert or an
        // append would need the wire, but the gate drops it first.
        let second = pool.pick().unwrap();
        assert!(matches!(second, Work::Pull { .. }), "{second:?}");
        let outcome = pool.run_one(&second).await;
        assert_eq!(outcome, TaskOutcome::Done, "stale pull consumed, no divert");
        assert!(!mirror.join("projects/lab/s1.jsonl").exists(),
            "just-pruned session must not resurrect");
        assert_eq!(
            shared.with_state(|st| st.sessions["s1"].materialization()),
            Materialized::Unloaded
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Full-review R2-C1 (= R4-C1): a pull intent read in AFTER an evict
    // must survive BOTH the parked record's replayed drain and the
    // evict's `materialized` commit — pre-fix, each re-applied
    // `pull_pending = None` and the pick-gate then dropped the pull as
    // stale, silently losing the intent. The provenance chain under
    // test: evict read-in stamps nothing; the pull's journal filename
    // becomes `pullPending.record`; drains while the evict is parked
    // skip `apply` for it; the commit clears only pendings naming an
    // EARLIER record. What the pull then does: reach the executor and
    // try the wire — `Degraded(no endpoint)` proves it was NOT gated.
    #[tokio::test]
    async fn post_evict_pull_pending_survives_replay_and_commit() {
        let (mirror, shared) = setup("post-evict-pull");
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
        let pool = Pool::new(ExecCtx::new(shared.clone()));
        // ① evict lands first and defers (parked, awaiting execution).
        let evict_path = journal::append(
            &shared.journal_dir,
            &Intent::Evict {
                sid: "s1".into(),
                mode: journal::EvictMode::Drop,
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        let evict_name = evict_path.file_name().unwrap().to_string_lossy().into_owned();
        pool.drain_journal().unwrap();
        // ② pull intent enqueued after the evict's read-in — post-evict
        // traffic by journal order; filename sorts after.
        let pull_path = journal::append(
            &shared.journal_dir,
            &Intent::Pull {
                sid: "s1".into(),
                target: "latest".into(),
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        let pull_name = pull_path.file_name().unwrap().to_string_lossy().into_owned();
        assert!(pull_name > evict_name, "later record orders later");
        pool.drain_journal().unwrap();
        assert_eq!(
            shared.with_state(|st| {
                st.sessions["s1"].pull_pending.as_ref().unwrap().record.clone()
            }),
            Some(pull_name.clone()),
            "pullPending carries journal-order provenance"
        );
        // ③ crash+replay: drains while the evict stays parked must not
        // re-fire its clear — the pending survives in memory AND on disk.
        pool.drain_journal().unwrap();
        pool.drain_journal().unwrap();
        assert!(shared.with_state(|st| st.sessions["s1"].pull_pending.is_some()),
            "parked evict re-read must not wipe the later pending");
        let disk = state::load(&shared.sync_dir).unwrap();
        assert!(disk.state.sessions["s1"].pull_pending.is_some(),
            "survival is durable, not just in-memory");
        // ④ evict executes — commit clears only pre-evict pendings.
        let mat = pool.pick().expect("deferred evict is work");
        assert!(matches!(mat, Work::Materialize { .. }));
        assert_eq!(pool.run_one(&mat).await, TaskOutcome::Done);
        pool.finish(&mat, TaskOutcome::Done);
        assert!(!mirror.join("projects/lab/s1.jsonl").exists());
        assert!(
            shared.with_state(|st| st.sessions["s1"].pull_pending.is_some()),
            "commit must not clear a pending stamped by a LATER record"
        );
        // ⑤ the pending now drives the pull through the §4.3-4② gate —
        // it reaches the executor (Degraded on the missing endpoint is
        // proof it was NOT dropped as stale).
        let pull = pool.pick().expect("post-evict pull is live work");
        assert!(matches!(&pull, Work::Pull { sid, .. } if sid == "s1"));
        let outcome = pool.run_one(&pull).await;
        assert!(
            matches!(outcome, TaskOutcome::Degraded(ref r) if r.contains("endpoint")),
            "post-evict pull must reach the wire path, got {outcome:?}"
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-805's settle half: after the hydrate divert consumes a
    // `latest` pullPending, `sync --wait`'s structural predicate sees the
    // session settled — a pruned session does not wedge the wait.
    #[tokio::test]
    async fn t805_pull_on_pruned_record_settles_for_sync_wait() {
        let body = b"{\"x\":1}\n";
        let next = "zpc1.7.bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let script = vec![
            vec![Scripted::Reply(http_octets("reset", next, body))],
            vec![Scripted::Reply(http_json(
                200,
                &json!({"ok": true, "state": "unchanged", "nextCursor": next}).to_string(),
            ))],
        ];
        let (base, seen, server) = serve_script(script).await;
        let (mirror, shared) = setup("t805settle");
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "s1".into(),
                    SessionRecord {
                        pull: Some(Pull {
                            cursor: "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                            file: "projects/lab/s1.jsonl".into(),
                        }),
                        materialized: Some(Materialized::Stub),
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
        let ctx = ctx_with(&base, &shared);
        let outcome = run_pull(&ctx, "s1", false).await;
        assert_eq!(outcome, TaskOutcome::Done);
        let rec = shared.with_state(|st| st.sessions["s1"].clone());
        assert_eq!(rec.materialization(), Materialized::Full);
        assert!(rec.pull_pending.is_none(), "pullPending consumed");
        // The §9 structural predicate: journal empty ∧ no pullPending ∧
        // no non-terminal task — the pruned-then-hydrated session settles.
        assert!(shared.with_state(|st| {
            crate::sync::state_machine::sync_settled(st, journal::pending_count(&shared.journal_dir) == 0)
        }));
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Tombstone read at snapshot → no request at all.
    #[tokio::test]
    async fn pull_on_tombstoned_session_sends_nothing() {
        let script = vec![];
        let (_base, seen, server) = serve_script(script).await;
        let (mirror, shared) = setup("tombed");
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "s1".into(),
                    SessionRecord {
                        deleted: Some(state::Deleted {
                            at: "t".into(),
                            published_at_delete: false,
                            file_id: None,
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        let ctx = ExecCtx::new(shared.clone());
        let outcome = run_pull(&ctx, "s1", false).await;
        assert_eq!(outcome, TaskOutcome::Done);
        assert!(lines(&seen).is_empty());
        let _ = server;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

}
