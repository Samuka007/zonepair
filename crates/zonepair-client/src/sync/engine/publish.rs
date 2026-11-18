//! The publish executor (RFC-0003-zonepaird §8.1): the four-part
//! precheck, one `publish` RPC, and the maximum single transaction —
//! `phase=published` + `publishedAt` + `pull` in ONE commit.

use std::sync::atomic::Ordering;

use crate::api;
use crate::transport::StatusError;

use crate::sync::persist::state::{Phase, Pull};
use crate::sync::persist::time::now_rfc3339;
use crate::sync::state_machine::FileProbe;

use super::pool::TaskOutcome;
use super::pull::classify_failure;
use super::shared::{ExecCtx, Shared};

/// The publish executor (§8.1): re-read the file (the spawn evidence may
/// predate the last write), run the four-part precheck — file exists ∧
/// carries `forkedFrom` ∧ still A-owned (no `pull`/`publishedAt`/
/// `registered`) ∧ tombstone missed — then one `publish` call, then the
/// maximum single transaction: `phase=published` + `publishedAt` + `pull`
/// in ONE commit.
pub async fn run_publish(ctx: &ExecCtx, sid: &str) -> TaskOutcome {
    // Snapshot the task row BEFORE requiring an endpoint: a missing row
    // or a tombstoned record ends the task without touching anything —
    // the waiter's own branches (task absent ∧ publishedAt, or
    // `deleted`) already tell the truth, and a vanished file owes the
    // tombstone commit regardless of network.
    let Some(file_rel) = ctx.shared.with_state(|st| {
        let rec = st.sessions.get(sid)?;
        if rec.deleted.is_some() {
            return Some(None);
        }
        rec.task.as_ref().map(|t| Some(t.file.clone()))
    }) else {
        return TaskOutcome::Done;
    };
    let Some(file_rel) = file_rel else {
        return TaskOutcome::Done;
    };
    let file = ctx.shared.mirror.join(&file_rel);
    let bytes = match std::fs::read(&file) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Precheck ① — file vanished: §8.1's explicit transition is
            // "terminate the task and lay a tombstone", not degraded.
            // Commit it so the waiter reads `Superseded`.
            if let Err(e) = ctx.shared.commit_state(|st| {
                st.sessions
                    .entry(sid.to_string())
                    .or_default()
                    .bury(now_rfc3339(), None);
            }) {
                return TaskOutcome::Retry(format!("commit tombstone: {e:#}"));
            }
            ctx.shared.notify.signal(sid);
            return TaskOutcome::Done;
        }
        Err(e) => return TaskOutcome::Degraded(format!("precheck: read {}: {e}", file.display())),
    };
    // Precheck ②–④ in one probe: forkedFrom marker + still-A-owned +
    // tombstone. `registered` counts as "not A-owned" — B already knows
    // this id as b-owned, publishing would fork the namespace.
    let probe = FileProbe::from_bytes(&bytes);
    let owned_check = ctx.shared.with_state(|st| {
        let rec = st.sessions.get(sid)?;
        if rec.deleted.is_some() {
            return Some(false);
        }
        Some(rec.pull.is_none() && rec.published_at.is_none() && rec.registered.is_none())
    });
    let ok = probe.forked_from && owned_check == Some(true);
    if !ok {
        let why = if !probe.forked_from {
            "precheck: no forkedFrom marker"
        } else {
            "precheck: no longer A-owned (pull/publishedAt/registered present)"
        };
        degrade_task(&ctx.shared, sid, why);
        return TaskOutcome::Degraded("precheck".into());
    }
    let Some(base) = ctx.base.clone() else {
        return TaskOutcome::Degraded("no sync endpoint configured".into());
    };
    if ctx.shared.fail_closed.load(Ordering::Relaxed) {
        return TaskOutcome::Pending("state corrupt — suspended".into());
    }
    match api::sync::publish(&base, ctx.token.as_deref(), sid, &bytes).await {
        Ok(api::sync::PublishReply::Published { cursor, .. }) => {
            // The maximum single transaction (§3.1/§8.1): three fields in
            // one commit — phase, the published fact, and the pull channel
            // handoff. `already:true` takes the same path (idempotent).
            if let Err(e) = ctx.shared.commit_state(|st| {
                let rec = st.sessions.entry(sid.to_string()).or_default();
                if rec.deleted.is_some() {
                    return;
                }
                if let Some(task) = rec.task.as_mut() {
                    task.phase = Phase::Published;
                }
                rec.published_at = Some(now_rfc3339());
                rec.begin_pull(Pull {
                    cursor,
                    file: file_rel.clone(),
                });
            }) {
                return TaskOutcome::Retry(format!("commit published: {e:#}"));
            }
            ctx.shared.notify.signal(sid);
            TaskOutcome::Done
        }
        Ok(api::sync::PublishReply::Conflict) => {
            // §6.3: terminal divergence — no retry, no silent merge.
            degrade_task(&ctx.shared, sid, "conflict");
            TaskOutcome::Degraded("conflict".into())
        }
        Err(e) => {
            // Caller-error family (4xx): degrade with the real reason in
            // `lastError`; 5xx/transport stays a queue-level retry.
            if let Some(se) = e.downcast_ref::<StatusError>() {
                if (400..500).contains(&se.status) {
                    degrade_task(&ctx.shared, sid, &format!("bad-request: {e:#}"));
                    return TaskOutcome::Degraded("bad-request".into());
                }
            }
            classify_failure(&e, "bad-request")
        }
    }
}

/// Commit `task.phase=degraded` + `lastError` — the durable terminal.
/// `may_advance_to` keeps the edge honest: a `published` row never
/// regresses to degraded.
fn degrade_task(shared: &Shared, sid: &str, reason: &str) {
    if let Err(e) = shared.commit_state(|st| {
        if let Some(task) = st
            .sessions
            .get_mut(sid)
            .and_then(|rec| rec.task.as_mut())
        {
            if task.phase.may_advance_to(Phase::Degraded) {
                task.phase = Phase::Degraded;
                task.last_error = Some(reason.to_string());
            }
        }
    }) {
        eprintln!("zonepaird: degraded commit for {sid} failed: {e:#}");
    }
    shared.notify.signal(sid);
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use crate::sync::engine::pool::{Pool, Work};
    use crate::sync::engine::testkit::*;
    use crate::sync::persist::journal::{self, Intent};
    use crate::sync::persist::state::{self, Phase, SessionRecord};

    use crate::sync::persist::fs::testkit::write_file;

    // -- publish executor -----------------------------------------------

    fn publish_ready(shared: &Shared, sid: &str) {
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    sid.into(),
                    SessionRecord {
                        task: Some(state::Task {
                            file: format!("projects/lab/{sid}.jsonl"),
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
    }

    // §8.1 happy path + T-607's `already:true`: precheck passes, publish
    // answers with a cursor, ONE commit lands phase+publishedAt+pull.
    #[tokio::test]
    async fn publish_confirmed_commits_triple_in_one_transaction() {
        let bytes = b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n";
        let cursor = "zpc1.34.0123456789abcdef0123456789abcdef";
        let script = vec![vec![Scripted::Reply(http_json(
            200,
            &json!({"ok": true, "published": true, "already": true, "cursor": cursor}).to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let (mirror, shared) = setup("pub-ok");
        write_file(&mirror, "projects/lab/s2.jsonl", bytes);
        publish_ready(&shared, "s2");
        let ctx = ctx_with(&base, &shared);
        let outcome = run_publish(&ctx, "s2").await;
        assert_eq!(outcome, TaskOutcome::Done);
        let rec = shared.with_state(|st| st.sessions["s2"].clone());
        assert_eq!(rec.task.as_ref().unwrap().phase, Phase::Published);
        assert!(rec.published_at.is_some());
        assert_eq!(rec.pull.as_ref().unwrap().cursor, cursor);
        assert_eq!(rec.pull.as_ref().unwrap().file, "projects/lab/s2.jsonl");
        // The disk commit is the same transaction — state.json holds all
        // three fields together (never a partial publish commit).
        let disk = state::load(&shared.sync_dir).unwrap();
        let d = &disk.state.sessions["s2"];
        assert!(d.published_at.is_some() && d.pull.is_some());
        assert_eq!(lines(&seen).len(), 1, "already:true counts as published — no retry");
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-606: publish 400 → degraded(bad-request), phase committed.
    #[tokio::test]
    async fn t606_publish_400_is_degraded_bad_request() {
        let script = vec![vec![Scripted::Reply(http_json(
            400,
            &json!({"ok": false, "error": "sha256 mismatch"}).to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let (mirror, shared) = setup("t606");
        write_file(&mirror, "projects/lab/s2.jsonl", b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n");
        publish_ready(&shared, "s2");
        let ctx = ctx_with(&base, &shared);
        let outcome = run_publish(&ctx, "s2").await;
        assert_eq!(outcome, TaskOutcome::Degraded("bad-request".into()));
        let task = shared.with_state(|st| st.sessions["s2"].task.clone()).unwrap();
        assert_eq!(task.phase, Phase::Degraded);
        assert!(task.last_error.unwrap().contains("bad-request"));
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-607: publish 409 conflict → degraded(conflict) terminal, no
    // retry, no silent merge.
    #[tokio::test]
    async fn t607_publish_conflict_is_degraded_conflict() {
        let script = vec![vec![Scripted::Reply(http_json(
            409,
            &json!({"ok": false, "error": "conflict"}).to_string(),
        ))]];
        let (base, seen, server) = serve_script(script).await;
        let (mirror, shared) = setup("t607");
        write_file(&mirror, "projects/lab/s2.jsonl", b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n");
        publish_ready(&shared, "s2");
        let ctx = ctx_with(&base, &shared);
        let outcome = run_publish(&ctx, "s2").await;
        assert_eq!(outcome, TaskOutcome::Degraded("conflict".into()));
        assert_eq!(lines(&seen).len(), 1, "409 must not retry");
        let task = shared.with_state(|st| st.sessions["s2"].task.clone()).unwrap();
        assert_eq!(task.phase, Phase::Degraded);
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-702's precheck arms: file vanished → tombstone commit (not
    // degraded); missing forkedFrom → degraded(precheck); a `pull` on the
    // record (no longer A-owned) → degraded(precheck).
    #[tokio::test]
    async fn t702_publish_precheck_transitions() {
        let (mirror, shared) = setup("t702");
        let ctx = ExecCtx::new(shared.clone());

        // Arm ① file gone → task terminates, tombstone lands.
        publish_ready(&shared, "gone");
        let outcome = run_publish(&ctx, "gone").await;
        assert_eq!(outcome, TaskOutcome::Done);
        let rec = shared.with_state(|st| st.sessions["gone"].clone());
        assert!(rec.deleted.is_some() && rec.task.is_none());

        // Arm ② no forkedFrom → degraded(precheck).
        write_file(&mirror, "projects/lab/nomark.jsonl", b"{\"a\":1}\n");
        publish_ready(&shared, "nomark");
        let outcome = run_publish(&ctx, "nomark").await;
        assert_eq!(outcome, TaskOutcome::Degraded("precheck".into()));
        let task = shared.with_state(|st| st.sessions["nomark"].task.clone()).unwrap();
        assert_eq!(task.phase, Phase::Degraded);
        assert!(task.last_error.unwrap().contains("forkedFrom"));

        // Arm ③ pull present → not A-owned → degraded(precheck).
        write_file(&mirror, "projects/lab/bowned.jsonl", b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n");
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "bowned".into(),
                    SessionRecord {
                        task: Some(state::Task {
                            file: "projects/lab/bowned.jsonl".into(),
                            phase: Phase::Validated,
                            first_seen: "t".into(),
                            enqueued_at: "t".into(),
                            last_error: None,
                        }),
                        pull: Some(Pull {
                            cursor: "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                            file: "projects/lab/bowned.jsonl".into(),
                        }),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        let outcome = run_publish(&ctx, "bowned").await;
        assert_eq!(outcome, TaskOutcome::Degraded("precheck".into()));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-609: degraded is a persistent terminal — pick() never resurfaces
    // it, reconcile never resets it; a fresh ensure-task intent's
    // `validated` phase is the only recovery edge.
    #[tokio::test]
    async fn t609_degraded_is_terminal_until_new_intent_resets() {
        let (mirror, shared) = setup("t609");
        write_file(&mirror, "projects/lab/s2.jsonl", b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n");
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "s2".into(),
                    SessionRecord {
                        task: Some(state::Task {
                            file: "projects/lab/s2.jsonl".into(),
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
        let pool = Pool::new(ExecCtx::new(shared.clone()));
        assert!(pool.pick().is_none(), "degraded task must never resurface");

        // The §6.3 recovery path: a fresh ensure-task intent lands
        // `validated` — the only out-edge — and pick() re-offers it.
        let jdir = shared.journal_dir.clone();
        journal::append(
            &jdir,
            &Intent::EnsureTask {
                sid: "s2".into(),
                file: "projects/lab/s2.jsonl".into(),
                phase: Phase::Validated,
                enqueued_at: "t2".into(),
            },
        )
        .unwrap();
        pool.drain_journal().unwrap();
        assert_eq!(
            shared.with_state(|st| st.sessions["s2"].task.as_ref().unwrap().phase),
            Phase::Validated,
            "fresh intent explicitly resets degraded → validated"
        );
        let work = pool.pick().expect("reset task must be pickable");
        assert_eq!(work, Work::Publish { sid: "s2".into() });
        std::fs::remove_dir_all(&mirror).unwrap();
    }

}
