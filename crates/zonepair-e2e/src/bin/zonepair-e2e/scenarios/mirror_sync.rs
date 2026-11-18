use crate::fixtures::*;
use crate::mirror::files::*;
use crate::mirror::journal::*;
use crate::mirror::waits::*;
use crate::mirror::{run_stub, spawn_daemon, spawn_daemon_up, spawn_stub, shutdown_writer, Daemon, MirrorHome};
use serde_json::{json, Value};
use std::time::Duration;
use zonepair_client::{
    api,
    daemon::owner,
    proto::cursor,
    sync::{
        journal::Intent,
        state::{self, Deleted, Phase, Pull, Registered, SessionRecord, State, Task},
    },
};

// ---- T-701..T-716 --------------------------------------------------------------

/// T-701: publish-wait full lifecycle T0–T6 — fork file → reconcile
/// `writing` → argv judgement → `ensure-task` → worker precheck → publish →
/// single-transaction `phase=published`+`publishedAt`+`pull` → Published →
/// the resume proceeds to spawn.
pub(crate) async fn t701_publish_wait_lifecycle(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t701";
    let parent = "t701-parent";
    let fork = lines_bytes(&session_lines(sid, 2, Some(parent)));
    write_mirror_file(&home, sid, &fork);

    // T0–T1: boot reconcile sees the fresh fork product → task(writing).
    let mut daemon = spawn_daemon_up(&home, server, &[]);
    wait_state(
        "T1 task(writing) registered",
        &home,
        Duration::from_secs(10),
        |st| st.sessions.get(sid).is_some_and(|r| r.task.as_ref().is_some_and(|t| t.phase == Phase::Writing)),
    );
    daemon.kill();
    wait_lock_free(&home, Duration::from_secs(10));

    // T2: resume argv → the adapter's judgement hits continuation-fork →
    // `ensure-task{validated}` + the hard wait. Holding the lock ourselves
    // freezes the sequence at T2: the intent lands durable and nothing can
    // consume it — the raw journal record is only observable this way.
    let held_lock = owner::acquire(&home.sync_dir()).unwrap().expect("lock must be free");
    let mut stub = spawn_stub(&home, server, &["--resume", sid]);
    wait_for(
        "T2 ensure-task journal record",
        Duration::from_secs(10),
        || {
            journal_intents(&home).iter().any(|i| {
                matches!(i, Intent::EnsureTask { sid: s, phase: Phase::Validated, .. } if s == sid)
            })
        },
    );

    // T3 onward: release the lock — the stub's own revive loop spawns the
    // writer (§7.1), T3 consume → T4 publish → T5 commit → T6 Published.
    drop(held_lock);

    // T3–T6: validated → publish → the max single transaction, then the
    // WS opens and the resume reaches a result frame.
    wait_state(
        "T5 published + publishedAt + pull committed",
        &home,
        Duration::from_secs(45),
        |st| {
            st.sessions.get(sid).is_some_and(|r| {
                r.task.as_ref().is_some_and(|t| t.phase == Phase::Published)
                    && r.published_at.is_some()
                    && r.pull.is_some()
            })
        },
    );
    let st = read_state(&home);
    let rec = &st.sessions[sid];
    assert_eq!(
        rec.pull.as_ref().unwrap().cursor,
        cursor_for(&fork),
        "pull.cursor must be the publish confirmation cursor"
    );

    stub.send(&json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":"resume me"}]}}).to_string());
    let result = stub.wait_frame("result", Duration::from_secs(15));
    assert_eq!(result.get("subtype").and_then(Value::as_str), Some("success"));
    stub.close_stdin();
    stub.wait_exit(Duration::from_secs(10));
    drop(stub);

    // A following reconcile pass must leave the terminal row untouched —
    // `published` is a durable terminal (row 5 → Steady), `publishedAt`
    // + `pull` persist, nothing regresses.
    write_reconcile_intent(&home);
    wait_state(
        "reconcile leaves the published terminal intact",
        &home,
        Duration::from_secs(20),
        |st| {
            journal_empty(&home)
                && st.sessions.get(sid).is_some_and(|r| {
                    r.task.as_ref().is_some_and(|t| t.phase == Phase::Published)
                        && r.published_at.is_some()
                        && r.pull.is_some()
                })
        },
    );

    // B holds the fork product byte-for-byte.
    assert_eq!(
        store_bytes(server, sid).unwrap_or_else(|| fail("published file missing on B")),
        fork
    );
    daemon.kill();
}

/// T-702: the worker's four-part precheck, each arm — file vanished →
/// tombstone; missing `forkedFrom` → degraded(precheck); record no longer
/// A-owned (`pull` present) → degraded(precheck); product gone before
/// resume → the stub exits loudly on Superseded.
pub(crate) async fn t702_publish_precheck_arms(server: &Server) {
    let home = MirrorHome::new();
    let now = || state::now_rfc3339();

    // Plant the three record-arm scenarios at once; one daemon resolves
    // all of them through the same worker path.
    let mut st = State::empty_v3();
    for sid in ["t702a", "t702b", "t702c"] {
        st.sessions.insert(
            sid.to_string(),
            SessionRecord {
                task: Some(Task {
                    file: format!("projects/lab/{sid}.jsonl"),
                    phase: Phase::Validated,
                    first_seen: now(),
                    enqueued_at: now(),
                    last_error: None,
                }),
                ..SessionRecord::default()
            },
        );
    }
    // Arm ③: `pull` already present — not A-owned any more.
    st.sessions.get_mut("t702c").unwrap().pull = Some(Pull {
        cursor: cursor::ZERO.to_string(),
        file: "projects/lab/t702c.jsonl".to_string(),
    });
    let sync_dir = home.sync_dir();
    state::commit(&sync_dir, &st).unwrap();

    // Arm ①: validated, file already gone. Arm ②: file without the marker.
    // Arm ③: marker present but the record says B-owned.
    write_mirror_file(&home, "t702b", &lines_bytes(&session_lines("t702b", 1, None)));
    write_mirror_file(&home, "t702c", &lines_bytes(&session_lines("t702c", 1, Some("p"))));

    let mut daemon = spawn_daemon_up(&home, server, &[]);
    wait_state(
        "arm① file vanished → tombstone",
        &home,
        Duration::from_secs(30),
        |st| st.sessions.get("t702a").is_some_and(|r| r.deleted.is_some()),
    );
    wait_state(
        "arm② no forkedFrom → degraded(precheck)",
        &home,
        Duration::from_secs(30),
        |st| {
            st.sessions.get("t702b").is_some_and(|r| {
                r.task.as_ref().is_some_and(|t| {
                    t.phase == Phase::Degraded
                        && t.last_error.as_deref().is_some_and(|e| e.contains("precheck"))
                })
            })
        },
    );
    wait_state(
        "arm③ pull on record → degraded(precheck)",
        &home,
        Duration::from_secs(30),
        |st| {
            st.sessions.get("t702c").is_some_and(|r| {
                r.task.as_ref().is_some_and(|t| {
                    t.phase == Phase::Degraded
                        && t.last_error.as_deref().is_some_and(|e| e.contains("precheck"))
                })
            })
        },
    );
    daemon.kill();
    wait_lock_free(&home, Duration::from_secs(10));

    // Arm ④ Superseded: the waiter holds its ensure-task while the file
    // vanishes — a parked owner lock guarantees that ordering. The daemon
    // spawned by the waiter's revive resolves the session to a tombstone
    // and the stub exits loudly without ever opening the WS.
    let sid = "t702d";
    write_mirror_file(&home, sid, &lines_bytes(&session_lines(sid, 1, Some("p"))));
    let held_lock = owner::acquire(&sync_dir).unwrap().expect("lock must be free after daemon kill");
    let mut stub = spawn_stub(&home, server, &["--resume", sid]);
    wait_for(
        "superseded arm: ensure-task intent durable",
        Duration::from_secs(10),
        || {
            journal_intents(&home).iter().any(|i| {
                matches!(i, Intent::EnsureTask { sid: s, .. } if s == sid)
            })
        },
    );
    std::fs::remove_file(mirror_file(&home, sid).unwrap()).unwrap();
    drop(held_lock);
    let status = stub
        .wait_exit(Duration::from_secs(45))
        .unwrap_or_else(|| fail("superseded stub did not exit"));
    let err = stub.stderr_text();
    assert!(
        !status.success() && err.contains("superseded"),
        "superseded must exit loudly: status={status:?} stderr={err}"
    );
    wait_state(
        "arm④ tombstone landed",
        &home,
        Duration::from_secs(20),
        |st| st.sessions.get(sid).is_some_and(|r| r.deleted.is_some()),
    );
    drop(stub);
}

/// T-703: N concurrent zonepaird spawns self-converge — exactly one holds
/// the lock, the rest exit 0 at startup step 1 (§3.2: no election, no
/// residue, no error).
pub(crate) async fn t703_self_converging_spawns(server: &Server) {
    let home = MirrorHome::new();
    let n = 4;
    let mut daemons: Vec<Daemon> = (0..n).map(|_| spawn_daemon(&home, server, &[])).collect();

    wait_for(
        "n-1 losing spawns exit",
        Duration::from_secs(20),
        || {
            daemons
                .iter_mut()
                .filter_map(|d| d.exited())
                .count()
                >= n - 1
        },
    );
    let mut exited = Vec::new();
    let mut running = Vec::new();
    for (i, d) in daemons.iter_mut().enumerate() {
        match d.exited() {
            Some(s) => exited.push((i, s)),
            None => running.push(i),
        }
    }
    assert_eq!(running.len(), 1, "exactly one daemon may hold the lock: {running:?}");
    for (i, status) in &exited {
        assert!(
            status.success(),
            "losing spawn {i} must exit 0 (self-convergence), got {status:?}"
        );
    }
    assert!(!owner::lock_free(&home.sync_dir()), "the survivor must hold owner.lock");
    for d in &mut daemons {
        d.kill();
    }
    wait_lock_free(&home, Duration::from_secs(10));
}

/// T-704: crash window — file already written past the committed cursor
/// (SIGKILL mid-commit). Restart's startup reconcile truncates the torn
/// tail (local len > cursor bytes), then the daemon resumes incrementally:
/// mirror converges to B's bytes and never claims progress past durable
/// content.
pub(crate) async fn t704_sigkill_mid_commit(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t704";
    let prefix = lines_bytes(&session_lines(sid, 2, None));
    let suffix = lines_bytes(&session_lines(&format!("{sid}-x"), 1, None));
    let mut full = prefix.clone();
    full.extend_from_slice(&suffix);
    write_store_file(server, sid, &full);

    // A live daemon first — kill it while it owns the write path, then
    // plant exactly the crash-window shape its mid-commit death leaves.
    let mut d1 = spawn_daemon_up(&home, server, &[]);
    write_pull_intent(&home, sid);
    wait_mirror_bytes("initial pull converged", &home, sid, &full, Duration::from_secs(20));
    d1.kill(); // SIGKILL — the crash injection
    wait_lock_free(&home, Duration::from_secs(10));

    // Residue: cursor committed at `prefix`, file carrying a torn tail —
    // "file written, cursor uncommitted".
    let mut st = State::empty_v3();
    st.sessions.insert(
        sid.to_string(),
        SessionRecord {
            pull: Some(Pull {
                cursor: cursor_for(&prefix),
                file: format!("projects/lab/{sid}.jsonl"),
            }),
            ..SessionRecord::default()
        },
    );
    let sync_dir = home.sync_dir();
    state::commit(&sync_dir, &st).unwrap();
    let mut torn = prefix.clone();
    torn.extend_from_slice(b"{\"uuid\":\"torn-half-line-no-newline");
    write_mirror_file(&home, sid, &torn);

    // Boot-only run: startup reconcile truncates our own torn tail to the
    // cursor (§7.2 startup partition) — deterministic intermediate assert.
    let out = run_stub(&home, server, &["zonepaird", "--mirror", &home.mirror.display().to_string(), "--url", server.base.as_str(), "--once"], Duration::from_secs(30));
    let Some(out) = out else {
        fail("--once boot timed out")
    };
    assert!(
        out.status.success(),
        "--once boot failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        mirror_bytes(&home, sid).unwrap_or_else(|| fail("mirror file vanished")),
        prefix,
        "startup reconcile must truncate to the cursor offset"
    );

    // The real daemon then resumes from the cursor — append, not reset.
    let mut d2 = spawn_daemon_up(&home, server, &[]);
    wait_mirror_bytes("converged to B prefix after resume", &home, sid, &full, Duration::from_secs(30));
    wait_state(
        "cursor committed at the boundary",
        &home,
        Duration::from_secs(10),
        |st| {
            st.sessions.get(sid).and_then(|r| r.pull.as_ref()).is_some_and(|p| {
                p.cursor == cursor_for(&full)
            })
        },
    );
    d2.kill();
}

/// T-705: crash window — publish confirmed, the confirmation transaction
/// never committed. The restarted worker redrives the publish; B answers
/// `already:true` and the single-transaction commit lands.
pub(crate) async fn t705_publish_replay_idempotent(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t705";
    let fork = lines_bytes(&session_lines(sid, 2, Some("t705-p")));

    // A live writer was running when the crash hit — the kill is real;
    // the residue below is what it left.
    let mut d1 = spawn_daemon_up(&home, server, &[]);
    d1.kill();
    wait_lock_free(&home, Duration::from_secs(10));

    // Crash shape: the publish already landed on B (byte-identical file
    // exists), A's state still says `validated` and the `ensure-task`
    // journal record was never consumed.
    write_mirror_file(&home, sid, &fork);
    let mut st = State::empty_v3();
    st.sessions.insert(
        sid.to_string(),
        SessionRecord {
            task: Some(Task {
                file: format!("projects/lab/{sid}.jsonl"),
                phase: Phase::Validated,
                first_seen: state::now_rfc3339(),
                enqueued_at: state::now_rfc3339(),
                last_error: None,
            }),
            ..SessionRecord::default()
        },
    );
    state::commit(&home.sync_dir(), &st).unwrap();
    write_intent(
        &home,
        &Intent::EnsureTask {
            sid: sid.to_string(),
            file: format!("projects/lab/{sid}.jsonl"),
            phase: Phase::Validated,
            enqueued_at: state::now_rfc3339(),
        },
    );
    // The confirmation the dead daemon never saw: B already holds it.
    match api::sync::publish(&server.base, None, sid, &fork).await.unwrap() {
        api::sync::PublishReply::Published { already, .. } => assert!(!already),
        other => fail(&format!("seed publish: {other:?}")),
    }

    let mut d2 = spawn_daemon_up(&home, server, &[]);
    wait_state(
        "replay publishes → published + pull",
        &home,
        Duration::from_secs(30),
        |st| {
            st.sessions.get(sid).is_some_and(|r| {
                r.published_at.is_some() && r.pull.is_some() && r.deleted.is_none()
            })
        },
    );
    let st = read_state(&home);
    assert_eq!(st.sessions[sid].pull.as_ref().unwrap().cursor, cursor_for(&fork));
    assert_eq!(store_bytes(server, sid).unwrap(), fork);
    wait_journal_drained(&home, Duration::from_secs(30));
    d2.kill();
}

/// T-706: crash window — journal records consumed and committed, but the
/// record files were never deleted. Restart replay must be idempotent:
/// same state, files gone, no duplicate side effects.
pub(crate) async fn t706_journal_replay_idempotent(server: &Server) {
    let home = MirrorHome::new();
    let dead = "t706a";
    let reg = "t706b";
    let now = || state::now_rfc3339();

    // State as the first (crashed) consume left it: tombstone on `dead`,
    // `registered` on `reg` — and both intent files still on disk.
    let file = write_mirror_file(&home, dead, &lines_bytes(&session_lines(dead, 1, None)));
    let file_id = state::file_id_of(&std::fs::read(&file).unwrap());
    let mut st = State::empty_v3();
    st.sessions.insert(
        dead.to_string(),
        SessionRecord {
            deleted: Some(Deleted {
                at: now(),
                published_at_delete: false,
                file_id: Some(file_id),
            }),
            ..SessionRecord::default()
        },
    );
    st.sessions.insert(
        reg.to_string(),
        SessionRecord {
            registered: Some(Registered {
                kind: "b-owned".to_string(),
                at: now(),
            }),
            ..SessionRecord::default()
        },
    );
    state::commit(&home.sync_dir(), &st).unwrap();
    let before = read_state(&home);

    // Leftover records: `enqueuedAt` equals the committed `at` so a
    // genuinely idempotent replay lands a byte-identical record.
    write_intent(&home, &Intent::NoteRemoteDelete {
        sid: dead.to_string(),
        enqueued_at: before.sessions[dead].deleted.as_ref().unwrap().at.clone(),
    });
    write_intent(&home, &Intent::Register {
        sid: reg.to_string(),
        enqueued_at: before.sessions[reg].registered.as_ref().unwrap().at.clone(),
    });
    assert!(!journal_empty(&home));

    let mut daemon = spawn_daemon_up(&home, server, &[]);
    wait_for("leftover records consumed", Duration::from_secs(20), || {
        journal_empty(&home)
    });
    let after = read_state(&home);
    assert_eq!(
        after.sessions.get(dead),
        before.sessions.get(dead),
        "tombstone replay must be a no-op"
    );
    assert_eq!(
        after.sessions.get(reg),
        before.sessions.get(reg),
        "register replay must be a no-op"
    );
    // The tombstone's file-deletion backstop still fires on reconcile.
    write_reconcile_intent(&home);
    wait_for("tombstoned file re-deleted", Duration::from_secs(20), || {
        mirror_file(&home, dead).is_none()
    });
    assert!(!api::store_exists(&server.base, None, dead).await.unwrap());
    daemon.kill();
}

/// T-707: a torn tail never crosses into a sync response, on either side.
/// Arm ①: a half-written line appended to B's file is excluded from
/// `changes` until the `\n` lands. Arm ②: a torn tail on the MIRROR file
/// is startup-truncated to the cursor (it never attested durable bytes).
pub(crate) async fn t707_torn_tail(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t707";

    // ① B-side: stable bytes + a torn tail; the pull must mirror only the
    // complete prefix, and completing the line moves the boundary later.
    let stable = lines_bytes(&session_lines(sid, 2, None));
    write_store_file(server, sid, &stable);
    append_store_file(server, sid, b"{\"uuid\":\"t707-torn\",\"x\":");
    let mut daemon = spawn_daemon_up(&home, server, &[]);
    write_pull_intent(&home, sid);
    wait_mirror_bytes("mirror excludes torn tail", &home, sid, &stable, Duration::from_secs(20));
    // boundary moves only when the line terminates.
    append_store_file(server, sid, b"1}\n");
    let mut want = stable.clone();
    want.extend_from_slice(b"{\"uuid\":\"t707-torn\",\"x\":1}\n");
    write_pull_intent(&home, sid);
    wait_mirror_bytes("completed line lands on next pull", &home, sid, &want, Duration::from_secs(20));
    daemon.kill();
    wait_lock_free(&home, Duration::from_secs(10));

    // ② Mirror-side: same torn shape as T-704 — the startup partition
    // truncates it rather than attesting it.
    let mut st = State::empty_v3();
    st.sessions.insert(
        sid.to_string(),
        SessionRecord {
            pull: Some(Pull {
                cursor: cursor_for(&want),
                file: format!("projects/lab/{sid}.jsonl"),
            }),
            ..SessionRecord::default()
        },
    );
    state::commit(&home.sync_dir(), &st).unwrap();
    let mut torn = want.clone();
    torn.extend_from_slice(b"{\"orphan\":");
    write_mirror_file(&home, sid, &torn);
    let out = run_stub(
        &home,
        server,
        &["zonepaird", "--mirror", &home.mirror.display().to_string(), "--url", server.base.as_str(), "--once"],
        Duration::from_secs(30),
    );
    assert!(out.is_some_and(|o| o.status.success()));
    assert_eq!(mirror_bytes(&home, sid).unwrap(), want);
}

/// T-708: `kill -9` the writer mid-service — the next intent writer finds
/// `owner.lock` free and a fresh daemon rebuilds from disk and takes over.
pub(crate) async fn t708_writer_takeover(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t708";
    let bytes = lines_bytes(&session_lines(sid, 2, None));
    write_store_file(server, sid, &bytes);

    let mut d1 = spawn_daemon_up(&home, server, &[]);
    write_pull_intent(&home, sid);
    wait_mirror_bytes("writer 1 pulled", &home, sid, &bytes, Duration::from_secs(20));
    wait_state(
        "pull committed",
        &home,
        Duration::from_secs(10),
        |st| st.sessions.get(sid).is_some_and(|r| r.pull.is_some()),
    );
    d1.kill(); // SIGKILL mid-service — not a graceful stop
    wait_lock_free(&home, Duration::from_secs(15));

    // `sync --wait` is the next writer-in-intent: its `ensure_owner`
    // probes the free lock, spawns a fresh daemon, and returns only when
    // the journal + state say "settled" — deterministic, no sleep.
    let out = run_stub(&home, server, &["sync", "--wait"], Duration::from_secs(90));
    let out = out.unwrap_or_else(|| fail("sync --wait timed out after takeover"));
    assert!(
        out.status.success() && String::from_utf8_lossy(&out.stdout).contains("sync ok"),
        "sync --wait must drain after takeover: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!owner::lock_free(&home.sync_dir()), "a new writer must have taken over");
    assert_eq!(mirror_bytes(&home, sid).unwrap(), bytes);
    shutdown_writer(&home.sync_dir());
}

/// T-709: `sync --wait` returns on the structural drain — journal empty,
/// pullPending satisfied same-transaction, no task mid-flight. The pull
/// intent is the work; "sync ok" only prints once it's consumed.
pub(crate) async fn t709_sync_wait_drain(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t709";
    let bytes = lines_bytes(&session_lines(sid, 3, None));
    write_store_file(server, sid, &bytes);
    write_pull_intent(&home, sid);

    let out = run_stub(&home, server, &["sync", "--wait"], Duration::from_secs(90));
    let out = out.unwrap_or_else(|| fail("sync --wait did not return"));
    assert!(
        out.status.success() && String::from_utf8_lossy(&out.stdout).contains("sync ok"),
        "sync --wait failed: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(mirror_bytes(&home, sid).unwrap(), bytes);
    let st = read_state(&home);
    assert!(st.sessions[sid].pull_pending.is_none(), "pullPending must clear same-transaction");
    assert!(journal_empty(&home));
    shutdown_writer(&home.sync_dir());
}

/// T-710: the first-pull crash window, e2e — `registered` + a local file
/// + no `pull` field. B has data → whole reset-pull overwrites the local
/// bytes (they are not trusted); `registered` clears same transaction.
pub(crate) async fn t710_first_pull_overwrite(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t710";
    let b_bytes = lines_bytes(&session_lines(sid, 3, None));
    write_store_file(server, sid, &b_bytes);
    write_mirror_file(&home, sid, b"{\"uuid\":\"a-side-only-garbage\"}\n");

    let mut st = State::empty_v3();
    st.sessions.insert(
        sid.to_string(),
        SessionRecord {
            registered: Some(Registered {
                kind: "b-owned".to_string(),
                at: state::now_rfc3339(),
            }),
            ..SessionRecord::default()
        },
    );
    state::commit(&home.sync_dir(), &st).unwrap();

    let mut daemon = spawn_daemon_up(&home, server, &[]);
    wait_state(
        "first pull committed, registered cleared",
        &home,
        Duration::from_secs(30),
        |st| {
            st.sessions.get(sid).is_some_and(|r| r.pull.is_some() && r.registered.is_none())
        },
    );
    assert_eq!(mirror_bytes(&home, sid).unwrap(), b_bytes);
    // The overwrite must not push anything back: B's file is untouched.
    assert_eq!(store_bytes(server, sid).unwrap(), b_bytes);
    daemon.kill();
}

/// T-711: B deletes a session we mirror — `changes` 404 suspends the
/// session for a human: the mirror file stays, the pull record stays,
/// nothing is rebuilt or pushed back to B. The suspension evidence
/// surface is the daemon's stderr (boot reconcile prints the row-18
/// note); the in-memory overlay itself has no disk form.
pub(crate) async fn t711_remote_delete_suspends(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t711";
    let bytes = lines_bytes(&session_lines(sid, 2, None));
    write_store_file(server, sid, &bytes);

    let mut d1 = spawn_daemon_up(&home, server, &[]);
    write_pull_intent(&home, sid);
    wait_mirror_bytes("pulled", &home, sid, &bytes, Duration::from_secs(20));
    // Gate on the COMMIT, not the bytes: apply fsyncs the file first and
    // the cursor commits a beat later — a kill in between replants the
    // crash window and the pull record never lands.
    wait_state(
        "pull committed",
        &home,
        Duration::from_secs(10),
        |st| st.sessions.get(sid).is_some_and(|r| r.pull.is_some()),
    );
    d1.kill();

    assert!(api::store_delete(&server.base, None, sid).await.unwrap());

    // The next writer's boot reconcile classifies `pull ∧ B 404` → row
    // 18 suspend (BLostRecovery) — logged on stderr — and queues the
    // healing pull, which itself answers Missing → Pending.
    let mut d2 = spawn_daemon_up(&home, server, &[]);
    wait_for(
        "row-18 suspension logged by boot reconcile",
        Duration::from_secs(30),
        || home.log_text().contains(&format!("{sid}: suspended")),
    );
    // Suspended, not healed: local file intact, pull record intact, B
    // still empty, nothing re-pushed.
    assert_eq!(mirror_bytes(&home, sid).unwrap(), bytes);
    let st = read_state(&home);
    let rec = &st.sessions[sid];
    assert!(rec.pull.is_some() && rec.deleted.is_none(), "record must survive suspension");

    // A fresh pull intent may land `pullPending`, but a suspended session
    // never rebuilds: the 404 stays a `Pending` (intent conserved), the
    // file keeps the last good bytes, and B is not recreated.
    write_pull_intent(&home, sid);
    wait_for("pull intent consumed", Duration::from_secs(20), || {
        journal_empty(&home)
    });
    std::thread::sleep(Duration::from_millis(800)); // observation window
    assert_eq!(mirror_bytes(&home, sid).unwrap(), bytes);
    assert!(!api::store_exists(&server.base, None, sid).await.unwrap());
    d2.kill();
}

/// T-712: `sess delete` writes the tombstone while B's file still exists;
/// the tombstone suppresses every resurrection — a later pull intent
/// cannot bring the file back, and the lingering mirror file is
/// re-deleted by the reconcile backstop.
pub(crate) async fn t712_tombstone_suppresses(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t712";
    let bytes = lines_bytes(&session_lines(sid, 2, None));
    write_store_file(server, sid, &bytes);

    let mut daemon = spawn_daemon_up(&home, server, &[]);
    write_pull_intent(&home, sid);
    wait_mirror_bytes("pulled", &home, sid, &bytes, Duration::from_secs(20));
    wait_state(
        "pull committed",
        &home,
        Duration::from_secs(10),
        |st| st.sessions.get(sid).is_some_and(|r| r.pull.is_some()),
    );

    // E6: the command lands the note-remote-delete intent; B keeps its
    // copy (propagation is the deferred contract).
    let out = run_stub(&home, server, &["sess", "delete", sid], Duration::from_secs(15));
    assert!(out.is_some_and(|o| o.status.success()), "sess delete failed");
    wait_state(
        "tombstone committed",
        &home,
        Duration::from_secs(20),
        |st| st.sessions.get(sid).is_some_and(|r| r.deleted.is_some()),
    );
    assert!(
        api::store_exists(&server.base, None, sid).await.unwrap(),
        "sess delete must not delete on B"
    );

    // Backstop: the lingerer is re-deleted by the next pass; a pull
    // intent may land pullPending but can never resurrect the file.
    write_reconcile_intent(&home);
    wait_for("tombstoned file re-deleted", Duration::from_secs(20), || {
        mirror_file(&home, sid).is_none()
    });
    write_pull_intent(&home, sid);
    wait_for("pull intent consumed", Duration::from_secs(20), || {
        journal_empty(&home)
    });
    std::thread::sleep(Duration::from_millis(800)); // observation window
    assert!(
        mirror_file(&home, sid).is_none(),
        "tombstone must suppress resurrection"
    );
    let st = read_state(&home);
    assert!(st.sessions[sid].pull.is_none(), "no pull may land under a tombstone");
    assert!(api::store_exists(&server.base, None, sid).await.unwrap());
    daemon.kill();
}

/// T-713: an external writer rewrites B's file wholesale — the next
/// `changes` verdict is `reset` and the mirror converges to the new
/// prefix (fingerprint mismatch, §6.3 branch 4).
pub(crate) async fn t713_b_rewrite_resets(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t713";
    let old = lines_bytes(&session_lines(sid, 4, None));
    write_store_file(server, sid, &old);

    let mut daemon = spawn_daemon_up(&home, server, &[]);
    write_pull_intent(&home, sid);
    wait_mirror_bytes("old prefix pulled", &home, sid, &old, Duration::from_secs(20));
    wait_state(
        "pull committed",
        &home,
        Duration::from_secs(10),
        |st| st.sessions.get(sid).is_some_and(|r| r.pull.is_some()),
    );

    // B-side whole-file rewrite (shorter AND different — `append` could
    // never produce this; the verdict must be `reset`).
    let new = lines_bytes(&session_lines(&format!("{sid}-v2"), 2, None));
    write_store_file(server, sid, &new);
    write_pull_intent(&home, sid);
    wait_mirror_bytes("mirror converged to rewritten prefix", &home, sid, &new, Duration::from_secs(30));
    // The cursor commits a beat after the bytes land (the write-order
    // rule) — wait on the commit itself.
    wait_state(
        "cursor re-sealed on the new prefix",
        &home,
        Duration::from_secs(10),
        |st| {
            st.sessions.get(sid).and_then(|r| r.pull.as_ref()).is_some_and(|p| {
                cursor::parse(&p.cursor).is_ok_and(|c| {
                    c.offset == new.len() as u64 && c.prefix_hash == cursor::prefix_hash(&new)
                })
            })
        },
    );
    daemon.kill();
}

/// T-714: merge-during-changes pressure — B keeps merging while pulls
/// fly; every sync response stays self-consistent (the fd-snapshot
/// contract) and the mirror ends byte-equal to B after each settle.
pub(crate) async fn t714_merge_during_changes(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t714";
    let base_lines = session_lines(sid, 3, None);
    api::store_merge(&server.base, None, sid, &base_lines).await.unwrap();

    let mut daemon = spawn_daemon_up(&home, server, &[]);
    for round in 0..5usize {
        // New-uuid lines land via merge while the previous pull may still
        // be in flight — the in-place/atomic rewrite is B's side of the
        // race.
        let more: Vec<Value> = (0..3)
            .map(|i| {
                json!({
                    "type": "user",
                    "uuid": format!("{sid}-r{round}-m{i}"),
                    "sessionId": sid,
                    "message": {"role": "user", "content": format!("round {round} merge {i}")},
                })
            })
            .collect();
        api::store_merge(&server.base, None, sid, &more).await.unwrap();
        write_pull_intent(&home, sid);
        let out = run_stub(&home, server, &["sync", "--wait"], Duration::from_secs(60));
        let out = out.unwrap_or_else(|| fail(&format!("sync --wait timed out in round {round}")));
        assert!(
            out.status.success(),
            "sync --wait failed in round {round}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let want = store_bytes(server, sid).unwrap();
        let got = mirror_bytes(&home, sid).unwrap_or_else(|| fail("mirror file missing"));
        assert_bytes_eq(&format!("round {round} mirror"), &got, &want);
        for (i, line) in got.split(|&b| b == b'\n').filter(|l| !l.is_empty()).enumerate() {
            serde_json::from_slice::<Value>(line)
                .unwrap_or_else(|e| fail(&format!("round {round} line {i} not JSON: {e}")));
        }
    }
    daemon.kill();
}

/// T-715: WS establishment order — a fork resume must not open the stdio
/// WS before `Published` (the init frame arrives only after the commit);
/// a non-fork spawn waits on nothing.
pub(crate) async fn t715_ws_ordering(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t715";

    // Hold the lock ourselves: the waiter's ensure-task is durable, no
    // writer exists, and `ensure_published` must sit in its revive loop —
    // the fork arm's WS stays shut the whole time.
    let sync_dir = home.sync_dir();
    state::ensure_sync_dir(&home.mirror).unwrap();
    write_mirror_file(&home, sid, &lines_bytes(&session_lines(sid, 1, Some("t715-p"))));
    let held_lock = owner::acquire(&sync_dir).unwrap().expect("fresh mirror — lock must be free");
    let logs_at_start = stderr_log_count(server);

    let mut stub = spawn_stub(&home, server, &["--resume", sid]);
    wait_for(
        "ensure-task intent enqueued",
        Duration::from_secs(10),
        || {
            journal_intents(&home).iter().any(|i| {
                matches!(i, Intent::EnsureTask { sid: s, .. } if s == sid)
            })
        },
    );
    // The whole lock-held window: no init frame, no new claude child.
    stub.assert_silent(Duration::from_millis(2500));
    assert_eq!(stderr_log_count(server), logs_at_start, "WS must not open before Published");
    assert!(
        read_state(&home).sessions.get(sid).is_none_or(|r| r.published_at.is_none()),
        "publish must not commit while no writer exists"
    );

    // The waiter's revive obligation: lock freed → it spawns the writer
    // itself; publish lands; only then does the WS open.
    drop(held_lock);
    wait_state(
        "published after revive",
        &home,
        Duration::from_secs(45),
        |st| st.sessions.get(sid).is_some_and(|r| r.published_at.is_some()),
    );
    let init = stub.wait_frame("system", Duration::from_secs(15));
    assert_eq!(init.get("subtype").and_then(Value::as_str), Some("init"));
    assert!(stderr_log_count(server) > logs_at_start);
    stub.close_stdin();
    stub.wait_exit(Duration::from_secs(10));
    drop(stub);

    // Non-fork arm: `--session-id` owes a register intent, not a wait —
    // init arrives promptly while the daemon is already running.
    let mut stub2 = spawn_stub(&home, server, &["--session-id", "t715-new"]);
    let init2 = stub2.wait_frame("system", Duration::from_secs(10));
    assert_eq!(init2.get("subtype").and_then(Value::as_str), Some("init"));
    assert_eq!(init2.get("session_id").and_then(Value::as_str), Some("t715-new"));
    stub2.close_stdin();
    stub2.wait_exit(Duration::from_secs(10));
    drop(stub2);
    shutdown_writer(&home.sync_dir());
}

/// T-716: `mirror_pulls_via_changes` — B-side growth flows through
/// `changes` into byte-equal mirror files; the pull cursor lands exactly
/// on the stable boundary, and a settled mirror answers `unchanged`.
/// (Replaces the retired `mirror_sync_pushes_and_pulls`.)
pub(crate) async fn t716_mirror_pulls_via_changes(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t716";
    let mut b = lines_bytes(&session_lines(sid, 2, None));
    write_store_file(server, sid, &b);

    let mut daemon = spawn_daemon_up(&home, server, &[]);
    write_pull_intent(&home, sid);
    wait_mirror_bytes("first pull", &home, sid, &b, Duration::from_secs(20));
    wait_state(
        "pull committed",
        &home,
        Duration::from_secs(10),
        |st| st.sessions.get(sid).is_some_and(|r| r.pull.is_some()),
    );

    // B grows → pull intent → incremental append lands byte-exact.
    let more = lines_bytes(&session_lines(&format!("{sid}-more"), 2, None));
    append_store_file(server, sid, &more);
    b.extend_from_slice(&more);
    write_pull_intent(&home, sid);
    wait_mirror_bytes("incremental pull byte-exact", &home, sid, &b, Duration::from_secs(20));

    // The boundary itself is the assertion: `pull.cursor` commits a beat
    // after the bytes (write-order rule) — wait on the committed cursor.
    wait_state(
        "pull.cursor at the boundary",
        &home,
        Duration::from_secs(10),
        |st| {
            st.sessions.get(sid).and_then(|r| r.pull.as_ref()).is_some_and(|p| {
                cursor::parse(&p.cursor).is_ok_and(|c| c.offset == b.len() as u64)
            })
        },
    );
    let st = read_state(&home);
    match api::sync::changes(
        &server.base,
        None,
        sid,
        &st.sessions[sid].pull.as_ref().unwrap().cursor,
    )
    .await
    .unwrap()
    {
        api::sync::ChangesReply::Unchanged { .. } => {}
        other => fail(&format!("settled mirror must answer unchanged, got {other:?}")),
    }
    daemon.kill();
    shutdown_writer(&home.sync_dir());
}
