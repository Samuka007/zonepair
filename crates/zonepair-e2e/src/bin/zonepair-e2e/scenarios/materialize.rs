use crate::fixtures::*;
use crate::mirror::files::*;
use crate::mirror::journal::*;
use crate::mirror::waits::*;
use crate::mirror::{run_stub, spawn_daemon_up, spawn_stub, MirrorHome};
use serde_json::{json, Value};
use std::time::Duration;
use zonepair_client::{
    api,
    sync::{
        materialize,
        state::{self, Materialized, Pull, SessionRecord, State},
    },
};

// ---- T-810..T-811 ---------------------------------------------------------------

/// T-810: `prune <sid> --drop` via the real `stub-client` subprocess →
/// the file is gone and `materialized="unloaded"` is durable; B-side
/// traffic (an E1 pull intent) hits the worker's `materialized` precheck,
/// diverts into the hydrate reset path, and the file comes back
/// byte-equal to B with `materialized` back at `full` and the cursor
/// advanced past the eviction point.
///
/// Determinism note: the daemon must see the evict intent while holding
/// zero queued `Work::Pull` for the sid — boot recovery and E4 both queue
/// pulls that legitimately serialize behind the evict and hydrate it right
/// back. The state is therefore planted directly (pull record + mirror
/// file, no pull intent ever consumed): `last_active` stays empty, E4
/// never queues the sid, and boot recovery's one pull is awaited on real
/// bytes before `prune` runs.
pub(crate) async fn t810_prune_drop_revives(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t810";
    let prefix = lines_bytes(&session_lines(sid, 3, None));
    let extra = lines_bytes(&session_lines(&format!("{sid}-x"), 1, None));
    let mut b0 = prefix.clone();
    b0.extend_from_slice(&extra);
    write_store_file(server, sid, &b0);

    // Plant `pull ∧ full` mid-catch-up: the daemon's boot recovery pull
    // has real bytes to land — `wait_mirror_bytes` is the proof it ran
    // and finished before the scenario begins.
    write_mirror_file(&home, sid, &prefix);
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
    state::commit(&home.sync_dir(), &st).unwrap();

    let mut daemon = spawn_daemon_up(&home, server, &[]);
    wait_mirror_bytes("boot recovery pull landed", &home, sid, &b0, Duration::from_secs(20));
    wait_state(
        "recovery cursor committed",
        &home,
        Duration::from_secs(10),
        |st| {
            st.sessions.get(sid).and_then(|r| r.pull.as_ref()).is_some_and(|p| {
                p.cursor == cursor_for(&b0)
            })
        },
    );

    // The command surface under test: `stub-client prune t810 --drop`
    // as a real subprocess. `ensure_owner` sees the live daemon
    // (AlreadyOwned), the intent lands durable and the writer consumes it.
    let out = run_stub(&home, server, &["prune", sid, "--drop"], Duration::from_secs(15));
    let out = out.unwrap_or_else(|| fail("prune --drop timed out"));
    assert!(
        out.status.success(),
        "prune --drop failed: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // Evict executes: file deleted + dir fsync, then `unloaded` commits.
    wait_state(
        "materialized=unloaded committed",
        &home,
        Duration::from_secs(20),
        |st| {
            st.sessions.get(sid).is_some_and(|r| {
                r.materialized == Some(Materialized::Unloaded) && r.pull.is_some()
            })
        },
    );
    assert!(
        mirror_file(&home, sid).is_none(),
        "evict --drop must delete the mirror file"
    );
    assert!(
        api::store_exists(&server.base, None, sid).await.unwrap(),
        "evict is A-local — B keeps its copy"
    );
    // No resurrection on its own: nothing queues a pull for the sid, and
    // E4 skips non-full records (T-807's rule) — the session sits
    // unloaded until traffic.
    std::thread::sleep(Duration::from_secs(2));
    assert!(
        mirror_file(&home, sid).is_none(),
        "an unloaded session must not come back without traffic"
    );

    // B produces new bytes → an E1 pull intent → the worker's
    // materialized precheck diverts into hydrate: zero cursor out, reset
    // in, full bytes back.
    let more = lines_bytes(&session_lines(&format!("{sid}-b2"), 2, None));
    append_store_file(server, sid, &more);
    let mut b1 = b0.clone();
    b1.extend_from_slice(&more);
    write_pull_intent(&home, sid);
    wait_mirror_bytes("hydrated back to B's bytes", &home, sid, &b1, Duration::from_secs(30));
    wait_state(
        "materialized=full + cursor advanced past the evict point",
        &home,
        Duration::from_secs(10),
        |st| {
            st.sessions.get(sid).is_some_and(|r| {
                r.materialization().is_full()
                    && r.pull_pending.is_none()
                    && r.pull.as_ref().is_some_and(|p| p.cursor == cursor_for(&b1))
            })
        },
    );

    // `sync --wait` drains on the structural condition — journal empty,
    // no pullPending, no mid-flight task.
    let out = run_stub(&home, server, &["sync", "--wait"], Duration::from_secs(90));
    let out = out.unwrap_or_else(|| fail("sync --wait timed out after revive"));
    assert!(
        out.status.success() && String::from_utf8_lossy(&out.stdout).contains("sync ok"),
        "sync --wait must drain after the revive: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    daemon.kill();
}

/// T-811: `prune <sid> --stub` replaces the mirror bytes with the
/// sentinel shell — one JSON line, `type=="zonepair-stub"` — so the
/// adapter's `exists` probe keeps answering true. A `--resume` then hits
/// the decision table's plain-resume row (the record carries `pull`,
/// the shell carries no `forkedFrom`): zero pre-WS sync work, no intents,
/// no wait — the WS passes straight through. The turn's E1/E3 intents
/// afterwards hydrate the session back to `full` (the §3.5 walkthrough).
///
/// Same determinism as T-810: planted state keeps the sid out of every
/// queued-work path before the evict lands.
pub(crate) async fn t811_prune_stub_resume(server: &Server) {
    let home = MirrorHome::new();
    let sid = "t811";
    let prefix = lines_bytes(&session_lines(sid, 3, None));
    let extra = lines_bytes(&session_lines(&format!("{sid}-x"), 1, None));
    let mut b0 = prefix.clone();
    b0.extend_from_slice(&extra);
    write_store_file(server, sid, &b0);

    write_mirror_file(&home, sid, &prefix);
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
    state::commit(&home.sync_dir(), &st).unwrap();

    let mut daemon = spawn_daemon_up(&home, server, &[]);
    wait_mirror_bytes("boot recovery pull landed", &home, sid, &b0, Duration::from_secs(20));
    wait_state(
        "recovery cursor committed",
        &home,
        Duration::from_secs(10),
        |st| {
            st.sessions.get(sid).and_then(|r| r.pull.as_ref()).is_some_and(|p| {
                p.cursor == cursor_for(&b0)
            })
        },
    );

    // `stub-client prune t811 --stub` — the real subprocess lands the
    // evict intent; the running writer consumes it.
    let out = run_stub(&home, server, &["prune", sid, "--stub"], Duration::from_secs(15));
    let out = out.unwrap_or_else(|| fail("prune --stub timed out"));
    assert!(
        out.status.success(),
        "prune --stub failed: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    wait_state(
        "materialized=stub committed",
        &home,
        Duration::from_secs(20),
        |st| {
            st.sessions.get(sid).is_some_and(|r| {
                r.materialized == Some(Materialized::Stub) && r.pull.is_some()
            })
        },
    );

    // The shell file: present (adapter `exists` → true), exactly one
    // line, valid JSON, `type=="zonepair-stub"`, carrying the session id
    // and the pre-evict cursor as provenance. Zero transcript lines is
    // what `is_stub_shell` attests — the normative shell shape.
    let shell = mirror_bytes(&home, sid).unwrap_or_else(|| fail("stub shell file missing"));
    let text = String::from_utf8(shell.clone()).unwrap_or_else(|_| fail("shell not UTF-8"));
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 1, "the shell must be a single line: {text:?}");
    let v: Value = serde_json::from_str(lines[0])
        .unwrap_or_else(|e| fail(&format!("shell line not valid JSON: {e}")));
    assert_eq!(
        v.get("type").and_then(Value::as_str),
        Some(materialize::STUB_TYPE)
    );
    assert_eq!(v.get("sessionId").and_then(Value::as_str), Some(sid));
    assert_eq!(
        v.get("originalBytes").and_then(Value::as_u64),
        Some(b0.len() as u64)
    );
    assert!(materialize::is_stub_shell(&shell), "shell probe must hold");

    // `--resume` against a shell: the argv decision lands on the plain
    // resume row — pull present + no forkedFrom ⇒ ResumeNoop. The init
    // frame is the WS-open observable; zero sync work means no intents
    // landed and no task/publish ever appeared on the record.
    let mut stub = spawn_stub(&home, server, &["--resume", sid]);
    let init = stub.wait_frame("system", Duration::from_secs(15));
    assert_eq!(init.get("subtype").and_then(Value::as_str), Some("init"));
    assert!(
        journal_empty(&home),
        "a stub resume owes zero pre-WS intents"
    );
    let st = read_state(&home);
    let rec = st.sessions.get(sid).unwrap_or_else(|| fail("record vanished"));
    assert!(
        rec.task.is_none() && rec.published_at.is_none(),
        "plain resume must not create a task or publish"
    );

    // The resume turn itself passes through: user frame → echo → result.
    stub.send(&json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":"resume me"}]}}).to_string());
    let result = stub.wait_frame("result", Duration::from_secs(15));
    assert_eq!(result.get("subtype").and_then(Value::as_str), Some("success"));
    stub.close_stdin();
    stub.wait_exit(Duration::from_secs(10));
    drop(stub);

    // The turn's E1/E3 pull intents re-materialize the session: the
    // worker's stub-arm hydrate resets the file back to B's bytes and
    // commits `full`. (The mock's init sid means the E3 pull is the one
    // guaranteed to name t811.)
    wait_mirror_bytes("resume traffic hydrated the stub", &home, sid, &b0, Duration::from_secs(30));
    wait_state(
        "materialized=full after resume traffic",
        &home,
        Duration::from_secs(10),
        |st| st.sessions.get(sid).is_some_and(|r| r.materialization().is_full()),
    );
    daemon.kill();
}
