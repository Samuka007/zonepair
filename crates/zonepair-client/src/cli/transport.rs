//! transport (default mode): acts as `pathToClaudeCodeExecutable` — dial
//! `/v1/stdio`, send the hello header, relay stdin/stdout one line per
//! frame — plus the argv adaptation layer (RFC-0003-zonepaird §8.3) that translates
//! the SDK's spawn argv, mirror file evidence and a read-only `state.json`
//! peek into journal intents. One-way translation: the queue core never
//! sees argv.

use super::zonepaird::enqueue;
use super::{mirror_enabled, resolve_mirror_dir, resolve_token, resolve_url};
use crate::sync::persist::journal::Intent;
use crate::sync::persist::state::{self, Phase, SessionRecord, State};
use crate::sync::wait::{self, PublishWait, WaitPolicy, Waits};
use crate::sync::{state_machine as classify};
use crate::sync::engine::{reconcile, shared::Shared};
use crate::{proto::envf, transport};
use anyhow::{bail, Result};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

// ---- stdio header -----------------------------------------------------------

fn stdio_header(argv: &[String]) -> Value {
    json!({
        "argv": argv,
        "env": envf::collect(),
        "cwd": std::env::current_dir().map(|p| p.display().to_string()).unwrap_or_default(),
    })
}

// ---- argv decision (§8.3) ----------------------------------------------------
// The four argv shapes the decision table assigns work to. Parsing and
// evidence stay separate: the table reads `(argv shape, file probe, state
// record)` and answers one output action.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionArg<'a> {
    /// `--session-id X`
    New(&'a str),
    /// `--resume X`
    Resume(&'a str),
}

struct ArgvShape<'a> {
    session: Option<SessionArg<'a>>,
    no_persist: bool,
}

fn parse_argv<'a>(argv: &'a [String]) -> ArgvShape<'a> {
    let mut session = None;
    let mut no_persist = false;
    let mut i = 0usize;
    while i < argv.len() {
        let arg = argv[i].as_str();
        if arg == "--no-session-persistence" {
            no_persist = true;
        } else if arg == "--session-id" {
            if let Some(v) = argv.get(i + 1) {
                session = Some(SessionArg::New(v));
                i += 1;
            }
        } else if let Some(v) = arg.strip_prefix("--session-id=") {
            session = Some(SessionArg::New(v));
        } else if arg == "--resume" {
            if let Some(v) = argv.get(i + 1) {
                session = Some(SessionArg::Resume(v));
                i += 1;
            }
        } else if let Some(v) = arg.strip_prefix("--resume=") {
            session = Some(SessionArg::Resume(v));
        }
        i += 1;
    }
    ArgvShape {
        session,
        no_persist,
    }
}

/// The pre-WS action — the §8.3 decision table's output column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PreRelay<'a> {
    /// `--session-id X` — journal `register`; E3 pull lands post-relay.
    Register(&'a str),
    /// `--session-id … --no-session-persistence`, or no session flag at
    /// all — literally nothing is owed.
    Nothing,
    /// `--resume X` + the §6.2 continuation-fork predicate — journal
    /// `ensure-task{phase:validated}` and block on `ensure_published`.
    Publish(&'a str),
    /// Every other `--resume` — pull/publishedAt present, tombstone hit,
    /// or zero complete lines with no record: plain resume, zero sync.
    /// The sid is never read after the decision, so no payload.
    ResumeNoop,
}

fn decide<'a>(
    shape: &ArgvShape<'a>,
    file: &classify::FileProbe,
    record: Option<&SessionRecord>,
) -> PreRelay<'a> {
    if shape.no_persist {
        return PreRelay::Nothing;
    }
    match shape.session {
        Some(SessionArg::New(sid)) => PreRelay::Register(sid),
        Some(SessionArg::Resume(sid)) => {
            if file.exists && classify::continuation_fork(record, file) {
                PreRelay::Publish(sid)
            } else {
                PreRelay::ResumeNoop
            }
        }
        None => PreRelay::Nothing,
    }
}

// ---- read-only state access --------------------------------------------------
// The adapter's privilege line is "never writes state.json" (§8.3), so
// `state::load`'s quarantine-on-corruption is off limits here — a
// schema-violating file decodes to whatever parses and never renames
// anything (the daemon's own load does the quarantining).

/// Read `state.json` strictly read-only. Corruption yields an empty table
/// — fail-open for *decisions*; the fail-closed latch is detected
/// separately via `state::corrupt_files`.
pub(crate) fn read_state_ro(sync_dir: &Path) -> State {
    let Ok(raw) = std::fs::read(state::state_path(sync_dir)) else {
        return State::empty_v3();
    };
    let Ok(value) = serde_json::from_slice::<Value>(&raw) else {
        return State::empty_v3();
    };
    let version_ok = value
        .get("version")
        .and_then(Value::as_u64)
        .is_some_and(|v| v == 2 || v == 3);
    if !version_ok {
        return State::empty_v3();
    }
    let mut state = State::empty_v3();
    if let Some(table) = value.get("sessions").and_then(Value::as_object) {
        for (sid, rv) in table {
            if let Ok(rec) = serde_json::from_value::<SessionRecord>(rv.clone()) {
                state.sessions.insert(sid.clone(), rec);
            }
        }
    }
    state
}

fn read_record_ro(sync_dir: &Path, sid: &str) -> Option<SessionRecord> {
    read_state_ro(sync_dir).sessions.get(sid).cloned()
}

// ---- intents -----------------------------------------------------------------

fn pull_latest(sid: &str) -> Intent {
    Intent::Pull {
        sid: sid.to_string(),
        target: "latest".to_string(),
        enqueued_at: state::now_rfc3339(),
    }
}

/// Cross-process wait wiring (§7): no in-process caches — `revive` is the
/// only live input, backstopping the writer the way §7.1 prescribes.
fn cross_process_waits(sync_dir: PathBuf, mirror: PathBuf) -> Waits {
    Waits {
        revive: Some(Arc::new(move || {
            let _ = super::zonepaird::ensure_owner(&sync_dir, &mirror);
        })),
        ..Waits::default()
    }
}

/// `Shared` holds an in-memory mirror of the authoritative table (the
/// daemon writes through it directly). A cross-process caller gets the
/// disk file mirrored in by this task — reloading every `poll`, the same
/// ≤50 ms authority the wait predicates already poll. A `state.corrupt-*`
/// residue latches `fail_closed` here too (no rename — detection only).
fn spawn_state_reloader(
    shared: Arc<Shared>,
    poll: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    let sync_dir = shared.sync_dir.clone();
    tokio::spawn(async move {
        loop {
            // Strict read-only reload: the same schema checks the writer
            // runs, minus quarantine. A parse violation latches fail_closed
            // here too (detection only — the file is never renamed), and a
            // `state.corrupt-*` residue does the same via `latched`.
            match state::load_readonly(&sync_dir) {
                Ok(out) => {
                    if out.corruption.is_some() {
                        shared.fail_closed.store(true, Ordering::Relaxed);
                    }
                    let mut guard = shared.lock();
                    *guard = out.state;
                }
                Err(_) => {
                    // Unreadable this cycle: keep the last known view —
                    // disk hiccups must not flip the waiter's decision.
                }
            }
            tokio::time::sleep(poll).await;
        }
    })
}

/// The fork-resume hard wait (§8.1): enqueue `ensure-task{validated}`,
/// then block until `ensure_published` answers a terminal branch — the
/// WS must not open before `Published` (T-715). Fail-closed corruption
/// surfaces as a loud degrade instead of burning the budget (§8-2).
async fn wait_for_publish(mirror: &Path, sid: &str, file: String) -> Result<()> {
    enqueue(
        mirror,
        Intent::EnsureTask {
            sid: sid.to_string(),
            file,
            phase: Phase::Validated,
            enqueued_at: state::now_rfc3339(),
        },
    )?;

    // Read-only open: this process must never write state.json — a schema
    // violation latches fail_closed and fails the wait loudly, but the
    // file itself is never renamed (R3-I4).
    let (shared, corruption) = Shared::open_readonly(mirror)?;
    if let Some(c) = corruption {
        bail!("state corrupt — publish wait cannot proceed: {c}");
    }
    let policy = WaitPolicy::default();
    let reloader = spawn_state_reloader(Arc::clone(&shared), policy.poll);
    let waits = cross_process_waits(shared.sync_dir.clone(), shared.mirror.clone());
    let sh = Arc::clone(&shared);
    let poll = policy.poll;
    let verdict = tokio::select! {
        v = wait::ensure_published(&shared, sid, &waits, &policy) => v,
        _ = async move {
            loop {
                if sh.fail_closed.load(Ordering::Relaxed) {
                    return;
                }
                tokio::time::sleep(poll).await;
            }
        } => PublishWait::Degraded("state corrupt — suspended".into()),
    };
    reloader.abort();
    match verdict {
        PublishWait::Published | PublishWait::Ready => Ok(()),
        PublishWait::Superseded => {
            bail!("session {sid} superseded before resume — the fork product vanished")
        }
        PublishWait::Degraded(reason) => {
            bail!("publish wait for {sid} degraded: {reason}")
        }
    }
}

// ---- the transport ------------------------------------------------------------

pub(crate) async fn transport(argv: &[String]) -> Result<()> {
    let base = transport::parse_base(&resolve_url(None)?)?;
    let token = resolve_token(None);
    let mirror = PathBuf::from(resolve_mirror_dir(None));
    let mirror_on = mirror_enabled();

    // ---- argv adaptation layer, the pre-WS window (§8.3) ----
    let shape = parse_argv(argv);
    if mirror_on {
        // Evidence: the `<sid>.jsonl` first-hit under `projects/**` plus a
        // read-only state record — only gathered when a resume could need
        // it; `--session-id` and flagless argv never touch the disk here.
        let (probe, path, record) = match shape.session {
            Some(SessionArg::Resume(sid)) => {
                let (probe, path) = reconcile::probe_session_file(&mirror, None, sid);
                (probe, path, read_record_ro(&state::sync_dir(&mirror), sid))
            }
            _ => (classify::FileProbe::absent(), None, None),
        };
        match decide(&shape, &probe, record.as_ref()) {
            PreRelay::Register(sid) => {
                // Best-effort: the relay is the product; a journal hiccup
                // must not kill it (same rule the old push lived by).
                if let Err(e) = enqueue(
                    &mirror,
                    Intent::Register {
                        sid: sid.to_string(),
                        enqueued_at: state::now_rfc3339(),
                    },
                ) {
                    eprintln!("stub-client: register intent failed: {e:#}");
                }
            }
            PreRelay::Publish(sid) => {
                let file = path
                    .and_then(|p| {
                        p.strip_prefix(&mirror)
                            .ok()
                            .map(|r| r.to_string_lossy().into_owned())
                    })
                    .unwrap_or_else(|| format!("projects/lab/{sid}.jsonl"));
                // The ONE hard wait in the system — loud failure, never
                // a resume started against a session B cannot see.
                wait_for_publish(&mirror, sid, file).await?;
            }
            PreRelay::ResumeNoop | PreRelay::Nothing => {}
        }
    }

    let stream = transport::open_stdio(&base, &stdio_header(argv), token.as_deref()).await?;

    // Feed claude's stdin: one line per WS frame; on stdin EOF ask the server
    // to close the child's stdin so claude flushes and exits.
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    use tokio_tungstenite::tungstenite::Message;

    let mut tx = stream.tx;
    let mut rx = stream.rx;
    let stdin_task = tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut lines = tokio::io::BufReader::new(stdin).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if tx.send(Message::Text(line.into())).await.is_err() {
                        break;
                    }
                }
                _ => {
                    // ask the server to close the child's stdin: claude flushes on EOF
                    let _ = tx
                        .send(Message::Text(
                            json!({"proto": "stdin-eof"}).to_string().into(),
                        ))
                        .await;
                    break;
                }
            }
        }
    });

    // Child stdout: one line per frame back to the SDK. E1 rides here:
    // B→A frames are observed for `session_id` and turn ends — a
    // `pull(latest)` intent per completed turn is a local journal append,
    // never a sync network round trip (§5 E1; T-715's reverse assertion).
    let mut observed_sid: Option<String> = None;
    let mut stdout = tokio::io::stdout();
    while let Some(msg) = rx.next().await {
        let line = match msg {
            Ok(Message::Text(line)) => line,
            Ok(_) => continue,
            Err(e) => {
                eprintln!("stub-client: stream error: {e}");
                break;
            }
        };
        if mirror_on {
            // Parse only frames that could carry evidence: anything while
            // the sid is unknown, or a turn-end marker. JSON errors are
            // ignored — this is observation, not interpretation.
            let interesting = observed_sid.is_none() || line.contains("\"result\"");
            if interesting {
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    if observed_sid.is_none() {
                        if let Some(s) = v.get("session_id").and_then(Value::as_str) {
                            observed_sid = Some(s.to_string());
                        }
                    }
                    if v.get("type").and_then(Value::as_str) == Some("result") {
                        if let Some(sid) = observed_sid.clone() {
                            if let Err(e) = enqueue(&mirror, pull_latest(&sid)) {
                                eprintln!("stub-client: E1 pull intent failed: {e:#}");
                            }
                        }
                    }
                }
            }
        }
        stdout.write_all(line.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    stdin_task.abort();

    // E3 — post-relay pull for the session this relay served (the
    // frame-observed id preferred: a fork's child sid is what B grew, and
    // it is also the argv `--resume` target). Same-sid merges idempotent.
    if mirror_on && !shape.no_persist {
        let arg_sid = match shape.session {
            Some(SessionArg::New(s)) | Some(SessionArg::Resume(s)) => Some(s.to_string()),
            None => None,
        };
        for sid in observed_sid.clone().into_iter().chain(arg_sid) {
            if let Err(e) = enqueue(&mirror, pull_latest(&sid)) {
                eprintln!("stub-client: E3 pull intent failed: {e:#}");
            }
        }
    }
    // Hard exit: the relay's job is done and machine A's stdin may still be
    // open. Tokio's Stdin worker is a blocking-pool thread parked in read(),
    // and a normal runtime shutdown would join it forever.
    std::process::exit(0);
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::owner;
    use crate::sync::persist::journal;
    use crate::sync::persist::state::{Deleted, Pull, Task};
    use std::time::Duration;

    /// Tests that write intents hold the owner lock so `ensure_owner`
    /// answers `AlreadyOwned` — the "writer exists" branch is the honest
    /// fixture; it also means no detached daemon spawns from a test.
    fn hold_owner(mirror: &Path) -> owner::Owner {
        let sync = state::ensure_sync_dir(mirror).unwrap();
        owner::acquire(&sync)
            .unwrap()
            .expect("test must own the lock")
    }

    fn tmp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "zonepair-transport-test-{}-{name}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    fn write_file(mirror: &Path, rel: &str, bytes: &[u8]) {
        let p = mirror.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, bytes).unwrap();
    }

    fn journal_intents(mirror: &Path) -> Vec<Intent> {
        let dir = state::sync_dir(mirror).join("journal");
        journal::read_pending(&dir)
            .map(|(entries, _)| {
                entries
                    .into_iter()
                    .filter_map(|e| e.record.ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn fork_probe(mirror: &Path, sid: &str) -> classify::FileProbe {
        reconcile::probe_session_file(mirror, None, sid).0
    }

    fn shape_of(args: &[&str]) -> ArgvShape<'static> {
        // Test convenience: argv vectors outlive the call.
        let leaked: &'static [String] = Box::leak(argv(args).into_boxed_slice());
        parse_argv(leaked)
    }

    fn rec_with_pull(sid: &str) -> SessionRecord {
        SessionRecord {
            pull: Some(Pull {
                cursor: "zpc1.9.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                file: format!("projects/lab/{sid}.jsonl"),
            }),
            ..SessionRecord::default()
        }
    }

    // T-620: `--session-id X` → `register` lands in the journal; post-relay
    // E3 is `pull(latest)` on the same sid.
    #[test]
    fn t620_session_id_registers_and_pulls() {
        let mirror = tmp("t620");
        let shape = shape_of(&["--session-id", "s1", "--verbose"]);
        assert_eq!(
            decide(&shape, &classify::FileProbe::absent(), None),
            PreRelay::Register("s1")
        );
        let _owner = hold_owner(&mirror);
        enqueue(
            &mirror,
            Intent::Register {
                sid: "s1".into(),
                enqueued_at: state::now_rfc3339(),
            },
        )
        .unwrap();
        enqueue(&mirror, pull_latest("s1")).unwrap();
        let intents = journal_intents(&mirror);
        assert_eq!(intents.len(), 2);
        assert!(matches!(intents[0], Intent::Register { ref sid, .. } if sid == "s1"));
        assert!(
            matches!(&intents[1], Intent::Pull { sid, target, .. } if sid == "s1" && target == "latest")
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-621: `--session-id X --no-session-persistence` → zero intents, and
    // no `.sync` tree materializes for a zero-action shape.
    #[test]
    fn t621_no_persistence_is_zero_action() {
        let mirror = tmp("t621");
        let shape = shape_of(&["--session-id", "s1", "--no-session-persistence"]);
        assert_eq!(
            decide(&shape, &classify::FileProbe::absent(), None),
            PreRelay::Nothing
        );
        assert!(journal_intents(&mirror).is_empty());
        assert!(!state::sync_dir(&mirror).exists());
    }

    // T-622: fork-resume — file carries `forkedFrom`, no pull/publishedAt,
    // no tombstone ⇒ `ensure-task{validated}` intent + `ensure_published`
    // returns once the published fact lands.
    #[tokio::test]
    async fn t622_fork_resume_enqueues_task_and_waits() {
        let mirror = tmp("t622");
        let sid = "s2";
        write_file(
            &mirror,
            "projects/lab/s2.jsonl",
            b"{\"forkedFrom\":{\"sessionId\":\"parent\"}}\n",
        );

        // Evidence gathering exactly as the pre-WS window does it.
        let (probe, path) = reconcile::probe_session_file(&mirror, None, sid);
        assert!(probe.forked_from);
        let rel = path
            .and_then(|p| {
                p.strip_prefix(&mirror)
                    .ok()
                    .map(|r| r.to_string_lossy().into_owned())
            })
            .unwrap();
        assert_eq!(rel, "projects/lab/s2.jsonl");
        let record = read_record_ro(&state::sync_dir(&mirror), sid);
        assert!(record.is_none());
        let shape = shape_of(&["--resume", "s2"]);
        assert_eq!(decide(&shape, &probe, record.as_ref()), PreRelay::Publish("s2"));

        // Hold the owner lock: `enqueue` sees an owned writer and never
        // spawns — the published transaction below stands in for it.
        let _owner = hold_owner(&mirror);

        // The writer's half (zonepaird): consume the intent, then commit
        // the published transaction — an in-process commit stands in for
        // the daemon's worker commit.
        let (writer, _) = Shared::open(&mirror).unwrap();
        let publisher = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(120)).await;
            writer
                .commit_state(|st| {
                    let rec = st.sessions.entry(sid.to_string()).or_default();
                    rec.task = Some(Task {
                        file: "projects/lab/s2.jsonl".into(),
                        phase: Phase::Published,
                        first_seen: "t".into(),
                        enqueued_at: "t".into(),
                        last_error: None,
                    });
                    rec.published_at = Some(state::now_rfc3339());
                    rec.begin_pull(Pull {
                        cursor: "zpc1.40.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                        file: "projects/lab/s2.jsonl".into(),
                    });
                })
                .unwrap();
        });

        wait_for_publish(&mirror, sid, rel).await.unwrap();
        publisher.await.unwrap();

        let intents = journal_intents(&mirror);
        assert!(intents.iter().any(|i| matches!(
            i,
            Intent::EnsureTask { sid: s, file, phase, .. }
                if s == "s2" && file == "projects/lab/s2.jsonl" && *phase == Phase::Validated
        )));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-622b: `forkedFrom` resolves under `projects/**` at ANY depth —
    // the same first-hit rule the SDK's find uses.
    #[test]
    fn t622b_forked_from_hits_at_arbitrary_depth() {
        let mirror = tmp("t622b");
        write_file(
            &mirror,
            "projects/-Users-x-proj/deep/s3.jsonl",
            b"{\"a\":1}\n{\"forkedFrom\":{\"sessionId\":\"p\"}}\n",
        );
        let probe = fork_probe(&mirror, "s3");
        assert!(probe.exists && probe.forked_from);
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-623: the other `--resume` shapes are all zero sync —
    // pull present, publishedAt present, tombstone hit, zero complete
    // lines with no record.
    #[test]
    fn t623_other_resume_shapes_do_nothing() {
        let cases: Vec<(&str, &[u8], Option<SessionRecord>)> = vec![
            // pull present — ordinary B-owned resume.
            (
                "pull",
                b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n",
                Some(rec_with_pull("s")),
            ),
            // publishedAt — the fork's origin; also plain resume.
            ("published", b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n", {
                let mut r = SessionRecord::default();
                r.published_at = Some("t".into());
                Some(r)
            }),
            // tombstone — never resume a deleted session.
            ("tombstone", b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n", {
                let mut r = SessionRecord::default();
                r.bury("t", None);
                Some(r)
            }),
            // zero complete lines + no record — the torn-tail pre-state.
            ("zero-lines", b"{\"forkedFrom\":{\"sessionId\":\"p\"}", None),
        ];
        for (name, bytes, record) in cases {
            let mirror = tmp(&format!("t623-{name}"));
            write_file(&mirror, "projects/lab/s.jsonl", bytes);
            let probe = fork_probe(&mirror, "s");
            let shape = shape_of(&["--resume", "s"]);
            assert_eq!(
                decide(&shape, &probe, record.as_ref()),
                PreRelay::ResumeNoop,
                "{name} must be zero sync action"
            );
            std::fs::remove_dir_all(&mirror).unwrap();
        }
        // Missing file entirely → same no-op (B's loud resume-miss backstop).
        let mirror = tmp("t623-nofile");
        let probe = fork_probe(&mirror, "ghost");
        let shape = shape_of(&["--resume", "ghost"]);
        assert_eq!(decide(&shape, &probe, None), PreRelay::ResumeNoop);
    }

    // T-715's ordering half: a fork-resume whose product vanished answers
    // Superseded BEFORE the WS could open — `wait_for_publish` fails
    // loudly instead of letting the relay start against a dead source.
    #[tokio::test]
    async fn t715_superseded_fork_fails_before_ws() {
        let mirror = tmp("t715");
        let sid = "s9";
        write_file(
            &mirror,
            "projects/lab/s9.jsonl",
            b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n",
        );
        state::ensure_sync_dir(&mirror).unwrap();
        let mut st = State::empty_v3();
        st.sessions.insert(
            sid.to_string(),
            SessionRecord {
                deleted: Some(Deleted {
                    at: "t".into(),
                    published_at_delete: false,
                    file_id: None,
                }),
                ..SessionRecord::default()
            },
        );
        state::commit(&state::sync_dir(&mirror), &st).unwrap();
        let _owner = hold_owner(&mirror);
        let err = wait_for_publish(&mirror, sid, "projects/lab/s9.jsonl".into())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("superseded"),
            "Superseded must abort the relay loudly: {err:#}"
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // argv parsing pins: the value/equals forms both resolve; a bare
    // `--resume` at argv end is no resume at all.
    #[test]
    fn argv_parse_edges() {
        let shape = shape_of(&["--resume=s7"]);
        assert_eq!(shape.session, Some(SessionArg::Resume("s7")));
        let shape = shape_of(&["--session-id=s8"]);
        assert_eq!(shape.session, Some(SessionArg::New("s8")));
        let shape = shape_of(&["--resume"]);
        assert_eq!(shape.session, None);
        let shape = shape_of(&["--no-session-persistence", "--resume", "s"]);
        assert!(shape.no_persist);
        assert_eq!(shape.session, Some(SessionArg::Resume("s")));
    }

    // R3-I4: `open_readonly` — the waiter's door into Shared — never
    // renames a corrupt state.json, never creates the `.sync` tree, and
    // still latches fail_closed on corruption.
    #[test]
    fn open_readonly_never_quarantines_or_creates() {
        // Corrupt file: reported + latched, but the file stays put.
        let mirror = tmp("ro-open");
        let sync_dir = state::ensure_sync_dir(&mirror).unwrap();
        std::fs::write(state::state_path(&sync_dir), "{not json").unwrap();
        let (shared, corruption) = Shared::open_readonly(&mirror).unwrap();
        assert!(corruption.is_some());
        assert!(shared.fail_closed.load(Ordering::Relaxed));
        assert!(
            state::state_path(&sync_dir).exists(),
            "a reader must never rename state.json"
        );
        assert!(state::corrupt_files(&sync_dir).unwrap().is_empty());
        std::fs::remove_dir_all(&mirror).unwrap();

        // Mirror exists but `.sync` does not: the open still creates nothing.
        let mirror = tmp("ro-open-absent");
        std::fs::create_dir_all(&mirror).unwrap();
        let (_shared, corruption) = Shared::open_readonly(&mirror).unwrap();
        assert!(corruption.is_none());
        assert!(
            !state::sync_dir(&mirror).exists(),
            "read-only open must not materialize .sync"
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Read-only state access: a corrupt state.json answers empty rather
    // than quarantining — and is NEVER renamed away by the adapter.
    #[test]
    fn read_state_ro_never_quarantines() {
        let mirror = tmp("ro");
        let sync_dir = state::ensure_sync_dir(&mirror).unwrap();
        std::fs::write(state::state_path(&sync_dir), "{not json").unwrap();
        let st = read_state_ro(&sync_dir);
        assert!(st.sessions.is_empty());
        // The file is still there — quarantine is the writer's privilege.
        assert!(state::state_path(&sync_dir).exists());
        assert!(state::corrupt_files(&sync_dir).unwrap().is_empty());
        // And a normal file decodes.
        let mut st = State::empty_v3();
        st.sessions.insert("s".into(), rec_with_pull("s"));
        state::commit(&sync_dir, &st).unwrap();
        let st = read_state_ro(&sync_dir);
        assert_eq!(st.sessions["s"], rec_with_pull("s"));
        std::fs::remove_dir_all(&mirror).unwrap();
    }
}
