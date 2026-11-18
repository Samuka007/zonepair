//! The materialize executor's disk half (tech-spec-p2 §3.2, RFC-0003-zonepaird
//! §4.5): the two `evict` disk orders, the stub-shell sentinel generator,
//! and the shell-identification probe classify/tombstone code uses.
//!
//! Write-order rule: the `materialized` commit ALWAYS lands after the
//! file operation it describes — a mark must never outrun the bytes it
//! attests. Per-mode sequence and crash window:
//!
//! | op | disk order | crash window |
//! |---|---|---|
//! | `evict --drop` | delete file → dir fsync → commit `unloaded` | mid-window: `pull∧no file∧full` suspends on the next reconcile (accepted misjudgment, §4.5) |
//! | `evict --stub` | read file → sentinel → `<file>.zp<PID>` + fsync → atomic rename → dir fsync → commit `stub` | before rename: `*.zp*` residue is swept by startup cleanup; a `full` record + real file classifies row 16 steady |
//! | `hydrate` | `apply.rs`'s reset disk path (worker `run_pull`) → same-tx `full` + `pull.cursor` | file written, mark pending → row 14 (unloaded: re-pull self-heals) / row 15 (stub: non-shell suspends) |
//!
//! Replay idempotency (§4.3-3): `evict --stub` on an existing shell is a
//! no-op; `evict --drop` and `hydrate` replay with the same outcome as
//! their first run. The journal record survives until this module's
//! commit lands — `finish(Done)` deletes it (§4.3-4③).
//!
//! The stub recipe is probe-fixed (tech-spec-p2 §5, probe6 C1): **one
//! sentinel JSON line, zero transcript lines** — any real transcript line
//! makes the shell forkable, the rejected shape.

use anyhow::{Context, Result};
use std::path::Path;

use crate::sync::persist::fs::{fsync_dir, write_zp_atomic};
use crate::sync::persist::journal::{EvictMode, Intent};
use crate::sync::persist::state::Materialized;
use super::pool::TaskOutcome;
use super::pull::run_pull;
use super::shared::{ExecCtx, Shared};

/// The sentinel's `type` marker — shell identification is this one
/// string-equality probe (tech-spec-p2 §3.2: cheaper and more reliable
/// than fingerprinting).
pub const STUB_TYPE: &str = "zonepair-stub";

/// Sentinel format version — reserved for SDK drift (spec §7).
pub const STUB_VERSION: u64 = 1;

/// Head/tail windows `make_stub` extracts summary fields from.
const SUMMARY_WINDOW: usize = 64 * 1024;

// ------------------------------------------------------------- the probe ---

/// `bytes` IS a generated stub shell: exactly one non-empty line, valid
/// JSON, `type == "zonepair-stub"`. Strict whole-file — a sentinel plus
/// anything else is NOT a shell (the zero-transcript rule is what makes
/// the shell unforkable; a tampered file must fail this probe so
/// classify row 15 suspends instead of row 12 blessing extra bytes).
pub fn is_stub_shell(bytes: &[u8]) -> bool {
    let mut lines = bytes.split(|&b| b == b'\n').filter(|l| !l.is_empty());
    let (Some(line), None) = (lines.next(), lines.next()) else {
        return false;
    };
    serde_json::from_slice::<serde_json::Value>(line)
        .ok()
        .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(str::to_owned))
        .as_deref()
        == Some(STUB_TYPE)
}

// ------------------------------------------------------------ make_stub ---

/// Everything `make_stub` needs that the file itself cannot carry.
pub struct StubInput<'a> {
    /// `sessions` key — the sentinel's `sessionId`.
    pub sid: &'a str,
    /// `pull.cursor` verbatim — `originalCursor` (`zpc1.<off>.<fp>`).
    pub cursor: &'a str,
    /// Eviction wall time (`now_rfc3339`) — `timestamp`/`evictedAt`.
    pub evicted_at: &'a str,
}

/// Synthesize the stub shell for a session file's current bytes: one
/// sentinel line + `\n`. Summary fields come from the head/tail 64 KiB
/// windows; a file yielding nothing extractable degrades to the bare
/// marker+provenance sentinel — `exists` still judges true, evict is
/// never blocked by an unreadable summary (spec §7).
pub fn make_stub(input: &StubInput, original: &[u8]) -> Vec<u8> {
    let summary = extract_summary(original);
    let mut sentinel = serde_json::json!({
        "type": STUB_TYPE,
        "stubVersion": STUB_VERSION,
        "sessionId": input.sid,
        "uuid": "zpstub-0",
        "timestamp": input.evicted_at,
        "evictedAt": input.evicted_at,
        "originalBytes": original.len(),
        "originalCursor": input.cursor,
    });
    let obj = sentinel.as_object_mut().expect("sentinel is an object");
    for (key, value) in [
        ("customTitle", summary.custom_title),
        ("firstPrompt", summary.first_prompt),
        ("lastPrompt", summary.last_prompt),
        ("cwd", summary.cwd),
        ("gitBranch", summary.git_branch),
    ] {
        if let Some(value) = value {
            obj.insert(key.to_string(), serde_json::Value::String(value));
        }
    }
    let mut out = serde_json::to_vec(&sentinel).expect("sentinel serializes");
    out.push(b'\n');
    out
}

/// Fields copied from the session file into the sentinel so
/// `getSessionInfo`'s type-blind extraction still answers them
/// (tech-spec-p2 §3.2). `firstPrompt`/`lastPrompt` bracket the user's own
/// messages; title/cwd/gitBranch take their latest appearance.
#[derive(Default)]
struct Summary {
    custom_title: Option<String>,
    first_prompt: Option<String>,
    last_prompt: Option<String>,
    cwd: Option<String>,
    git_branch: Option<String>,
}

fn extract_summary(bytes: &[u8]) -> Summary {
    let mut summary = Summary::default();
    for line in head_tail_windows(bytes) {
        if line.is_empty() {
            continue;
        }
        // Window-boundary torn lines fail the parse and are skipped —
        // summary extraction is best-effort by contract.
        let Ok(v) = serde_json::from_slice::<serde_json::Value>(line) else {
            continue;
        };
        if let Some(t) = v.get("customTitle").and_then(|t| t.as_str()) {
            summary.custom_title = Some(t.to_string());
        }
        if let Some(t) = v.get("cwd").and_then(|t| t.as_str()) {
            summary.cwd = Some(t.to_string());
        }
        if let Some(t) = v.get("gitBranch").and_then(|t| t.as_str()) {
            summary.git_branch = Some(t.to_string());
        }
        if let Some(text) = prompt_text(&v) {
            if summary.first_prompt.is_none() {
                summary.first_prompt = Some(text.clone());
            }
            summary.last_prompt = Some(text);
        }
    }
    summary
}

/// `type:"user"` message text — `message.content` is a string or an
/// array of `{type:"text", text}` parts.
fn prompt_text(v: &serde_json::Value) -> Option<String> {
    if v.get("type").and_then(|t| t.as_str()) != Some("user") {
        return None;
    }
    match v.get("message").and_then(|m| m.get("content")) {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Array(parts)) => parts
            .iter()
            .find(|p| p.get("type").and_then(|t| t.as_str()) == Some("text"))
            .and_then(|p| p.get("text").and_then(|t| t.as_str()))
            .map(str::to_owned),
        _ => None,
    }
}

/// Lines of the head window then the tail window (no second scan when
/// they overlap). A window cut may split a line — that fragment simply
/// fails JSON parsing above.
fn head_tail_windows(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let head_len = bytes.len().min(SUMMARY_WINDOW);
    let (head, rest) = bytes.split_at(head_len);
    let tail = if rest.is_empty() {
        &[][..]
    } else {
        &rest[rest.len().saturating_sub(SUMMARY_WINDOW)..]
    };
    head.split(|&b| b == b'\n').chain(tail.split(|&b| b == b'\n'))
}

// ----------------------------------------------------------------- evict ---

/// `evict{mode}` for `sid` — the §4.5 disk sequence, then the single
/// `materialized` transaction. Idempotent by shape (§4.3-3): a replay
/// landing on the already-evicted shape converges the mark if it is
/// missing, else returns without touching anything.
///
/// Skipped silently (intent consumed, nothing committed): tombstoned
/// records (§4.3-4① — read-in already drops these; execution re-checks
/// for the race), records without `pull` (`materialized` off-pull is the
/// row-25 corruption the table suspends on — never mint it), and the
/// mode's already-done shape.
///
/// `evict_record` is the journal filename this intent arrived under —
/// the `commit_materialized` pullPending clear orders against it by
/// provenance (`pullPending.record < evict_record` dies, newer survives).
/// Filename order IS journal order; no clock comparison exists anywhere
/// in this path.
pub fn evict(shared: &Shared, sid: &str, mode: EvictMode, evict_record: &str) -> Result<()> {
    let snapshot = shared.with_state(|st| {
        st.sessions.get(sid).and_then(|rec| {
            if rec.deleted.is_some() {
                return None; // tombstone outranks the intent — suppress
            }
            rec.pull.as_ref().map(|pull| (pull.file.clone(), pull.cursor.clone()))
        })
    });
    let Some((file_rel, cursor)) = snapshot else {
        // No record, tombstoned, or pull-less: nothing legitimately
        // evictable — consume the intent instead of minting a row-25
        // corruption field.
        return Ok(());
    };
    let file = shared.mirror.join(&file_rel);
    let now = crate::sync::persist::time::now_rfc3339();
    match mode {
        EvictMode::Drop => {
            // ① delete + dir fsync. NotFound is the replay shape — the
            // file's absence IS the goal state.
            match std::fs::remove_file(&file) {
                Ok(()) => {
                    if let Some(dir) = file.parent() {
                        fsync_dir(dir)?;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(e).with_context(|| format!("evict drop {}", file.display()))
                }
            }
            // ② single-transaction mark + provenance pullPending clear.
            commit_materialized(shared, sid, Materialized::Unloaded, evict_record)?;
        }
        EvictMode::Stub => {
            // ① read the original. NotFound is a NO-OP — fabricating a
            // shell over empty bytes would *create* a file on the
            // `pull∧full∧no file` (row-17 misjudgment) shape: the file is
            // already gone, the intent is consumed, nothing commits.
            // The spec's degraded-sentinel arm covers only UNREADABLE
            // originals, never absent ones.
            let original = match std::fs::read(&file) {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(e) => {
                    return Err(e).with_context(|| format!("reading {}", file.display()))
                }
            };
            if !is_stub_shell(&original) {
                // ② sentinel → `<file>.zp<PID>` + fsync → atomic rename →
                // dir fsync — the same four-step discipline as
                // apply::reset; `*.zp*` residue never enters the scan set.
                let shell = make_stub(
                    &StubInput {
                        sid,
                        cursor: &cursor,
                        evicted_at: &now,
                    },
                    &original,
                );
                write_shell(&file, &shell)
                    .with_context(|| format!("writing stub shell {}", file.display()))?;
            }
            // Already a shell = the replay no-op; the commit below still
            // lands a missing mark (crash-after-rename convergence).
            // ③ single-transaction mark + provenance pullPending clear.
            commit_materialized(shared, sid, Materialized::Stub, evict_record)?;
        }
    }
    Ok(())
}

/// `<file>.zp<PID>` + fsync → rename over `file` → dir fsync.
fn write_shell(file: &Path, shell: &[u8]) -> Result<()> {
    write_zp_atomic(file, shell)
}

/// The `materialized` transaction: the mark plus §4.3-4②'s `pullPending`
/// clear, ordered by provenance — a pending is cleared only when its
/// `record` names a journal file ordering BEFORE `evict_record`
/// (lexicographic filename order is consume order). A pending stamped
/// by a pull intent read in *after* this evict is post-evict traffic —
/// "evict outranks in-flight" claims the bytes, not the newer intent — and a
/// provenance-less pending (pre-fix file) counts as predating. A
/// tombstone committed mid-execution suppresses the mark — the bury
/// already cleared `materialized` and this intent is dead.
fn commit_materialized(
    shared: &Shared,
    sid: &str,
    materialized: Materialized,
    evict_record: &str,
) -> Result<()> {
    shared.commit_state(|st| {
        let Some(rec) = st.sessions.get_mut(sid) else {
            return;
        };
        if rec.deleted.is_some() || rec.pull.is_none() {
            return;
        }
        rec.materialized = Some(materialized);
        if rec
            .pull_pending
            .as_ref()
            .is_none_or(|p| p.predates(evict_record))
        {
            rec.pull_pending = None;
        }
    })?;
    shared.notify.signal(sid);
    Ok(())
}

/// The materialize executor (RFC-0003-zonepaird §4.5): `evict` is pure local disk —
/// `evict` (this module) runs the mode's write sequence and commits
/// `materialized`; `hydrate` IS the pull executor's reset path — the
/// `materialized != full` divert plus `force_reset` send the zero cursor
/// either way, B answers `reset`, and `commit_pull` lands
/// `materialized="full"` + `pull.cursor` in one transaction. Replays are
/// idempotent by shape (§4.3-3).
pub async fn run_materialize(ctx: &ExecCtx, record_name: &str, intent: &Intent) -> TaskOutcome {
    match intent {
        Intent::Evict { sid, mode, .. } => match evict(&ctx.shared, sid, *mode, record_name) {
            Ok(()) => TaskOutcome::Done,
            Err(e) => TaskOutcome::Retry(format!("evict {sid}: {e:#}")),
        },
        Intent::Hydrate { sid, .. } => {
            // The same suppression `materialize::evict` applies: hydrate
            // has meaning only on a `pull` record — without `pull` there
            // is no evicted byte-shape to restore, and a forced reset
            // would overwrite local content (e.g. an unpublished A-side
            // task file) and mint a `pull` the record never had. A
            // tombstone short-circuits inside `run_pull`; a missing or
            // pull-less record consumes the intent silently here.
            let pullable = ctx.shared.with_state(|st| {
                st.sessions
                    .get(sid)
                    .is_some_and(|r| r.deleted.is_none() && r.pull.is_some())
            });
            if !pullable {
                return TaskOutcome::Done;
            }
            run_pull(ctx, sid, true).await
        }
        // Only evict/hydrate intents reach this variant by construction.
        other => {
            eprintln!("zonepaird: materialize work carried non-materialize intent {other:?}");
            TaskOutcome::Done
        }
    }
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::persist::fs::testkit::{commit_rec, pull_rec, setup_shared, tmp, write_file};
    use crate::sync::persist::fs;
    use crate::sync::state::{self, PullPending, SessionRecord};

    fn setup(name: &str) -> (std::path::PathBuf, std::sync::Arc<Shared>) {
        setup_shared("materialize", name)
    }

    // T-802 (drop): file + dir fsync → single-tx `unloaded`; the in-flight
    // pullPending is cleared in the same commit; replay is a no-op.
    #[test]
    fn t802_evict_drop_sequence_and_replay() {
        let (mirror, shared) = setup("t802drop");
        write_file(&mirror, "projects/lab/s1.jsonl", b"{\"a\":1}\n");
        let mut rec = pull_rec("s1", "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        rec.pull_pending = Some(PullPending {
            target: "latest".into(),
            enqueued_at: "2026-11-18T00:00:00Z".into(),
            record: None,
        });
        commit_rec(&shared, "s1", rec);

        evict(&shared, "s1", EvictMode::Drop, "2026-11-18T01:00:00Z").unwrap();
        assert!(!mirror.join("projects/lab/s1.jsonl").exists());
        let rec = shared.with_state(|st| st.sessions["s1"].clone());
        assert_eq!(rec.materialization(), Materialized::Unloaded);
        assert!(rec.pull_pending.is_none(), "evict clears pullPending");
        assert!(rec.pull.is_some(), "the cursor outlives the bytes");
        // On-disk commit landed too.
        let disk = state::load(&shared.sync_dir).unwrap();
        assert_eq!(
            disk.state.sessions["s1"].materialization(),
            Materialized::Unloaded
        );

        // Replay: file already absent + mark already unloaded → no-op.
        evict(&shared, "s1", EvictMode::Drop, "2026-11-18T01:00:00Z").unwrap();
        assert!(!mirror.join("projects/lab/s1.jsonl").exists());
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // §4.3-4② provenance ordering: the materialized commit clears
    // `pullPending` only when its `record` orders BEFORE the evict record
    // in journal order. A pending stamped by a post-evict pull intent is
    // fresh traffic — it survives the commit (and the run_pull gate reads
    // it as the legal hydrate channel). Provenance-less pendings (pre-
    // fix files) count as predating and die.
    #[test]
    fn evict_commit_clears_pull_pending_by_provenance() {
        let (mirror, shared) = setup("evict-pending");
        write_file(&mirror, "projects/lab/s1.jsonl", b"{\"a\":1}\n");
        write_file(&mirror, "projects/lab/s2.jsonl", b"{\"a\":1}\n");
        write_file(&mirror, "projects/lab/s3.jsonl", b"{\"a\":1}\n");
        for (sid, record) in [
            ("s1", Some("100-1-1.json".to_string())), // pre-evict → cleared
            ("s2", Some("300-1-1.json".to_string())), // post-evict → survives
            ("s3", None),                             // no provenance → cleared
        ] {
            let mut rec = pull_rec(sid, "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            rec.pull_pending = Some(PullPending {
                target: "latest".into(),
                enqueued_at: "t".into(),
                record,
            });
            commit_rec(&shared, sid, rec);
        }
        for sid in ["s1", "s2", "s3"] {
            evict(&shared, sid, EvictMode::Drop, "200-1-1.json").unwrap();
        }
        assert!(shared.with_state(|st| st.sessions["s1"].pull_pending.is_none()));
        assert!(
            shared.with_state(|st| st.sessions["s2"].pull_pending.is_some()),
            "post-evict pending survives the commit clear"
        );
        assert!(shared.with_state(|st| st.sessions["s3"].pull_pending.is_none()));
        for sid in ["s1", "s2", "s3"] {
            assert_eq!(
                shared.with_state(|st| st.sessions[sid].materialization()),
                Materialized::Unloaded
            );
        }
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // R2-M4: `evict --stub` on an absent file is a no-op — it must NOT
    // fabricate a shell over empty bytes (creating a file on the row-17
    // `pull∧full∧no file` shape), and it must NOT commit `materialized`
    // either: the record stays as-is for the suspend row to classify.
    #[test]
    fn evict_stub_on_absent_file_is_noop() {
        let (mirror, shared) = setup("stub-absent");
        commit_rec(&shared, "s1", pull_rec("s1", "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        evict(&shared, "s1", EvictMode::Stub, "1-1-1.json").unwrap();
        assert!(
            !mirror.join("projects/lab/s1.jsonl").exists(),
            "absent file must not gain a fabricated shell"
        );
        let rec = shared.with_state(|st| st.sessions["s1"].clone());
        assert!(
            rec.materialization().is_full(),
            "no mark commits — the intent is consumed, not executed"
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-802 (stub): original → sentinel via `*.zp<PID>` + rename → `stub`;
    // no `*.zp*` residue; replay on the shell is a byte-identical no-op.
    #[test]
    fn t802_evict_stub_writes_sentinel_and_replays_noop() {
        let (mirror, shared) = setup("t802stub");
        let original = concat!(
            "{\"type\":\"user\",\"message\":{\"content\":\"first ask\"},\"cwd\":\"/p\",\"gitBranch\":\"main\"}\n",
            "{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"ok\"}]}}\n",
            "{\"type\":\"custom-title\",\"customTitle\":\"My Title\"}\n",
            "{\"type\":\"user\",\"message\":{\"content\":\"last ask\"}}\n",
        );
        write_file(&mirror, "projects/lab/s1.jsonl", original.as_bytes());
        commit_rec(&shared, "s1", pull_rec("s1", "zpc1.200.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));

        evict(&shared, "s1", EvictMode::Stub, "2026-11-18T01:00:00Z").unwrap();
        let file = mirror.join("projects/lab/s1.jsonl");
        let shell = std::fs::read(&file).unwrap();
        assert!(is_stub_shell(&shell), "file is now the sentinel shell");
        let v: serde_json::Value = serde_json::from_slice(&shell).unwrap();
        assert_eq!(v["originalBytes"], original.len() as u64);
        assert_eq!(v["originalCursor"], "zpc1.200.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(v["customTitle"], "My Title");
        assert_eq!(v["firstPrompt"], "first ask");
        assert_eq!(v["lastPrompt"], "last ask");
        assert_eq!(v["cwd"], "/p");
        assert_eq!(v["gitBranch"], "main");
        assert_eq!(
            shared.with_state(|st| st.sessions["s1"].materialization()),
            Materialized::Stub
        );
        // No `*.zp*` residue in the session dir.
        let leftovers: Vec<_> = std::fs::read_dir(file.parent().unwrap())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_str().is_some_and(|n| n.contains(".zp")))
            .collect();
        assert!(leftovers.is_empty(), "temp residue swept: {leftovers:?}");

        // Replay (§4.3-3): already a shell → no-op, bytes unchanged.
        evict(&shared, "s1", EvictMode::Stub, "2026-11-18T01:00:00Z").unwrap();
        assert_eq!(std::fs::read(&file).unwrap(), shell);

        // Tombstoned record → suppressed entirely (§4.3-4① at exec time).
        shared
            .commit_state(|st| {
                st.sessions.get_mut("s1").unwrap().bury("t", None);
            })
            .unwrap();
        evict(&shared, "s1", EvictMode::Drop, "2026-11-18T03:00:00Z").unwrap();
        assert!(file.exists(), "tombstone suppresses the evict");
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // `evict` on a pull-less record never mints `materialized` — that
    // combination is the row-25 corruption the table suspends on.
    #[test]
    fn evict_without_pull_commits_nothing() {
        let (mirror, shared) = setup("evict-nopull");
        commit_rec(&shared, "s1", SessionRecord::default());
        evict(&shared, "s1", EvictMode::Drop, "2026-11-18T01:00:00Z").unwrap();
        assert!(
            shared.with_state(|st| st.sessions["s1"].materialized.is_none()),
            "no pull → no materialized field"
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-808: `make_stub` on a real session file → one legal-JSON sentinel
    // line, `type`/`stubVersion` always true, summary fields extracted,
    // zero transcript lines; unreadable/degenerate input still yields the
    // marker sentinel (evict is never blocked).
    #[test]
    fn t808_make_stub_normal_and_degraded() {
        let input = StubInput {
            sid: "s9",
            cursor: "zpc1.42.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            evicted_at: "2026-11-18T01:00:00Z",
        };
        let original = concat!(
            "{\"type\":\"user\",\"message\":{\"content\":\"first\"},\"cwd\":\"/a\",\"gitBranch\":\"dev\"}\n",
            "{\"type\":\"user\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"second\"}]}}\n",
            "{\"type\":\"custom-title\",\"customTitle\":\"T\"}\n",
        );
        let out = make_stub(&input, original.as_bytes());
        // Exactly one line, valid JSON.
        let text = String::from_utf8(out.clone()).unwrap();
        assert_eq!(text.matches('\n').count(), 1);
        let v: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(v["type"], STUB_TYPE);
        assert_eq!(v["stubVersion"], STUB_VERSION);
        assert_eq!(v["sessionId"], "s9");
        assert_eq!(v["originalBytes"], original.len() as u64);
        assert_eq!(v["originalCursor"], input.cursor);
        assert_eq!(v["customTitle"], "T");
        assert_eq!(v["firstPrompt"], "first");
        assert_eq!(v["lastPrompt"], "second");
        assert_eq!(v["cwd"], "/a");
        assert_eq!(v["gitBranch"], "dev");
        // The zero-transcript normative rule IS the shell probe.
        assert!(is_stub_shell(&out));

        // Degraded inputs: garbage and empty still produce one legal
        // marker line — no summary fields, never an evict blocker.
        for original in [b"\x80\x81 not json {{{".as_slice(), b"".as_slice()] {
            let out = make_stub(&input, original);
            let v: serde_json::Value =
                serde_json::from_slice(out[..out.len() - 1].into()).unwrap();
            assert_eq!(v["type"], STUB_TYPE);
            assert_eq!(v["stubVersion"], STUB_VERSION);
            assert_eq!(v["originalBytes"], original.len() as u64);
            for key in ["customTitle", "firstPrompt", "lastPrompt", "cwd", "gitBranch"] {
                assert!(v.get(key).is_none(), "degraded sentinel lacks {key}");
            }
            assert!(is_stub_shell(&out));
        }
    }

    // T-809: the shell probe's three states — sentinel true, any real
    // transcript line false, a sentinel glued to extra bytes false;
    // `*.zp*` half-written shells never enter the scan set.
    #[test]
    fn t809_shell_probe_and_scan_set() {
        let sentinel = make_stub(
            &StubInput {
                sid: "s",
                cursor: "zpc1.1.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                evicted_at: "t",
            },
            b"{\"a\":1}\n",
        );
        assert!(is_stub_shell(&sentinel));
        assert!(!is_stub_shell(b"{\"type\":\"user\",\"message\":{\"content\":\"x\"}}\n"));
        // Sentinel + anything = not a shell (row-15 shape, not row-12).
        let mut tampered = b"{\"x\":1}\n".to_vec();
        tampered.extend_from_slice(&sentinel);
        assert!(!is_stub_shell(&tampered));

        let mirror = tmp("materialize", "t809scan");
        write_file(&mirror, "projects/lab/s.jsonl", b"{\"a\":1}\n");
        // A half-written stub temp must not join the session scan set.
        write_file(&mirror, "projects/lab/s.jsonl.zp4242", b"partial");
        let hits = fs::scan_session_files(&mirror);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "s");
        std::fs::remove_dir_all(&mirror).unwrap();
    }
}

// --------------------------------------------------- executor tests ---

#[cfg(test)]
mod executor_tests {
    use super::*;
    use serde_json::json;

    use crate::sync::engine::pool::{Pool, Work};
    use crate::sync::engine::testkit::*;
    use crate::sync::persist::journal;
    use crate::sync::persist::state::{self, Phase, Pull, SessionRecord};

    use crate::sync::persist::fs::testkit::write_file;

    // T-803/T-804: an evict intent drains to `Work::Materialize`, the
    // executor runs the drop disk order, and `finish(Done)` deletes the
    // journal record — the deferred file survives read-in and dies only
    // after the `materialized` commit.
    #[tokio::test]
    async fn t803_evict_intent_executes_and_record_deletes_post_commit() {
        let (mirror, shared) = setup("t803evict");
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
        journal::append(
            &shared.journal_dir,
            &Intent::Evict {
                sid: "s1".into(),
                mode: journal::EvictMode::Drop,
                enqueued_at: "2026-11-18T02:00:00Z".into(),
            },
        )
        .unwrap();
        let name = {
            let mut names: Vec<_> = std::fs::read_dir(&shared.journal_dir)
                .unwrap()
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            names.sort();
            names.pop().unwrap()
        };
        let pool = Pool::new(ExecCtx::new(shared.clone()));
        pool.drain_journal().unwrap();
        // ③ the deferred record still exists after read-in; ② pullPending
        // cleared; the work picked is the materialize slot.
        assert!(shared.journal_dir.join(&name).exists());
        assert!(shared.with_state(|st| st.sessions["s1"].pull_pending.is_none()));
        let work = pool.pick().expect("materialize work queued");
        assert!(matches!(work, Work::Materialize { .. }), "{work:?}");
        let outcome = pool.run_one(&work).await;
        assert_eq!(outcome, TaskOutcome::Done);
        assert!(!mirror.join("projects/lab/s1.jsonl").exists());
        assert_eq!(
            shared.with_state(|st| st.sessions["s1"].materialization()),
            Materialized::Unloaded
        );
        // The record file is deleted only on finish(Done) — commit-then-
        // delete for the deferred record (§4.3-4③).
        assert!(shared.journal_dir.join(&name).exists());
        pool.finish(&work, outcome);
        assert!(!shared.journal_dir.join(&name).exists());
        std::fs::remove_dir_all(&mirror).unwrap();
    }
    // Wave-2 I-3: `hydrate` carries evict's suppression — a pull-less
    // record (no record / task-only / registered-only) consumes the
    // intent silently. A forced reset on an unpublished A-side file
    // would overwrite local bytes and mint a `pull` the record never
    // had — silent data loss.
    #[tokio::test]
    async fn hydrate_on_pull_less_record_is_suppressed() {
        let (mirror, shared) = setup("hyd-suppress");
        write_file(
            &mirror,
            "projects/lab/s2.jsonl",
            b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n",
        );
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "s2".into(),
                    SessionRecord {
                        task: Some(state::Task {
                            file: "projects/lab/s2.jsonl".into(),
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
        // No base needed: suppression exits before any request.
        let ctx = ExecCtx::new(shared.clone());
        let intent = Intent::Hydrate {
            sid: "s2".into(),
            enqueued_at: "t".into(),
        };
        let outcome = run_materialize(&ctx, "1-1-1.json", &intent).await;
        assert_eq!(outcome, TaskOutcome::Done);
        let rec = shared.with_state(|st| st.sessions["s2"].clone());
        assert!(rec.pull.is_none(), "suppressed hydrate never mints pull");
        assert!(rec.materialization().is_full());
        assert_eq!(
            std::fs::read(mirror.join("projects/lab/s2.jsonl")).unwrap(),
            b"{\"forkedFrom\":{\"sessionId\":\"p\"}}\n",
            "local bytes untouched"
        );
        // A missing record is consumed the same way.
        let intent = Intent::Hydrate {
            sid: "ghost".into(),
            enqueued_at: "t".into(),
        };
        assert_eq!(
            run_materialize(&ctx, "1-1-1.json", &intent).await,
            TaskOutcome::Done
        );
        assert!(!shared.with_state(|st| st.sessions.contains_key("ghost")));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-803: `hydrate` is the pull executor's reset path — zero cursor
    // out, reset in, same-tx `materialized="full"` + cursor, record
    // deleted after. From a `stub` record this doubles as the
    // crash-window's intended landing.
    #[tokio::test]
    async fn t803_hydrate_intent_pulls_back_to_full() {
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
        let (mirror, shared) = setup("t803hyd");
        // A stub shell on disk + the stub mark.
        write_file(
            &mirror,
            "projects/lab/s1.jsonl",
            b"{\"type\":\"zonepair-stub\",\"stubVersion\":1}\n",
        );
        shared
            .commit_state(|st| {
                st.sessions.insert(
                    "s1".into(),
                    SessionRecord {
                        pull: Some(Pull {
                            cursor: "zpc1.200.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                            file: "projects/lab/s1.jsonl".into(),
                        }),
                        materialized: Some(Materialized::Stub),
                        ..Default::default()
                    },
                );
            })
            .unwrap();
        journal::append(
            &shared.journal_dir,
            &Intent::Hydrate {
                sid: "s1".into(),
                enqueued_at: "t".into(),
            },
        )
        .unwrap();
        let pool = Pool::new(ctx_with(&base, &shared));
        pool.drain_journal().unwrap();
        let work = pool.pick().expect("hydrate work queued");
        assert!(matches!(work, Work::Materialize { .. }));
        let outcome = pool.run_one(&work).await;
        assert_eq!(outcome, TaskOutcome::Done);
        let reqs = lines(&seen);
        assert!(!reqs[0].contains("cursor="), "hydrate starts from zero");
        let rec = shared.with_state(|st| st.sessions["s1"].clone());
        assert_eq!(rec.materialization(), Materialized::Full);
        assert_eq!(rec.pull.as_ref().unwrap().cursor, next);
        assert_eq!(
            std::fs::read(mirror.join("projects/lab/s1.jsonl")).unwrap(),
            body
        );
        finish_server(server, &seen).await;
        std::fs::remove_dir_all(&mirror).unwrap();
    }

}
