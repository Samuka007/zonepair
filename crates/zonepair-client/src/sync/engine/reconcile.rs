//! The reconciliation executor (RFC-0003-zonepaird §9.2): one pass over
//! `mirror × state.sessions`, tombstone filter first, then the §6.1 joint
//! decision table — `classify()` is re-run for every session every round
//! and its verdict is NEVER persisted ("the field combination IS the
//! classification"; the disk holds field facts only, no "classification
//! result" column).
//!
//! What a pass does per verdict:
//!
//! | Action | Disposition |
//! |---|---|
//! | Steady | `pull∧file∧full` additionally runs the §7.2 length predicate |
//! | Hold | nothing |
//! | WritingHold{orphan:false} | ensure a `task(writing)` row exists (row 22 / E7) |
//! | WritingHold{orphan:true} | orphan register: `task(writing)` permanent hold |
//! | Publish | hand work to the pool (`ReconcileReport.work`) |
//! | TerminateWithTombstone | `bury()` + commit (E7 disappearance edge) |
//! | Tombstone | `bury()` + commit |
//! | SuppressAndDelete | re-delete the file that should be gone |
//! | Suspend | suspended sids report out — NO healing pull: every §6.1 |
//! |   | suspend row is human-facing (15 non-shell, 17 file-gone, 18 B- |
//! |   | lost all say suspend-for-human/no-rebuild). Pull-path degraded |
//! |   | convergence flows through the Steady arm's §7.2 length predicate |
//! | HydrateReset / FirstPull | `Work::Pull{force_reset}` — worker zero-cursor |
//! | BackfillRegistered | `registered{kind:"b-owned"}` + commit |
//!
//! The scan set is `projects/**/<sid>.jsonl`, `*.zp*` temp residue
//! excluded. State IO goes through the `SessionStore` seam so a pass can
//! commit one record at a time under whatever lock discipline the caller
//! keeps (`State` directly, or the pool's `Arc<Shared>`); B probes come in
//! through a `&dyn Fn(&str) -> BProbe` seam (production = `store_exists`,
//! tests = map/constant) so the whole pass is tempdir-testable.
//!
//! Tombstone commits need `deleted.fileId` — `tombstone_file_id()` is also
//! exported for the journal consumer's `note-remote-delete` probe: the
//! fingerprint of content present AT delete time, `None` when the file is
//! absent, `stub`, or `unloaded` (§3.3 — a sentinel fingerprint must never
//! reach the tombstone).

use anyhow::Result;
use std::path::{Path, PathBuf};

use crate::sync::state_machine::{
    self, Action, BProbe, FileProbe, LengthSource, LengthVerdict, Verdict,
};
use crate::sync::persist::fs::{fsync_dir, scan_session_files};
use crate::sync::persist::state::{self, SessionRecord, Task};
#[cfg(test)]
use crate::sync::persist::state::State;
use crate::sync::persist::time::now_rfc3339;
use super::shared::SessionStore;

/// Where this pass runs in the daemon's lifecycle: the §7.2 length
/// predicate splits by caller — startup trusts a torn tail (truncate),
/// steady distrusts any mismatch (degrade + whole re-pull).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassKind {
    /// §9.1 step 8 — crash recovery: `len > bytes` truncates own residue.
    Startup,
    /// Periodic sweep / manual `reconcile(all)`: any mismatch degrades.
    Steady,
}

impl PassKind {
    fn length_source(self) -> LengthSource {
        match self {
            Self::Startup => LengthSource::Startup,
            Self::Steady => LengthSource::Steady,
        }
    }
}

/// Test seam: production passes `&mut Arc<Shared>` (shared.rs); `State`
/// implements the trait so `run` can be driven in unit tests without a
/// `Shared` on disk.
#[cfg(test)]
impl SessionStore for State {
    fn sids(&self) -> std::collections::BTreeSet<String> {
        self.sessions.keys().cloned().collect()
    }
    fn get(&self, sid: &str) -> Option<SessionRecord> {
        self.sessions.get(sid).cloned()
    }
    fn mutate_commit(
        &mut self,
        sync_dir: &Path,
        sid: &str,
        f: &mut dyn FnMut(&mut SessionRecord),
    ) -> Result<()> {
        f(self.sessions.entry(sid.to_string()).or_default());
        state::commit(sync_dir, self)
    }
}

/// Work a verdict produced for the pool. Reconcile itself never talks to B
/// beyond the probe: pulls and publishes are queued here and executed by
/// the worker layer under its retry/budget rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Work {
    /// `task{validated}` — re-enter the publish executor (rows 3/6).
    Publish { sid: String },
    /// A pull the pool runs: `force_reset` sends the zero cursor so B's
    /// own `reset` verdict overwrites the local bytes (length-mismatch
    /// recovery, hydrate crash window, first-pull overwrite).
    Pull { sid: String, force_reset: bool },
}

/// What one pass changed — also the report tests assert on.
#[derive(Clone, Debug, Default)]
pub struct ReconcileReport {
    /// sids whose verdict suspended them this round (the caller maps them
    /// into its runtime suspension overlay — nothing is persisted).
    pub suspended: Vec<String>,
    /// sids previously suspended whose verdict is clean again.
    pub unsuspended: Vec<String>,
    /// Pool work queued by the pass.
    pub work: Vec<Work>,
    /// sids whose records were committed to `state.json` this pass.
    pub committed: Vec<String>,
    /// Non-fatal notes worth a log line (length-truncate, re-deleted files,
    /// suspensions).
    pub notes: Vec<String>,
    /// sid → `Verdict` for every session scanned — diagnostics only
    /// (`ensure_published` re-derives its own probe; nothing persisted).
    pub verdicts: Vec<(String, Verdict)>,
}

/// `deleted.fileId` for a tombstone commit or the `note-remote-delete`
/// read-in probe (§3.3): the content fingerprint of the file AT delete
/// time; `None` whenever content fingerprints cannot apply — file absent,
/// `materialized != full`, or the file is a generated stub shell (the
/// sentinel is not session content; recording it would seal a sid that is
/// meant to hydrate back).
pub fn tombstone_file_id(
    rec: Option<&SessionRecord>,
    file: &FileProbe,
) -> Option<String> {
    if !file.exists {
        return None;
    }
    if !rec.is_none_or(|r| r.materialization().is_full()) {
        return None;
    }
    if file.is_stub_shell {
        return None;
    }
    file.fingerprint.clone()
}

/// Probe one session file on disk. `file_rel` is the record's `task.file`
/// or `pull.file` mirror-relative path when present — the record binds the
/// landing spot (§3.3, "same-name ambiguity"); otherwise the `projects/**` first-hit
/// scan. Read errors NEVER degrade to `absent()` — an unreadable file
/// marked `unreadable` and the caller skips the sid this round, because
/// the absence edges it would fire are destructive (a transient EACCES
/// during a sweep could tombstone a live publish task).
pub fn probe_session_file(
    mirror: &Path,
    file_rel: Option<&str>,
    sid: &str,
) -> (FileProbe, Option<PathBuf>) {
    let path = file_rel
        .map(|r| mirror.join(r))
        .or_else(|| session_file(mirror, sid));
    match path {
        Some(p) if p.is_file() => match std::fs::read(&p) {
            Ok(bytes) => (FileProbe::from_bytes(&bytes), Some(p)),
            Err(_) => {
                let mut probe = FileProbe::absent();
                probe.exists = true;
                probe.unreadable = true;
                (probe, Some(p))
            }
        },
        _ => (FileProbe::absent(), path),
    }
}

/// First `<sid>.jsonl` hit anywhere under `projects/**` (arbitrary depth —
/// same search rule as the SDK's find). The scan itself is
/// `fs::scan_session_files`.
pub fn session_file(mirror: &Path, sid: &str) -> Option<PathBuf> {
    let mut hits = scan_session_files(mirror)
        .into_iter()
        .filter(|(s, _)| s == sid)
        .map(|(_, p)| p)
        .collect::<Vec<_>>();
    hits.sort();
    hits.into_iter().next()
}

/// Run one reconciliation pass. `store` supplies the record table and
/// takes one commit per mutation — no cross-session batching: the crash
/// window must never widen beyond the record being settled.
///
/// `b_probe` answers "does B still hold this session" per sid; verdicts
/// that need it and get `BProbe::Unknown` skip the round (§6 preamble) —
/// this function never guesses. `previously_suspended` lets the caller
/// (the pool's runtime overlay) report which sids recovered.
pub fn run<S: SessionStore>(
    mirror: &Path,
    sync_dir: &Path,
    store: &mut S,
    kind: PassKind,
    b_probe: &dyn Fn(&str) -> BProbe,
    previously_suspended: &dyn Fn(&str) -> bool,
) -> Result<ReconcileReport> {
    let mut report = ReconcileReport::default();
    // The session set is mirror ∪ records: E7's appearance edge supplies
    // sids the record table has never seen; disappearance is covered by
    // records whose file is gone.
    let mut sids = store.sids();
    for (sid, _) in scan_session_files(mirror) {
        sids.insert(sid);
    }

    for sid in sids {
        let rec = store.get(&sid);
        let file_rel = rec
            .as_ref()
            .and_then(|r| r.task.as_ref().map(|t| t.file.as_str()))
            .or_else(|| {
                rec.as_ref()
                    .and_then(|r| r.pull.as_ref().map(|p| p.file.as_str()))
            });
        let (probe, path) = probe_session_file(mirror, file_rel, &sid);
        if probe.unreadable {
            // Same discipline as `BProbe::Unknown`: this round cannot
            // classify an unreadable file — every absence edge it would
            // feed is destructive. Skip, never guess.
            report
                .notes
                .push(format!("{sid}: file unreadable — skipped this round"));
            continue;
        }
        let b = b_probe(&sid);
        let verdict = state_machine::classify(rec.as_ref(), &probe, b);
        report.verdicts.push((sid.clone(), verdict));
        match verdict.action {
            Action::Steady => {
                // Row 16's steady pull session additionally takes the §7.2
                // length predicate: `pull.cursor` byte count is the anchor.
                if let Some(pull) = rec.as_ref().and_then(|r| r.pull.clone()) {
                    if let (true, Ok(cur)) = (probe.exists, state::parse_cursor(&pull.cursor)) {
                        match state_machine::length_verdict(
                            kind.length_source(),
                            probe.len,
                            cur.offset,
                        ) {
                            LengthVerdict::Clean => {}
                            LengthVerdict::Truncate { to } => {
                                // Startup only: our own torn tail. Truncate,
                                // fsync the file, then resume incrementally.
                                if let Some(p) = path.as_ref() {
                                    let f =
                                        std::fs::OpenOptions::new().write(true).open(p)?;
                                    f.set_len(to)?;
                                    f.sync_all()?;
                                }
                                report.notes.push(format!(
                                    "{sid}: startup truncate to {to} (torn tail)"
                                ));
                                report.work.push(Work::Pull {
                                    sid: sid.clone(),
                                    force_reset: false,
                                });
                            }
                            LengthVerdict::Degraded => {
                                // Steady mismatch / startup short-read:
                                // local bytes cannot prove the prefix —
                                // the whole re-pull converges.
                                report.notes.push(format!(
                                    "{sid}: length mismatch → degraded → whole re-pull"
                                ));
                                report.work.push(Work::Pull {
                                    sid: sid.clone(),
                                    force_reset: true,
                                });
                                report.suspended.push(sid.clone());
                            }
                        }
                    }
                }
            }
            Action::Hold => {}
            Action::WritingHold { orphan } => {
                // Rows 2/19(B404)/22/24: the writing slot exists or must be
                // created. `orphan` marks the permanent-hold registration
                // (§4.2); an already-writing record is left alone.
                let needs_task = rec.as_ref().is_none_or(|r| r.task.is_none());
                if needs_task {
                    let file_rel = path
                        .as_ref()
                        .and_then(|p| p.strip_prefix(mirror).ok())
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or_else(|| format!("projects/lab/{sid}.jsonl"));
                    let orphan_flag = orphan;
                    store.mutate_commit(sync_dir, &sid, &mut |rec| {
                        // Stale-snapshot guard: a tombstone may have
                        // committed between `store.get` and here (a
                        // concurrent drain). Never mint a task row on a
                        // buried record — the tombstone outranks this.
                        if rec.deleted.is_some() {
                            return;
                        }
                        let now = now_rfc3339();
                        rec.task = Some(Task {
                            file: file_rel.clone(),
                            phase: state::Phase::Writing,
                            first_seen: now.clone(),
                            enqueued_at: now,
                            last_error: if orphan_flag {
                                Some("orphan: no spawn evidence".into())
                            } else {
                                None
                            },
                        });
                    })?;
                    report.committed.push(sid.clone());
                }
            }
            Action::Publish => report.work.push(Work::Publish { sid: sid.clone() }),
            // The RFC table's `task ∧ pull` row (n=6) never fires under
            // first-match order — rows 1–5 already classify every `task`
            // shape, so it emits nothing. A published task row is
            // therefore a persistent terminal fact (`publishedAt`/`pull`
            // are authoritative, and `ensure_published` treats the row
            // as terminal), not the "pre-cleanup window" the RFC table's
            // `reconcile clears the task row` tail describes.
            Action::TerminateWithTombstone | Action::Tombstone => {
                // E7 disappearance / published-then-deleted: `fileId` is
                // None because the file is absent by definition here.
                // Already-tombstoned records take the residue-only arm —
                // a second bury must NEVER recompute `fileId` (the
                // instance binding) or `publishedAtDelete` (the folded
                // publish fact) from the cleared snapshot.
                store.mutate_commit(sync_dir, &sid, &mut |rec| {
                    if rec.deleted.is_some() {
                        rec.pull = None;
                        rec.pull_pending = None;
                        rec.task = None;
                        rec.published_at = None;
                        rec.registered = None;
                        rec.materialized = None;
                    } else {
                        rec.bury(now_rfc3339(), None);
                    }
                })?;
                report.committed.push(sid.clone());
            }
            Action::SuppressAndDelete => {
                // The file should be gone but exists: re-delete. The
                // tombstone commit happened at intent read-in — this pass
                // is the §9.2 backstop between commit and actual deletion.
                if let Some(p) = path.as_ref().filter(|_| probe.exists) {
                    match std::fs::remove_file(p) {
                        Ok(()) => {
                            if let Some(dir) = p.parent() {
                                let _ = fsync_dir(dir);
                            }
                            report.notes.push(format!(
                                "{sid}: re-deleted tombstoned file {}",
                                p.display()
                            ));
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => report
                            .notes
                            .push(format!("{sid}: tombstone re-delete failed: {e}")),
                    }
                }
            }
            Action::Suspend(reason) => {
                // Every suspend verdict is human-facing — the RFC rows
                // say so explicitly: 15 (stub∧non-shell) holds the suspect
                // file for review, 17 (pull∧no file∧full) is "never
                // rebuild someone else's delete as a loss", 18 (pull∧B404)
                // is "no automatic re-publish, no local delete".
                // Queueing a forced re-pull here would self-heal row 15's
                // crash/tamper evidence and silently undo the accepted
                // evict --drop crash-window misjudgment (row 17), exactly
                // what the suspend exists to prevent. Pull-path degraded
                // sessions (no persistent slot) are NOT this arm — their
                // convergence is the Steady arm's §7.2 length predicate
                // and the HydrateReset/FirstPull rows.
                report.suspended.push(sid.clone());
                report.notes.push(format!(
                    "{sid}: suspended ({reason:?}) row {}",
                    verdict.row
                ));
            }
            Action::HydrateReset | Action::FirstPull { .. } => {
                report.work.push(Work::Pull {
                    sid: sid.clone(),
                    force_reset: true,
                })
            }
            Action::BackfillRegistered => {
                let can_register = rec.as_ref().is_none_or(|r| {
                    r.registered.is_none() && r.pull.is_none() && r.deleted.is_none()
                });
                if can_register {
                    store.mutate_commit(sync_dir, &sid, &mut |rec| {
                        // Same stale-snapshot guard: never revive fields
                        // onto a record that was tombstoned mid-pass.
                        if rec.deleted.is_some() {
                            return;
                        }
                        rec.registered = Some(state::Registered {
                            kind: "b-owned".into(),
                            at: now_rfc3339(),
                        });
                    })?;
                    report.committed.push(sid.clone());
                }
            }
        }
        // `pullPending` presence is a pull task owed — report it so the
        // pool enqueues without journal involvement (the §9.1-7 replay
        // path, T-634).
        if store
            .get(&sid)
            .is_some_and(|r| r.pull_pending.is_some())
            && !report.suspended.contains(&sid)
        {
            report.work.push(Work::Pull {
                sid: sid.clone(),
                force_reset: false,
            });
        }
        if !report.suspended.contains(&sid) && previously_suspended(&sid) {
            report.unsuspended.push(sid.clone());
        }
    }
    Ok(report)
}

// ----------------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::persist::fs::testkit::{pull_rec, tmp};
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    fn setup(name: &str) -> (PathBuf, PathBuf) {
        let mirror = tmp("reconcile", name);
        let sync = state::ensure_sync_dir(&mirror).unwrap();
        (mirror, sync)
    }

    fn write_session(mirror: &Path, sid: &str, bytes: &[u8]) -> PathBuf {
        let dir = mirror.join("projects/lab");
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join(format!("{sid}.jsonl"));
        std::fs::write(&p, bytes).unwrap();
        p
    }

    // `pull_rec` comes from `fs::testkit` — (sid, cursor) order.

    fn never_suspended(_: &str) -> bool {
        false
    }

    fn b_unknown(_: &str) -> BProbe {
        BProbe::Unknown
    }

    // T-654/E7 appearance: a fork file with `forkedFrom` and no record gets
    // a task(writing) row committed — never published.
    #[test]
    fn t654_appeared_fork_registers_writing() {
        let (mirror, sync) = setup("t654a");
        write_session(&mirror, "s2", b"{\"forkedFrom\":{\"sessionId\":\"s1\"}}\n");
        let mut st = State::empty_v3();
        let report =
            run(&mirror, &sync, &mut st, PassKind::Steady, &b_unknown, &never_suspended)
                .unwrap();
        let rec = &st.sessions["s2"];
        assert_eq!(rec.task.as_ref().unwrap().phase, state::Phase::Writing);
        assert!(!report.work.iter().any(|w| matches!(w, Work::Publish { .. })));
        assert!(report.committed.contains(&"s2".to_string()));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-654/E7 disappearance: task row whose file vanished → terminate +
    // tombstone in one commit.
    #[test]
    fn t654_disappeared_task_tombstones() {
        let (mirror, sync) = setup("t654b");
        let mut st = State::empty_v3();
        st.sessions.insert(
            "s2".into(),
            SessionRecord {
                task: Some(Task {
                    file: "projects/lab/s2.jsonl".into(),
                    phase: state::Phase::Validated,
                    first_seen: "t".into(),
                    enqueued_at: "t".into(),
                    last_error: None,
                }),
                ..Default::default()
            },
        );
        let report =
            run(&mirror, &sync, &mut st, PassKind::Steady, &b_unknown, &never_suspended)
                .unwrap();
        let rec = &st.sessions["s2"];
        assert!(rec.deleted.is_some(), "task+no file → tombstone");
        assert!(rec.task.is_none() && rec.published_at.is_none());
        assert_eq!(rec.deleted.as_ref().unwrap().file_id, None);
        assert!(report.suspended.is_empty());
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-635 rule order: tombstone committed, file still present → the
    // pass re-deletes it instead of judging "B lost it".
    #[test]
    fn t635_tombstone_filter_runs_first_and_redeletes() {
        let (mirror, sync) = setup("t635");
        let bytes = b"{\"x\":1}\n";
        let file = write_session(&mirror, "s7", bytes);
        let mut st = State::empty_v3();
        let mut rec = pull_rec("s7", "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        rec.bury("t", Some(state::file_id_of(bytes)));
        st.sessions.insert("s7".into(), rec);
        let report = run(
            &mirror,
            &sync,
            &mut st,
            PassKind::Steady,
            &|_: &str| BProbe::Absent,
            &never_suspended,
        )
        .unwrap();
        assert!(!file.exists(), "re-delete is the reconcile backstop");
        assert!(
            report.notes.iter().any(|n| n.contains("re-deleted")),
            "notes: {:?}",
            report.notes
        );
        // The suspended set must NOT contain s7: tombstone hit, not B-loss.
        assert!(!report.suspended.contains(&"s7".to_string()));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-634 in-flight recovery surface: `task{validated}` re-enters the
    // worker; `pullPending` re-enqueues as pull work; `writing` holds.
    #[test]
    fn t634_inflight_recovery_requeues() {
        let (mirror, sync) = setup("t634");
        write_session(&mirror, "pub", b"{\"forkedFrom\":{\"sessionId\":\"x\"}}\n");
        write_session(&mirror, "w", b"{\"forkedFrom\":{\"sessionId\":\"x\"}}\n");
        let mut st = State::empty_v3();
        st.sessions.insert(
            "pub".into(),
            SessionRecord {
                task: Some(Task {
                    file: "projects/lab/pub.jsonl".into(),
                    phase: state::Phase::Validated,
                    first_seen: "t".into(),
                    enqueued_at: "t".into(),
                    last_error: None,
                }),
                ..Default::default()
            },
        );
        st.sessions.insert(
            "pp".into(),
            SessionRecord {
                pull_pending: Some(state::PullPending {
                    target: "latest".into(),
                    enqueued_at: "t".into(),
                    record: None,
                }),
                ..Default::default()
            },
        );
        st.sessions.insert(
            "w".into(),
            SessionRecord {
                task: Some(Task {
                    file: "projects/lab/w.jsonl".into(),
                    phase: state::Phase::Writing,
                    first_seen: "t".into(),
                    enqueued_at: "t".into(),
                    last_error: None,
                }),
                ..Default::default()
            },
        );
        let report =
            run(&mirror, &sync, &mut st, PassKind::Steady, &b_unknown, &never_suspended)
                .unwrap();
        assert!(report.work.contains(&Work::Publish { sid: "pub".into() }));
        assert!(report.work.contains(&Work::Pull {
            sid: "pp".into(),
            force_reset: false
        }));
        // writing: still holds — no publish work for it.
        assert!(!report
            .work
            .iter()
            .any(|w| matches!(w, Work::Publish { sid } if sid == "w")));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // §7.2 partition inside the pass: startup `len > bytes` truncates to
    // the cursor and enqueues a normal pull; steady any-mismatch queues a
    // forced whole re-pull and suspends.
    #[test]
    fn length_predicate_partitions_by_pass_kind() {
        let (mirror, sync) = setup("lenpart");
        // Cursor covers 7 bytes; file has 12 — startup truncates.
        let file = write_session(&mirror, "s1", b"{'a':1}\nEXTR");
        let mut st = State::empty_v3();
        st.sessions.insert(
            "s1".into(),
            pull_rec("s1", "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        );
        let report = run(
            &mirror,
            &sync,
            &mut st,
            PassKind::Startup,
            &b_unknown,
            &never_suspended,
        )
        .unwrap();
        assert_eq!(
            std::fs::metadata(&file).unwrap().len(),
            7,
            "truncated to cursor bytes"
        );
        assert!(report.work.contains(&Work::Pull {
            sid: "s1".into(),
            force_reset: false
        }));
        assert!(report.suspended.is_empty());

        // Same shape at steady state → degrade + forced re-pull. The
        // startup pass truncated the file on disk — regrow it so the
        // steady pass sees the same 12-vs-7 mismatch.
        write_session(&mirror, "s1", b"{'a':1}\nEXTR");
        let mut st2 = State::empty_v3();
        st2.sessions.insert(
            "s1".into(),
            pull_rec("s1", "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        );
        let report = run(
            &mirror,
            &sync,
            &mut st2,
            PassKind::Steady,
            &b_unknown,
            &never_suspended,
        )
        .unwrap();
        assert!(report.suspended.contains(&"s1".to_string()));
        assert!(report.work.contains(&Work::Pull {
            sid: "s1".into(),
            force_reset: true
        }));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // B-lost session (pull ∧ B 404): row-18 suspend is human-facing —
    // suspend-for-human, NO healing pull queues (no rebuild, no re-push,
    // no local delete); the
    // file stays for review. Same for the materialization-evidence
    // suspensions: row 15 (stub ∧ non-shell) and the accepted evict
    // --drop crash-window misjudgment (row 17, pull ∧ no file ∧ full)
    // must hold evidence instead of self-healing it away.
    #[test]
    fn suspend_verdicts_never_queue_healing_pulls() {
        let (mirror, sync) = setup("susp-norepull");
        let file = write_session(&mirror, "s9", b"{\"a\":1}\n");
        // Row 15 evidence shape: stub record + non-shell file.
        write_session(&mirror, "s15", b"{\"real\":1}\n");
        let mut st = State::empty_v3();
        st.sessions.insert(
            "s9".into(),
            pull_rec("s9", "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        );
        st.sessions.insert(
            "s15".into(),
            SessionRecord {
                materialized: Some(state::Materialized::Stub),
                ..pull_rec("s15", "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            },
        );
        st.sessions.insert(
            "s17".into(),
            pull_rec("s17", "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        );
        let report = run(
            &mirror,
            &sync,
            &mut st,
            PassKind::Steady,
            &|_: &str| BProbe::Absent,
            &never_suspended,
        )
        .unwrap();
        for sid in ["s9", "s15", "s17"] {
            assert!(report.suspended.contains(&sid.to_string()), "{sid} suspended");
            assert!(
                !report.work.iter().any(|w| matches!(w, Work::Pull { sid: s, .. } if s == sid)),
                "{sid} must not get a healing pull — suspend evidence is held"
            );
        }
        assert!(file.exists(), "reconcile never deletes on B-loss");
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Orphans: hand-dropped non-fork file with B 404 → task(writing) +
    // permanent hold (§4.2); registered ∧ file ∧ B 404 → same orphan row.
    #[test]
    fn orphan_rows_register_writing_permanent_hold() {
        let (mirror, sync) = setup("orph");
        write_session(&mirror, "orph", b"{\"a\":1}\n");
        write_session(&mirror, "regd", b"{\"a\":1}\n");
        let mut st = State::empty_v3();
        st.sessions.insert(
            "regd".into(),
            SessionRecord {
                registered: Some(state::Registered {
                    kind: "b-owned".into(),
                    at: "t".into(),
                }),
                ..Default::default()
            },
        );
        let report = run(
            &mirror,
            &sync,
            &mut st,
            PassKind::Steady,
            &|_: &str| BProbe::Absent,
            &never_suspended,
        )
        .unwrap();
        for sid in ["orph", "regd"] {
            let t = st.sessions[sid].task.as_ref().unwrap();
            assert_eq!(t.phase, state::Phase::Writing, "{sid}");
            assert_eq!(
                t.last_error.as_deref(),
                Some("orphan: no spawn evidence"),
                "{sid}"
            );
        }
        assert!(report.committed.contains(&"orph".to_string()));
        assert!(report.committed.contains(&"regd".to_string()));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // No record + B has the session → backfill `registered` (row 23): the
    // sid universe is mirror ∪ records, so this needs a record-less sid —
    // plant a pullPending-only record? No: pullPending IS a record field,
    // row 23 requires `no record`. A sid reachable only via journal read-in
    // is covered; here drive it through a scan-hit sid whose record is
    // truly absent... row 23's `rec.is_none()` needs the sid in the union
    // from state — an all-empty record (present key, no fields) is
    // `rec.is_some` not None, so the honest driver is a state entry? No —
    // row 23 says `no record`: the sid comes from the MIRROR scan? It has
    // no file then.
    // The reachable construction: sid in records with only pullPending →
    // that's `no_record_fields()==false`. Row 23 is for sids discovered
    // through B-side state the local table lacks — via scan: impossible
    // (scan needs a file). Direct `store.get` returning None happens when
    // the sid enters the union via... it can't with this union. Row 23's
    // trigger is `rec.is_none()` inside an iterated sid — only reachable
    // if the store yields the sid without a record: our SessionStore
    // always yields a record for its keys, but scan adds sids WITHOUT
    // records — which then have a file (scan found it) → row 22/24, not
    // 23. Row 23 is still exercised in classify's unit tests; here cover
    // BackfillRegistered via a store double below instead.
    #[test]
    fn backfill_registered_via_store_double() {
        struct FakeStore(std::collections::BTreeSet<String>);
        impl SessionStore for FakeStore {
            fn sids(&self) -> std::collections::BTreeSet<String> {
                self.0.clone()
            }
            fn get(&self, _: &str) -> Option<SessionRecord> {
                None
            }
            fn mutate_commit(
                &mut self,
                _: &Path,
                _: &str,
                _: &mut dyn FnMut(&mut SessionRecord),
            ) -> Result<()> {
                Ok(())
            }
        }
        let (mirror, sync) = setup("backfill");
        let mut store = FakeStore(["bonly".to_string()].into_iter().collect());
        let report = run(
            &mirror,
            &sync,
            &mut store,
            PassKind::Steady,
            &|_: &str| BProbe::Present,
            &never_suspended,
        )
        .unwrap();
        assert!(report
            .verdicts
            .iter()
            .any(|(s, v)| s == "bonly" && v.row == 23));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // published ∧ file gone ∧ full → tombstone {publishedAtDelete:true}.
    #[test]
    fn published_deleted_lands_tombstone() {
        let (mirror, sync) = setup("pubdel");
        let mut st = State::empty_v3();
        st.sessions.insert(
            "s1".into(),
            SessionRecord {
                pull: Some(state::Pull {
                    cursor: "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                    file: "projects/lab/s1.jsonl".into(),
                }),
                published_at: Some("t".into()),
                ..Default::default()
            },
        );
        run(&mirror, &sync, &mut st, PassKind::Steady, &b_unknown, &never_suspended)
            .unwrap();
        let d = st.sessions["s1"].deleted.as_ref().unwrap();
        assert!(d.published_at_delete);
        assert_eq!(d.file_id, None);
        assert!(st.sessions["s1"].pull.is_none());
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // R2-I3: a file that exists but cannot be read is `unreadable`, not
    // absent — the pass SKIPS the sid instead of letting absence edges
    // (tombstone / terminate / suspend) fire on a transient IO failure.
    #[test]
    fn unreadable_file_skips_the_sid() {
        let (mirror, sync) = setup("unreadable");
        let file = write_session(&mirror, "s1", b"{\"a\":1}\n");
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o000)).unwrap();
        let mut st = State::empty_v3();
        st.sessions.insert(
            "s1".into(),
            SessionRecord {
                task: Some(Task {
                    file: "projects/lab/s1.jsonl".into(),
                    phase: state::Phase::Validated,
                    first_seen: "t".into(),
                    enqueued_at: "t".into(),
                    last_error: None,
                }),
                ..Default::default()
            },
        );
        let (probe, _) = probe_session_file(&mirror, Some("projects/lab/s1.jsonl"), "s1");
        let report = run(&mirror, &sync, &mut st, PassKind::Steady, &b_unknown, &never_suspended)
            .unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        if probe.unreadable {
            // The destructive check: no tombstone, no task change.
            assert!(st.sessions["s1"].deleted.is_none(), "unreadable must not tombstone");
            assert!(st.sessions["s1"].task.is_some());
            assert!(report.notes.iter().any(|n| n.contains("unreadable")));
        } else {
            // Environment could not produce an unreadable file (e.g.
            // running as root) — the probe must still answer the old way.
            assert!(probe.exists);
        }
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // `materialized` exemption (§6): pull ∧ unloaded ∧ no file is the
    // normal pruned state — steady, no suspension, no tombstone.
    #[test]
    fn pruned_unloaded_is_steady() {
        let (mirror, sync) = setup("pruned");
        let mut st = State::empty_v3();
        let mut rec = pull_rec("s5", "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        rec.materialized = Some(state::Materialized::Unloaded);
        st.sessions.insert("s5".into(), rec);
        let report =
            run(&mirror, &sync, &mut st, PassKind::Steady, &b_unknown, &never_suspended)
                .unwrap();
        assert!(report.suspended.is_empty());
        assert!(report.work.is_empty());
        assert!(st.sessions["s5"].deleted.is_none());
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // `tombstone_file_id`: absent/stub/unloaded → None; present+full+real
    // content → the fingerprint; sentinel shell → None even under full.
    #[test]
    fn tombstone_file_id_binds_instance_or_null() {
        let (mirror, _sync) = setup("fid");
        let bytes = b"{\"a\":1}\n";
        write_session(&mirror, "s", bytes);
        let probe = FileProbe::from_bytes(bytes);
        let full = pull_rec("s", "zpc1.7.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(
            tombstone_file_id(Some(&full), &probe).as_deref(),
            Some(state::file_id_of(bytes).as_str())
        );
        // File absent → None (sealed).
        assert_eq!(
            tombstone_file_id(Some(&full), &FileProbe::absent()),
            None
        );
        // Unloaded record → None even though a file exists.
        let mut u = full.clone();
        u.materialized = Some(state::Materialized::Unloaded);
        assert_eq!(tombstone_file_id(Some(&u), &probe), None);
        // Stub shell under a full record → still None (sentinel ≠ content).
        let shell = FileProbe::from_bytes(b"{\"type\":\"zonepair-stub\"}\n");
        assert_eq!(tombstone_file_id(Some(&full), &shell), None);
        std::fs::remove_dir_all(&mirror).unwrap();
    }
}
