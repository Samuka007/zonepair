//! prune: the materialization command surface (RFC-0003-zonepaird §6.4, p2 spec
//! §3.2). Two shapes share one argv:
//!
//! - `prune <sid> --drop|--stub` — one `evict{sid,mode}` intent.
//! - `prune --older-than <dur>` / `prune --all-stale[=dur]` — fan out one
//!   `evict` per session whose `lastUnchangedAt` is older than the
//!   threshold (30d default for `--all-stale`; batch mode defaults to
//!   `--drop`, the conservative choice).
//!
//! Short-lived process rule: this command NEVER touches mirror files or
//! `state.json` writes — predicates evaluate against a read-only decode
//! (`read_state_ro`) and intents land through `enqueue` (`ensure_owner` +
//! the five-step journal append). zonepaird is the sole writer that
//! materializes.

use super::transport::read_state_ro;
use super::zonepaird::enqueue;
use super::{parse_cli, resolve_mirror_dir};
use crate::sync::persist::journal::{self, EvictMode, Intent};
use crate::sync::persist::state::{self, State};
use crate::sync::persist::time::unix_ms;
use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(clap::Parser)]
#[command(name = "prune")]
struct PruneCli {
    /// Session id to evict (requires --drop or --stub)
    sid: Option<String>,
    /// Mirror directory override (default: flag > env > config > builtin)
    #[arg(long)]
    mirror: Option<String>,
    /// Evict by deleting the file; the record stays `unloaded`
    #[arg(long = "drop", conflicts_with = "stub_mode")]
    drop_mode: bool,
    /// Evict by replacing bytes with a stub shell; the record becomes `stub`
    #[arg(long = "stub", conflicts_with = "drop_mode")]
    stub_mode: bool,
    /// Fan out: evict every non-tombstoned session whose lastUnchangedAt is
    /// older than DUR (e.g. 30d, 12h, 90m, 3600s)
    #[arg(long, value_name = "DUR", conflicts_with_all = ["sid", "all_stale"])]
    older_than: Option<String>,
    /// Fan out: evict every session past the stale threshold — 30d by
    /// default, override inline as --all-stale=90d
    #[arg(
        long,
        value_name = "DUR",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "30d",
        conflicts_with_all = ["sid", "older_than"]
    )]
    all_stale: Option<String>,
}

pub(crate) fn prune_cli(args: &[String]) -> Result<()> {
    let cli: PruneCli = parse_cli("prune", args);
    run(&cli)
}

fn run(cli: &PruneCli) -> Result<()> {
    let mirror = PathBuf::from(resolve_mirror_dir(cli.mirror.clone()));
    if let Some(sid) = &cli.sid {
        // Single-sid shape: a mode flag is mandatory — bare `prune <sid>`
        // is a loud failure, never a guessed default (RFC-0003-zonepaird §6.4).
        let mode = single_mode(cli)?;
        // M-6: advisory typo guard — an unknown sid may be legitimately
        // new (pre-register) or simply mistyped; warn on stderr but still
        // enqueue (the spec makes existence advisory, never a gate).
        if !read_state_ro(&state::sync_dir(&mirror))
            .sessions
            .contains_key(sid)
        {
            eprintln!("stub-client prune: no session {sid:?} in state.json — enqueuing anyway");
        }
        return enqueue(
            &mirror,
            Intent::Evict {
                sid: sid.clone(),
                mode,
                enqueued_at: state::now_rfc3339(),
            },
        )
        .with_context(|| format!("enqueuing evict{{{sid}}}"))
        .map(|_| ());
    }
    let Some(dur_src) = cli.older_than.as_deref().or(cli.all_stale.as_deref()) else {
        bail!(
            "prune needs a target: `prune <sid> --drop|--stub`, \
             `prune --older-than <dur>`, or `prune --all-stale`"
        );
    };
    let dur = parse_dur(dur_src).with_context(|| format!("invalid duration {dur_src:?}"))?;
    // Batch default is --drop: reclaiming bytes outright is the
    // conservative choice; --stub is opt-in per invocation.
    let mode = if cli.stub_mode {
        EvictMode::Stub
    } else {
        EvictMode::Drop
    };
    fan_out(&mirror, dur, mode)
}

/// The single-sid mode resolution: clap's `conflicts_with` already caps
/// the pair at one, so only "neither" can fail here.
fn single_mode(cli: &PruneCli) -> Result<EvictMode> {
    match (cli.drop_mode, cli.stub_mode) {
        (true, false) => Ok(EvictMode::Drop),
        (false, true) => Ok(EvictMode::Stub),
        _ => bail!("prune <sid> requires exactly one of --drop or --stub"),
    }
}

/// Batch fan-out: evaluate the stale predicate against a READ-ONLY
/// `state.json` decode, pre-check journal capacity, then append one
/// `evict` per hit. Sessions without a parseable `lastUnchangedAt`,
/// tombstoned sessions, `task`-only records and already-pruned
/// (`stub`/`unloaded`) records never match — emitting those intents
/// would only burn journal slots or fabricate unspecified transitions.
fn fan_out(mirror: &Path, older_than: Duration, mode: EvictMode) -> Result<()> {
    let sync_dir = state::sync_dir(mirror);
    let st = read_state_ro(&sync_dir);
    let now = (unix_ms() / 1000) as i64;
    let sids = stale_sids(&st, older_than.as_secs() as i64, now);
    if sids.is_empty() {
        println!("prune: no sessions match the stale predicate");
        return Ok(());
    }
    // §4.2-2 pre-check: refuse the WHOLE fan-out before the first append
    // rather than failing halfway through a partial batch. The append's
    // own check still guards the race where the backlog grows between
    // here and the last intent.
    let journal_dir = sync_dir.join("journal");
    let pending = journal::pending_count(&journal_dir);
    if pending + sids.len() > journal::MAX_PENDING {
        bail!(
            "prune: {} session(s) match but the journal holds {pending} \
             unconsumed record(s) — backlog limit is {} (is zonepaird running?)",
            sids.len(),
            journal::MAX_PENDING
        );
    }
    let enqueued_at = state::now_rfc3339();
    for sid in &sids {
        enqueue(
            mirror,
            Intent::Evict {
                sid: sid.clone(),
                mode,
                enqueued_at: enqueued_at.clone(),
            },
        )
        .with_context(|| format!("enqueuing evict{{{sid}}}"))?;
    }
    println!(
        "prune: enqueued {} evict intent(s) (mode {mode:?}, lastUnchangedAt older than {older_than:?})",
        sids.len()
    );
    Ok(())
}

/// The stale predicate, pure for tests: `lastUnchangedAt` strictly older
/// than `older_than_secs` relative to `now_unix`. BTreeMap iteration
/// keeps the hit set in sid order.
fn stale_sids(state: &State, older_than_secs: i64, now_unix: i64) -> Vec<String> {
    state
        .sessions
        .iter()
        .filter(|(_, rec)| {
            // M-2: only sessions whose bytes actually live in the mirror
            // are evict candidates — a `task`-only record (no `pull`) has
            // nothing to reclaim, and a non-`full` record under `--stub`
            // would fabricate a shell over empty/absent bytes (the
            // unspecified unloaded→stub transition). Tombstones were
            // already excluded; this adds the pull+full half.
            rec.deleted.is_none()
                && rec.pull.is_some()
                && rec.materialization().is_full()
                && rec
                    .last_unchanged_at
                    .as_deref()
                    .and_then(parse_ts)
                    .is_some_and(|t| now_unix - t > older_than_secs)
        })
        .map(|(sid, _)| sid.clone())
        .collect()
}

/// `<num><unit>` durations: `30d`, `12h`, `90m`, `3600s` (unit letters
/// s/m/h/d, case-insensitive). Single-component only — compound forms
/// like `1h30m` are rejected with a pointer at the accepted shapes.
fn parse_dur(s: &str) -> Result<Duration> {
    let s = s.trim();
    let split = s
        .find(|c: char| c.is_ascii_alphabetic())
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num
        .parse()
        .with_context(|| format!("duration {s:?} lacks a leading number"))?;
    let secs = match unit.to_ascii_lowercase().as_str() {
        "s" => Some(n),
        "m" => n.checked_mul(60),
        "h" => n.checked_mul(3600),
        "d" => n.checked_mul(86_400),
        _ => bail!("duration {s:?}: unit must be s, m, h or d (e.g. 30d, 12h, 90m, 3600s)"),
    }
    .context("duration overflows u64 seconds")?;
    Ok(Duration::from_secs(secs))
}

/// RFC3339 → unix seconds, covering the `YYYY-MM-DDTHH:MM:SS[.frac](Z|±HH:MM)`
/// shapes `state::now_rfc3339` emits plus the common offset spellings.
/// Returns `None` on anything outside that grammar — an unparseable
/// `lastUnchangedAt` simply never counts as stale.
fn parse_ts(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || (b[10] != b'T' && b[10] != b't')
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let dig = |i: usize| -> Option<i64> {
        b.get(i).filter(|c| c.is_ascii_digit()).map(|c| (c - b'0') as i64)
    };
    let field = |start: usize, len: usize| -> Option<i64> {
        (0..len).try_fold(0i64, |acc, k| Some(acc * 10 + dig(start + k)?))
    };
    let year = field(0, 4)?;
    let month = field(5, 2)?;
    let day = field(8, 2)?;
    let hour = field(11, 2)?;
    let min = field(14, 2)?;
    let sec = field(17, 2)?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || min > 59
        || sec > 60
    {
        return None;
    }
    // days-from-civil (Hinnant), inverse of sync::time's civil_from_days.
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let mut secs = days * 86_400 + hour * 3600 + min * 60 + sec;

    // Zone designator after SS: optional `.fff`, then `Z`/`z` or `±HH[:MM]`.
    let mut rest = &s[19..];
    if let Some(frac) = rest.strip_prefix('.') {
        let end = frac
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(frac.len());
        rest = &frac[end..];
    }
    match rest {
        "" | "Z" | "z" => {}
        _ => {
            let (sign, t) = match rest.as_bytes().first()? {
                b'-' => (-1i64, &rest[1..]),
                b'+' => (1i64, &rest[1..]),
                _ => return None,
            };
            let (oh, om) = if t.len() == 5 && t.as_bytes()[2] == b':' {
                (t[0..2].parse::<i64>().ok()?, t[3..5].parse::<i64>().ok()?)
            } else if t.len() == 4 {
                (t[0..2].parse::<i64>().ok()?, t[2..4].parse::<i64>().ok()?)
            } else {
                return None;
            };
            secs -= sign * (oh * 3600 + om * 60);
        }
    }
    Some(secs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::testkit::{hold_owner, journal_intents};
    use crate::cli::try_parse_cli;
    use crate::sync::persist::fs::testkit::tmp;
    use crate::sync::persist::journal;
    use crate::sync::persist::state::{Materialized, SessionRecord};

    fn s(v: &str) -> String {
        v.to_string()
    }

    fn mirror_arg(mirror: &Path) -> String {
        mirror.display().to_string()
    }

    // T-14 shape pins: flag=value gain, conflicts, optional --all-stale
    // value, junk rejection.
    #[test]
    fn t14_prune_parse_shapes() {
        let cli = try_parse_cli::<PruneCli>(
            "prune",
            &[s("s9"), s("--stub"), s("--mirror=/tmp/m")],
        )
        .unwrap();
        assert_eq!(cli.sid.as_deref(), Some("s9"));
        assert!(cli.stub_mode && !cli.drop_mode);
        assert_eq!(cli.mirror.as_deref(), Some("/tmp/m"));

        let cli = try_parse_cli::<PruneCli>("prune", &[s("--older-than=30d")]).unwrap();
        assert_eq!(cli.older_than.as_deref(), Some("30d"));
        assert!(cli.sid.is_none());

        // Bare --all-stale defaults to the 30d threshold spelling.
        let cli = try_parse_cli::<PruneCli>("prune", &[s("--all-stale")]).unwrap();
        assert_eq!(cli.all_stale.as_deref(), Some("30d"));
        // Inline override requires `=`.
        let cli = try_parse_cli::<PruneCli>("prune", &[s("--all-stale=90d")]).unwrap();
        assert_eq!(cli.all_stale.as_deref(), Some("90d"));

        // Conflicts and junk are clap errors.
        assert!(try_parse_cli::<PruneCli>(
            "prune",
            &[s("s9"), s("--drop"), s("--stub")]
        )
        .is_err());
        assert!(try_parse_cli::<PruneCli>(
            "prune",
            &[s("s9"), s("--older-than"), s("1d")]
        )
        .is_err());
        assert!(try_parse_cli::<PruneCli>(
            "prune",
            &[s("--older-than"), s("1d"), s("--all-stale")]
        )
        .is_err());
        // A single positional IS a valid sid — the mode requirement is a
        // run-time check (see t806_bare_sid_requires_mode), not clap's.
        assert!(try_parse_cli::<PruneCli>("prune", &[s("s9")]).is_ok());
        assert!(try_parse_cli::<PruneCli>("prune", &[s("s1"), s("extra")]).is_err());
    }

    // T-806: bare `prune <sid>` (no mode flag) is a loud failure — the
    // command never guesses a materialization mode.
    #[test]
    fn t806_bare_sid_requires_mode() {
        let cli = try_parse_cli::<PruneCli>("prune", &[s("s9")]).unwrap();
        let err = run(&cli).unwrap_err();
        assert!(err.to_string().contains("--drop"), "{err:#}");
        // No target at all fails too.
        let cli = try_parse_cli::<PruneCli>("prune", &[]).unwrap();
        assert!(run(&cli).is_err());
        // Mode flags without a target or predicate fail the same way.
        let cli = try_parse_cli::<PruneCli>("prune", &[s("--drop")]).unwrap();
        assert!(run(&cli).is_err());
    }

    // T-806: `prune s9 --stub` / `--drop` land `evict{sid,mode}`.
    #[test]
    fn t806_single_evict_modes_land() {
        for (flag, want) in [("--stub", EvictMode::Stub), ("--drop", EvictMode::Drop)] {
            let mirror = tmp("prunecli", &format!("t806-{flag}"));
            let _owner = hold_owner(&mirror);
            let cli = try_parse_cli::<PruneCli>(
                "prune",
                &[s("s9"), s(flag), s("--mirror"), mirror_arg(&mirror)],
            )
            .unwrap();
            run(&cli).unwrap();
            let intents = journal_intents(&mirror);
            assert_eq!(intents.len(), 1, "{flag}");
            assert!(
                matches!(&intents[0], Intent::Evict { sid, mode, .. }
                    if sid == "s9" && *mode == want),
                "{flag}: {intents:?}"
            );
            std::fs::remove_dir_all(&mirror).unwrap();
        }
    }

    /// A pulled, `full`-materialized record — the only shape the M-2
    /// predicate admits.
    fn pulled_rec(sid: &str, last: Option<&str>) -> SessionRecord {
        SessionRecord {
            pull: Some(state::Pull {
                cursor: "zpc1.9.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                file: format!("projects/lab/{sid}.jsonl"),
            }),
            last_unchanged_at: last.map(str::to_string),
            ..SessionRecord::default()
        }
    }

    // T-806: `--older-than`/`--all-stale` predicate — only sessions whose
    // `lastUnchangedAt` exceeds the threshold get an intent; tombstones
    // and missing stamps never match.
    #[test]
    fn t806_batch_predicate_fans_out() {
        let mirror = tmp("prunecli", "t806-batch");
        let _owner = hold_owner(&mirror);
        let sync_dir = state::ensure_sync_dir(&mirror).unwrap();
        let mut st = State::empty_v3();
        let old = "2020-01-01T00:00:00Z";
        let fresh = state::now_rfc3339();
        // Matches: pulled + full + ancient stamp.
        st.sessions.insert("old".into(), pulled_rec("old", Some(old)));
        // Fresh stamp — inside the 1h window.
        st.sessions.insert("fresh".into(), pulled_rec("fresh", Some(&fresh)));
        // Missing / unparseable stamps never match.
        st.sessions.insert("noStamp".into(), pulled_rec("noStamp", None));
        st.sessions.insert("badStamp".into(), pulled_rec("badStamp", Some("not-a-date")));
        // M-2 exclusions, all with ancient stamps:
        // (a) task-only record — no `pull`, nothing to reclaim;
        st.sessions.insert(
            "taskOnly".into(),
            SessionRecord {
                task: Some(state::Task {
                    file: "projects/lab/taskOnly.jsonl".into(),
                    phase: state::Phase::Published,
                    first_seen: "t".into(),
                    enqueued_at: "t".into(),
                    last_error: None,
                }),
                last_unchanged_at: Some(old.into()),
                ..SessionRecord::default()
            },
        );
        // (b) already-pruned records — `unloaded`/`stub` are not `full`;
        // re-evicting them is a wasted slot (and `--stub` on `unloaded`
        // is the unspecified shell-over-empty-bytes transition).
        for (sid, mat) in [
            ("unloaded", Materialized::Unloaded),
            ("stubbed", Materialized::Stub),
        ] {
            let mut rec = pulled_rec(sid, Some(old));
            rec.materialized = Some(mat);
            st.sessions.insert(sid.into(), rec);
        }
        // Tombstoned session with an ancient stamp must NOT match.
        let mut tomb = pulled_rec("tomb", Some(old));
        tomb.bury("t", None);
        st.sessions.insert("tomb".into(), tomb);
        state::commit(&sync_dir, &st).unwrap();

        let cli = try_parse_cli::<PruneCli>(
            "prune",
            &[
                s("--older-than"),
                s("1h"),
                s("--mirror"),
                mirror_arg(&mirror),
            ],
        )
        .unwrap();
        run(&cli).unwrap();
        let intents = journal_intents(&mirror);
        let sids: Vec<&str> = intents
            .iter()
            .map(|i| match i {
                Intent::Evict { sid, .. } => sid.as_str(),
                other => panic!("unexpected intent {other:?}"),
            })
            .collect();
        assert_eq!(sids, ["old"], "only the pulled+full+stale sid matches");
        assert!(
            intents
                .iter()
                .all(|i| matches!(i, Intent::Evict { mode: EvictMode::Drop, .. })),
            "batch defaults to --drop"
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-806: `--all-stale` bare = 30d threshold; `--stub` still applies.
    #[test]
    fn t806_all_stale_threshold() {
        let mirror = tmp("prunecli", "t806-stale");
        let _owner = hold_owner(&mirror);
        let sync_dir = state::ensure_sync_dir(&mirror).unwrap();
        let mut st = State::empty_v3();
        st.sessions.insert(
            "ancient".into(),
            pulled_rec("ancient", Some("2020-06-01T00:00:00Z")),
        );
        state::commit(&sync_dir, &st).unwrap();

        let cli = try_parse_cli::<PruneCli>(
            "prune",
            &[s("--all-stale"), s("--stub"), s("--mirror"), mirror_arg(&mirror)],
        )
        .unwrap();
        run(&cli).unwrap();
        let intents = journal_intents(&mirror);
        assert_eq!(intents.len(), 1);
        assert!(matches!(
            &intents[0],
            Intent::Evict { sid, mode: EvictMode::Stub, .. } if sid == "ancient"
        ));
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // T-806: journal ≥4096 pre-check — a batch that would overflow the
    // backlog fails loudly BEFORE the first append.
    #[test]
    fn t806_backlog_precheck_fails_before_append() {
        let mirror = tmp("prunecli", "t806-full");
        let _owner = hold_owner(&mirror);
        let sync_dir = state::ensure_sync_dir(&mirror).unwrap();
        let journal_dir = sync_dir.join("journal");
        state::ensure_dir(&journal_dir, 0o700).unwrap();
        let mut st = State::empty_v3();
        st.sessions.insert(
            "old".into(),
            pulled_rec("old", Some("2020-01-01T00:00:00Z")),
        );
        state::commit(&sync_dir, &st).unwrap();
        for i in 0..journal::MAX_PENDING {
            std::fs::write(
                journal_dir.join(format!("{:013}-1-1.json", i as u64)),
                r#"{"op":"reconcile","scope":"all","enqueuedAt":"t"}"#,
            )
            .unwrap();
        }
        let before = journal::pending_count(&journal_dir);
        assert_eq!(before, journal::MAX_PENDING);

        let cli = try_parse_cli::<PruneCli>(
            "prune",
            &[s("--all-stale"), s("--mirror"), mirror_arg(&mirror)],
        )
        .unwrap();
        let err = run(&cli).unwrap_err();
        assert!(err.to_string().contains("backlog"), "{err:#}");
        // Nothing was appended — the pre-check refused the whole batch.
        assert_eq!(journal::pending_count(&journal_dir), before);
        std::fs::remove_dir_all(&mirror).unwrap();
    }

    // Pure-function pins: duration grammar, timestamp parsing, predicate.
    #[test]
    fn t806_parse_dur_and_ts() {
        assert_eq!(parse_dur("30d").unwrap(), Duration::from_secs(30 * 86_400));
        assert_eq!(parse_dur("12h").unwrap(), Duration::from_secs(12 * 3600));
        assert_eq!(parse_dur("90m").unwrap(), Duration::from_secs(90 * 60));
        assert_eq!(parse_dur("3600s").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_dur("2D").unwrap(), Duration::from_secs(2 * 86_400));
        for bad in ["", "d", "10x", "1h30m", "-5d", "abc"] {
            assert!(parse_dur(bad).is_err(), "{bad:?} must fail");
        }

        // now_rfc3339's own output round-trips.
        let now = state::now_rfc3339();
        let t = parse_ts(&now).expect("now_rfc3339 output must parse");
        assert!(((unix_ms() / 1000) as i64 - t).abs() < 5);
        // Fixed instant: 2020-01-01T00:00:00Z = 1577836800.
        assert_eq!(parse_ts("2020-01-01T00:00:00Z"), Some(1_577_836_800));
        assert_eq!(parse_ts("2020-01-01T08:00:00+08:00"), Some(1_577_836_800));
        assert_eq!(parse_ts("garbage"), None);

        // Predicate: strictly-older-than; missing/unparseable/tombstone
        // never stale.
        let mut st = State::empty_v3();
        st.sessions.insert(
            "a".into(),
            pulled_rec("a", Some("2020-01-01T00:00:00Z")),
        );
        st.sessions.insert("b".into(), pulled_rec("b", None));
        st.sessions.insert(
            "c".into(),
            pulled_rec("c", Some("2020-01-01T00:00:00Z")),
        );
        st.sessions.get_mut("c").unwrap().deleted = Some(state::Deleted {
            at: "t".into(),
            published_at_delete: false,
            file_id: None,
        });
        let hits = stale_sids(&st, 3600, 1_600_000_000);
        assert_eq!(hits, ["a"]);
    }
}
