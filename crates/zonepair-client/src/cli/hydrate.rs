//! hydrate: pull a session's bytes back to `full` (RFC-0003-zonepaird §6.4).
//! One `hydrate{sid}` journal intent — the worker runs the §4.4 reset
//! path; this process writes nothing but the intent (the same rule
//! `prune`/`sync`/`sess delete` live by: CLI processes have zero write
//! rights over mirror files and `state.json`).

use super::transport::read_state_ro;
use super::zonepaird::enqueue;
use super::{parse_cli, resolve_mirror_dir};
use crate::sync::persist::journal::Intent;
use crate::sync::state;
use anyhow::{Context, Result};
use std::path::PathBuf;

#[derive(clap::Parser)]
#[command(name = "hydrate")]
struct HydrateCli {
    /// session id to materialize back to `full`
    sid: String,
    /// Mirror directory override (default: flag > env > config > builtin)
    #[arg(long)]
    mirror: Option<String>,
}

pub(crate) fn hydrate_cli(args: &[String]) -> Result<()> {
    let cli: HydrateCli = parse_cli("hydrate", args);
    run(&cli)
}

fn run(cli: &HydrateCli) -> Result<()> {
    let mirror = PathBuf::from(resolve_mirror_dir(cli.mirror.clone()));
    // M-6: advisory typo guard — hydrating an unknown sid is meaningless
    // but harmless (the executor no-ops); warn so typos surface.
    if !read_state_ro(&state::sync_dir(&mirror))
        .sessions
        .contains_key(&cli.sid)
    {
        eprintln!("stub-client hydrate: no session {:?} in state.json — enqueuing anyway", cli.sid);
    }
    enqueue(
        &mirror,
        Intent::Hydrate {
            sid: cli.sid.clone(),
            enqueued_at: state::now_rfc3339(),
        },
    )
    .with_context(|| format!("enqueuing hydrate{{{}}}", cli.sid))
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::testkit::{hold_owner, journal_intents};
    use crate::cli::try_parse_cli;
    use crate::sync::persist::fs::testkit::tmp;

    fn s(v: &str) -> String {
        v.to_string()
    }

    // T-14 shape: positional sid + --mirror, flag=value gain, junk rejected.
    #[test]
    fn t14_hydrate_parse() {
        let cli = try_parse_cli::<HydrateCli>(
            "hydrate",
            &[s("s9"), s("--mirror=/tmp/m")],
        )
        .unwrap();
        assert_eq!(cli.sid, "s9");
        assert_eq!(cli.mirror.as_deref(), Some("/tmp/m"));

        assert!(try_parse_cli::<HydrateCli>("hydrate", &[]).is_err());
        assert!(try_parse_cli::<HydrateCli>("hydrate", &[s("s1"), s("s2")]).is_err());
    }

    // T-806: `hydrate s9` lands exactly one `hydrate{sid}` intent.
    #[test]
    fn t806_hydrate_enqueues_intent() {
        let mirror = tmp("hydratecli", "t806");
        let _owner = hold_owner(&mirror);
        let cli = try_parse_cli::<HydrateCli>(
            "hydrate",
            &[s("s9"), s("--mirror"), mirror.display().to_string()],
        )
        .unwrap();
        run(&cli).unwrap();
        let intents = journal_intents(&mirror);
        assert_eq!(intents.len(), 1);
        assert!(
            matches!(&intents[0], Intent::Hydrate { sid, .. } if sid == "s9"),
            "expected hydrate{{s9}}, got {intents:?}"
        );
        std::fs::remove_dir_all(&mirror).unwrap();
    }
}
