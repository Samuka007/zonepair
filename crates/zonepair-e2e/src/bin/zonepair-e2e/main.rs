//! The integration suite as a runnable artifact.
//!
//! Native (default): spawns the stub-server binary and drives the whole mock
//! suite against it — this is what `cargo test` and `nix flake check` run.
//! Container: run it inside the server image (`ZONEPAIR_SERVER_BIN=/bin/stub-server
//! stub-client…/zonepair-e2e`) and it exercises the same suite in the
//! container's own network namespace, so the ADR-0001 no-dial check works
//! there too.
//!
//! `ZONEPAIR_EXTERNAL_URL=http://…`: drive an already-running server instead of
//! spawning one (host-against-container).

mod fixtures;
mod mirror;
mod nodial;
mod scenarios;

use crate::fixtures::{spawn_or_external, Server};
use crate::nodial::assert_no_dial;
use crate::scenarios::baseline::{
    basic_turn, doctor_accepts_url, doctor_uses_config_file, doctor_without_url_fails,
    real_model_suite, server_never_dials, sess_rides_http, setup_and_bare_shell,
    three_clients_one_server, tool_round_trip, unset_lifecycle,
};
use crate::scenarios::materialize::*;
use crate::scenarios::mirror_sync::*;
use zonepair_client::api;

/// The mirror-sync suite (T-701–T-716, T-810–T-811) — spawn-mode only:
/// daemon and crash-injection scenarios need the server's store dir on
/// this host and control of A-side processes.
async fn mirror_sync_suite(server: &Server) {
    eprintln!("mirror-sync suite (T-701..T-716, T-810..T-811)");
    t701_publish_wait_lifecycle(server).await;
    eprintln!("  t701 ok");
    t702_publish_precheck_arms(server).await;
    eprintln!("  t702 ok");
    t703_self_converging_spawns(server).await;
    eprintln!("  t703 ok");
    t704_sigkill_mid_commit(server).await;
    eprintln!("  t704 ok");
    t705_publish_replay_idempotent(server).await;
    eprintln!("  t705 ok");
    t706_journal_replay_idempotent(server).await;
    eprintln!("  t706 ok");
    t707_torn_tail(server).await;
    eprintln!("  t707 ok");
    t708_writer_takeover(server).await;
    eprintln!("  t708 ok");
    t709_sync_wait_drain(server).await;
    eprintln!("  t709 ok");
    t710_first_pull_overwrite(server).await;
    eprintln!("  t710 ok");
    t711_remote_delete_suspends(server).await;
    eprintln!("  t711 ok");
    t712_tombstone_suppresses(server).await;
    eprintln!("  t712 ok");
    t713_b_rewrite_resets(server).await;
    eprintln!("  t713 ok");
    t714_merge_during_changes(server).await;
    eprintln!("  t714 ok");
    t715_ws_ordering(server).await;
    eprintln!("  t715 ok");
    t716_mirror_pulls_via_changes(server).await;
    eprintln!("  t716 ok — mirror_pulls_via_changes");
    t810_prune_drop_revives(server).await;
    eprintln!("  t810 ok — prune --drop → traffic → auto-revive");
    t811_prune_stub_resume(server).await;
    eprintln!("  t811 ok — prune --stub → resume passthrough");
}

async fn run_suite() {
    if std::env::var("ZONEPAIR_REAL").is_ok_and(|v| v == "1") {
        eprintln!("real-model suite");
        real_model_suite().await;
        eprintln!("REAL-MODEL E2E PASS");
        return;
    }

    let server = spawn_or_external().await;
    basic_turn(&server.base).await;
    doctor_accepts_url(&server).await;
    doctor_uses_config_file(&server).await;
    doctor_without_url_fails().await;
    setup_and_bare_shell(&server).await;
    unset_lifecycle(&server).await;
    tool_round_trip(&server.base).await;
    sess_rides_http(&server.base, &server).await;
    three_clients_one_server(&server.base).await;
    if server.child.is_some() {
        mirror_sync_suite(&server).await;
    } else {
        eprintln!("  note: external server — daemon/crash scenarios not attributable, T-7xx skipped");
    }
    if server.child.is_some() {
        server_never_dials(&server).await;
    } else {
        eprintln!("  note: external server — no-dial not attributable, skipped");
    }
    assert_no_dial(&server);
    eprintln!("E2E PASS (carrier=ws-line-v1, {} sessions on B)", {
        api::store_list(&server.base, None).await.unwrap().len()
    });
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(run_suite());
}
