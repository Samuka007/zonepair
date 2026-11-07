//! Thin wrapper so `cargo test` runs the full e2e suite (see
//! `src/bin/zonepair-e2e.rs`). The suite is a binary so the server image can
//! also run it inside the container.

#[test]
fn e2e() {
    let out = Command::new(env!("CARGO_BIN_EXE_zonepair-e2e"))
        .env("ZONEPAIR_SERVER_BIN", env!("CARGO_BIN_EXE_stub-server"))
        .env("ZONEPAIR_MOCK_BIN", env!("CARGO_BIN_EXE_claude-mock"))
        .status()
        .expect("run zonepair-e2e");
    assert!(out.success(), "zonepair-e2e failed");
}

use std::process::Command;
