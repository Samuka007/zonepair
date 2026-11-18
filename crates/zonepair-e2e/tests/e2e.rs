//! Thin wrapper so `cargo test` runs the full e2e suite (see
//! `src/bin/zonepair-e2e.rs`). The suite is a binary so the server image can
//! also run it inside the container.

#[test]
fn e2e() {
    let out = Command::new(env!("CARGO_BIN_EXE_zonepair-e2e"))
        .env("ZONEPAIR_SERVER_BIN", workspace_bin("stub-server"))
        .env("ZONEPAIR_MOCK_BIN", workspace_bin("claude-mock"))
        .env("ZONEPAIR_CLIENT_BIN", workspace_bin("stub-client"))
        .status()
        .expect("run zonepair-e2e");
    assert!(out.success(), "zonepair-e2e failed");
}

/// Same-workspace sibling bin: the current test executable lives in
/// `target/<profile>/deps/`, sibling bins in `target/<profile>/`.
/// `CARGO_BIN_EXE_*` only covers same-package bins.
fn workspace_bin(name: &str) -> PathBuf {
    let mut p = std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("deps dir") // target/<profile>/deps
        .to_path_buf();
    p.pop(); // target/<profile>
    if cfg!(windows) {
        p.join(format!("{name}.exe"))
    } else {
        p.join(name)
    }
}

use std::path::PathBuf;
use std::process::Command;
