# Verification log

Evidence runs, newest first. Re-run anything you doubt; these are records,
not guarantees.

## 2026-11-06

- nix packaging: the flake now builds every artifact — `.#stub` (native;
  `cargo test` runs in `checkPhase`), `.#stub-static` (musl), 
  `.#stub-client-windows` (`x86_64-pc-windows-gnu`), `.#docker-server` /
  `.#docker-client` (nixpkgs `dockerTools.buildLayeredImage`). The Dockerfile
  is gone; `docker/` keeps only the entrypoint and `mcp.json`.
- `nix build .#stub`: 6/6 store tests pass (`fork` prefix/fresh-ids/chain,
  uuid-less skip, merge union/idempotency, exists/messages/delete, list).
- `nix build .#stub-static`: `static-pie linked` (`ldd`: "statically linked"),
  0 rust-toolchain references. `.#stub-client-windows`: PE32+ x86-64 `.exe`.
- `nix flake check`: green — `checks.default` (cargo test) + `checks.shellcheck`.
- `scripts/run-e2e-docker.sh` against the nix-built images: **PASS** — basic
  turn, MCP round trip, 3 concurrent clients, store RPC
  (exists/messages/fork/delete), mirror sync (ADR-0004), and the no-dial
  assertion inside container `b` (pid 1, :7788).
- `nix develop -c cargo build` + `scripts/run-e2e-native.sh`: PASS (the
  native suite CI runs).
- image size after dropping rustc's baked sysroot path
  (`--remap-path-prefix`; without it the whole fenix toolchain rode along):
  server 1.15 GB -> 241 MB, client 1.05 GB -> 146 MB.
- image layer fixups the e2e forced, now in the flake: `fakeNss`
  (`/etc/nsswitch.conf` — service names did not resolve), `usrBinEnv` +
  `binSh` (`#!/usr/bin/env bash` did not resolve), writable `/tmp` (`mktemp`
  in the mirror phase), and `gawk` (the no-dial assertion).

## 2026-11-01

- `cargo test`: 7/7 (env allowlist; store exists/messages/delete; fork
  prefix+fresh-ids+chain; fork error paths; merge union/idempotency/uuid-less
  drop/lab-path creation; list)
- `scripts/run-e2e-native.sh`: PASS on 127.0.0.2 (B) vs default zone (A) —
  basic turn, MCP round trip, 3 concurrent clients, sess RPC, file-level fork
  verification (`mock/verify-fork.py`: 3 lines, chain consistent), 3
  concurrent forks -> 3 distinct sessions, no-dial assertion via `ss`
- `scripts/run-e2e-docker.sh`: PASS across two containers (service `a` ->
  `b:7788`), same suite, no-dial assertion executed inside container `b`
  against pid 1
- `WIRING=deepseek scripts/run-e2e-docker-real.sh`: PASS — real claude
  **2.1.316** (native build, container `b`) answering through
  `api.deepseek.com/anthropic` with `deepseek-flash`; marker echo, real JSONL
  in B's store, RPC fork of the real store, and **`--resume <forked-id>`
  through the stub reproducing the parent session's marker** (cross-process
  context continuity), delete + no-dial clean
- ws-line-v1 full stack: real-claude e2e over **WS stdio + HTTP store + WS
  MCP tunnel** — marker echo, HTTP store ops, fork+resume continuity, real
  `mcp__lab__read_work_file` call reading a file that exists only on machine
  A, no-dial clean; mock suites (native + docker) regression PASS
- zero-patch mirror sync (ADR-0004): mock suites gained a mirror phase — a
  fork seeded on A's mirror pushes to B's store, a turn appends on B, and
  stub-client's auto-pull brings the mirror back (5 lines); exposed and
  fixed the `CLAUDE_CONFIG_DIR` pin-order bug (hello header env must not
  override the server's store pin)
- wiring env closure: `wiring-env.py` emits `unset` for keys it does not
  manage, so stale exported `ANTHROPIC_MODEL`/`WIRING` values from the
  calling shell can no longer leak into a real-model run (root cause of a
  503 mis-wired run: deepseek-flash sent to the anthropic gateway)
- ShellCheck: clean on all scripts
