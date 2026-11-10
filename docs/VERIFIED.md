# Verification log

Evidence runs, newest first. Re-run anything you doubt; these are records,
not guarantees.

## 2026-11-10 — store HTTP framing and verification cleanup

- Store requests now use reqwest 0.13 with the existing ring/rustls trust
  configuration. Removed the hand-written HTTP parser, stream adapter, and
  unconditional `close_notify` error suppression.
- Regression tests failed before the fix: chunk framing was parsed as JSON,
  and a valid JSON body shorter than `Content-Length` was accepted.
  Both now pass. Additional loopback TLS tests cover complete chunked bodies
  without `close_notify` and rejection of an empty, truncated response.
- Actual `stub-client sess list` smoke: Content-Length and chunked responses
  both return the expected session; a truncated body exits non-zero with an
  incomplete-body error. An invalid TLS certificate is rejected.
- `nix develop -c cargo test --workspace --locked`: **PASS** (8 unit tests,
  native mock suite, 6 store tests).
- `nix flake check -L`: **PASS** after removing the obsolete shellcheck gate.
  Removed the unused Docker test image and live references to deleted
  verification scripts; historical evidence entries below remain unchanged.
- `.#release-dist` (x86_64 Linux musl) and `.#release-dist-windows`: **PASS**.
  The staged Linux server/client also passed the native mock harness.
- No Docker verification or real Claude invocation. ARM64 and macOS builds
  remain delegated to CI's native runners.

## 2026-11-08 (part 3) — release mechanics move into the flake

- `.#release-dist` (linux) stages `stub-client-x86_64-linux` +
  `stub-server-x86_64-linux` — `file` confirms `static-pie linked`, names
  byte-identical to the rc.3 assets; `.#release-dist-windows` stages
  `stub-client-x86_64-windows.exe`.
- `.#stub-linux`: both production bins `dynamically linked` against the
  nixpkgs glibc (platform-gated to Linux); its checkPhase ran the full suite
  (8 suites + `E2E PASS`) in release mode. Not a release asset — this is the
  future apt/deb input.
- `.#push-image` against a local distribution v3.1.2 registry
  (`REGISTRY=127.0.0.1:5000`): built and pushed
  `zonepair-{server,client}:x86_64-linux`; `regctl image inspect` reads back
  `amd64/linux` for both. Non-ghcr registry gets TLS verification disabled
  for plain-HTTP smoke runs only.
- `.#merge-images` (`SYSTEMS=x86_64-linux`, version `smoke-test`): both repos
  gained `application/vnd.oci.image.index.v1+json` indexes with amd64
  descriptors; `:latest` digest == `:smoke-test` digest (`sha256:6f7f4868…`).
- Workflow: `release.yml` shrinks to `nix build`/`nix run` calls (image
  matrix 4 jobs → 2, one per arch); `actionlint` clean on both workflows.
- Full gates: `nix flake check -L` and `nix develop -c cargo test
  --workspace` green (run again at ticket close; see final section).
- Local registry used for smoke: `nix shell nixpkgs#distribution` on
  127.0.0.1:5000, torn down after the run — nothing touched ghcr.

## 2026-11-08 (part 2) — darwin verification path made real

- `zonepair-e2e`'s no-dial attribution (ADR-0001) reads `/proc/{pid}/fd` +
  `/proc/net/tcp` — procfs, linux-only. `assert_no_dial_for` now skips with a
  note on non-linux when no procfs data exists (same policy as an external
  server); the linux behavior is byte-identical. This was a hard failure on
  darwin: `socket_inodes` returns empty → `assert!(!inodes.is_empty())` panics.
- `ci.yml` check job is now a matrix (ubuntu-latest + macos-15): every PR
  compiles and `cargo test`s on darwin — `checks.aarch64-darwin.default`
  builds `stub-darwin` and runs the suite with the skipped-attribution branch.
  actionlint clean.
- Linux regression: `nix build .#stub` (cargo test in checkPhase) green;
  `result/bin/zonepair-e2e` against the built bins: **E2E PASS** (9 sessions,
  no-dial attribution still enforced).
- README: mac non-nix install gets the one-time
  `xattr -d com.apple.quarantine` note (ad-hoc signed binary; no notarization
  by design — CLI audience, same policy as nixpkgs darwin binaries).

## 2026-11-08 — darwin target + daemonless multi-arch image release

- `aarch64-darwin` joins `flake.nix` `systems`. `.#stub-darwin` (new): plain
  dynamic release build on the host fenix toolchain; `.#stub-static` is now
  `lib.mkIf hostPlatform.isLinux` — darwin has no musl target. Verified by
  eval: `packages.aarch64-darwin.stub-darwin` = `zonepair-0.1.0`,
  `packages.aarch64-darwin.stub-static` correctly absent, both linux
  static attrs unchanged.
- skopeo offline conversion of the `dockerTools` tarball verified on this
  host: `skopeo copy docker-archive:<nix out> dir:<d>` → docker manifest
  v2, 49 layers, `inspect --config` reports `linux/amd64`; no daemon touched.
  Auth/policy via `--insecure-policy`.
- `--override-arch` does **not** rewrite the config blob (two experiments,
  config stayed `amd64`) — platform descriptors cannot be faked, so the
  multi-arch release builds each arch natively (ubuntu-latest +
  ubuntu-24.04-arm) instead of cross-building images.
- registry-level flow verified end-to-end against a local `distribution`
  registry (no daemon): regctl `image copy ocidir://…` push, `index create`
  merging two per-arch tags → OCI index whose entries carry
  `platform: {architecture, os}` correctly; skopeo `inspect --raw` reads the
  index back. regctl's `--desc-platform` proved unreliable on local OCI
  layouts (descriptor lost) but works as expected against a registry.
- `release.yml` rewritten: per-arch build matrix (2 arch × 2 images) pushes
  `:<system>` tags via skopeo→regctl, then one job merges
  `index create` + `image copy` retag into `:<version>`/`:latest`.
  actionlint clean. Docker daemon, `docker load/tag/push` all gone.
- ghcr auth (regctl `--host reg=…,user=…,pass=…`) verified to reach the
  token exchange: bad creds → `unauthorized` from the realm, i.e. the
  challenge round-trip works; real push needs the workflow's
  `GITHUB_TOKEN` with `packages: write`.

## 2026-11-06 (part 4) — real-model e2e is Rust, wired from the omp credential store

- `agent.db` (`~/.omp/agent/agent.db`) is the live credential store, not the
  legacy `models.yml` scrape: `auth_credentials` carries `deepseek` as
  `{"key": …, "source": "login"}` (and a kimi-code OAuth entry). `src/wiring.rs`
  reads it read-only (URI mode + busy timeout) and maps provider →
  Anthropic-compatible endpoint + model (deepseek →
  `api.deepseek.com/anthropic` + `deepseek-flash`).
- `zonepair-e2e` gained the real-model suite (gated by `ZONEPAIR_WIRING=deepseek`,
  real claude via `ZONEPAIR_CLAUDE_BIN`): marker echo through A → WS → B →
  claude → provider, real session landing in B's store, fork of the real
  session, resume of the fork in a fresh claude process reproducing the marker,
  delete, no-dial. **PASS** with claude 2.1.317 + deepseek-flash, ~45 s wall.
- A direct probe (claude one-shot, bypassing the stub) was run first to
  separate credential/model failures from transport failures.
- rusqlite (bundled) joins Cargo for the credential read; devShell carries
  `pkgs.claude-code` (2.1.317) so the real claude is on PATH.
- ADR-0006's "wiring-env.py's job moves into the Rust harness" is now done —
  the models.yml line-scrape is dead code.

## 2026-11-06 (part 3) — opencode v2 as the A-side harness

- Facts, from primary sources (an earlier claim of mine that nixpkgs lacked
  opencode v2 came from querying the *stable* registry instead of unstable —
  wrong):
  - nixpkgs never shipped 2.2.3/2.2.4; it went `2.2.1 -> 2.2.5`
    (NixOS/nixpkgs@9c4790e0e8d4, 2026-11-01).
  - the "Claude Agent" provider — the thing that makes `OPENCODE_CLAUDE_PATH`
    mean anything — exists from **v2.2.3** (`specs/v2/claude-agent-provider.md`
    is present at tags v2.2.3+ and 404 at v2.2.0/1/2).
  - nixpkgs' 2.2.5 recorded **one platform-agnostic `node_modules` hash taken
    from a Darwin build**, so every x86_64-linux build failed the fixed-output
    check (and thus never reached cache.nixos.org). Upstream fixed it
    per-platform in NixOS/nixpkgs@c399e1b7fb90 (2026-11-04), not yet in
    nixos-unstable (HEAD 2026-11-03).
  - no community cache carries it: opencode's CI pushes nothing (no
    cachix/garnix/attic in any workflow, no `nixConfig`); `opencode.cachix.org`
    exists but is a personal cache (dominicnunez) without our paths;
    nix-community / numtide / nixpkgs-wayland all 404; FlakeHub says the flake
    does not exist.
- Local patch (`opencodeHashFix` overlay): override the `node_modules`
  `outputHash` with the linux value. It reproduces upstream's fix exactly —
  same drv (`jaj1m6ab5ng…`) and same output (`s5v1spmji7…`) as the fix commit.
- `nix build .#opencode` → `opencode v2.2.5`, and
  `opencode auth login claude-agent` answers "Connected to Claude Agent", so
  the provider is really present.
- nixpkgs input bumped 2026-10-30 → 2026-11-03; `nix flake check` green and
  `scripts/run-e2e-docker.sh` PASS again on the newer nixpkgs (claude-code
  2.1.312 → 2.1.317), no-dial included.
- README + ADR-0004 now state the v2 ≥ 2.2.3 requirement and that
  `OPENCODE_CLAUDE_PATH` is read by the opencode *server*.

## 2026-11-06 (part 2) — no entrypoint, packaged mock

- `stub-server` now falls back to the environment for every option
  (`STORE`, `CLAUDE_CMD`, `LOG_DIR`, `HTTP_LISTEN`, `HTTP_TOKEN`); a flag always
  wins and an empty value counts as unset. The server image therefore runs the
  bare binary (`Cmd = ["stub-server"]`) and `docker/server-entrypoint.sh` is
  deleted. The entrypoint's silent "fall back to the mock claude" is gone: the
  image defaults to the real `claude` it bundles.
- smoke (native binary): env fallbacks picked up; `--store` beat `STORE`; both
  an empty `CLAUDE_CMD` and a bare environment resolved to `claude` /
  `$HOME/.claude` / `./stub-logs`.
- `.#mock`: `mock/` is a derivation (scripts at `/opt/mock`, plus `claude-mock`
  and `seed-store` wrappers with a pinned python3); both images consume it, so
  the mock is content-addressed instead of a loose in-image file.
- store seeding is no longer an image side effect — it is test data, so
  `run-e2e-docker.sh` runs `seed-store "$STORE"` in container `b` first.
- `scripts/run-e2e-docker.sh`: **PASS** (mock pinned via `B_CLAUDE_CMD=claude-mock`,
  basic / mcp / 3 parallel / store RPC / mirror sync / no-dial).
- `scripts/run-e2e-native.sh`: PASS.
- image config check: `Entrypoint=null Cmd=["stub-server"]`, `Env` carries
  `STORE/LOG_DIR/HTTP_LISTEN/HOME`; `claude-mock`, `seed-store`, `stub-server`
  all resolve on PATH in both images.

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
