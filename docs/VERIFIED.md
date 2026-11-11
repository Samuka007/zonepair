# Verification log

Evidence runs, newest first. Re-run anything you doubt; these are records,
not guarantees.

## 2026-11-11 — bridged SDK host needs the mirror as its CLAUDE_CONFIG_DIR (#17)

- Reproduced with real components, no mocks in the data path: `cc-session-io`
  `createSession` wrote a two-message transcript for a fresh uuid into a
  throwaway A-side root, then the **real claude binary** was driven through
  **stub-client → stub-server** (`--resume <uuid>`, stream-json). With the
  bridge's root (default `~/.claude`) disagreeing with the configured mirror
  (`~/.claude-remote-mirror`), B's claude exited code 1 with
  `No conversation found with session ID: <uuid>` — zero provider calls, the
  exact #17 symptom.
- Root cause corrected against the issue body: it is **not** "OMP resends the
  same dead session id" and **not** mirror staleness. The OMP process had no
  `CLAUDE_CONFIG_DIR` at all, so claude-bridge (`getSessionPath(…,
  process.env.CLAUDE_CONFIG_DIR)`) wrote transcripts into `~/.claude` while
  `stub-client sync` pushed the (empty) configured mirror. Same uuid resumed
  fine once both roots pointed at the same directory.
- Fix: share the root. Machine A: `CLAUDE_CONFIG_DIR=<mirror>` in
  `~/.omp/agent/.env` (OMP loads it into `Bun.env` before plugins load —
  `pi-utils/src/env.ts` fills only unset keys, so explicit env still wins).
  Repo: README + docs/CLIENT.md document the contract for any Agent SDK host.
- Aligned-root resume proven with the same harness: first relayed turn
  resumed the imported transcript (marker echoed back), second turn resumed
  again; B's store grew `projects/lab/<uuid>.jsonl` (push by stem) and the
  mirror pulled it back. Negative controls: root left at `~/.claude` →
  reproduces; root pointed straight at B's store → also works (stub-client
  walks any root), confirming the invariant is only "both sides same dir".
- OMP end-to-end: `omp --model claude-bridge/claude-opus-5-5 -p --no-session`
  against a throwaway local stub-server + fake Anthropic provider exited 0
  with the provider's reply text — the real **installed** `~/.local/bin/
  stub-client` was spawned by the bridge, `ZONEPAIR_CLIENT_CONFIG` +
  `CLAUDE_CONFIG_DIR` isolated the data plane, and the B store gained the
  OMP session transcript under its cwd-hash dir (post-turn pull landed it in
  the mirror). A `billing: subscription` preflight blocks hosts without a
  claude.ai login (this box has none); `billing: inherit` + `ANTHROPIC_*`
  env passes. No repo code changed; `cargo test`/`nix flake check` run to
  confirm no drift.
- Original session uuids from #17 were never resumed or rewritten; the
  historical transcript was consulted read-only (seance) and shows no
  recorded cwd/argv/mirror state, i.e. the issue's single-variable isolation
  table was the only path evidence — and it tested claude, not the relay.

## 2026-11-11 — darwin cachix coverage, doctor config-file resolution, bins restriction

- Issue #13 closed by release `v0.1.0-rc.7` ([run
  42968228443](https://github.com/Samuka007/zonepair/actions/runs/42968228443),
  darwin job green): the darwin `release-binaries` entry now builds
  `.#release-dist`, `.#stub-client` and `.#stub-server`. narinfo on
  `samuka007.cachix.org`: stub-client `n2z7iikm…` **200**, stub-server
  `j6qlzfhf…` **200**, stub-darwin `ay5kc26v…` **200**, all carrying
  `Sig: samuka007.cachix.org-1:…`.
- Consumer substitution proven at the runtime closure: the `stub-client` /
  `stub-server` narinfos' `References` is exactly
  `libiconv-115.100.1`, present on `cache.nixos.org` (**200**). A
  single-source `nix copy --from samuka007` still fails on libiconv — a tool
  limitation, not a cache gap: consumers query both substituters (flake
  `nixConfig` declares samuka007; `cache.nixos.org` is default). Of the 1234
  build-closure paths (`nix derivation show -r`), samuka007 holds the 63 the
  darwin runner built (zonepair outputs, crate deps, fenix components); the
  rest are nixpkgs build inputs on cache.nixos.org. Paths absent from both
  caches are build-time-only (`patchutils`, `make-shell-wrapper-hook`) and
  are never substituted by a consumer. rc.6's `stub-client` path stays 404
  permanently — consumers need ≥ rc.7.
- Issue #15 fixed in `9488b52`: bare `doctor` routes URL resolution through
  the shared `resolve_url` (flag > env > config file). e2e:
  `doctor from config file ok`, `doctor without any url fails with the
  shared message`, existing `doctor --url ok` unchanged. `cargo test
  --test e2e` and `nix flake check -L` green.
- Issue #14 fixed in `2e975fa`: `mkStub`'s dead `bins` parameter wired into
  `cargoBuildFlags` (`--bin` per entry, the `windows.nix` precedent);
  `bins = null` merges no attribute, so `checks.default`'s derivation is
  unchanged (`stub` drv env has no `cargoBuildFlags`; `stub-server` shows
  `["--bin","stub-server"]`). Packages are now genuinely distinct — pre-fix
  `stub` == `stub-darwin` == `stub-server` were byte-identical derivations
  and `stub-client` differed only by `meta.mainProgram`; post-fix the three
  darwin paths are separate outPaths, so the production images no longer
  carry `claude-mock` / `zonepair-e2e`.
- Both pushes green on ci.yml (`9488b52`, `2e975fa`); `nix fmt` clean on
  `nix/rust.nix`.

## 2026-11-10 (part 3) - client config file (ADR-0008)

- `stub-client` relay knobs moved from the opencode service env to a
  zonepair-owned TOML file (`dirs::config_dir()/zonepair/client.toml`, 0600,
  atomic rename; `$ZONEPAIR_CLIENT_CONFIG` overrides the path). Resolution
  order everywhere: flag > env > file > default. `setup` is interactive
  (dialoguer) on a TTY or all-or-nothing with flags, and writes only
  `OPENCODE_CLAUDE_PATH` + `CLAUDE_CONFIG_DIR` into the opencode env;
  `unset` erases the file and the two keys. New deps via `cargo add`:
  toml 0.9, dialoguer 0.12 (password only), dirs 7.
- `cargo test`: unit tests 12/12 (4 new cfg store tests incl. the 0600
  permission check), store 6/6, e2e green including the new
  `setup_and_bare_shell` regression (strict flags, TTY gate, file-fed
  bare-shell `sync`, token never echoed to stdout) and the trimmed
  `unset_lifecycle` (file erase + two-key removal).
- Real-pty smoke: interactive `setup` against a spawned stub-server — prompts
  rendered with prefills, token input hidden, file written 0600, doctor
  chained, exit 0; bare `stub-client sync` with a cleared environment
  resolved everything from the file; partial-flag and non-TTY runs failed
  loudly with nothing written.
- `nix flake check -L` green (x86_64-linux; other systems delegated to CI's
  native runners). Migration path for rc.5 installs documented in
  docs/CLIENT.md.

## 2026-11-10 (part 2) - public Cachix cache

- `ci.yml`/`release.yml` replaced `magic-nix-cache-action` (GitHub-internal
  only, free tier EOL) with `cachix/cachix-action@v17` (cache `samuka007`,
  `CACHIX_AUTH_TOKEN` secret). Fork PRs pull without pushing via
  `skipPush: ${{ secrets.CACHIX_AUTH_TOKEN == '' }}`.
- `flake.nix` declares `flake.nixConfig` with the substituter and
  `samuka007.cachix.org-1:fywbWKGfnCfjhkNS8hHD9uAMm/91RGpeAl/iUijepcM=`;
  consumer instructions live in `docs/DEVELOPMENT.md` (Packaging).
- Populate run [42812731074](https://github.com/Samuka007/zonepair/actions/runs/42812731074):
  all 3 jobs green; post-phase pushed fenix toolchain components and both
  `zonepair-0.1.0` outputs (x86_64-linux, aarch64-darwin).
- Distribution proof: anonymous `GET /vhb624y5sgn8dm7cdajwqji1z9jx0hrn.narinfo`
  on `samuka007.cachix.org` returned 200 with
  `Sig: samuka007.cachix.org-1:...` - public read, signature present.

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
