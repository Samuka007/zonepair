# claude-io-stub

A C/S stub pair that lets an agent harness (OpenCode / Claude Agent SDK) on
machine **A** drive a real `claude` binary running on machine **B**: stdio
relay + session-store projection + MCP, in Rust. Built to validate splitting
OpenCode's `claude-agent` provider across two machines.

> Packaging is nix-first: `nix build` produces the binaries, the static
> release artifacts and both OCI images. See [Packaging](#packaging) below.
> The windows-gnu `.exe` is cross-built with crane.

Vocabulary: [GLOSSARY.md](GLOSSARY.md) · Protocol reference:
[docs/PROTOCOL.md](docs/PROTOCOL.md) · Decisions: [docs/adr/](docs/adr/) ·
Verification log: [docs/VERIFIED.md](docs/VERIFIED.md)

## Packaging

Everything is built by the flake (fenix-pinned Rust toolchain, nixpkgs
`dockerTools` for the images) — there is no `Dockerfile` and no in-image
`apt`/`npm`/`curl | bash` step.

| Output | What it is |
| --- | --- |
| `nix build` (`.default` / `.#stub`) | native `stub-server` + `stub-client`; `cargo test` runs in the build |
| `.#stub-static` | static musl `stub-server` + `stub-client` (the release artifact) |
| `.#stub-client-windows` | `stub-client.exe` cross-built for `x86_64-pc-windows-gnu` |
| `.#mock` | mock `claude` + store fixtures as commands (`claude-mock`, `seed-store`) |
| `.#opencode` | A-side harness (`opencode` 2.2.5, carrying the local hash patch) |
| `.#docker-server` / `.#docker-client` | OCI images (`docker load < result`) |
| `nix flake check` | `cargo test` + shellcheck |

```bash
nix build                          # native binaries in ./result/bin
nix build .#stub-static            # static musl binaries
nix build .#stub-client-windows    # stub-client.exe

scripts/nix-docker-load.sh         # build + docker load both images
scripts/run-e2e-docker.sh          # in-container suite against those images
```

CI (`.github/workflows/`) runs `nix flake check` plus the native and
docker e2e suites on every push/PR. Tagging `v*` publishes a GitHub release
with the static binaries (`x86_64-linux`, `aarch64-linux`), the Windows
`.exe`, and pushes both images to `ghcr.io/<owner>/zonepair-{server,client}`.


## Quickstart (mock claude)

Native — spawns `stub-server` on a free loopback port with the Rust mock
claude, then drives the whole suite against it:

```bash
nix develop -c cargo test --workspace
```

Docker — builds and loads the nix images, boots container `b` (server + mock
claude) and execs the same Rust harness inside it:

```bash
scripts/run-e2e-docker.sh
```

Both run the same suite: basic turn, in-band tool round trip, 3 concurrent
clients on one server, store RPC (exists/messages/fork/delete), mirror sync,
file-level fork verification, 3 concurrent forks, and the no-dial assertion.

## Real-model e2e (real claude, real provider)

Boots machine B with the genuine `claude` binary and drives a real model
backend through the relay (needs network + an API token). Everything
provider-specific lives outside this repository: export the variables claude
itself consumes, then opt in with `ZONEPAIR_REAL=1`:

```bash
export ANTHROPIC_BASE_URL=...             # any Anthropic-compatible endpoint
export ANTHROPIC_AUTH_TOKEN=...           # provider API key
export ANTHROPIC_MODEL=...                # optional
ZONEPAIR_REAL=1 nix develop -c cargo test --test e2e
```

Credentials travel only as harness env → hello header (allowlisted
`ANTHROPIC_*`/`CLAUDE_*`) → claude child env; never written to the repo,
never printed. The suite asserts: marker echo through A → B → provider, the
real JSONL session landing in B's store, **fork of the real store + `--resume`
of the forked session reproducing the parent's marker**, and cleanup +
no-dial on B.

### Server login (machine B)

The server image ships the real `claude` but no credentials, and there are two
ways to give it some:

* **Env wiring** — the reproducible one, and what the real e2e does: pass
  `ANTHROPIC_BASE_URL` / `ANTHROPIC_AUTH_TOKEN` (plus `ANTHROPIC_MODEL` when the
  gateway needs it) into the container. No interactive step, works headless.
* **Subscription login** — one-time
  `docker compose exec -it b claude auth login`. `compose.yaml` gives `b` a
  `b-home:/root` volume for exactly this, so the login survives `up`/`down`;
  without that volume it dies with the container.

Sessions persist separately and already: the server pins the child's
`CLAUDE_CONFIG_DIR` to the store (`/data/claude`, on the `stub-data` volume).

## Manual poking

```bash
export CLAUDE_STUB_ENDPOINT=127.0.0.2:7788
stub-client sess fork session_aaa u-aaa-2   # pretty JSON, exit 2 on rpc error
stub-client doctor                          # preflight: endpoint, opencode
                                            # env, version, mirror
```

## Hooking into OpenCode (zero patch)

Stock opencode, no source changes (mechanism: ADR-0004).

**Requirements**: opencode **v2 ≥ 2.2.3** — that is where the "Claude Agent"
provider lands, and v1 has no such provider, so `OPENCODE_CLAUDE_PATH` means
nothing there. `claude` on B must be ≥ 2.1.248 (opencode passes SDK options
older builds reject).

**Install** — the client is one static binary; that is the whole install:

```bash
nix build .#stub-static                                  # or the release asset
install -m755 result/bin/stub-client ~/.local/bin/stub-client
```

**Configure** — these variables are read by the opencode **server**, and the
stub-client it spawns inherits the server's environment. So set them through
opencode rather than your shell: the server is often started by its own daemon,
which never sees your exports.

```bash
opencode service set env OPENCODE_CLAUDE_PATH "$HOME/.local/bin/stub-client"
opencode service set env CLAUDE_STUB_ENDPOINT  b:7788
opencode service set env CLAUDE_CONFIG_DIR     "$HOME/.claude-remote-mirror"
opencode service set env CLAUDE_STUB_MIRROR    1
stub-client sync                                         # one-time mirror bootstrap
```

All four of those are what `stub-client setup` writes, so in practice:

```bash
stub-client setup --endpoint b:7788      # writes the server env, creates +
                                         # bootstraps the mirror, then doctor
stub-client doctor                       # preflight, any time
```

`doctor` exists because the failure mode is silent: if the opencode server
never sees `OPENCODE_CLAUDE_PATH`, opencode happily runs a local `claude` that
talks to Anthropic directly — no relay, no error. It checks the endpoint's
store RPC, that the **server's** `OPENCODE_CLAUDE_PATH` really points at a
stub-client, that opencode is ≥ 2.2.3, and that the mirror is populated.

Then `opencode auth login claude-agent` (or `/connect`) records *which* login
method you chose; the actual Claude login is claude's own, on B.

opencode's in-process Agent SDK store helpers (`sessionExists` / `fork` /
`remove`) read `CLAUDE_CONFIG_DIR` from the process environment, so they run
on the mirror as if it were a local claude store; the spawned stub-client
pushes A-side forks to B before each turn and pulls B's new messages back
after it. Adding/removing sessions on A: `stub-client sync` any time.

The login flow is intentionally not replaced — anything involving local
`claude auth login` on machine A is undefined behavior for this lab.

## Environment reference

| Variable | Used by | Meaning |
| --- | --- | --- |
| `CLAUDE_STUB_URL` | stub-client, harness | base URL of the stub-server (`ws://b:7789` / `http://gateway/...`) |
| `CLAUDE_STUB_TOKEN` | stub-client | bearer token when the server runs with `--token` |
| `CLAUDE_STUB_MIRROR` | stub-client | `1` enables push/pull sync around each relayed turn |
| `CLAUDE_CONFIG_DIR` | stub-client sync, opencode | A-local mirror of B's store |
| `LISTEN` / `TOKEN` / `STORE` / `LOG_DIR` / `CLAUDE_CMD` | stub-server | same names as the flags; see [PROTOCOL.md](docs/PROTOCOL.md#server-configuration) |
| `B_CLAUDE_CMD` | docker suite (host) | feeds compose's `CLAUDE_CMD` interpolation; the script sets `/usr/local/bin/claude-mock` |
| `ZONEPAIR_REAL` | real-model suite | `1` runs it (requires the `ANTHROPIC_*` vars) |
| `ZONEPAIR_CLAUDE_BIN` | real-model suite | claude binary to spawn (default `claude`) |
| `ZONEPAIR_SERVER_BIN` / `ZONEPAIR_MOCK_BIN` | zonepair-e2e | binaries to drive (cargo test sets them) |
| `ZONEPAIR_EXTERNAL_URL` / `ZONEPAIR_KEEP` | zonepair-e2e | drive an existing server / keep the temp dir |
| `ANTHROPIC_BASE_URL` / `ANTHROPIC_AUTH_TOKEN` / `ANTHROPIC_MODEL` | real-model suite | consumed by claude; set them yourself |

## Layout

```
src/bin/stub-server.rs   accept loop, sess RPC, ws-line-v1 listener, per-connection claude spawn + relay
src/bin/stub-client.rs   transport mode (SDK-facing) + store mirror sync + sess CLI
src/                     proto (line framing), envf (allowlist), store (JSONL + fork), http, claudeproc
tests/store.rs           fork/exists/messages/delete unit tests
scripts/                 e2e runners, no-dial assertion
```
