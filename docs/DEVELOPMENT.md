# Development

End-user Docker server deployment is in the [README](../README.md).
[Client integration](CLIENT.md), [protocol reference](PROTOCOL.md),
[decisions](adr/), and [verification log](VERIFIED.md) are separate documents.

## Native tests

Tests and protocol drivers are Rust. Unit tests live beside the code;
integration tests live in `tests/`.

```bash
nix develop -c cargo test --workspace
nix flake check -L
```

The mock suite starts `stub-server` on a free loopback port and exercises
basic turns, in-band tool round trips, concurrent clients, store RPC
(exists/messages/fork/delete), mirror synchronization, concurrent forks,
and the no-dial invariant.

## Docker tests

```bash
scripts/run-e2e-docker.sh
```

The script builds and loads the images, selects
`B_IMAGE=claude-io-stub-server-test:latest` and
`B_CLAUDE_CMD=/usr/local/bin/claude-mock`, starts service `b`, and executes
the same Rust harness inside its network namespace. The test image is the
production image plus the mock and harness; it is not a release image.
The script removes its Compose project and volumes afterward: do not run it
against a production deployment.

## Real-model suite

The opt-in real-model suite starts its own local server and real `claude`.
Run it only on a machine intended to execute Claude, with Claude's environment
already configured externally. The current harness additionally requires
`ANTHROPIC_BASE_URL` and `ANTHROPIC_AUTH_TOKEN`; these are harness
requirements, not deployment instructions.

```bash
ZONEPAIR_REAL=1 nix develop -c cargo test --test e2e
```

It checks a marker through the relay, the real JSONL session in B's store,
fork/resume reproducing the parent's marker, cleanup, and no-dial.
Credentials are passed via the allowlisted hello environment, not stored in
this repository.

## Packaging

The flake builds the binaries and OCI images; there is no Dockerfile.
`Cargo.lock` and `flake.lock` pin the build inputs.

| Output | Contents |
| --- | --- |
| `.#stub` / default | All four binaries, including the mock and harness; cargo tests run in the build |
| `.#stub-server` / `.#stub-client` | One production binary each |
| `.#stub-static` | Linux musl static server and client |
| `.#stub-darwin` | Dynamic server and client for `aarch64-darwin` |
| `.#stub-linux` | Dynamic glibc production binaries |
| `.#release-dist` / `.#release-dist-windows` | Staged release asset names |
| `.#stub-client-windows` | Cross-built `x86_64-pc-windows-gnu` client |
| `.#opencode` | OpenCode 2.2.5 with the local hash patch |
| `.#docker-server` / `.#docker-client` | Production OCI images |
| `.#docker-server-test` | Server image with mock and harness, for tests only |
| `.#push-image` | Build and push per-architecture production images |
| `.#merge-images` | Merge per-architecture images into versioned and latest multi-arch indexes |

```bash
nix build .#stub-static
nix build .#docker-server
docker load < result
scripts/nix-docker-load.sh
```

Locally loaded server images are named `claude-io-stub-server:latest`;
the registry name is `ghcr.io/<owner>/zonepair-server`.
Set `B_IMAGE=claude-io-stub-server:latest` to use a locally loaded image with
`compose.yaml`. Its default is the published image used by end users.

CI runs native checks and Docker tests on pushes and pull requests. Tags
matching `v*` publish binaries and push the production images. The image
matrix builds `x86_64-linux` and `aarch64-linux` natively, then merges them.
Registry operations do not require a Docker daemon:

```bash
OWNER=you PASSWORD=token nix run .#push-image -- x86_64-linux
OWNER=you PASSWORD=token nix run .#merge-images -- v0.1.0
```

`REGISTRY` overrides GHCR for local registry smoke runs; `SYSTEMS` can narrow
the merge inputs.

## Manual protocol commands

```bash
export CLAUDE_STUB_URL=ws://127.0.0.1:7789
stub-client sess fork session_aaa u-aaa-2
stub-client doctor
```

## Test environment variables

| Variable | Meaning |
| --- | --- |
| `B_IMAGE` | Compose image override; the Docker suite selects the test image |
| `B_CLAUDE_CMD` | Compose override for the server's Claude command; the Docker suite selects the mock |
| `ZONEPAIR_REAL` | `1` selects the real-model suite |
| `ZONEPAIR_CLAUDE_BIN` | Real suite's Claude command, default `claude` |
| `ZONEPAIR_SERVER_BIN` / `ZONEPAIR_MOCK_BIN` / `ZONEPAIR_CLIENT_BIN` | Binaries used by the Rust harness; the integration-test wrapper supplies them |
| `ZONEPAIR_EXTERNAL_URL` | Drive an existing server with the mock suite instead of starting one |
| `ZONEPAIR_KEEP` | Preserve the harness temporary directory |
| `ANTHROPIC_BASE_URL` / `ANTHROPIC_AUTH_TOKEN` / `ANTHROPIC_MODEL` | Environment consumed by Claude in the real-model suite; the model is optional |

## Layout

```
src/bin/stub-server.rs   HTTP/WS listener, store RPC, Claude process relay
src/bin/stub-client.rs   SDK-facing transport, mirror synchronization, CLI
src/bin/zonepair-e2e.rs  Protocol and deployment harness
src/                    Framing, env allowlist, store, HTTP, process spawning
tests/                  Rust integration tests
scripts/                Image loading and Docker suite startup
```
