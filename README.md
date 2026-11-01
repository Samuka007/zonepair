# claude-io-stub

A C/S stub pair that lets an agent harness (OpenCode / Claude Agent SDK) on
machine **A** drive a real `claude` binary running on machine **B**: stdio
relay + session-store projection + MCP, in Rust. Built to validate splitting
OpenCode's `claude-agent` provider across two machines.

> Not yet distributed — no installation instructions yet. Clone the repo and
> use the dev shell (`nix develop`) or a Rust toolchain.

Vocabulary: [CONTEXT.md](CONTEXT.md) · Protocol reference:
[docs/PROTOCOL.md](docs/PROTOCOL.md) · Decisions: [docs/adr/](docs/adr/) ·
Verification log: [docs/VERIFIED.md](docs/VERIFIED.md)

## Quickstart (mock claude)

Native — B emulated on 127.0.0.2, the second loopback address:

```bash
scripts/run-e2e-native.sh
```

Docker — two containers, one network, B named `b`:

```bash
scripts/run-e2e-docker.sh
```

Both run the same suite: basic turn, MCP round trip over the relay, 3
concurrent clients on one server, store RPC (exists/messages/fork/delete),
file-level fork verification, 3 concurrent forks, and the no-dial assertion.

## Real-model e2e (real claude, real provider)

Boots machine B with the genuine `claude` binary and drives a real model
backend through the relay:

```bash
scripts/run-e2e-docker-real.sh                 # WIRING=anthropic (default)
WIRING=deepseek scripts/run-e2e-docker-real.sh # DeepSeek <-> Claude Code bridge
```

Wiring is read from `~/.omp/agent/models.yml` (override with `MODELS_YML`) by
`mock/wiring-env.py`:

| WIRING | base URL | credential | model |
| --- | --- | --- | --- |
| `anthropic` (default) | models.yml `providers.anthropic.baseUrl` | `providers.anthropic.apiKey` | gateway-routed claude names |
| `deepseek` | `https://api.deepseek.com/anthropic` | `providers.deepseek.apiKey` | `deepseek-flash` |

Credentials travel only as container env → hello header (allowlisted
`ANTHROPIC_*`/`CLAUDE_*`) → claude child env; never written to the repo,
never printed. The suite asserts: marker echo through A → B → provider, the
real JSONL session landing in B's store, **fork of the real store + `--resume`
of the forked session reproducing the parent's marker**, and cleanup +
no-dial on B.

## MCP lab proof (tool runs on A)

```bash
node driver/sdk-driver.mjs --real --mcp --label mcptool
```

Claude spawns a stdio MCP helper on B (its `--mcp-config` behaviour); the
helper dials B's **own loopback** and gets paired with a pre-opened
A-initiated tunnel, so the tool implementation (`driver/mini-mcp-server.mjs`)
executes on A. The e2e proves it: claude reads `/work/secret.txt`, a file
that exists only on A. `ENABLE_TOOL_SEARCH=false` is required (claude ≥
2.1.316 otherwise hides MCP tools behind its ToolSearch meta tool). More
tools: extend `TOOLS` in `mini-mcp-server.mjs`; more servers: one
`mcpServers` entry + one queued tunnel each. Mechanism: ADR-0003.

## Manual poking

```bash
export CLAUDE_STUB_ENDPOINT=127.0.0.2:7788
stub-client sess fork session_aaa u-aaa-2   # pretty JSON, exit 2 on rpc error
```

## Hooking into OpenCode (zero patch)

Stock opencode, no source changes (mechanism: ADR-0004). On machine A:

```bash
export CLAUDE_STUB_ENDPOINT=b:7788
export OPENCODE_CLAUDE_PATH=/usr/local/bin/stub-client   # what the SDK spawns
export CLAUDE_CONFIG_DIR="$HOME/.claude-remote-mirror"   # A-local store mirror
export CLAUDE_STUB_MIRROR=1                              # sync around each turn
stub-client sync                                         # one-time bootstrap
```

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
| `WIRING` | `run-e2e-docker-real.sh` | `anthropic` (default) or `deepseek` |
| `MODELS_YML` | `run-e2e-docker-real.sh` | models.yml path (default `~/.omp/agent/models.yml`) |
| `CLAUDE_STUB_ENDPOINT` | stub-client, drivers | `host:port` of the stub-server |
| `STORE_HTTP_PORT` | real e2e | ws-line-v1 listener port on B (default 7789) |
| `B_CLAUDE_CMD` | docker mock suite | command the server spawns as "claude" (default `claude`) |
| `CLAUDE_CONFIG_DIR` | stub-client sync, opencode | A-local mirror of B's store |
| `CLAUDE_STUB_MIRROR` | stub-client | `1` enables push/pull sync around each relayed turn |
| `ANTHROPIC_BASE_URL` / `ANTHROPIC_AUTH_TOKEN` / `ANTHROPIC_MODEL` | real e2e | provider wiring (set via wiring-env.py, not by hand) |
| `ENABLE_TOOL_SEARCH` | MCP lab proof | must be `false`; see ADR-0003 |

## Layout

```
src/bin/stub-server.rs   accept loop, sess RPC, ws-line-v1 listener, per-connection claude spawn + relay
src/bin/stub-client.rs   transport mode (SDK-facing) + store mirror sync + sess CLI + tunnel helper
src/                     proto (line framing), envf (allowlist), store (JSONL + fork), http, claudeproc
tests/store.rs           fork/exists/messages/delete unit tests
mock/                    mock claude (transcript-persisting), store verifier, wiring/mirror helpers
driver/sdk-driver.mjs    SDK-side protocol driver used by all e2e suites
driver/mini-mcp-server.mjs  zero-dependency stdio MCP server for the lab proof
scripts/                 e2e runners, mirror-sync phase, no-dial assertion
docker/                  Dockerfile (server/client targets), entrypoint, mcp.json
```
