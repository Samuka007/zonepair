# Protocol & implementation reference

Design decisions and their rationale live in `docs/adr/`; the vocabulary in
`GLOSSARY.md`. This file is the technical reference: wire formats, store
format, and the traps we hit so you don't have to.

## Server configuration

`stub-server` takes flags, and each flag has an environment fallback so a
container image can run the bare binary (`Cmd = ["stub-server"]`) with no
wrapper/entrypoint script. A flag always wins over the environment; an empty
value counts as unset.

| Option | Env fallback | Default |
| --- | --- | --- |
| `--listen H:P` | `LISTEN` | `0.0.0.0:7789` |
| `--token V` | `TOKEN` | off |
| `--store DIR` | `STORE`, then `CLAUDE_CONFIG_DIR` | `$HOME/.claude` |
| `--claude "CMD ARG ..."` | `CLAUDE_CMD` | `claude` |
| `--log-dir DIR` | `LOG_DIR` | `./stub-logs` |

## Connection dispatch

The WS route chooses the protocol: `/v1/stdio` is `stdio-v1`, the
`/v1/store/...` endpoints are `sess-v1`. After the route, `stdio-v1` starts
with one hello frame.

### stdio-v1 (one per claude process)

```
A->B  {"proto":"stdio-v1","argv":[...],"env":{...allowlisted...},"cwd":"..."}
B->A  {"ok":true}                        // or {"ok":false,"error":...}
A<->B raw bytes: A-stdin -> child.stdin, child.stdout -> A-stdout
```

* `env` is an allowlist (`ANTHROPIC_*`, `CLAUDE_*`, `DISABLE_*`, `ENABLE_*`,
  `MCP_*`, `MAX_MCP*`, `SSL_CERT*`, `REQUESTS_CA*`, `CURL_CA*`, plus
  `HTTP(S)_PROXY`, `NO_PROXY`, `ALL_PROXY`, `API_TIMEOUT_MS`) — see
  `src/envf.rs`.
* The server pins `CLAUDE_CONFIG_DIR` to `--store` for the child, so a real
  `claude` writes its session JSONL where the projection RPC reads.
* Child stderr is NOT multiplexed into the pristine stdio stream; it goes to
  `--log-dir/stderr-<conn#>.log`. Child exit codes are not propagated (the
  Agent SDK communicates outcomes via protocol JSON); the client exits 0
  after the relay, non-zero only on connect/ack failure.
* When A closes the socket, B closes the child's stdin and `kill_on_drop` is
  armed — no orphaned claude processes.

### sess-v1 (stateless store RPC)

The store operations — exists, messages, fork, delete, list, merge — ride the
plain-HTTP endpoints in the `ws-line-v1` table below. Responses are the same
JSON shapes the ops always had (`{"ok":true,...}` / 404 / 401).

### ws-line-v1 (HTTP/WS listener)

The single HTTP/WS listener carries every protocol; front it with a gateway
for TLS and auth. Why: ADR-0005.

| Endpoint | Shape | Notes |
| --- | --- | --- |
| `GET /v1/store/sessions/{id}` | HTTP JSON | `{"exists":true}` / 404 |
| `GET /v1/store/sessions/{id}/messages` | HTTP JSON | `{"ok":true,"messages":[...]}` |
| `PUT /v1/store/sessions/{id}/messages` | HTTP JSON, body `{"messages":[...]}` | `{"ok":true,"added","total"}` — append-only union by uuid (mirror sync write path, ADR-0004) |
| `GET /v1/store/sessions` | HTTP JSON | `{"ok":true,"sessions":["S",...]}` |
| `POST /v1/store/sessions/{id}/fork` | HTTP JSON, body `{"upTo":uuid}` | `{"sessionId","pairs","messages"}` |
| `DELETE /v1/store/sessions/{id}` | HTTP JSON | `{"deleted":bool}` |
| `WS /v1/stdio` | text frame = one protocol line | first frame: hello `{"proto":"stdio-v1",argv,env,cwd}` → ack `{"ok":true}`; control frames use the `{"proto":...}` namespace (`stdin-eof` closes the child's stdin) |

Binary frames are a protocol violation (server warns and ignores). Optional
`--token TOKEN` enforces `Authorization: Bearer` on every route — real
auth/TLS/device management is expected to come from the gateway in front.

The client uses reqwest with the same verified rustls configuration as its
WebSocket transport. HTTP framing and response-body completion are handled
by the library: chunked responses are decoded and truncated bodies fail
before JSON parsing. Store requests do not automatically redirect or retry.

## Session store format (projection backend)

`<store>/projects/**/<sessionID>.jsonl`, one JSON object per line with at
least `uuid`, `sessionId`, optional `parentUuid`, plus payload fields. `fork`
mirrors the Agent SDK semantics: copy the prefix up to and including `upTo`,
assign a fresh session id, fresh uuids for every copied line, rewrite the
`parentUuid` chain (first line keeps its original parent), leave the source
untouched. Unit-tested in `tests/store.rs`.

**Production note:** this is a re-implementation for the lab. For a real
deployment the fork should be executed by the Agent SDK on machine B instead;
the RPC surface exists so machine A never touches B's filesystem.

## Constraints as implemented

| Constraint | Implementation | Proof |
| --- | --- | --- |
| C/S only — B never dials A (ADR-0001) | `stub-server` only `accept()`s; no outbound `connect` in the binary | the Rust harness fails if the server process owns any connection whose local port is not the listener (runs natively and in-container) |
| Multiple clients, one server | Every stdio connection gets its own `claude` child (argv/env/cwd from that connection's hello header); session RPC is stateless over a shared, mutex-guarded store | e2e runs 3 concurrent relay clients with distinct sessions through one server, plus 3 concurrent forks of one source |
| Tools execute on A (ADR-0003) | `mcp__opencode__*` bridged over the stdio control channel; no MCP crosses the machine boundary (ADR-0007) | the mock suite checks the tool_use/tool_result shapes in-band; the real-model suite drives them against a live model |

## Real-claude quirks handled

* `[claude-code:unrecognized_model]` warnings are printed on **stdout** next
  to protocol JSON — the driver skips non-JSON lines.
* `--verbose` stream-json re-emits `init`; the user turn is fed once.
* tokio's `ChildStdin::shutdown()` only flushes, it does NOT close the pipe —
  real EOF for claude requires dropping the write end.
* The server pins the child's `CLAUDE_CONFIG_DIR` to `--store` **after**
  merging the hello header env — a header carrying A's own value (e.g. the
  mirror dir) must not redirect where transcripts land.

## Build/deploy traps

* A nix `dockerTools` image is not a base image. It ships no
  `/etc/nsswitch.conf` (glibc then cannot use DNS — the client could not
  resolve service `b`), no `/usr/bin/env` and no `/bin/sh` (so
  `#!/usr/bin/env bash` scripts fail to exec), and no writable `/tmp`
  (`mktemp` fails). The flake adds `fakeNss`, `usrBinEnv`, `binSh` and a 1777
  `/tmp`.
* rustc bakes its own sysroot path into std panic locations, which drags the
  whole fenix toolchain into the binary's closure (1.15 GB images). The build
  remaps it away with `--remap-path-prefix=<toolchain>=/rustc`.
