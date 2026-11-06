# claude-io-stub

A lab for driving a `claude` binary that lives on a different machine than
the agent harness. One context: the stub pair, its protocol, and the e2e
harness around it.

## Language

**Machine A**:
The zone where the agent harness (OpenCode / Agent SDK) runs. It spawns the
stub-client and never touches machine B's filesystem.
_Avoid_: client side, local

**Machine B**:
The zone where the real `claude` binary runs, spawned and owned by the
stub-server. It only ever accepts connections.
_Avoid_: server side, remote (use it only with a qualifier, e.g. "remote store")

**stub-server**:
The B-side daemon. Accepts client connections, spawns/owns `claude` children,
and owns the session store.
_Avoid_: daemon, relay (relay is what the pair collectively does)

**stub-client**:
The A-side process the SDK spawns instead of `claude`. Two faces: transport
mode (long-lived stdio relay with mirror sync) and sess CLI (one-shot store
RPC).
_Avoid_: proxy

**stdio-v1**:
The raw-byte transport protocol: one hello header line, one ack line, then
transparent byte piping for the lifetime of one claude process.
_Avoid_: raw mode, tcp mode

**sess-v1**:
The stateless JSON-lines RPC for the session store: exists, messages, fork,
delete, merge, list.
_Avoid_: store RPC, projection RPC

**carrier**:
Which listener surface carries a protocol — `tcp-line-v1` or `ws-line-v1`.
Protocols (stdio-v1, sess-v1) are defined independently of the carrier; a
carrier is a choice of how those lines reach the server, not a protocol.
_Avoid_: transport mode, wire mode (they name the stub-client mode, not the surface)

**tcp-line-v1**:
The raw-TCP listener surface: a hello line, an ack line, then the protocol's
lines or byte stream directly on the socket. Has no request boundaries, so no
gateway can route, authenticate or observe it.
_Avoid_: raw listener, plain TCP, tcp mode

**ws-line-v1**:
The HTTP/WS listener surface: the store operations as plain HTTP endpoints,
stdio-v1 re-framed as one text frame per protocol line, and the tunnel as a
paired WS pump.
_Avoid_: v2, http mode (it is a second listener, not a replacement)

**tunnel**:
A pre-opened, A-initiated WS that carries one MCP JSON-RPC line per frame,
paired with a B-loopback helper so tool execution happens on A.
_Avoid_: reverse proxy, port forward

**hello header**:
The first line a client sends: proto name plus argv/env/cwd for the child to
spawn. Env keys are filtered by the allowlist.
_Avoid_: handshake (the ack is part of stdio-v1, not a separate thing)

**store**:
The JSONL session directory (`<store>/projects/**/*.jsonl`) that the server
pins as `CLAUDE_CONFIG_DIR`, so a real claude's own session files ARE the
projected state.
_Avoid_: database, cache

**fork**:
A store copy of a session prefix under a fresh session id with fresh uuids
and a rewritten parentUuid chain, mirroring Agent SDK semantics.
_Avoid_: branch, clone

**tunnel side**:
Which end of a paired WS connection initiates: side=a queues from machine A,
side=b pairs from B's own loopback (the MCP helper claude spawns).
_Avoid_: upstream/downstream

**wiring**:
The provider credential bundle (ANTHROPIC_BASE_URL / ANTHROPIC_AUTH_TOKEN /
optional model overrides) emitted by `mock/wiring-env.py` from models.yml.
_Avoid_: provider config, credentials (credentials are the values; wiring is
the resolved bundle)

**no-dial assertion**:
The e2e check that the server process owns no outbound connections — the
invariant that machine B never dials machine A.
_Avoid_: network test, ss check

**mirror**:
The A-local copy of B's store at `CLAUDE_CONFIG_DIR`, where opencode's
in-process store helpers do their file work.
_Avoid_: cache (it is the working copy the SDK mutates), replica

**sync**:
The append-only union of the mirror and B's store, by uuid, in both
directions — around each turn automatically, or on demand via
`stub-client sync`.
_Avoid_: replication, backup

**lab proof**:
A driver mode that demonstrates a production question in miniature (e.g.
`--mcp` proves A-side tool execution) without implying the production shape.
_Avoid_: demo, experiment (they don't name the proven property)
