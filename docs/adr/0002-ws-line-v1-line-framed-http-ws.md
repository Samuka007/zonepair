# ws-line-v1: line-framed HTTP/WS listener alongside raw TCP

Accepted, 2026-10-31.

The raw TCP listener is hostile to real deployments: session-store RPC and
stdio multiplexing share one port with no request boundaries, so no gateway
(CF Access, oauth2-proxy, mTLS terminator, WAF) can route or authenticate it.
We added a second listener (`--http-listen`) with three properties:

1. The store (sess-v1 ops) becomes **plain HTTP** endpoints — every proxy and
   browser can speak it.
2. stdio-v1 is re-framed as **WS text frames, one protocol line per frame** —
   requests get IDs so order is verifiable, frames are the unit of
   observation, and half-delivered lines are impossible.
3. The MCP tunnel becomes a **paired WS pump** (`/v1/tunnel?side=a|b`).

## Considered Options

- **SSE**: rejected — half-duplex; the stdio path needs A→B stdin frames on
  the same connection.
- **gRPC**: rejected — needs codegen on both ends and its own port conventions
  for exactly four endpoints and one pump; WS gives the same properties with
  zero dependencies.
- **Replace the raw listener**: rejected for now — the mock suites and the
  byte-transparent relay story depend on stdio-v1 over TCP; both listeners
  share one store/process state so nothing is duplicated.

Binary frames on ws-line-v1 are a protocol violation (server warns and
ignores), which keeps accidental binary clients loudly visible instead of
silently corrupting the stream.

## Consequences

- An optional bearer token (`--http-token`) protects every route, but real
  auth/TLS/device management is expected to come from the gateway in front.
- The wire protocol now has two shapes for the same store; docs must keep
  them semantically identical (see docs/PROTOCOL.md).
