# No MCP crosses the machine boundary

Accepted, 2026-11-08. Retires the lab-proof mechanism of ADR-0003; leaves
ADR-0005 untouched.

The pair carried MCP across A↔B only for one exhibit: stock claude configured
with `--mcp-config`, whose helper process (the stub-client tunnel face) dialed
B's own loopback and got paired with an A-initiated WS. When that exhibit's
runner was deleted (ADR-0006 moved the suites to Rust), the machinery stayed
behind with no consumer: nothing in the tree opened `/v1/tunnel`, no test
exercised it, and the production OpenCode integration never used it — there
the provider bridges A's tools over the stdio control channel the relay
already carries (ADR-0003).

So the tunnel apparatus is gone: the `/v1/tunnel` route and its pairing queue,
the `side=a|b` protocol, the stub-client `tunnel` face, the `mcp-helper`
binary, and the `mcp.json` payload in the server image. The server image no
longer ships stub-client at all; `/usr/local/bin` holds stub-server only.

## Considered Options

- **Keep the apparatus for a revivable exhibit**: rejected — dead code with
  no executor, one image payload and one wire protocol kept alive on faith.
- **Re-add the exhibit over ws-native MCP** (claude `type:"ws"` mcp.json
  pointing at a serve endpoint; an A-side ws MCP server dialing it): rejected
  as a smaller but still consumerless apparatus; it is the shape to build if
  the exhibit comes back.
- **Community stdio↔HTTP MCP proxies**: wrong direction — those bridge an
  HTTP client to a spawned stdio server; the exhibit needed a stdio server
  that forwards outbound over the carrier.

## Consequences

- The workspace has four binaries: `stub-server`, `stub-client`, `claude-mock`,
  `zonepair-e2e`.
- Any future cross-machine MCP exhibit is new work plus a new ADR, not a
  resurrection of the retired one.
