# Machine B never dials machine A

The stub-server must work with inbound `accept()` only — no outbound
`connect()` anywhere in the binary. Everything a component on B needs from A
is either pushed in through the connection A opened (stdio bytes, hello
header) or pulled by a helper that dials **B's own loopback** (the MCP
tunnel's side=b).

A direct B→A dial would be simpler for the MCP helper, but it would make B
network-reach A, which breaks the deployment story this lab validates: B sits
behind a NAT/gateway boundary, only A can initiate, and the whole point of
the exercise is that the claude zone stays closed. The invariant is enforced,
not aspirational: `scripts/assert-nodial.sh` fails any e2e suite where the
server process owns a connection whose local port is not its listen port.

## Consequences

- The MCP tunnel needs a pre-opened, A-initiated WS plus a B-loopback helper
  instead of one direct dial (see ADR-0003).
- Any future feature that "just dials back to A" is a design violation, not an
  implementation detail.
