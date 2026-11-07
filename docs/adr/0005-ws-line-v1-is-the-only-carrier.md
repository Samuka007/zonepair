# The stub pair speaks only ws-line-v1

Accepted, 2026-11-06. Supersedes the "two listeners" half of ADR-0002.

A↔B must be an HTTP-based protocol so that infrastructure in front — Cloudflare,
an mTLS terminator, an oauth2 proxy, a WAF — can do TLS, authentication and
policy. `tcp-line-v1` has no request boundaries: nothing in front of it can
route, authenticate or even observe it, so protecting the pair would mean
protecting only the half of the traffic that happens to be HTTP. We therefore
make `ws-line-v1` the only carrier and delete `tcp-line-v1`:

- `stdio-v1` rides a WS stream, one protocol line per text frame.
- `sess-v1` rides plain HTTP (the store endpoints).
- the tunnel is unchanged (WS pump, side=a initiated by A, side=b from B's own
  loopback).

Protocols keep their names and semantics; only the carrier changes. The
stub-client transport mode and sess CLI stop opening raw sockets.

## Consequences

- **ADR-0001 still holds.** Every connection is still A-initiated: WS
  handshakes and HTTP requests are accepted by B; B never dials A. Nothing here
  reintroduces an outbound dial, and `assert-nodial.sh` keeps enforcing it.
- **TLS is the deployment's job, not the binary's.** The client must be able to
  dial `https://` / `wss://`, which today it cannot: `tokio-tungstenite` is
  built without a TLS feature and `CLAUDE_STUB_ENDPOINT` is a bare `host:port`
  with no scheme. Both must change for a gateway-terminated deployment to work.
- **The bearer token becomes a fallback.** With a gateway in front, auth is
  asserted by the gateway; `--http-token` remains for direct/loopback use.
- **Endpoint becomes a URL.** One setting has to express scheme + host + path
  prefix (gateways mount services under a path), not just host and port.
- **Mock suites lose their byte-transparent story.** They exercise the same
  protocols, now framed per line; anything relying on raw-byte passthrough over
  a socket goes away with the carrier.
