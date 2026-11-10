# OpenCode integration (machine A)

Stock opencode, no source changes (mechanism: [ADR-0004](adr/0004-zero-patch-via-mirror.md)).

## Requirements

opencode **v2 ≥ 2.2.3** — that is where the "Claude Agent" provider lands, and
v1 has no such provider, so `OPENCODE_CLAUDE_PATH` means nothing there.
`claude` on B must be ≥ 2.1.248 (opencode passes SDK options older builds reject).

## Install

The client is one binary. Download the appropriate binary from
[Releases](https://github.com/Samuka007/zonepair/releases), then install it as
`~/.local/bin/stub-client`.

macOS: a browser-downloaded release asset carries the quarantine flag and
Gatekeeper blocks its first run. One-time fix (ad-hoc signed, like every
nixpkgs darwin binary — no notarization):

```bash
xattr -d com.apple.quarantine ~/.local/bin/stub-client
```

## Configure

These variables are read by the opencode **server**, and the stub-client it
spawns inherits the server's environment. Set them through opencode rather
than your shell: the server is often started by its own daemon, which never
sees your exports.

```bash
opencode service set env OPENCODE_CLAUDE_PATH "$HOME/.local/bin/stub-client"
opencode service set env CLAUDE_STUB_URL ws://b:7789
opencode service set env CLAUDE_CONFIG_DIR "$HOME/.claude-remote-mirror"
opencode service set env CLAUDE_STUB_MIRROR 1
```

All four are what `stub-client setup` writes:

```bash
stub-client setup --url ws://b:7789
stub-client doctor --url ws://b:7789
stub-client unset
```

`setup` creates and bootstraps the mirror, then runs doctor. `unset` removes
the server environment (plus a leftover PROBE), restarts opencode, never
contacts B, and leaves the mirror untouched. Without opencode on PATH it
prints the removal commands.

`doctor` exists because the failure mode is silent: if the opencode server
never sees `OPENCODE_CLAUDE_PATH`, opencode happily runs a local `claude` that
talks to Anthropic directly — no relay, no error. It checks the endpoint's
store RPC, that the server's `OPENCODE_CLAUDE_PATH` really points at a
stub-client, that opencode is ≥ 2.2.3, and that the mirror is populated.

`opencode auth login claude-agent` (or `/connect`) records which method you
chose; the actual Claude configuration lives entirely on B. The login flow
is intentionally not replaced: local `claude auth login` on A is outside
this integration.

opencode's in-process Agent SDK store helpers (`sessionExists` / `fork` /
`remove`) read `CLAUDE_CONFIG_DIR` from the process environment, so they run
on the mirror as if it were a local claude store. The spawned stub-client
pushes A-side forks to B before each turn and pulls B's new messages back
after it. To synchronize separately:

```bash
stub-client sync
```

## Environment reference

| Variable | Meaning |
| --- | --- |
| `CLAUDE_STUB_URL` | Server base URL. `ws://` / `http://` for plain transport; `wss://` / `https://` for TLS, including gateway path prefixes |
| `CLAUDE_STUB_TOKEN` | Bearer token matching the server's `TOKEN` |
| `CLAUDE_STUB_MIRROR` | `1` enables push/pull sync around each relayed turn |
| `CLAUDE_CONFIG_DIR` | A-local mirror of B's store |

For a token-protected server, set `CLAUDE_STUB_TOKEN` in both the shell
running setup/sync and the opencode server's environment.

See [server deployment](../README.md) and [protocol reference](PROTOCOL.md).
