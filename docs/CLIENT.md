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

```bash
stub-client setup
```

Interactive on a terminal: prompts for the server URL, the bearer token
(hidden input; empty = none), and the mirror directory — each prefilled with
the currently resolved value. Scripts pass all three flags together; anything
partial fails without writing anything:

```bash
stub-client setup --url wss://zonepair.example.com --token "$TOKEN" --mirror "$HOME/.claude-remote-mirror"
stub-client setup --url ws://127.0.0.1:7789 --token "" --mirror "$HOME/.claude-remote-mirror"   # tokenless server
```

Values resolve as **flag > environment > client config file > built-in
default**. The file lives in the platform config directory at
`zonepair/client.toml` — `~/.config/zonepair/client.toml` on Linux
(`$XDG_CONFIG_HOME` honored), `~/Library/Application
Support/zonepair/client.toml` on macOS, `%APPDATA%\zonepair\client.toml` on
Windows — with `$ZONEPAIR_CLIENT_CONFIG` overriding the whole path.
Permissions are 0600, and it holds the relay knobs: server URL, bearer
token, mirror dir, mirror switch.
`setup` writes that file plus exactly two keys into the opencode **server's**
environment — the only two opencode itself consumes: `OPENCODE_CLAUDE_PATH`
(the provider knob) and `CLAUDE_CONFIG_DIR` (what the in-process store helpers
read). It bootstraps the mirror from B's store, then runs `doctor`.

`doctor --url URL` re-runs the preflight anytime. It checks the endpoint, the
server's `OPENCODE_CLAUDE_PATH`, opencode's version, and the mirror — because
the silent failure mode (opencode happily running a local `claude` straight at
Anthropic, no relay, no error) is the one that costs an afternoon.

`stub-client unset` erases the client config file, removes the two opencode
keys, and restarts opencode. It never contacts B and leaves the mirror
untouched. Without opencode on PATH it prints the removal commands.

`opencode auth login claude-agent` (or `/connect`) records which method you
chose; the actual Claude configuration lives entirely on B. The login flow
is intentionally not replaced: local `claude auth login` on A is outside
this integration.

### Agent SDK hosts other than opencode (e.g. OMP's claude-bridge)

The mirror only contains what was pushed from it or pulled into it. Anything
else that writes Claude session transcripts on A must write them **into the
same mirror directory** set by `setup`, or the relay cannot see those
sessions: the bridge writes a session id under its own root, then
`--resume` reaches B, which has no such conversation — every turn fails
with `No conversation found with session ID`.

OMP's claude-bridge derives its transcript root from `CLAUDE_CONFIG_DIR`
too, so point the **OMP process** (the one that spawns the bridge) at the
mirror. OMP loads `~/.omp/agent/.env` (and `~/.env`) on startup — put it
there rather than in a service env, so interactive and scripted runs agree:

```bash
# ~/.omp/agent/.env
CLAUDE_CONFIG_DIR=/home/you/.claude-remote-mirror
```

Note the two roles this one variable plays, and that both must agree:

- for **opencode** and the **bridge**, `CLAUDE_CONFIG_DIR` is "where the
  Agent SDK store helpers read/write session transcripts" (the mirror),
- for **stub-client itself**, the same variable is only a default
  mirror-dir candidate (`flag > env > file > default`, ADR-0008) —
  `sync` unions the mirror with B's store by uuid (append-only, both
  directions); it never computes transcript paths from a cwd.

opencode's in-process Agent SDK store helpers (`sessionExists` / `fork` /
`remove`) read `CLAUDE_CONFIG_DIR` from the process environment, so they run
on the mirror as if it were a local claude store. The spawned stub-client
pushes A-side forks to B before each turn and pulls B's new messages back
after it. To synchronize separately:

```bash
stub-client sync
```

## Migrating from rc.5-and-earlier installs

Older setups stored the four relay variables in the opencode server env. The
relay still honors environment variables — they outrank the client config
file — so stale server-env keys keep shadowing the file until removed:

```bash
stub-client setup \
  --url "$(opencode service get env CLAUDE_STUB_URL)" \
  --token "$(opencode service get env CLAUDE_STUB_TOKEN)" \
  --mirror "$(opencode service get env CLAUDE_CONFIG_DIR)"
opencode service unset env CLAUDE_STUB_URL
opencode service unset env CLAUDE_STUB_TOKEN
opencode service unset env CLAUDE_STUB_MIRROR
```

`setup` rewrites `CLAUDE_CONFIG_DIR` itself, so the third flag only matters
if you want to relocate the mirror while migrating.

## Environment reference

| Variable | Meaning |
| --- | --- |
| `CLAUDE_STUB_URL` | Server base URL. `ws://` / `http://` for plain transport; `wss://` / `https://` for TLS, including gateway path prefixes. Overrides the file. |
| `CLAUDE_STUB_TOKEN` | Bearer token matching the server's `TOKEN`. Overrides the file. |
| `CLAUDE_STUB_MIRROR` | `1` forces push/pull sync around each relayed turn, regardless of the file's `mirror` flag. |
| `CLAUDE_CONFIG_DIR` | A-local mirror of B's store. Opencode's SDK helpers read it from their process environment; an Agent SDK host writing transcripts elsewhere (OMP's claude-bridge) must have it pointed at the same mirror. |

An empty/absent token at every layer means no `Authorization` header is sent;
the server accepts that only when its own `TOKEN` is empty.

See [server deployment](../README.md), [protocol reference](PROTOCOL.md),
and [ADR-0008](adr/0008-client-config-file.md) for why the config file exists.
