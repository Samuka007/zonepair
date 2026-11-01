# Zero-patch OpenCode via an A-side store mirror

Accepted, 2026-11-01.

OpenCode must run **unmodified** against the stub. Stock `io.ts` reaches
outside the spawn in exactly three places — `sessionExists`, `fork`, and
`remove` — and all three delegate to Agent SDK helpers that, without a
`sessionStore` option, operate on the **local filesystem** at
`CLAUDE_CONFIG_DIR` (the standalone helpers read the process environment;
confirmed against the Agent SDK session-storage docs). We make "local" true
without it being B:

- Machine A exports `CLAUDE_CONFIG_DIR=<mirror>`; opencode's in-process SDK
  helpers read and fork real files — in the mirror.
- `OPENCODE_CLAUDE_PATH=stub-client` (an existing official knob): the
  spawned client pushes the mirror's new sessions to B before the turn and
  pulls B's new messages into the mirror after it (`CLAUDE_STUB_MIRROR=1`),
  or does a full round trip on demand via `stub-client sync`.
- The login flow is not replaced; whatever local `claude auth login`
  produces on A is treated as undefined behavior by this lab.

The sync primitive is an append-only union by uuid (server `merge`, client
mirror of the same rule), which keeps the per-file line order the SDK's fork
verification checks. Related invariant discovered en route: the server must
pin `CLAUDE_CONFIG_DIR` **after** merging the hello header's env, or A's
mirror path leaks into the child and the B-side store silently stops
growing.

## Considered Options

- **Patch opencode** (`remote()`/`select()` in io.ts redirecting the three
  helpers to sess RPC): worked, but every opencode rebase re-pays the cost;
  rejected by requirement.
- **Agent SDK `sessionStore` adapter**: the clean production answer, but
  opencode does not pass one and adding it is — a patch.
- **FUSE/network filesystem projecting B's store onto A**: no code changes
  either, but drags a kernel module and a network dependency into the
  deployment for what a per-turn sync satisfies.

## Consequences

- A fresh machine A with a pre-existing B store starts with an empty mirror;
  run `stub-client sync` once to bootstrap.
- `deleteSession` on A removes the mirror file only; B keeps the session
  until a real deletion op is added to the sync.
