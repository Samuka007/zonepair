# The client config file over the opencode service env

Accepted, 2026-11-10.

stub-client's relay knobs (server URL, bearer token, mirror dir, mirror
switch) are durable state. Through rc.5 they lived in the opencode server's
stored environment (`opencode service set env …`), which made every bare
`sync`/`sess` shell out to opencode, hid the values behind a CLI whose argv
shape drifts between opencode releases, and offered no way to display what
was actually stored. A machine-A incident on rc.5 spent five commands
guessing at one opaque 401 because neither the configuration nor the auth
failure was observable anywhere.

## Decision

- One TOML file owned by zonepair: `zonepair/client.toml` under the platform
  config directory (`~/.config/zonepair/client.toml` on Linux via the XDG
  base dirs, `~/Library/Application Support` on macOS, Roaming AppData on
  Windows; `$ZONEPAIR_CLIENT_CONFIG` overrides the path), permissions 0600,
  written atomically by `setup`, erased by `unset`. Resolution order
  everywhere: flag > process environment > file > built-in default.
- The opencode server env keeps only the two keys opencode itself consumes:
  `OPENCODE_CLAUDE_PATH` and `CLAUDE_CONFIG_DIR`. It is not a read source.
- `setup` is interactive on a TTY (currently resolved values prefilled, token
  input hidden) or all-or-nothing with flags (`--token ""` = none); partial
  input fails before the first side effect.
- `unset` does not touch legacy `CLAUDE_STUB_*` keys in the opencode env:
  migration from pre-file installs is manual (docs/CLIENT.md). The
  environment layer keeps outranking the file so a not-yet-restarted daemon
  and a not-yet-migrated install keep working.

## Considered options

- **Keep the opencode service env as the store, add read-back verification**:
  still shells out per bare command, still drift-coupled to opencode's CLI,
  and the values remain invisible to anything but opencode.
- **Auto-migrate legacy keys inside `setup`**: rejected — making setup a
  janitor for state the new model does not read; the manual snippet is three
  commands and stays in the user's control.
- **Keyring / OS secret store for the token**: rejected — cross-platform pain
  and a new dependency for the same exposure the token already had in the
  opencode env.

## Consequences

- Bare `sync`/`sess`/`doctor` never spawn opencode; opencode version drift is
  confined to the two remaining keys.
- A stale legacy `CLAUDE_STUB_URL` in the opencode env silently outranks the
  file until the migration snippet removes it — accepted, because the spawned
  relay child still inherits that env and must keep working pre-migration.
- `CLAUDE_STUB_TOKEN` still rides the hello-header env allowlist
  (`envf.rs`) to B's claude child; narrowing that allowlist is separate,
  still-open work.
