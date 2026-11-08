# zonepair

Split a coding-agent's model driver across two machines: machine A runs the
harness (opencode) and never touches machine B; machine B runs the real
`claude` and only ever accepts connections.

See `README.md` for usage, `GLOSSARY.md` for vocabulary, `docs/adr/` for
decisions.

## Agent skills

### Issue tracker

GitHub Issues on `Samuka007/zonepair`, driven with the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical roles, each label string equal to its name (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `GLOSSARY.md` at the repo root plus `docs/adr/`. See `docs/agents/domain.md`.

## Testing

- Tests are Rust: unit tests beside the code, integration tests in `tests/`.
  `nix flake check -L` runs everything that runs natively.
- Mocks (`claude-mock`, the MCP helper) and protocol drivers are Rust binaries
  in this workspace — see ADR-0006.
- Do **not** add bash/python/node test files. bash may start containers and
  exec the Rust harness; it must not contain assertions.
- Container e2e: build/loads the images, then execs the same Rust harness
  inside them (see `scripts/run-e2e-docker.sh`).

## Rust dependencies

- Add, remove, or move versions with `cargo add` / `cargo remove` /
  `cargo upgrade`, never by hand-editing `Cargo.toml` version strings —
  cargo is the only thing that talks to crates.io; a hand-written number is
  a guess from memory. Background: issue #3's audit caught a wrong version
  recalled from memory.
- Before adopting a crate, check its docs.rs page for a maintenance-status
  disclaimer (e.g. `home` declares itself Cargo-team-internal, "not
  intended for external use"). Prefer `std` / `dirs` when the dedicated
  crate disclaims external use.
- Keep version strings at the loosest semver that builds (`"1"`, `"0.5"`):
  `Cargo.lock` is the pin, `Cargo.toml` is the range. Outdated majors are
  tracked in issue #3, not pinned away.

