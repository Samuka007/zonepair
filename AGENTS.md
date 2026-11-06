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
