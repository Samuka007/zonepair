# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`GLOSSARY.md`** at the repo root
- **`docs/adr/`**: read ADRs that touch the area you're about to work in

If any of these don't exist, **proceed silently**. Don't flag their absence; the `/domain-modeling` skill creates them lazily when terms or decisions actually get resolved.

## File structure

Single-context repo:

```
/
├── GLOSSARY.md
├── docs/adr/
└── src/
```

## Use the glossary's vocabulary

The glossary's canonical terms (and their `_Avoid_` lists) are binding: use them in issue titles, refactor proposals, hypotheses and test names. In particular, don't drift between the sanctioned carriers (`tcp-line-v1`, `ws-line-v1`) and the sanctioned protocols (`stdio-v1`, `sess-v1`) — they are different axes (see `GLOSSARY.md` → **carrier**).

If the concept you need isn't in the glossary yet, that's a signal: either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0002 (ws-line-v1 as a second listener), but worth reopening because…_

Note that ADR-0005 supersedes the "two listeners" half of ADR-0002.
