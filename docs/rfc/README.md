# zonepair RFC Index

This directory holds the **finalized designs**: the designs that converged after multiple rounds of audit, ruling, and review. When reading or implementing the design, this directory is authoritative; the audit trail of the design process is archived and not kept in the repository (the local `tmp/` workspace retains it).

## Document list

| Number | Title | Topic | Status |
|---|---|---|---|
| [RFC 0001](rfc-0001-sync-protocol.md) | Session sync protocol | The complete wire contract between A↔B: sync unit, cursor, the `changes` and `publish` operations | Final |
| [RFC 0002](rfc-0002-a-state.md) | A-side persistent state and session classification | `state.json` schema, field semantics, combined-decision classification, materialization tiers (full/stub/unloaded), crash recovery and corruption degradation | Final |
| [RFC 0003](rfc-0003-zonepaird.md) | A-side sync process `zonepaird` | Process model, journal intent channel, event sources, task model, materialization write sequence (evict/hydrate), the two waits, crash recovery, argv adaptation layer, `prune`/`hydrate` command surface | Final |
| [RFC 0004](rfc-0004-b-endpoints.md) | B-side sync endpoint implementation contract | The two routes, the server-side cursor interpretation algorithm, the stable boundary, the fingerprint cache, atomic write and lock shapes, version compatibility | Final |

## Reading order

Read RFC 0001 first. It defines the wire protocol's entire behavior — what the unit of sync is, what a cursor is, and each endpoint's reply semantics. After RFC 0001, you will know what every byte exchanged between A and B means.

RFC 0002 and RFC 0003 cover A's internal mechanisms: 0002 defines "what is recorded, what each field means, and what happens when things break", and 0003 defines "who writes, who advances, who waits". 0002 is the foundation of 0003 — read 0002 first.

RFC 0004 covers the B-side implementation: how the server interprets cursors, how `changes` is kept cheap enough, and how the write path and locks are arranged. It depends only on RFC 0001 and can be read independently.

## Dependency graph

```text
RFC 0001 (protocol contract)
   ├── RFC 0002 (A-side state)──┐
   ├── RFC 0003 (A-side process)─┤ 0002/0003/0004 each depend only on 0001's
   └── RFC 0004 (B endpoints)───┘ contract; none defines another's internal implementation
```

## Provenance

Each RFC's design rationale lives in the process archives (the local `tmp/designs/` workspace, not in the repository). RFC text does not cite process numbers — an RFC is a final design and its reasons are written into the body.
