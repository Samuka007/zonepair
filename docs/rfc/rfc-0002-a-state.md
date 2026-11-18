# RFC 0002 — A-Side Persistent State and Session Classification

| Field | Value |
|---|---|
| Status | Final |
| Date | 2026-11-17 |
| Scope | zonepair machine A's persistent state: `<mirror>/.sync/state.json`, session classification, crash recovery and corruption degradation |
| Depends on | RFC 0001 (cursor encoding, changes/publish semantics) |
| Depended on by | RFC 0003 |
| Process archive | `tmp/designs/` (for provenance only) |

## 1. Design overview

All of A's persistence can be stated in one sentence:

> **A persists exactly one file, `state.json`, holding one record per session with eight fields. Take any `<sid>.jsonl` sitting in the mirror, run the joint judgment over this record's fields, and you can uniquely read which of "normal pull / pending publish / published / deleted / lost on B / orphan" it belongs to — there is no ninth category.**

Breaking that sentence down gives four parts:

- **What is persisted** (§3): one file, one `sessions` table, one record per session, eight fields — no second place of persistence.
- **How a session's state is judged** (§5, §6): there is no "state" field; state is read out of the combination of fields in the record, and the deletion record is consulted first.
- **How fork products are recorded** (§4): a fork file advances through `task` until publish completes, after which confirmation converts into `pull.cursor`.
- **What happens on error** (§7, §8): crash = truncation or re-pull; corrupted state file = quarantine the bad file + partitioned degradation, never silent zeroing.

Two principles run through everything: **field combination is classification** (what state a session is in is read from the joint judgment of the record's fields; no redundant second copy of "state" is stored) and **corruption does not disguise as success** (a broken state file must degrade explicitly; it cannot silently zero and pretend normal — losing facts like `publishedAt` and tombstones, "who did what", distorts the truth, and fabricating them is lying to the recovery logic).

## 2. Why these eight fields

A being nearly stateless is a hard constraint. Every one of the eight fields must answer a real question; none can be removed:

| Field | The question it answers | What happens if removed |
|---|---|---|
| `pull` | How many bytes has this session synced to | Incremental sync loses its breakpoint, degrading to full pull every time |
| `pullPending` | Are pull requests still queued, and what is the target | After a crash the target of "confirmed pull requests" is lost; only blind resend via reconciliation remains; deleted in the same transaction upon satisfaction (§9) |
| `task` | Which phase has this fork's publish reached | The publish wait has no authoritative data source; after a crash it cannot answer "did it get sent" |
| `publishedAt` | Is this session one A published | After the task row is cleaned up, the "published" fact has no evidence; a hole appears in the classification table that cannot be judged |
| `deleted` | Was this session deleted, and which file instance was deleted | A deleted file resurrects via pull/rename; cannot distinguish "B lost the file" from "the user deleted the file" |
| `registered` | Was this id registered on B but has produced no data yet | A local file in the first-pull crash window gets misjudged as A-managed and sent to publish |
| `lastUnchangedAt` | When was the last time it was confirmed unchanged | `sync --wait` and freshness reports lose a decidable lower bound |
| `materialized` | What form do this session's bytes take locally | stub/unloaded cannot be distinguished from "modified/lost"; reconciliation misjudges intentional trimming as corruption and automatically pulls back the full body |

Conversely, any information not in this list must not be persisted — what can be asked of B is not stored on A (file content itself), what can be computed on the fly is not persisted (classification results), and clocks are not correctness criteria (all wall-clock fields are observation and ordering hints only).

## 3. `state.json` Schema

### 3.1 Layout and commit method

```text
<mirror>/
  .sync/                    # directory permission 0700; all files beneath 0600
    owner.lock              # single-writer lock (holding semantics in RFC 0003 §3)
    journal/                # intent record directory (protocol in RFC 0003 §4)
    state.json              # sole authoritative data: the entire content of this file
    state.corrupt-*         # corruption quarantine area (§8)
  projects/**/<sid>.jsonl   # session file copies; files persisted by pull are 0600
```

`state.json` is written by the single writer (RFC 0003's zonepaird). Each commit is a complete `temp file → fsync → atomic rename → directory fsync`; **the transaction unit is one file write**, and a single commit may update any combination of fields of the same session. The largest single-transaction shape is the publish-confirmation commit: `phase=published` + `publishedAt` + `pull`, three fields landing together.

**The write-order iron rule precedes everything**: before any `pull.cursor` advance, the corresponding mirror file write must have completed fsync-level persistence (incremental is end-append + fsync; whole-file replacement is temp file + rename + directory fsync). Reversing the order makes post-crash progress overtake actually-persisted content; the recovery logic would read a lie.

### 3.2 Complete schema

```jsonc
// <mirror>/.sync/state.json —— single-writer sole writer; replaced via temp file + atomic rename
{
  "version": 3,
  "sessions": {
    "<sessionId>": {
      "pull": {                                   // sync progress of a B-managed session
        "cursor": "zpc1.34567.d3c9f1a0…",          // opaque string per RFC 0001 §5; A reads only the byte-count segment as length anchor
        "file": "projects/lab/<sid>.jsonl"         // copy location on disk, mirror-relative path
      },
      "pullPending": {                            // pull-enqueue intent (pending target)
        "target": "latest" | "<cursor string>",
        "enqueuedAt": "<RFC3339 timestamp>"
      },
      "task": {                                   // pending-publish task, publish-only slot
        "file": "projects/lab/<sid>.jsonl",        // file to publish, relative path; does not assume projects/lab
        "phase": "writing" | "validated" | "published" | "degraded",
        "firstSeen": "<RFC3339>, first-detection time",  // observation field
        "enqueuedAt": "<RFC3339>, enqueue time",
        "lastError": "…?"                          // most recent failure reason
      },
      "publishedAt": "<RFC3339>?",                // publish-confirmation commit time; the published fact after task-row cleanup
      "deleted": {                                // tombstone
        "at": "<RFC3339>",
        "publishedAtDelete": true | false,         // whether the session was published at deletion time
        "fileId": "sha256:<32hex>" | null          // truncated content fingerprint of the file at deletion time; null if file already absent
      },
      "registered": {                             // --session-id registration: B-side session registered, no data yet
        "kind": "b-owned",
        "at": "<RFC3339>"
      },
      "materialized": "full" | "stub" | "unloaded", // form of byte existence locally; absent equals "full"
      "lastUnchangedAt": "<RFC3339>?"             // most recent time a pull reached "unchanged"
    }
  }
}
```

### 3.3 Field semantic details

**`pull`: the sole evidence of a B-managed session.** `cursor` is held opaquely (semantics belong to RFC 0001); A reads only the byte-count segment as a length anchor — comparing local length before incremental writes, truncating by it after a crash; the fingerprint segment is not parsed. `file` records the copy's location: same-name ambiguity is a real problem, so the persistence location is bound by the record, not "inferred from the id".

**`pullPending` and `pull` are layered.** `pull.cursor` is "what has been safely received so far" and only advances with successful persistence; `pullPending.target` is "what we still want to receive", writable as soon as the intent is enqueued. After a crash `pullPending` can be replayed and its target is not lost; it does not constitute "already pulled" evidence and does not participate in ownership judgment ("no pull" in §6.2 means no `pull` field; `pullPending` does not count). **`pullPending` is deleted in the same transaction when the pull is satisfied** — satisfaction means reaching the target; for `target="latest"`, `changes` returning `unchanged` is satisfaction (latest's semantics is "pull until unchanged"). This is the companion of `sync --wait`'s pure structural judgment (§9).

**`task` is a publish-only slot.** The existence of a `task` row itself is the judgment of "pending publish task"; no `kind` field is needed; `target` is not its field either — the publish target is carried by journal records, the pull target by `pullPending`. Pulls never land in `task` rows.

**`publishedAt`: evidence surviving cleanup.** Written in the same transaction as `phase=published` and `pull` at confirmation commit; after reconciliation cleans up the task row, it is the sole persistent fact of "this session was published by A".

**`registered`: removed the moment its mission completes.** Its sole duty is to block the first-pull crash-window misjudgment (§6.1); when the first pull commits `pull`, **`registered` is deleted in the same transaction** — after that, the `registered∧pull` combination is unreachable and the classification table keeps no row for it.

**`deleted.fileId`: binds a file instance, not a session ID.** The value is a truncated fingerprint of the file content at deletion time (same algorithm as the cursor fingerprint); **when the file no longer exists, or `materialized≠full` (a stub shell is not session content, unloaded has no file), it is always recorded as `null`** — if a stub shell's fingerprint were recorded into fileId, no real file's fingerprint would ever match it, the tombstone would never hit, and a deleted session would fully resurrect via "no record ∧ B has session → backfill". `null`'s semantics is "this id is not allowed to have a file again", stronger than binding a specific instance.

**A tombstone commit clears the session's other fields at the same time.** Landing a tombstone is a single transaction: while `deleted{…}` is written, `pull`/`pullPending`/`task`/`publishedAt`/`registered`/`materialized` are cleared — the "was published" fact folds into `publishedAtDelete` as evidence, and the other fields are buried together with the old instance. The consequences of not clearing are chain-like: when the tombstone's fileId does not match and the judgment says "new instance", the old `pull` still sits in the record → §6.2's `no pull` condition fails → a new fork file with the same sid gets misjudged as "normal resume" and released immediately, **never published**. After clearing, a new instance naturally starts from a clean record.

**`materialized`: the form of byte existence, meaningful only for `pull` sessions.** `full` = the mirror holds a byte-verbatim copy of `[0, cursor)`; `stub` = the mirror holds a deliberately generated summary shell (a few KB, enough for recognition only), whose bytes are incomplete and **not** a legal prefix; `unloaded` = the file was intentionally deleted, only this record remains. Absence equals `full` — v2 records upgrade without rewriting. It is evidence of "done on purpose": without it, `pull ∧ file absent` and `local length < byte count` would misjudge intentional trimming as external damage, and reconciliation would automatically pull back the full body, making disk reclamation never effective. This field may only be written by zonepaird's evict/hydrate write sequence (RFC 0003 §4.5), never hand-edited — hand-setting `unloaded` without deleting the file makes the next reconciliation treat the file as an external writer.

**Clock declaration**: `firstSeen`, `enqueuedAt`, `at`, `publishedAt`, `lastUnchangedAt` are all observation fields; any wall-clock-based comparison saturates to `max(0, now − t)`. No correctness path depends on the clock.

## 4. The `task` phase machine: how far a fork's publish has gone

### 4.1 Four phases and transitions

```text
writing ──spawn joint evidence(§4.2)──▶ validated ──publish confirmed──▶ published
   │                                │                            │
   │ file disappears                │ file disappears            │ reconciliation cleans task row
   ▼                                ▼                            ▼
 task terminated + tombstone      task terminated + tombstone   publishedAt persists,
                                                               pull takes over pulling

 worker execution failure → degraded (persistent terminal state; waiters read it and return failure)
```

| Phase | Meaning |
|---|---|
| `writing` | File detected, but no "adapter validation complete" evidence yet; **do not publish** |
| `validated` | Validation passed (sole evidence source in §4.2); publishable |
| `published` | publish confirmation returned and single transaction committed (terminal; task row cleaned by reconciliation afterward) |
| `degraded` | Persistent terminal state of worker execution failure; `ensure_published` reads it and returns `Degraded` |

**"Superseded" is not a phase.** If the file disappears while the task is in writing/validated (rollback or external deletion) → the task terminates and a tombstone is landed; no dedicated persistent phase is set for it — the tombstone already carries all semantics of "a pending-publish task existed and was then deleted", and adding a phase would only duplicate the same fact.

**`published` must be persisted.** If after confirmation returns only `publishedAt` is written without the phase, then in the "after confirmation commit, before reconciliation cleanup" window the task row looks non-terminal and waiters would misjudge it as still in progress. Persisting the phase gives this window a clear terminal state to read — Published judgment therefore has two branches: `task.phase==published`, or `task` absent but `publishedAt` present. When both are absent, Published must not be judged — preventing a corrupted-rebuild empty table from disguising as success.

### 4.2 The sole source of "validated": spawn joint evidence

`validated` has no file marker to check — the adapter writes no "validated" flag. The sole evidence is a control-flow fact: **`--resume <id>` reaching transport means the adapter's branch→validate→open has already run** — a control flow where validation fails rolls back and deletes the file, never reaching spawn. So "the resume-fork judgment hits" (§6.2) is itself evidence that validation passed.

The residual is declared honestly: validation failing but the file not being deleted (a fault outside the adapter's rollback path) would still be marked validated — covered by three defenses: publish idempotency, 409 conflict, and tombstones; the criterion is unchanged.

**Orphan handling**. When reconciliation scans a fork file with "file evidence but no spawn evidence", it registers `writing` and **suspends it permanently**, with no automatic advancement path. Reason: a file written but never reaching spawn has only two causes — validation-failure rollback (the file disappears soon; auto-publishing would resurrect an unvalidated product), or adapter decision interruption (no resume needed; publishing it is waste and creates a B-side orphan).

## 5. Tombstone filter: the judgment that always runs first

The first step of classification is always the tombstone check; this order cannot be reversed.

A tombstone's `fileId` is a truncated fingerprint of the **complete content** at deletion time — it binds a **file instance**, not a session ID; `null` is recorded for stub/unloaded/file-absent (semantics in §3.3). A tombstone commit simultaneously clears the session's other fields (§3.3), so what reaches here is a record containing only `deleted` — the judgment rules:

| Tombstone combination | Handling |
|---|---|
| Tombstone exists ∧ (`fileId` is null, or file fingerprint == `fileId`) | **Instance hit**: suppress all rebuild, pull, and publish; if the file should be gone but still exists → delete it once more. `fileId=null` **always hits** — it records "this id is not allowed to have a file again" (including the file-present case), not "matches some old instance" |
| Tombstone exists ∧ `fileId` non-null ∧ file fingerprint != `fileId` | The tombstone belongs to this id's old file; the new file goes through §6 classification as a new instance |
| No tombstone | Normal classification |

Example: s1 was deleted with content fingerprint `h1`, tombstone records `fileId: h1`. Later `s1.jsonl` reappears with fingerprint `h2 ≠ h1` — no match, an old tombstone; the new file classifies normally; if the fingerprint happens to be `h1` (the same file renamed back) → hit, the file is suppressed.

**The asymmetric consequence of `fileId=null` must be stated plainly**: for sessions deleted in stub/unloaded/absent state, `fileId=null` means that sid is **permanently sealed** — any same-sid file afterwards hits suppression unconditionally; there is no "fingerprint mismatch → pass as new instance" exit. The ability to resurrect under the same sid depends on whether the file was present at deletion time: present-at-deletion leaves a fingerprint → new instances can live; absent-at-deletion records null → the sid is permanently sealed. This is declared semantics: `sess delete` on a session "whose bytes are nowhere present" expresses "it never existed"; the sealing strength is correspondingly higher.

**Why the tombstone must be checked first**: several rows of the classification table cannot semantically distinguish "B lost the file" from "the user asked us to delete it" — the information distinguishing them lives only in the tombstone. One counterexample suffices to show the consequence of the order: the user runs `sess delete s7`, the tombstone has landed, the file on B is deleted, but the local file still exists due to propagation delay. If joint judgment ran first, the combination hits `pull ∧ file present ∧ B 404` → judged "B lost, pending recovery" and suspended with an alarm — a deliberate deletion misjudged as needing recovery. Checking the tombstone first gives an instance hit → delete the residual file once more, exit consistently, zero false alarms.

## 6. Session classification: joint judgment over field combinations

After tombstone filtering, what state a session is in is uniquely determined by one record's field combination **plus three file-content probes**. Judgment inputs:

```text
{ file present?, pull?, task?, publishedAt?, registered?, materialized? }  +  { file present on B? }
+ content probes: file fingerprint (tombstone fileId comparison), forkedFrom marker, stub-shell structural recognition
(pullPending does not participate in classification)
```

"Field combination is classification" must be honest up to here: most rows look only at fields, but three — tombstone hit, resume-fork judgment, stub-shell recognition — must read file content; classification is not a pure record judgment. **When B probing fails (B unreachable), every row depending on `file present on B?` is not judged and no action is taken this round** — rather miss one round than treat "B did not answer" as "B does not have".

**The materialization exemption precedes this table**: when `materialized` is `stub`/`unloaded`, **every judgment driven by local file existence and length yields** — including `pull ∧ file absent → suspend`, `publishedAt ∧ file absent → land tombstone`, and §7.2's length predicates. Missing any one of them lets intentional trimming be escalated by the classification table into "deleted/modified": if `pull ∧ publishedAt ∧ file absent` were not exempted, the first reconciliation after evict would turn "session still wanted, bytes reclaimed first" into a tombstone, and `hydrate` would then be forever blocked by the tombstone re-check — trimming would be silently escalated into irreversible deletion. The exemption's semantics is "when `materialized` is not full, file existence and length are no longer evidence of external damage or deletion"; it does not exempt tombstone filtering (§5 still runs first) nor judgments unrelated to local bytes like `pull ∧ B 404`. When a stub file itself is bare-`rm`'d and reaches here, the record is still `stub`; handling is the same as `unloaded` — not listed separately in the table.

### 6.1 Complete classification table

**Matching rule: first row hit in table order.** Rows with more specific conditions and disposition rows (degraded, tombstone, suspended) precede normal-state rows — when one record hits multiple rows, the first hit takes over and later rows are not evaluated.

| # | Field combination | Classification | Handling |
|---|---|---|---|
| 1 | `task.phase==degraded` | Persistent terminal state | Do not advance, do not clean; a new intent (retry/ensure-task) explicitly resets — degraded has no outgoing edge (§4.1); reconciliation never auto-resets it |
| 2 | `task{writing} ∧ file present` | A pending publish (unvalidated) | Stay suspended awaiting `ensure-task` intent — writing does not advance; spawn joint evidence (§4.2) is its sole advance condition |
| 3 | `task{validated} ∧ file present` | A pending publish (validated) | Re-enter worker to advance publish; `ensure_published` waits here |
| 4 | `task ∧ file absent ∧ tombstone not hit` | Pending-publish product disappeared | Task terminated + tombstone landed (in-table form of §4.1's downward edge) |
| 5 | `task.phase==published` (pre-cleanup window) | A published (commit window) | Treated as steady state; reconciliation cleans the task row afterward |
| 6 | `task ∧ pull` | Publish handover window | The normal shape of the confirmation single transaction: advance per task phase; pull channel takes over at terminal state |
| 7 | `pull ∧ publishedAt ∧ materialized∈{stub,unloaded}` | Published trimmed state | Normal state: no tombstone, no suspension — `prune` and `sess delete` diverge here; tombstones belong only to `sess delete` |
| 8 | `pull ∧ publishedAt ∧ file present` | A published and in pull channel (steady state) | Normal incremental channel |
| 9 | `publishedAt ∧ no pull ∧ file present` | Field corruption (unreachable in normal transition sequences — `pull` and `publishedAt` are written in one transaction) | Suspend; do not release as normal state |
| 10 | `publishedAt ∧ file absent ∧ materialized∈{full,absent}` | Deleted after publish | Land tombstone `{publishedAtDelete:true, fileId:null}`, `pull` invalidated |
| 11 | `publishedAt ∧ file present ∧ B 404` | Published product, lost on B | Explicitly suspend; do not auto-republish |
| 12 | `pull ∧ materialized=stub ∧ file present ∧ file is stub shell` | B-managed trimmed state | Normal state: no length check, no automatic pull-back; the session returns to full only on new traffic or explicit `hydrate` (RFC 0003 §5, §6) |
| 13 | `pull ∧ materialized∈{stub,unloaded} ∧ file absent` | B-managed unloaded state | Normal state: no suspension; waits for `hydrate` or traffic to trigger return to full (a bare-`rm`'d stub shell lands on the same row — the exemption section already declared handling identical to unloaded) |
| 14 | `pull ∧ materialized=unloaded ∧ file present` | Hydrate crash window or external write | Go through RFC 0003 §4.5's hydrate path to whole-file re-pull and **return to `full` in the same transaction** — unloaded promises "the file should not be here"; its presence means either our own hydration wrote halfway (re-pull converges) or a real external writer; after re-pull it lands full and never returns to this row; if the file is subsequently modified again, regular predicates take over |
| 15 | `pull ∧ materialized=stub ∧ file present ∧ file is not stub shell` | External write or record tampered | Suspend: stub-shell fingerprints are recognizable (zonepaird-generated shells carry identifiable structure); non-shell content is always suspect |
| 16 | `pull ∧ file present ∧ materialized∈{full,absent}` (no task/publishedAt) | B-managed normal | Normal incremental channel; length predicates verified per §7.2 partition |
| 17 | `pull ∧ file absent ∧ materialized∈{full,absent}` (no publishedAt) | B-side deletion or local loss | Suspend pending human decision — do not rebuild someone else's deletion as loss |
| 18 | `pull ∧ B 404` (file present) | B lost, pending recovery | Explicitly suspend: mark, log; no auto-retransmit, no auto-delete local |
| 19 | `registered ∧ no pull ∧ file present` | First-pull crash window | **Do not misregister as A-managed**: B has data → whole-file re-pull to build `pull` (overwrites local file); B 404 → orphan suspension |
| 20 | `registered ∧ no pull ∧ B 404 ∧ file absent` | File not yet produced on B | No suspension: the normal pre-state of registered-not-yet-produced, waits for B to write |
| 21 | `registered ∧ no pull ∧ B has data ∧ file absent` | B-side awaiting first pull | First pull builds `pull` (`projects/lab/` convention) |
| 22 | File present ∧ contains `forkedFrom` marker ∧ no pull/task/registered/publishedAt | Candidate A-managed | Register `task(writing)`, **do not publish** |
| 23 | No record ∧ B has session | B-side backfill | Backfill `registered` (after §5 tombstone filtering) |
| 24 | No record ∧ file present ∧ B 404 | Manually inserted non-fork file | Orphan: register `writing` + permanently suspended (same origin as §4.2 orphan handling) |
| 25 | Non-pull record shows `materialized≠full` | Field corruption | Suspend: `materialized` is meaningful only for pull sessions (§3.3); appearing in other record shapes can only mean it was written broken |
| 26 | All empty (no file, no fields) | Unknown | Conservatively suspend (rather stop than err) |

Three rows need separate explanation, because each hides an easy-to-make mistake.

**Publish handover window**: after the confirmation single transaction lands and before reconciliation cleanup, the record simultaneously shows `task{phase:published}` and `pull{cursor,file}` — this is not an anomaly; it is precisely the intermediate freeze-frame of ownership transferring from A to B. Implementers reading this row should not "fix" it as "fields fighting".

**First-pull crash window**: A once did a first pull for s1, wrote the whole file and fsynced, and crashed just before committing `pull`. After restart it sees `registered`, file present, no `pull` — this is the intermediate state a crash left, not a local product. The correct handling is to redo the first pull (if B has data, whole-file restart, overwrite the local file, and build `pull`); treating it as A-managed and sending it to publish would disguise a B session as an A product. The sole reason the `registered` field exists is to block this misjudgment.

**Unreachability of `publishedAt ∧ no pull`**: the confirmation commit is a single transaction; `pull` and `publishedAt` are born together — the latter appearing without the former can only mean fields were externally corrupted; suspend as field corruption, do not improvise.

### 6.2 Resume-fork judgment: the basis for the publish wait

This is the most critical rule in classification; it is raised separately because it is simultaneously RFC 0003 §8's publish-wait trigger judgment. All four conditions hold → **resume fork product**, go through the publish wait:

```text
file contains forkedFrom marker ∧ no pull ∧ no publishedAt ∧ tombstone not hit
```

| Evidence combination | Judgment | Follow-up |
|---|---|---|
| All four conditions hold | Resume fork product | `ensure_published` blocks until confirmation (RFC 0003 §8.1) |
| `pull` or `publishedAt` either hits | Normal resume | Released immediately — fork-origin sessions that were already published also hit this row |
| Tombstone hit | Tombstone semantics | Do not resume a deleted session |
| File exists but zero complete lines and no record at all | Normal resume | Missed detection is caught by B-side resume's loud failure, not a silent error |

The negations must be pinned down: `no pull` = the record has no `pull` field (`pullPending` does not constitute "already pulled" evidence); `tombstone not hit` = `deleted` absent **or** `fileId` does not match the current file fingerprint (§5's instance semantics).

**Contrast example**: `--resume s2` and `s2.jsonl` carries `forkedFrom`. If s2 has no `pull`, no `publishedAt`, tombstone not hit → resume fork product; the wait blocks until confirmation. If s2 already has `pull` (last fork was published) → normal resume, released immediately. **Same file, same marker; the judgment is decided entirely by persistent state** — the argv parameters themselves cannot distinguish the two kinds of resume; this is exactly why the judgment exists.

## 7. Crash consistency

### 7.1 The shape of crash windows

The write-order iron rule (§3.1) fixes crash windows to one shape: **file written, progress not committed** — progress never overtakes persisted content. Recovery logic only needs to handle this one direction.

### 7.2 Length predicate and handling partitions

When restart or periodic reconciliation finds `local file length` inconsistent with `pull.cursor`'s byte count, handling splits into two sets by caller source, because the two paths face different suspects:

| Caller source | Predicate | Handling |
|---|---|---|
| **Startup reconciliation** (crash recovery) | `local length > byte count` | **Truncate to byte count**: the extra tail is almost certainly our own uncommitted append left by the last crash; after truncation, normal incremental pull resumes |
| Startup reconciliation | `local length < byte count` | `degraded`: external deletion/modification or truncation; local cannot self-certify the prefix; whole-file re-pull |
| **Steady-state reconciliation** (periodic/manual sync) | `local length != byte count` (both directions) | `degraded` → whole-file re-pull: in steady state any-direction inconsistency means an unknown external writer exists; we cannot unilaterally truncate |

### 7.3 The boundary of A-side divergence guarding

Server-side fingerprint checks cover only B-side bytes; the A half of the "prefix equality" invariant is guarded by the length predicates above (pre-write comparison before incremental persistence + bidirectional comparison at reconciliation). A does not recompute content fingerprints. **Declared-level residual: content overwrites of unchanged length have no detection path on A**; they can only be covered by B's prefix on the next whole-file restart — this is the established cost of the "verbatim copy, B is authoritative" model, explicitly declared rather than implied.

## 8. Corruption semantics: quarantine + partitioned degradation

When `state.json` fails to load (corrupted JSON, unrecognized `version`, **or any record failing schema validation** — e.g. `materialized` showing a fourth enum value, `deleted` missing `at`), handle in four steps; never silently zero. **The version recognition set is pinned to exactly the two integers `{2,3}`** — version absent, non-integer, a string, or version=4 are all quarantined (version=4 additionally prevents old versions from destroying future files). A `version: 2` file **is not corruption** — v3's only difference from v2 is the `materialized` field, and absence equals `full`, so a v2 file upgrades in place by "absent fields get defaults", without entering the quarantine path; during upgrade, unknown fields are ignored, and known-field schema violations enter this path's quarantine:

1. **Quarantine**: atomically rename the original file to `state.corrupt-<timestamp>`, keeping it visible and manually recoverable.
2. **Non-rebuildable fields fail closed**: `task`/`publishedAt`/`deleted`/`registered` encode "who created/deleted" facts; losing them distorts — without `publishedAt` one cannot distinguish "published" from "never published", without tombstones one cannot distinguish "deleted" from "B-side lost". Related capabilities shut down: `ensure_published` returns "state corrupted, suspended" for all `--resume` (rather block than release wrongly), `sync` errors and points at the quarantined file.
3. **Rebuildable fields reset**: `pull`/`pullPending`/`lastUnchangedAt`/`materialized` are rebuildable — after clearing, all sessions whole-file re-pull once; `lastUnchangedAt` rewrites naturally on the next unchanged. Putting `materialized` among the rebuildable needs one more argument: it encodes "the user trimmed", but rebuilding it as full only costs re-pulling reclaimed bytes — no data loss, the user can prune again; **the log must loudly declare "trimming facts lost; everything re-pulled as full"** — silently rebuilding it would equally be lying to the recovery logic. Cost is amplified but semantics are safe (B is authoritative; what re-pull gets is the truth).
4. **Orphan files conservatively handled**: `*.jsonl` files in the mirror with no record are all registered `writing` + permanently suspended; the recovery path likewise has no automatic advancement.

Re-pulling is executed automatically by zonepaird's main loop (E4/sweep normal mechanism); `sync` erroring only closes the manual entry, it does not mean recovery stalls.

Old-version state files (e.g. `version:1` push progress) **get no field migration**; they enter this path's quarantine: the target fields do not exist in the v3 schema, nowhere to migrate to; the cost of lost progress is one round of redundant idempotent transfer, acceptable.

Why not "read-only refusal": pull progress has a safe rebuild path; a blanket refusal would turn a recoverable degradation into total stall. Why not "loudly rebuild everything": silently rebuilding `publishedAt`/tombstones is exactly the disguise "corruption does not disguise as success" forbids. The four-step shape takes the precise position between the two — **what can be safely rebuilt is rebuilt, what cannot is shut down, and everything is loud**.

## 9. `sync --wait` judgment inputs

The condition for manual draining is a **pure structural judgment, no clock**: journal directory empty ∧ no `pullPending` ∧ all `task`s reached terminal state, absent, or are spawn-evidence-less suspended `writing` (orphans, §4.2 — they never reach terminal but are also not "work still to do"). The companion clearing rule therefore becomes part of the contract: **`pullPending` is deleted in the same transaction when the pull is satisfied (reaching target or `changes` returning `unchanged`)** — intent fulfilled is intent consumed; no observation residue is kept for it; clock fields `enqueuedAt`/`lastUnchangedAt` return to pure observation positions and no longer serve as judgment basis (the clock clause of §3.3 thus truly holds on the judgment surface: clock rollback only makes wait wait longer, it cannot make it lie; in the old scheme clock jumps could report complete before draining).

Sessions without `pull` do not count as "in debt" — they may be registered sessions with no data yet or trimmed unloaded ones; `wait` does not wait for them.

## 10. Design boundaries

What this document explicitly does not do: it does not authorize B-side deletion propagation — tombstone semantics is "suppress rebuild by instance + record facts", not a "deletion directive"; whether B-side products are reclaimed is an independent lifecycle need; it builds no sync fields for auxiliary assets (the `<sid>/` directory); it builds no persistent `task` phase for pulls — pull degradation is expressed as `Degraded` return + reconciliation suspension (RFC 0003 §6.3), not restoring a persistent slot for it. **`materialized` is a protocol field**: adding a tier (e.g. a fourth form) or changing its exemption semantics equals modifying the protocol; it goes through the field-level change process, silently extending the enum is not allowed. **Adding or modifying any of the above fields equals modifying the protocol.**

## 11. Provenance

Same as RFC 0001 §12.
