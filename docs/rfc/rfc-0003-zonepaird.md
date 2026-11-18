# RFC 0003 — A-Side Sync Process `zonepaird`

| Field | Value |
|---|---|
| Status | Final |
| Date | 2026-11-17 |
| Scope | The execution mechanism advancing sync on zonepair machine A: process model, disk protocol, event sources, task model, waits, crash recovery, argv adaptation |
| Depends on | RFC 0001 (wire protocol), RFC 0002 (state model) |
| Depended on by | None |
| Process archive | `tmp/designs/` (for provenance only) |

## 1. Design overview

The entire A-side execution mechanism can be stated in one sentence:

> **Only one long-running process, `zonepaird`, touches sync state on the A side. Every short-lived process (stub-client, CLI) never writes the state file; it only appends one intent record to the `journal/` directory. `zonepaird` grabs a file lock to become the sole writer, and lands intents one by one as `changes`/`publish` requests to B and mirror-file persistence. Process crash is fine — after restart it replays the journal and scans the mirror once more, recovering to the pre-crash semantic position; nothing in memory needs to be remembered.**

Breaking that sentence down gives five parts:

- **Process form** (§3): an on-demand long-running process; whoever finds the writer absent brings it up, and N concurrent bring-ups self-converge on one lock.
- **Disk protocol** (§4): the journal is the intent channel (the system's only multi-writer point), `state.json` is authoritative data (sole writer); the two-phase write order prevents loss and disorder.
- **Event sources** (§5): all high-value triggers are local file evidence; polling is the only channel given B has no push capability.
- **Waits** (§7, §8): all waits converge to two predicates; the only hard wait is the fork resume's `ensure_published`; freshness is a budget contract.
- **Recovery** (§9): a fixed nine-step startup sequence + reconciliation executor; SIGKILL fully covered, power loss covered up to the fsync boundary.

The design obeys three constraints: **memory-level statelessness** (zonepaird's memory holds only discardable execution state, killable at any time), **normal session WS startup does not wait** (waits execute only when needed; unrelated pulls all run in background), **unexpected restart loses nothing** (intents and authoritative data both persist in fsync order).

## 2. Why a long-running process is needed

`zonepaird`'s existence is not an architectural preference; it is the only exit left after two facts collide. This section walks the full derivation chain and states "what premise change would make it unnecessary".

### 2.1 Derivation chain: two facts, one exit

**First link: the adapter cannot be intercepted when it reads session info.** Zero patches to opencode means we cannot insert a sync before "the read" — where and when the adapter reads session info (recognition, resume, selecting fork boundary) happens is beyond zonepair's reach. So the obligation "data must be fresh" has only one physical expression: **at the moment of read, the data is already on disk**. The mirror must be written before the adapter goes to read, not synced halfway through the read.

**Second link: stubs are short-lived.** `stub-client` spawns once per incoming request and dies at `process::exit(0)`. Anything that "keeps working between two stub lifetimes" cannot hang on a stub — the stub itself does not exist.

**Third link: something must advance pulls between two stubs.** Link one requires "in place at read time", link two says "no stub survives between two requests", and the two together leave only one corollary: **an independent long-lived process is needed that keeps pulling B's data into the mirror between two stubs.** This is the entire reason `zonepaird` exists — not "a daemon looks nice", but "data must be on disk before read" welded to "processes are all short-lived" leaves no other exit.

### 2.2 Three candidate forms, two eliminated

Enumerating the forms of "advancing between two stubs", two of three candidates are eliminated:

**"Sync once on stub entry and exit" — already falsified.** Fixing sync timing at stub spawn moment precisely regresses to the "sync once on process entry/exit" model: source-side data preparation and recognition needs all happen **before** the target stub starts — a sync at spawn moment is always a latecomer for these use cases. It cannot save fork, cannot save recognition; it only saves "want to read once more after spawn", a scenario that was never the bottleneck.

**"Anonymous detached child of stub bootstrap" — isomorphic mechanism but worse.** This form is completely isomorphic to the long-running process in mechanism: likewise a detached child grabbing the same lock. The only difference is the absence of a named-writer concept. The problem: once a detached child survives it decouples from its parent — the parent stub's `process::exit` cannot kill it at all; the "lives and dies with the stub" selling point does not hold: it is still a long-running process, just an unnamed one. The cost is actually higher: once lifecycle is implicit, the rule "who should fill in when the lock is idle" scatters into every stub's corners, `doctor` cannot point at a checkable object, and recovery logic's "ownership" and "task table" need one more layer of interpretation. At equal cost, the named process's explainability is strictly better.

**"`zonepaird` long-running + lock as authority" — selected.** It converges "who advances sync" into an observable, diagnosable object: the lock-grab result is the sole writer, lock release means death, on-demand bring-up, self-converging. The cost is one more process name and one `zonepaird` subcommand — the gain is that recovery logic, diagnostics, and crash-handover rules all converge around this named object rather than scattering inside every stub.

### 2.3 Premise boundary of this conclusion

The daemon's necessity is not universal; it depends on two concrete constraints — premises change, conclusions change:

- **Zero patches welded shut the "on-demand at read" door.** If OpenCode were ever modifiable — say the adapter could send an HTTP ask to A "where is this session up to" before reading session info, or hooks could be hung in the harness process — sync could revert to true "on-demand at read" and the daemon would instantly lose its reason to exist. ADR-0004 welded that door shut; `zonepaird` is the only shape after welding.
- **The "in place at read" requirement itself cannot be yielded.** As long as "when the adapter reads, the data must already be on disk" holds, the advancer must live between two requests; this is not design taste, it is a timing physical constraint.

In other words: the design did not choose "wanting a daemon"; it chose "admitting a daemon exists, but making it nearly stateless". Alternative "infrastructure" like SQLite, message queues, shared memory were all tried — they are either redundant in a design that has already eliminated multi-writers, or cannot escape in a design where on-demand sync is impossible. What was finally chosen is spending complexity on "admit the daemon exists, but make it killable anytime, memory-discardable, semantics entirely on disk", rather than on "pretend no daemon is needed, then secretly stuff its duties back into short-lived processes".

## 3. Process model

### 3.1 Sole writer and lock

`zonepaird` is a subcommand of `stub-client`, bound to one mirror directory, running resident. All authority comes from one file lock: `flock(LOCK_EX|LOCK_NB)` on `<mirror>/.sync/owner.lock`, held for the process's lifetime. **The lock is the authority** — judging "who is the writer" asks the result of this lock grab, not whether the lock file exists: the `.sync/` directory can be deleted wholesale; residual files constitute no ownership evidence.

While holding the lock, zonepaird is the sole executor of: `state.json` read-modify-write, journal record consumption, mirror file append and whole-file replacement, and changes/publish requests to B.

### 3.2 On-demand activation and self-convergence

No dedicated process is responsible for bringing up zonepaird. Whenever a process needs to write an intent and finds the writer absent, it spawns a detached zonepaird in place via `Command::new(current_exe)` (all standard IO redirected to `/dev/null`). N processes doing this simultaneously spawn N zonepairds all grabbing the same lock — exactly one survives; the rest fail the lock grab at step one of the startup sequence and exit immediately, leaving no residue, no retry, no error. **Self-converging, no election protocol.**

```text
T0  stub-A and stub-B almost simultaneously find the writer absent
T1  A and B each spawn a detached zonepaird
T2  The two zonepairds flock in turn; the instance A spawned gets the lock first
T3  The instance B spawned fails the lock grab → exits immediately
T4  A's instance continues the startup sequence (§9.1), becoming the sole writer
```

Four bring-up entries, all equivalent: tail of `setup`, before the first intent write in `transport()`, before `sync`/`sess`/`prune`/`hydrate` CLI enqueue, and **inside the wait loops of `ensure_published` (and the removed `wait_cursor`)** (§7). `doctor` only probes, never brings up — not changing system state is its job. The fixed detection means for "writer absent" is trying `flock`: grab it and immediately release, then spawn — lock-trying is the cheapest probe and is naturally race-free (no error can occur between grab and release). Crash handover works the same: the next write intent or waiter finds the lock idle → spawn again; the new writer rebuilds all state from disk. No dedicated watchdog — no trigger means no work; a watchdog has nothing to guard.

### 3.3 Exit and hang

Exit conditions: failing to grab the lock, panic, **self-heartbeat timeout** (a single main-loop round exceeding 30s → process exits and releases the lock), explicit `stub-client zonepaird stop` (sends SIGTERM to the lock-holding process), **lock-file inode self-check failure** (see below).

**flock-vs-unlink window**: if `.sync/` is deleted wholesale while the lock is held (user cleanup, external script), the holder's flock pins an already-unlinked inode — a newly spawned instance can grab the lock on the rebuilt `owner.lock`, and **two writers coexist**. The defense is cheap: each main-loop round `stat(".sync/owner.lock")` and compare against the held fd's `fstat` inode; mismatch (or path absent) → immediately self-kill and yield. A one-line check; not writing it is a landmine.

**Hang having no external handover is established semantics**: when zonepaird hangs, waiters get no second channel; they receive a loud failure on wait-budget timeout; after self-heartbeat exit releases the lock, the next enqueue triggers a new writer to take over.

### 3.4 Enqueue failure semantics

When `ensure_owner` fails (spawn failure or sustained lock contention), the write intent still persists successfully — journal append does not depend on writer liveness; waiters time out on wait budget and receive failure. If the journal append/fsync itself fails, the caller **returns loudly immediately** — when the intent is unconfirmed, letting the caller know at once is more honest than letting it wait for a terminal state that will never come.

## 4. Disk protocol

The division of labor between the two file kinds in one sentence: **the journal is the intent channel (the system's only multi-writer point), `state.json` is authoritative data (sole writer is zonepaird)**.

### 4.1 Journal record format

One file per intent: `<mirror>/.sync/journal/<unix-ms>-<pid>-<seq>.json`, written with `create_new`, 0600. The filename triple provides global uniqueness and a deterministic sort key.

```jsonc
// Unified form of intent records; op determines payload shape
{ "op": "register",              // register a B-side session
  "sid": "<sessionId>",
  "kind": "b-owned",
  "enqueuedAt": "<RFC3339>" }

{ "op": "pull",                  // pull intent
  "sid": "<sessionId>",
  "target": "latest" | "<cursor string>",
  "enqueuedAt": "<RFC3339>" }

{ "op": "ensure-task",           // task ensure triggered by publish wait
  "kind": "publish",
  "sid": "<sessionId>",
  "file": "projects/lab/<sid>.jsonl",
  "phase": "validated",           // validated only; writing admission narrowed (orphan task(writing) is written directly by reconciliation internally, does not enter wire)
  "enqueuedAt": "<RFC3339>" }

{ "op": "note-remote-delete",    // remote-delete notification such as sess delete → read in, land tombstone
  "sid": "<sessionId>",
  "enqueuedAt": "<RFC3339>" }

{ "op": "reconcile",             // manual sync: full-store reconciliation once
  "scope": "all",
  "enqueuedAt": "<RFC3339>" }

{ "op": "evict",                 // reclaim local bytes: stub keeps shell / drop deletes file (§4.5)
  "sid": "<sessionId>",
  "mode": "stub" | "drop",
  "enqueuedAt": "<RFC3339>" }

{ "op": "hydrate",               // get the bytes back: go reset back to full (§4.5)
  "sid": "<sessionId>",
  "enqueuedAt": "<RFC3339>" }
```

`target` is a journal attribute, not a persistent field: when read in, `pull`-type records write `pullPending{target,enqueuedAt}`, `ensure-task`-type records write/advance `task` rows. "latest" semantics is "call `changes` until it returns `unchanged`"; `nextCursor` is explained by the server — **not a client-guessed file tail**.

### 4.2 Writing intents (any process, multi-writer)

Strictly four steps, each corresponding to one crash point:

1. `ensure_owner()` — ensure someone consumes the record written into the journal. Crash point prevented: without this step the journal only receives and never drains; the caller thinks the task is queued yet nobody ever executes it.
2. Check unconsumed journal records **< 4096**; exceeding the limit fails loudly immediately — turning "writer lost contact causing unbounded backlog" into an observable signal at the first overflow.
3. `create_new` write `<unix-ms>-<pid>-<seq>.tmp` → write content → fsync file → **rename to formal name** → fsync `journal/` directory; **any step failing → return loudly immediately**. Must "write temp name first then rename" rather than directly `create_new` the formal name: direct writing lets consumers read an empty or half-JSON file in the "file created, content unfinished" window — startup cleanup quarantines and deletes it, silently swallowing an intent the caller is about to confirm successfully. Consumers only recognize formal names; residual `*.tmp` files are reclaimed by startup cleanup (§9.1 step 2) together.
4. Neither rename nor directory fsync may be omitted: file-data fsync only protects content, not the directory entry — the filename itself must be fsynced to survive power loss.

**Why journal rather than "callers grab the lock and modify `state.json` directly"**: that would make `state.json` a multi-writer read-modify-write point; any caller crashing inside the lock would block all enqueues. The journal's append-only form lets multi-writers avoid mutual exclusion — convergence is completed single-sidedly by the consumer; this is the correct minimal boundary of the sole-writer discipline.

### 4.3 Writer consumption (sole writer)

1. Scan the journal directory, read records **in filename order** into `state.json` read-modify-write; commit path `temp file → fsync → atomic rename → directory fsync`; after successful commit, delete the consumed journal file (then fsync the directory). **Commit first, delete second** — reversing it makes a crash landing exactly between the two permanently lose an already-consumed record.
2. Ordering is for determinism, not correctness: two `pull` records for the same session merge by taking max of `pullPending.target` (commutative, holds); different sessions are unrelated.
3. **Replay holds iff all ops are idempotent** — "commit first, delete second" accepts replay, so every op replayed a second time must be semantically identical to the first. Pinned one by one: `register`/`pull`/`note-remote-delete`/`reconcile`/`hydrate`/`evict --drop` are naturally idempotent; `ensure-task` phases **may only advance, never regress** (a replayed `validated` meeting an already-`published` is a no-op); `evict --stub` replay on an already-stub file is a no-op (skip if the target is already a shell). Without this master rule, "what does a second replay do" is each op's own luck.
4. **Three special rules for reading in `evict`/`hydrate`**: ① check the tombstone before reading in; on hit, discard the intent — the tombstone means "this session is unwanted", and its semantics overrides any materialization operation; otherwise `evict --stub` would leave a deleted session's shell on disk escaping tombstone suppression; ② `evict` clears that sid's `pullPending` in the same transaction — the "reclaim bytes" intent overrides all in-flight pull intents; otherwise a queued pull would pull the file back after evict lands and evict would never take effect; ③ **evict/hydrate journal files are deleted only after the execution commit (`materialized` lands on disk)** — they produce no `pullPending`-style persistent prerequisite field; if executed literally per "delete right after read-in commit", a crash would silently lose the prune intent: journal deleted, `materialized` unmoved, file still present — nothing happened.
5. **Write-order iron rule** (same rule as RFC 0002 §3.1, restated here): any progress advance happens after its data lands — a new cursor is committed only after mirror-file append fsync / rename+fsync completes; `published` is committed only after B's confirmation returns.

### 4.4 The two paths for mirror persistence

Same rules as RFC 0001 §6.4, fixed here from the writer's perspective on execution details:

| changes response | Application method | Precondition check |
|---|---|---|
| `append` | End-append at `pull.cursor` byte count + fsync (file 0600) | `local length == byte count`; if not satisfied → `degraded`, switch to whole-file re-pull |
| `reset` | `body → <sid>.jsonl.zp<PID> → fsync → atomic rename → directory fsync` | After landing, **re-check tombstone**: on hit, delete file, do not commit cursor |
| `unchanged` | No file write; update `lastUnchangedAt` | — |
| 404 | Do not create file; enter RFC 0002 §6 classification | — |

The SDK helper's new-file writes (`<newid>.jsonl`) and zonepaird's pull-write filenames never intersect by construction; the two writer systems need no file-level lock — coordination is entirely via the state machine (§10).

### 4.5 Materialization write sequence (evict/hydrate only)

evict and hydrate change "whether bytes are on disk"; they are the two directions of the same write-order iron rule as "what bytes the pull writes": **the `materialized` marker commit always happens after the file operation completes** — the marker must not overtake persisted content; reversing the order makes post-crash recovery logic read a lie (RFC 0002 §3.1).

| op | Execution order | Crash window |
|---|---|---|
| `evict --stub` | ① read original file, generate stub shell → write `<sid>.jsonl.zp<PID>` + fsync → atomic rename + directory fsync; ② single-transaction commit `materialized="stub"` | Crash in ①: original file or half-finished shell residue; reconciliation falls back to "file present + full" normal state or half-finished cleanup; no new classification |
| `evict --drop` | ① delete `<sid>.jsonl` + directory fsync; ② single-transaction commit `materialized="unloaded"` | Crash between ①②: file gone, record still `full` → next reconciliation hits `pull∧file absent`, handled as suspension (see below) |
| `hydrate` | Go through §4.4 `reset` path: pull full body → temp file + rename → after landing, single-transaction commit `materialized="full"` + `pull.cursor` | Same as reset: file written, marker uncommitted → next re-pull |

**Stub shell shape**: a zonepaird-generated stub is a recognizable structure of "several complete head lines + one synthesized summary line" — it must satisfy two requirements: let the adapter's `exists`/`getSessionInfo` recognize this session (the entire reason it exists), and its content fingerprint must be distinguishable from a normal session file (so reconciliation encountering "file present but content is not a stub" can recognize external writing and suspend per RFC 0002 §6.1). The concrete byte recipe is an evidence gate (§12-9); the first version only allows `--drop`.

**evict crash fallback**: when `evict --drop` crashes between "file deleted, marker uncommitted", the next startup sees `pull ∧ file absent ∧ materialized=full` — per the classification table this row is "suspend pending human". Accept this misjudgment in handling: it is safer than "marker before file" (the latter would treat a still-living file as deleted); the cost is one human confirmation — evict is a low-frequency operation anyway, suspension once is an acceptable failure shape.

**Pull execution is aware of `materialized`**: before the worker executes any `pull`/`pullPending` persistence it first checks `materialized` — when not `full`, local bytes are not a legal prefix and the append precondition necessarily fails; in that case do not go `degraded`, switch directly into this section's hydrate path (reset full body + same-transaction commit `materialized="full"`), then continue pull-until-target. This rule is simultaneously the implementation mechanism of "traffic triggers return to full": E1/E3/pullPending hitting a stub or unloaded session all revive via this path — **after hydration the pull still goes to `unchanged` as usual, `pullPending` is then consumed, and `sync --wait`'s structural judgment holds for trimmed sessions too, never permanently stuck** (RFC 0002 §9). The `hydrate` task itself is this path's explicit entry; semantics are completely identical.

## 5. Event sources and triggers

All high-value triggers are local file evidence; no new process or sync round trip needed.

| # | Trigger | Who writes journal | Output | New process/network |
|---|---|---|---|---|
| E1 | B→A traffic for this session observed during WS forwarding (including turn end) | inside transport process | `pull(sid,"latest")`, written once after relay ends; **on `materialized!=full` hit the worker automatically switches to §4.5 hydrate path** (traffic = session revival signal; trimmed-state bytes are not a legal prefix and cannot append) | None — frame observation is zero-cost; bursts are naturally absorbed by the worker's pull-until-unchanged |
| E2 | transport pre-relay | transport | Only when argv judges resume fork: `ensure-task` + publish wait (§8); zero action otherwise | None |
| E3 | transport post-relay | transport | `pull(sid,"latest")` (merged with same-session E1, idempotent) | None |
| E4 | Periodic freshness polling | zonepaird internal, no journal | Recently-active sessions ~2s interval `changes`; full-store low-frequency sweep ~60s goes through reconciliation; **skips sessions with `materialized!=full`** (trimmed-state freshness is triggered by traffic/hydrate; no point spinning for them) | No new process; B has no push capability, polling is the only channel |
| E5 | Manual `sync` | sync CLI | `reconcile(all)` — the only full-store reconciliation trigger; `--wait` judgment in RFC 0002 §9 | Brings up zonepaird (if absent) |
| E6 | `sess`/`doctor` CLI | respective CLIs | `sess delete` writes `note-remote-delete` → read in, land tombstone; `doctor` is read-only diagnostics | None |
| E7 | mirror file-tree changes | zonepaird reconciliation observation, not journal | Appeared → register `task(writing)`; disappeared → task terminated → tombstone | None; periodic reconciliation scan covers it |

E4's "recently active" judgment comes from zonepaird's in-memory last-traffic table: `HashMap<sid, last E1 time>`, TTL at minute level (default 10min), purely in-memory and discardable — if the table empties, the active set at worst degrades to sweep coverage; semantics unchanged.

`--session-id` new sessions write `register` → read in, land `registered`; `--session-id --no-session-persistence` **zero action** (no registration, no obligation to generate files).

## 6. Task model

### 6.1 Task types converge to three + one internal

| Type | Description |
|---|---|
| `pull(sid,target)` | Incremental pull, serving freshness/recognition/source-prefix needs |
| `publish(sid)` | A-managed file delivery to B; read and validation happen at worker execution time (§8.1) |
| `reconcile` | Reconciliation executor: periodic reconciliation is writer-internal behavior, no journal; manual `sync` enqueues via journal `reconcile(all)` |
| `evict` / `hydrate` | Materialization-level changes; reuse the journal→state→worker three-stage form; write sequence in §4.5 |

Rejected types: `delete-remote` (B reclamation contract undecided), `upload-arbitrary` (unauthorized reverse upload forbidden), watch/notify tasks (E4 polling already covers). At most one `task` row per session (publish-only, RFC 0002 §3.3); pull's persistent carrier is the `pull`+`pullPending` layering; **materialization levels do not land in `task` rows** — `evict`/`hydrate` are transient intents, consumed upon execution; the persistent carrier is the `materialized` field itself.

### 6.2 Merging, concurrency, and timeouts

- **Merging**: duplicate enqueue for the same sid goes through upsert — `pull` merges into `pullPending` (`target` takes max), `ensure-task` merges into `task` rows (advance phase or append `lastError`); selection order `publish > evict/hydrate > pull > reconcile` (the former two-level `pull(with waiter) > pull(background)` layering retired together with `wait_cursor` — in-process waiters no longer exist, no layer to mark) — materialization intents take priority over pulls: they are human actions on disk, timelier than background freshness.
- **Concurrency**: worker pool = **4**. B's write path is globally serial (RFC 0004 §8); concurrency beyond ~4 only queues server-side; 4 lanes suffice to spread a 45-session sweep to seconds. A tunable constant, not protocol. **Same-sid tasks strictly serial**: pull, evict, hydrate operate on the same file; concurrent execution lets interleavings like "pull appends with old cursor after evict" hit `local length != byte count` → degraded → whole-file re-pull writes the file back but `materialized` marker unmoved — a self-made race finally reported as "unknown external writer". Same-sid tasks queue in read-in order; different sids may run concurrently freely.
- **Timeout layering**: request-level 30s reqwest timeout — **only effective for stages with no body or body fully transmitted; during body streaming, liveness is judged by throughput progress** (e.g. no bytes arriving for 10s counts as hung), not cut by total duration — a few-hundred-MB reset/hydrate under WAN almost certainly exceeds 30s total; cutting by wall clock is a death sentence for large sessions. Task-level single-task 300s logical budget, overdue → `degraded`; wait-level see §7.

### 6.3 Two-layer retry

**Transport layer** keeps existing semantics: network jitter/5xx → 3 attempts, 100/300ms backoff; 4xx no retry. Only handles "did the packet arrive".

**Queue layer** handles "what the result means":

| Failure class | Handling |
|---|---|
| Transport retries exhausted | Task backs off and re-enqueues 1s→5s→30s capped; cumulative over task budget → `degraded(network)` |
| `changes` → `reset` | Not a retry: discard local prefix assumption, go whole-file replacement write sequence (§4.4), commit new cursor directly |
| `changes` → 400 | `degraded(bad-cursor)`: persisted cursor corruption is corruption semantics; disguising it as server divergence masks the corruption; recovery goes through reconciliation, no automatic re-pull |
| `changes` → 404 | Enter RFC 0002 §6 classification (including B-hasn't-produced-file normal pre-state); **do not consume `pullPending`** — 404 does not equal "caught up"; the intent survives until B produces the file and is satisfied normally (`sync --wait` therefore waits for registered pre-states, matching semantics) |
| Pre-persistence `local length != byte count` | `degraded(local-diverged)` → whole-file re-pull |
| publish → 400 | `degraded(bad-request)`: line validation/sha/id mismatch is terminal caller error; no auto-retry; fork products will not be "fixed" by anyone; suspend loudly for humans |
| publish → 409 | `degraded(conflict)`: retry and silent merge forbidden; `already:true` counts as `published` |
| Old B without changes route | `degraded(unsupported)`: explicit failure prompting B upgrade, no automatic full-pull downgrade |

degraded's recovery path is unique: **a new intent enqueued** — `task.phase==degraded` is a persistent terminal state with no outgoing edge (RFC 0002 §4.1); reconciliation never auto-resets it (otherwise deterministic failures like `degraded(conflict)` would form an infinite deterministic retry loop, one per sweep). Recovery relies on a new journal intent (retry/ensure-task) explicitly resetting the task row back to `validated`; **every degraded is visible to waiters** (including `Degraded(suspended)`, §7.1) — this is "corruption does not disguise as success"'s counterpart clause on the wait surface.

### 6.4 Command surface

Materialization-management commands also hang under `stub-client`: besides `sync`/`sess`/`doctor`/`zonepaird`, two new subcommands:

| Command | Intent written | Semantics |
|---|---|---|
| `stub-client prune <sid>` | `evict{sid, mode:"stub"}` | Reclaim local bytes, keep recognizable shell (opens after stub recipe closes, see §12-9) |
| `stub-client prune <sid> --drop` | `evict{sid, mode:"drop"}` | File deleted, record stays `unloaded` — **the only form allowed in the first version** |
| `stub-client prune --older-than <dur>` / `--all-stale` | Fan out multiple `evict`s per condition | Batch: enqueue one per qualifying session; mechanism identical to single, only more intents |
| `stub-client hydrate <sid>` | `hydrate{sid}` | Pull stub/unloaded back to `full` — worker goes through §4.4 reset path |

The division of labor between `prune` and `sess delete` must be pinned: `sess delete` is "this session is unwanted" (lands tombstone, may propagate on B); `prune` is "session still wanted, bytes reclaimed first" (B-side semantics unchanged, hydratable back anytime). Tombstone filtering (RFC 0002 §5) governs the former, the materialization exemption (RFC 0002 §6) governs the latter — the two paths share no judgment at the reconciliation layer.

Batch `prune`'s predicates `--older-than`/`--all-stale` read `lastUnchangedAt` and the E1 traffic table to approximate "how long since touched", **without promising precision** — it is after all a convenience wrapper fanning evict intents out to multiple sids; the mechanism is completely identical to single `prune`, and the cost of a wrong predicate is only "trimmed something that shouldn't be" (hydrate is reversible), no data loss.

## 7. Wait predicates

All wait surfaces converge to two predicates; callers learn no third word. **The authoritative data source is always on-disk `state.json`**; zonepaird's internal `HashMap<SessionId, watch::Sender>` only serves to wake in-process waiters faster — it is a cache, not truth; if lost, rebuild. The default implementation of cross-process waiting = inotify on `.sync/` (filtering `state.json`'s MOVED_TO/CREATE) or 50ms polling; the optional unix-socket fast path is not part of the protocol surface — semantics are completely unchanged when it is absent; this is "memory statelessness" implemented on the IPC surface.

```text
ensure_published(sid)    → Ready | Published | Superseded | Degraded(reason)
wait_cursor(sid, target) → Reached | Divergent | Degraded(reason)   [REMOVED — no consumer]
```

### 7.1 `ensure_published`'s complete lifecycle

```text
T0  SDK helper writes out projects/lab/<s2>.jsonl (fork product, contains forkedFrom)
T1  zonepaird periodic reconciliation scans the new file → registers task(writing) (no publish)
T2  User resumes s2 → transport argv judgment hits resume fork (§8)
     → journal append {op:ensure-task,kind:publish,sid:s2,phase:validated}
     → transport blocks on ensure_published(s2)
T3  zonepaird reads in that journal → task.phase=validated
T4  Worker takes task: re-reads file + precondition checks (§8.1) → publish → confirmation received
T5  Single transaction commits phase=published + publishedAt + pull{cursor=confirmation cursor}
T6  Waiter reads terminal state → Published → resume continues spawn
```

Terminal-state judgment has four branches, strictly in order: `task.phase==published` or (`task` absent ∧ `publishedAt` present) → `Published`; `deleted` tombstone present → `Superseded`; `task.phase==degraded` → `Degraded(reason)`; `task` absent and `publishedAt`/`deleted` both absent → **keep waiting** — the task row may not have been read in yet; absence is not terminal. **Fifth branch: session suspended by reconciliation → `Degraded(suspended)`** — suspension is a diagnosable failure; letting the waiter sit out its budget violates "every degraded is visible to waiters". The cap = task budget 300s (not a fixed small budget: payload upload scales linearly with fork file size; a fixed budget would falsely report timeout on large forks).

**Waiter's reactivation obligation**: inside the wait loop, periodically (e.g. 1s) probe lock idle → self `ensure_owner()` to bring up a new writer. Without this, when the writer dies mid-wait, a waiter holding an already-consumed intent can only sit until budget exhaustion — when the very next spawn could have taken over. This is isomorphic to §3.2's "whoever finds absence brings up"; it just missed the waiter entry.

### 7.2 `wait_cursor`'s comparison rules

> **Status: spec'd, no consumer** — pinned-cursor waits had no caller;
> implementation removed (impl/mirror-sync). Reimplement per this
> section's semantics if a consumer appears. `ensure_published` is unaffected,
> and remains the production wait predicate.

Original comparison-rule memo (implementation removed together with `wait_cursor`; the §7.2 semantics below continue to take effect inside the worker `pending_satisfied` commit transaction): `pull` absent → keep waiting, do not judge Divergent; same fingerprint segment compares byte counts, `>=` → `Reached`; different fingerprint segment → immediate `Divergent`; `"latest"` always safe; cap likewise task budget 300s.

## 8. Waits and the argv adaptation layer

### 8.1 Publish wait (the only hard wait)

Causal chain: when the user executes `--resume` on a local fork, B does not yet have this session — the fork file only exists on the local mirror. Spawning a stub with `--resume X` directly makes B fail because the session cannot be found. So a resume fork must wait for three things: the file is published to B, B confirms receipt, and this fact is persisted. The wait sits in the window before WS establishment, because that is the last position where clean abortion is possible.

**Call site**: `transport()`'s pre-WS-establishment window. Sequence: `--resume X` → read-only `state.json` + read mirror `<X>.jsonl` → resume-fork judgment hits (RFC 0002 §6.2) → journal append `ensure-task` → `ensure_published(X)` blocks → `Published` → `open_stdio`.

**Worker execution sequence**: a validated-phase task **re-reads the file** in the worker (defending against "file still being written when validated was committed"); the four-element precondition — file exists ∧ contains `forkedFrom` ∧ ownership still A-managed ∧ tombstone not hit — on pass calls `publish` → confirmation → single-transaction commit (`phase=published` + `publishedAt` + `pull{cursor=confirmation cursor, file=task.file}`). Explicit transitions on validation failure: file disappears → tombstone; forkedFrom missing/ownership not A-managed/read error → `degraded(precheck)`. Both transitions are visible to waiters.

**Failure semantics**: `Superseded` (file already gone = rollback) → stub exits loudly, never starts a resume against a nonexistent source; `Degraded` → loud failure, reason diagnosable.

### 8.2 Freshness degenerates to a budget contract

"Data must be fresh enough" has no natural sync waiter — the adapter is unreachable; nobody can stand before "the read" to intercept it. So it is not a blocking point, it is a **lag budget contract**:

> For B-managed sessions, zonepaird maintains `pull.cursor ≥ serverStableCursor(t−Δ)`, where serverStableCursor = B's safely-persisted newline-terminated position. Δ is tiered: active sessions `Δ_active ≈ 2s` (E4 poll period), inactive `Δ_idle ≈ 60s` (sweep period).

`wait_cursor`'s consumers are only two kinds (implementation removed; semantics retained here for reimplementation reference): internal (the worker pull-until-target convergence judgment — already inlined into `pending_satisfied`'s commit transaction) and acceptance/diagnostics (`sync --wait`, doctor freshness reports, test assertions "progress reached <B confirmation cursor>" — comparing the same file, §7.2).

**boundary spot-checks do not belong to zonepaird**: zonepaird's read domain = mirror + `.sync/`; it cannot get the adapter SessionRecord; boundary UUID-level verification is handed to `doctor` (declaring the adapter SessionRecord path as an input dependency).

**Remaining window**: B newly wrote within the last Δ while the adapter happens to pick a boundary from it — concretely "source session still active on B + snapshot/fork request arrives within Δ after an active turn ends". Cannot be eliminated under zero patches; jointly narrowed by Δ and E1 traffic hints; over-window exposure takes the form of silent degradation, observable by doctor.

### 8.3 argv adaptation layer

The adaptation layer does **one-way translation** between transport and zonepaird: input = SDK spawn argv + mirror file evidence + read-only `state.json` judgment, output = journal records. The queue core is completely argv-unaware — argv is an upstream SDK contract, isolated in the adaptation layer.

| argv form | File/state evidence | Output action |
|---|---|---|
| `--session-id X` | — | journal `register` → `registered`; E3 pull after post-relay |
| `--session-id X --no-session-persistence` | — | Zero action |
| `--resume X` | file contains `forkedFrom` ∧ no `pull` ∧ no `publishedAt` ∧ tombstone not hit | Publish wait (§8.1) |
| `--resume X` | `pull` or `publishedAt` present / tombstone hit / zero complete lines and no record | Zero sync action |

How `forkedFrom` is read: resolve `<X>.jsonl` under `projects/**` (any depth, first hit, same semantics as SDK find), check whether any line's `forkedFrom.sessionId` exists. Ordering guarantee comes from the adapter's own branch (write fork file + validate) → open (spawn) sequence: by the time the stub reads, the file is fully persisted. If the file is coincidentally manually deleted at that moment, treat as normal resume — B-side resume of a missing session already fails loudly; it is B's missing partition, not a new failure mode added by this design.

What the adaptation layer does not do: does not parse parameter-order integrity, does not infer fork-source liveness, does not treat `--resume` itself as a fork marker (same shape on argv), does not write `state.json` (read-only).

## 9. Crash recovery

### 9.1 Startup sequence (strictly nine steps, order not swappable)

Each step corresponds to the crash point it prevents:

1. **Grab lock**: ensure `.sync/` exists (0700); `flock(LOCK_EX|LOCK_NB)`; occupied → exit immediately (a live writer exists). Prevents: two zonepairds coexisting, destroying sole-writer discipline.
2. **Clean half-finished artifacts**: `*.zp*` residual tmp files under `projects/**`, empty/unparseable records in journal → quarantine-delete. Prevents: half files left by the last crash mixing into the scan set, half records poisoning read-in.
3. **Read authoritative data**: `state.json` missing → initialize empty v3; `version: 2` → in-place upgrade (absent fields filled with `materialized=full`, bytes not rewritten); corrupted JSON **or any record failing schema validation** → rename to `state.corrupt-<ts>` quarantine, set global failure state, continue with empty table; unknown version (including old v1 files) → likewise quarantine + loud error, no schema guessing. **No separate degradation record written**: if another crash happens after quarantine, the next startup rediscovers the corruption and runs quarantine again — the cost of losing the degradation state once is repeating one quarantine; not worth persisting an intent for it.
4. **Failure-state handover**: detect `state.corrupt-*` → restore global failure state (RFC 0002 §8): orphans all `writing`+permanently suspended, `ensure_published` all "state corrupted, suspended", `sync` errors pointing at the quarantined file; **the log loudly declares the failure-state scope and the reset cost of rebuildable fields** (including "trimming facts lost, everything re-pulled as full", RFC 0002 §8-3). **The failure state's only exit path: human handling of `state.corrupt-*` (fix or delete), next startup lifts it** — `state.corrupt-*` itself is the persistent failure latch; while it remains, every startup re-enters failure state; this is the counterpart of "corruption does not disguise as success": degradation cannot disguise as a permanent verdict either — once a human handles the file, that is the release condition.
5. **Rebuild in-memory notification channels** [RETIRED with `wait_cursor` — impl/mirror-sync removed]: originally "build a `watch::Sender` for every session with `pull`/`task`/`pullPending`" — its served pinned-cursor waiters have no consumer; `Notify` remains as `Waits::wake`'s in-process wake hint, `subscribe` lazily creates senders. Step order retained to prevent §9.1 numbering drift.
6. **Replay journal**: all unconsumed records read in filename order → first commit. **Under failure state, replay as usual but all tasks set suspended** — intents are not lost (intents replayed before corruption recovery remain valid), execution halts (degradation semantics are not bypassed by replay).
7. **Resume in-flight tasks**: `writing`/`validated` re-enter the queue directly (publish is idempotent, redoing is safe); `pull` continues from `pull.cursor`; `pullPending` re-enqueues.
8. **Run one full reconciliation** (§9.2).
9. **Enter main loop**: journal discovery (inotify/polling) + E4 polling + periodic reconciliation + self-heartbeat.

### 9.2 Reconciliation executor

Compares the two sides `mirror × sessions`; the scan set is limited to exact-name `<sid>.jsonl` under `projects/**`, excluding `*.zp*`. **Rule order is fixed: tombstone filter first (RFC 0002 §5), then joint judgment (RFC 0002 §6.1's complete table)** — the disposition column's execution surface (delete-again/suspend/backfill `registered`/register `task(writing)`/first-pull build `pull`) is implemented by this document; classification semantics themselves defer to RFC 0002. Length-predicate partitioning (startup truncation vs steady-state bidirectional degraded) defers to RFC 0002 §7.2. **Local file deletion after tombstone read-in is backstopped by this executor**: between tombstone commit and actual deletion there is at most one reconciliation period — the tombstone first suppresses all rebuild/pull/resume, the file itself is then cleaned by "should-be-deleted but still present → delete once more"; in this window the file is present but semantically dead.

### 9.3 Coverage boundary

| Event | Coverage |
|---|---|
| SIGKILL / process crash | **Fully covered**: rename atomicity + journal replay guarantee confirmed intents are not lost, committed progress is usable, in-flight tasks redo idempotently |
| Power loss | **Covered except residue**: all commit points fsync; media-layer loss after fsync is not defended; append torn tails are recovered by startup reconciliation truncation |

## 10. Races with the SDK helper

The SDK helper is outside the writer and unmodifiable under zero patches; coordination can only be the state machine:

| Helper behavior | Queue-side signal | Handling |
|---|---|---|
| `forkSession` writes `<newid>.jsonl` | Reconciliation observes file appeared + `forkedFrom` | `task(writing)`, no publish |
| Validation passed → spawn | argv + state joint evidence | `writing→validated→publish` |
| Validation failed `deleteSession` / release deletes | Reconciliation observes file disappeared | Task terminated + tombstone; already published → tombstone `{publishedAtDelete:true}` + `publishedAt` retained |
| `deleteSession` races pull writes | Target file disappears / rename resurrects deleted file | Former per "file disappeared" row; latter tombstone re-check (§4.4) hits → delete |
| File deleted during in-flight published-confirmation window | File disappears after `published`+`publishedAt` committed | Tombstone `{publishedAtDelete:true}` — semantics = not rolled back at publish time |
| `forkSession` picks a stub session as source | Stub file too short, cannot find compact boundary | Goes through the defined F03 fallback (degrading without corruption): if the fork source cannot get a boundary, generate a branch file via the "no boundary" path; window ≈ freshness Δ, doctor-observable. **Unreachable in v1** (gated by `--stub`) |
| `getSessionInfo`/`forkSession` reads file mid-append | Torn tail line during write() | Tolerated as normal "tail line incomplete": opencode must already tolerate crash torn tails; a half line read in at worst misses the last record; no read lock added (helper is an unpatched third party) |

## 11. Use-case family quick reference

| Use-case family | Trigger | Wait | New network cost this time |
|---|---|---|---|
| New session / live resume / tool continuation / model switch | `--session-id`→`registered`; E1→background pull | Zero | 0 on WS path |
| Normal resume / recognition needs | E4 freshness budget | Zero (data already on disk) | 0 pre-startup; false-negative risk = freshness window |
| Fork with boundary | `--resume`+judgment → E2 publish | `ensure_published` ≤300s | 1×RTT + upload (linear with file) |
| Native fork / generate / compaction | Source prefix = E4 budget; product = E2 publish | Publish must wait; source prefix does not | publish 1×RTT + upload |
| Sourceless temp / cleanup | `--no-session-persistence`→zero action | Zero | 0 |
| interrupt / idle / reload / crash | E1/E3; crash→§9 | Zero | changes only for related sessions |

## 12. Known boundaries

1. **Freshness residue**: source-side new writes within the Δ window expose as silent degradation (§8.2).
2. **Half-finished residue**: deleted after published → B-side orphan (tombstone `publishedAtDelete` is the handover input for the reclamation contract).
3. **Fork-source validation window**: source file whole-file replaced between the worker's two reads before publish → false validation failure; covered by publish idempotency, 409 conflict, and tombstones.
4. **zonepaird hang**: no external handover; relies on 30s self-heartbeat releasing the lock + waiters failing loudly.
5. **Same-size overwrite**: external writer rewrites a B-managed file with unchanged length → locally undetectable; caught by the next B-side fingerprint check.
6. **Pulls have no persistent task phase**: pull degradation has no `lastError` disk evidence; after crash `pullPending` can replay but "last failure reason" is lost — the established cost of the publish-only ruling.
7. **Clock is not a correctness basis**: all wall-clock fields serve observation and ordering hints only.
8. **Polling cost upper bound**: 45-session sweep at 4 concurrency + `unchanged` empty responses ≈12 requests/round; E4's active-set TTL and sweep frequency are cost knobs.
9. **Stub byte recipe not closed**: what byte content lets `getSessionInfo` reliably recognize while being distinguishable from normal files by reconciliation remains an evidence gate (design archive F02/Q6/Q2 not all passed). **The first version of `prune` only allows `--drop`; `--stub` mode (E1 automatic hydrate, stub-state classification rows) opens after recipe verification** — the mechanism and fields are already in place in RFC 0002/0003; what's missing is only the evidence of "what the shell looks like".
10. **Batch prune predicates imprecise**: `--older-than`/`--all-stale` approximate "how long since touched" via `lastUnchangedAt`+traffic table; wrong predicates only over-trim without data loss (hydrate reversible); precise liveness judgment requires reading the adapter SessionRecord, same doctor read domain as §8.2's boundary spot-check.

## 13. Provenance

Same as RFC 0001 §12.
