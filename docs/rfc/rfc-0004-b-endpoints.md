# RFC 0004 — B-Side Sync Endpoint Implementation Contract

| Field | Value |
|---|---|
| Status | Final |
| Date | 2026-11-17 |
| Scope | zonepair machine B's stub-server: the server-side implementation contract for the two endpoints `changes` and `publish` |
| Depends on | RFC 0001 (wire contract) |
| Depended on by | None |
| Process archive | `tmp/designs/` (for provenance only) |

## 1. Design overview

What B needs to add can be stated in one sentence:

> **B adds only two routes: `GET …/changes?cursor=` ("am I behind?") and `POST …/publish` ("this is a new session I forked; please take it"). Nothing else changes — the Store needs no new persistent state, claude needs no awareness, old clients are zero-impacted.**

Behind that sentence are three key decisions, each made after hitting a real constraint first. All following sections are their expansion.

- **Correctness hangs only on the cursor fingerprint** (§4): the Store's merge is whole-file rewrite, claude is an external writer with no hook — no mechanism guarantees "written prefix never changes". So the cursor self-encodes `(byte count, prefix fingerprint)`; the server does single-round-trip interpretation per request; filesystem metadata serves only as cache hints, never as judgment basis.
- **`nextCursor`'s boundary = after the last newline** (§5): incremental pull must answer "which bytes are stable and safe for the caller to commit progress on". JSONL lines contain no newlines, so a line carrying a terminator was necessarily written out completely; a half-written line naturally falls outside the boundary and is excluded.
- **Read path fully unlocks** (§8): lock-free reads are safe only under "file content is either old or new, never torn". So `write_lines` is first changed to same-directory temp file + atomic rename; after that `changes`/`messages` run on the same open fd snapshot without taking the lock, while the write path keeps global mutual exclusion.

## 2. Existing skeleton and interface disposition

### 2.1 New surface and retained surface

stub-server is a single-listener structure; Bearer middleware hangs on the whole Router — the two new routes land inside the existing `{id}/` resource naming family, automatically inheriting auth, needing no wiring; deployment surface (CF Tunnel single service URL) has zero changes.

| Interface | Disposition | Role under new contract |
|---|---|---|
| `GET /v1/store/sessions/{id}` (exists) | Retained | CLI diagnostics; no longer a mandatory sync step |
| `GET …/messages` | Retained | CLI and troubleshooting full reads; compatibility floor for first pull on old servers |
| `PUT …/messages` (merge) | Retained | sess merge CLI's manual write path; **not used by the new sync flow** |
| `POST …/fork` | Retained | sess CLI's lab role |
| `DELETE …/` | Retained | Lifecycle reclamation; no deletion-propagation automation |
| `GET /v1/store/sessions` (list) | Retained | Explicit reconciliation path for full-store discovery |
| **`GET …/changes`** | **New** | The sole incremental-pull read channel (§3–§7) |
| **`POST …/publish`** | **New** | The protocol's sole A→B write (§9) |

### 2.2 The settled division between merge and publish

merge is not replaced, but its sole legitimate use inside the new sync system is the sess CLI's manual-intervention path; `publish` is the protocol's only A→B write. **Subsequent changes to an already-published session are written by claude on B — A should never send write requests for existing sessions**; this invariant itself eliminates an entire family of silent-overwrite races.

## 3. `changes` route shape

```text
GET /v1/store/sessions/{id}/changes?cursor=<opaque string>
```

**GET + query rather than POST**: idempotent read, retries naturally safe; the cursor is an opaque string of ≤80 characters with no request-body need.

**No pagination.** Under every reply the response is a single body — `append` returns the whole delta range, `reset` returns the entire stable prefix. The supporting scale assumption: single session-file size is of the same order as the existing `GET messages` response; the existing client's chunked decoding already carries it. The extension point is reserved in `nextCursor`: the server may truncate the body at any newline boundary and stop `nextCursor` there; the client continuing to pull with it is natural resume, protocol shape unchanged (RFC 0001 §11).

Response shape and error-code table defer to RFC 0001 §6.2/§8, not restated here; this document defines how the server **computes** that response.

## 4. Server-side cursor interpretation algorithm

This is `changes`'s only non-trivial part, and the reason the cache (§6) and lock (§8) exist.

**Key invariant: a single request holds the same open fd throughout, and re-checks the snapshot before sending.** Locate→stat→verify→read all happen on one file snapshot, so a concurrent merge/fork/delete atomic replacement either happens before the fd opens (seeing the old file, results self-consistent for the old file) or after (seeing the new file) — a half-written file is never visible. But the fd pins a **file instance**, not **content** — claude is an in-place writer on the same inode; `O_TRUNC`+rewrite does not change the fd. So after response assembly and before sending, the server must **re-`fstat` and compare the request-start `(size, mtime_ns)`**; mismatch voids the whole attempt and retries; `read_range` short read (request range truncated in place) likewise voids and retries. This compresses the "judgment and range extraction from different sources" window below mtime granularity — the protocol-layer invariant declaration is in RFC 0001 §6.3. This is read-consistency discipline, a different matter from "cursor validity judgment" — judgment has only the fingerprint.

### 4.1 Algorithm (pseudocode)

```text
// Note: {state, body, nextCursor} is an internal sketch, not a wire schema;
// on the wire append/reset go octet-stream + X-ZP-* response headers, unchanged/errors go JSON (RFC 0001 §6.2)

fd  := locate(sessionId)                 // §4.2, returns an open fd; miss → 404 + code:"missing"
                                         // existence check precedes all cursor judgment (RFC 0001 §6.3)
p := parse_cursor(query.cursor)          // absent/empty/zero-value → reset branch;
                                         // parse failure/unknown version → 400
st  := fstat(fd)                         // fstat(fd), not stat(path): pins this request's file snapshot
b   := stable_boundary(fd, st.size)      // §5: [0, b) is the stable prefix; on slot hit reuse slot.offset (§6.3)

// —— judgment order (return on first hit) ——
if p.offset == 0:              → reset(fd, b)     // first pull
if p.offset > b:               → reset(fd, b)     // prefix length promise failed (truncate-rewrite/recreate)
if prefix_hash(fd, p.offset) != p.hash: → reset(fd, b)  // prefix content promise failed
if p.offset == b:              → {state:"unchanged", nextCursor: echo original cursor}
else /* p.offset < b */:       → {state:"append",
                                  body: fd[p.offset .. b],
                                  nextCursor: encode(b, prefix_hash(fd, b))}

reset(fd, b) := {state:"reset", body: fd[0 .. b],
                 nextCursor: encode(b, prefix_hash(fd, b))}

// —— pre-send snapshot re-check (in-place writer defense) ——
st' := fstat(fd)
if (st'.size, st'.mtime_ns) != (st.size, st.mtime_ns): → void whole attempt, retry
// read_range short read (range truncated in place) likewise voids and retries
```

The four verdict branches each correspond to a real scenario: **parse failure → 400** (A's persisted cursor truncated on disk, etc. — if silently treated as first pull, A would take "my own state file corrupted" as "server judged divergence", pointlessly rebuilding everything while never knowing local was broken); **`p.offset > b` → reset** (file truncated and rewritten or deleted and recreated; the interval the cursor promised to cover no longer physically exists); **fingerprint mismatch → reset** (merge whole-file rewrite of the prefix, or "same uuid, different content" divergence — length promise still holds, content promise failed); **caught up/incremental** (boundary advanced past `p.offset` → `append`, otherwise `unchanged` echo).

### 4.2 `locate`: sessionId → file

`locate` recursively searches `projects/**` for the exact filename `<sid>.jsonl`, returning an **open fd** rather than a path — pinning "locate" and "read" to the same file snapshot (if find+open were separated, a rename between stat(path) and open(path) could pin them to different files). sessionId first passes validity checks (rejecting forms containing `/`, `..`) to prevent path traversal. Three boundary rules: only regular files match (symlinks not followed); multiple same-name `<sid>.jsonl` under `projects/**` resolve to the newest mtime with a warn, lexicographic path order being only the tie-break (duplicates are themselves anomalous); `*.tmp.*`, `*.zp*` temp files are skipped. **Duplicate criterion is newest mtime, not lexicographic first** [CLARIFIED — impl/mirror-sync]: claude writes one `<sid>.jsonl` per cwd-mangled `projects/` dir, so resuming the same session from a second directory leaves same-sid files in two dirs; the live copy is the one still being appended, so newest-mtime pins the writable file while a lexicographic winner can pin a stale dead copy and silently stall incremental pull.

Optional implementation optimization: in-process `RwLock<HashMap<sid,(PathBuf,ino)>>` scan-cache — on miss do one full-tree scan and rebuild the whole table (the scan enumerates all files anyway, zero extra cost to build the table); on hit, fstat verifies `ino` still points at the same-name file, mismatch voids that entry and rescans. The index's ino check is an internal consistency self-check (worst cost of a wrong belief is one rescan), unrelated to cursor judgment. At lab scale (45-session-class directory trees) the index is not required; listed as optional optimization, not a correctness condition.

## 5. Stable boundary `stable_boundary`

Definition: file ends with `\n` → boundary = size; otherwise boundary = position after the last `\n`; file has no `\n` or is entirely empty → boundary = 0.

Implementation: seek to a window near the end (last 64KiB) and scan for the last `\n`; if not found in the window, page backward and keep rescanning until found or file head reached. Everything read is on the same fd snapshot. **Two cost clauses**: ① fingerprint-slot hit (`ino,size,mtime,ctime` quadruple re-verification passes) means file bytes unchanged → boundary directly reuses `slot.offset`, O(1) scan-free — this is where the steady-state `unchanged` path's real cost lives; ② incremental rescan for pure-growth scenarios: last scan reached size S with no `\n`, new `\n`s can only land in `[S, size')`, scan only the incremental range (size shrink invalidates and rescans). A large file with no `\n` is pathological input: boundary=0 makes every poll go reset returning empty body + offset-0 cursor — the protocol shape is not wrong, but the client should back off polling rather than 2s-death-loop; listed as a known boundary. `\r\n` line endings are naturally compatible (scanning for the `\n` byte); an all-`\r`-only file with no `\n` has boundary=0 forever and silently stalls, likewise a known boundary.

Basis: JSONL lines contain no newlines, so a line carrying a terminator was necessarily written out completely; Store's `write_lines` ends every line with `\n`, claude JSONL is isomorphic. The unterminated tail is always excluded — when drain and persistence are out of sync, only the terminated part can be taken; this is the strongest guarantee a boundary can provide.

**Explicit qualification**: `nextCursor` only promises "does not cross safely-persisted content" (RFC 0001 §11); **it does not promise "everything claude should write has landed"** — drain→persist ordering is a fact of claude's process lifecycle that B cannot witness; the protocol layer adds no extra field expressing "write what should be written"; the freshness window is expressed by A-side sync progress (RFC 0003 §8.2).

## 6. Fingerprint and cache

### 6.1 Fingerprint algorithm

`prefix_hash(fd, n)` = SHA-256 over the raw bytes `[0, n)`, taking the first 16 bytes. Byte-level fingerprint chosen over "parse lines then fingerprint concatenated content": the Store is a byte-level file projection; tail reads and fingerprint share the same byte definition, no JSON parse needed throughout; merge rewrites produce "semantically equivalent but byte-different" variants (serde re-serialization changes whitespace/field order), and the byte fingerprint judging `reset` on them is conservative and correct — rather transmit once more than miss "same uuid, different content".

### 6.2 Single-slot cache structure

Fingerprint verification is `changes`'s only possibly-expensive step. An in-process cache amortizes it:

```text
Mutex<LruMap<sid → Slot>>            // capacity cap 256 sessions, LRU evict

Slot = { ino: u64,                   // ─┐ slot validity verification fields (fstat compare)
         size: u64,                  //  │ answers "does this slot still describe the same file"
         mtime_ns: i64,              //  │
         ctime_ns: i64,              // ─┘ ctime cannot be forged: content writes must update it, utimes can only change mtime
         offset: u64,                 // byte count at which the last nextCursor was generated (= boundary then)
         hash: [u8; 16] }             // final fingerprint computed over [0, offset)
```

**Single slot, storing only "results" not "process".** Each session keeps only the most recently generated `(offset, hash)` pair; hash is the computed final fingerprint, not a resumable intermediate state. Reason for this simplest form: the cursor a client returns in steady state **is exactly the last `nextCursor` received**, `off == offset` dominates absolutely, and at that point verification hits O(1) with zero reads; `off != offset` (old cursor returns, reconciliation rebuild) is inherently rare, and maintaining "resumable intermediate state" for it — picking a hash library supporting clone/serialize, managing the state's serialization format, one more resume-computation code path — is paying complexity for hypothetical traffic. Rather let the rare path recompute from scratch, keeping the cache as a dictionary "remembering last position and result".

### 6.3 Verification path

```text
Slot hit and (ino,size,mtime,ctime) re-verification passes and off == slot.offset:
    → directly compare slot.hash                    // O(1), zero reads; boundary reuses slot.offset
All other cases (off unequal, slot verification mismatch, slot absent):
    → full recompute prefix_hash(fd, off) from 0     // O(off), rare path
Generating new nextCursor:
    → compute prefix_hash(fd, boundary) from 0, update slot to (boundary, hash)
```

Lock granularity declaration: `Mutex<LruMap>` only does "lookup → release → compute → reinsert" — O(off) recomputation never executes while holding the lock, otherwise one session's slow verification would block all sessions' cache paths.

**`ino` in the cache is a performance hint; in the protocol it is nothing.** The fstat quadruple answers "does this slot still describe the same file" — the worst consequence of a wrong belief is one recomputation. This is different from "cursor carries inode participating in judgment": the latter would write false-positive `reset`s (content-unchanged rewrite swaps inode) into the protocol contract (RFC 0001 §5.3). The same field has exactly opposite standing at the two layers — precisely why it stays in Slot and out of cursor encoding.

### 6.4 The cache is not a correctness basis

**`prefix_hash`'s return value is always "computed from bytes actually read from the fd"** — the cache only decides where to start computing and whether computation can be skipped; it never decides what to judge or how. Walk through one invalidation path to make it clear: merge went temp+rename swapping inode → the next `changes`'s fstat finds the slot's `ino` mismatched → slot voided, full recompute from 0. This request degrades to O(file), but the verdict itself has no change — still the equality comparison "freshly computed fingerprint vs cursor fingerprint".

Boundary rule: in-process memory, never persisted (persisting would turn stub-server from a stateless projection into a stateful service, and serialization across restarts of externally-invisible sequences carries stale-proof risk; restart cost is only one full fingerprint); `fstat` re-verification every request; any suspicious slot is dropped and recomputed.

**Honest declaration of the cache's false-positive residue**: the quadruple (`ino,size,mtime,ctime`) blocks in-place equal-length overwrites that forge mtime like `cp -p`/`utimes` — content writes must update `ctime`, and `ctime` cannot be forged with `utimes`. The residue is only two extreme cases: filesystem `ctime` granularity insufficient (rare) or writer and metadata tampering completing within a nanosecond window (theoretical). **If a false positive occurs, the consequence is permanent silent divergence** — the old `hash` judges valid against a no-longer-existing old prefix, new `[off,b)` bytes get spliced onto A's old prefix, and every subsequent verification passes against the current file; the hybrid has no detection path. This is the worst window of the entire cache mechanism; the reason for accepting it can only be that the trigger surface (requiring simultaneous inode-reuse forging + ctime-granularity escape) is far below the cost of adding protocol-layer defense for it. If deployed in strict environments, another content spot-check layer can be stacked on slot verification (e.g. comparing the first several bytes of the first line), compressing the residue to spot-check granularity.

## 7. Read primitives the Store must add

`changes` needs six read primitives grown inside the Store; all are private/internal — externally there are only the two HTTP surfaces `changes`/`publish`, no proliferation of methods on the Store public API (callers learn two HTTP semantics, not fingerprint/locate mechanisms).

| # | Primitive | Shape | Necessity |
|---|---|---|---|
| 7.1 | Locate | `locate(sid) → Option<File>`: recursive exact-name search, returns open fd | Remove: find+open separation lets a rename between stat/read swap files, fd snapshot invalidated |
| 7.2 | Metadata | `fstat(fd) → (ino, size, mtime_ns)` | Remove: no basis for boundary/cache verification |
| 7.3 | Stable boundary | `stable_boundary(fd, size) → u64` | Remove: half-line writes cannot be excluded, boundary undefined |
| 7.4 | Prefix fingerprint | `prefix_hash(fd, offset)` + single-slot cache | Remove: no criterion for content-level divergence (size/mtime/ino/uuid all fail to cover) |
| 7.5 | Path index | `RwLock<HashMap<sid,(path,ino)>>` | Optional optimization (§4.2) |
| 7.6 | Range read | `read_range(fd, from, to) → Vec<u8>` (pread) | Remove: only whole-file reads exist, incrementals impossible |

**Explicitly not added**: a stat HTTP endpoint (boundary info already folded into changes responses); file-watch notification (B having no change notification is an objective constraint, polling is in-contract behavior); JSON parse on the read path (byte-layer work; parsing is left to the consumer).

## 8. Write path and locks

### 8.1 Premise: write-path atomicity

`write_lines` changes from `std::fs::write` (non-atomic truncate+write) to **same-directory temp file + atomic rename**. This is the premise for unlocking the read path: lock-free reads are safe only under "file content is either old or new, never torn", and rename provides exactly that guarantee, consistent with existing atomic-write convention. The change lands inside `write_lines`; merge/fork/publish's three callers are zero-aware.

Side-effect declaration: this change makes merge start swapping inodes — only affects the cache slot's verification fields (§6.2), produces no protocol semantics.

### 8.2 Lock shape

- **Read path fully lock-free**: `changes`/`messages`/`exists`/`list` do not take `Store.lock`. The fd snapshot + rename atomic write already give a self-consistent view; directory enumeration is rename-concurrency-safe. Gain: changes polling does not serialize with merge/fork; unrelated sessions' writes do not block reads.
- **Write path keeps global mutual exclusion**: `merge`/`fork`/`delete`/`publish` hold the same lock — the first three are already read-modify-write; publish's "existence check + write" must also be mutually exclusive with merge (§9.3). **per-file locks deferred**: write-path call frequency is low (sess CLI manual + publish once per fork); global lock serialization cost is invisible; a per-file lock table is complexity paid for hypothetical throughput.

### 8.3 The exact conditions under which cursor invalidates under merge concurrency

`sess merge` (or any whole-file write) produces two outcomes; the criterion is only prefix bytes:

- **merge changed `[0, cursor.offset)` prefix bytes** → next `changes` fingerprint mismatches → `reset`: that session's issued cursors are void; the caller rebuilds the prefix. This is the expected degradation path (cost one full pull, frequency equals manual merge frequency).
- **merge did not change prefix bytes** (no-op merge, or the union only touches after the cursor) → fingerprint identical → `unchanged` (or normal `append` if new lines exist after the prefix). This is **correct behavior, not a bug**: what the cursor promises is prefix bytes; bytes unchanged means no divergence to report.

Contrast example: A's cursor covers `[0, 34567)`. Ops runs `sess merge` adding a few lines, the union all landing after 34567 — although temp+rename swapped the inode, the fingerprint still matches → `append`; if the union inserted a record inside the first 34567 bytes → fingerprint mismatch → `reset`. Same action, two outcomes; the criterion is always just the prefix bytes.

## 9. Server-side implementation of `publish`

The wire contract (request body, idempotency/conflict semantics, confirmation-cursor construction) defers to RFC 0001 §7; this section fixes three implementation-side matters.

### 9.1 Persistence

The "file does not exist" branch: decode `content` → line validation (every line `\n`-terminated, every line parses as JSON) → sha recheck → write `projects/lab/<id>.jsonl.tmp.<pid>` + fsync → **no-clobber atomic create**: `renameat2(RENAME_NOREPLACE)` (or first `link()` the temp name to target, `EEXIST` rejects then `unlink`) → directory fsync. **Bare rename cannot be used**: between `locate` miss and rename there is a window (fsync can take tens of ms); an external creator (claude self-creating a session, ops manually placing a file) landing inside the window would be silently overwritten by bare rename — on `EEXIST` fall back to §9.2's sha comparison, landing in `already` or `conflict` two-state. Crash-residual `*.tmp.<pid>` is pure garbage: `locate`'s exact name is unaffected; startup or periodic cleanup can sweep-delete. The landing point `projects/lab/` shares the directory with merge's missing-file landing (inheriting existing convention; real claude's resume search scope is an open item; the publish landing point does not change the criterion).

### 9.2 Idempotency judgment order

```text
fd := locate(sid)
fd absent        → atomic create (§9.1), return already:false + cursor(generated)
sha256(fd bytes) == contentSha256
                  → return already:true + cursor(computed fresh over existing file, lands on §6 cache-slot generation path)
otherwise        → 409 {"ok":false,"error":"conflict"}  // never merge, never overwrite
```

Identical sha means identical bytes (256-bit collision margin), no second byte-by-byte comparison needed. The `already:true` branch's fresh fingerprint computation lands exactly on the single-slot cache's generation path; the immediately-following first `changes` hits the slot directly.

### 9.3 Atomicity

"Check existence → decide write/conflict" sits under the same global write lock as merge/fork/delete (§8.2), naturally atomic; the persistence layer's no-clobber (§9.1) patches the residual window of out-of-lock writers. Concurrent writes to the same id are ordered by the lock: whoever gets the lock first is "the fact that happened first"; the later one correctly lands in `already` or `conflict` — no third error state exists. B does not perform "was this id ever deleted" tombstone checks — B stays free of persistent state; "do not publish rolled-back products" is guaranteed by A-side task tables (RFC 0003 §8.1); B's duty ends at faithfully reflecting the relationship between the current file and the incoming content.

## 10. Per-request cost upper bound

| state | Computation (cache hot) | IO |
|---|---|---|
| `unchanged` | `locate` + `fstat` + O(1) (slot quadruple hit → boundary reuses `slot.offset` + fingerprint compare, all read-free) | 0 reads |
| `append` | + full fingerprint computation to boundary + read delta range | O(boundary) |
| `reset` | O(file) fingerprint + O(file) read | O(file), one-shot single body |

Steady-state polling (file unchanged) = `locate` + `fstat` + O(1). Under 45-session sweep, 4 concurrency, `unchanged`-dominated load, each round ≈12 requests, each request sub-millisecond local cost — the cheapness of freshness polling is precisely the premise recognition needs rely on (RFC 0003 §8.2).

**Honest budget during activity**: `append`'s fingerprint **generation** path is O(boundary) regardless of slot hits — a 300MB session, line-dense increments, 2s polling ≈ 300MB read + SHA-256 per active session per round, ~15%+ of a single core plus page-cache flushing, N active sessions stacking linearly. This is the cost face of the "store only final hash" decision: what was rejected back then was resumable intermediate-state complexity on the verification path; the generation path's O(file) is the bill left behind. Beyond this scale there are two mitigation surfaces: E4 active-set TTL and sweep-frequency reduction (RFC 0003 §12-8), or upgrading Slot to chunked checkpoints (store hash state per fixed chunk; prefix fingerprint = resume from nearest checkpoint + tail segment) — the latter only touches Slot's internal structure, protocol shape unchanged, left to pay when scale truly arrives.

## 11. Version compatibility and known boundaries

Version-compatibility behavior defers to RFC 0001 §9 (404 body shape distinguishing "session missing" from "server too old", first-pull compatibility floor, no capability endpoint); not restated here.

| Conclusion | Confidence/boundary |
|---|---|
| Fingerprint as sole decided criterion, three-state responses, publish and merge separation, atomic write unlocking reads | Directly supported by code evidence |
| Single-slot cache correctness | Byte-stream fingerprint properties; cache not a verdict input; residue after quadruple including `ctime` narrows to granularity escape (§6.4); false-positive consequence honestly declared as permanent divergence |
| No-pagination scale assumption | Lab-scale session files same order as messages responses; `reset` of oversized sessions is O(file) single body, `append` generation path O(boundary)/round — declared out-of-domain load, extension point reserved |
| claude append-only assumption | Fingerprint judging `reset` backstops non-append writes; **same-inode in-place writes rely on §4's snapshot re-check defense** (mtime granularity); correctness half-depends on this mechanism |
| Cache residue (same ino+size+mtime+ctime prefix-overwrite missed) | Residue = ctime granularity escape/theoretical-level concurrent tampering; false positive = permanent silent divergence; acceptance reason = trigger surface far below defense cost (§6.4) |
| Global write-lock crash semantics | Lock recommended as `parking_lot::Mutex` (no poisoning) or `lock().unwrap_or_else(|e| e.into_inner())` — a panic inside the lock must not permanently deadlock the write path; lock-internal fsync hangs belong to process-level watchdog |
| publish landing `projects/lab` sufficiency for B resume | Open item: real claude resume directory search scope unverified; landing point inherits existing convention |
| drain→persist ordering | Open item: boundary can only express "written part not crossed", cannot express "everything that should be written is written" (§5) |

## 12. Provenance

Same as RFC 0001 §12.
