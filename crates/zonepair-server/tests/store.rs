//! Store semantics: exists / messages / fork / delete against JSONL sessions,
//! plus the RFC-0004-b-endpoints changes-pipeline primitives (locate / fstat /
//! stable_boundary / prefix_hash cache / read_range / no-clobber create).

use zonepair_server::store::{ReadSpan, Store};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use support::{store_at, tmpdir};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

mod support;

fn write_session(root: &Path, id: &str, count: usize) {
    let mut body = String::new();
    let mut parent = json!(null);
    for i in 0..count {
        let line = json!({
            "type": "user",
            "uuid": format!("u-{id}-{i}"),
            "sessionId": id,
            "parentUuid": parent,
            "message": {"role": "user", "content": format!("message {i}")},
        });
        body.push_str(&line.to_string());
        body.push('\n');
        parent = json!(format!("u-{id}-{i}"));
    }
    fs::write(root.join("projects/proj-a").join(format!("{id}.jsonl")), body).unwrap();
}

#[tokio::test]
async fn exists_messages_delete() {
    let (root, store) = store_at("basic");
    assert!(!store.exists("s1").await);
    write_session(&root, "s1", 4);
    assert!(store.exists("s1").await);
    let messages = store.messages("s1").await.unwrap();
    assert_eq!(messages.len(), 4);
    assert_eq!(messages[0]["uuid"], json!("u-s1-0"));
    assert!(store.delete("s1").await.unwrap());
    assert!(!store.exists("s1").await);
    assert!(!store.delete("s1").await.unwrap());
    assert!(store.messages("s1").await.is_err());
}

#[tokio::test]
async fn fork_copies_prefix_with_fresh_ids_and_chain() {
    let (root, store) = store_at("fork");
    write_session(&root, "orig", 5);

    let out = store.fork("orig", "u-orig-2").await.unwrap();
    assert_eq!(out.pairs.len(), 3);
    assert_eq!(out.messages.len(), 3);
    // Old ids preserved in order, new ids all fresh and distinct.
    for (i, (old, new)) in out.pairs.iter().enumerate() {
        assert_eq!(old, &format!("u-orig-{i}"));
        assert_ne!(old, new);
    }
    let news: Vec<&String> = out.pairs.iter().map(|(_, n)| n).collect();
    assert_eq!(news.len(), news.iter().collect::<std::collections::HashSet<_>>().len());

    // New file exists next to the original with the rewritten chain.
    let fork_path = root.join("projects/proj-a").join(format!("{}.jsonl", out.session_id));
    assert!(fork_path.exists());
    let lines: Vec<Value> = fs::read_to_string(&fork_path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 3);
    for (i, line) in lines.iter().enumerate() {
        assert_eq!(line["sessionId"], json!(out.session_id));
        assert_eq!(line["uuid"], json!(out.pairs[i].1));
    }
    // First line keeps its original parent (chain root); the rest point at
    // the previous NEW uuid, so the forked chain is internally consistent.
    assert_eq!(lines[0]["parentUuid"], json!(null));
    assert_eq!(lines[1]["parentUuid"], json!(out.pairs[0].1));
    assert_eq!(lines[2]["parentUuid"], json!(out.pairs[1].1));

    // Original is untouched and the suffix is not copied.
    let orig_lines: Vec<Value> = fs::read_to_string(root.join("projects/proj-a/orig.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(orig_lines.len(), 5);
    assert_eq!(orig_lines[0]["uuid"], json!("u-orig-0"));
    assert_eq!(out.messages[2]["message"]["content"], json!("message 2"));
}

#[tokio::test]
async fn fork_skips_uuidless_bookkeeping_lines() {
    let (root, store) = store_at("fork-bookkeeping");
    let path = root.join("projects/proj-a/real.jsonl");
    let body = [
        json!({"type": "queue-operation", "operation": "enqueue"}),
        json!({"type": "user", "uuid": "ru-0", "sessionId": "real", "parentUuid": null,
               "message": {"role": "user", "content": "hi"}}),
        json!({"type": "attachment", "uuid": "ru-att", "sessionId": "real",
               "parentUuid": "ru-0", "attachment": {"type": "skill_listing"}}),
        json!({"type": "assistant", "uuid": "ru-1", "sessionId": "real", "parentUuid": "ru-att",
               "message": {"role": "assistant", "content": [{"type": "text", "text": "yo"}]}}),
        json!({"type": "last-prompt", "lastPrompt": "hi"}),
    ];
    let mut body_str = String::new();
    for line in body {
        body_str.push_str(&line.to_string());
        body_str.push('\n');
    }
    fs::write(&path, body_str).unwrap();

    let out = store.fork("real", "ru-1").await.unwrap();
    // Bookkeeping lines (no uuid) are not part of the transcript chain.
    assert_eq!(out.pairs, vec![("ru-0".into(), out.pairs[0].1.clone()),
                              ("ru-att".into(), out.pairs[1].1.clone()),
                              ("ru-1".into(), out.pairs[2].1.clone())]);
    let forked = store.messages(&out.session_id).await.unwrap();
    assert_eq!(forked.len(), 3);
    assert_eq!(forked[0]["parentUuid"], json!(null));
    assert_eq!(forked[1]["parentUuid"], json!(out.pairs[0].1));
    assert_eq!(forked[2]["parentUuid"], json!(out.pairs[1].1));
    assert_eq!(forked[2]["message"]["content"][0]["text"], json!("yo"));
}

#[tokio::test]
async fn fork_errors_are_precise() {
    let (root, store) = store_at("fork-err");
    write_session(&root, "orig", 2);
    assert!(store.fork("missing", "u-x").await.is_err());
    assert!(store.fork("orig", "u-not-there").await.is_err());
    // A failed fork must not have left a stray file behind.
    let count = fs::read_dir(root.join("projects/proj-a")).unwrap().count();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn merge_unions_by_uuid_in_stable_order() {
    let (root, store) = store_at("merge");
    write_session(&root, "s1", 3);

    // Same session, one known uuid + one new uuid: only the new one lands,
    // appended after the existing lines (order-stable union).
    let incoming = vec![
        json!({"uuid": "u-s1-0", "sessionId": "s1", "message": {"content": "dup"}}),
        json!({"uuid": "u-new", "sessionId": "s1", "parentUuid": "u-s1-2"}),
    ];
    let out = store.merge("s1", &incoming).await.unwrap();
    assert_eq!(out.added, 1);
    assert_eq!(out.total, 4);
    let messages = store.messages("s1").await.unwrap();
    assert_eq!(messages.len(), 4);
    assert_eq!(messages[0]["uuid"], json!("u-s1-0"));
    assert_eq!(messages[3]["uuid"], json!("u-new"));
    assert_eq!(messages[3]["parentUuid"], json!("u-s1-2"));

    // Re-merging the same lines is idempotent.
    let out = store.merge("s1", &incoming).await.unwrap();
    assert_eq!(out.added, 0);
    assert_eq!(out.total, 4);

    // uuid-less incoming lines are dropped (bookkeeping is regenerated).
    let out = store
        .merge("s1", &[json!({"type": "queue-operation"})])
        .await
        .unwrap();
    assert_eq!(out.added, 0);

    // A merge into an unknown session creates it under projects/lab.
    let out = store
        .merge("fresh", &[json!({"uuid": "u-f1", "sessionId": "fresh"})])
        .await
        .unwrap();
    assert_eq!(out.added, 1);
    assert!(root.join("projects/lab/fresh.jsonl").exists());
}

#[tokio::test]
async fn list_returns_all_session_stems() {
    let (root, store) = store_at("list");
    write_session(&root, "b-second", 1);
    write_session(&root, "a-first", 1);
    let sessions = store.list().await.unwrap();
    assert_eq!(sessions, vec!["a-first".to_string(), "b-second".to_string()]);
}

// ---- RFC-0004-b-endpoints changes-pipeline primitives (T-2xx) --------------------------

fn write_raw(root: &Path, rel: &str, content: &[u8]) -> PathBuf {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, content).unwrap();
    path
}

/// Stamps a file's mtime back `ago` seconds — `set_modified` needs a handle
/// whose metadata may be set, so the file is (re)opened writable.
fn set_mtime(path: &Path, ago: Duration) {
    fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(SystemTime::now() - ago)
        .unwrap();
}

/// T-201: stable_boundary's four shapes (RFC-0004-b-endpoints §5).
#[tokio::test]
async fn stable_boundary_four_shapes() {
    let root = tmpdir("t201");
    let boundary_of = |rel: &str| {
        let path = root.join(rel);
        let fd = fs::File::open(&path).unwrap();
        let size = Store::fstat(&fd).unwrap().size;
        Store::stable_boundary(&fd, size).unwrap()
    };

    // newline-terminated file: boundary == size.
    let p = write_raw(&root, "projects/proj-a/t201-term.jsonl", b"{\"a\":1}\n{\"b\":2}\n");
    assert_eq!(boundary_of("projects/proj-a/t201-term.jsonl"), 16);
    let _ = p;

    // unterminated tail: boundary = just past the LAST newline; the half
    // line is excluded.
    write_raw(&root, "projects/proj-a/t201-tail.jsonl", b"line1\nline2\npartial");
    assert_eq!(boundary_of("projects/proj-a/t201-tail.jsonl"), 12);

    // empty file: 0.
    write_raw(&root, "projects/proj-a/t201-empty.jsonl", b"");
    assert_eq!(boundary_of("projects/proj-a/t201-empty.jsonl"), 0);

    // no newline at all: 0.
    write_raw(&root, "projects/proj-a/t201-nonl.jsonl", b"no newlines here");
    assert_eq!(boundary_of("projects/proj-a/t201-nonl.jsonl"), 0);

    // \r\n endings work for free — only the \n byte is searched.
    write_raw(&root, "projects/proj-a/t201-crlf.jsonl", b"a\r\nb\r\n");
    assert_eq!(boundary_of("projects/proj-a/t201-crlf.jsonl"), 6);

    // page-back: the only newline sits 64KiB before EOF, forcing the scan
    // past the first window. boundary lands at position 2.
    let mut content = b"A\n".to_vec();
    content.extend(std::iter::repeat(b'x').take(70 * 1024));
    write_raw(&root, "projects/proj-a/t201-paged.jsonl", &content);
    assert_eq!(boundary_of("projects/proj-a/t201-paged.jsonl"), 2);
}

/// T-210: a merge mid-read must never tear — the fd pinned by locate keeps
/// the OLD file instance coherent after the atomic rename (RFC-0004-b-endpoints §4.1).
#[tokio::test]
async fn locate_fd_pins_snapshot_across_merge() {
    let (root, store) = store_at("t210");
    write_session(&root, "s1", 3);

    let fd = store.locate("s1").unwrap().unwrap();
    let st = Store::fstat(&fd).unwrap();
    let b = Store::stable_boundary(&fd, st.size).unwrap();
    assert_eq!(b, st.size); // file is newline-terminated

    // merge (temp+rename) swaps the inode while our fd is open.
    let new_ino = fs::metadata(root.join("projects/proj-a/s1.jsonl")).unwrap().ino();
    let _ = store
        .merge("s1", &[json!({"uuid": "u-s1-new", "sessionId": "s1"})])
        .await
        .unwrap();

    // The pinned fd still reads the complete OLD file.
    let span = Store::read_range(&fd, 0, b).unwrap();
    let ReadSpan::Full(bytes) = span else {
        panic!("expected a full pre-merge read");
    };
    assert!(!String::from_utf8_lossy(&bytes).contains("u-s1-new"));

    // The live file is a different inode carrying the merge.
    assert_ne!(fs::metadata(root.join("projects/proj-a/s1.jsonl")).unwrap().ino(), new_ino);
    let st2 = Store::fstat(&fd).unwrap();
    assert_eq!(st2.ino, st.ino); // fd pins the old inode — size may still match
    assert_eq!(st2.size, st.size);
}

/// T-211: in-place O_TRUNC on the same inode is what the second line of
/// defence is for — fstat recheck catches it and read_range reports a
/// distinguishable Short (RFC-0004-b-endpoints §4.1).
///
/// HALF-COVERED per test-spec §8: this drives the truncation explicitly;
/// the real mid-request window needs an injector hook or a stress loop and
/// is covered by e2e T-714.
#[tokio::test]
async fn in_place_truncation_signals_short_read_and_fstat_drift() {
    let (root, store) = store_at("t211");
    write_raw(&root, "projects/proj-a/s1.jsonl", b"line1\nline2\nline3\n");

    let fd = store.locate("s1").unwrap().unwrap();
    let st = Store::fstat(&fd).unwrap();
    let b = Store::stable_boundary(&fd, st.size).unwrap();

    // Same inode, O_TRUNC + rewrite — the classic claude in-place write.
    let w = fs::OpenOptions::new()
        .write(true)
        .open(root.join("projects/proj-a/s1.jsonl"))
        .unwrap();
    w.set_len(10).unwrap();
    use std::os::unix::fs::FileExt;
    w.write_all_at(b"REWRITTEN\n", 0).unwrap();

    // The requested span now runs past the real end → distinguishable Short.
    match Store::read_range(&fd, 0, b).unwrap() {
        ReadSpan::Short { available } => assert!(available.len() < b as usize),
        ReadSpan::Full(_) => panic!("truncated file must not read full"),
    }

    // Same inode (fd pinning exposes in-place writers), drifted snapshot.
    let st2 = Store::fstat(&fd).unwrap();
    assert_eq!(st2.ino, st.ino);
    assert_ne!((st2.size, st2.mtime_ns), (st.size, st.mtime_ns));
}

/// T-220/222: single-slot fingerprint cache — hit path reuses the verified
/// tuple, inode swap (merge's temp+rename) drops the slot and recomputes,
/// and the verdict never depends on the cache (RFC-0004-b-endpoints §6.3/§6.4).
#[tokio::test]
async fn fingerprint_cache_hit_and_invalidation() {
    let (root, store) = store_at("t220");
    write_session(&root, "s1", 2);

    let fd = store.locate("s1").unwrap().unwrap();
    let st = Store::fstat(&fd).unwrap();
    let b = Store::stable_boundary(&fd, st.size).unwrap();

    // Mint a nextCursor → seals the slot with (boundary, hash).
    let sealed = store.seal_prefix("s1", &fd, &st, b).unwrap();

    // Cache hit: same fstat tuple + off == slot.offset → O(1), boundary
    // reusable as slot.offset without a rescan.
    assert_eq!(store.cached_cursor("s1", &st), Some((b, sealed)));
    assert_eq!(store.prefix_hash("s1", &fd, &st, b).unwrap(), sealed);

    // off != slot.offset → no hit, recompute from byte 0.
    assert!(b > 1);
    let short_fp = store.prefix_hash("s1", &fd, &st, b - 1).unwrap();
    assert_ne!(short_fp, sealed); // different prefix → different fingerprint

    // merge's temp+rename swaps the inode: the slot's tuple no longer
    // verifies → drop + recompute (one O(file) request), verdict unchanged.
    store
        .merge("s1", &[json!({"uuid": "u-s1-new", "sessionId": "s1"})])
        .await
        .unwrap();
    let fd2 = store.locate("s1").unwrap().unwrap();
    let st2 = Store::fstat(&fd2).unwrap();
    assert_ne!(st2.ino, st.ino);
    assert!(store.cached_cursor("s1", &st2).is_none()); // stale slot dropped
    let b2 = Store::stable_boundary(&fd2, st2.size).unwrap();
    assert_ne!(store.seal_prefix("s1", &fd2, &st2, b2).unwrap(), sealed);
    // A stale slot is NEVER trusted: tuple mismatch = full recompute.
}

/// T-230: write_lines is atomic (temp+rename) — a reader racing merges sees
/// only complete old or complete new files, never a torn mix (RFC-0004-b-endpoints §8.1).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_merge_never_tears_reads() {
    let (root, store) = store_at("t230");
    let store = Arc::new(store);
    write_session(&root, "s1", 20);

    // Writer: merge one new line at a time so every snapshot is a valid
    // JSONL prefix with contiguous uuids u-s1-0..N.
    let writer = {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            for j in 20..50 {
                store
                    .merge("s1", &[json!({"uuid": format!("u-s1-{j}"), "sessionId": "s1"})])
                    .await
                    .unwrap();
            }
        })
    };

    // Reader: hammer messages() while merges land.
    for _ in 0..60 {
        let messages = store.messages("s1").await.unwrap();
        assert!((20..=50).contains(&messages.len()));
        for (idx, line) in messages.iter().enumerate() {
            assert_eq!(line["uuid"], json!(format!("u-s1-{idx}")));
        }
    }
    writer.await.unwrap();
}

/// T-231 (RFC-0004-b-endpoints §8.3): the only verdict input is the prefix's bytes.
/// (a) merge reserializes every line — a non-canonical file's prefix bytes
/// change → fingerprint moves → reset material.
/// (b) merge whose union lands only AFTER the cursor keeps the prefix
/// byte-identical → fingerprint still matches even though temp+rename
/// swapped the inode → unchanged/append material.
#[tokio::test]
async fn merge_prefix_bytes_are_the_only_verdict() {
    let (root, store) = store_at("t231");
    let seal = |sid: &str| {
        let fd = store.locate(sid).unwrap().unwrap();
        let st = Store::fstat(&fd).unwrap();
        let b = Store::stable_boundary(&fd, st.size).unwrap();
        let fp = store.seal_prefix(sid, &fd, &st, b).unwrap();
        (b, fp, st)
    };

    // (a) non-canonical seed: unsorted keys + inner whitespace — merge
    // rewrites the whole file (BTreeMap reserialize), so the [0, b) prefix
    // BYTES change and the fingerprint moves → reset.
    write_raw(
        &root,
        "projects/proj-a/noncanon.jsonl",
        b"{ \"uuid\": \"u-0\", \"sessionId\": \"noncanon\",  \"z\": 1 }\n",
    );
    let (b, before, _st) = seal("noncanon");
    store
        .merge("noncanon", &[json!({"uuid": "u-1", "sessionId": "noncanon"})])
        .await
        .unwrap();
    let fd = store.locate("noncanon").unwrap().unwrap();
    let st2 = Store::fstat(&fd).unwrap();
    let after = store.prefix_hash("noncanon", &fd, &st2, b).unwrap();
    assert_ne!(after, before);

    // (b) canonical seed: merge appends only after the cursor → prefix is
    // byte-identical; inode swapped but the fingerprint still matches.
    write_session(&root, "s1", 2);
    let (b, before, st) = seal("s1");
    store
        .merge("s1", &[json!({"uuid": "u-s1-new", "sessionId": "s1"})])
        .await
        .unwrap();
    let fd = store.locate("s1").unwrap().unwrap();
    let st2 = Store::fstat(&fd).unwrap();
    assert_ne!(st2.ino, st.ino); // temp+rename swapped the inode
    let after = store.prefix_hash("s1", &fd, &st2, b).unwrap();
    assert_eq!(after, before); // prefix bytes unchanged → fingerprint holds
}

/// T-240: publish's no-clobber create — an external file landing in the
/// window must NOT be silently overwritten; EEXIST sends the caller back to
/// the sha comparison (RFC-0004-b-endpoints §9.1, handler in T-04).
#[tokio::test]
async fn create_exclusive_refuses_to_clobber() {
    let (root, store) = store_at("t240");
    let dir = root.join("projects/lab");

    // Fresh create lands durable content.
    assert!(store.create_exclusive(&dir, "pub1", b"line\n").unwrap());
    assert_eq!(fs::read(dir.join("pub1.jsonl")).unwrap(), b"line\n");

    // Simulated external creator in the window → no clobber, Ok(false) so
    // the caller falls through to the already/conflict sha comparison.
    assert!(!store.create_exclusive(&dir, "pub1", b"OTHER\n").unwrap());
    assert_eq!(fs::read(dir.join("pub1.jsonl")).unwrap(), b"line\n"); // untouched

    // A stray *.tmp.<pid> residue neither blocks creation nor fools locate.
    fs::write(dir.join("stray.jsonl.tmp.12345"), b"junk").unwrap();
    assert!(store.create_exclusive(&dir, "stray", b"s\n").unwrap());
    let fd = store.locate("stray").unwrap().unwrap();
    let st = Store::fstat(&fd).unwrap();
    let ReadSpan::Full(bytes) = Store::read_range(&fd, 0, st.size).unwrap() else {
        panic!()
    };
    assert_eq!(bytes, b"s\n");
}

/// T-241: locate boundaries — invalid ids refused, miss vs hit, symlinks
/// never followed (RFC-0004-b-endpoints §4.2); duplicate-name resolution is
/// covered by the newest-mtime test below.
#[tokio::test]
async fn locate_boundaries() {
    let (root, store) = store_at("t241");
    write_session(&root, "real", 1);

    // Path traversal / malformed ids are refused outright.
    for bad in ["a/b", "..", "../x", "", "a..b", "x/../y"] {
        assert!(store.locate(bad).is_err(), "{bad:?} must be rejected");
    }
    // merge/fork/delete defend the same way (they join paths).
    assert!(store.merge("a/b", &[]).await.is_err());
    assert!(store.fork("../x", "u").await.is_err());
    assert!(store.delete("..").await.is_err());
    // exists keeps its boolean contract for illegal ids.
    assert!(!store.exists("a/b").await);

    // Miss vs hit.
    assert!(store.locate("missing").unwrap().is_none());
    assert!(store.locate("real").unwrap().is_some());

    // Symlinks are never followed: a link named like the session file does
    // not answer, and a link to a real directory is not descended (a cyclic
    // link cannot loop the scan).
    let target = root.join("projects/proj-a/real.jsonl");
    std::os::unix::fs::symlink(&target, root.join("projects/proj-a/linked.jsonl")).unwrap();
    std::os::unix::fs::symlink(
        root.join("projects"),
        root.join("projects/proj-a/dirlink"),
    )
    .unwrap();
    assert!(store.locate("linked").unwrap().is_none());
    let fd = store.locate("real").unwrap().unwrap();
    // Still the real file, readable, and the cyclic link didn't hang us.
    assert!(Store::fstat(&fd).unwrap().size > 0);
}

/// T-241b [CLARIFIED — impl/mirror-sync]: duplicate `<sid>.jsonl` under the
/// tree is abnormal-but-real — the NEWEST-MTIME path wins (with a warn),
/// lexicographic order is only the tie-break (RFC-0004-b-endpoints §4.2).
#[tokio::test]
async fn locate_duplicate_names_pick_newest_mtime() {
    let (root, store) = store_at("t241dup");

    let read_winner = |store: &Store| -> String {
        let fd = store.locate("dup").unwrap().unwrap();
        let st = Store::fstat(&fd).unwrap();
        let ReadSpan::Full(bytes) = Store::read_range(&fd, 0, st.size).unwrap() else {
            panic!()
        };
        String::from_utf8(bytes).unwrap()
    };

    // Newest mtime wins even when it is the lexicographically LATER path —
    // projects/z-proj sorts after projects/a-proj but carries the live copy
    // (the file claude is still appending).
    let stale = write_raw(&root, "projects/a-proj/dup.jsonl", b"MARKER_STALE\n");
    let live = write_raw(&root, "projects/z-proj/dup.jsonl", b"MARKER_LIVE\n");
    set_mtime(&stale, Duration::from_secs(3600));
    set_mtime(&live, Duration::from_secs(60));
    assert_eq!(read_winner(&store), "MARKER_LIVE\n");

    // Equal mtimes: lexicographic path order is the deterministic tie-break.
    let t = SystemTime::now() - Duration::from_secs(120);
    for p in [&stale, &live] {
        fs::OpenOptions::new()
            .write(true)
            .open(p)
            .unwrap()
            .set_modified(t)
            .unwrap();
    }
    assert_eq!(read_winner(&store), "MARKER_STALE\n");
}

