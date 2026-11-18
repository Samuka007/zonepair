use crate::fixtures::{fail, Server};
use crate::mirror::MirrorHome;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use zonepair_client::{proto::cursor, sync::engine::reconcile};

// ---- file fixtures ------------------------------------------------------------

/// `n` uuid-chained session lines; `fork_from` stamps `forkedFrom` on every
/// line — the SDK fork product's marker (§8.3's read rule).
pub(crate) fn session_lines(sid: &str, n: usize, fork_from: Option<&str>) -> Vec<Value> {
    let mut parent = Value::Null;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let uuid = format!("{sid}-u{i}");
        let mut line = json!({
            "type": "user",
            "uuid": uuid,
            "sessionId": sid,
            "parentUuid": parent,
            "message": {"role": "user", "content": format!("{sid} message {i}")},
        });
        if let Some(src) = fork_from {
            line["forkedFrom"] = json!({"sessionId": src});
        }
        parent = json!(uuid);
        out.push(line);
    }
    out
}

pub(crate) fn lines_bytes(lines: &[Value]) -> Vec<u8> {
    let mut out = String::new();
    for l in lines {
        out.push_str(&l.to_string());
        out.push('\n');
    }
    out.into_bytes()
}

pub(crate) fn cursor_for(bytes: &[u8]) -> String {
    cursor::Cursor::new(bytes.len() as u64, cursor::prefix_hash(bytes)).encode()
}

/// Write a whole mirror-side session file (e2e plant), parent dirs + 0600.
pub(crate) fn write_mirror_file(home: &MirrorHome, sid: &str, bytes: &[u8]) -> PathBuf {
    let path = home.mirror.join("projects").join("lab").join(format!("{sid}.jsonl"));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap_or_else(|e| fail(&format!("write {}: {e}", path.display())));
    path
}

pub(crate) fn find_store_file(store_root: &Path, sid: &str) -> Option<PathBuf> {
    let name = format!("{sid}.jsonl");
    let mut stack = vec![store_root.join("projects")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.file_name().is_some_and(|f| f == name.as_str()) {
                return Some(p);
            }
        }
    }
    None
}

/// B's raw session bytes — locate-equivalent scan over the store root.
pub(crate) fn store_bytes(server: &Server, sid: &str) -> Option<Vec<u8>> {
    find_store_file(&server.store, sid).and_then(|p| std::fs::read(&p).ok())
}

pub(crate) fn write_store_file(server: &Server, sid: &str, bytes: &[u8]) -> PathBuf {
    let path = find_store_file(&server.store, sid)
        .unwrap_or_else(|| server.store.join("projects").join("lab").join(format!("{sid}.jsonl")));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap_or_else(|e| fail(&format!("write store {}: {e}", path.display())));
    path
}

/// Append raw bytes to B's file in place — growth that merge/fork don't
/// produce (models a live writer between syncs, torn tails included).
pub(crate) fn append_store_file(server: &Server, sid: &str, tail: &[u8]) {
    use std::io::Write;
    let path = find_store_file(&server.store, sid)
        .unwrap_or_else(|| fail(&format!("no B file for {sid}")));
    let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
    f.write_all(tail).unwrap();
    f.sync_all().unwrap();
}

pub(crate) fn mirror_file(home: &MirrorHome, sid: &str) -> Option<PathBuf> {
    reconcile::session_file(&home.mirror, sid)
}

pub(crate) fn mirror_bytes(home: &MirrorHome, sid: &str) -> Option<Vec<u8>> {
    mirror_file(home, sid).and_then(|p| std::fs::read(&p).ok())
}
pub(crate) fn assert_bytes_eq(what: &str, got: &[u8], want: &[u8]) {
    if got != want {
        let off = got.iter().zip(want.iter()).position(|(a, b)| a != b);
        fail(&format!(
            "{what} diverge (got {} bytes, want {} bytes, first diff at {off:?})",
            got.len(),
            want.len()
        ));
    }
}
