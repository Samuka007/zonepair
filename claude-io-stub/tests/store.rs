//! Store semantics: exists / messages / fork / delete against JSONL sessions.

use claude_io_stub::store::Store;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};

fn tmpdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("stub-store-{tag}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(dir.join("projects/proj-a")).unwrap();
    dir
}

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
    let root = tmpdir("basic");
    let store = Store::new(root.clone());
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
    let root = tmpdir("fork");
    let store = Store::new(root.clone());
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
    let root = tmpdir("fork-bookkeeping");
    let store = Store::new(root.clone());
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
    let root = tmpdir("fork-err");
    let store = Store::new(root.clone());
    write_session(&root, "orig", 2);
    assert!(store.fork("missing", "u-x").await.is_err());
    assert!(store.fork("orig", "u-not-there").await.is_err());
    // A failed fork must not have left a stray file behind.
    let count = fs::read_dir(root.join("projects/proj-a")).unwrap().count();
    assert_eq!(count, 1);
}
