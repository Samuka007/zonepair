//! Mock `claude` speaking a minimal slice of the stream-json protocol.
//!
//! init -> (assistant text | assistant tool_use -> tool_result -> assistant
//! text) -> result.
//!
//! Like the real claude, each turn is appended to the session's JSONL under
//! `CLAUDE_CONFIG_DIR` (skipped when unset) — the store-mirror suite relies on
//! transcripts actually landing in the store.

use serde_json::{json, Value};
use std::io::{BufRead, Write};

fn emit(obj: &Value) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{}", obj);
    let _ = out.flush();
}

fn arg(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    match args.iter().position(|a| a == name) {
        Some(i) => args.get(i + 1).cloned().unwrap_or_else(|| default.to_string()),
        None => default.to_string(),
    }
}

fn session_id() -> String {
    arg("--session-id", "mock-session")
}

/// Fixed usage numbers the usage-tap suite asserts against. Also shapes the
/// frames closer to real claude, which always carries `message.usage`.
fn usage() -> Value {
    json!({
        "input_tokens": 11,
        "output_tokens": 22,
        "cache_creation_input_tokens": 3,
        "cache_read_input_tokens": 5,
    })
}

fn assistant(session: &str, blocks: Value) -> Value {
    json!({
        "type": "assistant",
        "session_id": session,
        "parent_tool_use_id": null,
        "message": {"id": format!("msg-{session}"), "role": "assistant", "model": "mock-opus", "content": blocks, "usage": usage()},
    })
}

/// Append transcript lines the way real claude does, if a store is set. One
/// session = one file, wherever it was created before us (the store mirror may
/// have pushed it under projects/lab): reuse any existing `<session>.jsonl`,
/// else create under `projects/mock`.
fn persist(session: &str, messages: &[Value]) {
    let Some(config_dir) = std::env::var("CLAUDE_CONFIG_DIR").ok().filter(|v| !v.is_empty()) else {
        return;
    };
    let file_name = format!("{session}.jsonl");
    let mut path = None;
    let mut stack = vec![config_dir.clone()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p.to_string_lossy().to_string());
            } else if p.file_name().is_some_and(|f| f.to_string_lossy() == file_name) {
                path = Some(p);
                break;
            }
        }
        if path.is_some() {
            break;
        }
    }
    let path = path.unwrap_or_else(|| {
        let project = std::path::Path::new(&config_dir).join("projects").join("mock");
        let _ = std::fs::create_dir_all(&project);
        project.join(file_name)
    });

    let mut parent: Option<String> = None;
    if let Ok(existing) = std::fs::read_to_string(&path) {
        parent = existing
            .lines()
            .filter(|l| !l.trim().is_empty())
            .next_back()
            .and_then(|l| serde_json::from_str::<Value>(l).ok())
            .and_then(|v| v.get("uuid").and_then(Value::as_str).map(str::to_string));
    }

    let mut out = String::new();
    for msg in messages {
        let mut entry = msg.clone();
        let uuid = uuid_v4();
        entry["uuid"] = json!(uuid);
        entry["sessionId"] = json!(session);
        entry["parentUuid"] = match &parent {
            Some(p) => json!(p),
            None => json!(null),
        };
        parent = Some(uuid);
        out.push_str(&entry.to_string());
        out.push('\n');
    }
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&path).expect("open transcript");
    let _ = f.write_all(out.as_bytes());
}

/// Enough of a uuid for transcript chaining: random hex, same shape as the
/// real one.
fn uuid_v4() -> String {
    use std::io::Read;
    let mut b = [0u8; 16];
    let _ = std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut b));
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
}

fn tool_result_payload(content: &Value) -> String {
    match content {
        Value::Array(items) => items
            .iter()
            .filter_map(|b| {
                if b.get("type").and_then(Value::as_str) == Some("text") {
                    b.get("text").and_then(Value::as_str).map(str::to_string)
                } else if let Some(s) = b.as_str() {
                    Some(s.to_string())
                } else {
                    None
                }
            })
            .collect(),
        Value::String(s) => s.clone(),
        _ => String::new(),
    }
}

fn result_frame(session: &str, num_turns: u64) -> Value {
    json!({
        "type": "result",
        "subtype": "success",
        "is_error": false,
        "duration_ms": 42,
        "num_turns": num_turns,
        "cost_usd": 0.003,
        "usage": usage(),
        "session_id": session,
        "result": "done",
    })
}

fn main() {
    let session = session_id();
    emit(&json!({
        "type": "system",
        "subtype": "init",
        "session_id": session,
        "cwd": std::env::current_dir().map(|p| p.display().to_string()).unwrap_or_default(),
        "tools": ["mcp__opencode__echo"],
        "model": "mock-opus",
        "permissionMode": "dontAsk",
    }));

    let stdin = std::io::stdin();
    for raw in stdin.lock().lines() {
        let Ok(raw) = raw else { break };
        let raw = raw.trim().to_string();
        if raw.is_empty() {
            continue;
        }
        let Ok(msg) = serde_json::from_str::<Value>(&raw) else { continue };
        if msg.get("type").and_then(Value::as_str) != Some("user") {
            continue;
        }
        let content = msg
            .pointer("/message/content")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let blocks = content.as_array().cloned().unwrap_or_default();
        let texts: Vec<String> = blocks
            .iter()
            .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|b| b.get("text").and_then(Value::as_str).map(str::to_string))
            .collect();
        let tool_results: Vec<&Value> = blocks
            .iter()
            .filter(|b| b.get("type").and_then(Value::as_str) == Some("tool_result"))
            .collect();

        if !tool_results.is_empty() {
            let mut payload = String::new();
            for tr in tool_results {
                payload += &tool_result_payload(tr.get("content").unwrap_or(&json!(null)));
            }
            let ack = json!({"type": "text", "text": format!("TOOL OK {payload}")});
            emit(&assistant(&session, json!([ack.clone()])));
            persist(
                &session,
                &[
                    json!({"type": "user", "message": msg.get("message").cloned().unwrap_or(json!(null))}),
                    json!({"type": "assistant", "message": {"id": format!("msg-{session}"), "role": "assistant", "content": [ack]}}),
                ],
            );
            emit(&result_frame(&session, 2));
            break;
        }

        if texts.iter().any(|t| t.contains("__TOOL__")) {
            emit(&assistant(
                &session,
                json!([{
                    "type": "tool_use",
                    "id": "toolu_mock_1",
                    "name": "mcp__opencode__echo",
                    "input": {"text": "ping"},
                }]),
            ));
            continue;
        }

        let text = format!("ECHO {}", texts.join(" "));
        let ack = json!({"type": "text", "text": text});
        emit(&assistant(&session, json!([ack.clone()])));
        persist(
            &session,
            &[
                json!({"type": "user", "message": msg.get("message").cloned().unwrap_or(json!(null))}),
                json!({"type": "assistant", "message": {"id": format!("msg-{session}"), "role": "assistant", "content": [ack]}}),
            ],
        );
        emit(&result_frame(&session, 1));
    }
}
