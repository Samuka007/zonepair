//! Minimal MCP stdio server (newline-delimited JSON-RPC 2.0), zero deps.
//! Runs on machine A next to the harness; every tool executes HERE, proving
//! A-side execution through the stub relay.

use serde_json::{json, Value};
use std::io::{BufRead, Write};

fn tools() -> Value {
    json!([
        {
            "name": "read_work_file",
            "description": "Read a UTF-8 text file from machine A's /work directory",
            "inputSchema": {
                "type": "object",
                "properties": {"path": {"type": "string", "description": "Path under /work"}},
                "required": ["path"],
            },
        },
        {
            "name": "ping",
            "description": "Liveness probe; returns pong plus the machine A hostname",
            "inputSchema": {"type": "object", "properties": {}},
        },
    ])
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

fn call_tool(name: &str, args: &Value) -> Value {
    if name == "read_work_file" {
        let path = args.get("path").and_then(Value::as_str).unwrap_or_default();
        let resolved = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/work/{path}")
        };
        if !resolved.starts_with("/work/") || resolved.contains("..") {
            return json!({"isError": true, "content": [{"type": "text", "text": format!("path escapes /work: {path}")}]});
        }
        return match std::fs::read_to_string(&resolved) {
            Ok(text) => json!({"content": [{"type": "text", "text": text}]}),
            Err(e) => json!({"isError": true, "content": [{"type": "text", "text": e.to_string()}]}),
        };
    }
    if name == "ping" {
        return json!({"content": [{"type": "text", "text": format!("pong from machine A ({})", hostname())}]});
    }
    json!({"isError": true, "content": [{"type": "text", "text": format!("unknown tool {name}")}]})
}

fn respond(id: &Value, result: Value) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{}", json!({"jsonrpc": "2.0", "id": id, "result": result}));
    let _ = out.flush();
}

fn main() {
    let stdin = std::io::stdin();
    for raw in stdin.lock().lines() {
        let Ok(raw) = raw else { break };
        let raw = raw.trim().to_string();
        if raw.is_empty() {
            continue;
        }
        let Ok(msg) = serde_json::from_str::<Value>(&raw) else { continue };
        let Some(id) = msg.get("id") else { continue }; // notification
        let method = msg.get("method").and_then(Value::as_str).unwrap_or_default();
        match method {
            "initialize" => respond(
                id,
                json!({
                    "protocolVersion": msg.pointer("/params/protocolVersion").cloned().unwrap_or(json!("2024-11-05")),
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "lab", "version": "0.1.0"},
                }),
            ),
            "tools/list" => respond(id, json!({"tools": tools()})),
            "tools/call" => respond(
                id,
                call_tool(
                    msg.pointer("/params/name").and_then(Value::as_str).unwrap_or_default(),
                    msg.pointer("/params/arguments").unwrap_or(&json!({})),
                ),
            ),
            other => respond(id, json!({"error": {"code": -32601, "message": format!("unknown method {other}")}})),
        }
    }
}
