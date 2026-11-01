//! Session-store projection on the server side (machine B).
//!
//! Format assumption (documented in the README): the store is a directory tree
//! of `<sessionID>.jsonl` files, each line a JSON object with at least
//! `uuid`, `sessionId`, optional `parentUuid`, plus arbitrary payload fields
//! (`type`, `message`, ...). Fork semantics mirror the Claude Agent SDK:
//! copy the prefix up to and including `upTo`, assign a fresh session id and
//! fresh uuids to every copied line, and rewrite the parentUuid chain.
//!
//! Production note: for a real deployment the fork should be performed by the
//! Agent SDK itself on machine B rather than by this re-implementation; the
//! RPC surface exists so machine A never needs filesystem access to B.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct Store {
    root: PathBuf,
    /// One writer at a time keeps read-modify-write forks atomic. Lab-grade;
    /// shard per file for production throughput.
    lock: Mutex<()>,
}

pub struct ForkOut {
    pub session_id: String,
    /// (old uuid, new uuid) in prefix order.
    pub pairs: Vec<(String, String)>,
    pub messages: Vec<Value>,
}

pub struct MergeOut {
    pub added: usize,
    pub total: usize,
}

impl Store {
    pub fn new(root: PathBuf) -> Self {
        Self { root, lock: Mutex::new(()) }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn find_sync(&self, session: &str) -> Option<PathBuf> {
        let target = format!("{session}.jsonl");
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.file_name().is_some_and(|n| n == target.as_str()) {
                    return Some(path);
                }
            }
        }
        None
    }

    fn require(&self, session: &str) -> Result<PathBuf> {
        self.find_sync(session)
            .with_context(|| format!("session {session} not found under {}", self.root.display()))
    }

    fn read_lines(path: &Path) -> Result<Vec<Value>> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        raw.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).context("store line is not JSON"))
            .collect()
    }

    fn write_lines(path: &Path, lines: &[Value]) -> Result<()> {
        let mut out = String::new();
        for line in lines {
            out.push_str(&line.to_string());
            out.push('\n');
        }
        std::fs::write(path, out).with_context(|| format!("writing {}", path.display()))
    }

    pub async fn exists(&self, session: &str) -> bool {
        let _guard = self.lock.lock().await;
        self.find_sync(session).is_some()
    }

    pub async fn messages(&self, session: &str) -> Result<Vec<Value>> {
        let _guard = self.lock.lock().await;
        Self::read_lines(&self.require(session)?)
    }

    pub async fn fork(&self, session: &str, up_to: &str) -> Result<ForkOut> {
        let _guard = self.lock.lock().await;
        let source = self.require(session)?;
        let lines = Self::read_lines(&source)?;

        let cut = lines
            .iter()
            .position(|l| l.get("uuid").and_then(Value::as_str) == Some(up_to))
            .with_context(|| format!("message {up_to} not found in session {session}"))?;

        let new_id = Uuid::new_v4().to_string();
        let mut pairs = Vec::with_capacity(cut + 1);
        let mut messages = Vec::with_capacity(cut + 1);
        let mut prev_new: Option<String> = None;
        let mut staged: Vec<Value> = Vec::with_capacity(cut + 1);
        for line in &lines[..=cut] {
            // Real stores interleave uuid-less bookkeeping lines
            // (queue-operation, last-prompt, ...). Forks copy the transcript
            // only; claude regenerates bookkeeping itself.
            let Some(old) = line.get("uuid").and_then(Value::as_str) else {
                continue;
            };
            let mut obj = line.clone();
            let old = old.to_string();
            let new = Uuid::new_v4().to_string();
            obj["sessionId"] = json!(new_id);
            obj["uuid"] = json!(new);
            match &prev_new {
                Some(parent) => obj["parentUuid"] = json!(parent),
                // First copied line keeps its original parent (usually null):
                // the fork is a fresh chain root, not a continuation.
                None => {}
            }
            prev_new = Some(new.clone());
            pairs.push((old, new));
            messages.push(obj.clone());
            staged.push(obj);
        }

        let dest = source.with_file_name(format!("{new_id}.jsonl"));
        Self::write_lines(&dest, &staged)?;
        Ok(ForkOut { session_id: new_id, pairs, messages })
    }

    pub async fn delete(&self, session: &str) -> Result<bool> {
        let _guard = self.lock.lock().await;
        match self.find_sync(session) {
            Some(path) => Ok(std::fs::remove_file(path).is_ok()),
            None => Ok(false),
        }
    }

    /// All session ids found under the root (file stems of *.jsonl).
    pub async fn list(&self) -> Result<Vec<String>> {
        let _guard = self.lock.lock().await;
        let mut out = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|e| e == "jsonl") {
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        out.push(stem.to_string());
                    }
                }
            }
        }
        out.sort();
        Ok(out)
    }

    /// Append-only union by uuid, the sync primitive behind the A-side store
    /// mirror (ADR-0004): base order = the existing file, then incoming lines
    /// whose uuid is new, in incoming order. Incoming uuid-less lines are
    /// dropped — bookkeeping lines are regenerated by claude and carry no
    /// cross-machine identity. Both sides only ever append, so the union is
    /// order-stable for the fork/resume prefix checks the Agent SDK makes.
    pub async fn merge(&self, session: &str, incoming: &[Value]) -> Result<MergeOut> {
        let _guard = self.lock.lock().await;
        let path = match self.find_sync(session) {
            Some(path) => path,
            None => {
                let dir = self.root.join("projects").join("lab");
                std::fs::create_dir_all(&dir)
                    .with_context(|| format!("creating {}", dir.display()))?;
                dir.join(format!("{session}.jsonl"))
            }
        };
        let mut lines = if path.exists() { Self::read_lines(&path)? } else { Vec::new() };
        let mut seen: std::collections::HashSet<String> = lines
            .iter()
            .filter_map(|l| l.get("uuid").and_then(Value::as_str).map(str::to_string))
            .collect();
        let mut added = 0usize;
        for line in incoming {
            let Some(uuid) = line.get("uuid").and_then(Value::as_str) else {
                continue;
            };
            if seen.insert(uuid.to_string()) {
                let mut obj = line.clone();
                obj["sessionId"] = json!(session);
                lines.push(obj);
                added += 1;
            }
        }
        Self::write_lines(&path, &lines)?;
        Ok(MergeOut { added, total: lines.len() })
    }
}
