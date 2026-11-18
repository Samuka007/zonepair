//! Shared test-support for the zonepair-server integration suites — the
//! crate's ONE copy of the loopback wire harness plus the two-line fixture
//! preambles (`tmpdir`, `store_at`).
//!
//! Why a hand-rolled codec at all: the test-spec calls for
//! `tower::ServiceExt::oneshot` against the Router; `tower` is not a
//! dependency of this crate and Cargo.toml stays untouched, so the harness
//! drives the real `axum::serve` on a loopback port and parses HTTP off the
//! wire — same assertions, strictly more coverage (the bearer middleware,
//! hyper's response framing and the handler all run). This is the only
//! place the codec lives; suites import it instead of re-rolling it.

// Every `tests/*.rs` binary links this module wholesale; a suite that only
// uses `store_at`/`tmpdir` would otherwise report the wire half as dead.
#![allow(dead_code)]

use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zonepair_server::http::{self, AppState};
use zonepair_server::store::Store;

// ---- fixtures ---------------------------------------------------------------

/// Fresh tempdir holding the `projects/proj-a` search dir the session
/// layout scans.
pub fn tmpdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("stub-{tag}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(dir.join("projects/proj-a")).unwrap();
    dir
}

/// `tmpdir` + `Store::new` in one call — the preamble every store-semantics
/// test used to spell out.
pub fn store_at(tag: &str) -> (PathBuf, Store) {
    let root = tmpdir(tag);
    (root.clone(), Store::new(root))
}

// ---- serve ------------------------------------------------------------------

pub struct Server {
    addr: String,
    token: Option<String>,
}

impl Server {
    /// The token this server expects, for building authenticated requests.
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }
}

/// The production Router on a loopback port: bearer middleware, hyper
/// framing and handlers all run for real.
pub async fn serve(root: &Path, token: Option<&str>) -> Server {
    let state = AppState {
        store: Arc::new(Store::new(root.to_path_buf())),
        claude_cmd: vec!["true".into()],
        log_dir: root.join("logs"),
        token: token.map(str::to_string),
        seq: Arc::new(AtomicU64::new(0)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        axum::serve(listener, http::router(state)).await.ok();
    });
    Server {
        addr,
        token: token.map(str::to_string),
    }
}

// ---- wire codec -------------------------------------------------------------

pub struct RawResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl RawResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "body is not JSON ({e}): {:?}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}

/// `token == None` sends no Authorization header at all.
pub async fn request(
    server: &Server,
    method: &str,
    path: &str,
    body: &[u8],
    token: Option<&str>,
) -> RawResponse {
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n");
    if let Some(token) = token {
        req.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    if !body.is_empty() {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    req.push_str("\r\n");

    let mut stream = tokio::net::TcpStream::connect(&server.addr).await.unwrap();
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    parse_response(&raw)
}

fn parse_response(raw: &[u8]) -> RawResponse {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has a header terminator");
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let raw_body = &raw[split + 4..];
    let mut lines = head.lines();
    let status: u16 = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("response has a status line");
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let body = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"))
    {
        decode_chunked(raw_body)
    } else {
        match headers
            .get("content-length")
            .and_then(|v| v.parse::<usize>().ok())
        {
            // `.get(..len)` not `[..len]`: a truncated body must name the
            // framing fault, not panic on a slice bound (S2 §2.1).
            Some(len) => raw_body
                .get(..len)
                .unwrap_or_else(|| {
                    panic!(
                        "body truncated: Content-Length {len} but {} bytes arrived",
                        raw_body.len()
                    )
                })
                .to_vec(),
            None => raw_body.to_vec(),
        }
    };
    RawResponse {
        status,
        headers,
        body,
    }
}

fn decode_chunked(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < raw.len() {
        let Some(end) = raw[pos..].windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let size = usize::from_str_radix(
            std::str::from_utf8(&raw[pos..pos + end])
                .unwrap_or("0")
                .trim(),
            16,
        )
        .unwrap_or(0);
        pos += end + 2;
        if size == 0 {
            break;
        }
        out.extend_from_slice(&raw[pos..pos + size]);
        pos += size + 2; // trailing CRLF
    }
    out
}

pub async fn get(server: &Server, path: &str) -> RawResponse {
    request(server, "GET", path, &[], server.token()).await
}

pub async fn post(server: &Server, path: &str, body: Vec<u8>) -> RawResponse {
    request(server, "POST", path, &body, server.token()).await
}
