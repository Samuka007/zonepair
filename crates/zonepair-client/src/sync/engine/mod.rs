//! The task execution layer (RFC-0003-zonepaird §6): the executors
//! (`pull`/`publish`/`materialize`/`reconcile`), a worker `pool` with
//! the RFC's selection order, the two-layer retry taxonomy, and the
//! `shared` executor context every one of them reads.
//!
//! Retry layers, exactly as §6.3 draws them:
//!
//! - **transport** — inside `request_with_retry`: 3 attempts, 100/300 ms;
//!   4xx never retries. Already lived-in `transport/http.rs`; this layer
//!   only sees the exhausted result.
//! - **queue** — `Pool::finish`: an executor returning `Retry` books the
//!   task into a backoff slot (`1s → 5s → 30s` cap, `RetryPolicy`
//!   injectable for tests) with a single deadline of `TASK_BUDGET`
//!   (300 s). Over the deadline the task degrades. `Pending` is the
//!   budget-free variant (B answered "not yet" — e.g. a registered
//!   session that has produced no bytes): a short cooldown re-queue, no
//!   retry accounting, no degradation.
//!
//! `watch::Sender` wake-up cache (§7): `Shared.notify` keeps one sender
//! per sid with receivers; every commit signals the sid. It is a cache,
//! never truth — the authoritative data stays on disk.

pub mod apply;
pub mod materialize;
pub mod pool;
pub mod pull;
pub mod publish;
pub mod reconcile;
pub mod shared;

// Flat re-exports — `sync::engine::X` and (through `sync::worker`) the
// pre-restructure `sync::worker::X` paths keep resolving.
pub use pool::{
    consume_journal, delete_journal_records, JournalReport, Pool, RetryPolicy, TaskOutcome, Work,
    POOL_SIZE, PENDING_COOLDOWN, QUEUE_BACKOFFS,
};
pub use shared::{ExecCtx, Notify, Shared, Suspended, TASK_BUDGET};

// ----------------------------------------------------------------- tests ---

/// Test-only helpers the executor and pool test modules share: the
/// scripted-HTTP harness, a mirror/`Shared` factory, and the fast retry
/// policy. Lives here (not in a file) so `engine/{pull,publish,
/// materialize,pool}::tests` all import one `testkit` path.
#[cfg(test)]
    pub(crate) mod testkit {
    use std::path::PathBuf;
    use std::sync::Mutex as StdMutex;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::sync::persist::state::Pull;
    use crate::sync::state_machine::BProbe;
    use crate::transport::{parse_base, Base};

    use super::pool::RetryPolicy;
    use super::shared::{ExecCtx, Shared};

    // ---- serve_script harness (same pattern as api::sync::tests) --------

    pub(crate) enum Scripted {
        Reply(Vec<u8>),
        Drop,
    }

    pub(crate) struct SeenRequest {
        request_line: String,
        /// Read through (the client blocks if we don't consume it) but
        /// only the request line is asserted on — the publish body is
        /// opaque base64 to these tests.
        #[allow(dead_code)]
        body: String,
    }

    pub(crate) fn http_json(status: u16, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status} Z\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        )
        .into_bytes()
    }

    pub(crate) fn http_octets(state: &str, next_cursor: &str, body: &[u8]) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-ZP-State: {state}\r\nX-ZP-Next-Cursor: {next_cursor}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len(),
        )
        .into_bytes()
        .into_iter()
        .chain(body.iter().copied())
        .collect()
    }

    pub(crate) async fn read_request<S: AsyncReadExt + Unpin>(stream: &mut S) -> SeenRequest {
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = stream.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0, "client closed before request headers");
            request.extend_from_slice(&buffer[..count]);
        }
        let head_end = request.windows(4).position(|p| p == b"\r\n\r\n").unwrap();
        let (request_line, content_length) = {
            let head = std::str::from_utf8(&request[..head_end]).unwrap();
            let length = head
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(str::to_string)
                })
                .and_then(|v| v.trim().parse::<usize>().ok());
            (head.lines().next().unwrap().to_string(), length)
        };
        if let Some(length) = content_length {
            while request.len() - (head_end + 4) < length {
                let count = stream.read(&mut buffer).await.unwrap();
                assert_ne!(count, 0, "client closed before request body");
                request.extend_from_slice(&buffer[..count]);
            }
        }
        SeenRequest {
            request_line,
            body: String::from_utf8_lossy(&request[head_end + 4..]).into_owned(),
        }
    }

    pub(crate) async fn serve_script(
        script: Vec<Vec<Scripted>>,
    ) -> (
        Base,
        Arc<StdMutex<Vec<SeenRequest>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = parse_base(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let seen_task = Arc::clone(&seen);
        let server = tokio::spawn(async move {
            for responses in script {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request(&mut stream).await;
                seen_task.lock().unwrap().push(request);
                for response in responses {
                    match response {
                        Scripted::Reply(bytes) => {
                            stream.write_all(&bytes).await.unwrap();
                            stream.flush().await.unwrap();
                        }
                        Scripted::Drop => break,
                    }
                }
                stream.shutdown().await.unwrap();
            }
        });
        (base, seen, server)
    }

    pub(crate) fn lines(seen: &Arc<StdMutex<Vec<SeenRequest>>>) -> Vec<String> {
        seen.lock()
            .unwrap()
            .iter()
            .map(|r| r.request_line.clone())
            .collect()
    }

    pub(crate) async fn finish_server(
        server: tokio::task::JoinHandle<()>,
        seen: &Arc<StdMutex<Vec<SeenRequest>>>,
    ) {
        if tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .is_err()
        {
            panic!(
                "server did not finish its script; requests seen: {:?}",
                lines(seen)
            );
        }
    }

    pub(crate) fn setup(name: &str) -> (PathBuf, Arc<Shared>) {
        crate::sync::persist::fs::testkit::setup_shared("worker", name)
    }

    pub(crate) fn ctx_with(base: &Base, shared: &Arc<Shared>) -> ExecCtx {
        ExecCtx {
            shared: shared.clone(),
            base: Some(base.clone()),
            token: None,
            b_probe: Arc::new(|_| BProbe::Unknown),
        }
    }

    pub(crate) fn pull_state(shared: &Shared, sid: &str) -> Option<Pull> {
        shared.with_state(|st| st.sessions.get(sid).and_then(|r| r.pull.clone()))
    }

    pub(crate) fn fast_policy() -> RetryPolicy {
        RetryPolicy {
            backoffs: vec![Duration::from_millis(1), Duration::from_millis(2)],
            budget: Duration::from_millis(5),
            pending_cooldown: Duration::from_millis(1),
        }
    }
}
