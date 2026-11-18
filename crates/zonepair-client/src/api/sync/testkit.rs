//! Scripted HTTP/1.1 server for wire-layer tests: every connection reads
//! one full request, records it, and answers from a per-connection script
//! — sequential and inline, so a client that diverges from the script
//! fails the join timeout instead of hanging.
//!
//! `#[cfg(test)]` and `pub(crate)`: test-only code, shared inside the
//! crate. `sync::worker::tests` still carries its own earlier copy of the
//! Reply/Drop subset; a later consolidation pass can re-home that one
//! here (it only lacks the `Hold`/`Drip` arms this version grew for
//! T-613). The server's own `zonepair-server/tests/sync_routes.rs` codec
//! is intentionally separate — different crate, different assertions.

use crate::transport::{parse_base, Base};
use anyhow::Result;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// One scripted server response: write `bytes` and shut down, drop the
/// connection without answering (a reset mid-request), write an
/// optional head then hold the connection open until the client gives
/// up (a stalled peer — the T-613 timeout input), or write slices of
/// one reply with a delay between them (slow liveness — per-chunk,
/// never total).
pub(crate) enum Scripted {
    Reply(Vec<u8>),
    Drop,
    Hold(Vec<u8>),
    Drip(Vec<Vec<u8>>, Duration),
}

/// What the harness recorded of one client request.
pub(crate) struct SeenRequest {
    pub(crate) request_line: String,
    pub(crate) body: String,
}

/// A `200`-series JSON reply (status line + body), `Connection: close`.
pub(crate) fn http_json(status: u16, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status} Z\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    )
    .into_bytes()
}

/// An octet-stream `changes` reply: verdict + next cursor in `X-ZP-*`
/// headers, raw bytes as body (§6.2).
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

async fn read_request<S: AsyncReadExt + Unpin>(stream: &mut S) -> SeenRequest {
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
    // Read the body too, so the client never sees a truncated request
    // before we answer or drop — and so tests can inspect what was sent.
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

/// Serve exactly `script.len()` connections: connection N reads one full
/// request, records it, and answers from `script[N]`. Sequential and
/// inline — the client never pipelines — so a script longer than the
/// client's request sequence fails the join timeout instead of hanging.
pub(crate) async fn serve_script(
    script: Vec<Vec<Scripted>>,
) -> (Base, Arc<Mutex<Vec<SeenRequest>>>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = parse_base(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_task = Arc::clone(&seen);
    let server = tokio::spawn(async move {
        for responses in script {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            seen_task.lock().unwrap().push(request);
            // `hold` keeps the accepted socket open (stalled-peer shape)
            // instead of shutting it down; the client must time out
            // first or `finish_server` fails the test.
            let mut hold = false;
            for response in responses {
                match response {
                    Scripted::Reply(bytes) => {
                        stream.write_all(&bytes).await.unwrap();
                        stream.flush().await.unwrap();
                    }
                    // Drop without responding: the client sees a reset.
                    Scripted::Drop => break,
                    // Write an optional partial reply, then wait for the
                    // client to close — a peer that stalls mid-response.
                    Scripted::Hold(bytes) => {
                        if !bytes.is_empty() {
                            stream.write_all(&bytes).await.unwrap();
                            stream.flush().await.unwrap();
                        }
                        hold = true;
                        break;
                    }
                    // Slow liveness: write each slice, sleeping `gap`
                    // between writes — total time exceeds the injected
                    // idle budget while every gap stays under it.
                    Scripted::Drip(slices, gap) => {
                        for slice in slices {
                            stream.write_all(&slice).await.unwrap();
                            stream.flush().await.unwrap();
                            tokio::time::sleep(gap).await;
                        }
                    }
                }
            }
            if hold {
                let mut sink = [0u8; 256];
                let _ = stream.read(&mut sink).await;
            } else {
                stream.shutdown().await.unwrap();
            }
        }
    });
    (base, seen, server)
}

/// Run a wire call with a 5s bound — a stuck client fails the test (with
/// the recorded request list) instead of hanging it.
pub(crate) async fn run<Fut, T>(seen: &Arc<Mutex<Vec<SeenRequest>>>, fut: Fut) -> T
where
    Fut: std::future::Future<Output = Result<T>>,
{
    match tokio::time::timeout(std::time::Duration::from_secs(5), fut).await {
        Ok(outcome) => outcome.unwrap(),
        Err(_) => panic!("request timed out; requests seen: {:?}", lines(seen)),
    }
}

/// The server must finish its whole script; a client that diverged
/// (extra or missing requests) fails here with the recorded list rather
/// than hanging the suite.
pub(crate) async fn finish_server(
    server: tokio::task::JoinHandle<()>,
    seen: &Arc<Mutex<Vec<SeenRequest>>>,
) {
    if tokio::time::timeout(std::time::Duration::from_secs(2), server)
        .await
        .is_err()
    {
        panic!(
            "server did not finish its script; requests seen: {:?}",
            lines(seen)
        );
    }
}

/// The recorded request lines, in accept order.
pub(crate) fn lines(seen: &Arc<Mutex<Vec<SeenRequest>>>) -> Vec<String> {
    seen.lock()
        .unwrap()
        .iter()
        .map(|r| r.request_line.clone())
        .collect()
}
