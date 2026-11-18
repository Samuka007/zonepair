//! Store HTTP transport: the reqwest client, TLS configuration and the
//! bounded-retry request path the store API calls build on.

use super::Base;
use anyhow::{bail, Context, Result};
use reqwest::{header::HeaderMap, Client, Method};
use serde_json::{json, Value};
use std::future::Future;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

/// Shared rustls client config (webpki-roots: no system trust store needed,
/// which is what keeps the musl static build self-contained).
pub(crate) fn tls_config() -> Arc<rustls::ClientConfig> {
    static CONFIG: LazyLock<Arc<rustls::ClientConfig>> = LazyLock::new(|| {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    });
    CONFIG.clone()
}

// ---- session store over HTTP -------------------------------------------------

/// TCP connect deadline (RFC-0003-zonepaird §6.2 timeout layering). Deliberately NOT
/// `Client::timeout` — reqwest's request timeout is one wall-clock over
/// the whole exchange including the response body, which would kill a
/// slow-but-alive `reset` download; the streaming phase below judges
/// liveness per chunk instead.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Deadline for everything before the response body when no upload is in
/// flight: request send plus waiting for the response head (§6.2's
/// "no body / body already sent" phases). Once headers arrive the clock moves
/// to `BODY_IDLE_TIMEOUT`.
const REQUEST_PHASE_TIMEOUT: Duration = Duration::from_secs(30);

/// Slowest link the upload phase is willing to believe is still alive
/// (§6.2 throughput liveness applied to the send direction). `reqwest::Body` offers
/// no write-progress hook (the `stream` feature is off), so a byte-level
/// upload liveness watch is unreachable here; the honest minimum is a
/// budget scaled to body size — total wall clock only as a floor, never
/// a fixed 30 s false-kill on multi-MB publishes. A truly dead socket
/// still dies inside the scaled budget, and the queue layer's 300 s task
/// deadline caps the effective wait for worker traffic well below it.
/// 64 KiB/s is far under any real WAN throughput, so a live upload never
/// trips the wall while a wedged one cannot outlive the scaled budget.
const UPLOAD_FLOOR_BPS: u64 = 64 * 1024;

/// Liveness for the streaming body: no bytes for this long means the
/// connection is dead (§6.2 throughput liveness). Re-armed per chunk, so total
/// transfer time is never the trigger — a multi-hundred-MB `reset`
/// that keeps making progress is never cut off.
const BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Test seam (T-613): the stall assertions inject millisecond budgets
/// instead of sleeping real seconds. Production reads the constants.
#[cfg(test)]
pub(crate) static REQUEST_PHASE_TIMEOUT_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
pub(crate) static BODY_IDLE_TIMEOUT_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn request_phase_timeout() -> Duration {
    #[cfg(test)]
    {
        use std::sync::atomic::Ordering;
        let ms = REQUEST_PHASE_TIMEOUT_MS.load(Ordering::Relaxed);
        if ms != 0 {
            return Duration::from_millis(ms);
        }
    }
    REQUEST_PHASE_TIMEOUT
}

/// The phase deadline for one attempt: the flat `REQUEST_PHASE_TIMEOUT`
/// for bodiless requests, `REQUEST_PHASE_TIMEOUT + size/floor` when an
/// upload is in flight. The scaled term is the throughput-liveness proxy —
/// any link sustaining `UPLOAD_FLOOR_BPS` finishes inside it, and only a
/// dead or dying upload hits the wall. See `UPLOAD_FLOOR_BPS` for why a
/// per-byte progress watch is not reachable through `reqwest::Body` here.
fn request_phase_timeout_for(body_len: usize) -> Duration {
    let head = request_phase_timeout();
    if body_len == 0 {
        return head;
    }
    head + Duration::from_secs((body_len as u64) / UPLOAD_FLOOR_BPS)
}

fn body_idle_timeout() -> Duration {
    #[cfg(test)]
    {
        use std::sync::atomic::Ordering;
        let ms = BODY_IDLE_TIMEOUT_MS.load(Ordering::Relaxed);
        if ms != 0 {
            return Duration::from_millis(ms);
        }
    }
    BODY_IDLE_TIMEOUT
}

fn http_client(tls: rustls::ClientConfig) -> Result<Client> {
    Client::builder()
        .tls_backend_preconfigured(tls)
        .connect_timeout(CONNECT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .build()
        .context("building store HTTP client")
}

pub static HTTP_CLIENT: LazyLock<Client> = LazyLock::new(|| {
    http_client(tls_config().as_ref().clone()).expect("valid store HTTP client configuration")
});

/// `request` decodes every reply as JSON (`Value::Null` for empty bodies) —
/// the shape of the original store endpoints. `changes` (RFC-0001-sync-protocol §6.2) does
/// not fit that shape: a byte-carrying verdict is `application/octet-stream`
/// with its metadata in `X-ZP-*` headers, and the 404 body decides between
/// "session missing" and "server too old". Those callers use `request_raw`
/// and classify the reply themselves.
pub(crate) async fn request(
    client: &Client,
    base: &Base,
    method: Method,
    path: &str,
    body: Option<&Value>,
    token: Option<&str>,
) -> Result<(u16, Value)> {
    let (status, _headers, body) = request_raw(client, base, method, path, body, token).await?;
    let value = if body.iter().all(u8::is_ascii_whitespace) {
        Value::Null
    } else {
        serde_json::from_slice(&body).context("non-JSON store response")?
    };
    Ok((status, value))
}

/// `request` minus the JSON decoding: status, raw response headers and the
/// response body as bytes. The body is still buffered whole — every sync
/// endpoint sends one bounded reply (`changes` caps at one file, same scale
/// as `GET …/messages`), so a streaming path adds no value.
///
/// Timeout layering (§6.2): the pre-body phase (send + response head) is
/// capped by `REQUEST_PHASE_TIMEOUT`; once the head lands, the body streams
/// under `BODY_IDLE_TIMEOUT` per chunk — liveness, never a total wall clock.
/// Send failures surface as `reqwest::Error`, timeouts as
/// `tokio::time::error::Elapsed`; the retry policy classifies both.
pub(crate) async fn request_raw(
    client: &Client,
    base: &Base,
    method: Method,
    path: &str,
    body: Option<&Value>,
    token: Option<&str>,
) -> Result<(u16, HeaderMap, Vec<u8>)> {
    let url = base.url(if base.is_tls() { "https" } else { "http" }, path);
    let mut request = client
        .request(method, url)
        .header("Accept", "application/json");
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    // Serialize once here: the wire bytes set the upload-phase budget and
    // are the request body verbatim (`request.json` would re-serialize and
    // hide the length behind the builder).
    let mut body_len = 0usize;
    if let Some(body) = body {
        let bytes = serde_json::to_vec(body).context("serializing store request body")?;
        body_len = bytes.len();
        request = request
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(bytes);
    }
    let mut response = tokio::time::timeout(
        request_phase_timeout_for(body_len),
        request.send(),
    )
    .await
    .context("store request: no response head before the phase timeout")?
    .context("sending store request")?;
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let body = read_body(&mut response).await?;
    Ok((status, headers, body))
}

/// Aggregate the response body one chunk at a time with `BODY_IDLE_TIMEOUT`
/// as a per-chunk liveness check. `Response::bytes()` would also aggregate,
/// but a single timeout around it caps the WHOLE transfer — the exact wall
/// clock §6.2 forbids. Per-chunk re-arming means a stalled peer dies in
/// `BODY_IDLE_TIMEOUT` while a large healthy reply runs as long as it must.
async fn read_body(response: &mut reqwest::Response) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = tokio::time::timeout(body_idle_timeout(), response.chunk())
        .await
        .context("store response body stalled (no bytes before idle timeout)")?
        .context("reading store response")?
    {
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Backoff between attempts of one store request. Three attempts total; the
/// delays are fixed (no jitter) so tests are deterministic and the whole
/// pre-relay sync cannot stall for more than ~0.4 s on a flaky link.
const RETRY_BACKOFF: [std::time::Duration; 2] = [
    std::time::Duration::from_millis(100),
    std::time::Duration::from_millis(300),
];

/// A response whose shape violates the endpoint's reply contract, treated as
/// a transport error: the `changes` octet-stream verdict without its
/// `X-ZP-*` headers or with an unexpected `Content-Type` is the same class
/// of failure as a short read — the reply never became meaningful data, so
/// the retry budget applies (RFC-0001-sync-protocol §6.4, "short read / missing metadata header" row).
#[derive(Debug)]
pub struct TransportFault(String);

impl TransportFault {
    pub fn new(detail: impl Into<String>) -> Self {
        Self(detail.into())
    }
}

impl std::fmt::Display for TransportFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "malformed sync reply: {}", self.0)
    }
}

impl std::error::Error for TransportFault {}

/// A definitive HTTP failure status (any non-2xx the caller cannot satisfy
/// with a dedicated reply variant). Carries the status so the retry policy
/// can apply the same rule as raw-response callers — 5xx retries, 4xx does
/// not — and surfaces as `store op failed (<status>): <error body>` after
/// retries are exhausted.
#[derive(Debug)]
pub struct StatusError {
    pub status: u16,
    pub detail: String,
}

impl StatusError {
    pub fn new(status: u16, detail: impl Into<String>) -> Self {
        Self {
            status,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for StatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "store op failed ({}): {}", self.status, self.detail)
    }
}

impl std::error::Error for StatusError {}

/// Transport-level failure worth one more attempt: connect refused/reset,
/// timeout, or a request/body/decode error (a peer that hung up
/// mid-exchange, including a body cut short of its Content-Length — reqwest
/// surfaces that truncation as a decode error, and it is the same class of
/// failure as a dropped connection). The `tokio::time::Elapsed` produced by
/// the request-phase and body-idle deadlines is classified alongside in
/// `retryable_error` — same failure class, different error type. Nothing in
/// this crate decodes response bodies through `Response::json`, so
/// `is_decode` here can only mean a framing/encoding failure on the wire.
fn retryable_transport(error: &reqwest::Error) -> bool {
    error.is_timeout()
        || error.is_connect()
        || error.is_request()
        || error.is_body()
        || error.is_decode()
}

/// 5xx only. 4xx is definitive (`404` must stay "not stored") and is never
/// retried.
fn retryable_status(status: u16) -> bool {
    (500..600).contains(&status)
}

/// One error-channel retry decision, shared by every caller of `with_retry`:
/// `reqwest` send/read errors as before, our own `Elapsed` timeouts (a dead
/// peer is a transport failure regardless of which clock noticed),
/// `TransportFault` (a reply that failed to become meaningful data) always,
/// and `StatusError` only when its status is in 5xx territory.
fn retryable_error(error: &anyhow::Error) -> bool {
    if let Some(e) = error.downcast_ref::<reqwest::Error>() {
        return retryable_transport(e);
    }
    if error.is::<tokio::time::error::Elapsed>() {
        return true;
    }
    if error.is::<TransportFault>() {
        return true;
    }
    if let Some(e) = error.downcast_ref::<StatusError>() {
        return retryable_status(e.status);
    }
    false
}

/// Bounded retries around one attempt of a request: three tries total,
/// `RETRY_BACKOFF` between them. `op` produces one classified attempt —
/// `(status, reply)` — and the decision to try again applies the same rule
/// to every failure form: retryable HTTP status, retryable `reqwest` error,
/// `TransportFault`, or retryable `StatusError`. Everything else (bad JSON,
/// a bug, a 4xx the caller did not classify away) fails on the spot.
///
/// Classification that turns a *received reply* into an error — a missing
/// `X-ZP-*` header, an unexpected `Content-Type` — must live inside `op`, or
/// it runs once outside the retry loop and never sees the budget.
pub async fn with_retry<T, F, Fut>(mut op: F) -> Result<(u16, T)>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(u16, T)>>,
{
    let mut attempt = 0;
    loop {
        let outcome = op().await;
        let retries_left = RETRY_BACKOFF.len() - attempt;
        let retry = retries_left > 0
            && match &outcome {
                Ok((status, _)) => retryable_status(*status),
                Err(error) => retryable_error(error),
            };
        if !retry {
            return outcome;
        }
        tokio::time::sleep(RETRY_BACKOFF[attempt]).await;
        attempt += 1;
    }
}

/// `request` with bounded retries on transport errors and 5xx responses. The
/// final failure keeps the same contextual error `request` produced.
pub async fn request_with_retry(
    client: &Client,
    base: &Base,
    method: Method,
    path: &str,
    body: Option<&Value>,
    token: Option<&str>,
) -> Result<(u16, Value)> {
    with_retry(|| request(client, base, method.clone(), path, body, token)).await
}

pub fn ensure_ok(status: u16, value: &Value) -> Result<()> {
    if (200..300).contains(&status) && value.get("ok") != Some(&json!(false)) {
        return Ok(());
    }
    let error = value
        .get("error")
        .map(|e| e.to_string())
        .unwrap_or_else(|| value.to_string());
    bail!("store op failed ({status}): {error}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::store_list;
    use crate::transport::parse_base;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    const CHUNKED_RESPONSE: &[u8] =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
          17\r\n{\"ok\":true,\"sessions\":[\r\n\
          a\r\n\"gateway\"]\r\n\
          1\r\n}\r\n0\r\n\r\n";

    async fn reply<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, response: &[u8]) {
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = stream.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0, "client closed before request headers");
            request.extend_from_slice(&buffer[..count]);
        }
        stream.write_all(response).await.unwrap();
        stream.flush().await.unwrap();
    }

    async fn serve_http(response: &'static [u8]) -> (Base, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = parse_base(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            reply(&mut stream, response).await;
            stream.shutdown().await.unwrap();
        });
        (base, server)
    }

    // R1-I2: the phase deadline scales with upload size — the flat 30 s is
    // only for bodiless sends; a multi-MB body gets the floor-throughput
    // budget so a live slow upload is never wall-clocked (T-613 direction).
    #[test]
    fn phase_timeout_scales_with_body_size() {
        // No injection in this test's assertions — measure the delta, which
        // is the size-proportional term regardless of the injected head.
        let no_body = request_phase_timeout_for(0);
        let small = request_phase_timeout_for(1);
        // floor = 64 KiB/s → each 64 KiB of body buys exactly one second.
        assert_eq!(small - no_body, Duration::from_secs(0));
        let mb = request_phase_timeout_for(64 * 1024 * 1024);
        assert_eq!(mb - no_body, Duration::from_secs(1024));
        let kb = request_phase_timeout_for(64 * 1024);
        assert_eq!(kb - no_body, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn store_list_decodes_chunked_gateway_response() {
        let (base, server) = serve_http(CHUNKED_RESPONSE).await;
        let sessions =
            tokio::time::timeout(std::time::Duration::from_secs(5), store_list(&base, None))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(sessions, ["gateway"]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn store_list_rejects_truncated_body() {
        let (base, server) = serve_http(
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n\
              {\"ok\":true,\"sessions\":[\"incomplete\"]}",
        )
        .await;
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(5), store_list(&base, None))
                .await
                .unwrap();
        assert!(
            result.is_err(),
            "accepted a body shorter than Content-Length"
        );
        server.await.unwrap();
    }

    async fn serve_tls(response: &'static [u8]) -> (Base, Client, tokio::task::JoinHandle<()>) {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.der().clone()).unwrap();
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let client = http_client(tls).unwrap();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(signing_key.serialize_der());
        let tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.der().clone()], key.into())
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = parse_base(&format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port(),
        ))
        .unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            reply(&mut stream, response).await;
            // Deliberately drop TCP without TLS close_notify, as gateways may.
        });
        (base, client, server)
    }

    #[tokio::test]
    async fn tls_chunked_body_completes_without_close_notify() {
        let (base, client, server) = serve_tls(CHUNKED_RESPONSE).await;
        let (status, value) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            request(
                &client,
                &base,
                Method::GET,
                "/v1/store/sessions",
                None,
                None,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(status, 200);
        assert_eq!(value, json!({"ok": true, "sessions": ["gateway"]}));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tls_truncated_body_is_not_an_empty_success() {
        let (base, client, server) =
            serve_tls(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n").await;
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            request(
                &client,
                &base,
                Method::GET,
                "/v1/store/sessions",
                None,
                None,
            ),
        )
        .await
        .unwrap()
        .expect_err("accepted a truncated TLS response as empty data");
        let cause = error
            .downcast_ref::<reqwest::Error>()
            .expect("HTTP body error");
        assert!(cause.is_body() || cause.is_decode(), "{error:#}");
        server.await.unwrap();
    }
}
