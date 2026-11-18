//! Client-side speaker for the `ws-line-v1` carrier: WS for stdio, plain HTTP
//! for the session store. This is the only way a client talks to the server;
//! there is no raw-TCP path.
//!
//! TLS: a `wss://` / `https://` base URL makes the client itself speak TLS
//! (rustls, webpki-roots) to the gateway edge — cloudflare tunnel, nginx, any
//! ADR-0005 terminator. Behind the gateway the carrier stays plain
//! `ws://` / `http://`; direct and loopback deployments keep dialing those.

mod http;
mod ws;

use anyhow::{bail, Result};

pub(crate) use http::request_raw;
pub use http::{
    ensure_ok, request_with_retry, with_retry, StatusError, TransportFault,
    HTTP_CLIENT,
};
/// T-613 test seam: millisecond overrides for the request-phase and
/// body-idle deadlines (0 = production constants).
#[cfg(test)]
pub(crate) use http::{BODY_IDLE_TIMEOUT_MS, REQUEST_PHASE_TIMEOUT_MS};
pub use ws::{open_stdio, Stdio, WsRx, WsTx};

/// Base URL of a ws-line-v1 server: scheme + authority + optional path prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Base(String);

pub fn parse_base(raw: &str) -> Result<Base> {
    let raw = raw.trim().trim_end_matches('/');
    let Some((scheme, rest)) = raw.split_once("://") else {
        bail!("CLAUDE_STUB_URL must include a scheme (ws:// or http://), got {raw:?}");
    };
    match scheme {
        "ws" | "http" | "wss" | "https" => {}
        other => bail!("unknown scheme {other:?} in CLAUDE_STUB_URL"),
    }
    if rest.is_empty() {
        bail!("CLAUDE_STUB_URL needs a host");
    }
    Ok(Base(format!("{scheme}://{rest}")))
}

impl Base {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn url(&self, scheme: &str, path: &str) -> String {
        let rest = self.0.split_once("://").expect("parsed base").1;
        format!("{scheme}://{rest}{path}")
    }

    /// WS endpoint carrying stdio-v1. A TLS base dials `wss://`.
    pub fn stdio_ws(&self) -> String {
        self.url(if self.is_tls() { "wss" } else { "ws" }, "/v1/stdio")
    }

    pub(crate) fn is_tls(&self) -> bool {
        self.0.starts_with("https://") || self.0.starts_with("wss://")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_base_accepts_all_carrier_schemes() {
        for raw in [
            "ws://b:7789",
            "http://b:7789",
            "wss://stub.example.com",
            "https://stub.example.com/zonepair",
        ] {
            parse_base(raw).unwrap_or_else(|e| panic!("{raw}: {e:#}"));
        }
    }

    #[test]
    fn parse_base_rejects_unknown_and_schemeless() {
        assert!(parse_base("ftp://b:7789").is_err());
        assert!(parse_base("b:7789").is_err());
        assert!(parse_base("https://").is_err());
    }

    #[test]
    fn stdio_ws_matches_base_scheme() {
        assert_eq!(
            parse_base("http://b:7789").unwrap().stdio_ws(),
            "ws://b:7789/v1/stdio"
        );
        assert_eq!(
            parse_base("wss://stub.example.com").unwrap().stdio_ws(),
            "wss://stub.example.com/v1/stdio"
        );
        assert_eq!(
            parse_base("https://cf.example.com/zonepair")
                .unwrap()
                .stdio_ws(),
            "wss://cf.example.com/zonepair/v1/stdio"
        );
    }
}
