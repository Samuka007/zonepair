//! Environment allowlist forwarded from client (A) to server (B).
//!
//! Deliberately an allowlist, not a denylist: the server machine must never
//! inherit client-side secrets it does not need (and vice versa).

use std::collections::BTreeMap;

/// Key prefixes forwarded verbatim.
const PREFIXES: &[&str] = &[
    "ANTHROPIC_",
    "CLAUDE_",
    "DISABLE_",
    "ENABLE_",
    "MCP_",
    "MAX_MCP",
    "SSL_CERT",
    "REQUESTS_CA",
    "CURL_CA",
];

/// Exact keys forwarded (proxy + timeout knobs the harness documents).
const EXACT: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "ALL_PROXY",
    "API_TIMEOUT_MS",
];

fn forwarded(key: &str) -> bool {
    EXACT.contains(&key) || PREFIXES.iter().any(|p| key.starts_with(p))
}

/// Snapshot of the current process env restricted to the allowlist.
pub fn collect() -> BTreeMap<String, String> {
    std::env::vars().filter(|(k, _)| forwarded(k)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlist_is_applied() {
        std::env::set_var("CLAUDE_STUB_TEST_ALLOW", "1");
        std::env::set_var("STUB_TEST_DENY", "1");
        std::env::set_var("ANTHROPIC_API_KEY", "sk-test");
        std::env::set_var("RANDOM_TOOLKIT", "1");
        let env = collect();
        assert_eq!(env.get("CLAUDE_STUB_TEST_ALLOW").map(String::as_str), Some("1"));
        assert_eq!(env.get("ANTHROPIC_API_KEY").map(String::as_str), Some("sk-test"));
        assert!(!env.contains_key("STUB_TEST_DENY"));
        assert!(!env.contains_key("RANDOM_TOOLKIT"));
        std::env::remove_var("CLAUDE_STUB_TEST_ALLOW");
        std::env::remove_var("STUB_TEST_DENY");
        std::env::remove_var("ANTHROPIC_API_KEY");
    }
}
