//! Shared framing constants for the `ws-line-v1` carrier.
//!
//! The stdio channel is line-framed: one protocol line per WS text frame, and
//! one child-stdout line per frame back. Reads are bounded so an oversized
//! line fails loudly instead of buffering without limit.

use serde::Deserialize;

pub const MAX_LINE: usize = 256 * 1024;

#[derive(Deserialize, Debug)]
pub struct StdioHeader {
    #[serde(default)]
    pub argv: Vec<String>,
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub cwd: Option<String>,
}
