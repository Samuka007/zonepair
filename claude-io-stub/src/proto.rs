//! Line-based hello/request framing shared by both connection types.
//!
//! The stdio channel is raw bytes after the ack; only hello/ack and the sess
//! protocol are line-delimited JSON. Reads are byte-wise so no bytes past the
//! first newline are ever buffered away from the raw relay.

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

pub const MAX_LINE: usize = 256 * 1024;

#[derive(Deserialize, Debug)]
pub struct StdioHeader {
    #[serde(default)]
    pub argv: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub cwd: Option<String>,
}

#[derive(Debug)]
pub enum Hello {
    Stdio(StdioHeader),
    Sess,
    /// Pre-opened reverse tunnel. `side: "a"` queues an A-initiated stream;
    /// `side: "b"` (from claude's MCP helper on B, dialing B loopback) gets
    /// paired with one queued stream. B never dials A's address.
    Tunnel { side: String },
}

/// Read one `\n`-terminated line without consuming past it.
/// `Ok(None)` on clean EOF with no bytes read.
pub async fn read_line(rx: &mut OwnedReadHalf) -> Result<Option<String>> {
    let mut buf: Vec<u8> = Vec::new();
    let mut one = [0u8; 1];
    loop {
        let n = rx.read(&mut one).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            bail!("connection closed mid-line");
        }
        if one[0] == b'\n' {
            if buf.len() > MAX_LINE {
                bail!("line exceeds {} bytes", MAX_LINE);
            }
            return Ok(Some(String::from_utf8(buf).context("hello line not utf-8")?));
        }
        buf.push(one[0]);
        if buf.len() > MAX_LINE {
            bail!("line exceeds {} bytes", MAX_LINE);
        }
    }
}

pub async fn write_line(tx: &mut OwnedWriteHalf, value: &Value) -> Result<()> {
    let mut line = value.to_string();
    line.push('\n');
    tx.write_all(line.as_bytes()).await?;
    tx.flush().await?;
    Ok(())
}

pub fn parse_hello(line: &str) -> Result<Hello> {
    let v: Value = serde_json::from_str(line).context("hello line is not JSON")?;
    let proto = v
        .get("proto")
        .and_then(Value::as_str)
        .context("hello missing proto")?;
    match proto {
        "stdio-v1" => {
            let header: StdioHeader =
                serde_json::from_value(v).context("bad stdio-v1 header")?;
            Ok(Hello::Stdio(header))
        }
        "sess-v1" => Ok(Hello::Sess),
        "tunnel-v1" => {
            let side = v.get("side").and_then(Value::as_str).context("tunnel hello missing side")?;
            Ok(Hello::Tunnel { side: side.to_string() })
        }
        other => bail!("unknown proto {other:?}"),
    }
}
