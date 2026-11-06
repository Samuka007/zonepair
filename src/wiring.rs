//! Provider wiring for the real-model suite: where the API calls actually go,
//! and which credential pays for them.
//!
//! Source of truth is the omp agent credential store (`~/.omp/agent/agent.db`,
//! table `auth_credentials`), not the legacy `models.yml` line scrape. The
//! provider name maps to an Anthropic-compatible endpoint, because claude
//! speaks the Anthropic protocol and the provider only has to accept it.

use anyhow::{bail, Context, Result};
use rusqlite::Connection;
use serde_json::Value;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct Wiring {
    pub base_url: String,
    pub auth_token: String,
    pub model: Option<String>,
}

/// Default credential store location.
pub fn agent_db() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_default();
    Path::new(&home).join(".omp/agent/agent.db")
}

/// Anthropic-compatible endpoints per provider, mirroring the official
/// provider integrations (claude speaks Anthropic; the provider accepts it).
fn endpoint_for(provider: &str) -> Option<(&'static str, &'static str)> {
    match provider {
        // Official DeepSeek <-> Claude Code integration.
        "deepseek" => Some(("https://api.deepseek.com/anthropic", "deepseek-flash")),
        // Zhipu's Z.ai gateway speaks the Anthropic protocol with GLM models.
        "zai" | "zhipu-coding-plan" => Some(("https://open.bigmodel.cn/api/anthropic", "glm-5.3-flash")),
        _ => None,
    }
}

/// Read the API key for `provider` from the omp credential store.
fn api_key_from_db(db: &Path, provider: &str) -> Result<String> {
    let con = Connection::open_with_flags(
        format!("file:{}?mode=ro", db.display()),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening credential store {}", db.display()))?;
    let _ = con.busy_timeout(std::time::Duration::from_secs(5));
    let mut stmt = con
        .prepare("SELECT data FROM auth_credentials WHERE provider = ?1")
        .context("query auth_credentials")?;
    let data: String = stmt
        .query_row([provider], |row| row.get(0))
        .with_context(|| format!("no credential for provider {provider:?}"))?;
    let parsed: Value = serde_json::from_str(&data).context("credential data is not JSON")?;
    let key = parsed
        .get("key")
        .and_then(Value::as_str)
        .filter(|k| !k.is_empty())
        .with_context(|| format!("credential for {provider:?} has no key"))?;
    Ok(key.to_string())
}

/// Wire the real-model suite for `provider`.
pub fn for_provider(provider: &str, db: Option<&Path>) -> Result<Wiring> {
    let Some((base_url, model)) = endpoint_for(provider) else {
        bail!(
            "no Anthropic-compatible endpoint known for provider {provider:?} \
             (known: deepseek, zai)"
        );
    };
    let db = db.map(PathBuf::from).unwrap_or_else(agent_db);
    let auth_token = api_key_from_db(&db, provider)?;
    Ok(Wiring {
        base_url: base_url.to_string(),
        auth_token,
        model: Some(model.to_string()),
    })
}
