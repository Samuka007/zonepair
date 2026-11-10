//! The client config store (ADR-0008): stub-client's own durable knobs —
//! server URL, bearer token, mirror dir, mirror switch — in one TOML file on
//! machine A. Resolution order everywhere is flag > process env > this file >
//! built-in default; the opencode server env is not a read source. Only the
//! two keys opencode itself consumes (`OPENCODE_CLAUDE_PATH`,
//! `CLAUDE_CONFIG_DIR`) are written there, by `setup`.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};

pub const FILE_VERSION: i64 = 1;

fn default_version() -> i64 {
    FILE_VERSION
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientConfig {
    #[serde(default = "default_version")]
    pub version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mirror_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mirror: Option<bool>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            version: FILE_VERSION,
            url: None,
            token: None,
            mirror_dir: None,
            mirror: None,
        }
    }
}

/// Config file path: `$ZONEPAIR_CLIENT_CONFIG` overrides, else the platform
/// config dir + `zonepair/client.toml` (via the `dirs` crate: XDG base dirs on
/// Linux — `$XDG_CONFIG_HOME` or `~/.config` —, `~/Library/Application
/// Support` on macOS, Roaming AppData on Windows).
pub fn path() -> Result<PathBuf> {
    if let Some(p) = nonempty_env("ZONEPAIR_CLIENT_CONFIG") {
        return Ok(PathBuf::from(p));
    }
    let dir = dirs::config_dir()
        .ok_or_else(|| anyhow::anyhow!("no config directory — set ZONEPAIR_CLIENT_CONFIG"))?;
    Ok(dir.join("zonepair").join("client.toml"))
}

fn nonempty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// Missing file = empty config. A malformed file is a loud error: silently
/// ignoring it would make resolution fall through to defaults and shadow the
/// user's real values.
pub fn load() -> Result<ClientConfig> {
    load_from(&path()?)
}

pub fn load_from(path: &Path) -> Result<ClientConfig> {
    if !path.exists() {
        return Ok(ClientConfig::default());
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading client config {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("parsing client config {}", path.display()))
}

/// Write atomically (temp file in the same dir, then rename). The file holds
/// the bearer token, so it is created 0600 on unix — the secret never sits at
/// default permissions, not even briefly.
pub fn save(cfg: &ClientConfig) -> Result<PathBuf> {
    let path = path()?;
    save_to(&path, cfg)?;
    Ok(path)
}

pub fn save_to(path: &Path, cfg: &ClientConfig) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating client config dir {}", dir.display()))?;
    }
    let body = toml::to_string_pretty(cfg).context("serializing client config")?;
    let tmp = path.with_extension("toml.tmp");
    // A crashed run can leave the tmp behind; start clean.
    let _ = std::fs::remove_file(&tmp);
    {
        let mut file = tmp_open(&tmp)?;
        file.write_all(body.as_bytes())
            .with_context(|| format!("writing {}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path)
        .with_context(|| format!("renaming {} onto {}", tmp.display(), path.display()))?;
    Ok(())
}

#[cfg(unix)]
fn tmp_open(tmp: &Path) -> Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    Ok(std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(tmp)?)
}

#[cfg(not(unix))]
fn tmp_open(tmp: &Path) -> Result<std::fs::File> {
    Ok(std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(tmp)?)
}

/// Remove the file. Returns false when there was nothing to remove.
pub fn erase() -> Result<bool> {
    erase_from(&path()?)
}

pub fn erase_from(path: &Path) -> Result<bool> {
    if path.exists() {
        std::fs::remove_file(path)
            .with_context(|| format!("removing client config {}", path.display()))?;
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        std::env::temp_dir().join(format!("zonepair-cfg-test-{}-{port}", std::process::id()))
    }

    fn sample(token: Option<&str>) -> ClientConfig {
        ClientConfig {
            url: Some("ws://b:7789".into()),
            token: token.map(str::to_string),
            mirror_dir: Some("/tmp/mirror".into()),
            mirror: Some(true),
            ..Default::default()
        }
    }

    #[test]
    fn roundtrip_and_permissions() {
        let dir = tmp();
        let path = dir.join("client.toml");
        let cfg = sample(Some("sekret"));
        save_to(&path, &cfg).unwrap();
        assert_eq!(load_from(&path).unwrap(), cfg);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = path.metadata().unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "token file must be 0600, got {mode:o}");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn missing_file_is_empty() {
        let dir = tmp();
        let path = dir.join("nested").join("client.toml");
        assert_eq!(load_from(&path).unwrap(), ClientConfig::default());
    }

    #[test]
    fn malformed_is_loud() {
        let dir = tmp();
        let path = dir.join("client.toml");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&path, "url = [\nbroken").unwrap();
        assert!(load_from(&path).is_err(), "malformed file must error");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn overwrite_and_erase() {
        let dir = tmp();
        let path = dir.join("client.toml");
        save_to(&path, &sample(Some("a"))).unwrap();
        save_to(&path, &sample(None)).unwrap();
        assert_eq!(load_from(&path).unwrap(), sample(None));
        assert!(erase_from(&path).unwrap());
        assert!(!erase_from(&path).unwrap(), "second erase is a no-op");
        assert_eq!(load_from(&path).unwrap(), ClientConfig::default());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
