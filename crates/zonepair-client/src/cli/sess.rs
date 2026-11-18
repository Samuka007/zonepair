//! sess: one-shot session-projection client over the HTTP store endpoints.
//! `delete` is the exception (RFC-0003-zonepaird §5 E6): it writes a
//! `note-remote-delete` journal intent — the tombstone is A-side
//! authoritative, and B-side delete propagation is an open contract the
//! RFC deferred, so this command never calls the remote delete as the
//! sync.

use super::zonepaird::enqueue;
use super::{parse_cli, resolve_mirror_dir, resolve_token, resolve_url};
use crate::sync::{journal::Intent, state};
use crate::{api, transport};
use anyhow::{Context, Result};
use serde_json::json;

// ---- sess ---------------------------------------------------------------------

#[derive(clap::Parser)]
#[command(name = "sess")]
struct SessCli {
    /// stub-server base URL (ws:// or http(s)://); falls back to
    /// CLAUDE_STUB_URL, then the client config file
    #[arg(long, global = true)]
    url: Option<String>,
    #[command(subcommand)]
    op: SessOp,
}

#[derive(clap::Subcommand)]
enum SessOp {
    /// Check whether a session exists on B
    Exists {
        /// session id
        session: String,
    },
    /// Read a session's messages
    Messages {
        /// session id
        session: String,
    },
    /// Fork a session at a message index
    Fork {
        /// session id
        session: String,
        /// fork point (message index / cursor)
        #[arg(value_name = "upTo")]
        up_to: String,
    },
    /// Merge messages from a local JSONL file into a session
    Merge {
        /// session id
        session: String,
        /// path to a .jsonl file of messages
        file: String,
    },
    /// Tombstone a session (writes a note-remote-delete intent)
    Delete {
        /// session id
        session: String,
    },
    /// List sessions on B
    List,
}

pub(crate) async fn sess_cli(args: &[String]) -> Result<()> {
    let cli: SessCli = parse_cli("sess", args);

    // E6 `delete` writes a journal intent and never dials B — it must
    // not pay the endpoint resolution the HTTP arms need, or deleting
    // a session fails on a machine with no server configured.
    if let SessOp::Delete { session } = &cli.op {
        let mirror = std::path::PathBuf::from(resolve_mirror_dir(None));
        enqueue(
            &mirror,
            Intent::NoteRemoteDelete {
                sid: session.clone(),
                enqueued_at: state::now_rfc3339(),
            },
        )
        .with_context(|| format!("enqueuing note-remote-delete for {session}"))?;
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({"ok": true, "deleted": true}))?
        );
        return Ok(());
    }

    let base = transport::parse_base(&resolve_url(cli.url)?)?;
    let token = resolve_token(None);

    let out = match cli.op {
        SessOp::Exists { session } => {
            json!({"exists": api::store_exists(&base, token.as_deref(), &session).await?})
        }
        SessOp::Messages { session } => {
            let messages = api::store_messages(&base, token.as_deref(), &session).await?;
            json!({"ok": true, "messages": messages})
        }
        SessOp::Fork { session, up_to } => {
            let (session_id, pairs, messages) =
                api::store_fork(&base, token.as_deref(), &session, &up_to).await?;
            json!({"ok": true, "sessionId": session_id, "pairs": pairs, "messages": messages})
        }
        SessOp::Delete { .. } => unreachable!("delete returns before endpoint resolution"),
        SessOp::List => {
            let sessions = api::store_list(&base, token.as_deref()).await?;
            json!({"ok": true, "sessions": sessions})
        }
        SessOp::Merge { session, file } => {
            // The manual path survives (uuid-union semantics live on B's
            // merge endpoint) — the new sync never goes through here.
            let messages = api::store::read_jsonl(std::path::Path::new(&file))?;
            let (added, total) =
                api::store_merge(&base, token.as_deref(), &session, &messages).await?;
            json!({"ok": true, "added": added, "total": total})
        }
    };

    println!("{}", serde_json::to_string_pretty(&out)?);
    if out.get("ok") == Some(&json!(false)) || out.get("exists") == Some(&json!(false)) {
        std::process::exit(2);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::try_parse_cli;

    fn s(v: &str) -> String {
        v.to_string()
    }

    #[test]
    fn t14_sess_flag_equals_and_subcommands() {
        // --flag=value form (clap gain; the old parser rejected it).
        let cli = try_parse_cli::<SessCli>(
            "sess",
            &[s("--url=http://b:7789"), s("list")],
        )
        .unwrap();
        assert_eq!(cli.url.as_deref(), Some("http://b:7789"));
        assert!(matches!(cli.op, SessOp::List));

        // Space-separated form still works, flag may follow the op now.
        let cli = try_parse_cli::<SessCli>(
            "sess",
            &[s("exists"), s("sess-1"), s("--url"), s("ws://b")],
        )
        .unwrap();
        assert_eq!(cli.url.as_deref(), Some("ws://b"));
        assert!(matches!(cli.op, SessOp::Exists { session } if session == "sess-1"));

        let cli = try_parse_cli::<SessCli>(
            "sess",
            &[s("fork"), s("s1"), s("42")],
        )
        .unwrap();
        assert!(matches!(cli.op, SessOp::Fork { session, up_to } if session == "s1" && up_to == "42"));

        let cli = try_parse_cli::<SessCli>(
            "sess",
            &[s("merge"), s("s1"), s("/tmp/x.jsonl")],
        )
        .unwrap();
        assert!(matches!(cli.op, SessOp::Merge { session, file } if session == "s1" && file == "/tmp/x.jsonl"));

        let cli = try_parse_cli::<SessCli>("sess", &[s("delete"), s("s1")]).unwrap();
        assert!(matches!(cli.op, SessOp::Delete { session } if session == "s1"));

        // Missing subcommand and unknown ops are clap errors now.
        assert!(try_parse_cli::<SessCli>("sess", &[]).is_err());
        assert!(try_parse_cli::<SessCli>("sess", &[s("bogus")]).is_err());
        // Missing positional is a clap error, not a runtime bail.
        assert!(try_parse_cli::<SessCli>("sess", &[s("fork"), s("s1")]).is_err());
    }
}
