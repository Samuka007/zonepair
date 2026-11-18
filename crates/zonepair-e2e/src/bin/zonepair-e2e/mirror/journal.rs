use crate::fixtures::fail;
use crate::mirror::MirrorHome;
use serde_json::Value;
use std::path::PathBuf;
use zonepair_client::sync::{
    journal::{self, Intent},
    state::{self, SessionRecord, State},
};

// ---- journal / state inspection ---------------------------------------------

/// `state.json` strictly read-only: never quarantines, never mutates — the
/// cross-process observer the spec's `read_state` helper prescribes.
pub(crate) fn read_state(home: &MirrorHome) -> State {
    let path = state::state_path(&home.sync_dir());
    let Ok(raw) = std::fs::read(&path) else {
        return State::empty_v3();
    };
    let Ok(value) = serde_json::from_slice::<Value>(&raw) else {
        return State::empty_v3();
    };
    let mut st = State::empty_v3();
    if let Some(table) = value.get("sessions").and_then(Value::as_object) {
        for (sid, rv) in table {
            if let Ok(rec) = serde_json::from_value::<SessionRecord>(rv.clone()) {
                st.sessions.insert(sid.clone(), rec);
            }
        }
    }
    st
}

/// Append one journal intent — the same five-step write every short-lived
/// process performs; e2e never touches `.sync` ordering files otherwise.
pub(crate) fn write_intent(home: &MirrorHome, intent: &Intent) {
    journal::append(&home.sync_dir().join("journal"), intent)
        .unwrap_or_else(|e| fail(&format!("journal append {intent:?}: {e:#}")));
}

pub(crate) fn write_pull_intent(home: &MirrorHome, sid: &str) {
    write_intent(
        home,
        &Intent::Pull {
            sid: sid.to_string(),
            target: "latest".to_string(),
            enqueued_at: state::now_rfc3339(),
        },
    );
}

pub(crate) fn write_reconcile_intent(home: &MirrorHome) {
    write_intent(
        home,
        &Intent::Reconcile {
            enqueued_at: state::now_rfc3339(),
        },
    );
}

pub(crate) fn journal_dir(home: &MirrorHome) -> PathBuf {
    home.sync_dir().join("journal")
}

pub(crate) fn journal_empty(home: &MirrorHome) -> bool {
    journal::pending_count(&journal_dir(home)) == 0
}

pub(crate) fn journal_intents(home: &MirrorHome) -> Vec<Intent> {
    journal::read_pending(&journal_dir(home))
        .map(|(entries, _)| entries.into_iter().filter_map(|e| e.record.ok()).collect())
        .unwrap_or_default()
}
