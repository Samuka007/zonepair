//! `#[cfg(test)]` helpers shared by the cli test modules — the
//! intent-channel seam (journal read-back + owner-lock hold) that the
//! prune/hydrate adapter tests exercise. Plain-fs helpers (`tmp`,
//! `write_file`) already have a home in `sync::fs::testkit`; this module
//! carries only the pieces that speak journal/owner, not plain fs.

use crate::daemon::owner;
use crate::sync::persist::journal::{self, Intent};
use crate::sync::state;
use std::path::Path;

/// Hold the owner lock so `enqueue`'s `ensure_owner` sees a live writer
/// and never spawns a daemon from the test.
pub(crate) fn hold_owner(mirror: &Path) -> owner::Owner {
    let sync = state::ensure_sync_dir(mirror).unwrap();
    owner::acquire(&sync)
        .unwrap()
        .expect("test must own the lock")
}

/// Every formally-recorded intent in `<mirror>/.sync/journal`, parsed —
/// `read_pending` already excludes `*.tmp` residue and debris.
pub(crate) fn journal_intents(mirror: &Path) -> Vec<Intent> {
    let dir = state::sync_dir(mirror).join("journal");
    journal::read_pending(&dir)
        .map(|(entries, _)| {
            entries
                .into_iter()
                .filter_map(|e| e.record.ok())
                .collect()
        })
        .unwrap_or_default()
}
