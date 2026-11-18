//! The zonepaird-era sync engine (RFC-0002-a-state/0003).
//!
//! The old incremental mirror push engine is retired: A no longer
//! parses or merges session
//! content — it pulls B's byte prefix through `changes`, publishes A-side
//! fork files once through `publish`, and coordinates everything through
//! journal intents plus one writer. `.sync-state.json` is ignored (not
//! migrated); all sessions cold-start at the zero cursor and converge by
//! reconcile + first pull.
//!
//! Module map: `persist` = the disk layer (`state`/`journal`/`fs`/`time`)
//! — "what lands on disk"; `state_machine` = the pure tombstone/decision
//! table (formerly `classify`); `wait` = the `ensure_published`
//! predicate; `engine` = "verdicts into disk facts" — the executors'
//! `shared` context, the `pool` scheduler, and the
//! `pull`/`publish`/`materialize`/`reconcile`/`apply` executors.

pub mod engine;
pub mod persist;
pub mod state_machine;
pub mod wait;

// Transitional re-exports: `journal`/`state` moved under `persist/`,
// `fs`/`time` likewise (crate-internal); `classify` was renamed
// `state_machine` (the RFC's name for the layer); `apply`/`materialize`/
// `reconcile`/`shared` moved under `engine/`; the old `worker.rs` split
// into `engine::pool` + the executor files. Established paths like
// `crate::sync::state::X`, `crate::sync::worker::Shared` and
// `zonepair_client::sync::reconcile::session_file` keep resolving.
pub use engine::{apply, materialize, reconcile};
pub use engine as worker;
pub use persist::{journal, state};
pub use state_machine as classify;
