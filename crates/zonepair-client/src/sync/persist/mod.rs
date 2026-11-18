//! Persistence layer — "what lands on disk": the `.sync/` IO discipline
//! (`fs`), the clock (`time`), the authoritative `state.json` schema and
//! commit/quarantine (`state`), and the multi-writer intent channel
//! (`journal`). Nothing here knows about executors, probes or waits —
//! the layer imports no module above `sync::`.

pub(crate) mod fs;
pub mod journal;
pub mod state;
pub(crate) mod time;
