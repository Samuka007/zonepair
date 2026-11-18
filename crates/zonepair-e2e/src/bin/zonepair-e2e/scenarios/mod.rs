//! Scenario groups. File names carry the semantic group; function names keep
//! the internal T-numbers verbatim — they are the anchors into
//! `tmp/impl/test-spec.md` (T-7xx) and `tmp/impl/test-spec-p2.md` (T-8xx).
//!
//! | File | Group | ID range |
//! |---|---|---|
//! | `baseline.rs` | pre-mirror WS/CLI baseline contracts | — |
//! | `mirror_sync.rs` | zonepaird sync: publish/crash/recovery | T-701…T-716 |
//! | `materialize.rs` | evict/hydrate/stub execution surface | T-810…T-811 |

pub(crate) mod baseline;
pub(crate) mod materialize;
pub(crate) mod mirror_sync;
