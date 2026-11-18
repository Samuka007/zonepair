//! stub-client's library half (machine A): transport, store API, mirror
//! sync, client config and the CLI the thin `stub-client` bin forwards to.

pub use zonepair_proto as proto;

pub mod api;
pub mod cfg;
pub mod cli;
pub mod daemon;
pub mod sync;
pub mod transport;
