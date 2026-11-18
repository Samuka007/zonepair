//! Store RPC client: calls against the server's `/v1/store/*` endpoints.

pub mod store;
pub mod sync;

pub use store::*;
pub use sync::*;
