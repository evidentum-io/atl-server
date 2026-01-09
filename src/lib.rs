//! atl-server library exports (for testing)

pub mod api;
pub mod config;
pub mod error;
pub mod traits;

#[cfg(feature = "sqlite")]
pub mod storage;

#[cfg(any(feature = "rfc3161", feature = "ots"))]
pub mod anchoring;

pub mod receipt;

// Re-exports will be added when modules are implemented
// pub use error::{ServerError, ServerResult};
// pub use traits::{Anchor, Anchorer, Entry, Storage};
// #[cfg(feature = "sqlite")]
// pub use storage::SqliteStore;
