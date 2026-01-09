//! atl-server library exports (for testing)

pub mod config;
pub mod error;
pub mod traits;
pub mod api;

#[cfg(feature = "sqlite")]
pub mod storage;

#[cfg(any(feature = "rfc3161", feature = "ots"))]
pub mod anchoring;

pub mod receipt;

// Re-exports
pub use traits::{Storage, Anchorer, Entry, Anchor};
pub use error::{ServerError, ServerResult};

#[cfg(feature = "sqlite")]
pub use storage::SqliteStore;
