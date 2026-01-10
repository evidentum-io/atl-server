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

// Re-export key types at crate root
pub use error::{ServerError, ServerResult};

#[cfg(feature = "sqlite")]
pub use storage::SqliteStore;
