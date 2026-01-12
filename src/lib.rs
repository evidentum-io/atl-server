//! atl-server library exports (for testing)

pub mod api;
pub mod config;
pub mod error;
pub mod traits;

pub mod storage;

#[cfg(any(feature = "rfc3161", feature = "ots"))]
pub mod anchoring;

pub mod receipt;

pub mod background;

pub mod sequencer;

#[cfg(feature = "grpc")]
pub mod grpc;

// Re-export key types at crate root
pub use error::{ServerError, ServerResult};

pub use storage::{StorageEngine, TreeRecord, TreeStatus, AnchorWithId};
