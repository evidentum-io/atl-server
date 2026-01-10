// File: src/storage/mod.rs

#![allow(unused_imports)]

// Backend implementations
#[cfg(feature = "sqlite")]
pub mod sqlite;

// Re-export Storage trait from traits module
pub use crate::traits::Storage;

// Re-export default backend (sqlite) for convenience
#[cfg(feature = "sqlite")]
pub use sqlite::{AnchorWithId, SqliteConfig, SqliteStore, StorageStats, TreeRecord, TreeStatus};
