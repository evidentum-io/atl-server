// File: src/storage/mod.rs

#![allow(unused_imports)]

// Backend implementations
#[cfg(feature = "sqlite")]
pub mod sqlite;

// High-Throughput Storage components
pub mod index;
pub mod slab;
pub mod wal;

// Re-export Storage trait from traits module
pub use crate::traits::Storage;

// Re-export default backend (sqlite) for convenience
#[cfg(feature = "sqlite")]
pub use sqlite::{AnchorWithId, SqliteConfig, SqliteStore, StorageStats, TreeRecord, TreeStatus};
