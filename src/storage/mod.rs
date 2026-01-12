//! Storage module
//!
//! Provides the unified storage engine coordinating WAL, Slab, and SQLite components.

#![allow(unused_imports)]

// High-Throughput Storage components
pub mod config;
pub mod engine;
pub mod index;
pub mod recovery;
pub mod slab;
pub mod wal;

// Old backend (will be deleted in Wave 4)
#[cfg(feature = "sqlite")]
pub mod sqlite;

// Re-export Storage trait from traits module
pub use crate::traits::Storage;

// Re-export main storage types
pub use config::StorageConfig;
pub use engine::StorageEngine;

// Re-export old backend for backward compatibility (will be removed in Wave 4)
#[cfg(feature = "sqlite")]
pub use sqlite::{AnchorWithId, SqliteConfig, SqliteStore, StorageStats, TreeRecord, TreeStatus};
