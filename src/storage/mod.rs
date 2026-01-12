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

// Re-export Storage trait from traits module
pub use crate::traits::Storage;

// Re-export main storage types
pub use config::StorageConfig;
pub use engine::StorageEngine;

// Re-export index types (lifecycle, anchors)
pub use index::{AnchorWithId, TreeRecord, TreeStatus};
