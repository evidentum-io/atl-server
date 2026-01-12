// File: src/storage/high_throughput/index/mod.rs

//! SQLite index store for HTS (High Throughput Storage)
//!
//! This module provides a SQLite-based index that stores:
//! - Entry metadata and pointers to Slab files
//! - Checkpoints (signed tree heads)
//! - Anchors (TSA/Bitcoin attestations)
//! - Configuration
//!
//! Tree nodes are NOT stored in SQLite - they live in Slab files.

pub mod queries;
pub mod schema;

pub use queries::{BatchInsert, IndexEntry, IndexStore};
pub use schema::{MIGRATE_V2_TO_V3, SCHEMA_V3, SCHEMA_VERSION};
