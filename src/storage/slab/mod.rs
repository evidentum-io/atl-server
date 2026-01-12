//! Slab-based Merkle tree storage
//!
//! Memory-mapped slab files for efficient Merkle tree node storage.
//! Each slab stores a complete binary tree with fixed capacity.

pub mod format;
pub mod manager;
pub mod mmap;

// Re-export main types
pub use format::{SlabHeader, DEFAULT_SLAB_CAPACITY, NODE_SIZE, SLAB_MAGIC, SLAB_VERSION};
pub use manager::{SlabConfig, SlabManager};
pub use mmap::SlabFile;
