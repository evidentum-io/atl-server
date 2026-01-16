// File: src/storage/chain_index/mod.rs

//! Chain Index - Separate SQLite database for tree metadata
//!
//! The Chain Index provides:
//! - Persistent metadata for all trees (active, closed, archived)
//! - Chain verification without loading full trees
//! - Archive location tracking
//!
//! ## Architecture
//!
//! Chain Index is a SEPARATE SQLite database from the main `atl.db`:
//! - File: `chain_index.db` (in same directory as `atl.db`)
//! - Always small (~200 bytes per tree)
//! - Lives forever, never cleaned
//! - Can be rebuilt from main DB if corrupted
//!
//! ## Usage
//!
//! ```rust,ignore
//! use atl_server::storage::chain_index::ChainIndex;
//!
//! let chain_index = ChainIndex::open(Path::new("data/chain_index.db"))?;
//!
//! // Record a closed tree (metadata includes data_tree_index from Super-Tree)
//! chain_index.record_closed_tree(&metadata)?;
//!
//! // Verify the full chain
//! let result = chain_index.verify_full_chain()?;
//! assert!(result.valid);
//! ```

mod archive;
mod schema;
mod store;
mod verification;

pub use archive::{ArchiveLocation, ArchivedTree};
pub use schema::{CHAIN_INDEX_SCHEMA, CHAIN_INDEX_SCHEMA_VERSION};
pub use store::{ChainIndex, ChainTreeRecord, ChainTreeStatus};
pub use verification::ChainVerificationResult;

// Re-export types from lifecycle for caller convenience
pub use crate::storage::index::lifecycle::{ClosedTreeMetadata, TreeRotationResult};
