//! Legacy types for SQLite storage (pre-HTS refactoring)
//!
//! These types are used by the old sync SQLite storage implementation.
//! They will be removed in Wave 4 (HTS-MIGRATE) after migration to the new async StorageEngine.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::traits::{InclusionProof, TreeHead};

/// Result of appending an entry (legacy sync API)
///
/// This is different from the new `BatchResult` type in the async Storage trait.
#[derive(Debug, Clone)]
pub struct AppendResult {
    /// Generated entry ID
    pub id: Uuid,

    /// Position in the Merkle tree
    pub leaf_index: u64,

    /// Tree head after append
    pub tree_head: TreeHead,

    /// Inclusion proof for the entry
    pub inclusion_proof: InclusionProof,

    /// Timestamp when entry was created
    pub timestamp: DateTime<Utc>,

    /// TSA anchor (optional, feature-gated)
    #[cfg(feature = "rfc3161")]
    pub tsa_anchor: Option<crate::traits::Anchor>,
}
