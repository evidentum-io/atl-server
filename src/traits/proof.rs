//! Read-only proof generation interface

use async_trait::async_trait;
use uuid::Uuid;

use super::storage::{ConsistencyProof, Entry, InclusionProof};
use crate::error::StorageError;

/// Read-only proof generation interface
///
/// Separated from the write path (`Storage`) to allow independent
/// caching and optimization strategies. Proof generation is read-heavy
/// and can be served from replicas or caches.
#[async_trait]
pub trait ProofProvider: Send + Sync {
    /// Get entry by ID
    ///
    /// # Arguments
    /// * `id` - Entry UUID
    ///
    /// # Returns
    /// * `Entry` with all metadata
    ///
    /// # Errors
    /// * `StorageError::NotFound` - Entry does not exist
    async fn get_entry(&self, id: &Uuid) -> Result<Entry, StorageError>;

    /// Get entry by leaf index
    ///
    /// # Arguments
    /// * `index` - Zero-based leaf index in the tree
    ///
    /// # Returns
    /// * `Entry` at the given index
    ///
    /// # Errors
    /// * `StorageError::NotFound` - Index out of bounds
    async fn get_entry_by_index(&self, index: u64) -> Result<Entry, StorageError>;

    /// Get entry by external ID
    ///
    /// External IDs are optional client-provided identifiers.
    ///
    /// # Arguments
    /// * `external_id` - Client-provided identifier
    ///
    /// # Returns
    /// * `Entry` with matching external_id
    ///
    /// # Errors
    /// * `StorageError::NotFound` - No entry with this external_id
    async fn get_entry_by_external_id(&self, external_id: &str) -> Result<Entry, StorageError>;

    /// Generate inclusion proof for an entry
    ///
    /// Proves that an entry is included in the tree at a specific size.
    ///
    /// # Arguments
    /// * `leaf_index` - Index of the entry to prove
    /// * `tree_size` - Tree size for proof (None = current tree size)
    ///
    /// # Returns
    /// * `InclusionProof` with Merkle audit path
    ///
    /// # Errors
    /// * `StorageError::NotFound` - Leaf index out of bounds
    /// * `StorageError::InvalidTreeSize` - tree_size < leaf_index + 1
    async fn get_inclusion_proof(
        &self,
        leaf_index: u64,
        tree_size: Option<u64>,
    ) -> Result<InclusionProof, StorageError>;

    /// Generate consistency proof between two tree sizes
    ///
    /// Proves that a tree of size `from_size` is a prefix of a tree
    /// of size `to_size`. Essential for log verification.
    ///
    /// # Arguments
    /// * `from_size` - Older tree size
    /// * `to_size` - Newer tree size
    ///
    /// # Returns
    /// * `ConsistencyProof` with Merkle path
    ///
    /// # Errors
    /// * `StorageError::InvalidTreeSize` - from_size > to_size
    /// * `StorageError::NotFound` - tree_size out of bounds
    async fn get_consistency_proof(
        &self,
        from_size: u64,
        to_size: u64,
    ) -> Result<ConsistencyProof, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compile-time test: trait is object-safe
    fn _assert_object_safe(_: &dyn ProofProvider) {}
}
