//! Async storage trait definition

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::StorageError;

/// A log entry (stored in the database)
///
/// Contains only hashes - NO payload data.
#[derive(Debug, Clone)]
pub struct Entry {
    /// Unique identifier (UUID v4)
    pub id: Uuid,

    /// SHA-256 hash of the payload (computed by server from streaming input)
    pub payload_hash: [u8; 32],

    /// SHA-256 hash of the metadata JSON
    pub metadata_hash: [u8; 32],

    /// Cleartext metadata (optional, for receipt generation)
    /// This is the ONLY non-hash data stored
    pub metadata_cleartext: Option<serde_json::Value>,

    /// Position in the Merkle tree (None if not yet appended)
    pub leaf_index: Option<u64>,

    /// When the entry was created
    pub created_at: DateTime<Utc>,

    /// Client-provided external ID for correlation (optional)
    pub external_id: Option<String>,
}

/// Parameters for appending an entry to the log
#[derive(Debug, Clone)]
pub struct AppendParams {
    /// SHA-256 hash of the payload (computed by server from streaming input)
    pub payload_hash: [u8; 32],

    /// SHA-256 hash of the metadata JSON (computed by server)
    pub metadata_hash: [u8; 32],

    /// Cleartext metadata (stored for receipt generation)
    pub metadata_cleartext: Option<serde_json::Value>,

    /// Client-provided external ID for correlation (optional)
    pub external_id: Option<String>,
}

/// Result for a single entry in a batch
#[derive(Debug, Clone)]
pub struct EntryResult {
    /// Generated entry ID
    pub id: Uuid,

    /// Leaf index in tree
    pub leaf_index: u64,

    /// Precomputed leaf hash
    pub leaf_hash: [u8; 32],
}

/// Result of a batch append operation
#[derive(Debug, Clone)]
pub struct BatchResult {
    /// Results for each entry in submission order
    pub entries: Vec<EntryResult>,

    /// Tree head after batch commit
    pub tree_head: TreeHead,

    /// Batch commit timestamp
    pub committed_at: DateTime<Utc>,
}

/// Current state of the Merkle tree
#[derive(Debug, Clone)]
pub struct TreeHead {
    /// Number of leaves in the tree
    pub tree_size: u64,

    /// Root hash of the tree
    pub root_hash: [u8; 32],

    /// Origin ID (hash of the log's public key)
    pub origin: [u8; 32],
}

/// Inclusion proof for an entry
#[derive(Debug, Clone)]
pub struct InclusionProof {
    /// Leaf index being proved
    pub leaf_index: u64,

    /// Tree size the proof is valid for
    pub tree_size: u64,

    /// Merkle audit path
    pub path: Vec<[u8; 32]>,
}

/// Consistency proof between two tree sizes
#[derive(Debug, Clone)]
pub struct ConsistencyProof {
    /// Older tree size
    pub from_size: u64,

    /// Newer tree size
    pub to_size: u64,

    /// Merkle consistency path
    pub path: Vec<[u8; 32]>,
}

/// Result of a single append operation (used by Sequencer)
///
/// This is returned to individual API handlers after batch commit.
#[derive(Debug, Clone)]
pub struct AppendResult {
    /// Generated entry ID
    pub id: Uuid,

    /// Position in the Merkle tree
    pub leaf_index: u64,

    /// Tree head after append
    pub tree_head: TreeHead,

    /// Inclusion proof (empty - generated lazily on GET)
    pub inclusion_proof: Vec<[u8; 32]>,

    /// Timestamp when entry was committed
    pub timestamp: DateTime<Utc>,
}

/// Async storage backend for high-throughput operations
///
/// This is the ONLY storage trait - no sync version exists.
/// All write operations are batched for optimal throughput.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// Append a batch of entries atomically
    ///
    /// All entries are written together. Either all succeed or all fail.
    /// This is the ONLY write method - no single-entry append.
    ///
    /// # Arguments
    /// * `params` - Batch of entries to append (typically 1-10,000)
    ///
    /// # Returns
    /// * `BatchResult` with entry IDs and new tree head
    ///
    /// # Errors
    /// * `StorageError::Full` - WAL or buffer capacity exceeded
    /// * `StorageError::Io` - Disk write failure
    async fn append_batch(&self, params: Vec<AppendParams>) -> Result<BatchResult, StorageError>;

    /// Flush pending writes to durable storage
    ///
    /// After this returns, all previously appended entries are guaranteed
    /// to survive a crash. Called automatically by append_batch, but can
    /// be called explicitly for checkpoint operations.
    async fn flush(&self) -> Result<(), StorageError>;

    /// Get current tree head (size + root hash)
    ///
    /// This is a fast operation - reads from memory cache.
    fn tree_head(&self) -> TreeHead;

    /// Get origin ID (hash of log's public key)
    fn origin_id(&self) -> [u8; 32];

    /// Check if storage is healthy
    fn is_healthy(&self) -> bool;

    /// Get entry by ID
    fn get_entry(&self, id: &Uuid) -> crate::error::ServerResult<Entry>;

    /// Get inclusion proof for entry
    fn get_inclusion_proof(
        &self,
        entry_id: &Uuid,
        tree_size: Option<u64>,
    ) -> crate::error::ServerResult<InclusionProof>;

    /// Get consistency proof between tree sizes
    fn get_consistency_proof(
        &self,
        from_size: u64,
        to_size: u64,
    ) -> crate::error::ServerResult<ConsistencyProof>;

    /// Get anchors for a specific tree size
    fn get_anchors(&self, tree_size: u64) -> crate::error::ServerResult<Vec<crate::traits::anchor::Anchor>>;

    /// Get the most recent anchored tree size
    fn get_latest_anchored_size(&self) -> crate::error::ServerResult<Option<u64>>;

    /// Get anchors covering a target tree size
    fn get_anchors_covering(&self, target_tree_size: u64, limit: usize) -> crate::error::ServerResult<Vec<crate::traits::anchor::Anchor>>;

    /// Get root hash at specific tree size
    fn get_root_at_size(&self, tree_size: u64) -> crate::error::ServerResult<[u8; 32]>;

    /// Check if storage is initialized
    fn is_initialized(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compile-time test: trait is object-safe
    fn _assert_object_safe(_: &dyn Storage) {}

    #[test]
    fn test_batch_result_creation() {
        let result = BatchResult {
            entries: vec![],
            tree_head: TreeHead {
                tree_size: 0,
                root_hash: [0u8; 32],
                origin: [0u8; 32],
            },
            committed_at: chrono::Utc::now(),
        };
        assert_eq!(result.entries.len(), 0);
    }

    #[test]
    fn test_entry_result_creation() {
        let entry_result = EntryResult {
            id: Uuid::new_v4(),
            leaf_index: 42,
            leaf_hash: [1u8; 32],
        };
        assert_eq!(entry_result.leaf_index, 42);
        assert_eq!(entry_result.leaf_hash[0], 1);
    }
}
