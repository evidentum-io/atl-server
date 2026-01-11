//! Storage trait definition

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::ServerResult;
use crate::traits::anchor::Anchor;

/// A log entry (stored in the database)
///
/// Contains only hashes - NO payload data.
#[allow(dead_code)]
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
#[allow(dead_code)]
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

/// Result of an append operation
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AppendResult {
    /// The generated entry ID
    pub id: Uuid,

    /// Leaf index in the Merkle tree
    pub leaf_index: u64,

    /// Current tree state after append
    pub tree_head: TreeHead,

    /// Inclusion proof for this entry
    pub inclusion_proof: Vec<[u8; 32]>,

    /// Timestamp of append
    pub timestamp: DateTime<Utc>,

    /// Optional TSA anchor (when batch includes RFC 3161 timestamp)
    #[cfg(feature = "rfc3161")]
    pub tsa_anchor: Option<crate::anchoring::rfc3161::TsaAnchor>,
}

/// Current state of the Merkle tree
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ConsistencyProof {
    /// Older tree size
    pub from_size: u64,

    /// Newer tree size
    pub to_size: u64,

    /// Merkle consistency path
    pub path: Vec<[u8; 32]>,
}

/// Append-only transparency log storage
///
/// Stores hash records in a Merkle tree structure.
/// NEVER stores payload content - only hashes.
#[allow(dead_code)]
pub trait Storage: Send + Sync {
    // ========== Initialization ==========

    /// Initialize storage (create tables, etc.)
    fn initialize(&self) -> ServerResult<()>;

    /// Check if storage is initialized
    fn is_initialized(&self) -> bool;

    /// Get the origin ID (hash of public key)
    fn origin_id(&self) -> [u8; 32];

    // ========== Entry Operations ==========

    /// Append a new entry to the log
    ///
    /// This is the ONLY way to add entries. Entries are immutable once appended.
    ///
    /// # Returns
    /// - Entry ID, leaf index, tree state, and inclusion proof
    fn append(&self, params: AppendParams) -> ServerResult<AppendResult>;

    /// Atomically append a batch of entries to the log
    ///
    /// All entries are written in a single transaction. Either all succeed
    /// or all fail (atomic). Returns results in the same order as input.
    ///
    /// # Performance
    /// - 10,000 entries in one transaction is ~100x faster than individual appends
    /// - Inclusion proofs are computed within the transaction (hot data)
    /// - Tree nodes are batch-updated with minimal hash computations
    ///
    /// # Errors
    /// Returns error if the transaction fails. Callers should retry with
    /// exponential backoff on transient errors (DATABASE_BUSY).
    fn append_batch(&self, params: Vec<AppendParams>) -> ServerResult<Vec<AppendResult>>;

    /// Get an entry by its ID
    fn get_entry(&self, id: &Uuid) -> ServerResult<Entry>;

    /// Get an entry by leaf index
    fn get_entry_by_index(&self, index: u64) -> ServerResult<Entry>;

    /// Get an entry by external ID (if provided during append)
    fn get_entry_by_external_id(&self, external_id: &str) -> ServerResult<Entry>;

    // ========== Tree Operations ==========

    /// Get current tree head (size + root hash)
    fn get_tree_head(&self) -> ServerResult<TreeHead>;

    /// Get inclusion proof for an entry
    ///
    /// # Arguments
    /// - `entry_id`: Entry to prove
    /// - `tree_size`: Optional tree size (defaults to current)
    fn get_inclusion_proof(
        &self,
        entry_id: &Uuid,
        tree_size: Option<u64>,
    ) -> ServerResult<InclusionProof>;

    /// Get consistency proof between two tree sizes
    fn get_consistency_proof(&self, from_size: u64, to_size: u64)
        -> ServerResult<ConsistencyProof>;

    // ========== Checkpoint Operations ==========

    /// Store a signed checkpoint
    fn store_checkpoint(&self, checkpoint: &atl_core::Checkpoint) -> ServerResult<()>;

    /// Get the most recent checkpoint
    fn get_latest_checkpoint(&self) -> ServerResult<Option<atl_core::Checkpoint>>;

    /// Get checkpoint by tree size
    fn get_checkpoint_by_size(&self, tree_size: u64) -> ServerResult<Option<atl_core::Checkpoint>>;

    /// Get checkpoint closest to (but not exceeding) a tree size
    fn get_checkpoint_at_or_before(
        &self,
        tree_size: u64,
    ) -> ServerResult<Option<atl_core::Checkpoint>>;

    // ========== Anchor Operations ==========

    /// Store an external anchor for a tree size
    fn store_anchor(&self, tree_size: u64, anchor: &Anchor) -> ServerResult<()>;

    /// Get all anchors for a tree size
    fn get_anchors(&self, tree_size: u64) -> ServerResult<Vec<Anchor>>;

    /// Get the most recent anchored tree size
    fn get_latest_anchored_size(&self) -> ServerResult<Option<u64>>;
}
