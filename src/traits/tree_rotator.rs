//! Tree rotation abstraction for tree lifecycle management

use async_trait::async_trait;

use crate::error::StorageError;
use crate::storage::index::lifecycle::TreeRotationResult;

/// Trait for tree rotation operations
///
/// Implemented by `StorageEngine` to provide coordinated tree rotation
/// with genesis leaf insertion into both Slab and SQLite.
///
/// This trait exists to provide clean separation of concerns:
/// - `Storage` trait handles read/write operations on entries
/// - `TreeRotator` trait handles tree lifecycle operations
///
/// This allows background jobs to access rotation functionality without
/// exposing the full `StorageEngine` implementation details.
#[async_trait]
pub trait TreeRotator: Send + Sync {
    /// Rotate the tree: close active tree and create new one with genesis leaf
    ///
    /// This is the ONLY correct way to rotate trees in production. Do NOT call
    /// `IndexStore::close_tree_and_create_new()` directly - it does not insert
    /// genesis leaf into Slab.
    ///
    /// # Atomicity
    ///
    /// This operation atomically:
    /// 1. Closes the active tree (marks as pending_bitcoin)
    /// 2. Creates new active tree with chain link to previous tree
    /// 3. Computes genesis leaf hash linking to closed tree
    /// 4. Inserts genesis leaf into BOTH Slab and SQLite
    /// 5. Updates internal tree state cache
    ///
    /// # Arguments
    ///
    /// * `origin_id` - Origin ID of the log (hash of log's public key)
    /// * `end_size` - Final size of the tree being closed
    /// * `root_hash` - Root hash of the tree being closed
    ///
    /// # Returns
    ///
    /// `TreeRotationResult` containing:
    /// - Closed tree ID and metadata
    /// - New tree ID
    /// - Genesis leaf index (equals end_size)
    /// - New tree head with genesis leaf included
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if:
    /// - Database transaction fails
    /// - Slab append fails
    /// - Tree state is inconsistent
    async fn rotate_tree(
        &self,
        origin_id: &[u8; 32],
        end_size: u64,
        root_hash: &[u8; 32],
    ) -> Result<TreeRotationResult, StorageError>;
}
