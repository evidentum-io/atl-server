//! Checkpoint storage interface

use async_trait::async_trait;
use atl_core::Checkpoint;

use crate::error::StorageError;

/// Checkpoint storage interface
///
/// Handles signed tree head checkpoints. Separated from the main
/// write path as checkpoint operations are low-frequency and
/// typically involve cryptographic signing.
///
/// NOTE: Currently unused - HTS generates checkpoints on-the-fly.
/// Will be needed for historical checkpoint queries per RFC 6962.
#[allow(dead_code)]
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Store a signed checkpoint
    ///
    /// Checkpoints are signed tree heads that serve as verifiable
    /// commitments to the log state.
    ///
    /// # Arguments
    /// * `checkpoint` - Signed checkpoint to store
    ///
    /// # Errors
    /// * `StorageError::Io` - Database write failure
    async fn store_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), StorageError>;

    /// Get checkpoint by tree size
    ///
    /// Retrieves the checkpoint for an exact tree size.
    ///
    /// # Arguments
    /// * `tree_size` - Exact tree size to look up
    ///
    /// # Returns
    /// * `Some(Checkpoint)` if exists, `None` otherwise
    ///
    /// # Errors
    /// * `StorageError::Io` - Database read failure
    async fn get_checkpoint(&self, tree_size: u64) -> Result<Option<Checkpoint>, StorageError>;

    /// Get latest checkpoint
    ///
    /// Returns the most recent checkpoint stored in the log.
    ///
    /// # Returns
    /// * `Some(Checkpoint)` if any exists, `None` for empty log
    ///
    /// # Errors
    /// * `StorageError::Io` - Database read failure
    async fn get_latest_checkpoint(&self) -> Result<Option<Checkpoint>, StorageError>;

    /// Get checkpoint at or before given tree size
    ///
    /// Finds the most recent checkpoint where checkpoint.tree_size <= target.
    /// Useful for finding the closest historical checkpoint.
    ///
    /// # Arguments
    /// * `tree_size` - Maximum tree size to search
    ///
    /// # Returns
    /// * `Some(Checkpoint)` if found, `None` if no checkpoint <= tree_size
    ///
    /// # Errors
    /// * `StorageError::Io` - Database read failure
    async fn get_checkpoint_at_or_before(
        &self,
        tree_size: u64,
    ) -> Result<Option<Checkpoint>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compile-time test: trait is object-safe
    fn _assert_object_safe(_: &dyn CheckpointStore) {}
}
