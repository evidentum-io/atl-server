//! Unit tests for tree closer logic with TreeRotator trait

use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::{ServerError, StorageError};
use crate::storage::index::lifecycle::{ClosedTreeMetadata, TreeRotationResult};
use crate::traits::{TreeHead, TreeRotator};

/// Mock implementation of TreeRotator for testing
struct MockTreeRotator {
    /// Track if rotate_tree was called
    rotate_called: Arc<Mutex<bool>>,
    /// Optional error to return
    should_fail: bool,
}

impl MockTreeRotator {
    fn new() -> Self {
        Self {
            rotate_called: Arc::new(Mutex::new(false)),
            should_fail: false,
        }
    }

    fn with_failure() -> Self {
        Self {
            rotate_called: Arc::new(Mutex::new(false)),
            should_fail: true,
        }
    }

    async fn was_called(&self) -> bool {
        *self.rotate_called.lock().await
    }
}

#[async_trait::async_trait]
impl TreeRotator for MockTreeRotator {
    async fn rotate_tree(
        &self,
        _origin_id: &[u8; 32],
        end_size: u64,
        root_hash: &[u8; 32],
    ) -> Result<TreeRotationResult, StorageError> {
        *self.rotate_called.lock().await = true;

        if self.should_fail {
            return Err(StorageError::Database("mock rotation failure".into()));
        }

        // Return mock successful rotation result
        Ok(TreeRotationResult {
            closed_tree_id: 1,
            new_tree_id: 2,
            genesis_leaf_index: end_size,
            closed_tree_metadata: ClosedTreeMetadata {
                tree_id: 1,
                origin_id: [0u8; 32],
                root_hash: *root_hash,
                tree_size: end_size,
                prev_tree_id: None,
                closed_at: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            },
            new_tree_head: TreeHead {
                tree_size: end_size + 1,
                root_hash: [1u8; 32],
                origin: [0u8; 32],
            },
        })
    }
}

#[tokio::test]
async fn test_tree_rotator_trait_successful_rotation() {
    let rotator = MockTreeRotator::new();
    let origin_id = [0u8; 32];
    let end_size = 100;
    let root_hash = [42u8; 32];

    // Call rotate_tree
    let result = rotator.rotate_tree(&origin_id, end_size, &root_hash).await;

    // Verify success
    assert!(result.is_ok());
    assert!(rotator.was_called().await);

    let result = result.unwrap();
    assert_eq!(result.closed_tree_id, 1);
    assert_eq!(result.new_tree_id, 2);
    assert_eq!(result.genesis_leaf_index, end_size);
    assert_eq!(result.new_tree_head.tree_size, end_size + 1);
}

#[tokio::test]
async fn test_tree_rotator_trait_handles_failure() {
    let rotator = MockTreeRotator::with_failure();
    let origin_id = [0u8; 32];
    let end_size = 100;
    let root_hash = [42u8; 32];

    // Call rotate_tree
    let result = rotator.rotate_tree(&origin_id, end_size, &root_hash).await;

    // Verify failure
    assert!(result.is_err());
    assert!(rotator.was_called().await);

    match result.unwrap_err() {
        StorageError::Database(msg) => {
            assert_eq!(msg, "mock rotation failure");
        }
        _ => panic!("Expected Database error"),
    }
}

#[tokio::test]
async fn test_rotator_converts_storage_error_to_server_error() {
    let rotator = MockTreeRotator::with_failure();
    let origin_id = [0u8; 32];
    let end_size = 100;
    let root_hash = [42u8; 32];

    // Call rotate_tree and convert to ServerError
    let result = rotator
        .rotate_tree(&origin_id, end_size, &root_hash)
        .await
        .map_err(ServerError::Storage);

    // Verify conversion
    assert!(result.is_err());
    match result.unwrap_err() {
        ServerError::Storage(StorageError::Database(msg)) => {
            assert_eq!(msg, "mock rotation failure");
        }
        _ => panic!("Expected ServerError::Storage(Database)"),
    }
}
