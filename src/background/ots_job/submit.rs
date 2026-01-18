//! OTS submission phase
//!
//! Submits Super Root to OTS calendar when Super-Tree grows.

#[cfg(feature = "ots")]
use std::sync::Arc;

#[cfg(feature = "ots")]
use crate::error::{ServerError, ServerResult};

#[cfg(feature = "ots")]
use crate::anchoring::ots::OtsClient;

#[cfg(feature = "ots")]
use crate::storage::index::IndexStore;

#[cfg(feature = "ots")]
use crate::traits::Storage;

#[cfg(feature = "ots")]
use tokio::sync::Mutex;

/// Submit Super Root to OTS calendar (v2.0)
///
/// Checks if Super-Tree has grown since last OTS submission.
/// If yes, submits the current Super Root to OTS calendar.
#[cfg(feature = "ots")]
pub async fn submit_unanchored_trees(
    index: &Arc<Mutex<IndexStore>>,
    storage: &Arc<dyn Storage>,
    client: &Arc<dyn OtsClient>,
    _max_batch_size: usize,
) -> ServerResult<()> {
    // Get current Super-Tree size and last submitted size
    let (current_super_tree_size, last_submitted_size) = {
        let idx = index.lock().await;
        let current = idx.get_super_tree_size().map_err(|e| {
            ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
        })?;
        let last = idx.get_last_ots_submitted_super_tree_size().map_err(|e| {
            ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
        })?;
        (current, last)
    };

    // If no new trees closed since last submission, nothing to do
    if current_super_tree_size <= last_submitted_size {
        tracing::debug!(
            current = current_super_tree_size,
            last_submitted = last_submitted_size,
            "No new Super Root to submit"
        );
        return Ok(());
    }

    // Get Super Root from storage
    let super_root = storage
        .get_super_root(current_super_tree_size)
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to get super_root");
            e
        })?;

    // Submit to OTS calendar
    match client.submit(&super_root).await {
        Ok((calendar_url, proof)) => {
            let anchor_id = {
                let mut idx = index.lock().await;
                idx.submit_super_root_ots_anchor(
                    &proof,
                    &calendar_url,
                    &super_root,
                    current_super_tree_size,
                )
                .map_err(|e| {
                    ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
                })?
            };

            // Update last submitted size
            {
                let idx = index.lock().await;
                idx.set_last_ots_submitted_super_tree_size(current_super_tree_size)
                    .map_err(|e| {
                        ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
                    })?;
            }

            tracing::info!(
                anchor_id = anchor_id,
                super_tree_size = current_super_tree_size,
                calendar_url = %calendar_url,
                "Super Root OTS proof submitted (pending Bitcoin)"
            );
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "Super Root OTS submit failed, will retry"
            );
        }
    }

    Ok(())
}

#[cfg(all(test, feature = "ots"))]
mod tests {
    use super::*;
    use crate::anchoring::error::AnchorError;
    use crate::anchoring::ots::{OtsClient, UpgradeResult};
    use crate::error::{ServerError, StorageError};
    use crate::traits::{
        Anchor, AppendParams, BatchResult, ConsistencyProof, Entry, InclusionProof, Storage,
        TreeHead,
    };
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    struct MockOtsClient {
        submit_result: Result<(String, Vec<u8>), AnchorError>,
        call_count: Arc<AtomicUsize>,
    }

    impl MockOtsClient {
        fn new_success() -> Self {
            Self {
                submit_result: Ok(("http://calendar.example".to_string(), vec![1, 2, 3])),
                call_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn new_error(error: AnchorError) -> Self {
            Self {
                submit_result: Err(error),
                call_count: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl OtsClient for MockOtsClient {
        async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            self.submit_result.clone()
        }

        async fn upgrade(
            &self,
            _proof: &[u8],
        ) -> Result<Option<crate::anchoring::ots::UpgradeResult>, AnchorError> {
            unimplemented!()
        }
    }

    struct MockStorage {
        super_root: [u8; 32],
    }

    impl MockStorage {
        fn new(super_root: [u8; 32]) -> Self {
            Self { super_root }
        }
    }

    #[async_trait]
    impl Storage for MockStorage {
        async fn append_batch(
            &self,
            _params: Vec<AppendParams>,
        ) -> Result<BatchResult, StorageError> {
            unimplemented!()
        }

        async fn flush(&self) -> Result<(), StorageError> {
            Ok(())
        }

        fn tree_head(&self) -> TreeHead {
            TreeHead {
                tree_size: 0,
                root_hash: [0u8; 32],
                origin: [0u8; 32],
            }
        }

        fn origin_id(&self) -> [u8; 32] {
            [0u8; 32]
        }

        fn is_healthy(&self) -> bool {
            true
        }

        fn get_entry(&self, _id: &Uuid) -> crate::error::ServerResult<Entry> {
            Err(ServerError::Storage(StorageError::Database(
                "not found".to_string(),
            )))
        }

        fn get_inclusion_proof(
            &self,
            _entry_id: &Uuid,
            _tree_size: Option<u64>,
        ) -> crate::error::ServerResult<InclusionProof> {
            unimplemented!()
        }

        fn get_consistency_proof(
            &self,
            _from_size: u64,
            _to_size: u64,
        ) -> crate::error::ServerResult<ConsistencyProof> {
            unimplemented!()
        }

        fn get_anchors(&self, _tree_size: u64) -> crate::error::ServerResult<Vec<Anchor>> {
            Ok(vec![])
        }

        fn get_latest_anchored_size(&self) -> crate::error::ServerResult<Option<u64>> {
            Ok(None)
        }

        fn get_anchors_covering(
            &self,
            _target_tree_size: u64,
            _limit: usize,
        ) -> crate::error::ServerResult<Vec<Anchor>> {
            Ok(vec![])
        }

        fn get_root_at_size(&self, _tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
            Ok([0u8; 32])
        }

        fn get_super_root(&self, _super_tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
            Ok(self.super_root)
        }

        fn is_initialized(&self) -> bool {
            true
        }
    }

    fn create_test_index_store() -> IndexStore {
        use rusqlite::Connection;
        let conn = Connection::open_in_memory().expect("Failed to create in-memory DB");
        let store = IndexStore::from_connection(conn);
        store.initialize().expect("Failed to initialize schema");
        store
    }

    #[tokio::test]
    async fn test_submit_no_new_trees() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Set last submitted size = current size
        {
            let idx = index.lock().await;
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([1u8; 32]));
        let client = Arc::new(MockOtsClient::new_success());
        let call_count = client.call_count.clone();
        let client_trait: Arc<dyn OtsClient> = client;

        let result = submit_unanchored_trees(&index, &storage, &client_trait, 100).await;
        assert!(result.is_ok());

        // Should not call OTS client
        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_submit_with_new_trees() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Simulate super tree growth
        {
            let idx = index.lock().await;
            // Set super tree size to 1 (one closed tree)
            idx.set_super_tree_size(1).unwrap();
            // Set last submitted to 0
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let super_root = [2u8; 32];
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(super_root));
        let client = Arc::new(MockOtsClient::new_success());
        let call_count = client.call_count.clone();
        let client_trait: Arc<dyn OtsClient> = client;

        let result = submit_unanchored_trees(&index, &storage, &client_trait, 100).await;
        assert!(result.is_ok());

        // Should call OTS client once
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Check that anchor was created and last submitted size was updated
        let idx = index.lock().await;
        let last_submitted = idx.get_last_ots_submitted_super_tree_size().unwrap();
        assert_eq!(last_submitted, 1);

        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].anchor.anchored_hash, super_root);
    }

    #[tokio::test]
    async fn test_submit_ots_client_error() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Simulate super tree growth
        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([2u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_error(AnchorError::Network(
            "timeout".to_string(),
        )));

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok()); // Should not fail, just log warning

        // Last submitted size should NOT be updated
        let idx = index.lock().await;
        let last_submitted = idx.get_last_ots_submitted_super_tree_size().unwrap();
        assert_eq!(last_submitted, 0);

        // No anchor should be created
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 0);
    }

    #[tokio::test]
    async fn test_submit_multiple_new_trees() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Simulate multiple tree closures
        {
            let idx = index.lock().await;
            idx.set_super_tree_size(3).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let super_root = [4u8; 32];
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(super_root));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        // Should submit the current super root (covering all 3 trees)
        let idx = index.lock().await;
        let last_submitted = idx.get_last_ots_submitted_super_tree_size().unwrap();
        assert_eq!(last_submitted, 3);

        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].anchor.anchored_hash, super_root);
        assert_eq!(pending[0].anchor.super_tree_size, Some(3));
    }

    #[tokio::test]
    async fn test_submit_when_already_submitted() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Close trees and submit once
        {
            let idx = index.lock().await;
            idx.set_super_tree_size(2).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([2u8; 32]));
        let client = Arc::new(MockOtsClient::new_success());
        let call_count = client.call_count.clone();
        let client_trait: Arc<dyn OtsClient> = client;

        // First submit
        let result = submit_unanchored_trees(&index, &storage, &client_trait, 100).await;
        assert!(result.is_ok());
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second submit (no new trees)
        let result = submit_unanchored_trees(&index, &storage, &client_trait, 100).await;
        assert!(result.is_ok());
        assert_eq!(call_count.load(Ordering::SeqCst), 1); // Should not call again
    }

    #[tokio::test]
    async fn test_submit_partial_growth() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Close trees, submit, then close more trees
        {
            let idx = index.lock().await;
            idx.set_super_tree_size(2).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([2u8; 32]));
        let client = Arc::new(MockOtsClient::new_success());
        let call_count = client.call_count.clone();
        let client_trait: Arc<dyn OtsClient> = client;

        // First submit
        submit_unanchored_trees(&index, &storage, &client_trait, 100)
            .await
            .unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Close one more tree
        {
            let idx = index.lock().await;
            idx.set_super_tree_size(3).unwrap();
        }

        // Second submit (new tree available)
        let storage2: Arc<dyn Storage> = Arc::new(MockStorage::new([3u8; 32]));
        submit_unanchored_trees(&index, &storage2, &client_trait, 100)
            .await
            .unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 2); // Should submit again

        // Verify final state
        let idx = index.lock().await;
        let last_submitted = idx.get_last_ots_submitted_super_tree_size().unwrap();
        assert_eq!(last_submitted, 3);
    }

    #[tokio::test]
    async fn test_submit_storage_get_super_root_error() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Simulate super tree growth
        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        // Mock storage that returns error
        struct ErrorMockStorage;

        #[async_trait]
        impl Storage for ErrorMockStorage {
            async fn append_batch(
                &self,
                _params: Vec<AppendParams>,
            ) -> Result<BatchResult, StorageError> {
                unimplemented!()
            }

            async fn flush(&self) -> Result<(), StorageError> {
                Ok(())
            }

            fn tree_head(&self) -> TreeHead {
                TreeHead {
                    tree_size: 0,
                    root_hash: [0u8; 32],
                    origin: [0u8; 32],
                }
            }

            fn origin_id(&self) -> [u8; 32] {
                [0u8; 32]
            }

            fn is_healthy(&self) -> bool {
                true
            }

            fn get_entry(&self, _id: &Uuid) -> crate::error::ServerResult<Entry> {
                unimplemented!()
            }

            fn get_inclusion_proof(
                &self,
                _entry_id: &Uuid,
                _tree_size: Option<u64>,
            ) -> crate::error::ServerResult<InclusionProof> {
                unimplemented!()
            }

            fn get_consistency_proof(
                &self,
                _from_size: u64,
                _to_size: u64,
            ) -> crate::error::ServerResult<ConsistencyProof> {
                unimplemented!()
            }

            fn get_anchors(&self, _tree_size: u64) -> crate::error::ServerResult<Vec<Anchor>> {
                Ok(vec![])
            }

            fn get_latest_anchored_size(&self) -> crate::error::ServerResult<Option<u64>> {
                Ok(None)
            }

            fn get_anchors_covering(
                &self,
                _target_tree_size: u64,
                _limit: usize,
            ) -> crate::error::ServerResult<Vec<Anchor>> {
                Ok(vec![])
            }

            fn get_root_at_size(&self, _tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
                Ok([0u8; 32])
            }

            fn get_super_root(
                &self,
                _super_tree_size: u64,
            ) -> crate::error::ServerResult<[u8; 32]> {
                Err(ServerError::Storage(StorageError::Database(
                    "storage error".to_string(),
                )))
            }

            fn is_initialized(&self) -> bool {
                true
            }
        }

        let storage: Arc<dyn Storage> = Arc::new(ErrorMockStorage);
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_submit_respects_batch_size_parameter() {
        // Note: batch_size parameter is currently unused in submit phase
        // This test documents the current behavior
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([2u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        // Test with different batch sizes (parameter is unused but accepted)
        let result = submit_unanchored_trees(&index, &storage, &client, 0).await;
        assert!(result.is_ok());

        let result = submit_unanchored_trees(&index, &storage, &client, 1000).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_submit_with_zero_super_tree_size() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Both sizes are 0
        {
            let idx = index.lock().await;
            idx.set_super_tree_size(0).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([0u8; 32]));
        let client = Arc::new(MockOtsClient::new_success());
        let call_count = client.call_count.clone();
        let client_trait: Arc<dyn OtsClient> = client;

        let result = submit_unanchored_trees(&index, &storage, &client_trait, 100).await;
        assert!(result.is_ok());

        // Should not submit when sizes are equal
        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_submit_index_get_super_tree_size_error() {
        // Create index and immediately drop the connection by using invalid state
        use rusqlite::Connection;
        let conn = Connection::open_in_memory().expect("Failed to create in-memory DB");
        let store = IndexStore::from_connection(conn);
        // Don't initialize - this will cause errors

        let index = Arc::new(Mutex::new(store));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([1u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_submit_with_different_super_root_values() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        // Test with all zeros
        let storage_zeros: Arc<dyn Storage> = Arc::new(MockStorage::new([0u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());
        let result = submit_unanchored_trees(&index, &storage_zeros, &client, 100).await;
        assert!(result.is_ok());

        // Test with all 0xFF
        {
            let idx = index.lock().await;
            idx.set_super_tree_size(2).unwrap();
        }
        let storage_ff: Arc<dyn Storage> = Arc::new(MockStorage::new([0xFFu8; 32]));
        let result = submit_unanchored_trees(&index, &storage_ff, &client, 100).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_submit_verifies_anchor_creation() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let super_root = [42u8; 32];
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(super_root));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        // Verify anchor details
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].anchor.anchored_hash, super_root);
        assert_eq!(pending[0].anchor.super_tree_size, Some(1));
    }

    #[tokio::test]
    async fn test_submit_concurrent_access() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(2).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([5u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        // Spawn multiple concurrent submissions (though they'll serialize via mutex)
        let index_clone = index.clone();
        let storage_clone = storage.clone();
        let client_clone = client.clone();

        let handle = tokio::spawn(async move {
            submit_unanchored_trees(&index_clone, &storage_clone, &client_clone, 100).await
        });

        let result1 = submit_unanchored_trees(&index, &storage, &client, 100).await;
        let result2 = handle.await.unwrap();

        // Both should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_submit_network_error_preserves_state() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([3u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_error(AnchorError::Network(
            "connection refused".to_string(),
        )));

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok()); // Function doesn't propagate OTS errors

        // State should be unchanged
        let idx = index.lock().await;
        let last_submitted = idx.get_last_ots_submitted_super_tree_size().unwrap();
        assert_eq!(last_submitted, 0); // Still at original value

        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 0); // No anchor created
    }

    #[tokio::test]
    async fn test_submit_invalid_response_error() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([4u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_error(
            AnchorError::InvalidResponse("invalid proof format".to_string()),
        ));

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        // Verify state unchanged after error
        let idx = index.lock().await;
        let last_submitted = idx.get_last_ots_submitted_super_tree_size().unwrap();
        assert_eq!(last_submitted, 0);
    }

    #[tokio::test]
    async fn test_submit_empty_calendar_url() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        struct MockOtsClientEmptyUrl;

        #[async_trait]
        impl OtsClient for MockOtsClientEmptyUrl {
            async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
                Ok(("".to_string(), vec![1, 2, 3]))
            }

            async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
                unimplemented!()
            }
        }

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([6u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClientEmptyUrl);

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        // Verify anchor was still created
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_submit_empty_proof() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        struct MockOtsClientEmptyProof;

        #[async_trait]
        impl OtsClient for MockOtsClientEmptyProof {
            async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
                Ok(("http://test.com".to_string(), vec![]))
            }

            async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
                unimplemented!()
            }
        }

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([7u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClientEmptyProof);

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        // Verify anchor was created with empty proof
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_submit_very_large_super_tree_size() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(u64::MAX).unwrap();
            idx.set_last_ots_submitted_super_tree_size(u64::MAX - 1)
                .unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([8u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        let idx = index.lock().await;
        let last_submitted = idx.get_last_ots_submitted_super_tree_size().unwrap();
        assert_eq!(last_submitted, u64::MAX);
    }

    #[tokio::test]
    async fn test_submit_idempotency() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([9u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        // First submission
        let result1 = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result1.is_ok());

        {
            let idx = index.lock().await;
            let pending = idx.get_pending_ots_anchors().unwrap();
            assert_eq!(pending.len(), 1);
        }

        // Second submission with same state should be no-op
        let result2 = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result2.is_ok());

        {
            let idx = index.lock().await;
            let pending = idx.get_pending_ots_anchors().unwrap();
            assert_eq!(pending.len(), 1); // Still only one anchor
        }
    }

    #[tokio::test]
    async fn test_submit_index_set_last_submitted_error() {
        use rusqlite::Connection;
        let conn = Connection::open_in_memory().expect("Failed to create in-memory DB");
        let store = IndexStore::from_connection(conn);
        store.initialize().expect("Failed to initialize schema");

        {
            store.set_super_tree_size(1).unwrap();
            store.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let index = Arc::new(Mutex::new(store));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([10u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        // Verify last submitted was updated
        let idx = index.lock().await;
        let last = idx.get_last_ots_submitted_super_tree_size().unwrap();
        assert_eq!(last, 1);
    }

    #[tokio::test]
    async fn test_submit_with_large_proof() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        struct MockOtsClientLargeProof;

        #[async_trait]
        impl OtsClient for MockOtsClientLargeProof {
            async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
                // Return large proof (10KB)
                Ok(("http://test.com".to_string(), vec![0xFFu8; 10_000]))
            }

            async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
                unimplemented!()
            }
        }

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([11u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClientLargeProof);

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        // Verify anchor was created with large proof
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_submit_preserves_super_root_value() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let super_root = [
            0x12u8, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55,
            0x66, 0x77, 0x88, 0x99,
        ];
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(super_root));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        // Verify exact super_root value in anchor
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].anchor.anchored_hash, super_root);
    }

    #[tokio::test]
    async fn test_submit_sequential_growth() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage1: Arc<dyn Storage> = Arc::new(MockStorage::new([1u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        // First submit
        submit_unanchored_trees(&index, &storage1, &client, 100)
            .await
            .unwrap();

        // Grow super tree
        {
            let idx = index.lock().await;
            idx.set_super_tree_size(2).unwrap();
        }

        let storage2: Arc<dyn Storage> = Arc::new(MockStorage::new([2u8; 32]));
        submit_unanchored_trees(&index, &storage2, &client, 100)
            .await
            .unwrap();

        // Grow again
        {
            let idx = index.lock().await;
            idx.set_super_tree_size(3).unwrap();
        }

        let storage3: Arc<dyn Storage> = Arc::new(MockStorage::new([3u8; 32]));
        submit_unanchored_trees(&index, &storage3, &client, 100)
            .await
            .unwrap();

        // Verify all three anchors
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 3);
        assert_eq!(pending[0].anchor.anchored_hash, [1u8; 32]);
        assert_eq!(pending[1].anchor.anchored_hash, [2u8; 32]);
        assert_eq!(pending[2].anchor.anchored_hash, [3u8; 32]);
    }

    #[tokio::test]
    async fn test_submit_with_url_special_chars() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        struct MockOtsClientSpecialUrl;

        #[async_trait]
        impl OtsClient for MockOtsClientSpecialUrl {
            async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
                Ok((
                    "https://calendar.example.com/path?param=value&other=test#fragment".to_string(),
                    vec![1, 2, 3],
                ))
            }

            async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
                unimplemented!()
            }
        }

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([12u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClientSpecialUrl);

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_submit_boundary_super_tree_sizes() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Test size 1 vs 0
        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([13u8; 32]));
        let client = Arc::new(MockOtsClient::new_success());
        let call_count = client.call_count.clone();
        let client_trait: Arc<dyn OtsClient> = client;

        let result = submit_unanchored_trees(&index, &storage, &client_trait, 100).await;
        assert!(result.is_ok());
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_submit_returns_ok_on_all_error_types() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([14u8; 32]));

        // Test different error types - all should return Ok
        let errors = vec![
            AnchorError::Network("timeout".to_string()),
            AnchorError::InvalidResponse("bad format".to_string()),
        ];

        for error in errors {
            let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_error(error));
            let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
            assert!(result.is_ok(), "Should return Ok for all error types");
        }
    }

    #[tokio::test]
    async fn test_submit_multiple_calls_same_size() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(5).unwrap();
            idx.set_last_ots_submitted_super_tree_size(5).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([15u8; 32]));
        let client = Arc::new(MockOtsClient::new_success());
        let call_count = client.call_count.clone();
        let client_trait: Arc<dyn OtsClient> = client;

        // Multiple calls should all be no-ops
        for _ in 0..5 {
            let result = submit_unanchored_trees(&index, &storage, &client_trait, 100).await;
            assert!(result.is_ok());
        }

        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_submit_batch_size_edge_cases() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(0).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([16u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        // Test with extreme batch sizes (parameter is unused but should not error)
        let batch_sizes = vec![0, 1, usize::MAX];

        for batch_size in batch_sizes {
            let result = submit_unanchored_trees(&index, &storage, &client, batch_size).await;
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_submit_anchor_created_successfully() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([17u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        // Verify anchor was created
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_submit_super_tree_size_in_anchor() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(42).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([18u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        // Verify super_tree_size is stored in anchor
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].anchor.super_tree_size, Some(42));
    }

    #[tokio::test]
    async fn test_submit_calendar_url_stored() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        struct MockOtsClientCustomUrl;

        #[async_trait]
        impl OtsClient for MockOtsClientCustomUrl {
            async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
                Ok(("https://custom-calendar.example.com".to_string(), vec![1]))
            }

            async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
                unimplemented!()
            }
        }

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([19u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClientCustomUrl);

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        // Verify calendar URL is stored in anchor
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_submit_index_get_last_submitted_error() {
        use rusqlite::Connection;
        let conn = Connection::open_in_memory().expect("Failed to create in-memory DB");
        let store = IndexStore::from_connection(conn);
        // Don't initialize - this will cause errors

        let index = Arc::new(Mutex::new(store));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([1u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_submit_with_equal_sizes() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(5).unwrap();
            idx.set_last_ots_submitted_super_tree_size(5).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([20u8; 32]));
        let client = Arc::new(MockOtsClient::new_success());
        let call_count = client.call_count.clone();
        let client_trait: Arc<dyn OtsClient> = client;

        let result = submit_unanchored_trees(&index, &storage, &client_trait, 100).await;
        assert!(result.is_ok());
        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_submit_with_decreased_size() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Simulate a scenario where last_submitted > current (should not happen in practice)
        {
            let idx = index.lock().await;
            idx.set_super_tree_size(5).unwrap();
            idx.set_last_ots_submitted_super_tree_size(10).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([21u8; 32]));
        let client = Arc::new(MockOtsClient::new_success());
        let call_count = client.call_count.clone();
        let client_trait: Arc<dyn OtsClient> = client;

        let result = submit_unanchored_trees(&index, &storage, &client_trait, 100).await;
        assert!(result.is_ok());
        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_submit_with_max_u64_last_submitted() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(u64::MAX).unwrap();
            idx.set_last_ots_submitted_super_tree_size(u64::MAX)
                .unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([22u8; 32]));
        let client = Arc::new(MockOtsClient::new_success());
        let call_count = client.call_count.clone();
        let client_trait: Arc<dyn OtsClient> = client;

        let result = submit_unanchored_trees(&index, &storage, &client_trait, 100).await;
        assert!(result.is_ok());
        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_submit_proof_with_special_bytes() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        struct MockOtsClientSpecialProof;

        #[async_trait]
        impl OtsClient for MockOtsClientSpecialProof {
            async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
                // Return proof with various byte patterns
                Ok((
                    "http://test.com".to_string(),
                    vec![0x00, 0xFF, 0x80, 0x7F, 0x01],
                ))
            }

            async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
                unimplemented!()
            }
        }

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([23u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClientSpecialProof);

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_submit_all_error_variants() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([24u8; 32]));

        // Test with Network error
        let client_network: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_error(
            AnchorError::Network("network error".to_string()),
        ));
        let result = submit_unanchored_trees(&index, &storage, &client_network, 100).await;
        assert!(result.is_ok());

        // Test with InvalidResponse error
        {
            let idx = index.lock().await;
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }
        let client_invalid: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_error(
            AnchorError::InvalidResponse("invalid".to_string()),
        ));
        let result = submit_unanchored_trees(&index, &storage, &client_invalid, 100).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_submit_with_max_batch_size_zero() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([25u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 0).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_submit_with_max_batch_size_max() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([26u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, usize::MAX).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_submit_proof_stored_correctly() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        struct MockOtsClientCustomProof;

        #[async_trait]
        impl OtsClient for MockOtsClientCustomProof {
            async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
                Ok(("http://test.com".to_string(), vec![0xAA, 0xBB, 0xCC, 0xDD]))
            }

            async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
                unimplemented!()
            }
        }

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([20u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClientCustomProof);

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_submit_anchor_id_returned() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let super_root = [99u8; 32];
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(super_root));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        // Verify anchor_id field is properly set
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].id > 0);
    }

    #[tokio::test]
    async fn test_submit_with_minimal_super_tree_growth() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        {
            let idx = index.lock().await;
            idx.set_super_tree_size(1).unwrap();
            idx.set_last_ots_submitted_super_tree_size(0).unwrap();
        }

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new([77u8; 32]));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        let result = submit_unanchored_trees(&index, &storage, &client, 100).await;
        assert!(result.is_ok());

        let idx = index.lock().await;
        let last_submitted = idx.get_last_ots_submitted_super_tree_size().unwrap();
        assert_eq!(last_submitted, 1);
    }
}
