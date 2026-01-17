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
    use crate::anchoring::ots::OtsClient;
    use crate::error::{ServerError, StorageError};
    use crate::traits::{
        Anchor, AnchorType, AppendParams, BatchResult, ConsistencyProof, Entry, InclusionProof,
        TreeHead, Storage,
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

        fn get_anchors(
            &self,
            _tree_size: u64,
        ) -> crate::error::ServerResult<Vec<Anchor>> {
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
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_error(
            AnchorError::Network("timeout".to_string()),
        ));

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

            fn get_anchors(
                &self,
                _tree_size: u64,
            ) -> crate::error::ServerResult<Vec<Anchor>> {
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
}
