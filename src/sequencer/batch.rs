//! Batch flushing logic with retry

use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, warn};

use crate::error::ServerError;
use crate::traits::{AppendResult, BatchResult, Storage};

use super::buffer::AppendRequest;
use super::config::SequencerConfig;

/// Flush accumulated batch to storage
///
/// Uses async Storage trait - no spawn_blocking needed.
/// Maps BatchResult entries to individual AppendResults for each handler.
pub async fn flush_batch(
    batch: &mut Vec<AppendRequest>,
    storage: &Arc<dyn Storage>,
    config: &SequencerConfig,
) {
    if batch.is_empty() {
        return;
    }

    let batch_size = batch.len();
    let requests: Vec<AppendRequest> = std::mem::take(batch);

    // Extract params and response channels
    let (params, response_txs): (Vec<_>, Vec<_>) = requests
        .into_iter()
        .map(|r| (r.params, r.response_tx))
        .unzip();

    // Attempt batch write with retry (fully async)
    let result = write_batch_with_retry(storage, &params, config).await;

    match result {
        Ok(batch_result) => {
            // Map BatchResult entries to individual AppendResults
            for (entry_result, tx) in batch_result.entries.into_iter().zip(response_txs) {
                let append_result = AppendResult {
                    id: entry_result.id,
                    leaf_index: entry_result.leaf_index,
                    tree_head: batch_result.tree_head.clone(),
                    inclusion_proof: vec![], // Proofs generated lazily on GET
                    timestamp: batch_result.committed_at,
                };
                let _ = tx.send(Ok(append_result));
            }
            debug!(batch_size, "Batch committed successfully");
        }
        Err(e) => {
            error!(batch_size, error = %e, "Batch failed after retries");
            let error_msg = e.to_string();
            for tx in response_txs {
                let _ = tx.send(Err(ServerError::Internal(error_msg.clone())));
            }
        }
    }
}

/// Write batch with exponential backoff retry
///
/// Direct async call - no spawn_blocking wrapper needed!
async fn write_batch_with_retry(
    storage: &Arc<dyn Storage>,
    params: &[crate::traits::AppendParams],
    config: &SequencerConfig,
) -> Result<BatchResult, ServerError> {
    let mut attempt = 0;
    let mut delay_ms = config.retry_base_ms;

    loop {
        // Direct async call to storage
        let result = storage.append_batch(params.to_vec()).await;

        match result {
            Ok(batch_result) => return Ok(batch_result),
            Err(e) => {
                attempt += 1;
                if attempt >= config.retry_count {
                    return Err(ServerError::Storage(e));
                }

                warn!(
                    attempt,
                    max_attempts = config.retry_count,
                    delay_ms,
                    error = %e,
                    "Batch write failed, retrying"
                );

                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                delay_ms *= 2;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex as StdMutex;
    use tokio::sync::oneshot;
    use uuid::Uuid;

    use crate::error::StorageError;
    use crate::traits::{
        AppendParams, ConsistencyProof, Entry, EntryResult, InclusionProof, TreeHead,
    };

    /// Mock storage that tracks calls and can be configured to succeed or fail
    #[derive(Clone)]
    struct MockStorage {
        call_count: Arc<AtomicU32>,
        should_fail: Arc<StdMutex<bool>>,
        fail_count: Arc<AtomicU32>,
    }

    impl MockStorage {
        fn new(should_fail: bool) -> Self {
            Self {
                call_count: Arc::new(AtomicU32::new(0)),
                should_fail: Arc::new(StdMutex::new(should_fail)),
                fail_count: Arc::new(AtomicU32::new(0)),
            }
        }

        fn get_call_count(&self) -> u32 {
            self.call_count.load(Ordering::SeqCst)
        }

        fn get_fail_count(&self) -> u32 {
            self.fail_count.load(Ordering::SeqCst)
        }

        fn set_should_fail(&self, fail: bool) {
            *self.should_fail.lock().expect("lock") = fail;
        }
    }

    #[async_trait]
    impl Storage for MockStorage {
        async fn append_batch(
            &self,
            params: Vec<AppendParams>,
        ) -> Result<crate::traits::BatchResult, StorageError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);

            let should_fail = *self.should_fail.lock().expect("lock");
            if should_fail {
                self.fail_count.fetch_add(1, Ordering::SeqCst);
                return Err(StorageError::Database("mock failure".into()));
            }

            let entries: Vec<EntryResult> = params
                .iter()
                .enumerate()
                .map(|(idx, _)| EntryResult {
                    id: Uuid::new_v4(),
                    leaf_index: idx as u64,
                    leaf_hash: [1u8; 32],
                })
                .collect();

            Ok(crate::traits::BatchResult {
                entries,
                tree_head: TreeHead {
                    tree_size: params.len() as u64,
                    root_hash: [2u8; 32],
                    origin: [3u8; 32],
                },
                committed_at: Utc::now(),
            })
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
            Err(crate::error::ServerError::EntryNotFound("mock".into()))
        }

        fn get_inclusion_proof(
            &self,
            _entry_id: &Uuid,
            _tree_size: Option<u64>,
        ) -> crate::error::ServerResult<InclusionProof> {
            Err(crate::error::ServerError::EntryNotFound("mock".into()))
        }

        fn get_consistency_proof(
            &self,
            _from_size: u64,
            _to_size: u64,
        ) -> crate::error::ServerResult<ConsistencyProof> {
            Err(crate::error::ServerError::InvalidArgument("mock".into()))
        }

        fn get_anchors(
            &self,
            _tree_size: u64,
        ) -> crate::error::ServerResult<Vec<crate::traits::anchor::Anchor>> {
            Ok(vec![])
        }

        fn get_latest_anchored_size(&self) -> crate::error::ServerResult<Option<u64>> {
            Ok(None)
        }

        fn get_anchors_covering(
            &self,
            _target_tree_size: u64,
            _limit: usize,
        ) -> crate::error::ServerResult<Vec<crate::traits::anchor::Anchor>> {
            Ok(vec![])
        }

        fn get_root_at_size(&self, _tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
            Ok([0u8; 32])
        }

        fn get_super_root(&self, _super_tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
            Ok([0u8; 32])
        }

        fn is_initialized(&self) -> bool {
            true
        }
    }

    fn create_test_request() -> AppendRequest {
        let (tx, _rx) = oneshot::channel();
        AppendRequest {
            params: AppendParams {
                payload_hash: [1u8; 32],
                metadata_hash: [2u8; 32],
                metadata_cleartext: None,
                external_id: None,
            },
            response_tx: tx,
        }
    }

    #[tokio::test]
    async fn test_flush_batch_empty() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();
        let mut batch = vec![];

        flush_batch(&mut batch, &storage, &config).await;

        assert_eq!(mock.get_call_count(), 0);
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn test_flush_batch_success_single_entry() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();
        let mut batch = vec![create_test_request()];

        flush_batch(&mut batch, &storage, &config).await;

        assert!(batch.is_empty());
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_flush_batch_success_multiple_entries() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();
        let mut batch = vec![
            create_test_request(),
            create_test_request(),
            create_test_request(),
        ];

        flush_batch(&mut batch, &storage, &config).await;

        assert!(batch.is_empty());
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_flush_batch_failure_after_retries() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 2,
            retry_base_ms: 1, // Fast retries for testing
            ..Default::default()
        };
        let mut batch = vec![create_test_request()];

        flush_batch(&mut batch, &storage, &config).await;

        assert!(batch.is_empty());
        // Should try initial + 2 retries = 3 attempts, but fail before reaching retry_count
        // Actually it tries until attempt >= retry_count, so 2 attempts total
        assert_eq!(mock.get_call_count(), 2);
    }

    #[tokio::test]
    async fn test_flush_batch_responses_sent_on_success() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();

        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let mut batch = vec![
            AppendRequest {
                params: AppendParams {
                    payload_hash: [1u8; 32],
                    metadata_hash: [2u8; 32],
                    metadata_cleartext: None,
                    external_id: None,
                },
                response_tx: tx1,
            },
            AppendRequest {
                params: AppendParams {
                    payload_hash: [3u8; 32],
                    metadata_hash: [4u8; 32],
                    metadata_cleartext: None,
                    external_id: None,
                },
                response_tx: tx2,
            },
        ];

        flush_batch(&mut batch, &storage, &config).await;

        // Check that both responses were sent and are Ok
        let result1 = rx1.await.expect("should receive response");
        let result2 = rx2.await.expect("should receive response");

        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_flush_batch_error_responses_sent_on_failure() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 1,
            retry_base_ms: 1,
            ..Default::default()
        };

        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let mut batch = vec![
            AppendRequest {
                params: AppendParams {
                    payload_hash: [1u8; 32],
                    metadata_hash: [2u8; 32],
                    metadata_cleartext: None,
                    external_id: None,
                },
                response_tx: tx1,
            },
            AppendRequest {
                params: AppendParams {
                    payload_hash: [3u8; 32],
                    metadata_hash: [4u8; 32],
                    metadata_cleartext: None,
                    external_id: None,
                },
                response_tx: tx2,
            },
        ];

        flush_batch(&mut batch, &storage, &config).await;

        // Check that both responses were sent and are Err
        let result1 = rx1.await.expect("should receive response");
        let result2 = rx2.await.expect("should receive response");

        assert!(result1.is_err());
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn test_write_batch_with_retry_success_first_attempt() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();
        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let result = write_batch_with_retry(&storage, &params, &config).await;

        assert!(result.is_ok());
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_write_batch_with_retry_eventual_success() {
        use std::sync::atomic::AtomicU32;

        // Create a mock that fails exactly once, then succeeds
        #[derive(Clone)]
        struct OneFailureMockStorage {
            call_count: Arc<AtomicU32>,
        }

        #[async_trait]
        impl Storage for OneFailureMockStorage {
            async fn append_batch(
                &self,
                params: Vec<AppendParams>,
            ) -> Result<crate::traits::BatchResult, StorageError> {
                let count = self.call_count.fetch_add(1, Ordering::SeqCst);

                // Fail on first attempt only
                if count == 0 {
                    return Err(StorageError::Database(
                        "mock failure on first attempt".into(),
                    ));
                }

                // Succeed on subsequent attempts
                let entries: Vec<EntryResult> = params
                    .iter()
                    .enumerate()
                    .map(|(idx, _)| EntryResult {
                        id: Uuid::new_v4(),
                        leaf_index: idx as u64,
                        leaf_hash: [1u8; 32],
                    })
                    .collect();

                Ok(crate::traits::BatchResult {
                    entries,
                    tree_head: TreeHead {
                        tree_size: params.len() as u64,
                        root_hash: [2u8; 32],
                        origin: [3u8; 32],
                    },
                    committed_at: Utc::now(),
                })
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
                Err(crate::error::ServerError::EntryNotFound("mock".into()))
            }

            fn get_inclusion_proof(
                &self,
                _entry_id: &Uuid,
                _tree_size: Option<u64>,
            ) -> crate::error::ServerResult<InclusionProof> {
                Err(crate::error::ServerError::EntryNotFound("mock".into()))
            }

            fn get_consistency_proof(
                &self,
                _from_size: u64,
                _to_size: u64,
            ) -> crate::error::ServerResult<ConsistencyProof> {
                Err(crate::error::ServerError::InvalidArgument("mock".into()))
            }

            fn get_anchors(
                &self,
                _tree_size: u64,
            ) -> crate::error::ServerResult<Vec<crate::traits::anchor::Anchor>> {
                Ok(vec![])
            }

            fn get_latest_anchored_size(&self) -> crate::error::ServerResult<Option<u64>> {
                Ok(None)
            }

            fn get_anchors_covering(
                &self,
                _target_tree_size: u64,
                _limit: usize,
            ) -> crate::error::ServerResult<Vec<crate::traits::anchor::Anchor>> {
                Ok(vec![])
            }

            fn get_root_at_size(&self, _tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
                Ok([0u8; 32])
            }

            fn get_super_root(
                &self,
                _super_tree_size: u64,
            ) -> crate::error::ServerResult<[u8; 32]> {
                Ok([0u8; 32])
            }

            fn is_initialized(&self) -> bool {
                true
            }
        }

        let mock = Arc::new(OneFailureMockStorage {
            call_count: Arc::new(AtomicU32::new(0)),
        });
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 3,
            retry_base_ms: 1,
            ..Default::default()
        };
        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let result = write_batch_with_retry(&storage, &params, &config).await;

        // Should succeed after 1 retry
        assert!(result.is_ok());
        assert_eq!(mock.call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_write_batch_with_retry_exhausts_retries() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 2,
            retry_base_ms: 1,
            ..Default::default()
        };
        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let result = write_batch_with_retry(&storage, &params, &config).await;

        assert!(result.is_err());
        assert_eq!(mock.get_call_count(), 2);
        match result {
            Err(ServerError::Storage(StorageError::Database(msg))) => {
                assert_eq!(msg, "mock failure");
            }
            _ => panic!("Expected Storage error"),
        }
    }

    #[tokio::test]
    async fn test_write_batch_with_retry_exponential_backoff() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 3,
            retry_base_ms: 10,
            ..Default::default()
        };
        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let start = tokio::time::Instant::now();
        let result = write_batch_with_retry(&storage, &params, &config).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        // Should have delays: 10ms + 20ms = 30ms minimum
        assert!(elapsed.as_millis() >= 20);
        assert_eq!(mock.get_call_count(), 3);
    }

    #[tokio::test]
    async fn test_write_batch_with_retry_zero_retries() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 0,
            retry_base_ms: 10,
            ..Default::default()
        };
        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let result = write_batch_with_retry(&storage, &params, &config).await;

        assert!(result.is_err());
        // With retry_count = 0, should fail immediately without retry
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_flush_batch_clears_batch_on_success() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();
        let mut batch = vec![create_test_request(), create_test_request()];
        let original_size = batch.len();

        flush_batch(&mut batch, &storage, &config).await;

        assert!(batch.is_empty());
        assert_eq!(original_size, 2);
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_flush_batch_clears_batch_on_failure() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 1,
            retry_base_ms: 1,
            ..Default::default()
        };
        let mut batch = vec![create_test_request(), create_test_request()];

        flush_batch(&mut batch, &storage, &config).await;

        // Batch should be cleared even on failure
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn test_flush_batch_multiple_entries_different_hashes() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();

        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let (tx3, rx3) = oneshot::channel();

        let mut batch = vec![
            AppendRequest {
                params: AppendParams {
                    payload_hash: [1u8; 32],
                    metadata_hash: [2u8; 32],
                    metadata_cleartext: None,
                    external_id: None,
                },
                response_tx: tx1,
            },
            AppendRequest {
                params: AppendParams {
                    payload_hash: [10u8; 32],
                    metadata_hash: [20u8; 32],
                    metadata_cleartext: Some(serde_json::json!({"test": "metadata"})),
                    external_id: Some("ext-123".to_string()),
                },
                response_tx: tx2,
            },
            AppendRequest {
                params: AppendParams {
                    payload_hash: [100u8; 32],
                    metadata_hash: [200u8; 32],
                    metadata_cleartext: None,
                    external_id: Some("ext-456".to_string()),
                },
                response_tx: tx3,
            },
        ];

        flush_batch(&mut batch, &storage, &config).await;

        // All three should receive OK responses
        assert!(rx1.await.expect("rx1").is_ok());
        assert!(rx2.await.expect("rx2").is_ok());
        assert!(rx3.await.expect("rx3").is_ok());
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_flush_batch_preserves_entry_order() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();

        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let mut batch = vec![
            AppendRequest {
                params: AppendParams {
                    payload_hash: [1u8; 32],
                    metadata_hash: [2u8; 32],
                    metadata_cleartext: None,
                    external_id: None,
                },
                response_tx: tx1,
            },
            AppendRequest {
                params: AppendParams {
                    payload_hash: [3u8; 32],
                    metadata_hash: [4u8; 32],
                    metadata_cleartext: None,
                    external_id: None,
                },
                response_tx: tx2,
            },
        ];

        flush_batch(&mut batch, &storage, &config).await;

        let result1 = rx1.await.expect("rx1").expect("ok");
        let result2 = rx2.await.expect("rx2").expect("ok");

        // First request should get leaf_index 0, second should get 1
        assert_eq!(result1.leaf_index, 0);
        assert_eq!(result2.leaf_index, 1);
    }

    #[tokio::test]
    async fn test_write_batch_with_retry_multiple_params() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();
        let params = vec![
            AppendParams {
                payload_hash: [1u8; 32],
                metadata_hash: [2u8; 32],
                metadata_cleartext: None,
                external_id: None,
            },
            AppendParams {
                payload_hash: [3u8; 32],
                metadata_hash: [4u8; 32],
                metadata_cleartext: Some(serde_json::json!({"key": "value"})),
                external_id: None,
            },
            AppendParams {
                payload_hash: [5u8; 32],
                metadata_hash: [6u8; 32],
                metadata_cleartext: None,
                external_id: Some("ext-id".to_string()),
            },
        ];

        let result = write_batch_with_retry(&storage, &params, &config).await;

        assert!(result.is_ok());
        let batch_result = result.unwrap();
        assert_eq!(batch_result.entries.len(), 3);
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_write_batch_with_retry_empty_params() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();
        let params: Vec<AppendParams> = vec![];

        let result = write_batch_with_retry(&storage, &params, &config).await;

        assert!(result.is_ok());
        let batch_result = result.unwrap();
        assert_eq!(batch_result.entries.len(), 0);
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_flush_batch_append_result_fields() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();

        let (tx, rx) = oneshot::channel();
        let mut batch = vec![AppendRequest {
            params: AppendParams {
                payload_hash: [99u8; 32],
                metadata_hash: [88u8; 32],
                metadata_cleartext: None,
                external_id: None,
            },
            response_tx: tx,
        }];

        flush_batch(&mut batch, &storage, &config).await;

        let result = rx.await.expect("should receive").expect("should be ok");

        // Verify AppendResult fields
        assert!(!result.id.to_string().is_empty()); // UUID is set
        assert_eq!(result.leaf_index, 0);
        assert_eq!(result.tree_head.tree_size, 1);
        assert_eq!(result.tree_head.root_hash, [2u8; 32]);
        assert_eq!(result.tree_head.origin, [3u8; 32]);
        assert!(result.inclusion_proof.is_empty()); // Proofs are lazy
    }

    #[tokio::test]
    async fn test_write_batch_with_retry_fails_with_storage_error() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 2,
            retry_base_ms: 1,
            ..Default::default()
        };
        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let result = write_batch_with_retry(&storage, &params, &config).await;

        assert!(result.is_err());
        if let Err(ServerError::Storage(StorageError::Database(msg))) = result {
            assert_eq!(msg, "mock failure");
        } else {
            panic!("Expected Storage error with Database variant");
        }
    }

    #[tokio::test]
    async fn test_flush_batch_with_large_batch() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();

        // Create a batch of 10 requests
        let mut batch = Vec::new();
        let mut receivers = Vec::new();

        for _ in 0..10 {
            let (tx, rx) = oneshot::channel();
            batch.push(AppendRequest {
                params: AppendParams {
                    payload_hash: [1u8; 32],
                    metadata_hash: [2u8; 32],
                    metadata_cleartext: None,
                    external_id: None,
                },
                response_tx: tx,
            });
            receivers.push(rx);
        }

        flush_batch(&mut batch, &storage, &config).await;

        // All 10 should receive responses
        for (idx, rx) in receivers.into_iter().enumerate() {
            let result = rx.await.expect("should receive").expect("should be ok");
            assert_eq!(result.leaf_index, idx as u64);
        }
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_write_batch_retry_backoff_timing() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 4,
            retry_base_ms: 5,
            ..Default::default()
        };
        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let start = tokio::time::Instant::now();
        let result = write_batch_with_retry(&storage, &params, &config).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        // Should have delays: 5ms + 10ms + 20ms = 35ms minimum
        // We allow some slack for scheduling overhead
        assert!(elapsed.as_millis() >= 30);
        assert_eq!(mock.get_call_count(), 4);
    }

    #[tokio::test]
    async fn test_mock_storage_set_should_fail() {
        let mock = MockStorage::new(false);
        assert_eq!(mock.get_call_count(), 0);
        assert_eq!(mock.get_fail_count(), 0);

        // Set to fail
        mock.set_should_fail(true);

        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let result = mock.append_batch(params).await;
        assert!(result.is_err());
        assert_eq!(mock.get_call_count(), 1);
        assert_eq!(mock.get_fail_count(), 1);
    }

    #[tokio::test]
    async fn test_mock_storage_multiple_calls() {
        let mock = MockStorage::new(false);

        for i in 0..5 {
            let params = vec![AppendParams {
                payload_hash: [1u8; 32],
                metadata_hash: [2u8; 32],
                metadata_cleartext: None,
                external_id: None,
            }];

            let result = mock.append_batch(params).await;
            assert!(result.is_ok());
            assert_eq!(mock.get_call_count(), i + 1);
        }

        assert_eq!(mock.get_call_count(), 5);
        assert_eq!(mock.get_fail_count(), 0);
    }

    #[tokio::test]
    async fn test_flush_batch_with_single_failure_then_success() {
        // Test recovery after transient failure
        #[derive(Clone)]
        struct TransientFailureMockStorage {
            call_count: Arc<AtomicU32>,
        }

        #[async_trait]
        impl Storage for TransientFailureMockStorage {
            async fn append_batch(
                &self,
                params: Vec<AppendParams>,
            ) -> Result<BatchResult, StorageError> {
                let count = self.call_count.fetch_add(1, Ordering::SeqCst);

                // Fail first attempt, succeed on second
                if count == 0 {
                    return Err(StorageError::Database("transient failure".into()));
                }

                let entries: Vec<EntryResult> = params
                    .iter()
                    .enumerate()
                    .map(|(idx, _)| EntryResult {
                        id: Uuid::new_v4(),
                        leaf_index: idx as u64,
                        leaf_hash: [1u8; 32],
                    })
                    .collect();

                Ok(BatchResult {
                    entries,
                    tree_head: TreeHead {
                        tree_size: params.len() as u64,
                        root_hash: [2u8; 32],
                        origin: [3u8; 32],
                    },
                    committed_at: Utc::now(),
                })
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
                Err(crate::error::ServerError::EntryNotFound("mock".into()))
            }

            fn get_inclusion_proof(
                &self,
                _entry_id: &Uuid,
                _tree_size: Option<u64>,
            ) -> crate::error::ServerResult<InclusionProof> {
                Err(crate::error::ServerError::EntryNotFound("mock".into()))
            }

            fn get_consistency_proof(
                &self,
                _from_size: u64,
                _to_size: u64,
            ) -> crate::error::ServerResult<ConsistencyProof> {
                Err(crate::error::ServerError::InvalidArgument("mock".into()))
            }

            fn get_anchors(
                &self,
                _tree_size: u64,
            ) -> crate::error::ServerResult<Vec<crate::traits::anchor::Anchor>> {
                Ok(vec![])
            }

            fn get_latest_anchored_size(&self) -> crate::error::ServerResult<Option<u64>> {
                Ok(None)
            }

            fn get_anchors_covering(
                &self,
                _target_tree_size: u64,
                _limit: usize,
            ) -> crate::error::ServerResult<Vec<crate::traits::anchor::Anchor>> {
                Ok(vec![])
            }

            fn get_root_at_size(&self, _tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
                Ok([0u8; 32])
            }

            fn get_super_root(
                &self,
                _super_tree_size: u64,
            ) -> crate::error::ServerResult<[u8; 32]> {
                Ok([0u8; 32])
            }

            fn is_initialized(&self) -> bool {
                true
            }
        }

        let mock = Arc::new(TransientFailureMockStorage {
            call_count: Arc::new(AtomicU32::new(0)),
        });
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 3,
            retry_base_ms: 1,
            ..Default::default()
        };

        let (tx, rx) = oneshot::channel();
        let mut batch = vec![AppendRequest {
            params: AppendParams {
                payload_hash: [1u8; 32],
                metadata_hash: [2u8; 32],
                metadata_cleartext: None,
                external_id: None,
            },
            response_tx: tx,
        }];

        flush_batch(&mut batch, &storage, &config).await;

        let result = rx.await.expect("should receive");
        assert!(result.is_ok());
        assert_eq!(mock.call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_write_batch_with_retry_large_delay() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 2,
            retry_base_ms: 100,
            ..Default::default()
        };
        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let start = tokio::time::Instant::now();
        let result = write_batch_with_retry(&storage, &params, &config).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        // Should have delay: 100ms minimum
        assert!(elapsed.as_millis() >= 80);
        assert_eq!(mock.get_call_count(), 2);
    }

    #[tokio::test]
    async fn test_flush_batch_response_channel_closed() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();

        let (tx, rx) = oneshot::channel();
        drop(rx); // Drop receiver before flush

        let mut batch = vec![AppendRequest {
            params: AppendParams {
                payload_hash: [1u8; 32],
                metadata_hash: [2u8; 32],
                metadata_cleartext: None,
                external_id: None,
            },
            response_tx: tx,
        }];

        // Should not panic even if receiver is dropped
        flush_batch(&mut batch, &storage, &config).await;

        assert!(batch.is_empty());
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_flush_batch_mixed_metadata() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();

        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let (tx3, rx3) = oneshot::channel();

        let mut batch = vec![
            AppendRequest {
                params: AppendParams {
                    payload_hash: [1u8; 32],
                    metadata_hash: [2u8; 32],
                    metadata_cleartext: Some(serde_json::json!({"type": "a"})),
                    external_id: Some("id1".to_string()),
                },
                response_tx: tx1,
            },
            AppendRequest {
                params: AppendParams {
                    payload_hash: [3u8; 32],
                    metadata_hash: [4u8; 32],
                    metadata_cleartext: None,
                    external_id: Some("id2".to_string()),
                },
                response_tx: tx2,
            },
            AppendRequest {
                params: AppendParams {
                    payload_hash: [5u8; 32],
                    metadata_hash: [6u8; 32],
                    metadata_cleartext: Some(serde_json::json!({"type": "b"})),
                    external_id: None,
                },
                response_tx: tx3,
            },
        ];

        flush_batch(&mut batch, &storage, &config).await;

        assert!(rx1.await.expect("rx1").is_ok());
        assert!(rx2.await.expect("rx2").is_ok());
        assert!(rx3.await.expect("rx3").is_ok());
    }

    #[tokio::test]
    async fn test_write_batch_with_retry_one_retry() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 1,
            retry_base_ms: 5,
            ..Default::default()
        };
        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let result = write_batch_with_retry(&storage, &params, &config).await;

        assert!(result.is_err());
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_flush_batch_verify_timestamp() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();

        let (tx, rx) = oneshot::channel();
        let mut batch = vec![AppendRequest {
            params: AppendParams {
                payload_hash: [1u8; 32],
                metadata_hash: [2u8; 32],
                metadata_cleartext: None,
                external_id: None,
            },
            response_tx: tx,
        }];

        let before = Utc::now();
        flush_batch(&mut batch, &storage, &config).await;
        let after = Utc::now();

        let result = rx.await.expect("should receive").expect("should be ok");

        // Verify timestamp is within reasonable range
        assert!(result.timestamp >= before);
        assert!(result.timestamp <= after);
    }

    #[tokio::test]
    async fn test_flush_batch_entry_result_mapping() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();

        let mut receivers = Vec::new();
        let mut batch = Vec::new();

        for i in 0..5 {
            let (tx, rx) = oneshot::channel();
            batch.push(AppendRequest {
                params: AppendParams {
                    payload_hash: [i; 32],
                    metadata_hash: [i + 1; 32],
                    metadata_cleartext: None,
                    external_id: None,
                },
                response_tx: tx,
            });
            receivers.push(rx);
        }

        flush_batch(&mut batch, &storage, &config).await;

        // Verify all entries got correct indices
        for (idx, rx) in receivers.into_iter().enumerate() {
            let result = rx.await.expect("should receive").expect("should be ok");
            assert_eq!(result.leaf_index, idx as u64);
        }
    }

    #[tokio::test]
    async fn test_write_batch_with_very_high_retry_count() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 10,
            retry_base_ms: 1,
            ..Default::default()
        };
        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let result = write_batch_with_retry(&storage, &params, &config).await;

        assert!(result.is_err());
        assert_eq!(mock.get_call_count(), 10);
    }

    #[tokio::test]
    async fn test_flush_batch_tree_head_consistency() {
        let mock = Arc::new(MockStorage::new(false));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig::default();

        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let mut batch = vec![
            AppendRequest {
                params: AppendParams {
                    payload_hash: [1u8; 32],
                    metadata_hash: [2u8; 32],
                    metadata_cleartext: None,
                    external_id: None,
                },
                response_tx: tx1,
            },
            AppendRequest {
                params: AppendParams {
                    payload_hash: [3u8; 32],
                    metadata_hash: [4u8; 32],
                    metadata_cleartext: None,
                    external_id: None,
                },
                response_tx: tx2,
            },
        ];

        flush_batch(&mut batch, &storage, &config).await;

        let result1 = rx1.await.expect("rx1").expect("ok");
        let result2 = rx2.await.expect("rx2").expect("ok");

        // Both should have the same tree_head
        assert_eq!(result1.tree_head.tree_size, result2.tree_head.tree_size);
        assert_eq!(result1.tree_head.root_hash, result2.tree_head.root_hash);
        assert_eq!(result1.tree_head.origin, result2.tree_head.origin);
    }

    #[tokio::test]
    async fn test_write_batch_max_backoff() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 5,
            retry_base_ms: 50,
            ..Default::default()
        };
        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let start = tokio::time::Instant::now();
        let result = write_batch_with_retry(&storage, &params, &config).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        // Delays: 50 + 100 + 200 + 400 = 750ms minimum
        assert!(elapsed.as_millis() >= 700);
        assert_eq!(mock.get_call_count(), 5);
    }

    #[tokio::test]
    async fn test_write_batch_retry_backoff_doubling() {
        let mock = Arc::new(MockStorage::new(true));
        let storage: Arc<dyn Storage> = mock.clone();
        let config = SequencerConfig {
            retry_count: 3,
            retry_base_ms: 8,
            ..Default::default()
        };
        let params = vec![AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];

        let start = tokio::time::Instant::now();
        let result = write_batch_with_retry(&storage, &params, &config).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        // Backoff: 8ms, then 16ms (doubled) = 24ms minimum
        assert!(elapsed.as_millis() >= 20);
        assert_eq!(mock.get_call_count(), 3);
    }
}
