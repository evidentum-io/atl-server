//! OTS polling phase
//!
//! Polls pending OTS anchors for Bitcoin confirmation and updates them atomically.

#[cfg(feature = "ots")]
use std::sync::Arc;

#[cfg(feature = "ots")]
use crate::error::{ServerError, ServerResult};

#[cfg(feature = "ots")]
use crate::anchoring::ots::{OtsClient, OtsStatus};

#[cfg(feature = "ots")]
use crate::storage::index::IndexStore;

#[cfg(feature = "ots")]
use tokio::sync::Mutex;

/// Poll pending OTS anchors for Bitcoin confirmation
///
/// Upgrades pending proofs via OTS calendar and updates database atomically
/// when Bitcoin confirmation is detected.
#[cfg(feature = "ots")]
pub async fn poll_pending_anchors(
    index: &Arc<Mutex<IndexStore>>,
    client: &Arc<dyn OtsClient>,
    max_batch_size: usize,
) -> ServerResult<()> {
    let pending = {
        let idx = index.lock().await;
        idx.get_pending_ots_anchors().map_err(|e| {
            ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
        })?
    };

    if pending.is_empty() {
        tracing::debug!("No pending OTS anchors");
        return Ok(());
    }

    let anchors: Vec<_> = pending.into_iter().take(max_batch_size).collect();

    tracing::info!(
        count = anchors.len(),
        "Polling OTS anchors for Bitcoin confirmation"
    );

    let mut confirmed = 0;
    for anchor in anchors {
        let result = match client.upgrade(&anchor.anchor.token).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    anchor_id = anchor.id,
                    error = %e,
                    "OTS upgrade failed, will retry"
                );
                continue;
            }
        };

        let upgrade_result = match result {
            None => {
                tracing::debug!(anchor_id = anchor.id, "No OTS upgrade available yet");
                continue;
            }
            Some(r) => r,
        };

        match upgrade_result.status {
            OtsStatus::Confirmed {
                block_height,
                block_time,
            } => {
                {
                    let mut idx = index.lock().await;
                    idx.confirm_ots_anchor_atomic(
                        anchor.id,
                        &upgrade_result.proof,
                        block_height,
                        block_time,
                    )
                    .map_err(|e| {
                        ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
                    })?
                };

                tracing::info!(
                    anchor_id = anchor.id,
                    block_height = block_height,
                    "OTS anchor confirmed in Bitcoin"
                );
                confirmed += 1;
            }
            OtsStatus::Pending { calendar_url } => {
                {
                    let idx = index.lock().await;
                    idx.update_anchor_token(anchor.id, &upgrade_result.proof)
                        .map_err(|e| {
                            ServerError::Storage(crate::error::StorageError::Database(
                                e.to_string(),
                            ))
                        })?
                };
                tracing::debug!(
                    anchor_id = anchor.id,
                    calendar_url = %calendar_url,
                    "OTS proof upgraded but still pending"
                );
            }
        }
    }

    if confirmed > 0 {
        tracing::info!(
            confirmed = confirmed,
            "OTS poll phase completed with confirmations"
        );
    }

    Ok(())
}

#[cfg(all(test, feature = "ots"))]
mod tests {
    use super::*;
    use crate::anchoring::error::AnchorError;
    use crate::anchoring::ots::{OtsClient, OtsStatus, UpgradeResult};
    use crate::storage::index::AnchorWithId;
    use crate::traits::{Anchor, AnchorType};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockOtsClient {
        upgrade_result: Option<UpgradeResult>,
        upgrade_error: Option<AnchorError>,
        call_count: Arc<AtomicUsize>,
    }

    impl MockOtsClient {
        fn new_pending() -> Self {
            Self {
                upgrade_result: Some(UpgradeResult {
                    proof: vec![1, 2, 3],
                    status: OtsStatus::Pending {
                        calendar_url: "http://calendar.example".to_string(),
                    },
                }),
                upgrade_error: None,
                call_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn new_confirmed(block_height: u64, block_time: u64) -> Self {
            Self {
                upgrade_result: Some(UpgradeResult {
                    proof: vec![4, 5, 6],
                    status: OtsStatus::Confirmed {
                        block_height,
                        block_time,
                    },
                }),
                upgrade_error: None,
                call_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn new_no_upgrade() -> Self {
            Self {
                upgrade_result: None,
                upgrade_error: None,
                call_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn new_error(error: AnchorError) -> Self {
            Self {
                upgrade_result: None,
                upgrade_error: Some(error),
                call_count: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl OtsClient for MockOtsClient {
        async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
            unimplemented!()
        }

        async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            if let Some(ref err) = self.upgrade_error {
                return Err(err.clone());
            }
            Ok(self.upgrade_result.clone())
        }
    }

    fn create_test_anchor(id: i64, token: Vec<u8>) -> AnchorWithId {
        AnchorWithId {
            id,
            anchor: Anchor {
                anchor_type: AnchorType::BitcoinOts,
                target: "bitcoin".to_string(),
                anchored_hash: [1u8; 32],
                tree_size: 100,
                super_tree_size: Some(10),
                timestamp: 1234567890,
                token,
                metadata: serde_json::json!({}),
            },
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
    async fn test_poll_no_pending_anchors() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_no_upgrade());

        let result = poll_pending_anchors(&index, &client, 100).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_poll_anchor_no_upgrade_available() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Create a pending OTS anchor
        let anchor_id = {
            let mut idx = index.lock().await;
            idx.submit_super_root_ots_anchor(&[0u8; 32], "http://calendar.example", &[0u8; 32], 1)
                .unwrap()
        };

        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_no_upgrade());

        let result = poll_pending_anchors(&index, &client, 100).await;
        assert!(result.is_ok());

        // Anchor should still be pending
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, anchor_id);
    }

    #[tokio::test]
    async fn test_poll_anchor_pending_upgrade() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Create a pending OTS anchor
        let anchor_id = {
            let mut idx = index.lock().await;
            idx.submit_super_root_ots_anchor(&[0u8; 32], "http://calendar.example", &[0u8; 32], 1)
                .unwrap()
        };

        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_pending());

        let result = poll_pending_anchors(&index, &client, 100).await;
        assert!(result.is_ok());

        // Anchor should still be pending but token updated
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, anchor_id);
        assert_eq!(pending[0].anchor.token, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_poll_anchor_confirmed() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Create a pending OTS anchor
        let _anchor_id = {
            let mut idx = index.lock().await;
            idx.submit_super_root_ots_anchor(&[0u8; 32], "http://calendar.example", &[0u8; 32], 1)
                .unwrap()
        };

        let block_height = 800000u64;
        let block_time = 1700000000u64;
        let client: Arc<dyn OtsClient> =
            Arc::new(MockOtsClient::new_confirmed(block_height, block_time));

        let result = poll_pending_anchors(&index, &client, 100).await;
        assert!(result.is_ok());

        // Anchor should be confirmed and removed from pending
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 0);
    }

    #[tokio::test]
    async fn test_poll_anchor_upgrade_error() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Create a pending OTS anchor
        let _anchor_id = {
            let mut idx = index.lock().await;
            idx.submit_super_root_ots_anchor(&[0u8; 32], "http://calendar.example", &[0u8; 32], 1)
                .unwrap()
        };

        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_error(AnchorError::Network(
            "timeout".to_string(),
        )));

        let result = poll_pending_anchors(&index, &client, 100).await;
        assert!(result.is_ok()); // Should not fail, just log warning

        // Anchor should still be pending (retry later)
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_poll_respects_batch_size() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Create multiple pending anchors
        {
            let mut idx = index.lock().await;
            for i in 0..5 {
                let hash = [i as u8; 32];
                let root = [i as u8; 32];
                idx.submit_super_root_ots_anchor(
                    &hash,
                    "http://calendar.example",
                    &root,
                    i as u64 + 1,
                )
                .unwrap();
            }
        }

        let client = Arc::new(MockOtsClient::new_no_upgrade());
        let call_count = client.call_count.clone();
        let client_trait: Arc<dyn OtsClient> = client;

        // Poll with batch size 3
        let result = poll_pending_anchors(&index, &client_trait, 3).await;
        assert!(result.is_ok());

        // Should only process 3 anchors
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_poll_multiple_anchors_mixed_results() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Create 3 pending anchors
        {
            let mut idx = index.lock().await;
            for i in 0..3 {
                let hash = [i as u8; 32];
                let root = [i as u8; 32];
                idx.submit_super_root_ots_anchor(
                    &hash,
                    "http://calendar.example",
                    &root,
                    i as u64 + 1,
                )
                .unwrap();
            }
        }

        // Mock client that returns confirmed for first call, pending for others
        struct MixedMockClient {
            call_count: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl OtsClient for MixedMockClient {
            async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
                unimplemented!()
            }

            async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
                let count = self.call_count.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    Ok(Some(UpgradeResult {
                        proof: vec![10, 11, 12],
                        status: OtsStatus::Confirmed {
                            block_height: 800000,
                            block_time: 1700000000,
                        },
                    }))
                } else {
                    Ok(None)
                }
            }
        }

        let client: Arc<dyn OtsClient> = Arc::new(MixedMockClient {
            call_count: Arc::new(AtomicUsize::new(0)),
        });

        let result = poll_pending_anchors(&index, &client, 100).await;
        assert!(result.is_ok());

        // One should be confirmed, two should remain pending
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 2);
    }

    #[tokio::test]
    async fn test_poll_empty_batch_size() {
        let index = Arc::new(Mutex::new(create_test_index_store()));

        // Create a pending anchor
        {
            let mut idx = index.lock().await;
            idx.submit_super_root_ots_anchor(&[0u8; 32], "http://calendar.example", &[0u8; 32], 1)
                .unwrap();
        }

        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_no_upgrade());

        // Poll with batch size 0 - should process nothing
        let result = poll_pending_anchors(&index, &client, 0).await;
        assert!(result.is_ok());

        // Anchor should still be pending
        let idx = index.lock().await;
        let pending = idx.get_pending_ots_anchors().unwrap();
        assert_eq!(pending.len(), 1);
    }
}
