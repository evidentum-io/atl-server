//! Sequencer core logic - batch accumulation and flushing

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};
use tracing::{debug, info};

use crate::traits::Storage;

use super::buffer::{AppendRequest, SequencerHandle};
use super::config::SequencerConfig;

/// The Sequencer accumulates append requests and flushes them in batches
pub struct Sequencer {
    /// Storage backend
    storage: Arc<dyn Storage>,

    /// Configuration
    config: SequencerConfig,

    /// Receiver for incoming requests
    rx: mpsc::Receiver<AppendRequest>,
}

impl Sequencer {
    /// Create a new Sequencer and return its handle
    pub fn new(storage: Arc<dyn Storage>, config: SequencerConfig) -> (Self, SequencerHandle) {
        let (tx, rx) = mpsc::channel(config.buffer_size);

        let sequencer = Self {
            storage,
            config,
            rx,
        };

        let handle = SequencerHandle::new(tx);

        (sequencer, handle)
    }

    /// Run the Sequencer loop (spawn as tokio task)
    pub async fn run(mut self) {
        info!(
            batch_size = self.config.batch_size,
            batch_timeout_ms = self.config.batch_timeout_ms,
            buffer_size = self.config.buffer_size,
            "Sequencer started"
        );

        let mut batch: Vec<AppendRequest> = Vec::with_capacity(self.config.batch_size);
        let mut flush_interval = interval(Duration::from_millis(self.config.batch_timeout_ms));
        let mut batch_start = Instant::now();

        loop {
            tokio::select! {
                // Receive new request
                request = self.rx.recv() => {
                    match request {
                        Some(req) => {
                            if batch.is_empty() {
                                batch_start = Instant::now();
                            }
                            batch.push(req);

                            // Flush if batch is full
                            if batch.len() >= self.config.batch_size {
                                debug!(
                                    batch_size = batch.len(),
                                    trigger = "size",
                                    "Flushing batch"
                                );
                                super::batch::flush_batch(
                                    &mut batch,
                                    &self.storage,
                                    &self.config,
                                )
                                .await;
                            }
                        }
                        None => {
                            // Channel closed, flush remaining and exit
                            if !batch.is_empty() {
                                info!(
                                    batch_size = batch.len(),
                                    "Flushing final batch before shutdown"
                                );
                                super::batch::flush_batch(
                                    &mut batch,
                                    &self.storage,
                                    &self.config,
                                )
                                .await;
                            }
                            info!("Sequencer shutting down");
                            return;
                        }
                    }
                }

                // Timeout tick
                _ = flush_interval.tick() => {
                    if !batch.is_empty() {
                        let elapsed = batch_start.elapsed().as_millis();
                        debug!(
                            batch_size = batch.len(),
                            elapsed_ms = elapsed,
                            trigger = "timeout",
                            "Flushing batch"
                        );
                        super::batch::flush_batch(
                            &mut batch,
                            &self.storage,
                            &self.config,
                        )
                        .await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use uuid::Uuid;

    use crate::error::StorageError;
    use crate::traits::{
        AppendParams, BatchResult, ConsistencyProof, Entry, EntryResult, InclusionProof, TreeHead,
    };

    /// Minimal mock storage for testing Sequencer construction
    struct MockStorage;

    #[async_trait]
    impl Storage for MockStorage {
        async fn append_batch(
            &self,
            params: Vec<AppendParams>,
        ) -> Result<BatchResult, StorageError> {
            Ok(BatchResult {
                entries: params
                    .iter()
                    .enumerate()
                    .map(|(i, _)| EntryResult {
                        id: Uuid::new_v4(),
                        leaf_index: i as u64,
                        leaf_hash: [0u8; 32],
                    })
                    .collect(),
                tree_head: TreeHead {
                    tree_size: params.len() as u64,
                    root_hash: [0u8; 32],
                    origin: [0u8; 32],
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

        #[cfg(not(tarpaulin_include))]
        fn get_inclusion_proof_by_leaf_index(
            &self,
            _leaf_index: u64,
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

        fn get_tsa_anchor_covering(
            &self,
            _tree_size: u64,
        ) -> crate::error::ServerResult<Option<crate::traits::anchor::Anchor>> {
            Ok(None)
        }

        fn get_ots_anchor_covering(
            &self,
            _data_tree_index: u64,
        ) -> crate::error::ServerResult<Option<crate::traits::anchor::Anchor>> {
            Ok(None)
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

    fn create_mock_storage() -> Arc<dyn Storage> {
        Arc::new(MockStorage)
    }

    #[test]
    fn test_sequencer_new_creates_valid_instance() {
        let storage = create_mock_storage();
        let config = SequencerConfig::default();

        let (sequencer, _handle) = Sequencer::new(storage, config.clone());

        assert_eq!(sequencer.config.batch_size, config.batch_size);
        assert_eq!(sequencer.config.batch_timeout_ms, config.batch_timeout_ms);
        assert_eq!(sequencer.config.buffer_size, config.buffer_size);
    }

    #[test]
    fn test_sequencer_handle_is_cloneable() {
        let storage = create_mock_storage();
        let config = SequencerConfig::default();

        let (_sequencer, handle) = Sequencer::new(storage, config);
        let _handle2 = handle.clone();
    }

    #[test]
    fn test_sequencer_with_custom_config() {
        let storage = create_mock_storage();
        let config = SequencerConfig {
            batch_size: 50,
            batch_timeout_ms: 200,
            buffer_size: 500,
            ..Default::default()
        };

        let (sequencer, _handle) = Sequencer::new(storage, config);

        assert_eq!(sequencer.config.batch_size, 50);
        assert_eq!(sequencer.config.batch_timeout_ms, 200);
        assert_eq!(sequencer.config.buffer_size, 500);
    }

    #[test]
    fn test_mock_storage_anchor_methods() {
        use crate::traits::storage::Storage;
        let storage = MockStorage;
        assert!(Storage::get_tsa_anchor_covering(&storage, 0)
            .unwrap()
            .is_none());
        assert!(Storage::get_ots_anchor_covering(&storage, 0)
            .unwrap()
            .is_none());
    }
}
