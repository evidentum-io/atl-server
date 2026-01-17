//! OTS job implementation
//!
//! Unified job that runs both submit and poll phases.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;

use super::config::OtsJobConfig;
use crate::storage::index::IndexStore;
use crate::traits::Storage;

#[cfg(feature = "ots")]
use super::{poll, submit};

#[cfg(feature = "ots")]
use crate::anchoring::ots::OtsClient;

/// Unified OTS background job
///
/// Runs two phases on each tick:
/// 1. Submit: Find trees with status='pending_bitcoin' and bitcoin_anchor_id=NULL, submit to OTS
/// 2. Poll: Check pending OTS anchors for Bitcoin confirmation
pub struct OtsJob {
    index: Arc<Mutex<IndexStore>>,
    storage: Arc<dyn Storage>,
    #[cfg(feature = "ots")]
    ots_client: Arc<dyn OtsClient>,
    config: OtsJobConfig,
}

#[cfg(feature = "ots")]
impl OtsJob {
    /// Get configuration reference
    pub fn config(&self) -> &OtsJobConfig {
        &self.config
    }
}

impl OtsJob {
    #[cfg(feature = "ots")]
    pub fn new(
        index: Arc<Mutex<IndexStore>>,
        storage: Arc<dyn Storage>,
        ots_client: Arc<dyn OtsClient>,
        config: OtsJobConfig,
    ) -> Self {
        Self {
            index,
            storage,
            ots_client,
            config,
        }
    }

    /// Run OTS job as a background task
    ///
    /// Runs until shutdown signal is received via broadcast channel.
    pub async fn run(&self, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        let mut ticker = interval(Duration::from_secs(self.config.interval_secs));

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    #[cfg(feature = "ots")]
                    {
                        // Phase 1: Submit unanchored trees
                        if let Err(e) = submit::submit_unanchored_trees(
                            &self.index,
                            &self.storage,
                            &self.ots_client,
                            self.config.max_batch_size,
                        )
                        .await
                        {
                            tracing::error!(error = %e, "OTS submit phase failed");
                        }

                        // Phase 2: Poll pending anchors
                        if let Err(e) = poll::poll_pending_anchors(
                            &self.index,
                            &self.ots_client,
                            self.config.max_batch_size,
                        )
                        .await
                        {
                            tracing::error!(error = %e, "OTS poll phase failed");
                        }
                    }
                }
                _ = shutdown.recv() => {
                    tracing::info!("OTS job shutting down");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "ots")]
    use crate::anchoring::error::AnchorError;
    #[cfg(feature = "ots")]
    use crate::anchoring::ots::{OtsClient, UpgradeResult};
    #[cfg(feature = "ots")]
    use crate::error::{ServerError, StorageError};
    #[cfg(feature = "ots")]
    use crate::traits::{AppendParams, BatchResult, ConsistencyProof, Entry, InclusionProof, TreeHead, Storage};
    #[cfg(feature = "ots")]
    use async_trait::async_trait;
    #[cfg(feature = "ots")]
    use std::sync::atomic::{AtomicBool, Ordering};
    #[cfg(feature = "ots")]
    use tokio::time::{sleep, timeout};
    #[cfg(feature = "ots")]
    use uuid::Uuid;

    #[cfg(feature = "ots")]
    struct MockOtsClient;

    #[cfg(feature = "ots")]
    #[async_trait]
    impl OtsClient for MockOtsClient {
        async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
            Ok(("http://mock.calendar".to_string(), vec![0u8; 32]))
        }

        async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
            Ok(None)
        }
    }

    #[cfg(feature = "ots")]
    struct MockStorage;

    #[cfg(feature = "ots")]
    #[async_trait]
    impl Storage for MockStorage {
        async fn append_batch(&self, _params: Vec<AppendParams>) -> Result<BatchResult, StorageError> {
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
            Err(ServerError::Storage(StorageError::Database("not found".to_string())))
        }

        fn get_inclusion_proof(&self, _entry_id: &Uuid, _tree_size: Option<u64>) -> crate::error::ServerResult<InclusionProof> {
            unimplemented!()
        }

        fn get_consistency_proof(&self, _from_size: u64, _to_size: u64) -> crate::error::ServerResult<ConsistencyProof> {
            unimplemented!()
        }

        fn get_anchors(&self, _tree_size: u64) -> crate::error::ServerResult<Vec<crate::traits::anchor::Anchor>> {
            Ok(vec![])
        }

        fn get_latest_anchored_size(&self) -> crate::error::ServerResult<Option<u64>> {
            Ok(None)
        }

        fn get_anchors_covering(&self, _target_tree_size: u64, _limit: usize) -> crate::error::ServerResult<Vec<crate::traits::anchor::Anchor>> {
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

    #[cfg(feature = "ots")]
    fn create_test_index_store() -> IndexStore {
        use rusqlite::Connection;
        let conn = Connection::open_in_memory().expect("Failed to create in-memory DB");
        let store = IndexStore::from_connection(conn);
        store.initialize().expect("Failed to initialize schema");
        store
    }

    #[test]
    #[cfg(feature = "ots")]
    fn test_ots_job_new() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig::default();

        let job = OtsJob::new(index, storage, ots_client, config.clone());
        assert_eq!(job.config().interval_secs, config.interval_secs);
        assert_eq!(job.config().max_batch_size, config.max_batch_size);
    }

    #[tokio::test]
    #[cfg(feature = "ots")]
    async fn test_ots_job_shutdown_on_signal() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig {
            interval_secs: 3600, // Long interval
            max_batch_size: 100,
        };

        let job = OtsJob::new(index, storage, ots_client, config);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let job_handle = tokio::spawn(async move {
            job.run(shutdown_rx).await;
        });

        // Give the job a moment to start
        sleep(Duration::from_millis(50)).await;

        // Send shutdown signal
        shutdown_tx.send(()).unwrap();

        // Wait for job to complete with timeout
        let result = timeout(Duration::from_secs(1), job_handle).await;
        assert!(result.is_ok(), "Job should complete within timeout");
    }

    #[tokio::test]
    #[cfg(feature = "ots")]
    async fn test_ots_job_tick_execution() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);

        // Create a mock client that tracks if methods were called
        struct TrackingMockOtsClient {
            submit_called: Arc<AtomicBool>,
            upgrade_called: Arc<AtomicBool>,
        }

        #[async_trait]
        impl OtsClient for TrackingMockOtsClient {
            async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
                self.submit_called.store(true, Ordering::SeqCst);
                Ok(("http://mock.calendar".to_string(), vec![0u8; 32]))
            }

            async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
                self.upgrade_called.store(true, Ordering::SeqCst);
                Ok(None)
            }
        }

        let submit_called = Arc::new(AtomicBool::new(false));
        let upgrade_called = Arc::new(AtomicBool::new(false));

        let ots_client: Arc<dyn OtsClient> = Arc::new(TrackingMockOtsClient {
            submit_called: submit_called.clone(),
            upgrade_called: upgrade_called.clone(),
        });

        let config = OtsJobConfig {
            interval_secs: 1, // Short interval for testing
            max_batch_size: 100,
        };

        let job = OtsJob::new(index, storage, ots_client, config);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let job_handle = tokio::spawn(async move {
            job.run(shutdown_rx).await;
        });

        // Wait for at least one tick
        sleep(Duration::from_millis(1100)).await;

        // Send shutdown signal
        shutdown_tx.send(()).unwrap();

        // Wait for job to complete
        let _ = timeout(Duration::from_secs(2), job_handle).await;

        // Note: We can't reliably test if submit/upgrade were called because
        // they depend on database state. The job executes without panicking.
    }

    #[test]
    #[cfg(feature = "ots")]
    fn test_ots_job_config_access() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig {
            interval_secs: 123,
            max_batch_size: 456,
        };

        let job = OtsJob::new(index, storage, ots_client, config);
        assert_eq!(job.config().interval_secs, 123);
        assert_eq!(job.config().max_batch_size, 456);
    }

    #[tokio::test]
    #[cfg(feature = "ots")]
    async fn test_ots_job_immediate_shutdown() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig {
            interval_secs: 3600,
            max_batch_size: 100,
        };

        let job = OtsJob::new(index, storage, ots_client, config);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        // Send shutdown before job starts
        shutdown_tx.send(()).unwrap();

        let job_handle = tokio::spawn(async move {
            job.run(shutdown_rx).await;
        });

        let result = timeout(Duration::from_millis(500), job_handle).await;
        assert!(result.is_ok(), "Job should complete immediately");
    }

    #[tokio::test]
    #[cfg(feature = "ots")]
    async fn test_ots_job_multiple_ticks() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);

        let submit_count = Arc::new(AtomicBool::new(false));
        let upgrade_count = Arc::new(AtomicBool::new(false));

        struct CountingMockOtsClient {
            submit_count: Arc<AtomicBool>,
            upgrade_count: Arc<AtomicBool>,
        }

        #[async_trait]
        impl OtsClient for CountingMockOtsClient {
            async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
                self.submit_count.store(true, Ordering::SeqCst);
                Ok(("http://mock.calendar".to_string(), vec![0u8; 32]))
            }

            async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
                self.upgrade_count.store(true, Ordering::SeqCst);
                Ok(None)
            }
        }

        let ots_client: Arc<dyn OtsClient> = Arc::new(CountingMockOtsClient {
            submit_count: submit_count.clone(),
            upgrade_count: upgrade_count.clone(),
        });

        let config = OtsJobConfig {
            interval_secs: 1, // 1 second interval
            max_batch_size: 100,
        };

        let job = OtsJob::new(index, storage, ots_client, config);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let job_handle = tokio::spawn(async move {
            job.run(shutdown_rx).await;
        });

        // Wait for multiple ticks
        sleep(Duration::from_millis(2500)).await;

        shutdown_tx.send(()).unwrap();

        let result = timeout(Duration::from_secs(2), job_handle).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[cfg(feature = "ots")]
    async fn test_ots_job_with_custom_config() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);

        let config = OtsJobConfig {
            interval_secs: 5,
            max_batch_size: 25,
        };

        let job = OtsJob::new(index, storage, ots_client, config.clone());

        assert_eq!(job.config().interval_secs, config.interval_secs);
        assert_eq!(job.config().max_batch_size, config.max_batch_size);
    }

    #[tokio::test]
    #[cfg(feature = "ots")]
    async fn test_ots_job_shutdown_during_tick() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);

        struct SlowMockOtsClient;

        #[async_trait]
        impl OtsClient for SlowMockOtsClient {
            async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
                sleep(Duration::from_millis(100)).await;
                Ok(("http://mock.calendar".to_string(), vec![0u8; 32]))
            }

            async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
                sleep(Duration::from_millis(100)).await;
                Ok(None)
            }
        }

        let ots_client: Arc<dyn OtsClient> = Arc::new(SlowMockOtsClient);

        let config = OtsJobConfig {
            interval_secs: 1,
            max_batch_size: 100,
        };

        let job = OtsJob::new(index, storage, ots_client, config);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let job_handle = tokio::spawn(async move {
            job.run(shutdown_rx).await;
        });

        // Let one tick complete
        sleep(Duration::from_millis(1200)).await;

        // Shutdown during next tick
        shutdown_tx.send(()).unwrap();

        let result = timeout(Duration::from_secs(3), job_handle).await;
        assert!(result.is_ok(), "Job should shutdown cleanly");
    }

    #[tokio::test]
    #[cfg(feature = "ots")]
    async fn test_ots_job_error_handling() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);

        struct ErrorMockOtsClient;

        #[async_trait]
        impl OtsClient for ErrorMockOtsClient {
            async fn submit(&self, _hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
                Err(AnchorError::Network("test error".to_string()))
            }

            async fn upgrade(&self, _proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
                Err(AnchorError::Network("test error".to_string()))
            }
        }

        let ots_client: Arc<dyn OtsClient> = Arc::new(ErrorMockOtsClient);

        let config = OtsJobConfig {
            interval_secs: 1,
            max_batch_size: 100,
        };

        let job = OtsJob::new(index, storage, ots_client, config);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let job_handle = tokio::spawn(async move {
            job.run(shutdown_rx).await;
        });

        // Let it run through at least one tick with errors
        sleep(Duration::from_millis(1200)).await;

        shutdown_tx.send(()).unwrap();

        let result = timeout(Duration::from_secs(2), job_handle).await;
        assert!(result.is_ok(), "Job should handle errors gracefully");
    }

    #[test]
    #[cfg(feature = "ots")]
    fn test_ots_job_with_zero_interval() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig {
            interval_secs: 0,
            max_batch_size: 100,
        };

        let job = OtsJob::new(index, storage, ots_client, config);
        assert_eq!(job.config().interval_secs, 0);
    }

    #[test]
    #[cfg(feature = "ots")]
    fn test_ots_job_with_zero_batch_size() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig {
            interval_secs: 600,
            max_batch_size: 0,
        };

        let job = OtsJob::new(index, storage, ots_client, config);
        assert_eq!(job.config().max_batch_size, 0);
    }

    #[test]
    #[cfg(feature = "ots")]
    fn test_ots_job_with_large_batch_size() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig {
            interval_secs: 600,
            max_batch_size: usize::MAX,
        };

        let job = OtsJob::new(index, storage, ots_client, config);
        assert_eq!(job.config().max_batch_size, usize::MAX);
    }

    #[tokio::test]
    #[cfg(feature = "ots")]
    async fn test_ots_job_shutdown_receiver_dropped() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig {
            interval_secs: 1,
            max_batch_size: 100,
        };

        let job = OtsJob::new(index, storage, ots_client, config);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let _job_handle = tokio::spawn(async move {
            job.run(shutdown_rx).await;
        });

        // Drop sender immediately
        drop(shutdown_tx);

        // Job should continue running despite sender being dropped
        sleep(Duration::from_millis(100)).await;

        // Note: Without a way to signal, the job will run until timeout
        // This tests that dropping sender doesn't panic
    }

    #[tokio::test]
    #[cfg(feature = "ots")]
    async fn test_ots_job_config_reference_lifetime() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig {
            interval_secs: 999,
            max_batch_size: 888,
        };

        let job = OtsJob::new(index, storage, ots_client, config);

        // Access config multiple times
        assert_eq!(job.config().interval_secs, 999);
        assert_eq!(job.config().max_batch_size, 888);
        assert_eq!(job.config().interval_secs, 999);
        assert_eq!(job.config().max_batch_size, 888);
    }

    #[test]
    #[cfg(feature = "ots")]
    fn test_ots_job_construction_with_default_config() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig::default();

        let job = OtsJob::new(index, storage, ots_client, config);
        assert_eq!(job.config().interval_secs, 600);
        assert_eq!(job.config().max_batch_size, 100);
    }

    #[tokio::test]
    #[cfg(feature = "ots")]
    async fn test_ots_job_with_very_short_interval() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig {
            interval_secs: 1, // Very short interval
            max_batch_size: 100,
        };

        let job = OtsJob::new(index, storage, ots_client, config);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let job_handle = tokio::spawn(async move {
            job.run(shutdown_rx).await;
        });

        // Let it run for a bit
        sleep(Duration::from_millis(500)).await;

        shutdown_tx.send(()).unwrap();

        let result = timeout(Duration::from_secs(2), job_handle).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[cfg(feature = "ots")]
    async fn test_ots_job_concurrent_shutdown_signals() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig {
            interval_secs: 3600,
            max_batch_size: 100,
        };

        let job = OtsJob::new(index, storage, ots_client, config);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(10);

        let job_handle = tokio::spawn(async move {
            job.run(shutdown_rx).await;
        });

        sleep(Duration::from_millis(50)).await;

        // Send multiple shutdown signals
        let _ = shutdown_tx.send(());
        let _ = shutdown_tx.send(());
        let _ = shutdown_tx.send(());

        let result = timeout(Duration::from_secs(1), job_handle).await;
        assert!(result.is_ok());
    }

    #[test]
    #[cfg(feature = "ots")]
    fn test_ots_job_stores_components() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage);
        let ots_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient);
        let config = OtsJobConfig {
            interval_secs: 100,
            max_batch_size: 200,
        };

        let job = OtsJob::new(
            index.clone(),
            storage.clone(),
            ots_client.clone(),
            config.clone(),
        );

        // Verify config is stored correctly
        assert_eq!(job.config().interval_secs, config.interval_secs);
        assert_eq!(job.config().max_batch_size, config.max_batch_size);
    }
}
