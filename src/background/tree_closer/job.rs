// File: src/background/tree_closer/job.rs

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;

use super::config::TreeCloserConfig;
use super::logic;
use crate::storage::chain_index::ChainIndex;
use crate::storage::index::IndexStore;
use crate::traits::{Storage, TreeRotator};

/// Tree closer background job
///
/// Periodically checks if the active tree should be closed based on:
/// - Tree lifetime (elapsed since first entry, NOT creation)
/// - Tree has at least one entry (first_entry_at IS NOT NULL)
///
/// When closing a tree:
/// 1. Closes tree with status='pending_bitcoin'
/// 2. Atomically creates new active tree with genesis leaf (via TreeRotator)
/// 3. Genesis leaf is inserted into BOTH Slab and SQLite
/// 4. Records closed tree in Chain Index
/// 5. OTS anchoring will be handled by ots_job separately
pub struct TreeCloser {
    index: Arc<Mutex<IndexStore>>,
    storage: Arc<dyn Storage>,
    rotator: Arc<dyn TreeRotator>,
    chain_index: Arc<Mutex<ChainIndex>>,
    config: TreeCloserConfig,
}

impl TreeCloser {
    pub fn new(
        index: Arc<Mutex<IndexStore>>,
        storage: Arc<dyn Storage>,
        rotator: Arc<dyn TreeRotator>,
        chain_index: Arc<Mutex<ChainIndex>>,
        config: TreeCloserConfig,
    ) -> Self {
        Self {
            index,
            storage,
            rotator,
            chain_index,
            config,
        }
    }

    /// Run the tree closer as a background task
    ///
    /// Runs until shutdown signal is received via broadcast channel.
    pub async fn run(&self, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        let mut ticker = interval(Duration::from_secs(self.config.interval_secs));

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = logic::check_and_close_if_needed(
                        &self.index,
                        &self.storage,
                        &self.rotator,
                        &self.chain_index,
                        self.config.tree_lifetime_secs,
                    ).await {
                        tracing::error!(error = %e, "Tree close check failed");
                    }
                }
                _ = shutdown.recv() => {
                    tracing::info!("Tree closer shutting down");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StorageError;
    use crate::storage::chain_index::ChainIndex;
    use crate::storage::index::lifecycle::ClosedTreeMetadata;
    use crate::storage::index::lifecycle::TreeRotationResult;
    use crate::storage::index::queries::IndexStore;
    use crate::traits::{InclusionProof, Storage, TreeHead, TreeRotator};
    use std::path::Path;
    use tempfile::tempdir;

    /// Mock storage for testing
    struct MockStorage {
        tree_head: TreeHead,
        origin_id: [u8; 32],
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                tree_head: TreeHead {
                    tree_size: 100,
                    root_hash: [42u8; 32],
                    origin: [1u8; 32],
                },
                origin_id: [1u8; 32],
            }
        }
    }

    #[async_trait::async_trait]
    impl Storage for MockStorage {
        fn tree_head(&self) -> TreeHead {
            self.tree_head.clone()
        }

        fn origin_id(&self) -> [u8; 32] {
            self.origin_id
        }

        fn is_healthy(&self) -> bool {
            true
        }

        fn is_initialized(&self) -> bool {
            true
        }

        async fn append_batch(
            &self,
            _params: Vec<crate::traits::AppendParams>,
        ) -> Result<crate::traits::BatchResult, StorageError> {
            unimplemented!("Not needed for tree closer tests")
        }

        async fn flush(&self) -> Result<(), StorageError> {
            unimplemented!("Not needed for tree closer tests")
        }

        fn get_entry(&self, _id: &uuid::Uuid) -> crate::error::ServerResult<crate::traits::Entry> {
            unimplemented!("Not needed for tree closer tests")
        }

        fn get_inclusion_proof(
            &self,
            _entry_id: &uuid::Uuid,
            _tree_size: Option<u64>,
        ) -> crate::error::ServerResult<crate::traits::InclusionProof> {
            unimplemented!("Not needed for tree closer tests")
        }

        #[cfg(not(tarpaulin_include))]
        fn get_inclusion_proof_by_leaf_index(
            &self,
            _leaf_index: u64,
            _tree_size: Option<u64>,
        ) -> crate::error::ServerResult<InclusionProof> {
            unimplemented!()
        }

        fn get_consistency_proof(
            &self,
            _from_size: u64,
            _to_size: u64,
        ) -> crate::error::ServerResult<crate::traits::ConsistencyProof> {
            unimplemented!("Not needed for tree closer tests")
        }

        fn get_anchors(
            &self,
            _tree_size: u64,
        ) -> crate::error::ServerResult<Vec<crate::traits::Anchor>> {
            unimplemented!("Not needed for tree closer tests")
        }

        fn get_latest_anchored_size(&self) -> crate::error::ServerResult<Option<u64>> {
            unimplemented!("Not needed for tree closer tests")
        }

        fn get_tsa_anchor_covering(
            &self,
            _tree_size: u64,
        ) -> crate::error::ServerResult<Option<crate::traits::Anchor>> {
            Ok(None)
        }

        fn get_ots_anchor_covering(
            &self,
            _data_tree_index: u64,
        ) -> crate::error::ServerResult<Option<crate::traits::Anchor>> {
            Ok(None)
        }

        fn get_root_at_size(&self, _tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
            unimplemented!("Not needed for tree closer tests")
        }

        fn get_super_root(&self, _super_tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
            unimplemented!("Not needed for tree closer tests")
        }
    }

    /// Mock tree rotator for testing
    struct MockTreeRotator {
        should_fail: bool,
    }

    impl MockTreeRotator {
        fn new() -> Self {
            Self { should_fail: false }
        }

        #[allow(dead_code)]
        fn with_failure() -> Self {
            Self { should_fail: true }
        }
    }

    #[async_trait::async_trait]
    impl TreeRotator for MockTreeRotator {
        async fn rotate_tree(
            &self,
            origin_id: &[u8; 32],
            end_size: u64,
            root_hash: &[u8; 32],
        ) -> Result<TreeRotationResult, StorageError> {
            if self.should_fail {
                return Err(StorageError::Database("mock rotation failure".into()));
            }

            Ok(TreeRotationResult {
                closed_tree_id: 1,
                new_tree_id: 2,
                data_tree_index: 0,
                super_root: [42u8; 32],
                closed_tree_metadata: ClosedTreeMetadata {
                    tree_id: 1,
                    origin_id: *origin_id,
                    root_hash: *root_hash,
                    tree_size: end_size,
                    closed_at: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                    data_tree_index: 0,
                },
                new_tree_head: TreeHead {
                    tree_size: end_size,
                    root_hash: [1u8; 32],
                    origin: *origin_id,
                },
            })
        }
    }

    fn setup_index_with_active_tree(path: &Path) -> IndexStore {
        let db_path = path.join("atl.db");
        let index = IndexStore::open(&db_path).unwrap();
        index.initialize().unwrap();
        let origin_id = [1u8; 32];
        index.create_active_tree(&origin_id, 0).unwrap();
        index
    }

    #[tokio::test]
    async fn test_tree_closer_new() {
        let temp_dir = tempdir().unwrap();
        let index = setup_index_with_active_tree(temp_dir.path());
        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());
        let config = TreeCloserConfig::default();

        let closer = TreeCloser::new(
            Arc::new(Mutex::new(index)),
            storage,
            rotator,
            Arc::new(Mutex::new(chain_index)),
            config,
        );

        assert_eq!(closer.config.interval_secs, 60);
        assert_eq!(closer.config.tree_lifetime_secs, 3600);
    }

    #[tokio::test]
    async fn test_tree_closer_shutdown_signal() {
        let temp_dir = tempdir().unwrap();
        let index = setup_index_with_active_tree(temp_dir.path());
        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        // Use very short interval for test
        let config = TreeCloserConfig {
            interval_secs: 3600, // Long interval - won't tick during test
            tree_lifetime_secs: 3600,
        };

        let closer = TreeCloser::new(
            Arc::new(Mutex::new(index)),
            storage,
            rotator,
            Arc::new(Mutex::new(chain_index)),
            config,
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        // Run closer in background
        let closer_handle = tokio::spawn(async move {
            closer.run(shutdown_rx).await;
        });

        // Give it a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send shutdown signal
        shutdown_tx.send(()).unwrap();

        // Wait for closer to finish
        let result = tokio::time::timeout(Duration::from_secs(1), closer_handle).await;
        assert!(result.is_ok(), "TreeCloser should shutdown gracefully");
    }

    #[tokio::test]
    async fn test_tree_closer_with_custom_config() {
        let temp_dir = tempdir().unwrap();
        let index = setup_index_with_active_tree(temp_dir.path());
        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let config = TreeCloserConfig {
            interval_secs: 30,
            tree_lifetime_secs: 1800,
        };

        let closer = TreeCloser::new(
            Arc::new(Mutex::new(index)),
            storage,
            rotator,
            Arc::new(Mutex::new(chain_index)),
            config.clone(),
        );

        assert_eq!(closer.config.interval_secs, 30);
        assert_eq!(closer.config.tree_lifetime_secs, 1800);
    }

    #[tokio::test]
    async fn test_tree_closer_fields_initialization() {
        let temp_dir = tempdir().unwrap();
        let index = setup_index_with_active_tree(temp_dir.path());
        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());
        let config = TreeCloserConfig::default();

        let closer = TreeCloser::new(
            Arc::new(Mutex::new(index)),
            storage.clone(),
            rotator.clone(),
            Arc::new(Mutex::new(chain_index)),
            config,
        );

        // Verify config was properly stored
        assert_eq!(closer.config.interval_secs, 60);
        assert_eq!(closer.config.tree_lifetime_secs, 3600);
    }

    #[tokio::test]
    async fn test_tree_closer_with_zero_interval() {
        let temp_dir = tempdir().unwrap();
        let index = setup_index_with_active_tree(temp_dir.path());
        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let config = TreeCloserConfig {
            interval_secs: 0, // Edge case: zero interval
            tree_lifetime_secs: 3600,
        };

        let closer = TreeCloser::new(
            Arc::new(Mutex::new(index)),
            storage,
            rotator,
            Arc::new(Mutex::new(chain_index)),
            config,
        );

        assert_eq!(closer.config.interval_secs, 0);
    }

    #[tokio::test]
    async fn test_tree_closer_shutdown_immediate() {
        let temp_dir = tempdir().unwrap();
        let index = setup_index_with_active_tree(temp_dir.path());
        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let config = TreeCloserConfig {
            interval_secs: 3600,
            tree_lifetime_secs: 3600,
        };

        let closer = TreeCloser::new(
            Arc::new(Mutex::new(index)),
            storage,
            rotator,
            Arc::new(Mutex::new(chain_index)),
            config,
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        // Send shutdown immediately before run starts processing
        let closer_handle = tokio::spawn(async move {
            closer.run(shutdown_rx).await;
        });

        // Send shutdown right away
        shutdown_tx.send(()).unwrap();

        // Should complete quickly
        let result = tokio::time::timeout(Duration::from_millis(500), closer_handle).await;
        assert!(result.is_ok(), "TreeCloser should shutdown immediately");
    }

    #[tokio::test]
    async fn test_tree_closer_multiple_shutdown_signals() {
        let temp_dir = tempdir().unwrap();
        let index = setup_index_with_active_tree(temp_dir.path());
        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let config = TreeCloserConfig {
            interval_secs: 3600,
            tree_lifetime_secs: 3600,
        };

        let closer = TreeCloser::new(
            Arc::new(Mutex::new(index)),
            storage,
            rotator,
            Arc::new(Mutex::new(chain_index)),
            config,
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(2);

        let closer_handle = tokio::spawn(async move {
            closer.run(shutdown_rx).await;
        });

        // Send multiple shutdown signals (only first one matters)
        shutdown_tx.send(()).unwrap();
        let _ = shutdown_tx.send(()); // This might fail if receiver already closed

        let result = tokio::time::timeout(Duration::from_millis(500), closer_handle).await;
        assert!(
            result.is_ok(),
            "TreeCloser should handle multiple shutdown signals"
        );
    }

    #[tokio::test]
    async fn test_tree_closer_with_short_interval() {
        let temp_dir = tempdir().unwrap();
        let index = setup_index_with_active_tree(temp_dir.path());
        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        // Very short interval for testing
        let config = TreeCloserConfig {
            interval_secs: 1, // 1 second interval
            tree_lifetime_secs: 3600,
        };

        let closer = TreeCloser::new(
            Arc::new(Mutex::new(index)),
            storage,
            rotator,
            Arc::new(Mutex::new(chain_index)),
            config,
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let closer_handle = tokio::spawn(async move {
            closer.run(shutdown_rx).await;
        });

        // Wait for at least one tick to happen
        tokio::time::sleep(Duration::from_millis(1100)).await;

        // Send shutdown
        shutdown_tx.send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), closer_handle).await;
        assert!(result.is_ok(), "TreeCloser should shutdown after ticks");
    }

    #[tokio::test]
    async fn test_mock_storage_tree_head() {
        let storage = MockStorage::new();
        let tree_head = storage.tree_head();

        assert_eq!(tree_head.tree_size, 100);
        assert_eq!(tree_head.root_hash, [42u8; 32]);
        assert_eq!(tree_head.origin, [1u8; 32]);
    }

    #[tokio::test]
    async fn test_mock_storage_origin_id() {
        let storage = MockStorage::new();
        let origin_id = storage.origin_id();

        assert_eq!(origin_id, [1u8; 32]);
    }

    #[tokio::test]
    async fn test_mock_storage_is_healthy() {
        let storage = MockStorage::new();
        assert!(storage.is_healthy());
    }

    #[tokio::test]
    async fn test_mock_storage_is_initialized() {
        let storage = MockStorage::new();
        assert!(storage.is_initialized());
    }

    #[tokio::test]
    async fn test_mock_tree_rotator_success() {
        let rotator = MockTreeRotator::new();
        let origin_id = [1u8; 32];
        let end_size = 100;
        let root_hash = [42u8; 32];

        let result = rotator.rotate_tree(&origin_id, end_size, &root_hash).await;

        assert!(result.is_ok());
        let rotation_result = result.unwrap();
        assert_eq!(rotation_result.closed_tree_id, 1);
        assert_eq!(rotation_result.new_tree_id, 2);
        assert_eq!(rotation_result.data_tree_index, 0);
        assert_eq!(rotation_result.super_root, [42u8; 32]);
    }

    #[tokio::test]
    async fn test_mock_tree_rotator_failure() {
        let rotator = MockTreeRotator::with_failure();
        let origin_id = [1u8; 32];
        let end_size = 100;
        let root_hash = [42u8; 32];

        let result = rotator.rotate_tree(&origin_id, end_size, &root_hash).await;

        assert!(result.is_err());
        if let Err(StorageError::Database(msg)) = result {
            assert_eq!(msg, "mock rotation failure");
        } else {
            panic!("Expected Database error");
        }
    }

    #[tokio::test]
    async fn test_tree_closer_config_values_preserved() {
        let temp_dir = tempdir().unwrap();
        let index = setup_index_with_active_tree(temp_dir.path());
        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let config = TreeCloserConfig {
            interval_secs: 123,
            tree_lifetime_secs: 456,
        };

        let closer = TreeCloser::new(
            Arc::new(Mutex::new(index)),
            storage,
            rotator,
            Arc::new(Mutex::new(chain_index)),
            config,
        );

        // Verify exact values are preserved
        assert_eq!(closer.config.interval_secs, 123);
        assert_eq!(closer.config.tree_lifetime_secs, 456);
    }

    #[tokio::test]
    async fn test_setup_index_with_active_tree_helper() {
        let temp_dir = tempdir().unwrap();
        let index = setup_index_with_active_tree(temp_dir.path());

        // Verify index was properly initialized
        let active_tree = index.get_active_tree().unwrap();
        assert!(active_tree.is_some());

        let tree = active_tree.unwrap();
        assert_eq!(tree.origin_id, [1u8; 32]);
        assert_eq!(tree.start_size, 0);
    }

    #[tokio::test]
    async fn test_tree_closer_run_with_failing_logic() {
        let temp_dir = tempdir().unwrap();
        let index = setup_index_with_active_tree(temp_dir.path());
        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::with_failure());

        // Set first_entry_at to 2 hours ago to trigger closure attempt
        {
            let conn = index.connection();
            let two_hours_ago =
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - (2 * 3600 * 1_000_000_000);
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![two_hours_ago],
            )
            .unwrap();
        }

        let config = TreeCloserConfig {
            interval_secs: 1, // Fast interval
            tree_lifetime_secs: 3600,
        };

        let closer = TreeCloser::new(
            Arc::new(Mutex::new(index)),
            storage,
            rotator,
            Arc::new(Mutex::new(chain_index)),
            config,
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let closer_handle = tokio::spawn(async move {
            closer.run(shutdown_rx).await;
        });

        // Wait for a tick to happen (which will fail due to mock rotator failure)
        tokio::time::sleep(Duration::from_millis(1100)).await;

        // Send shutdown
        shutdown_tx.send(()).unwrap();

        // Should still shutdown gracefully despite errors in logic
        let result = tokio::time::timeout(Duration::from_secs(2), closer_handle).await;
        assert!(
            result.is_ok(),
            "TreeCloser should shutdown gracefully even with errors"
        );
    }

    #[test]
    fn test_mock_storage_anchor_methods() {
        use crate::traits::storage::Storage;
        let storage = MockStorage::new();
        assert!(Storage::get_tsa_anchor_covering(&storage, 0)
            .unwrap()
            .is_none());
        assert!(Storage::get_ots_anchor_covering(&storage, 0)
            .unwrap()
            .is_none());
    }
}
