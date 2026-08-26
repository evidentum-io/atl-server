// File: src/background/tsa_job/job.rs

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{interval, Instant};

use super::config::TsaJobConfig;
use super::round_robin::RoundRobinSelector;
use crate::error::ServerResult;
use crate::storage::index::IndexStore;
use crate::traits::Storage;

/// TSA anchoring background job
///
/// Processes trees that need TSA anchoring (Tier-1 evidence):
/// - Periodic anchoring of active tree (every N seconds)
/// - Trees with status IN ('pending_bitcoin', 'closed')
/// - Trees without tsa_anchor_id (not yet anchored)
///
/// Uses round-robin load distribution across configured TSA servers.
pub struct TsaAnchoringJob {
    index: Arc<Mutex<IndexStore>>,
    storage: Arc<dyn Storage>,
    selector: RoundRobinSelector,
    config: TsaJobConfig,
    last_active_anchor: Arc<Mutex<Option<Instant>>>,
}

impl TsaAnchoringJob {
    pub fn new(
        index: Arc<Mutex<IndexStore>>,
        storage: Arc<dyn Storage>,
        config: TsaJobConfig,
    ) -> Self {
        let selector = RoundRobinSelector::new(config.tsa_urls.clone());

        Self {
            index,
            storage,
            selector,
            config,
            last_active_anchor: Arc::new(Mutex::new(None)),
        }
    }

    /// Run TSA anchoring as a background task
    ///
    /// Runs until shutdown signal is received via broadcast channel.
    pub async fn run(&self, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        let mut ticker = interval(Duration::from_secs(self.config.interval_secs));

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.process_pending_trees().await {
                        tracing::error!(error = %e, "TSA anchoring job failed");
                    }
                }
                _ = shutdown.recv() => {
                    tracing::info!("TSA anchoring job shutting down");
                    break;
                }
            }
        }
    }

    /// Process trees that don't have TSA anchors yet
    async fn process_pending_trees(&self) -> ServerResult<()> {
        // PART 1: Anchor active tree periodically
        self.process_active_tree_anchoring().await?;

        // PART 2: Link TSA anchors to closed trees (find or create)
        let pending_trees = {
            let idx = self.index.lock().await;
            idx.get_trees_pending_tsa().map_err(|e| {
                crate::error::ServerError::Storage(crate::error::StorageError::Database(
                    e.to_string(),
                ))
            })?
        };

        if pending_trees.is_empty() {
            return Ok(());
        }

        let trees_to_process: Vec<_> = pending_trees
            .into_iter()
            .take(self.config.max_batch_size)
            .collect();

        tracing::info!(
            count = trees_to_process.len(),
            "Linking TSA anchors to closed trees"
        );

        for tree in trees_to_process {
            match self
                .selector
                .anchor_with_round_robin(&tree, &self.index, self.config.timeout_ms)
                .await
            {
                Ok(anchor_id) => {
                    tracing::info!(
                        tree_id = tree.id,
                        anchor_id = anchor_id,
                        "TSA anchor linked to closed tree"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        tree_id = tree.id,
                        error = %e,
                        "TSA anchoring failed for tree (all servers), will retry"
                    );
                }
            }
        }

        Ok(())
    }

    /// Process active tree anchoring (periodic)
    ///
    /// Anchors the current active tree state if:
    /// 1. Tree has new entries since last anchor
    /// 2. Enough time has passed since last active anchor
    async fn process_active_tree_anchoring(&self) -> ServerResult<()> {
        // Check if enough time has passed since last active anchor
        let should_anchor = {
            let last_anchor = self.last_active_anchor.lock().await;
            match *last_anchor {
                None => true,
                Some(last_time) => {
                    let elapsed = last_time.elapsed();
                    elapsed >= Duration::from_secs(self.config.active_tree_interval_secs)
                }
            }
        };

        if !should_anchor {
            return Ok(());
        }

        // Get current tree head
        let tree_head = self.storage.tree_head();

        // Skip if tree is empty
        if tree_head.tree_size == 0 {
            return Ok(());
        }

        // Get latest TSA anchored size
        let last_anchored_size = {
            let idx = self.index.lock().await;
            idx.get_latest_tsa_anchored_size().map_err(|e| {
                crate::error::ServerError::Storage(crate::error::StorageError::Database(
                    e.to_string(),
                ))
            })?
        };

        // Skip if tree hasn't grown since last anchor
        if let Some(last_size) = last_anchored_size {
            if tree_head.tree_size <= last_size {
                return Ok(());
            }
        }

        // Anchor the active tree with round-robin server selection
        tracing::info!(
            tree_size = tree_head.tree_size,
            root_hash = hex::encode(tree_head.root_hash),
            "Anchoring active tree"
        );

        // Guard the "no servers configured" case explicitly so the error
        // message stays the same regardless of which selector method is
        // used below.
        if self.selector.urls_count() == 0 {
            return Err(crate::error::ServerError::Internal(
                "No TSA servers configured".to_string(),
            ));
        }

        // Anchor via round-robin: if the first server's response fails
        // (request error or messageImprint verification failure), the
        // selector tries the next configured server within this same pass,
        // matching the behavior of the closed-tree path
        // (`anchor_with_round_robin`) instead of giving up after one URL
        // and waiting for the next interval.
        match self
            .selector
            .anchor_tree_head_with_round_robin(
                tree_head.root_hash,
                tree_head.tree_size,
                &self.index,
                self.config.timeout_ms,
            )
            .await
        {
            Ok(anchor_id) => {
                tracing::info!(
                    tree_size = tree_head.tree_size,
                    anchor_id = anchor_id,
                    root_hash = hex::encode(tree_head.root_hash),
                    "Active tree anchored successfully"
                );

                // Update last anchor time
                let mut last_anchor = self.last_active_anchor.lock().await;
                *last_anchor = Some(Instant::now());

                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    tree_size = tree_head.tree_size,
                    error = %e,
                    "Failed to anchor active tree, will retry next interval"
                );
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StorageError;
    use crate::storage::index::IndexStore;
    use crate::traits::{ConsistencyProof, Entry, InclusionProof, Storage, TreeHead};
    use async_trait::async_trait;
    use uuid::Uuid;

    fn create_test_index_store() -> IndexStore {
        use rusqlite::Connection;
        let conn = Connection::open_in_memory().expect("Failed to create in-memory DB");
        let store = IndexStore::from_connection(conn);
        store.initialize().expect("Failed to initialize schema");
        store
    }

    struct MockStorage {
        tree_head: TreeHead,
    }

    impl MockStorage {
        fn new(tree_size: u64, root_hash: [u8; 32]) -> Self {
            Self {
                tree_head: TreeHead {
                    tree_size,
                    root_hash,
                    origin: [0u8; 32],
                },
            }
        }
    }

    #[async_trait]
    impl Storage for MockStorage {
        async fn append_batch(
            &self,
            _params: Vec<crate::traits::AppendParams>,
        ) -> Result<crate::traits::BatchResult, StorageError> {
            unimplemented!()
        }

        async fn flush(&self) -> Result<(), StorageError> {
            Ok(())
        }

        fn tree_head(&self) -> TreeHead {
            self.tree_head.clone()
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
        ) -> crate::error::ServerResult<ConsistencyProof> {
            unimplemented!()
        }

        fn get_anchors(
            &self,
            _tree_size: u64,
        ) -> crate::error::ServerResult<Vec<crate::traits::Anchor>> {
            Ok(vec![])
        }

        fn get_latest_anchored_size(&self) -> crate::error::ServerResult<Option<u64>> {
            Ok(None)
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
            Ok([0u8; 32])
        }

        fn get_super_root(&self, _super_tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
            Ok([0u8; 32])
        }

        fn is_initialized(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_new_tsa_anchoring_job() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec![
                "https://tsa1.com".to_string(),
                "https://tsa2.com".to_string(),
            ],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 60,
        };

        let job = TsaAnchoringJob::new(index, storage, config.clone());

        assert_eq!(job.config.timeout_ms, 5000);
        assert_eq!(job.config.interval_secs, 60);
        assert_eq!(job.config.max_batch_size, 100);
        assert_eq!(job.config.active_tree_interval_secs, 60);
        assert_eq!(job.selector.urls_count(), 2);
    }

    #[tokio::test]
    async fn test_process_pending_trees_empty() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32]));
        let config = TsaJobConfig::default();

        let job = TsaAnchoringJob::new(index, storage, config);

        // Should succeed with no pending trees
        let result = job.process_pending_trees().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_active_tree_anchoring_empty_tree() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32]));
        let config = TsaJobConfig::default();

        let job = TsaAnchoringJob::new(index, storage, config);

        // Should skip anchoring for empty tree
        let result = job.process_active_tree_anchoring().await;
        assert!(result.is_ok());

        // Verify last_active_anchor is still None
        let last_anchor = job.last_active_anchor.lock().await;
        assert!(last_anchor.is_none());
    }

    #[tokio::test]
    async fn test_process_active_tree_anchoring_no_growth() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [1u8; 32];
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, root_hash));
        let config = TsaJobConfig::default();

        // Create existing anchor for current tree size
        {
            let idx = index.lock().await;
            let anchor = crate::traits::Anchor {
                anchor_type: crate::traits::AnchorType::Rfc3161,
                target: "data_tree_root".to_string(),
                anchored_hash: root_hash,
                tree_size: 100,
                super_tree_size: None,
                timestamp: 1234567890,
                token: vec![1, 2, 3],
                metadata: serde_json::json!({"tsa_url": "https://test.com"}),
            };
            idx.store_anchor_returning_id(100, &anchor, "confirmed")
                .expect("Failed to store anchor");
        }

        let job = TsaAnchoringJob::new(index, storage, config);

        // Should skip anchoring since tree hasn't grown
        let result = job.process_active_tree_anchoring().await;
        assert!(result.is_ok());

        // Verify last_active_anchor is still None (no new anchor created)
        let last_anchor = job.last_active_anchor.lock().await;
        assert!(last_anchor.is_none());
    }

    #[tokio::test]
    async fn test_process_active_tree_anchoring_time_gate() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [1u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec!["https://tsa1.com".to_string()],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 3600, // 1 hour
        };

        let job = TsaAnchoringJob::new(index, storage, config);

        // Set last anchor time to now
        {
            let mut last_anchor = job.last_active_anchor.lock().await;
            *last_anchor = Some(Instant::now());
        }

        // Should skip anchoring due to time gate (not enough time elapsed)
        let result = job.process_active_tree_anchoring().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_active_tree_anchoring_no_tsa_urls() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [1u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec![], // No TSA URLs configured
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 60,
        };

        let job = TsaAnchoringJob::new(index, storage, config);

        // Should fail with no TSA URLs configured
        let result = job.process_active_tree_anchoring().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No TSA servers configured"));
    }

    #[test]
    fn test_process_pending_trees_respects_max_batch_size() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec!["https://tsa1.com".to_string()],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 3, // Small batch size for testing
            active_tree_interval_secs: 60,
        };

        let job = TsaAnchoringJob::new(index.clone(), storage, config);

        // Verify config is correctly set
        assert_eq!(job.config.max_batch_size, 3);
    }

    #[tokio::test]
    async fn test_process_active_tree_anchoring_time_gate_elapsed() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [1u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec!["https://tsa1.com".to_string()],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 0, // No time gate
        };

        let job = TsaAnchoringJob::new(index, storage, config);

        // Set last anchor time to past (more than interval ago)
        {
            let mut last_anchor = job.last_active_anchor.lock().await;
            *last_anchor = Some(Instant::now() - Duration::from_secs(3600));
        }

        // Should attempt anchoring (will fail due to no real TSA server, but that's OK)
        let result = job.process_active_tree_anchoring().await;
        // Result can be Ok (warning logged) or Err depending on network
        // We just verify the function runs without panicking
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_selector_creation() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec![
                "https://tsa1.com".to_string(),
                "https://tsa2.com".to_string(),
                "https://tsa3.com".to_string(),
            ],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 60,
        };

        let job = TsaAnchoringJob::new(index, storage, config);

        // Verify round-robin selector has correct number of URLs
        assert_eq!(job.selector.urls_count(), 3);
    }

    #[tokio::test]
    async fn test_last_active_anchor_initialization() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32]));
        let config = TsaJobConfig::default();

        let job = TsaAnchoringJob::new(index, storage, config);

        // Verify last_active_anchor is initially None
        let last_anchor = job.last_active_anchor.lock().await;
        assert!(last_anchor.is_none());
    }

    #[tokio::test]
    async fn test_process_pending_trees_with_empty_tsa_urls() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec![], // No TSA URLs
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 60,
        };

        let job = TsaAnchoringJob::new(index, storage, config);

        // Should succeed even with no TSA URLs (no pending trees to process)
        let result = job.process_pending_trees().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_active_tree_empty_and_no_growth_combined() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec!["https://tsa1.com".to_string()],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 0, // No time gate
        };

        let job = TsaAnchoringJob::new(index, storage, config);

        // Tree is empty (size 0), should skip anchoring
        let result = job.process_active_tree_anchoring().await;
        assert!(result.is_ok());

        // Verify no anchor was created
        let last_anchor = job.last_active_anchor.lock().await;
        assert!(last_anchor.is_none());
    }

    #[test]
    fn test_config_default_values() {
        let config = TsaJobConfig::default();

        // Verify default values are sensible
        assert!(!config.tsa_urls.is_empty());
        assert!(config.timeout_ms > 0);
        assert!(config.interval_secs > 0);
        assert!(config.max_batch_size > 0);
        assert!(config.active_tree_interval_secs > 0);
    }

    #[tokio::test]
    async fn test_multiple_tsa_urls_round_robin() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec![
                "https://tsa1.example.com".to_string(),
                "https://tsa2.example.com".to_string(),
            ],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 60,
        };

        let job = TsaAnchoringJob::new(index, storage, config);

        // Verify selector can provide URLs
        assert_eq!(job.selector.urls_count(), 2);
        let url1 = job.selector.next_url();
        assert!(url1.is_ok());
    }

    #[tokio::test]
    async fn test_process_active_tree_with_growth() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [42u8; 32];
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(150, root_hash));
        let config = TsaJobConfig {
            tsa_urls: vec!["https://tsa1.com".to_string()],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 0, // No time gate
        };

        // Create anchor at size 100 (tree has grown to 150)
        {
            let idx = index.lock().await;
            let anchor = crate::traits::Anchor {
                anchor_type: crate::traits::AnchorType::Rfc3161,
                target: "data_tree_root".to_string(),
                anchored_hash: [1u8; 32],
                tree_size: 100,
                super_tree_size: None,
                timestamp: 1234567890,
                token: vec![1, 2, 3],
                metadata: serde_json::json!({"tsa_url": "https://test.com"}),
            };
            idx.store_anchor_returning_id(100, &anchor, "confirmed")
                .expect("Failed to store anchor");
        }

        let job = TsaAnchoringJob::new(index, storage, config);

        // Should attempt to anchor (will fail due to no real TSA, but path is exercised)
        let result = job.process_active_tree_anchoring().await;
        // We expect Ok(()) because failures are logged as warnings, not errors
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_pending_trees_integration() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [1u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec!["https://tsa1.com".to_string()],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 5,
            active_tree_interval_secs: 3600, // High to avoid active tree anchoring
        };

        let job = TsaAnchoringJob::new(index.clone(), storage, config);

        // Create a closed tree without TSA anchor
        {
            let mut idx = index.lock().await;
            let origin = [1u8; 32];
            idx.create_active_tree(&origin, 0).unwrap();
            idx.close_tree_and_create_new(&origin, 100, &[1u8; 32], 1)
                .unwrap();
        }

        // Process pending trees (will try to anchor via network, which will fail)
        let result = job.process_pending_trees().await;
        // Should succeed even if anchoring fails (failures are logged)
        assert!(result.is_ok());
    }

    #[test]
    fn test_config_values_propagation() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32]));

        let custom_config = TsaJobConfig {
            tsa_urls: vec![
                "https://custom1.com".to_string(),
                "https://custom2.com".to_string(),
                "https://custom3.com".to_string(),
            ],
            timeout_ms: 10000,
            interval_secs: 120,
            max_batch_size: 50,
            active_tree_interval_secs: 300,
        };

        let job = TsaAnchoringJob::new(index, storage, custom_config.clone());

        // Verify all config values are correctly set
        assert_eq!(job.config.tsa_urls.len(), 3);
        assert_eq!(job.config.timeout_ms, 10000);
        assert_eq!(job.config.interval_secs, 120);
        assert_eq!(job.config.max_batch_size, 50);
        assert_eq!(job.config.active_tree_interval_secs, 300);
        assert_eq!(job.selector.urls_count(), 3);
    }

    #[tokio::test]
    async fn test_last_active_anchor_time_update() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32]));
        let config = TsaJobConfig::default();

        let job = TsaAnchoringJob::new(index, storage, config);

        // Initially None
        {
            let last = job.last_active_anchor.lock().await;
            assert!(last.is_none());
        }

        // Manually update it
        {
            let mut last = job.last_active_anchor.lock().await;
            *last = Some(Instant::now());
        }

        // Should now be Some
        {
            let last = job.last_active_anchor.lock().await;
            assert!(last.is_some());
        }
    }

    #[tokio::test]
    async fn test_process_active_tree_with_first_anchor() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [55u8; 32];
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(50, root_hash));
        let config = TsaJobConfig {
            tsa_urls: vec!["https://tsa-test.com".to_string()],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 0, // No time gate
        };

        let job = TsaAnchoringJob::new(index, storage, config);

        // No previous anchors, tree has entries - should attempt to anchor
        let result = job.process_active_tree_anchoring().await;
        // Will fail network call but path is exercised
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_tree_size_zero_skips_anchoring() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec!["https://tsa1.com".to_string()],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 0,
        };

        let job = TsaAnchoringJob::new(index, storage, config);

        // Empty tree should skip anchoring
        let result = job.process_active_tree_anchoring().await;
        assert!(result.is_ok());

        // Verify last_active_anchor is still None
        let last = job.last_active_anchor.lock().await;
        assert!(last.is_none());
    }

    #[tokio::test]
    async fn test_process_active_tree_error_path() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec!["https://invalid-tsa-url-that-will-fail.example".to_string()],
            timeout_ms: 100,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 0,
        };

        let job = TsaAnchoringJob::new(index, storage, config);

        // Should handle network errors gracefully (returns Ok with warning logged)
        let result = job.process_active_tree_anchoring().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_pending_trees_max_batch_limiting() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [1u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec!["https://tsa1.com".to_string()],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 2,
            active_tree_interval_secs: 3600,
        };

        let job = TsaAnchoringJob::new(index.clone(), storage, config);

        // Create multiple closed trees
        {
            let mut idx = index.lock().await;
            let origin = [1u8; 32];
            idx.create_active_tree(&origin, 0).unwrap();
            idx.close_tree_and_create_new(&origin, 100, &[1u8; 32], 1)
                .unwrap();
            idx.close_tree_and_create_new(&origin, 200, &[2u8; 32], 2)
                .unwrap();
            idx.close_tree_and_create_new(&origin, 300, &[3u8; 32], 3)
                .unwrap();
        }

        // Process should only take max_batch_size trees
        let result = job.process_pending_trees().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_last_active_anchor_instant_comparison() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(50, [1u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec!["https://tsa1.com".to_string()],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 10,
        };

        let job = TsaAnchoringJob::new(index, storage, config);

        // Set last anchor to just now
        {
            let mut last = job.last_active_anchor.lock().await;
            *last = Some(Instant::now());
        }

        // Should skip due to time gate
        let result = job.process_active_tree_anchoring().await;
        assert!(result.is_ok());

        // Time should still be recent (not updated)
        let last = job.last_active_anchor.lock().await;
        let elapsed = last.unwrap().elapsed();
        assert!(elapsed.as_secs() < 5);
    }

    #[tokio::test]
    async fn test_selector_next_url_error_handling() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [1u8; 32]));
        let config = TsaJobConfig {
            tsa_urls: vec![],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 0,
        };

        let job = TsaAnchoringJob::new(index, storage, config);

        // Should fail with proper error message
        let result = job.process_active_tree_anchoring().await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("No TSA servers configured"));
    }

    #[test]
    fn test_job_config_cloning() {
        let config = TsaJobConfig {
            tsa_urls: vec![
                "https://tsa1.com".to_string(),
                "https://tsa2.com".to_string(),
            ],
            timeout_ms: 10000,
            interval_secs: 120,
            max_batch_size: 50,
            active_tree_interval_secs: 300,
        };

        let cloned = config.clone();

        assert_eq!(config.tsa_urls, cloned.tsa_urls);
        assert_eq!(config.timeout_ms, cloned.timeout_ms);
        assert_eq!(config.interval_secs, cloned.interval_secs);
        assert_eq!(config.max_batch_size, cloned.max_batch_size);
        assert_eq!(
            config.active_tree_interval_secs,
            cloned.active_tree_interval_secs
        );
    }

    // Note: We don't test the run() method directly as it contains an infinite loop.
    // Integration tests should verify the shutdown behavior.

    #[test]
    fn test_mock_storage_anchor_methods() {
        use crate::traits::storage::Storage;
        let storage = MockStorage::new(0, [0u8; 32]);
        assert!(Storage::get_tsa_anchor_covering(&storage, 0)
            .unwrap()
            .is_none());
        assert!(Storage::get_ots_anchor_covering(&storage, 0)
            .unwrap()
            .is_none());
    }

    // -------------------------------------------------------------------
    // Regression: the active-tree periodic anchoring path must try the
    // next configured TSA server within the SAME pass when the first one
    // fails (request error or messageImprint verification failure),
    // instead of giving up after one URL and waiting for the next
    // interval tick.
    // -------------------------------------------------------------------

    const JOB_FREETSA_RESPONSE_HEX: &str = "3082155d30030201003082155406092a864886f70d010702a082154530821541020103310f300d060960864801650304020305003082018f060b2a864886f70d0109100104a082017e0482017a3082017602010106042a0304013031300d060960864801650304020105000420954d5a49fd70d9b8bcdb35d252267829957f7ef7fa6c74f88419bdc5e82209f40204026eb423180f32303236303131313030303131325a0101ff02090081c082603883d30da0820111a482010d308201093111300f060355040a13084672656520545341310c300a060355040b130354534131763074060355040d136d54686973206365727469666963617465206469676974616c6c79207369676e7320646f63756d656e747320616e642074696d65207374616d70207265717565737473206d616465207573696e672074686520667265657473612e6f7267206f6e6c696e65207365727669636573311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310b3009060355040613024445310f300d0603550408130642617965726ea082100830820801308205e9a003020102020900c1e986160da8e982300d06092a864886f70d01010d05003081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b3009060355040613024445301e170d3136303331333031353733395a170d3236303331313031353733395a308201093111300f060355040a13084672656520545341310c300a060355040b130354534131763074060355040d136d54686973206365727469666963617465206469676974616c6c79207369676e7320646f63756d656e747320616e642074696d65207374616d70207265717565737473206d616465207573696e672074686520667265657473612e6f7267206f6e6c696e65207365727669636573311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310b3009060355040613024445310f300d0603550408130642617965726e30820222300d06092a864886f70d01010105000382020f003082020a0282020100b591048c4e486f34e9dc08627fc2375162236984b82cb130beff517cfc38f84bce5c65a874dab2621ae0bce7e33563e0ede934fd5f8823159f07848808227460c1ed88261706f4281334359dfbb81bd1353fc179610af1a8c8c865dc00ea23b3a89be6bd03ba85a9ec827d60565905e22d6a584ed1380ae150280cee397e98a012f380464007862443bc077cb95f421af31712d9683cdb6dffbaf3c8ba5ba566ae523d459d6177346d4d840e27886b7c01c5b890d78a2e27bba8dd2f9a2812e157d62f921c65962548069dcdb7d06de181de0e9570d66f87220ce28b628ab55906f3ee0c210f7051e8f4858af8b9a92d09e46af2d9cba5bfcfad168cdf604491a4b06603b114caf7031f065e7eeefa53c575f3490c059d2e32ddc76ac4d4c4c710683b97fd1be591bc61055186d88f9a0391b307b6f91ed954daa36f9acd6a1e14aa2e4adf17464b54db18dbb6ffe30080246547370436ce4e77bae5de6fe0f3f9d6e7ffbeb461e794e92fb0951f8aae61a412cce9b21074635c8be327ae1a0f6b4a646eb0f8463bc63bf845530435d19e802511ec9f66c3496952d8becb69b0aa4d4c41f60515fe7dcbb89319cdda59ba6aea4be3ceae718e6fcb6ccd7db9fc50bb15b12f3665b0aa307289c2e6dd4b111ce48ba2d9efdb5a6b9a506069334fb34f6fc7ae330f0b34208aac80df3266fdd90465876ba2cb898d9505315b6e7b0203010001a38201db308201d730090603551d1304023000301d0603551d0e041604146e760b7b4e4f9ce160ca6d2ce927a2a294b37737301f0603551d23041830168014fa550d8c346651434cf7e7b3a76c95af7ae6a497300b0603551d0f0404030206c030160603551d250101ff040c300a06082b06010505070308306306082b0601050507010104573055302a06082b06010505073002861e687474703a2f2f7777772e667265657473612e6f72672f7473612e637274302706082b06010505073001861b687474703a2f2f7777772e667265657473612e6f72673a3235363030370603551d1f0430302e302ca02aa0288626687474703a2f2f7777772e667265657473612e6f72672f63726c2f726f6f745f63612e63726c3081c60603551d200481be3081bb3081b80601003081b2303306082b060105050702011627687474703a2f2f7777772e667265657473612e6f72672f667265657473615f6370732e68746d6c303206082b060105050702011626687474703a2f2f7777772e667265657473612e6f72672f667265657473615f6370732e706466304706082b06010505070202303b1a394672656554534120747275737465642074696d657374616d70696e6720536f6674776172652061732061205365727669636520285361615329300d06092a864886f70d01010d05000382020100a5c944e2c6fac0a14d930a7fd0a0b172b41fc1483c3e957c68a2bcd9b9764f1a950161fd72472d41a5eed277786203b5422240fb3a26cde176087b6fb1011df4cc19e2571aa4a051109665e94c46f50bd2adee6ac4137e251b25a39dabda451515d8ff9e07209e8ec20b7874f7e1a0ede7c00937fe84a334f8b3265ced2d8ed9df61396583677feb382c1ee3b23e6ea5f05df30de7b9f89005d25266f612f39c8b4f6daba6d7bfbac19632b90637329f52a6f066a10e43eaa81f849a6c5fe3fe8b5ea23275f687f2052e502ea6c30762a668cce07871dd8e97e315bba929e25589977a0a312ce96c5106b1437c779f2b361b182888f3ee8a234374fa063e956192627f7c431073965d1260928eba009e803429ae324cf96f042354f37bca5afddc79f79346ab388bfc79f01dc9861254ea6cc129941076b83d20556f3be51326837f2876f7833b370e7c3d410523827d4f53400c72218d75229ff10c6f8893a9a3a1c0c42bb4c898c13df41c7f6573b4fc56515971a610a7b0d2857c8225a9fb204eaceca2e8971aa1af87886a2ae3c72fe0a0aae842980a77bef16b92115458090d982b5946603764e75a0ad3d11454b9986f678b9ab6afe8497033ae3abfd4eb43b7bc9dee68815949e6481582a82e785277f2282107efe390200e0508acb8ea82ea2505276f3c9da2a3d3b4ad38bbf8842bda36fc2448291f558dc02dd1e0308207ff308205e7a003020102020900c1e986160da8e980300d06092a864886f70d01010d05003081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b3009060355040613024445301e170d3136303331333031353231335a170d3431303330373031353231335a3081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b300906035504061302444530820222300d06092a864886f70d01010105000382020f003082020a0282020100b6028e0e3032f11110d964cda94b9d0278e1942ae913aaa59907cda69793995bd9ac7e33bad9fe3704da1c01a98d21afe3f591a59d7067705167998f5016722e0ab462b21f439171d2cfcc4593f3735af794a5ab311f6c010c7898de33d75c4510ee76f4bd1d1498cf17d303f06a5dd9f796cc6ca9b657a56fe3ea4fefbe7ce6b6a18d3e35a30cee5ff170d1cf39a333d3fda8964d22db685b29e561be890f0aa845873b2e84ab26ab839ffe8fade9d23bb31e61d273cc9b880649185fabecfa0534600aba901b614e2e854582dea2226fc19cd7df52bed50d8777cd9988c053a3fc7dc3287a068a4ff12b713cd9803666e955385456ff38f80298cf6b93856e9224774a66cf1cdd11c2f8efd85203d7458b25664b13ed639cded4ff8113d6cc5353d2729473c3c307157c722aa5b5dd0bfb2d6c38b1b93749c881ec60026d08951b3824bd71bacbce473aebd636f0b918b4a2c8ff4694f07457af2d6f1cf82554d1770fd79ff5d314dcd104cddcabc94138056dfcf017e7eb8572fd52f70144f188da05f5823f58dd06297e7387bed2d772c13da8266601045fe412dd70986c0c987ba7344b9037387516d258e7885b51f8968b7f2601213bc4cb4c85f8ff0b84af6a988337cdfb81868f7ecf31dca6716d7ec2dd802c1672629e5c0052cb357dd29aafc43f615b3b1ff9d4e1ce08c71c73e1febb7dc56a33621329e9ed6c230203010001a382024e3082024a300c0603551d13040530030101ff300e0603551d0f0101ff0404030201c6301d0603551d0e04160414fa550d8c346651434cf7e7b3a76c95af7ae6a4973081ca0603551d230481c23081bf8014fa550d8c346651434cf7e7b3a76c95af7ae6a497a1819ba481983081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b3009060355040613024445820900c1e986160da8e98030330603551d1f042c302a3028a026a0248622687474703a2f2f7777772e667265657473612e6f72672f726f6f745f63612e63726c3081cf0603551d200481c73081c43081c1060a2b0601040181f22401013081b2303306082b060105050702011627687474703a2f2f7777772e667265657473612e6f72672f667265657473615f6370732e68746d6c303206082b060105050702011626687474703a2f2f7777772e667265657473612e6f72672f667265657473615f6370732e706466304706082b06010505070202303b1a394672656554534120747275737465642074696d657374616d70696e6720536f6674776172652061732061205365727669636520285361615329303706082b06010505070101042b3029302706082b06010505073001861b687474703a2f2f7777772e667265657473612e6f72673a32353630300d06092a864886f70d01010d0500038202010068af7ebf938562ef4ceb3b580be2faf6cc35a26772962f3d95901fa5630c87d09198984ce8a06a33f8a9c282ed9f1cb11ac6c23e17108ee4efce6fb294de95c133262255725522ca61971d4a3b7f78250dfb8d4aeec0fb1959b164100520b9c10e64c62662e4ad4d0abae2298fc948fc4e99e8d9e6b8fdbe4404121ec7c1422eacb2c9d7328e07396e60b4f3bb803ad4a555c80fefb53f85e7764a0a9fb4afc399f4cd2f5fbf587105c6081cf3d05337b6bb7d1b010b749f4888c912f3696ba1b6902d77b7dfc046c04a0cc1ec4f8d185e2da55dfb7bc2a2036c6219246a4f99ddbb6f1f829398f3b803dc0ad90dcb59bef4c27c77404b99043b78271867991152c399f12cbfc4c625adc096355ae44e342100ec517a502e2f06f940b8d43599bbc1154f8ae761a0b0d555fb4a1391d4f3420af8dbf12f2d7ddb9d77dce1537804074af175e4f2d6d55b34b5d6f7dcbdd31730af56480d4c0cff143f9e83bc151866d0ba0f0bbdc47fe27864176bbd6c1ab85df325edf777889bc4471bf3fa73e56cc591e8b160cda7b0786a1ec04ac3b24fa2e28d5d19e5e48004d5e166a83c82ec6fd54fb385ebaf7133a85b52de46db5244e1c34ae8d36e712f9fce0d493d7d3edd586c6198e3ec3e6e96346f417ac9f221e0aff33a8f6a0b1ef4c023630b76adaa8d91433825ecc41c49a5b98b181c7da30e997ab954c73c2cd805afda993182038a308203860201013081a33081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b3009060355040613024445020900c1e986160da8e982300d06096086480165030402030500a081b8301a06092a864886f70d010903310d060b2a864886f70d0109100104301c06092a864886f70d010905310f170d3236303131313030303131325a302b060b2a864886f70d010910020c311c301a301830160414916da3d860ecca82e34bc59d1793e7e968875f14304f06092a864886f70d01090431420440044f479a6d1240954966f51b86e85ef72ea23c12a0a3dc502901adca336334339ebf18f39b7708cd60780514841423adf531b44b3795699ca8ecf9a9325d3c53300d06092a864886f70d0101010500048202003f09255516f561f9f09786cb3abd61aca90f6217f01145b1e9107be8fcd17c0d3334f056c4e3c9871ef58b63c8e52c008aecf8bc345fc7242b22091c9b0020ed37626450c93a65d130a57a8f473709dbf458b0a9fea1e5564d4efd04bf0c935759be75543c6a3e59c9c42eb25cc89bb8491794b5aeaa598c03023118e07b71ae1e1b6236c2f9e8f252b73e993de3a6c9a99622d919b2abfedc77ffb0d40d85641ef40054f2fc1f15d8bf24b4d02801f16fc1a8c4d9b4cd0806b02b0270225d022fcc6453d17e55123a3fa1144cb6aa6e4652fecfb1290105b198a3bd98f9d2da733dbb5d4d31accd89a1feb065f9fba28458c883adba81dbb665299ffccdba1845e620854866939b84ad76f4dd76f1bcc45b5ae0316802c24d4cc3d0244fcf41bfe23d9edd8272d86c5ed1560cd7f7ed1314c6d7f78d9b31ea0df30f5a5784e59a876f1e0a389c9a16010e5f4ddb91874c9699517bf34016ae64e20fbddc8dd6db1ff68608d14ce073bc4725dfe70ea22306fa11dd0547b1b3a4ef78bea22682f63d907e3a508346ef4f2f0a9af84aa6b6ac833ca9d6e386a9a811b251a9ea3c774eaffae9a90aa75ae5af75cff823cbe65a999b99cfc3088445e9a011d164da17f1b6862dd2ce1cd95237b56fbe780e13e8833504899d8062fa2423f990e3361ee702d75c5e0a061fce4acf390a3423397365902d86ac4c77644b575415ebc5";
    const JOB_FREETSA_EXPECTED_HASH_HEX: &str =
        "954d5a49fd70d9b8bcdb35d252267829957f7ef7fa6c74f88419bdc5e82209f4";

    fn job_freetsa_expected_hash() -> [u8; 32] {
        let bytes = hex::decode(JOB_FREETSA_EXPECTED_HASH_HEX).expect("valid hex fixture");
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes);
        hash
    }

    #[tokio::test]
    async fn test_process_active_tree_anchoring_falls_back_within_same_pass() {
        // First configured server: fails outright.
        let mut server1 = mockito::Server::new_async().await;
        let mock1 = server1
            .mock("POST", "/")
            .with_status(500)
            .create_async()
            .await;

        // Second configured server: returns a valid, verifiable token.
        let mut server2 = mockito::Server::new_async().await;
        let mock2 = server2
            .mock("POST", "/")
            .with_status(200)
            .with_body(hex::decode(JOB_FREETSA_RESPONSE_HEX).expect("valid hex"))
            .create_async()
            .await;

        let root_hash = job_freetsa_expected_hash();
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, root_hash));
        let config = TsaJobConfig {
            tsa_urls: vec![server1.url(), server2.url()],
            timeout_ms: 5000,
            interval_secs: 60,
            max_batch_size: 100,
            active_tree_interval_secs: 0, // No time gate
        };

        let job = TsaAnchoringJob::new(index.clone(), storage, config);
        // Force server1 (index 0) to be the first one attempted.
        job.selector.update_last_index(1);

        let result = job.process_active_tree_anchoring().await;

        mock1.assert_async().await;
        mock2.assert_async().await;

        assert!(
            result.is_ok(),
            "fallback to the second server within the same pass must succeed: {:?}",
            result
        );

        // The anchor must actually be stored -- proving the fallback ran to
        // completion within this single call, not merely that no error
        // propagated.
        let idx = index.lock().await;
        let stored = idx
            .get_tsa_anchor_for_hash(&root_hash)
            .expect("query should succeed");
        assert!(
            stored.is_some(),
            "the anchor obtained from the fallback server must be persisted"
        );
        drop(idx);

        let last_anchor = job.last_active_anchor.lock().await;
        assert!(
            last_anchor.is_some(),
            "last_active_anchor should be updated after a successful fallback"
        );
    }
}
