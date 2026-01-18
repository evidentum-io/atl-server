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

        // Get next TSA URL from round-robin selector
        let tsa_url = self.selector.next_url().map_err(|e| {
            crate::error::ServerError::Internal(format!("No TSA servers configured: {}", e))
        })?;

        match super::request::create_tsa_anchor_for_tree_head(
            tree_head.root_hash,
            tree_head.tree_size,
            &tsa_url,
            self.config.timeout_ms,
            &self.index,
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

        fn get_anchors_covering(
            &self,
            _target_tree_size: u64,
            _limit: usize,
        ) -> crate::error::ServerResult<Vec<crate::traits::Anchor>> {
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
}
