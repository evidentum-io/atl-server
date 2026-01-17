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
            tsa_urls: vec!["https://tsa1.com".to_string(), "https://tsa2.com".to_string()],
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

    // Note: We don't test the run() method directly as it contains an infinite loop.
    // Integration tests should verify the shutdown behavior.
}
