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
