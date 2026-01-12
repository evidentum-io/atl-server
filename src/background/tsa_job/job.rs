// File: src/background/tsa_job/job.rs

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;

use super::config::TsaJobConfig;
use super::round_robin::RoundRobinSelector;
use crate::error::ServerResult;
use crate::storage::index::{IndexStore, TreeRecord};
use crate::traits::Storage;

/// TSA anchoring background job
///
/// Processes trees that need TSA anchoring (Tier-1 evidence):
/// - Trees with status IN ('pending_bitcoin', 'closed')
/// - Trees without tsa_anchor_id (not yet anchored)
///
/// Uses round-robin load distribution across configured TSA servers.
pub struct TsaAnchoringJob {
    index: Arc<Mutex<IndexStore>>,
    storage: Arc<dyn Storage>,
    selector: RoundRobinSelector,
    config: TsaJobConfig,
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
        // PART 2: Anchor closed trees that don't have final TSA anchor
        let pending_trees = {
            let idx = self.index.lock().await;
            idx.get_trees_pending_tsa()
                .map_err(|e| crate::error::ServerError::Storage(crate::error::StorageError::Database(e.to_string())))?
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
            "Processing closed trees for final TSA anchoring"
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
                        "Final TSA anchor created for closed tree"
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
}
