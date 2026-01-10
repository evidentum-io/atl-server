// File: src/background/tsa_job/job.rs

use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

use super::config::TsaJobConfig;
use super::round_robin::RoundRobinSelector;
use crate::error::ServerResult;

#[cfg(feature = "sqlite")]
use crate::storage::SqliteStore;

/// TSA anchoring background job
///
/// Processes trees that need TSA anchoring (Tier-1 evidence):
/// - Trees with status IN ('pending_bitcoin', 'closed')
/// - Trees without tsa_anchor_id (not yet anchored)
///
/// Uses round-robin load distribution across configured TSA servers.
pub struct TsaAnchoringJob {
    #[cfg(feature = "sqlite")]
    storage: Arc<SqliteStore>,
    selector: RoundRobinSelector,
    config: TsaJobConfig,
}

impl TsaAnchoringJob {
    #[cfg(feature = "sqlite")]
    pub fn new(storage: Arc<SqliteStore>, config: TsaJobConfig) -> Self {
        let selector = RoundRobinSelector::new(config.tsa_urls.clone());

        Self {
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
                    #[cfg(feature = "sqlite")]
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
    #[cfg(feature = "sqlite")]
    async fn process_pending_trees(&self) -> ServerResult<()> {
        let pending_trees = self.storage.get_trees_pending_tsa()?;

        if pending_trees.is_empty() {
            return Ok(());
        }

        let trees_to_process: Vec<_> = pending_trees
            .into_iter()
            .take(self.config.max_batch_size)
            .collect();

        tracing::info!(
            count = trees_to_process.len(),
            "Processing trees for TSA anchoring"
        );

        for tree in trees_to_process {
            match self
                .selector
                .anchor_with_round_robin(&tree, &self.storage, self.config.timeout_ms)
                .await
            {
                Ok(anchor_id) => {
                    tracing::info!(
                        tree_id = tree.id,
                        anchor_id = anchor_id,
                        "TSA anchor created for tree"
                    );
                }
                Err(e) => {
                    // All TSA servers failed - leave pending, retry on next tick
                    tracing::warn!(
                        tree_id = tree.id,
                        error = %e,
                        "TSA anchoring failed for tree (all servers), will retry next tick"
                    );
                }
            }
        }

        Ok(())
    }
}
