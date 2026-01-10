// File: src/background/ots_poll_job/job.rs

use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

use super::config::OtsPollJobConfig;
use super::upgrade;
use crate::error::ServerResult;

#[cfg(feature = "sqlite")]
use crate::storage::SqliteStore;

/// OTS poll background job
///
/// Polls pending OTS anchors for Bitcoin confirmation.
/// Once confirmed:
/// - Updates anchor status to 'confirmed'
/// - Stores upgraded proof (contains Bitcoin attestation)
/// - Updates tree status to 'closed'
pub struct OtsPollJob {
    #[cfg(feature = "sqlite")]
    storage: Arc<SqliteStore>,
    config: OtsPollJobConfig,
}

impl OtsPollJob {
    #[cfg(feature = "sqlite")]
    pub fn new(storage: Arc<SqliteStore>, config: OtsPollJobConfig) -> Self {
        Self { storage, config }
    }

    /// Run OTS poll as a background task
    ///
    /// Runs until shutdown signal is received via broadcast channel.
    pub async fn run(&self, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        let mut ticker = interval(Duration::from_secs(self.config.interval_secs));

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    #[cfg(feature = "sqlite")]
                    if let Err(e) = self.check_pending_anchors().await {
                        tracing::error!(error = %e, "OTS poll job failed");
                    }
                }
                _ = shutdown.recv() => {
                    tracing::info!("OTS poll job shutting down");
                    break;
                }
            }
        }
    }

    /// Check pending OTS anchors for Bitcoin confirmation
    #[cfg(feature = "sqlite")]
    async fn check_pending_anchors(&self) -> ServerResult<()> {
        let pending_anchors = self.storage.get_pending_ots_anchors()?;

        if pending_anchors.is_empty() {
            tracing::debug!("No pending OTS anchors to check");
            return Ok(());
        }

        let anchors_to_check: Vec<_> = pending_anchors
            .into_iter()
            .take(self.config.max_batch_size)
            .collect();

        tracing::info!(
            count = anchors_to_check.len(),
            "Polling pending OTS anchors for Bitcoin confirmation"
        );

        let mut confirmed_count = 0;
        for anchor in anchors_to_check {
            match upgrade::check_and_upgrade(&anchor, &self.storage).await {
                Ok(true) => {
                    confirmed_count += 1;
                    tracing::info!(anchor_id = anchor.id, "OTS anchor confirmed in Bitcoin");
                }
                Ok(false) => {
                    tracing::debug!(
                        anchor_id = anchor.id,
                        "OTS anchor still pending (not yet in Bitcoin block)"
                    );
                }
                Err(e) => {
                    // Don't stop - continue checking other anchors
                    tracing::warn!(
                        anchor_id = anchor.id,
                        error = %e,
                        "OTS poll check failed, will retry next tick"
                    );
                }
            }
        }

        if confirmed_count > 0 {
            tracing::info!(
                confirmed = confirmed_count,
                "OTS poll job completed with confirmations"
            );
        }

        Ok(())
    }
}
