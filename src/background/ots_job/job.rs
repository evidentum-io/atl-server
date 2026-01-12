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
