// File: src/background/tree_closer/job.rs

use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

use super::config::TreeCloserConfig;
use super::logic;

#[cfg(feature = "sqlite")]
use crate::storage::SqliteStore;

/// Tree closer background job
///
/// Periodically checks if the active tree should be closed based on:
/// - Tree lifetime (elapsed since first entry, NOT creation)
/// - Tree has at least one entry (first_entry_at IS NOT NULL)
///
/// When closing a tree:
/// 1. Submits root hash to OTS calendar
/// 2. Creates anchor record with status='pending'
/// 3. Atomically closes old tree and creates new active tree
pub struct TreeCloser {
    #[cfg(feature = "sqlite")]
    storage: Arc<SqliteStore>,
    config: TreeCloserConfig,
}

impl TreeCloser {
    #[cfg(feature = "sqlite")]
    pub fn new(storage: Arc<SqliteStore>, config: TreeCloserConfig) -> Self {
        Self { storage, config }
    }

    /// Run the tree closer as a background task
    ///
    /// Runs until shutdown signal is received via broadcast channel.
    pub async fn run(&self, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        let mut ticker = interval(Duration::from_secs(self.config.interval_secs));

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    #[cfg(feature = "sqlite")]
                    if let Err(e) = logic::check_and_close_if_needed(
                        &self.storage,
                        self.config.tree_lifetime_secs,
                        &self.config.ots_calendar_url,
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
