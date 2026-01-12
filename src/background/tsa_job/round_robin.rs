// File: src/background/tsa_job/round_robin.rs

use crate::error::{ServerError, ServerResult};
use crate::storage::index::{IndexStore, TreeRecord};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Round-robin TSA server selection
///
/// Distributes TSA requests evenly across configured servers.
/// If one server fails, tries the next in the list.
/// If all servers fail, returns error (tree remains pending for retry).
pub struct RoundRobinSelector {
    urls: Vec<String>,
    last_index: AtomicUsize,
}

impl RoundRobinSelector {
    pub fn new(urls: Vec<String>) -> Self {
        Self {
            urls,
            last_index: AtomicUsize::new(0),
        }
    }

    /// Try to anchor tree with round-robin TSA selection
    ///
    /// Returns anchor_id on success, error if all servers fail.
    pub async fn anchor_with_round_robin(
        &self,
        tree: &TreeRecord,
        index: &Arc<Mutex<IndexStore>>,
        timeout_ms: u64,
    ) -> ServerResult<i64> {
        if self.urls.is_empty() {
            return Err(ServerError::Internal("No TSA URLs configured".into()));
        }

        let num_servers = self.urls.len();
        let start_index = (self.last_index.load(Ordering::Relaxed) + 1) % num_servers;

        // Try each server in round-robin order
        for i in 0..num_servers {
            let current_index = (start_index + i) % num_servers;
            let tsa_url = &self.urls[current_index];

            tracing::debug!(
                tree_id = tree.id,
                tsa_url = %tsa_url,
                attempt = i + 1,
                total_servers = num_servers,
                "Attempting TSA timestamp"
            );

            match super::request::try_tsa_timestamp(tree, tsa_url, index, timeout_ms).await {
                Ok(anchor_id) => {
                    // Update round-robin index for next request
                    self.last_index.store(current_index, Ordering::Relaxed);
                    return Ok(anchor_id);
                }
                Err(e) => {
                    tracing::warn!(
                        tree_id = tree.id,
                        tsa_url = %tsa_url,
                        error = %e,
                        "TSA server failed, trying next"
                    );
                    continue;
                }
            }
        }

        Err(ServerError::Internal(format!(
            "All {} TSA servers failed for tree {}",
            num_servers, tree.id
        )))
    }

    /// Get number of configured URLs
    pub fn urls_count(&self) -> usize {
        self.urls.len()
    }

    /// Get URL by index
    pub fn get_url(&self, index: usize) -> &str {
        &self.urls[index % self.urls.len()]
    }

    /// Update last used index
    pub fn update_last_index(&self, index: usize) {
        self.last_index.store(index, Ordering::Relaxed);
    }

    /// Get current last_index value
    pub fn last_index(&self) -> usize {
        self.last_index.load(Ordering::Relaxed)
    }
}
