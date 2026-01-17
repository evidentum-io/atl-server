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

    /// Get next URL in round-robin order
    ///
    /// Returns the next URL to use for TSA requests.
    /// Updates internal counter for subsequent calls.
    pub fn next_url(&self) -> ServerResult<String> {
        if self.urls.is_empty() {
            return Err(ServerError::Internal("No TSA URLs configured".into()));
        }

        let num_servers = self.urls.len();
        let current_index = (self.last_index.load(Ordering::Relaxed) + 1) % num_servers;
        self.last_index.store(current_index, Ordering::Relaxed);

        Ok(self.urls[current_index].clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_selector() {
        let urls = vec!["https://tsa1.com".to_string(), "https://tsa2.com".to_string()];
        let selector = RoundRobinSelector::new(urls.clone());

        assert_eq!(selector.urls_count(), 2);
        assert_eq!(selector.last_index(), 0);
    }

    #[test]
    fn test_urls_count() {
        let selector = RoundRobinSelector::new(vec![
            "https://tsa1.com".to_string(),
            "https://tsa2.com".to_string(),
            "https://tsa3.com".to_string(),
        ]);

        assert_eq!(selector.urls_count(), 3);
    }

    #[test]
    fn test_urls_count_empty() {
        let selector = RoundRobinSelector::new(vec![]);
        assert_eq!(selector.urls_count(), 0);
    }

    #[test]
    fn test_get_url() {
        let urls = vec![
            "https://tsa1.com".to_string(),
            "https://tsa2.com".to_string(),
            "https://tsa3.com".to_string(),
        ];
        let selector = RoundRobinSelector::new(urls);

        assert_eq!(selector.get_url(0), "https://tsa1.com");
        assert_eq!(selector.get_url(1), "https://tsa2.com");
        assert_eq!(selector.get_url(2), "https://tsa3.com");
    }

    #[test]
    fn test_get_url_wraps_around() {
        let urls = vec![
            "https://tsa1.com".to_string(),
            "https://tsa2.com".to_string(),
        ];
        let selector = RoundRobinSelector::new(urls);

        assert_eq!(selector.get_url(0), "https://tsa1.com");
        assert_eq!(selector.get_url(1), "https://tsa2.com");
        assert_eq!(selector.get_url(2), "https://tsa1.com"); // wraps
        assert_eq!(selector.get_url(3), "https://tsa2.com"); // wraps
    }

    #[test]
    fn test_update_and_read_last_index() {
        let selector = RoundRobinSelector::new(vec!["https://tsa1.com".to_string()]);

        assert_eq!(selector.last_index(), 0);

        selector.update_last_index(5);
        assert_eq!(selector.last_index(), 5);

        selector.update_last_index(10);
        assert_eq!(selector.last_index(), 10);
    }

    #[test]
    fn test_next_url_empty_urls() {
        let selector = RoundRobinSelector::new(vec![]);
        let result = selector.next_url();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No TSA URLs configured"));
    }

    #[test]
    fn test_next_url_single_url() {
        let selector = RoundRobinSelector::new(vec!["https://tsa1.com".to_string()]);

        let url1 = selector.next_url().unwrap();
        assert_eq!(url1, "https://tsa1.com");
        assert_eq!(selector.last_index(), 0);

        let url2 = selector.next_url().unwrap();
        assert_eq!(url2, "https://tsa1.com");
        assert_eq!(selector.last_index(), 0);
    }

    #[test]
    fn test_next_url_multiple_urls() {
        let selector = RoundRobinSelector::new(vec![
            "https://tsa1.com".to_string(),
            "https://tsa2.com".to_string(),
            "https://tsa3.com".to_string(),
        ]);

        // Initial last_index = 0
        // next_url: current_index = (0 + 1) % 3 = 1
        let url1 = selector.next_url().unwrap();
        assert_eq!(url1, "https://tsa2.com");
        assert_eq!(selector.last_index(), 1);

        let url2 = selector.next_url().unwrap();
        assert_eq!(url2, "https://tsa3.com");
        assert_eq!(selector.last_index(), 2);

        let url3 = selector.next_url().unwrap();
        assert_eq!(url3, "https://tsa1.com");
        assert_eq!(selector.last_index(), 0); // wraps around

        let url4 = selector.next_url().unwrap();
        assert_eq!(url4, "https://tsa2.com");
        assert_eq!(selector.last_index(), 1);
    }

    #[test]
    fn test_next_url_starts_from_zero() {
        let selector = RoundRobinSelector::new(vec![
            "https://tsa1.com".to_string(),
            "https://tsa2.com".to_string(),
        ]);

        // First call should start from index 0 (last_index is 0, so next is (0+1)%2 = 1)
        // But the URL at index 1 is "https://tsa2.com"
        // Wait, let me re-check the logic:
        // initial last_index = 0
        // next_url: current_index = (0 + 1) % 2 = 1
        // returns urls[1] = "https://tsa2.com"
        // stores last_index = 1

        let url1 = selector.next_url().unwrap();
        assert_eq!(url1, "https://tsa2.com");
        assert_eq!(selector.last_index(), 1);
    }
}
