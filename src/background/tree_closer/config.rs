// File: src/background/tree_closer/config.rs

/// Tree closer configuration
#[derive(Debug, Clone)]
pub struct TreeCloserConfig {
    /// How often to check if tree should be closed (seconds)
    pub interval_secs: u64,

    /// How long a tree lives before being closed (seconds)
    /// After this time, tree is closed and marked for OTS anchoring by ots_job
    pub tree_lifetime_secs: u64,
}

impl Default for TreeCloserConfig {
    fn default() -> Self {
        Self {
            interval_secs: 60,        // Check every minute
            tree_lifetime_secs: 3600, // Close tree after 1 hour
        }
    }
}

impl TreeCloserConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        Self {
            interval_secs: std::env::var("ATL_TREE_CLOSE_INTERVAL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(60),
            tree_lifetime_secs: std::env::var("ATL_TREE_LIFETIME_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3600),
        }
    }
}
