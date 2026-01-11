// File: src/background/ots_poll_job/config.rs

/// OTS job configuration
#[derive(Debug, Clone)]
pub struct OtsJobConfig {
    /// How often to poll for confirmations (seconds)
    pub interval_secs: u64,

    /// Max anchors to check per run
    pub max_batch_size: usize,
}

impl Default for OtsJobConfig {
    fn default() -> Self {
        Self {
            interval_secs: 600,  // Poll every 10 minutes
            max_batch_size: 100, // Check up to 100 anchors per run
        }
    }
}

impl OtsJobConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        Self {
            interval_secs: std::env::var("ATL_OTS_POLL_INTERVAL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(600),
            max_batch_size: std::env::var("ATL_OTS_UPGRADE_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100),
        }
    }
}
