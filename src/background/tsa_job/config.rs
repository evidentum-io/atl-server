// File: src/background/tsa_job/config.rs

/// TSA anchoring job configuration
#[derive(Debug, Clone)]
pub struct TsaJobConfig {
    /// List of TSA server URLs (round-robin)
    pub tsa_urls: Vec<String>,

    /// Timeout per TSA request in milliseconds
    pub timeout_ms: u64,

    /// How often to check for pending trees (seconds)
    pub interval_secs: u64,

    /// Max trees to process per run
    pub max_batch_size: usize,
}

impl Default for TsaJobConfig {
    fn default() -> Self {
        Self {
            tsa_urls: vec!["https://freetsa.org/tsr".to_string()],
            timeout_ms: 5000,    // 5 second timeout
            interval_secs: 60,   // Check every minute
            max_batch_size: 100, // Process up to 100 trees per run
        }
    }
}

impl TsaJobConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        let tsa_urls = std::env::var("ATL_TSA_URLS")
            .map(|s| s.split(',').map(|u| u.trim().to_string()).collect())
            .unwrap_or_else(|_| vec!["https://freetsa.org/tsr".to_string()]);

        Self {
            tsa_urls,
            timeout_ms: std::env::var("ATL_TSA_TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5000),
            interval_secs: std::env::var("ATL_TSA_INTERVAL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(60),
            max_batch_size: std::env::var("ATL_TSA_JOB_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100),
        }
    }
}
