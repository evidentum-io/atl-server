//! Sequencer configuration

/// Sequencer configuration
#[derive(Debug, Clone)]
pub struct SequencerConfig {
    /// Maximum number of facts per batch
    pub batch_size: usize,

    /// Maximum time to wait before flushing batch (milliseconds)
    pub batch_timeout_ms: u64,

    /// Bounded channel capacity (backpressure threshold)
    pub buffer_size: usize,

    /// Number of retry attempts on database error
    pub retry_count: u32,

    /// Base delay for exponential backoff (milliseconds)
    pub retry_base_ms: u64,

    /// Sync mode (bypass sequencer, direct writes)
    pub sync_mode: bool,

    /// TSA URLs for batch timestamping
    pub tsa_urls: Vec<String>,

    /// TSA request timeout (milliseconds)
    pub tsa_timeout_ms: u64,

    /// Strict TSA mode: fail batch if all TSA servers fail
    pub tsa_strict: bool,
}

impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            batch_timeout_ms: 100,
            buffer_size: 100_000,
            retry_count: 3,
            retry_base_ms: 10,
            sync_mode: false,
            tsa_urls: vec![],
            tsa_timeout_ms: 5000,
            tsa_strict: false,
        }
    }
}

impl SequencerConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        Self {
            batch_size: std::env::var("ATL_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10_000),
            batch_timeout_ms: std::env::var("ATL_BATCH_TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100),
            buffer_size: std::env::var("ATL_BUFFER_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100_000),
            retry_count: std::env::var("ATL_BATCH_RETRY_COUNT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
            retry_base_ms: std::env::var("ATL_BATCH_RETRY_BASE_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10),
            sync_mode: std::env::var("ATL_SYNC_MODE")
                .ok()
                .map(|s| s == "true" || s == "1")
                .unwrap_or(false),
            tsa_urls: std::env::var("ATL_TSA_URLS")
                .ok()
                .map(|s| s.split(',').map(|u| u.trim().to_string()).collect())
                .unwrap_or_default(),
            tsa_timeout_ms: std::env::var("ATL_TSA_TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5000),
            tsa_strict: std::env::var("ATL_STRICT_TSA")
                .ok()
                .map(|s| s == "true" || s == "1")
                .unwrap_or(false),
        }
    }
}
