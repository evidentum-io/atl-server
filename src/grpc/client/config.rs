//! Configuration for gRPC client

/// Configuration for gRPC client
#[derive(Debug, Clone)]
pub struct GrpcClientConfig {
    /// Sequencer URL (e.g., "http://sequencer:50051")
    pub sequencer_url: String,

    /// Authentication token
    pub token: Option<String>,

    /// Keep-alive interval in seconds
    pub keep_alive_secs: u64,

    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,

    /// Request timeout in seconds
    pub request_timeout_secs: u64,

    /// Retry count for transient errors
    pub retry_count: u32,

    /// Base delay for exponential backoff (milliseconds)
    pub retry_backoff_ms: u64,

    /// Concurrency limit
    pub concurrency_limit: usize,
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            sequencer_url: "http://localhost:50051".to_string(),
            token: None,
            keep_alive_secs: 30,
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
            retry_count: 3,
            retry_backoff_ms: 100,
            concurrency_limit: 1000,
        }
    }
}

impl GrpcClientConfig {
    /// Create configuration from environment variables
    ///
    /// # Environment Variables
    ///
    /// - `ATL_SEQUENCER_URL`: Sequencer gRPC endpoint
    /// - `ATL_SEQUENCER_TOKEN`: Shared secret for authentication
    /// - `ATL_GRPC_KEEP_ALIVE_SECS`: Keep-alive interval
    /// - `ATL_GRPC_RETRY_COUNT`: Number of retry attempts
    /// - `ATL_GRPC_RETRY_BACKOFF_MS`: Base retry delay
    /// - `ATL_GRPC_CONCURRENCY_LIMIT`: Max concurrent requests
    pub fn from_env() -> Self {
        Self {
            sequencer_url: std::env::var("ATL_SEQUENCER_URL")
                .unwrap_or_else(|_| "http://localhost:50051".to_string()),
            token: std::env::var("ATL_SEQUENCER_TOKEN").ok(),
            keep_alive_secs: std::env::var("ATL_GRPC_KEEP_ALIVE_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
            retry_count: std::env::var("ATL_GRPC_RETRY_COUNT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
            retry_backoff_ms: std::env::var("ATL_GRPC_RETRY_BACKOFF_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100),
            concurrency_limit: std::env::var("ATL_GRPC_CONCURRENCY_LIMIT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000),
        }
    }
}
