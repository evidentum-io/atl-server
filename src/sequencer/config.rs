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
    #[allow(dead_code)]
    pub sync_mode: bool,
    // TSA config removed - handled by background job (src/background/tsa_job/)
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_default_config_values() {
        let config = SequencerConfig::default();

        assert_eq!(config.batch_size, 10_000);
        assert_eq!(config.batch_timeout_ms, 100);
        assert_eq!(config.buffer_size, 100_000);
        assert_eq!(config.retry_count, 3);
        assert_eq!(config.retry_base_ms, 10);
        assert_eq!(config.sync_mode, false);
    }

    #[test]
    fn test_config_is_cloneable() {
        let config = SequencerConfig::default();
        let cloned = config.clone();

        assert_eq!(config.batch_size, cloned.batch_size);
        assert_eq!(config.batch_timeout_ms, cloned.batch_timeout_ms);
        assert_eq!(config.buffer_size, cloned.buffer_size);
    }

    #[test]
    fn test_config_debug_format() {
        let config = SequencerConfig::default();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("SequencerConfig"));
        assert!(debug_str.contains("batch_size"));
    }

    #[test]
    #[serial]
    fn test_from_env_uses_defaults_when_no_env_vars() {
        // Clear any existing env vars
        std::env::remove_var("ATL_BATCH_SIZE");
        std::env::remove_var("ATL_BATCH_TIMEOUT_MS");
        std::env::remove_var("ATL_BUFFER_SIZE");
        std::env::remove_var("ATL_BATCH_RETRY_COUNT");
        std::env::remove_var("ATL_BATCH_RETRY_BASE_MS");
        std::env::remove_var("ATL_SYNC_MODE");

        let config = SequencerConfig::from_env();

        assert_eq!(config.batch_size, 10_000);
        assert_eq!(config.batch_timeout_ms, 100);
        assert_eq!(config.buffer_size, 100_000);
        assert_eq!(config.retry_count, 3);
        assert_eq!(config.retry_base_ms, 10);
        assert_eq!(config.sync_mode, false);
    }

    #[test]
    #[serial]
    fn test_from_env_respects_batch_size() {
        std::env::set_var("ATL_BATCH_SIZE", "5000");
        let config = SequencerConfig::from_env();
        assert_eq!(config.batch_size, 5000);
        std::env::remove_var("ATL_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_respects_batch_timeout_ms() {
        std::env::set_var("ATL_BATCH_TIMEOUT_MS", "200");
        let config = SequencerConfig::from_env();
        assert_eq!(config.batch_timeout_ms, 200);
        std::env::remove_var("ATL_BATCH_TIMEOUT_MS");
    }

    #[test]
    #[serial]
    fn test_from_env_respects_buffer_size() {
        std::env::set_var("ATL_BUFFER_SIZE", "50000");
        let config = SequencerConfig::from_env();
        assert_eq!(config.buffer_size, 50_000);
        std::env::remove_var("ATL_BUFFER_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_respects_retry_count() {
        std::env::set_var("ATL_BATCH_RETRY_COUNT", "5");
        let config = SequencerConfig::from_env();
        assert_eq!(config.retry_count, 5);
        std::env::remove_var("ATL_BATCH_RETRY_COUNT");
    }

    #[test]
    #[serial]
    fn test_from_env_respects_retry_base_ms() {
        std::env::set_var("ATL_BATCH_RETRY_BASE_MS", "20");
        let config = SequencerConfig::from_env();
        assert_eq!(config.retry_base_ms, 20);
        std::env::remove_var("ATL_BATCH_RETRY_BASE_MS");
    }

    #[test]
    #[serial]
    fn test_from_env_sync_mode_true() {
        std::env::set_var("ATL_SYNC_MODE", "true");
        let config = SequencerConfig::from_env();
        assert_eq!(config.sync_mode, true);
        std::env::remove_var("ATL_SYNC_MODE");
    }

    #[test]
    #[serial]
    fn test_from_env_sync_mode_one() {
        std::env::set_var("ATL_SYNC_MODE", "1");
        let config = SequencerConfig::from_env();
        assert_eq!(config.sync_mode, true);
        std::env::remove_var("ATL_SYNC_MODE");
    }

    #[test]
    #[serial]
    fn test_from_env_sync_mode_false() {
        std::env::set_var("ATL_SYNC_MODE", "false");
        let config = SequencerConfig::from_env();
        assert_eq!(config.sync_mode, false);
        std::env::remove_var("ATL_SYNC_MODE");
    }

    #[test]
    #[serial]
    fn test_from_env_sync_mode_invalid() {
        std::env::set_var("ATL_SYNC_MODE", "invalid");
        let config = SequencerConfig::from_env();
        assert_eq!(config.sync_mode, false);
        std::env::remove_var("ATL_SYNC_MODE");
    }

    #[test]
    #[serial]
    fn test_from_env_invalid_number_uses_default() {
        std::env::set_var("ATL_BATCH_SIZE", "not_a_number");
        let config = SequencerConfig::from_env();
        assert_eq!(config.batch_size, 10_000);
        std::env::remove_var("ATL_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_empty_string_uses_default() {
        std::env::set_var("ATL_BATCH_SIZE", "");
        let config = SequencerConfig::from_env();
        assert_eq!(config.batch_size, 10_000);
        std::env::remove_var("ATL_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_all_vars_custom() {
        std::env::set_var("ATL_BATCH_SIZE", "1000");
        std::env::set_var("ATL_BATCH_TIMEOUT_MS", "50");
        std::env::set_var("ATL_BUFFER_SIZE", "20000");
        std::env::set_var("ATL_BATCH_RETRY_COUNT", "7");
        std::env::set_var("ATL_BATCH_RETRY_BASE_MS", "15");
        std::env::set_var("ATL_SYNC_MODE", "true");

        let config = SequencerConfig::from_env();

        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.batch_timeout_ms, 50);
        assert_eq!(config.buffer_size, 20_000);
        assert_eq!(config.retry_count, 7);
        assert_eq!(config.retry_base_ms, 15);
        assert_eq!(config.sync_mode, true);

        // Cleanup
        std::env::remove_var("ATL_BATCH_SIZE");
        std::env::remove_var("ATL_BATCH_TIMEOUT_MS");
        std::env::remove_var("ATL_BUFFER_SIZE");
        std::env::remove_var("ATL_BATCH_RETRY_COUNT");
        std::env::remove_var("ATL_BATCH_RETRY_BASE_MS");
        std::env::remove_var("ATL_SYNC_MODE");
    }
}
