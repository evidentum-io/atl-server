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

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_default_config() {
        let config = OtsJobConfig::default();
        assert_eq!(config.interval_secs, 600);
        assert_eq!(config.max_batch_size, 100);
    }

    #[test]
    fn test_from_env_with_no_vars() {
        // Clear environment variables
        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");

        let config = OtsJobConfig::from_env();
        assert_eq!(config.interval_secs, 600);
        assert_eq!(config.max_batch_size, 100);
    }

    #[test]
    #[ignore] // Env vars cause race conditions in parallel test execution
    fn test_from_env_with_valid_vars() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "300");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "50");

        let config = OtsJobConfig::from_env();
        assert_eq!(config.interval_secs, 300);
        assert_eq!(config.max_batch_size, 50);

        // Cleanup
        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[ignore] // Env vars cause race conditions in parallel test execution
    fn test_from_env_with_invalid_interval() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "invalid");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "50");

        let config = OtsJobConfig::from_env();
        assert_eq!(config.interval_secs, 600); // Falls back to default
        assert_eq!(config.max_batch_size, 50);

        // Cleanup
        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[ignore] // Env vars cause race conditions in parallel test execution
    fn test_from_env_with_invalid_batch_size() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "300");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "not_a_number");

        let config = OtsJobConfig::from_env();
        assert_eq!(config.interval_secs, 300);
        assert_eq!(config.max_batch_size, 100); // Falls back to default

        // Cleanup
        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[ignore] // Env vars cause race conditions in parallel test execution
    fn test_from_env_with_negative_values() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "-100");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "-50");

        let config = OtsJobConfig::from_env();
        // Negative values fail to parse, fall back to defaults
        assert_eq!(config.interval_secs, 600);
        assert_eq!(config.max_batch_size, 100);

        // Cleanup
        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[ignore] // Env vars cause race conditions in parallel test execution
    fn test_from_env_with_zero_values() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "0");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "0");

        let config = OtsJobConfig::from_env();
        assert_eq!(config.interval_secs, 0);
        assert_eq!(config.max_batch_size, 0);

        // Cleanup
        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[ignore] // Env vars cause race conditions in parallel test execution
    fn test_from_env_with_large_values() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "86400"); // 1 day
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "10000");

        let config = OtsJobConfig::from_env();
        assert_eq!(config.interval_secs, 86400);
        assert_eq!(config.max_batch_size, 10000);

        // Cleanup
        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    fn test_config_clone() {
        let config1 = OtsJobConfig::default();
        let config2 = config1.clone();
        assert_eq!(config1.interval_secs, config2.interval_secs);
        assert_eq!(config1.max_batch_size, config2.max_batch_size);
    }

    #[test]
    fn test_config_debug() {
        let config = OtsJobConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("OtsJobConfig"));
        assert!(debug_str.contains("interval_secs"));
        assert!(debug_str.contains("max_batch_size"));
    }
}
