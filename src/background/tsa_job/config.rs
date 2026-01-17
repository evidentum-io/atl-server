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

    /// How often to anchor active tree (seconds)
    /// Default: 60 (anchor every minute while tree is active)
    pub active_tree_interval_secs: u64,
}

impl Default for TsaJobConfig {
    fn default() -> Self {
        Self {
            tsa_urls: vec!["https://freetsa.org/tsr".to_string()],
            timeout_ms: 5000,              // 5 second timeout
            interval_secs: 60,             // Check every minute
            max_batch_size: 100,           // Process up to 100 trees per run
            active_tree_interval_secs: 60, // Anchor active tree every minute
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
            active_tree_interval_secs: std::env::var("ATL_TSA_ACTIVE_INTERVAL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(60),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_default_config() {
        let config = TsaJobConfig::default();

        assert_eq!(config.tsa_urls, vec!["https://freetsa.org/tsr"]);
        assert_eq!(config.timeout_ms, 5000);
        assert_eq!(config.interval_secs, 60);
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.active_tree_interval_secs, 60);
    }

    #[test]
    #[serial]
    fn test_from_env_defaults() {
        // Clean environment
        std::env::remove_var("ATL_TSA_URLS");
        std::env::remove_var("ATL_TSA_TIMEOUT_MS");
        std::env::remove_var("ATL_TSA_INTERVAL_SECS");
        std::env::remove_var("ATL_TSA_JOB_BATCH_SIZE");
        std::env::remove_var("ATL_TSA_ACTIVE_INTERVAL_SECS");

        let config = TsaJobConfig::from_env();

        assert_eq!(config.tsa_urls, vec!["https://freetsa.org/tsr"]);
        assert_eq!(config.timeout_ms, 5000);
        assert_eq!(config.interval_secs, 60);
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.active_tree_interval_secs, 60);
    }

    #[test]
    #[serial]
    fn test_from_env_single_url() {
        std::env::set_var("ATL_TSA_URLS", "https://test.com/tsr");

        let config = TsaJobConfig::from_env();

        assert_eq!(config.tsa_urls, vec!["https://test.com/tsr"]);

        std::env::remove_var("ATL_TSA_URLS");
    }

    #[test]
    #[serial]
    fn test_from_env_multiple_urls() {
        std::env::set_var("ATL_TSA_URLS", "https://test1.com/tsr, https://test2.com/tsr, https://test3.com/tsr");

        let config = TsaJobConfig::from_env();

        assert_eq!(config.tsa_urls, vec![
            "https://test1.com/tsr",
            "https://test2.com/tsr",
            "https://test3.com/tsr"
        ]);

        std::env::remove_var("ATL_TSA_URLS");
    }

    #[test]
    #[serial]
    fn test_from_env_custom_timeout() {
        std::env::set_var("ATL_TSA_TIMEOUT_MS", "10000");

        let config = TsaJobConfig::from_env();

        assert_eq!(config.timeout_ms, 10000);

        std::env::remove_var("ATL_TSA_TIMEOUT_MS");
    }

    #[test]
    #[serial]
    fn test_from_env_invalid_timeout_falls_back_to_default() {
        std::env::set_var("ATL_TSA_TIMEOUT_MS", "not_a_number");

        let config = TsaJobConfig::from_env();

        assert_eq!(config.timeout_ms, 5000);

        std::env::remove_var("ATL_TSA_TIMEOUT_MS");
    }

    #[test]
    #[serial]
    fn test_from_env_custom_interval() {
        std::env::set_var("ATL_TSA_INTERVAL_SECS", "120");

        let config = TsaJobConfig::from_env();

        assert_eq!(config.interval_secs, 120);

        std::env::remove_var("ATL_TSA_INTERVAL_SECS");
    }

    #[test]
    #[serial]
    fn test_from_env_invalid_interval_falls_back_to_default() {
        std::env::set_var("ATL_TSA_INTERVAL_SECS", "invalid");

        let config = TsaJobConfig::from_env();

        assert_eq!(config.interval_secs, 60);

        std::env::remove_var("ATL_TSA_INTERVAL_SECS");
    }

    #[test]
    #[serial]
    fn test_from_env_custom_batch_size() {
        std::env::set_var("ATL_TSA_JOB_BATCH_SIZE", "50");

        let config = TsaJobConfig::from_env();

        assert_eq!(config.max_batch_size, 50);

        std::env::remove_var("ATL_TSA_JOB_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_invalid_batch_size_falls_back_to_default() {
        std::env::set_var("ATL_TSA_JOB_BATCH_SIZE", "abc");

        let config = TsaJobConfig::from_env();

        assert_eq!(config.max_batch_size, 100);

        std::env::remove_var("ATL_TSA_JOB_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_custom_active_interval() {
        std::env::set_var("ATL_TSA_ACTIVE_INTERVAL_SECS", "30");

        let config = TsaJobConfig::from_env();

        assert_eq!(config.active_tree_interval_secs, 30);

        std::env::remove_var("ATL_TSA_ACTIVE_INTERVAL_SECS");
    }

    #[test]
    #[serial]
    fn test_from_env_invalid_active_interval_falls_back_to_default() {
        std::env::set_var("ATL_TSA_ACTIVE_INTERVAL_SECS", "xyz");

        let config = TsaJobConfig::from_env();

        assert_eq!(config.active_tree_interval_secs, 60);

        std::env::remove_var("ATL_TSA_ACTIVE_INTERVAL_SECS");
    }

    #[test]
    #[serial]
    fn test_from_env_all_custom_values() {
        std::env::set_var("ATL_TSA_URLS", "https://custom1.com/tsr,https://custom2.com/tsr");
        std::env::set_var("ATL_TSA_TIMEOUT_MS", "15000");
        std::env::set_var("ATL_TSA_INTERVAL_SECS", "90");
        std::env::set_var("ATL_TSA_JOB_BATCH_SIZE", "200");
        std::env::set_var("ATL_TSA_ACTIVE_INTERVAL_SECS", "45");

        let config = TsaJobConfig::from_env();

        assert_eq!(config.tsa_urls, vec!["https://custom1.com/tsr", "https://custom2.com/tsr"]);
        assert_eq!(config.timeout_ms, 15000);
        assert_eq!(config.interval_secs, 90);
        assert_eq!(config.max_batch_size, 200);
        assert_eq!(config.active_tree_interval_secs, 45);

        std::env::remove_var("ATL_TSA_URLS");
        std::env::remove_var("ATL_TSA_TIMEOUT_MS");
        std::env::remove_var("ATL_TSA_INTERVAL_SECS");
        std::env::remove_var("ATL_TSA_JOB_BATCH_SIZE");
        std::env::remove_var("ATL_TSA_ACTIVE_INTERVAL_SECS");
    }
}
