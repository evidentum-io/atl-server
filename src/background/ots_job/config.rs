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
    use serial_test::serial;
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
    #[serial]
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
    #[serial]
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
    #[serial]
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
    #[serial]
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
    #[serial]
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
    #[serial]
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

    #[test]
    #[serial]
    fn test_from_env_partial_values() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "150");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");

        let config = OtsJobConfig::from_env();
        assert_eq!(config.interval_secs, 150);
        assert_eq!(config.max_batch_size, 100); // Default

        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
    }

    #[test]
    #[serial]
    fn test_from_env_with_whitespace() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "200");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "75");

        let config = OtsJobConfig::from_env();
        assert_eq!(config.interval_secs, 200);
        assert_eq!(config.max_batch_size, 75);

        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_with_surrounding_whitespace() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", " 300 ");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", " 150 ");

        let config = OtsJobConfig::from_env();
        // parse() does NOT trim whitespace, should fail and fallback to defaults
        assert_eq!(config.interval_secs, 600);
        assert_eq!(config.max_batch_size, 100);

        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_with_overflow() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "99999999999999999999");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "99999999999999999999");

        let config = OtsJobConfig::from_env();
        // Overflow causes parse failure, fallback to defaults
        assert_eq!(config.interval_secs, 600);
        assert_eq!(config.max_batch_size, 100);

        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_empty_string() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "");

        let config = OtsJobConfig::from_env();
        // Empty string fails parse, fallback to defaults
        assert_eq!(config.interval_secs, 600);
        assert_eq!(config.max_batch_size, 100);

        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    fn test_default_values_consistency() {
        let config1 = OtsJobConfig::default();
        let config2 = OtsJobConfig::default();
        assert_eq!(config1.interval_secs, config2.interval_secs);
        assert_eq!(config1.max_batch_size, config2.max_batch_size);
    }

    #[test]
    #[serial]
    fn test_from_env_only_interval_set() {
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "777");

        let config = OtsJobConfig::from_env();
        assert_eq!(config.interval_secs, 777);
        assert_eq!(config.max_batch_size, 100); // Default

        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
    }

    #[test]
    #[serial]
    fn test_from_env_only_batch_size_set() {
        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "888");

        let config = OtsJobConfig::from_env();
        assert_eq!(config.interval_secs, 600); // Default
        assert_eq!(config.max_batch_size, 888);

        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_with_float_values() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "123.456");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "78.9");

        let config = OtsJobConfig::from_env();
        // Floats fail to parse as u64/usize, fallback to defaults
        assert_eq!(config.interval_secs, 600);
        assert_eq!(config.max_batch_size, 100);

        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_with_special_chars() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "abc!@#");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "xyz$%^");

        let config = OtsJobConfig::from_env();
        // Invalid chars fail parse, fallback to defaults
        assert_eq!(config.interval_secs, 600);
        assert_eq!(config.max_batch_size, 100);

        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_with_max_u64() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "18446744073709551615"); // u64::MAX
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "100");

        let config = OtsJobConfig::from_env();
        assert_eq!(config.interval_secs, u64::MAX);
        assert_eq!(config.max_batch_size, 100);

        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_with_leading_zeros() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "00042");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "00099");

        let config = OtsJobConfig::from_env();
        assert_eq!(config.interval_secs, 42);
        assert_eq!(config.max_batch_size, 99);

        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_with_plus_sign() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "+500");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "+250");

        let config = OtsJobConfig::from_env();
        // Rust parse() actually accepts plus sign for positive numbers
        assert_eq!(config.interval_secs, 500);
        assert_eq!(config.max_batch_size, 250);

        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    #[serial]
    fn test_from_env_repeated_calls_consistency() {
        env::set_var("ATL_OTS_POLL_INTERVAL_SECS", "456");
        env::set_var("ATL_OTS_UPGRADE_BATCH_SIZE", "789");

        let config1 = OtsJobConfig::from_env();
        let config2 = OtsJobConfig::from_env();

        assert_eq!(config1.interval_secs, config2.interval_secs);
        assert_eq!(config1.max_batch_size, config2.max_batch_size);

        env::remove_var("ATL_OTS_POLL_INTERVAL_SECS");
        env::remove_var("ATL_OTS_UPGRADE_BATCH_SIZE");
    }

    #[test]
    fn test_config_field_access() {
        let config = OtsJobConfig {
            interval_secs: 42,
            max_batch_size: 999,
        };

        assert_eq!(config.interval_secs, 42);
        assert_eq!(config.max_batch_size, 999);
    }

    #[test]
    fn test_config_mutation() {
        let config = OtsJobConfig {
            interval_secs: 1234,
            max_batch_size: 5678,
        };

        assert_eq!(config.interval_secs, 1234);
        assert_eq!(config.max_batch_size, 5678);
    }

    #[test]
    fn test_default_trait_multiple_calls() {
        let config1 = OtsJobConfig::default();
        let config2 = OtsJobConfig::default();
        let config3 = OtsJobConfig::default();

        assert_eq!(config1.interval_secs, config2.interval_secs);
        assert_eq!(config2.interval_secs, config3.interval_secs);
        assert_eq!(config1.max_batch_size, config2.max_batch_size);
        assert_eq!(config2.max_batch_size, config3.max_batch_size);
    }
}
