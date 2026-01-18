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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Mutex to ensure tests run sequentially (env vars are global state)
    static TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_default_config() {
        let config = TreeCloserConfig::default();
        assert_eq!(config.interval_secs, 60);
        assert_eq!(config.tree_lifetime_secs, 3600);
    }

    #[test]
    fn test_config_from_env_with_no_env_vars() {
        let _lock = TEST_MUTEX.lock().unwrap();

        // Clear env vars to test defaults
        std::env::remove_var("ATL_TREE_CLOSE_INTERVAL_SECS");
        std::env::remove_var("ATL_TREE_LIFETIME_SECS");

        let config = TreeCloserConfig::from_env();
        assert_eq!(config.interval_secs, 60);
        assert_eq!(config.tree_lifetime_secs, 3600);
    }

    #[test]
    fn test_config_from_env_with_valid_env_vars() {
        let _lock = TEST_MUTEX.lock().unwrap();

        std::env::set_var("ATL_TREE_CLOSE_INTERVAL_SECS", "120");
        std::env::set_var("ATL_TREE_LIFETIME_SECS", "7200");

        let config = TreeCloserConfig::from_env();
        assert_eq!(config.interval_secs, 120);
        assert_eq!(config.tree_lifetime_secs, 7200);

        // Cleanup
        std::env::remove_var("ATL_TREE_CLOSE_INTERVAL_SECS");
        std::env::remove_var("ATL_TREE_LIFETIME_SECS");
    }

    #[test]
    fn test_config_from_env_with_invalid_interval() {
        let _lock = TEST_MUTEX.lock().unwrap();

        std::env::set_var("ATL_TREE_CLOSE_INTERVAL_SECS", "not_a_number");
        std::env::set_var("ATL_TREE_LIFETIME_SECS", "7200");

        let config = TreeCloserConfig::from_env();
        assert_eq!(config.interval_secs, 60); // Falls back to default
        assert_eq!(config.tree_lifetime_secs, 7200);

        // Cleanup
        std::env::remove_var("ATL_TREE_CLOSE_INTERVAL_SECS");
        std::env::remove_var("ATL_TREE_LIFETIME_SECS");
    }

    #[test]
    fn test_config_from_env_with_invalid_lifetime() {
        let _lock = TEST_MUTEX.lock().unwrap();

        std::env::set_var("ATL_TREE_CLOSE_INTERVAL_SECS", "120");
        std::env::set_var("ATL_TREE_LIFETIME_SECS", "invalid");

        let config = TreeCloserConfig::from_env();
        assert_eq!(config.interval_secs, 120);
        assert_eq!(config.tree_lifetime_secs, 3600); // Falls back to default

        // Cleanup
        std::env::remove_var("ATL_TREE_CLOSE_INTERVAL_SECS");
        std::env::remove_var("ATL_TREE_LIFETIME_SECS");
    }

    #[test]
    fn test_config_from_env_with_empty_strings() {
        let _lock = TEST_MUTEX.lock().unwrap();

        std::env::set_var("ATL_TREE_CLOSE_INTERVAL_SECS", "");
        std::env::set_var("ATL_TREE_LIFETIME_SECS", "");

        let config = TreeCloserConfig::from_env();
        assert_eq!(config.interval_secs, 60);
        assert_eq!(config.tree_lifetime_secs, 3600);

        // Cleanup
        std::env::remove_var("ATL_TREE_CLOSE_INTERVAL_SECS");
        std::env::remove_var("ATL_TREE_LIFETIME_SECS");
    }

    #[test]
    fn test_config_clone() {
        let config1 = TreeCloserConfig {
            interval_secs: 100,
            tree_lifetime_secs: 5000,
        };
        let config2 = config1.clone();

        assert_eq!(config1.interval_secs, config2.interval_secs);
        assert_eq!(config1.tree_lifetime_secs, config2.tree_lifetime_secs);
    }

    #[test]
    fn test_config_debug() {
        let config = TreeCloserConfig {
            interval_secs: 100,
            tree_lifetime_secs: 5000,
        };
        let debug_str = format!("{config:?}");
        assert!(debug_str.contains("TreeCloserConfig"));
        assert!(debug_str.contains("100"));
        assert!(debug_str.contains("5000"));
    }
}
