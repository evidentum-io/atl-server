// File: src/background/config.rs

use super::ots_job::OtsJobConfig;
use super::tree_closer::TreeCloserConfig;
use super::tsa_job::TsaJobConfig;

/// Global background jobs configuration
#[derive(Debug, Clone, Default)]
pub struct BackgroundConfig {
    /// Disable all background jobs (for testing)
    pub disabled: bool,

    pub tree_closer: TreeCloserConfig,
    pub tsa_job: TsaJobConfig,
    pub ots_job: OtsJobConfig,
}

impl BackgroundConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        Self {
            disabled: std::env::var("ATL_BACKGROUND_DISABLED")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            tree_closer: TreeCloserConfig::from_env(),
            tsa_job: TsaJobConfig::from_env(),
            ots_job: OtsJobConfig::from_env(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    /// Helper to safely set/unset environment variable
    fn with_env<F>(key: &str, value: Option<&str>, f: F)
    where
        F: FnOnce(),
    {
        let original = std::env::var(key).ok();

        if let Some(v) = value {
            std::env::set_var(key, v);
        } else {
            std::env::remove_var(key);
        }

        f();

        // Restore original state
        match original {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
    }

    #[test]
    fn test_default_construction() {
        let config = BackgroundConfig::default();

        assert!(!config.disabled, "disabled should be false by default");
        // Sub-configs use their own defaults (validated in their respective test modules)
    }

    #[test]
    fn test_clone() {
        let config1 = BackgroundConfig {
            disabled: true,
            tree_closer: TreeCloserConfig::default(),
            tsa_job: TsaJobConfig::default(),
            ots_job: OtsJobConfig::default(),
        };

        let config2 = config1.clone();

        assert_eq!(config1.disabled, config2.disabled);
    }

    #[test]
    fn test_debug_format() {
        let config = BackgroundConfig::default();
        let debug_output = format!("{:?}", config);

        assert!(debug_output.contains("BackgroundConfig"));
        assert!(debug_output.contains("disabled"));
    }

    #[test]
    #[serial]
    fn test_from_env_disabled_not_set() {
        with_env("ATL_BACKGROUND_DISABLED", None, || {
            let config = BackgroundConfig::from_env();
            assert!(!config.disabled, "disabled should default to false");
        });
    }

    #[test]
    #[serial]
    fn test_from_env_disabled_true() {
        with_env("ATL_BACKGROUND_DISABLED", Some("true"), || {
            let config = BackgroundConfig::from_env();
            assert!(
                config.disabled,
                "disabled should be true when set to 'true'"
            );
        });
    }

    #[test]
    #[serial]
    fn test_from_env_disabled_1() {
        with_env("ATL_BACKGROUND_DISABLED", Some("1"), || {
            let config = BackgroundConfig::from_env();
            assert!(config.disabled, "disabled should be true when set to '1'");
        });
    }

    #[test]
    #[serial]
    fn test_from_env_disabled_false() {
        with_env("ATL_BACKGROUND_DISABLED", Some("false"), || {
            let config = BackgroundConfig::from_env();
            assert!(
                !config.disabled,
                "disabled should be false when set to 'false'"
            );
        });
    }

    #[test]
    #[serial]
    fn test_from_env_disabled_0() {
        with_env("ATL_BACKGROUND_DISABLED", Some("0"), || {
            let config = BackgroundConfig::from_env();
            assert!(!config.disabled, "disabled should be false when set to '0'");
        });
    }

    #[test]
    #[serial]
    fn test_from_env_disabled_invalid_value() {
        with_env("ATL_BACKGROUND_DISABLED", Some("invalid"), || {
            let config = BackgroundConfig::from_env();
            assert!(
                !config.disabled,
                "disabled should default to false on invalid value"
            );
        });
    }

    #[test]
    #[serial]
    fn test_from_env_disabled_empty_string() {
        with_env("ATL_BACKGROUND_DISABLED", Some(""), || {
            let config = BackgroundConfig::from_env();
            assert!(
                !config.disabled,
                "disabled should be false for empty string"
            );
        });
    }

    #[test]
    #[serial]
    fn test_from_env_creates_sub_configs() {
        with_env("ATL_BACKGROUND_DISABLED", None, || {
            let config = BackgroundConfig::from_env();

            // Verify that sub-configs are properly initialized
            // (Their specific values are tested in their own modules)
            assert!(
                config.tree_closer.interval_secs > 0,
                "tree_closer should be initialized"
            );
            assert!(
                config.tsa_job.timeout_ms > 0,
                "tsa_job should be initialized"
            );
            assert!(
                config.ots_job.interval_secs > 0,
                "ots_job should be initialized"
            );
        });
    }

    #[test]
    #[serial]
    fn test_from_env_case_sensitive() {
        // Environment variable names are case-sensitive
        with_env("atl_background_disabled", Some("true"), || {
            let config = BackgroundConfig::from_env();
            assert!(
                !config.disabled,
                "lowercase env var should not be recognized"
            );
        });
    }

    #[test]
    #[serial]
    fn test_from_env_with_whitespace() {
        with_env("ATL_BACKGROUND_DISABLED", Some(" true "), || {
            let config = BackgroundConfig::from_env();
            assert!(
                !config.disabled,
                "whitespace should not be trimmed, resulting in false"
            );
        });
    }

    #[test]
    #[serial]
    fn test_from_env_disabled_true_uppercase() {
        with_env("ATL_BACKGROUND_DISABLED", Some("TRUE"), || {
            let config = BackgroundConfig::from_env();
            assert!(
                !config.disabled,
                "uppercase TRUE should not match, expecting lowercase 'true'"
            );
        });
    }

    #[test]
    #[serial]
    fn test_multiple_from_env_calls() {
        // Ensure from_env() can be called multiple times and reads fresh values
        with_env("ATL_BACKGROUND_DISABLED", Some("true"), || {
            let config1 = BackgroundConfig::from_env();
            assert!(config1.disabled);

            std::env::set_var("ATL_BACKGROUND_DISABLED", "false");
            let config2 = BackgroundConfig::from_env();
            assert!(!config2.disabled);

            std::env::remove_var("ATL_BACKGROUND_DISABLED");
        });
    }

    #[test]
    fn test_disabled_field_is_public() {
        let config = BackgroundConfig {
            disabled: true,
            ..Default::default()
        };
        assert!(
            config.disabled,
            "disabled field should be public and mutable"
        );
    }

    #[test]
    fn test_sub_config_fields_are_public() {
        let mut config = BackgroundConfig::default();

        // Verify all sub-config fields are accessible
        let _ = &config.tree_closer;
        let _ = &config.tsa_job;
        let _ = &config.ots_job;

        // Verify they are mutable
        config.tree_closer = TreeCloserConfig::default();
        config.tsa_job = TsaJobConfig::default();
        config.ots_job = OtsJobConfig::default();
    }
}
