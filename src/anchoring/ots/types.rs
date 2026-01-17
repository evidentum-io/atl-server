//! OpenTimestamps types and configuration

use serde::{Deserialize, Serialize};

/// OTS proof status (derived from attestations)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OtsStatus {
    /// Proof submitted, waiting for Bitcoin confirmation
    Pending { calendar_url: String },

    /// Proof confirmed in Bitcoin blockchain
    Confirmed { block_height: u64, block_time: u64 },
}

/// OpenTimestamps configuration
#[derive(Debug, Clone)]
pub struct OtsConfig {
    /// Calendar server URLs (tried in order)
    pub calendar_urls: Vec<String>,

    /// Request timeout in seconds
    pub timeout_secs: u64,

    /// Minimum Bitcoin confirmations for finality
    pub min_confirmations: u64,
}

impl Default for OtsConfig {
    fn default() -> Self {
        Self {
            calendar_urls: vec![
                "https://alice.btc.calendar.opentimestamps.org".to_string(),
                "https://bob.btc.calendar.opentimestamps.org".to_string(),
                "https://finney.calendar.eternitywall.com".to_string(),
                "https://ots.btc.catallaxy.com".to_string(),
                "https://b.pool.opentimestamps.org".to_string(),
            ],
            timeout_secs: 30,
            min_confirmations: 6,
        }
    }
}

impl OtsConfig {
    /// Create with default calendar servers
    #[must_use]
    pub fn default_calendars() -> Self {
        Self::default()
    }

    /// Create with custom calendar URLs
    #[must_use]
    pub fn with_calendars(urls: Vec<String>) -> Self {
        Self {
            calendar_urls: urls,
            timeout_secs: 30,
            min_confirmations: 6,
        }
    }

    /// Create config from environment variables
    ///
    /// Environment variables:
    /// - `ATL_OTS_CALENDAR_URLS`: Comma-separated list of calendar URLs
    /// - `ATL_OTS_TIMEOUT_SECS`: HTTP timeout in seconds (default: 30)
    /// - `ATL_OTS_MIN_CONFIRMATIONS`: Minimum Bitcoin confirmations (default: 6)
    #[must_use]
    pub fn from_env() -> Self {
        let calendar_urls = std::env::var("ATL_OTS_CALENDAR_URLS")
            .map(|s| {
                s.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_else(|_| {
                vec![
                    "https://alice.btc.calendar.opentimestamps.org".to_string(),
                    "https://bob.btc.calendar.opentimestamps.org".to_string(),
                    "https://finney.calendar.eternitywall.com".to_string(),
                    "https://ots.btc.catallaxy.com".to_string(),
                    "https://b.pool.opentimestamps.org".to_string(),
                ]
            });

        let timeout_secs = std::env::var("ATL_OTS_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        let min_confirmations = std::env::var("ATL_OTS_MIN_CONFIRMATIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(6);

        Self {
            calendar_urls,
            timeout_secs,
            min_confirmations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_ots_config_default() {
        let config = OtsConfig::default();
        assert_eq!(config.calendar_urls.len(), 5);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.min_confirmations, 6);
        assert!(config.calendar_urls[0].contains("alice.btc"));
        assert!(config.calendar_urls[1].contains("bob.btc"));
    }

    #[test]
    fn test_ots_config_default_calendars() {
        let config = OtsConfig::default_calendars();
        assert_eq!(config.calendar_urls.len(), 5);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.min_confirmations, 6);
        assert_eq!(config.calendar_urls, OtsConfig::default().calendar_urls);
    }

    #[test]
    fn test_ots_config_with_calendars_custom() {
        let urls = vec![
            "https://custom1.example.com".to_string(),
            "https://custom2.example.com".to_string(),
        ];
        let config = OtsConfig::with_calendars(urls.clone());

        assert_eq!(config.calendar_urls, urls);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.min_confirmations, 6);
    }

    #[test]
    fn test_ots_config_with_calendars_empty() {
        let config = OtsConfig::with_calendars(vec![]);
        assert_eq!(config.calendar_urls.len(), 0);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.min_confirmations, 6);
    }

    #[test]
    fn test_ots_config_with_calendars_single() {
        let urls = vec!["https://single.example.com".to_string()];
        let config = OtsConfig::with_calendars(urls.clone());

        assert_eq!(config.calendar_urls.len(), 1);
        assert_eq!(config.calendar_urls[0], "https://single.example.com");
    }

    #[test]
    #[serial]
    fn test_ots_config_from_env_defaults() {
        // Clear any existing env vars
        std::env::remove_var("ATL_OTS_CALENDAR_URLS");
        std::env::remove_var("ATL_OTS_TIMEOUT_SECS");
        std::env::remove_var("ATL_OTS_MIN_CONFIRMATIONS");

        let config = OtsConfig::from_env();

        assert_eq!(config.calendar_urls.len(), 5);
        assert!(config.calendar_urls[0].contains("alice.btc"));
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.min_confirmations, 6);
    }

    #[test]
    #[serial]
    fn test_ots_config_from_env_custom_urls() {
        std::env::set_var(
            "ATL_OTS_CALENDAR_URLS",
            "https://test1.com,https://test2.com,https://test3.com",
        );
        std::env::remove_var("ATL_OTS_TIMEOUT_SECS");
        std::env::remove_var("ATL_OTS_MIN_CONFIRMATIONS");

        let config = OtsConfig::from_env();

        assert_eq!(config.calendar_urls.len(), 3);
        assert_eq!(config.calendar_urls[0], "https://test1.com");
        assert_eq!(config.calendar_urls[1], "https://test2.com");
        assert_eq!(config.calendar_urls[2], "https://test3.com");
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.min_confirmations, 6);

        std::env::remove_var("ATL_OTS_CALENDAR_URLS");
    }

    #[test]
    #[serial]
    fn test_ots_config_from_env_custom_timeout() {
        std::env::remove_var("ATL_OTS_CALENDAR_URLS");
        std::env::set_var("ATL_OTS_TIMEOUT_SECS", "60");
        std::env::remove_var("ATL_OTS_MIN_CONFIRMATIONS");

        let config = OtsConfig::from_env();

        assert_eq!(config.timeout_secs, 60);
        assert_eq!(config.calendar_urls.len(), 5);
        assert_eq!(config.min_confirmations, 6);

        std::env::remove_var("ATL_OTS_TIMEOUT_SECS");
    }

    #[test]
    #[serial]
    fn test_ots_config_from_env_custom_confirmations() {
        std::env::remove_var("ATL_OTS_CALENDAR_URLS");
        std::env::remove_var("ATL_OTS_TIMEOUT_SECS");
        std::env::set_var("ATL_OTS_MIN_CONFIRMATIONS", "12");

        let config = OtsConfig::from_env();

        assert_eq!(config.min_confirmations, 12);
        assert_eq!(config.calendar_urls.len(), 5);
        assert_eq!(config.timeout_secs, 30);

        std::env::remove_var("ATL_OTS_MIN_CONFIRMATIONS");
    }

    #[test]
    #[serial]
    fn test_ots_config_from_env_all_custom() {
        std::env::set_var("ATL_OTS_CALENDAR_URLS", "https://custom.com");
        std::env::set_var("ATL_OTS_TIMEOUT_SECS", "120");
        std::env::set_var("ATL_OTS_MIN_CONFIRMATIONS", "3");

        let config = OtsConfig::from_env();

        assert_eq!(config.calendar_urls.len(), 1);
        assert_eq!(config.calendar_urls[0], "https://custom.com");
        assert_eq!(config.timeout_secs, 120);
        assert_eq!(config.min_confirmations, 3);

        std::env::remove_var("ATL_OTS_CALENDAR_URLS");
        std::env::remove_var("ATL_OTS_TIMEOUT_SECS");
        std::env::remove_var("ATL_OTS_MIN_CONFIRMATIONS");
    }

    #[test]
    #[serial]
    fn test_ots_config_from_env_invalid_timeout_uses_default() {
        std::env::remove_var("ATL_OTS_CALENDAR_URLS");
        std::env::set_var("ATL_OTS_TIMEOUT_SECS", "invalid");
        std::env::remove_var("ATL_OTS_MIN_CONFIRMATIONS");

        let config = OtsConfig::from_env();

        assert_eq!(config.timeout_secs, 30);

        std::env::remove_var("ATL_OTS_TIMEOUT_SECS");
    }

    #[test]
    #[serial]
    fn test_ots_config_from_env_invalid_confirmations_uses_default() {
        std::env::remove_var("ATL_OTS_CALENDAR_URLS");
        std::env::remove_var("ATL_OTS_TIMEOUT_SECS");
        std::env::set_var("ATL_OTS_MIN_CONFIRMATIONS", "not_a_number");

        let config = OtsConfig::from_env();

        assert_eq!(config.min_confirmations, 6);

        std::env::remove_var("ATL_OTS_MIN_CONFIRMATIONS");
    }

    #[test]
    #[serial]
    fn test_ots_config_from_env_empty_urls_returns_empty() {
        std::env::set_var("ATL_OTS_CALENDAR_URLS", "");
        std::env::remove_var("ATL_OTS_TIMEOUT_SECS");
        std::env::remove_var("ATL_OTS_MIN_CONFIRMATIONS");

        let config = OtsConfig::from_env();

        // Empty string is treated as explicit empty list (user choice)
        assert_eq!(config.calendar_urls.len(), 0);

        std::env::remove_var("ATL_OTS_CALENDAR_URLS");
    }

    #[test]
    #[serial]
    fn test_ots_config_from_env_urls_with_whitespace() {
        std::env::set_var(
            "ATL_OTS_CALENDAR_URLS",
            " https://test1.com , https://test2.com , https://test3.com ",
        );
        std::env::remove_var("ATL_OTS_TIMEOUT_SECS");
        std::env::remove_var("ATL_OTS_MIN_CONFIRMATIONS");

        let config = OtsConfig::from_env();

        assert_eq!(config.calendar_urls.len(), 3);
        assert_eq!(config.calendar_urls[0], "https://test1.com");
        assert_eq!(config.calendar_urls[1], "https://test2.com");
        assert_eq!(config.calendar_urls[2], "https://test3.com");

        std::env::remove_var("ATL_OTS_CALENDAR_URLS");
    }

    #[test]
    #[serial]
    fn test_ots_config_from_env_urls_with_empty_entries() {
        std::env::set_var("ATL_OTS_CALENDAR_URLS", "https://test1.com,,https://test2.com");
        std::env::remove_var("ATL_OTS_TIMEOUT_SECS");
        std::env::remove_var("ATL_OTS_MIN_CONFIRMATIONS");

        let config = OtsConfig::from_env();

        assert_eq!(config.calendar_urls.len(), 2);
        assert_eq!(config.calendar_urls[0], "https://test1.com");
        assert_eq!(config.calendar_urls[1], "https://test2.com");

        std::env::remove_var("ATL_OTS_CALENDAR_URLS");
    }

    #[test]
    #[serial]
    fn test_ots_config_from_env_zero_timeout() {
        std::env::remove_var("ATL_OTS_CALENDAR_URLS");
        std::env::set_var("ATL_OTS_TIMEOUT_SECS", "0");
        std::env::remove_var("ATL_OTS_MIN_CONFIRMATIONS");

        let config = OtsConfig::from_env();

        assert_eq!(config.timeout_secs, 0);

        std::env::remove_var("ATL_OTS_TIMEOUT_SECS");
    }

    #[test]
    #[serial]
    fn test_ots_config_from_env_zero_confirmations() {
        std::env::remove_var("ATL_OTS_CALENDAR_URLS");
        std::env::remove_var("ATL_OTS_TIMEOUT_SECS");
        std::env::set_var("ATL_OTS_MIN_CONFIRMATIONS", "0");

        let config = OtsConfig::from_env();

        assert_eq!(config.min_confirmations, 0);

        std::env::remove_var("ATL_OTS_MIN_CONFIRMATIONS");
    }

    #[test]
    fn test_ots_status_pending_equality() {
        let status1 = OtsStatus::Pending {
            calendar_url: "https://example.com".to_string(),
        };
        let status2 = OtsStatus::Pending {
            calendar_url: "https://example.com".to_string(),
        };
        let status3 = OtsStatus::Pending {
            calendar_url: "https://other.com".to_string(),
        };

        assert_eq!(status1, status2);
        assert_ne!(status1, status3);
    }

    #[test]
    fn test_ots_status_confirmed_equality() {
        let status1 = OtsStatus::Confirmed {
            block_height: 800000,
            block_time: 1672531200,
        };
        let status2 = OtsStatus::Confirmed {
            block_height: 800000,
            block_time: 1672531200,
        };
        let status3 = OtsStatus::Confirmed {
            block_height: 800001,
            block_time: 1672531200,
        };

        assert_eq!(status1, status2);
        assert_ne!(status1, status3);
    }

    #[test]
    fn test_ots_status_pending_serialization() {
        let status = OtsStatus::Pending {
            calendar_url: "https://example.com".to_string(),
        };
        let json = serde_json::to_string(&status).unwrap();
        let deserialized: OtsStatus = serde_json::from_str(&json).unwrap();

        assert_eq!(status, deserialized);
    }

    #[test]
    fn test_ots_status_confirmed_serialization() {
        let status = OtsStatus::Confirmed {
            block_height: 800000,
            block_time: 1672531200,
        };
        let json = serde_json::to_string(&status).unwrap();
        let deserialized: OtsStatus = serde_json::from_str(&json).unwrap();

        assert_eq!(status, deserialized);
    }

    #[test]
    fn test_ots_status_clone() {
        let status = OtsStatus::Pending {
            calendar_url: "https://example.com".to_string(),
        };
        let cloned = status.clone();

        assert_eq!(status, cloned);
    }

    #[test]
    fn test_ots_config_clone() {
        let config = OtsConfig::default();
        let cloned = config.clone();

        assert_eq!(config.calendar_urls, cloned.calendar_urls);
        assert_eq!(config.timeout_secs, cloned.timeout_secs);
        assert_eq!(config.min_confirmations, cloned.min_confirmations);
    }
}
