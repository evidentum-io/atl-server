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

    #[test]
    fn test_ots_config_default() {
        let config = OtsConfig::default();
        assert_eq!(config.calendar_urls.len(), 5);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.min_confirmations, 6);
    }

    #[test]
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
}
