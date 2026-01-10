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
                "https://a.pool.opentimestamps.org".to_string(),
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
}
