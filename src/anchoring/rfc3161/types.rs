//! RFC 3161 types and configuration

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// TSA response for receipt generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsaResponse {
    /// Raw TimeStampResp (DER encoded)
    pub token_der: Vec<u8>,

    /// Parsed timestamp (nanoseconds since Unix epoch)
    pub timestamp: u64,
}

/// TSA anchor for inclusion in receipts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsaAnchor {
    /// URL of the TSA that signed this timestamp
    pub tsa_url: String,

    /// Raw RFC 3161 TimeStampResp (DER encoded)
    pub tsa_response: Vec<u8>,

    /// Timestamp from TSA (nanoseconds since Unix epoch)
    pub timestamp: u64,
}

/// TSA client configuration
#[derive(Debug, Clone)]
pub struct TsaConfig {
    /// List of TSA URLs to try (fallback order)
    pub urls: Vec<String>,

    /// Timeout per TSA request in milliseconds
    pub timeout_ms: u64,

    /// Optional HTTP basic auth username
    pub username: Option<String>,

    /// Optional HTTP basic auth password
    pub password: Option<String>,

    /// Optional CA certificate (PEM) for verification
    pub ca_cert: Option<String>,
}

impl Default for TsaConfig {
    fn default() -> Self {
        Self {
            urls: vec!["https://freetsa.org/tsr".to_string()],
            timeout_ms: 30_000,
            username: None,
            password: None,
            ca_cert: None,
        }
    }
}

impl TsaConfig {
    /// Create config with FreeTSA.org as default
    #[must_use]
    pub fn free_tsa() -> Self {
        Self::default()
    }

    /// Create config with multiple fallback TSAs
    #[must_use]
    pub fn with_fallback(urls: Vec<String>, timeout_ms: u64) -> Self {
        Self {
            urls,
            timeout_ms,
            username: None,
            password: None,
            ca_cert: None,
        }
    }
}
