//! Formatting helpers for receipt fields

use std::time::{SystemTime, UNIX_EPOCH};

/// Format a hash as "sha256:..."
///
/// # Arguments
/// * `hash` - 32-byte SHA-256 hash
///
/// # Returns
/// * Formatted string "sha256:{64-char hex}"
#[must_use]
#[allow(dead_code)]
pub fn format_hash(hash: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(hash))
}

/// Format a signature as "base64:..."
///
/// # Arguments
/// * `sig` - 64-byte Ed25519 signature
///
/// # Returns
/// * Formatted string "base64:{base64-encoded signature}"
#[must_use]
#[allow(dead_code)]
pub fn format_signature(sig: &[u8; 64]) -> String {
    use base64::Engine;
    format!(
        "base64:{}",
        base64::engine::general_purpose::STANDARD.encode(sig)
    )
}

/// Get current timestamp in nanoseconds since Unix epoch
///
/// # Panics
/// Will panic if system time is before Unix epoch (extremely unlikely)
#[must_use]
#[allow(dead_code)]
pub fn current_timestamp_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_nanos() as u64
}

/// Format timestamp as ISO 8601 string
///
/// # Arguments
/// * `nanos` - Unix timestamp in nanoseconds
///
/// # Returns
/// * ISO 8601 formatted string (RFC 3339)
#[must_use]
pub fn format_timestamp_iso8601(nanos: u64) -> String {
    use chrono::DateTime;
    let secs = (nanos / 1_000_000_000) as i64;
    let nsecs = (nanos % 1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nsecs)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_hash() {
        let hash = [0xABu8; 32];
        let formatted = format_hash(&hash);
        assert!(formatted.starts_with("sha256:"));
        assert_eq!(formatted.len(), 7 + 64); // "sha256:" + 64 hex chars
    }

    #[test]
    fn test_format_signature() {
        let sig = [0xCDu8; 64];
        let formatted = format_signature(&sig);
        assert!(formatted.starts_with("base64:"));
    }

    #[test]
    fn test_current_timestamp_nanos() {
        let ts = current_timestamp_nanos();
        assert!(ts > 0);
        // Should be reasonable (after year 2020)
        assert!(ts > 1_600_000_000_000_000_000);
    }

    #[test]
    fn test_format_timestamp_iso8601() {
        // 2024-01-01 00:00:00 UTC
        let nanos = 1_704_067_200_000_000_000u64;
        let formatted = format_timestamp_iso8601(nanos);
        assert!(formatted.starts_with("2024-01-01"));
    }
}
