//! Helper functions for formatting data in receipts

use base64::Engine;
use chrono::{DateTime, Utc};

/// Format hash as "sha256:hex"
pub fn format_hash(hash: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(hash))
}

/// Format Ed25519 signature as "base64:..."
pub fn format_signature(sig: &[u8; 64]) -> String {
    format!(
        "base64:{}",
        base64::engine::general_purpose::STANDARD.encode(sig)
    )
}

/// Format timestamp (nanoseconds since Unix epoch) as ISO 8601
#[allow(dead_code)]
pub fn format_timestamp(nanos: u64) -> String {
    let secs = (nanos / 1_000_000_000) as i64;
    let nsecs = (nanos % 1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nsecs)
        .map(|dt: DateTime<Utc>| dt.to_rfc3339())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_hash() {
        let hash = [0u8; 32];
        let formatted = format_hash(&hash);
        assert!(formatted.starts_with("sha256:"));
        assert_eq!(formatted.len(), 7 + 64); // "sha256:" + 64 hex chars
    }

    #[test]
    fn test_format_signature() {
        let sig = [0u8; 64];
        let formatted = format_signature(&sig);
        assert!(formatted.starts_with("base64:"));
    }

    #[test]
    fn test_format_timestamp() {
        let nanos = 1_704_278_400_000_000_000u64;
        let formatted = format_timestamp(nanos);
        assert!(formatted.contains("2024"));
    }
}
