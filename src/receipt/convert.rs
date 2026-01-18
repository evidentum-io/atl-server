//! Anchor to receipt format conversion

use atl_core::ReceiptAnchor;
use base64::Engine;

use crate::receipt::format::format_timestamp_iso8601;
use crate::traits::anchor::{Anchor, AnchorType};

/// Convert storage Anchor to receipt format (per IETF ATL Protocol v1.0.0)
///
/// # Arguments
/// * `anchor` - Anchor from storage
///
/// # Returns
/// * `ReceiptAnchor` ready for inclusion in receipt JSON
#[must_use]
pub fn convert_anchor_to_receipt(anchor: &Anchor) -> ReceiptAnchor {
    match anchor.anchor_type {
        AnchorType::Rfc3161 => ReceiptAnchor::Rfc3161 {
            target: anchor.target.clone(),
            target_hash: format!("sha256:{}", hex::encode(anchor.anchored_hash)),
            tsa_url: anchor
                .metadata
                .get("tsa_url")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_string(),
            timestamp: format_timestamp_iso8601(anchor.timestamp),
            token_der: format!(
                "base64:{}",
                base64::engine::general_purpose::STANDARD.encode(&anchor.token)
            ),
        },
        AnchorType::BitcoinOts => {
            // Per IETF ATL Protocol v1.0.0: bitcoin_ots anchor format
            let block_time = anchor
                .metadata
                .get("bitcoin_block_time")
                .and_then(|v| {
                    if let Some(ts) = v.as_i64() {
                        // Convert Unix timestamp (seconds) to ISO 8601
                        chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.to_rfc3339())
                    } else {
                        // If already a string, use as-is
                        v.as_str().map(std::string::ToString::to_string)
                    }
                })
                .unwrap_or_default();

            ReceiptAnchor::BitcoinOts {
                target: anchor.target.clone(),
                target_hash: format!("sha256:{}", hex::encode(anchor.anchored_hash)),
                timestamp: if block_time.is_empty() {
                    format_timestamp_iso8601(anchor.timestamp)
                } else {
                    block_time.clone()
                },
                bitcoin_block_height: anchor
                    .metadata
                    .get("bitcoin_block_height")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                bitcoin_block_time: block_time,
                ots_proof: format!(
                    "base64:{}",
                    base64::engine::general_purpose::STANDARD.encode(&anchor.token)
                ),
            }
        }
        AnchorType::Other => {
            // Other anchors fallback to RFC 3161 format
            ReceiptAnchor::Rfc3161 {
                target: anchor.target.clone(),
                target_hash: format!("sha256:{}", hex::encode(anchor.anchored_hash)),
                tsa_url: anchor
                    .metadata
                    .get("tsa_url")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                timestamp: format_timestamp_iso8601(anchor.timestamp),
                token_der: format!(
                    "base64:{}",
                    base64::engine::general_purpose::STANDARD.encode(&anchor.token)
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_rfc3161_anchor() {
        let anchor = Anchor {
            anchor_type: AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: [0u8; 32],
            tree_size: 100,
            super_tree_size: None,
            timestamp: 1_704_067_200_000_000_000, // 2024-01-01
            token: vec![1, 2, 3, 4],
            metadata: serde_json::json!({
                "tsa_url": "https://freetsa.org/tsr"
            }),
        };

        let receipt_anchor = convert_anchor_to_receipt(&anchor);

        match receipt_anchor {
            ReceiptAnchor::Rfc3161 {
                tsa_url,
                timestamp,
                token_der,
                target,
                ..
            } => {
                assert_eq!(target, "data_tree_root");
                assert_eq!(tsa_url, "https://freetsa.org/tsr");
                assert!(timestamp.starts_with("2024-01-01"));
                assert!(token_der.starts_with("base64:"));
            }
            _ => panic!("Expected Rfc3161 anchor"),
        }
    }

    #[test]
    fn test_convert_bitcoin_ots_anchor() {
        let anchor = Anchor {
            anchor_type: AnchorType::BitcoinOts,
            target: "super_root".to_string(),
            anchored_hash: [1u8; 32],
            tree_size: 0,
            super_tree_size: Some(5),
            timestamp: 1_704_067_200_000_000_000,
            token: vec![5, 6, 7, 8],
            metadata: serde_json::json!({
                "bitcoin_block_height": 825000,
                "bitcoin_block_time": "2024-01-01T12:00:00Z"
            }),
        };

        let receipt_anchor = convert_anchor_to_receipt(&anchor);

        match receipt_anchor {
            ReceiptAnchor::BitcoinOts {
                target,
                timestamp,
                bitcoin_block_height,
                bitcoin_block_time,
                ots_proof,
                ..
            } => {
                assert_eq!(target, "super_root");
                assert_eq!(timestamp, "2024-01-01T12:00:00Z");
                assert_eq!(bitcoin_block_height, 825000);
                assert_eq!(bitcoin_block_time, "2024-01-01T12:00:00Z");
                assert!(ots_proof.starts_with("base64:"));
            }
            _ => panic!("Expected BitcoinOts anchor"),
        }
    }

    #[test]
    fn test_convert_bitcoin_ots_anchor_with_unix_timestamp() {
        let anchor = Anchor {
            anchor_type: AnchorType::BitcoinOts,
            target: "super_root".to_string(),
            anchored_hash: [2u8; 32],
            tree_size: 0,
            super_tree_size: Some(10),
            timestamp: 1_704_067_200_000_000_000,
            token: vec![9, 10, 11, 12],
            metadata: serde_json::json!({
                "bitcoin_block_height": 830000,
                "bitcoin_block_time": 1_704_110_400  // Unix timestamp (seconds)
            }),
        };

        let receipt_anchor = convert_anchor_to_receipt(&anchor);

        match receipt_anchor {
            ReceiptAnchor::BitcoinOts {
                target,
                timestamp,
                bitcoin_block_height,
                bitcoin_block_time,
                ots_proof,
                ..
            } => {
                assert_eq!(target, "super_root");
                assert_eq!(bitcoin_block_height, 830000);
                // Check that Unix timestamp was converted to ISO 8601
                assert!(bitcoin_block_time.starts_with("2024-01-"));
                assert_eq!(timestamp, bitcoin_block_time);
                assert!(ots_proof.starts_with("base64:"));
            }
            _ => panic!("Expected BitcoinOts anchor"),
        }
    }

    #[test]
    fn test_convert_rfc3161_anchor_missing_tsa_url() {
        let anchor = Anchor {
            anchor_type: AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: [3u8; 32],
            tree_size: 200,
            super_tree_size: None,
            timestamp: 1_704_067_200_000_000_000,
            token: vec![13, 14, 15, 16],
            metadata: serde_json::json!({}),
        };

        let receipt_anchor = convert_anchor_to_receipt(&anchor);

        match receipt_anchor {
            ReceiptAnchor::Rfc3161 {
                tsa_url,
                timestamp,
                token_der,
                ..
            } => {
                assert_eq!(tsa_url, "");
                assert!(timestamp.starts_with("2024-01-01"));
                assert!(token_der.starts_with("base64:"));
            }
            _ => panic!("Expected Rfc3161 anchor"),
        }
    }

    #[test]
    fn test_convert_bitcoin_ots_missing_metadata() {
        let anchor = Anchor {
            anchor_type: AnchorType::BitcoinOts,
            target: "super_root".to_string(),
            anchored_hash: [4u8; 32],
            tree_size: 0,
            super_tree_size: Some(15),
            timestamp: 1_704_067_200_000_000_000,
            token: vec![17, 18, 19, 20],
            metadata: serde_json::json!({}),
        };

        let receipt_anchor = convert_anchor_to_receipt(&anchor);

        match receipt_anchor {
            ReceiptAnchor::BitcoinOts {
                target,
                timestamp,
                bitcoin_block_height,
                bitcoin_block_time,
                ots_proof,
                ..
            } => {
                assert_eq!(target, "super_root");
                assert_eq!(bitcoin_block_height, 0);
                assert_eq!(bitcoin_block_time, "");
                assert!(timestamp.starts_with("2024-01-01"));
                assert!(ots_proof.starts_with("base64:"));
            }
            _ => panic!("Expected BitcoinOts anchor"),
        }
    }

    #[test]
    fn test_convert_other_anchor_type() {
        let anchor = Anchor {
            anchor_type: AnchorType::Other,
            target: "custom_target".to_string(),
            anchored_hash: [5u8; 32],
            tree_size: 300,
            super_tree_size: None,
            timestamp: 1_704_067_200_000_000_000,
            token: vec![21, 22, 23, 24],
            metadata: serde_json::json!({
                "tsa_url": "https://custom.tsa.org"
            }),
        };

        let receipt_anchor = convert_anchor_to_receipt(&anchor);

        match receipt_anchor {
            ReceiptAnchor::Rfc3161 {
                target,
                tsa_url,
                timestamp,
                token_der,
                ..
            } => {
                assert_eq!(target, "custom_target");
                assert_eq!(tsa_url, "https://custom.tsa.org");
                assert!(timestamp.starts_with("2024-01-01"));
                assert!(token_der.starts_with("base64:"));
            }
            _ => panic!("Expected Rfc3161 anchor (fallback for Other type)"),
        }
    }
}
