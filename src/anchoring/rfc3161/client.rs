//! Synchronous RFC 3161 client

#![allow(dead_code)]

#[cfg(feature = "rfc3161")]
use crate::anchoring::error::AnchorError;
#[cfg(feature = "rfc3161")]
use crate::anchoring::rfc3161::asn1::{
    build_timestamp_request, parse_timestamp_response, verify_message_imprint,
};
#[cfg(feature = "rfc3161")]
use crate::anchoring::rfc3161::types::{TsaConfig, TsaResponse};
#[cfg(feature = "rfc3161")]
use crate::error::ServerResult;
#[cfg(feature = "rfc3161")]
use crate::traits::anchor::{Anchor, AnchorRequest, AnchorResult, AnchorType, Anchorer};
#[cfg(feature = "rfc3161")]
use std::time::Duration;

/// Synchronous RFC 3161 TSA client
///
/// Implements the `Anchorer` trait for blocking I/O contexts.
#[cfg(feature = "rfc3161")]
pub struct Rfc3161Client {
    /// TSA configuration
    config: TsaConfig,

    /// HTTP client for blocking requests
    client: reqwest::blocking::Client,
}

#[cfg(feature = "rfc3161")]
impl Rfc3161Client {
    /// Create a new RFC 3161 client with custom configuration
    pub fn new(config: TsaConfig) -> Result<Self, AnchorError> {
        let timeout = Duration::from_millis(config.timeout_ms);

        let client = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| AnchorError::Network(e.to_string()))?;

        Ok(Self { config, client })
    }

    /// Create client with FreeTSA.org default configuration
    pub fn free_tsa() -> Result<Self, AnchorError> {
        Self::new(TsaConfig::free_tsa())
    }

    /// Request timestamp from a specific TSA URL
    fn timestamp_from_url(
        &self,
        tsa_url: &str,
        hash: &[u8; 32],
    ) -> Result<TsaResponse, AnchorError> {
        tracing::debug!(tsa_url = %tsa_url, "Requesting timestamp");

        let request = build_timestamp_request(hash)?;

        let response = self
            .client
            .post(tsa_url)
            .header("Content-Type", "application/timestamp-query")
            .body(request)
            .send()?;

        if !response.status().is_success() {
            return Err(AnchorError::ServiceError(format!(
                "TSA returned status {}",
                response.status()
            )));
        }

        let body = response.bytes()?.to_vec();

        let tsa_response = parse_timestamp_response(&body)?;

        // Verify message imprint
        verify_message_imprint(&tsa_response.token_der, hash)?;

        tracing::info!(tsa_url = %tsa_url, "Timestamp received successfully");

        Ok(tsa_response)
    }

    /// Try to get timestamp with fallback to multiple TSA URLs
    fn timestamp_with_fallback(
        &self,
        hash: &[u8; 32],
    ) -> Result<(String, TsaResponse), AnchorError> {
        if self.config.urls.is_empty() {
            return Err(AnchorError::NotConfigured("no TSA URLs configured".into()));
        }

        let mut last_error = None;

        for url in &self.config.urls {
            match self.timestamp_from_url(url, hash) {
                Ok(response) => return Ok((url.clone(), response)),
                Err(e) => {
                    tracing::warn!(tsa_url = %url, error = %e, "TSA request failed");
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| AnchorError::ServiceError("all TSAs failed".into())))
    }
}

#[cfg(feature = "rfc3161")]
impl Anchorer for Rfc3161Client {
    fn anchor_type(&self) -> AnchorType {
        AnchorType::Rfc3161
    }

    fn anchor(&self, request: &AnchorRequest) -> ServerResult<AnchorResult> {
        let (tsa_url, response) = self.timestamp_with_fallback(&request.hash)?;

        Ok(AnchorResult {
            anchor: Anchor {
                anchor_type: AnchorType::Rfc3161,
                target: "data_tree_root".to_string(),
                anchored_hash: request.hash,
                tree_size: request.tree_size,
                super_tree_size: None,
                timestamp: response.timestamp,
                token: response.token_der,
                metadata: serde_json::json!({
                    "tsa_url": tsa_url,
                }),
            },
            is_final: true,
            estimated_finality_secs: None,
        })
    }

    fn verify(&self, anchor: &Anchor) -> ServerResult<()> {
        if anchor.anchor_type != AnchorType::Rfc3161 {
            return Err(AnchorError::TokenInvalid("not an RFC 3161 anchor".into()).into());
        }

        verify_message_imprint(&anchor.token, &anchor.anchored_hash)?;

        Ok(())
    }

    fn is_final(&self, _anchor: &Anchor) -> ServerResult<bool> {
        Ok(true)
    }

    fn service_id(&self) -> &str {
        if self.config.urls.is_empty() {
            "rfc3161"
        } else {
            &self.config.urls[0]
        }
    }
}

#[cfg(not(feature = "rfc3161"))]
pub struct Rfc3161Client;

#[cfg(test)]
#[cfg(feature = "rfc3161")]
mod tests {
    use super::*;
    use sha2::Digest;

    #[test]
    fn test_client_creation_success() {
        let config = TsaConfig {
            urls: vec!["https://test.example.com/tsr".to_string()],
            timeout_ms: 5000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let result = Rfc3161Client::new(config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_client_creation_free_tsa() {
        let result = Rfc3161Client::free_tsa();
        assert!(result.is_ok());
        let client = result.unwrap();
        assert_eq!(client.config.urls.len(), 1);
        assert_eq!(client.config.urls[0], "https://freetsa.org/tsr");
    }

    #[test]
    fn test_anchor_type() {
        let client = Rfc3161Client::free_tsa().unwrap();
        assert_eq!(client.anchor_type(), AnchorType::Rfc3161);
    }

    #[test]
    fn test_service_id_with_urls() {
        let config = TsaConfig {
            urls: vec![
                "https://tsa1.example.com/tsr".to_string(),
                "https://tsa2.example.com/tsr".to_string(),
            ],
            timeout_ms: 5000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let client = Rfc3161Client::new(config).unwrap();
        assert_eq!(client.service_id(), "https://tsa1.example.com/tsr");
    }

    #[test]
    fn test_service_id_no_urls() {
        let config = TsaConfig {
            urls: vec![],
            timeout_ms: 5000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let client = Rfc3161Client::new(config).unwrap();
        assert_eq!(client.service_id(), "rfc3161");
    }

    #[test]
    fn test_is_final_always_true() {
        let client = Rfc3161Client::free_tsa().unwrap();
        let anchor = Anchor {
            anchor_type: AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: [0u8; 32],
            tree_size: 1,
            super_tree_size: None,
            timestamp: 1000000000,
            token: vec![1, 2, 3],
            metadata: serde_json::json!({}),
        };
        let result = client.is_final(&anchor);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_verify_wrong_anchor_type() {
        let client = Rfc3161Client::free_tsa().unwrap();
        let anchor = Anchor {
            anchor_type: AnchorType::BitcoinOts,
            target: "data_tree_root".to_string(),
            anchored_hash: [0u8; 32],
            tree_size: 1,
            super_tree_size: None,
            timestamp: 1000000000,
            token: vec![1, 2, 3],
            metadata: serde_json::json!({}),
        };
        let result = client.verify(&anchor);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("not an RFC 3161 anchor"));
    }

    #[test]
    fn test_verify_invalid_token() {
        let client = Rfc3161Client::free_tsa().unwrap();
        let anchor = Anchor {
            anchor_type: AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: [0u8; 32],
            tree_size: 1,
            super_tree_size: None,
            timestamp: 1000000000,
            token: vec![0xDE, 0xAD, 0xBE, 0xEF], // Invalid DER
            metadata: serde_json::json!({}),
        };
        let result = client.verify(&anchor);
        assert!(result.is_err());
    }

    #[test]
    fn test_timestamp_with_fallback_no_urls() {
        let config = TsaConfig {
            urls: vec![],
            timeout_ms: 5000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let client = Rfc3161Client::new(config).unwrap();
        let hash = [42u8; 32];
        let result = client.timestamp_with_fallback(&hash);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            AnchorError::NotConfigured(msg) => {
                assert!(msg.contains("no TSA URLs configured"));
            }
            _ => panic!("Expected NotConfigured error"),
        }
    }

    #[test]
    fn test_config_default() {
        let config = TsaConfig::default();
        assert_eq!(config.urls.len(), 1);
        assert_eq!(config.urls[0], "https://freetsa.org/tsr");
        assert_eq!(config.timeout_ms, 30_000);
        assert!(config.username.is_none());
        assert!(config.password.is_none());
        assert!(config.ca_cert.is_none());
    }

    #[test]
    fn test_config_free_tsa() {
        let config = TsaConfig::free_tsa();
        assert_eq!(config.urls.len(), 1);
        assert_eq!(config.urls[0], "https://freetsa.org/tsr");
    }

    #[test]
    fn test_config_with_fallback() {
        let urls = vec![
            "https://tsa1.example.com/tsr".to_string(),
            "https://tsa2.example.com/tsr".to_string(),
        ];
        let config = TsaConfig::with_fallback(urls.clone(), 10_000);
        assert_eq!(config.urls, urls);
        assert_eq!(config.timeout_ms, 10_000);
    }

    #[test]
    fn test_anchor_request_structure() {
        let hash = sha2::Sha256::digest(b"test data").into();
        let request = AnchorRequest {
            hash,
            tree_size: 42,
            metadata: Some(serde_json::json!({"key": "value"})),
        };
        assert_eq!(request.tree_size, 42);
        assert!(request.metadata.is_some());
    }

    #[test]
    #[ignore] // Requires network access
    fn test_freetsa_request() {
        let client = Rfc3161Client::free_tsa().unwrap();
        let hash = sha2::Sha256::digest(b"test data").into();

        let request = AnchorRequest {
            hash,
            tree_size: 1,
            metadata: None,
        };

        let result = client.anchor(&request);
        assert!(result.is_ok());
    }

    #[test]
    fn test_client_timeout_configuration() {
        let config = TsaConfig {
            urls: vec!["https://test.example.com/tsr".to_string()],
            timeout_ms: 1000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let result = Rfc3161Client::new(config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_tsa_response_structure() {
        let response = TsaResponse {
            token_der: vec![1, 2, 3, 4],
            timestamp: 1705000000000000000,
        };
        assert_eq!(response.token_der.len(), 4);
        assert!(response.timestamp > 0);
    }
}
