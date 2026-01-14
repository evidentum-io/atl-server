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
    fn test_client_creation() {
        let result = Rfc3161Client::free_tsa();
        assert!(result.is_ok());
    }

    #[test]
    #[ignore]
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
}
