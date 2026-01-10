//! Asynchronous RFC 3161 TSA client

#![allow(dead_code)]

#[cfg(feature = "rfc3161")]
use crate::anchoring::error::AnchorError;
#[cfg(feature = "rfc3161")]
use crate::anchoring::rfc3161::asn1::{build_timestamp_request, parse_timestamp_response};
#[cfg(feature = "rfc3161")]
use crate::anchoring::rfc3161::types::{TsaConfig, TsaResponse};
#[cfg(feature = "rfc3161")]
use async_trait::async_trait;
#[cfg(feature = "rfc3161")]
use std::time::Duration;

/// Async TSA client trait for background job integration
#[cfg(feature = "rfc3161")]
#[async_trait]
pub trait TsaClient: Send + Sync {
    /// Request timestamp for a hash from a specific TSA URL
    async fn timestamp(
        &self,
        tsa_url: &str,
        hash: &[u8; 32],
        timeout_ms: u64,
    ) -> Result<TsaResponse, AnchorError>;

    /// Verify a TSA response
    fn verify(&self, response: &TsaResponse, expected_hash: &[u8; 32]) -> Result<(), AnchorError>;
}

/// Async RFC 3161 client implementation
#[cfg(feature = "rfc3161")]
pub struct AsyncRfc3161Client {
    /// HTTP client (reqwest with async runtime)
    client: reqwest::Client,
}

#[cfg(feature = "rfc3161")]
impl AsyncRfc3161Client {
    /// Create a new async TSA client
    pub fn new() -> Result<Self, AnchorError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| AnchorError::Network(e.to_string()))?;

        Ok(Self { client })
    }

    /// Create with custom configuration
    pub fn with_config(config: &TsaConfig) -> Result<Self, AnchorError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(|e| AnchorError::Network(e.to_string()))?;

        Ok(Self { client })
    }
}

#[cfg(feature = "rfc3161")]
impl Default for AsyncRfc3161Client {
    fn default() -> Self {
        Self::new().expect("failed to create default async client")
    }
}

#[cfg(feature = "rfc3161")]
#[async_trait]
impl TsaClient for AsyncRfc3161Client {
    async fn timestamp(
        &self,
        tsa_url: &str,
        hash: &[u8; 32],
        timeout_ms: u64,
    ) -> Result<TsaResponse, AnchorError> {
        tracing::debug!(tsa_url = %tsa_url, "Async timestamp request");

        let tsa_req = build_timestamp_request(hash)?;

        let response = self
            .client
            .post(tsa_url)
            .header("Content-Type", "application/timestamp-query")
            .timeout(Duration::from_millis(timeout_ms))
            .body(tsa_req)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(AnchorError::ServiceError(format!(
                "TSA returned status {}",
                response.status()
            )));
        }

        let body = response.bytes().await?.to_vec();

        let tsa_response = parse_timestamp_response(&body)?;

        tracing::info!(tsa_url = %tsa_url, "Async timestamp received");

        Ok(tsa_response)
    }

    fn verify(&self, response: &TsaResponse, expected_hash: &[u8; 32]) -> Result<(), AnchorError> {
        crate::anchoring::rfc3161::asn1::verify_message_imprint(&response.token_der, expected_hash)
    }
}

#[cfg(not(feature = "rfc3161"))]
pub struct AsyncRfc3161Client;

#[cfg(test)]
#[cfg(feature = "rfc3161")]
mod tests {
    use super::*;
    use sha2::Digest;

    #[test]
    fn test_async_client_creation() {
        let result = AsyncRfc3161Client::new();
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_async_freetsa_request() {
        let client = AsyncRfc3161Client::new().unwrap();
        let hash = sha2::Sha256::digest(b"test async data").into();

        let result = client
            .timestamp("https://freetsa.org/tsr", &hash, 30_000)
            .await;

        assert!(result.is_ok());
    }
}
