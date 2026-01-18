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

    #[test]
    fn test_async_client_with_config() {
        let config = TsaConfig {
            urls: vec!["https://test.example.com/tsr".to_string()],
            timeout_ms: 5000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let result = AsyncRfc3161Client::with_config(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_async_client_with_config_timeout() {
        let config = TsaConfig {
            urls: vec![],
            timeout_ms: 1000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let result = AsyncRfc3161Client::with_config(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_async_client_default() {
        let _client = AsyncRfc3161Client::default();
        // Should not panic
    }

    #[tokio::test]
    async fn test_async_timestamp_invalid_url() {
        let client = AsyncRfc3161Client::new().unwrap();
        let hash = [42u8; 32];

        let result = client
            .timestamp(
                "http://invalid-tsa-url-that-does-not-exist.example",
                &hash,
                1000,
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_async_timestamp_timeout() {
        let client = AsyncRfc3161Client::new().unwrap();
        let hash = [42u8; 32];

        // Use a non-routable IP to force timeout
        let result = client
            .timestamp("http://192.0.2.1:9999/tsr", &hash, 100)
            .await;

        assert!(result.is_err());
    }

    #[test]
    fn test_verify_with_invalid_token() {
        let client = AsyncRfc3161Client::new().unwrap();
        let hash = [42u8; 32];
        let response = TsaResponse {
            token_der: vec![0xDE, 0xAD, 0xBE, 0xEF], // Invalid DER
            timestamp: 1000000000,
        };

        let result = client.verify(&response, &hash);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_with_wrong_hash() {
        let client = AsyncRfc3161Client::new().unwrap();
        let _hash = [42u8; 32];
        let wrong_hash = [99u8; 32];

        // Create a minimal valid DER structure (this will still fail verification)
        let response = TsaResponse {
            token_der: vec![0x30, 0x03, 0x02, 0x01, 0x00], // Minimal SEQUENCE
            timestamp: 1000000000,
        };

        let result = client.verify(&response, &wrong_hash);
        assert!(result.is_err());
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

    // Mock TSA client for testing service layer
    pub(crate) struct MockTsaClient {
        pub should_fail: bool,
        pub response: Option<TsaResponse>,
    }

    #[async_trait]
    impl TsaClient for MockTsaClient {
        async fn timestamp(
            &self,
            _tsa_url: &str,
            _hash: &[u8; 32],
            _timeout_ms: u64,
        ) -> Result<TsaResponse, AnchorError> {
            if self.should_fail {
                Err(AnchorError::ServiceError("mock failure".to_string()))
            } else {
                Ok(self.response.clone().unwrap_or(TsaResponse {
                    token_der: vec![0x30, 0x03, 0x02, 0x01, 0x00],
                    timestamp: 1705000000000000000,
                }))
            }
        }

        fn verify(
            &self,
            _response: &TsaResponse,
            _expected_hash: &[u8; 32],
        ) -> Result<(), AnchorError> {
            if self.should_fail {
                Err(AnchorError::TokenInvalid(
                    "mock verification failure".to_string(),
                ))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn test_mock_client_success() {
        let mock = MockTsaClient {
            should_fail: false,
            response: Some(TsaResponse {
                token_der: vec![1, 2, 3, 4],
                timestamp: 1705000000000000000,
            }),
        };

        let hash = [42u8; 32];
        let result = mock
            .timestamp("http://mock.example.com/tsr", &hash, 5000)
            .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.token_der, vec![1, 2, 3, 4]);
        assert_eq!(response.timestamp, 1705000000000000000);
    }

    #[tokio::test]
    async fn test_mock_client_failure() {
        let mock = MockTsaClient {
            should_fail: true,
            response: None,
        };

        let hash = [42u8; 32];
        let result = mock
            .timestamp("http://mock.example.com/tsr", &hash, 5000)
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            AnchorError::ServiceError(msg) => assert_eq!(msg, "mock failure"),
            _ => panic!("Expected ServiceError"),
        }
    }

    #[test]
    fn test_mock_client_verify_success() {
        let mock = MockTsaClient {
            should_fail: false,
            response: None,
        };

        let response = TsaResponse {
            token_der: vec![1, 2, 3],
            timestamp: 1000000000,
        };
        let hash = [42u8; 32];

        let result = mock.verify(&response, &hash);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_client_verify_failure() {
        let mock = MockTsaClient {
            should_fail: true,
            response: None,
        };

        let response = TsaResponse {
            token_der: vec![1, 2, 3],
            timestamp: 1000000000,
        };
        let hash = [42u8; 32];

        let result = mock.verify(&response, &hash);
        assert!(result.is_err());
        match result.unwrap_err() {
            AnchorError::TokenInvalid(msg) => assert_eq!(msg, "mock verification failure"),
            _ => panic!("Expected TokenInvalid"),
        }
    }
}
