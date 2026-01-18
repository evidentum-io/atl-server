//! TSA service with fallback logic

#![allow(dead_code)]

#[cfg(feature = "rfc3161")]
use crate::anchoring::error::AnchorError;
#[cfg(feature = "rfc3161")]
use crate::anchoring::rfc3161::async_client::{AsyncRfc3161Client, TsaClient};
#[cfg(feature = "rfc3161")]
use crate::anchoring::rfc3161::types::{TsaAnchor, TsaConfig};
#[cfg(feature = "rfc3161")]
use std::sync::Arc;

/// TSA service with URL list and fallback
///
/// High-level service that tries multiple TSA URLs in order until one succeeds.
#[cfg(feature = "rfc3161")]
pub struct TsaService {
    /// Async TSA client
    client: Arc<dyn TsaClient>,

    /// TSA configuration
    config: TsaConfig,
}

#[cfg(feature = "rfc3161")]
impl TsaService {
    /// Create a new TSA service
    pub fn new(config: TsaConfig) -> Result<Self, AnchorError> {
        let client = Arc::new(AsyncRfc3161Client::new()?);
        Ok(Self { client, config })
    }

    /// Create with custom TSA client
    pub fn with_client(config: TsaConfig, client: Arc<dyn TsaClient>) -> Self {
        Self { client, config }
    }

    /// Get timestamp from TSA with fallback to multiple URLs
    ///
    /// Tries each URL in order until one succeeds.
    /// Returns the TSA URL that succeeded along with the response.
    pub async fn timestamp_with_fallback(&self, hash: &[u8; 32]) -> Result<TsaAnchor, AnchorError> {
        if self.config.urls.is_empty() {
            return Err(AnchorError::NotConfigured("no TSA URLs configured".into()));
        }

        let mut last_error = None;

        for tsa_url in &self.config.urls {
            tracing::debug!(tsa_url = %tsa_url, "Attempting TSA request");

            match self
                .client
                .timestamp(tsa_url, hash, self.config.timeout_ms)
                .await
            {
                Ok(response) => {
                    tracing::info!(tsa_url = %tsa_url, "TSA request succeeded");
                    return Ok(TsaAnchor {
                        tsa_url: tsa_url.clone(),
                        tsa_response: response.token_der,
                        timestamp: response.timestamp,
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        tsa_url = %tsa_url,
                        error = %e,
                        "TSA request failed, trying next"
                    );
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| AnchorError::ServiceError("all TSAs failed".into())))
    }

    /// Check if TSA is configured
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !self.config.urls.is_empty()
    }

    /// Get configured TSA URLs
    #[must_use]
    pub fn urls(&self) -> &[String] {
        &self.config.urls
    }
}

#[cfg(not(feature = "rfc3161"))]
pub struct TsaService;

#[cfg(test)]
#[cfg(feature = "rfc3161")]
mod tests {
    use super::*;
    use crate::anchoring::rfc3161::async_client::TsaClient;
    use crate::anchoring::rfc3161::types::TsaResponse;
    use async_trait::async_trait;
    use sha2::Digest;

    // Mock TSA client for testing
    struct MockTsaClient {
        should_fail: bool,
        fail_count: std::sync::Arc<std::sync::Mutex<usize>>,
        response: TsaResponse,
    }

    impl MockTsaClient {
        fn new_success() -> Self {
            Self {
                should_fail: false,
                fail_count: std::sync::Arc::new(std::sync::Mutex::new(0)),
                response: TsaResponse {
                    token_der: vec![0x30, 0x03, 0x02, 0x01, 0x00],
                    timestamp: 1705000000000000000,
                },
            }
        }

        fn new_failure() -> Self {
            Self {
                should_fail: true,
                fail_count: std::sync::Arc::new(std::sync::Mutex::new(0)),
                response: TsaResponse {
                    token_der: vec![],
                    timestamp: 0,
                },
            }
        }

        fn new_fail_then_succeed(fail_times: usize) -> Self {
            Self {
                should_fail: false,
                fail_count: std::sync::Arc::new(std::sync::Mutex::new(fail_times)),
                response: TsaResponse {
                    token_der: vec![0x30, 0x03, 0x02, 0x01, 0x00],
                    timestamp: 1705000000000000000,
                },
            }
        }
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
                return Err(AnchorError::ServiceError("mock TSA failure".to_string()));
            }

            let mut count = self.fail_count.lock().unwrap();
            if *count > 0 {
                *count -= 1;
                return Err(AnchorError::ServiceError("mock TSA failure".to_string()));
            }

            Ok(self.response.clone())
        }

        fn verify(
            &self,
            _response: &TsaResponse,
            _expected_hash: &[u8; 32],
        ) -> Result<(), AnchorError> {
            Ok(())
        }
    }

    #[test]
    fn test_service_creation() {
        let config = TsaConfig::free_tsa();
        let result = TsaService::new(config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_service_creation_with_custom_config() {
        let config = TsaConfig {
            urls: vec!["https://test.example.com/tsr".to_string()],
            timeout_ms: 10_000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let result = TsaService::new(config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_service_with_client() {
        let config = TsaConfig::free_tsa();
        let mock_client = Arc::new(MockTsaClient::new_success());
        let service = TsaService::with_client(config, mock_client);
        assert!(service.is_enabled());
    }

    #[test]
    fn test_service_is_enabled() {
        let config = TsaConfig::free_tsa();
        let service = TsaService::new(config).unwrap();
        assert!(service.is_enabled());
    }

    #[test]
    fn test_service_is_disabled() {
        let config = TsaConfig {
            urls: vec![],
            timeout_ms: 5000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let service = TsaService::new(config).unwrap();
        assert!(!service.is_enabled());
    }

    #[test]
    fn test_service_urls() {
        let urls = vec![
            "https://tsa1.example.com/tsr".to_string(),
            "https://tsa2.example.com/tsr".to_string(),
        ];
        let config = TsaConfig {
            urls: urls.clone(),
            timeout_ms: 5000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let service = TsaService::new(config).unwrap();
        assert_eq!(service.urls(), urls.as_slice());
    }

    #[tokio::test]
    async fn test_timestamp_with_fallback_success() {
        let config = TsaConfig {
            urls: vec!["https://test.example.com/tsr".to_string()],
            timeout_ms: 5000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let mock_client = Arc::new(MockTsaClient::new_success());
        let service = TsaService::with_client(config, mock_client);
        let hash = [42u8; 32];

        let result = service.timestamp_with_fallback(&hash).await;
        assert!(result.is_ok());
        let anchor = result.unwrap();
        assert_eq!(anchor.tsa_url, "https://test.example.com/tsr");
        assert!(!anchor.tsa_response.is_empty());
        assert_eq!(anchor.timestamp, 1705000000000000000);
    }

    #[tokio::test]
    async fn test_timestamp_with_fallback_no_urls() {
        let config = TsaConfig {
            urls: vec![],
            timeout_ms: 5000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let mock_client = Arc::new(MockTsaClient::new_success());
        let service = TsaService::with_client(config, mock_client);
        let hash = [42u8; 32];

        let result = service.timestamp_with_fallback(&hash).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            AnchorError::NotConfigured(msg) => {
                assert!(msg.contains("no TSA URLs configured"));
            }
            _ => panic!("Expected NotConfigured error"),
        }
    }

    #[tokio::test]
    async fn test_timestamp_with_fallback_all_fail() {
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
        let mock_client = Arc::new(MockTsaClient::new_failure());
        let service = TsaService::with_client(config, mock_client);
        let hash = [42u8; 32];

        let result = service.timestamp_with_fallback(&hash).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            AnchorError::ServiceError(msg) => {
                assert!(msg.contains("mock TSA failure"));
            }
            _ => panic!("Expected ServiceError"),
        }
    }

    #[tokio::test]
    async fn test_timestamp_with_fallback_first_fails_second_succeeds() {
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
        let mock_client = Arc::new(MockTsaClient::new_fail_then_succeed(1));
        let service = TsaService::with_client(config, mock_client);
        let hash = [42u8; 32];

        let result = service.timestamp_with_fallback(&hash).await;
        assert!(result.is_ok());
        let anchor = result.unwrap();
        assert_eq!(anchor.tsa_url, "https://tsa2.example.com/tsr");
    }

    #[tokio::test]
    async fn test_timestamp_with_fallback_multiple_attempts() {
        let config = TsaConfig {
            urls: vec![
                "https://tsa1.example.com/tsr".to_string(),
                "https://tsa2.example.com/tsr".to_string(),
                "https://tsa3.example.com/tsr".to_string(),
            ],
            timeout_ms: 5000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let mock_client = Arc::new(MockTsaClient::new_fail_then_succeed(2));
        let service = TsaService::with_client(config, mock_client);
        let hash = [42u8; 32];

        let result = service.timestamp_with_fallback(&hash).await;
        assert!(result.is_ok());
        let anchor = result.unwrap();
        assert_eq!(anchor.tsa_url, "https://tsa3.example.com/tsr");
        assert!(!anchor.tsa_response.is_empty());
    }

    #[tokio::test]
    async fn test_timestamp_with_fallback_hash_consistency() {
        let config = TsaConfig {
            urls: vec!["https://test.example.com/tsr".to_string()],
            timeout_ms: 5000,
            username: None,
            password: None,
            ca_cert: None,
        };
        let mock_client = Arc::new(MockTsaClient::new_success());
        let service = TsaService::with_client(config, mock_client);

        let hash1 = sha2::Sha256::digest(b"test data 1").into();
        let hash2 = sha2::Sha256::digest(b"test data 2").into();

        let result1 = service.timestamp_with_fallback(&hash1).await;
        let result2 = service.timestamp_with_fallback(&hash2).await;

        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_timestamp_with_fallback_real_network() {
        let config = TsaConfig::free_tsa();
        let service = TsaService::new(config).unwrap();
        let hash = sha2::Sha256::digest(b"test fallback").into();

        let result = service.timestamp_with_fallback(&hash).await;
        assert!(result.is_ok());
    }
}
