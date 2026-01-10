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
    use sha2::Digest;

    #[test]
    fn test_service_creation() {
        let config = TsaConfig::free_tsa();
        let result = TsaService::new(config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_service_is_enabled() {
        let config = TsaConfig::free_tsa();
        let service = TsaService::new(config).unwrap();
        assert!(service.is_enabled());
    }

    #[tokio::test]
    #[ignore]
    async fn test_timestamp_with_fallback() {
        let config = TsaConfig::free_tsa();
        let service = TsaService::new(config).unwrap();
        let hash = sha2::Sha256::digest(b"test fallback").into();

        let result = service.timestamp_with_fallback(&hash).await;
        assert!(result.is_ok());
    }
}
