//! Async wrapper for OpenTimestamps client

use crate::anchoring::error::AnchorError;
use crate::anchoring::ots::client::OpenTimestampsClient;
use crate::anchoring::ots::proof::{detect_status, parse_proof};
use crate::anchoring::ots::types::{OtsConfig, OtsStatus};
use async_trait::async_trait;

/// Result of a successful OTS upgrade
#[derive(Debug, Clone)]
pub struct UpgradeResult {
    /// Upgraded proof bytes (serialized DetachedTimestampFile)
    pub proof: Vec<u8>,
    /// Status after upgrade (Pending or Confirmed)
    pub status: OtsStatus,
}

/// Async OTS client trait for background job integration
#[async_trait]
pub trait OtsClient: Send + Sync {
    /// Submit a hash to OTS calendar servers
    /// Returns (calendar_url, proof_bytes) on success
    async fn submit(&self, hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError>;

    /// Attempt to upgrade a pending proof
    /// Returns Some(UpgradeResult) if upgrade succeeded
    /// Returns None if no upgrade available yet
    async fn upgrade(&self, proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError>;
}

/// Async wrapper around OpenTimestampsClient
pub struct AsyncOtsClient {
    client: OpenTimestampsClient,
}

impl AsyncOtsClient {
    /// Create a new async OTS client with default configuration
    pub fn new() -> Result<Self, AnchorError> {
        let client = OpenTimestampsClient::new()?;
        Ok(Self { client })
    }

    /// Create a new async OTS client with custom configuration
    pub fn with_config(config: OtsConfig) -> Result<Self, AnchorError> {
        let client = OpenTimestampsClient::with_config(config)?;
        Ok(Self { client })
    }
}

impl Default for AsyncOtsClient {
    fn default() -> Self {
        Self::new().expect("failed to create default async OTS client")
    }
}

#[async_trait]
impl OtsClient for AsyncOtsClient {
    async fn submit(&self, hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
        let (calendar_url, file) = self.client.timestamp_with_fallback(hash).await?;

        let proof_bytes = file.to_bytes().map_err(|e| {
            AnchorError::InvalidResponse(format!("failed to serialize proof: {}", e))
        })?;

        Ok((calendar_url, proof_bytes))
    }

    async fn upgrade(&self, proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
        let file = parse_proof(proof)?;

        if let Some(upgraded_file) = self.client.upgrade_proof(&file).await? {
            let status = detect_status(&upgraded_file.timestamp);

            let proof_bytes = upgraded_file.to_bytes().map_err(|e| {
                AnchorError::InvalidResponse(format!("failed to serialize upgraded proof: {}", e))
            })?;

            Ok(Some(UpgradeResult {
                proof: proof_bytes,
                status,
            }))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_async_client_creation() {
        let result = AsyncOtsClient::new();
        assert!(result.is_ok());
    }

    #[test]
    fn test_async_client_with_config() {
        let config = OtsConfig::default();
        let result = AsyncOtsClient::with_config(config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_async_client_default() {
        let _client = AsyncOtsClient::default();
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// This test verifies the critical bug fix:
    /// AsyncOtsClient::new() MUST work inside async Tokio context.
    ///
    /// Before the fix, this would fail with "builder error" because
    /// reqwest::blocking::Client cannot be created inside async runtime.
    #[tokio::test]
    async fn test_async_client_creation_in_async_context() {
        // This is the exact scenario that was failing before
        let result = AsyncOtsClient::new();

        assert!(
            result.is_ok(),
            "AsyncOtsClient::new() MUST work in async context. Error: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_async_client_with_config_in_async_context() {
        let config = OtsConfig::default();
        let result = AsyncOtsClient::with_config(config);

        assert!(
            result.is_ok(),
            "AsyncOtsClient::with_config() MUST work in async context. Error: {:?}",
            result.err()
        );
    }

    /// Verify client can be created multiple times in async context
    #[tokio::test]
    async fn test_multiple_client_creation() {
        for i in 0..5 {
            let result = AsyncOtsClient::new();
            assert!(
                result.is_ok(),
                "Failed to create client on iteration {}: {:?}",
                i,
                result.err()
            );
        }
    }
}

#[cfg(test)]
mod network_tests {
    use super::*;
    use sha2::Digest;

    /// Test actual submission to OTS calendar
    /// Ignored by default - requires network access
    #[tokio::test]
    #[ignore]
    async fn test_submit_to_real_calendar() {
        let client = AsyncOtsClient::new().expect("client creation failed");
        let hash: [u8; 32] = sha2::Sha256::digest(b"test async ots").into();

        let result = client.submit(&hash).await;

        assert!(result.is_ok(), "Submit failed: {:?}", result.err());

        let (calendar_url, proof) = result.unwrap();
        assert!(!calendar_url.is_empty());
        assert!(!proof.is_empty());

        // Proof should start with OTS magic header after serialization
        // (actual format depends on atl_core::ots implementation)
    }

    /// Test upgrade on a pending proof
    /// Ignored by default - requires network and a real pending proof
    #[tokio::test]
    #[ignore]
    async fn test_upgrade_pending_proof() {
        let client = AsyncOtsClient::new().expect("client creation failed");
        let hash: [u8; 32] = sha2::Sha256::digest(b"test upgrade").into();

        // First submit
        let (_, proof) = client.submit(&hash).await.expect("submit failed");

        // Upgrade should return None for fresh submission (not confirmed yet)
        let upgrade_result = client.upgrade(&proof).await;
        assert!(upgrade_result.is_ok());

        // Fresh proof won't have Bitcoin confirmation
        // It might have calendar upgrade though
    }
}

#[cfg(test)]
mod background_simulation {
    use super::*;
    use std::sync::Arc;

    /// Simulate how BackgroundJobRunner uses the client
    #[tokio::test]
    async fn test_background_job_scenario() {
        // This simulates BackgroundJobRunner::new() creating the client
        let ots_client: Arc<dyn OtsClient> =
            Arc::new(AsyncOtsClient::new().expect("client creation in async context failed"));

        // Verify trait object works
        let hash = [1u8; 32];

        // We can't actually submit without network, but verify it compiles
        // and the Arc<dyn OtsClient> pattern works
        let _client_ref: &dyn OtsClient = ots_client.as_ref();
    }
}
