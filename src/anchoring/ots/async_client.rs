//! Async wrapper for OpenTimestamps client

use crate::anchoring::error::AnchorError;
use crate::anchoring::ots::client::OpenTimestampsClient;
use crate::anchoring::ots::proof::{detect_status, parse_proof};
use crate::anchoring::ots::types::{OtsConfig, OtsStatus};
use async_trait::async_trait;
use std::sync::Arc;

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

/// Async wrapper around OpenTimestampsClient using spawn_blocking
pub struct AsyncOtsClient {
    client: Arc<OpenTimestampsClient>,
}

impl AsyncOtsClient {
    /// Create a new async OTS client with default configuration
    pub fn new() -> Result<Self, AnchorError> {
        let client = OpenTimestampsClient::new()?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    /// Create a new async OTS client with custom configuration
    pub fn with_config(config: OtsConfig) -> Result<Self, AnchorError> {
        let client = OpenTimestampsClient::with_config(config)?;
        Ok(Self {
            client: Arc::new(client),
        })
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
        let client = Arc::clone(&self.client);
        let hash_owned = *hash;

        tokio::task::spawn_blocking(move || {
            let (calendar_url, file) = client.timestamp_with_fallback(&hash_owned)?;

            let proof_bytes = file.to_bytes().map_err(|e| {
                AnchorError::InvalidResponse(format!("failed to serialize proof: {}", e))
            })?;

            Ok((calendar_url, proof_bytes))
        })
        .await
        .map_err(|e| AnchorError::ServiceError(format!("task join error: {}", e)))?
    }

    async fn upgrade(&self, proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
        let client = Arc::clone(&self.client);
        let proof_owned = proof.to_vec();

        tokio::task::spawn_blocking(move || {
            let file = parse_proof(&proof_owned)?;

            if let Some(upgraded_file) = client.upgrade_proof(&file)? {
                let status = detect_status(&upgraded_file.timestamp);

                let proof_bytes = upgraded_file.to_bytes().map_err(|e| {
                    AnchorError::InvalidResponse(format!(
                        "failed to serialize upgraded proof: {}",
                        e
                    ))
                })?;

                Ok(Some(UpgradeResult {
                    proof: proof_bytes,
                    status,
                }))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(|e| AnchorError::ServiceError(format!("task join error: {}", e)))?
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
