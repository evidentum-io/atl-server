//! Async wrapper for OpenTimestamps client

use crate::anchoring::error::AnchorError;
use crate::anchoring::ots::client::OpenTimestampsClient;
use crate::anchoring::ots::proof::{detect_status, parse_proof};
use crate::anchoring::ots::types::{OtsConfig, OtsStatus};
use async_trait::async_trait;
use std::time::Duration;

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
    timeout: Duration,
}

impl AsyncOtsClient {
    /// Create a new async OTS client with default configuration
    pub fn new() -> Result<Self, AnchorError> {
        let config = OtsConfig::default();
        let client = OpenTimestampsClient::new()?;
        Ok(Self {
            client,
            timeout: Duration::from_secs(config.timeout_secs),
        })
    }

    /// Create a new async OTS client with custom configuration
    pub fn with_config(config: OtsConfig) -> Result<Self, AnchorError> {
        let timeout = Duration::from_secs(config.timeout_secs);
        let client = OpenTimestampsClient::with_config(config)?;
        Ok(Self { client, timeout })
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
            let status = detect_status(&upgraded_file.timestamp, self.timeout).await?;

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
        let _hash = [1u8; 32];

        // We can't actually submit without network, but verify it compiles
        // and the Arc<dyn OtsClient> pattern works
        let _client_ref: &dyn OtsClient = ots_client.as_ref();
    }

    /// Test UpgradeResult structure
    #[test]
    fn test_upgrade_result_debug() {
        let result = UpgradeResult {
            proof: vec![1, 2, 3],
            status: OtsStatus::Confirmed {
                block_height: 700000,
                block_time: 1234567890,
            },
        };
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("UpgradeResult"));
        assert!(debug_str.contains("proof"));
        assert!(debug_str.contains("status"));
    }

    #[test]
    fn test_upgrade_result_clone() {
        let result = UpgradeResult {
            proof: vec![4, 5, 6],
            status: OtsStatus::Pending {
                calendar_url: "https://test.com".to_string(),
            },
        };
        let cloned = result.clone();
        assert_eq!(cloned.proof, result.proof);
    }

    /// Test timeout is properly configured
    #[test]
    fn test_client_timeout_configuration() {
        let config = OtsConfig {
            timeout_secs: 42,
            ..Default::default()
        };
        let client = AsyncOtsClient::with_config(config).unwrap();
        assert_eq!(client.timeout, Duration::from_secs(42));
    }

    /// Test timeout with zero value
    #[test]
    fn test_client_timeout_zero() {
        let config = OtsConfig {
            timeout_secs: 0,
            ..Default::default()
        };
        let client = AsyncOtsClient::with_config(config).unwrap();
        assert_eq!(client.timeout, Duration::from_secs(0));
    }

    /// Test timeout with maximum value
    #[test]
    fn test_client_timeout_max() {
        let config = OtsConfig {
            timeout_secs: u64::MAX,
            ..Default::default()
        };
        let client = AsyncOtsClient::with_config(config).unwrap();
        assert_eq!(client.timeout, Duration::from_secs(u64::MAX));
    }

    /// Test upgrade result with pending status
    #[test]
    fn test_upgrade_result_pending_status() {
        let result = UpgradeResult {
            proof: vec![1, 2, 3, 4, 5],
            status: OtsStatus::Pending {
                calendar_url: "https://calendar.example.com".to_string(),
            },
        };

        assert_eq!(result.proof.len(), 5);
        match result.status {
            OtsStatus::Pending { calendar_url } => {
                assert_eq!(calendar_url, "https://calendar.example.com");
            }
            _ => panic!("Expected Pending status"),
        }
    }

    /// Test upgrade result with confirmed status
    #[test]
    fn test_upgrade_result_confirmed_status() {
        let result = UpgradeResult {
            proof: vec![10, 20, 30],
            status: OtsStatus::Confirmed {
                block_height: 800000,
                block_time: 1700000000,
            },
        };

        assert_eq!(result.proof.len(), 3);
        match result.status {
            OtsStatus::Confirmed {
                block_height,
                block_time,
            } => {
                assert_eq!(block_height, 800000);
                assert_eq!(block_time, 1700000000);
            }
            _ => panic!("Expected Confirmed status"),
        }
    }

    /// Test upgrade result with empty proof
    #[test]
    fn test_upgrade_result_empty_proof() {
        let result = UpgradeResult {
            proof: vec![],
            status: OtsStatus::Pending {
                calendar_url: "https://test.com".to_string(),
            },
        };

        assert!(result.proof.is_empty());
    }

    /// Test upgrade result with large proof
    #[test]
    fn test_upgrade_result_large_proof() {
        let large_proof = vec![0u8; 10000];
        let result = UpgradeResult {
            proof: large_proof.clone(),
            status: OtsStatus::Confirmed {
                block_height: 700000,
                block_time: 1234567890,
            },
        };

        assert_eq!(result.proof.len(), 10000);
        assert_eq!(result.proof, large_proof);
    }

    /// Test multiple clients can coexist
    #[tokio::test]
    async fn test_multiple_clients_in_parallel() {
        let clients: Vec<_> = (0..5)
            .map(|_| AsyncOtsClient::new().expect("client creation failed"))
            .collect();

        assert_eq!(clients.len(), 5);
        // All clients should be independently usable
        for client in clients {
            assert!(client.timeout > Duration::from_secs(0));
        }
    }

    /// Test default client can be created multiple times
    #[test]
    fn test_default_client_repeatability() {
        let _client1 = AsyncOtsClient::default();
        let _client2 = AsyncOtsClient::default();
        let _client3 = AsyncOtsClient::default();
        // Should not panic
    }

    /// Test AsyncOtsClient can be sent across threads
    #[tokio::test]
    async fn test_client_is_send() {
        let client = AsyncOtsClient::new().unwrap();

        // Spawn task that moves client
        let handle = tokio::spawn(async move {
            let _c = client;
            42
        });

        let result = handle.await.unwrap();
        assert_eq!(result, 42);
    }

    /// Test AsyncOtsClient can be shared across threads with Arc
    #[tokio::test]
    async fn test_client_is_sync() {
        let client = Arc::new(AsyncOtsClient::new().unwrap());

        // Spawn multiple tasks sharing the same client
        let mut handles = vec![];
        for _ in 0..3 {
            let client_clone = Arc::clone(&client);
            handles.push(tokio::spawn(async move {
                let _c = client_clone;
                true
            }));
        }

        for handle in handles {
            assert!(handle.await.unwrap());
        }
    }

    /// Test client creation with various config values
    #[test]
    fn test_client_with_various_configs() {
        let configs = vec![
            OtsConfig {
                timeout_secs: 1,
                ..Default::default()
            },
            OtsConfig {
                timeout_secs: 30,
                ..Default::default()
            },
            OtsConfig {
                timeout_secs: 300,
                ..Default::default()
            },
        ];

        for config in configs {
            let client = AsyncOtsClient::with_config(config.clone());
            assert!(client.is_ok());
            assert_eq!(
                client.unwrap().timeout,
                Duration::from_secs(config.timeout_secs)
            );
        }
    }

    /// Test default client matches new client
    #[test]
    fn test_default_matches_new() {
        let default_client = AsyncOtsClient::default();
        let new_client = AsyncOtsClient::new().unwrap();

        assert_eq!(default_client.timeout, new_client.timeout);
    }

    /// Test client creation is deterministic
    #[test]
    fn test_client_creation_deterministic() {
        let client1 = AsyncOtsClient::new().unwrap();
        let client2 = AsyncOtsClient::new().unwrap();

        // Both clients should have the same timeout
        assert_eq!(client1.timeout, client2.timeout);
    }

    /// Test OtsClient trait implementation for AsyncOtsClient
    #[tokio::test]
    async fn test_ots_client_trait_bounds() {
        fn require_ots_client<T: OtsClient>(_: &T) {}

        let client = AsyncOtsClient::new().unwrap();
        require_ots_client(&client);
    }

    /// Test Arc<dyn OtsClient> can be cloned
    #[tokio::test]
    async fn test_trait_object_clone() {
        let client: Arc<dyn OtsClient> = Arc::new(AsyncOtsClient::new().unwrap());
        let client_clone = Arc::clone(&client);

        assert_eq!(Arc::strong_count(&client), 2);
        drop(client_clone);
        assert_eq!(Arc::strong_count(&client), 1);
    }

    /// Test multiple sequential client creations
    #[tokio::test]
    async fn test_sequential_client_creation() {
        for i in 0..10 {
            let client = AsyncOtsClient::new();
            assert!(
                client.is_ok(),
                "Client creation failed on iteration {}: {:?}",
                i,
                client.err()
            );
        }
    }

    /// Test client with custom config edge cases
    #[test]
    fn test_client_config_edge_cases() {
        // Very small timeout
        let config_small = OtsConfig {
            timeout_secs: 1,
            ..Default::default()
        };
        let client_small = AsyncOtsClient::with_config(config_small);
        assert!(client_small.is_ok());

        // Large timeout
        let config_large = OtsConfig {
            timeout_secs: 86400, // 1 day
            ..Default::default()
        };
        let client_large = AsyncOtsClient::with_config(config_large);
        assert!(client_large.is_ok());
    }

    /// Test upgrade result equality after clone
    #[test]
    fn test_upgrade_result_clone_equality() {
        let original = UpgradeResult {
            proof: vec![1, 2, 3],
            status: OtsStatus::Pending {
                calendar_url: "https://example.com".to_string(),
            },
        };

        let cloned = original.clone();

        assert_eq!(original.proof, cloned.proof);
    }

    /// Test upgrade result debug format contains key information
    #[test]
    fn test_upgrade_result_debug_format() {
        let result = UpgradeResult {
            proof: vec![0xaa, 0xbb, 0xcc],
            status: OtsStatus::Confirmed {
                block_height: 750000,
                block_time: 1600000000,
            },
        };

        let debug_str = format!("{:?}", result);

        assert!(debug_str.contains("UpgradeResult"));
        assert!(debug_str.contains("proof"));
        assert!(debug_str.contains("status"));
    }

    /// Test client timeout duration conversion
    #[test]
    fn test_timeout_duration_conversion() {
        let timeout_secs = 60u64;
        let config = OtsConfig {
            timeout_secs,
            ..Default::default()
        };
        let client = AsyncOtsClient::with_config(config).unwrap();

        assert_eq!(client.timeout.as_secs(), timeout_secs);
        assert_eq!(client.timeout.as_millis(), (timeout_secs * 1000) as u128);
    }

    /// Test client creation with zero timeout edge case
    #[test]
    fn test_zero_timeout_edge_case() {
        let config = OtsConfig {
            timeout_secs: 0,
            ..Default::default()
        };
        let result = AsyncOtsClient::with_config(config);

        assert!(result.is_ok());
        let client = result.unwrap();
        assert_eq!(client.timeout, Duration::from_secs(0));
    }
}
