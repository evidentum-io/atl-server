//! Mock OTS client for testing
//!
//! Provides configurable mock implementations for unit tests.

use super::async_client::{OtsClient, UpgradeResult};
use super::types::OtsStatus;
use crate::anchoring::error::AnchorError;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Mock OTS client for testing
///
/// Configurable responses for submit and upgrade operations.
pub struct MockOtsClient {
    /// Whether submit should fail
    pub submit_should_fail: AtomicBool,

    /// Whether upgrade should return a result
    pub upgrade_returns_result: AtomicBool,

    /// Whether upgrade result shows confirmed status
    pub upgrade_is_confirmed: AtomicBool,

    /// Block height to return on confirmation
    pub confirmed_block_height: u64,

    /// Block time to return on confirmation
    pub confirmed_block_time: u64,

    /// Calendar URL to return
    pub calendar_url: String,

    /// Count of submit calls
    pub submit_call_count: AtomicUsize,

    /// Count of upgrade calls
    pub upgrade_call_count: AtomicUsize,
}

impl Default for MockOtsClient {
    fn default() -> Self {
        Self {
            submit_should_fail: AtomicBool::new(false),
            upgrade_returns_result: AtomicBool::new(false),
            upgrade_is_confirmed: AtomicBool::new(false),
            confirmed_block_height: 800000,
            confirmed_block_time: 1700000000,
            calendar_url: "https://mock.calendar".to_string(),
            submit_call_count: AtomicUsize::new(0),
            upgrade_call_count: AtomicUsize::new(0),
        }
    }
}

impl MockOtsClient {
    /// Create a new mock client with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a mock that always fails on submit
    pub fn failing() -> Self {
        Self {
            submit_should_fail: AtomicBool::new(true),
            ..Self::default()
        }
    }

    /// Create a mock that returns confirmed status on upgrade
    pub fn with_confirmation(block_height: u64, block_time: u64) -> Self {
        Self {
            upgrade_returns_result: AtomicBool::new(true),
            upgrade_is_confirmed: AtomicBool::new(true),
            confirmed_block_height: block_height,
            confirmed_block_time: block_time,
            ..Self::default()
        }
    }

    /// Create a mock that returns pending status on upgrade
    pub fn with_pending_upgrade() -> Self {
        Self {
            upgrade_returns_result: AtomicBool::new(true),
            upgrade_is_confirmed: AtomicBool::new(false),
            ..Self::default()
        }
    }

    /// Get number of submit calls
    pub fn submit_calls(&self) -> usize {
        self.submit_call_count.load(Ordering::SeqCst)
    }

    /// Get number of upgrade calls
    pub fn upgrade_calls(&self) -> usize {
        self.upgrade_call_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl OtsClient for MockOtsClient {
    async fn submit(&self, hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
        self.submit_call_count.fetch_add(1, Ordering::SeqCst);

        if self.submit_should_fail.load(Ordering::SeqCst) {
            return Err(AnchorError::Network("mock submit failure".into()));
        }

        // Generate mock proof: magic + hash
        let mut proof = vec![0x00, 0x4f, 0x54, 0x01];
        proof.extend_from_slice(hash);

        Ok((self.calendar_url.clone(), proof))
    }

    async fn upgrade(&self, proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
        self.upgrade_call_count.fetch_add(1, Ordering::SeqCst);

        if !self.upgrade_returns_result.load(Ordering::SeqCst) {
            return Ok(None);
        }

        let status = if self.upgrade_is_confirmed.load(Ordering::SeqCst) {
            OtsStatus::Confirmed {
                block_height: self.confirmed_block_height,
                block_time: self.confirmed_block_time,
            }
        } else {
            OtsStatus::Pending {
                calendar_url: self.calendar_url.clone(),
            }
        };

        let mut upgraded_proof = proof.to_vec();
        upgraded_proof.extend_from_slice(&[0xFF, 0xFE]);

        Ok(Some(UpgradeResult {
            proof: upgraded_proof,
            status,
        }))
    }
}

/// Generate a fake pending OTS proof for testing
#[allow(dead_code)]
pub fn fake_pending_proof(hash: &[u8; 32]) -> Vec<u8> {
    let mut proof = vec![0x00, 0x4f, 0x54, 0x01];
    proof.extend_from_slice(hash);
    proof.extend_from_slice(b"pending");
    proof
}

/// Generate a fake confirmed OTS proof for testing
#[allow(dead_code)]
pub fn fake_confirmed_proof(hash: &[u8; 32], block_height: u64) -> Vec<u8> {
    let mut proof = vec![0x00, 0x4f, 0x54, 0x01];
    proof.extend_from_slice(hash);
    proof.extend_from_slice(b"confirmed:");
    proof.extend_from_slice(&block_height.to_le_bytes());
    proof
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_client_default() {
        let client = MockOtsClient::new();
        let hash = [0u8; 32];

        let result = client.submit(&hash).await;
        assert!(result.is_ok());
        assert_eq!(client.submit_calls(), 1);

        let (_, proof) = result.unwrap();
        let upgrade = client.upgrade(&proof).await;
        assert!(upgrade.is_ok());
        assert!(upgrade.unwrap().is_none());
        assert_eq!(client.upgrade_calls(), 1);
    }

    #[tokio::test]
    async fn test_mock_client_failing() {
        let client = MockOtsClient::failing();
        let hash = [0u8; 32];

        let result = client.submit(&hash).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_client_with_confirmation() {
        let client = MockOtsClient::with_confirmation(800000, 1700000000);
        let proof = vec![1, 2, 3];

        let result = client.upgrade(&proof).await;
        assert!(result.is_ok());

        let upgraded = result.unwrap().expect("should have upgrade");
        assert!(matches!(
            upgraded.status,
            OtsStatus::Confirmed {
                block_height: 800000,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_mock_client_with_pending() {
        let client = MockOtsClient::with_pending_upgrade();
        let proof = vec![1, 2, 3];

        let result = client.upgrade(&proof).await;
        assert!(result.is_ok());

        let upgraded = result.unwrap().expect("should have upgrade");
        assert!(matches!(upgraded.status, OtsStatus::Pending { .. }));
    }

    #[test]
    fn test_fake_pending_proof() {
        let hash = [1u8; 32];
        let proof = fake_pending_proof(&hash);
        assert!(proof.len() > 32);
        assert_eq!(&proof[0..4], &[0x00, 0x4f, 0x54, 0x01]);
    }

    #[test]
    fn test_fake_confirmed_proof() {
        let hash = [1u8; 32];
        let proof = fake_confirmed_proof(&hash, 800000);
        assert!(proof.len() > 32);
        assert_eq!(&proof[0..4], &[0x00, 0x4f, 0x54, 0x01]);
    }
}
