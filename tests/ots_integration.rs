#![cfg(all(feature = "sqlite", feature = "ots"))]
//! OTS Integration Tests
//!
//! Mock tests run in CI, network tests require `--ignored`

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use atl_server::anchoring::error::AnchorError;
use atl_server::anchoring::ots::{AsyncOtsClient, OtsClient, OtsConfig, OtsStatus, UpgradeResult};

// ============================================================================
// MOCK CLIENT
// ============================================================================

/// Mock OTS client for testing without network
struct MockOtsClient {
    submit_should_fail: AtomicBool,
    upgrade_returns_confirmed: AtomicBool,
    submit_count: AtomicUsize,
    upgrade_count: AtomicUsize,
}

impl MockOtsClient {
    fn new_success() -> Self {
        Self {
            submit_should_fail: AtomicBool::new(false),
            upgrade_returns_confirmed: AtomicBool::new(false),
            submit_count: AtomicUsize::new(0),
            upgrade_count: AtomicUsize::new(0),
        }
    }

    fn new_with_confirmation() -> Self {
        Self {
            submit_should_fail: AtomicBool::new(false),
            upgrade_returns_confirmed: AtomicBool::new(true),
            submit_count: AtomicUsize::new(0),
            upgrade_count: AtomicUsize::new(0),
        }
    }

    fn new_failing() -> Self {
        Self {
            submit_should_fail: AtomicBool::new(true),
            upgrade_returns_confirmed: AtomicBool::new(false),
            submit_count: AtomicUsize::new(0),
            upgrade_count: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl OtsClient for MockOtsClient {
    async fn submit(&self, hash: &[u8; 32]) -> Result<(String, Vec<u8>), AnchorError> {
        self.submit_count.fetch_add(1, Ordering::SeqCst);

        if self.submit_should_fail.load(Ordering::SeqCst) {
            return Err(AnchorError::Network("mock failure".into()));
        }

        // Generate mock proof: OTS-like magic + hash
        let mut proof = vec![0x00, 0x4f, 0x54, 0x53]; // "OTS" with leading 0
        proof.extend_from_slice(hash);
        Ok(("https://mock.calendar".to_string(), proof))
    }

    async fn upgrade(&self, proof: &[u8]) -> Result<Option<UpgradeResult>, AnchorError> {
        self.upgrade_count.fetch_add(1, Ordering::SeqCst);

        if !self.upgrade_returns_confirmed.load(Ordering::SeqCst) {
            return Ok(None);
        }

        Ok(Some(UpgradeResult {
            proof: proof.to_vec(),
            status: OtsStatus::Confirmed {
                block_height: 800_000,
                block_time: 1_700_000_000,
            },
        }))
    }
}

// ============================================================================
// MOCK CLIENT TESTS
// ============================================================================

#[tokio::test]
async fn test_mock_client_submit() {
    let client = MockOtsClient::new_success();
    let hash = [0u8; 32];

    let result = client.submit(&hash).await;
    assert!(result.is_ok());

    let (calendar_url, proof) = result.unwrap();
    assert_eq!(calendar_url, "https://mock.calendar");
    assert!(proof.starts_with(&[0x00, 0x4f, 0x54, 0x53]));
    assert_eq!(client.submit_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_mock_client_submit_failure() {
    let client = MockOtsClient::new_failing();
    let hash = [0u8; 32];

    let result = client.submit(&hash).await;
    assert!(result.is_err());
    assert_eq!(client.submit_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_mock_client_upgrade_none() {
    let client = MockOtsClient::new_success();
    let proof = vec![1, 2, 3];

    let result = client.upgrade(&proof).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
    assert_eq!(client.upgrade_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_mock_client_upgrade_confirmed() {
    let client = MockOtsClient::new_with_confirmation();
    let proof = vec![1, 2, 3];

    let result = client.upgrade(&proof).await;
    assert!(result.is_ok());

    let upgrade_result = result.unwrap().expect("should have upgrade");
    match upgrade_result.status {
        OtsStatus::Confirmed {
            block_height,
            block_time,
        } => {
            assert_eq!(block_height, 800_000);
            assert_eq!(block_time, 1_700_000_000);
        }
        _ => panic!("Expected confirmed status"),
    }
    assert_eq!(client.upgrade_count.load(Ordering::SeqCst), 1);
}

// ============================================================================
// NETWORK TESTS (ignored)
// ============================================================================

#[tokio::test]
#[ignore] // Requires network
async fn test_async_ots_submit_real() {
    let client = AsyncOtsClient::with_config(OtsConfig::default()).expect("create client");

    // Real hash from Bitcoin genesis block
    let mut hasher = Sha256::new();
    hasher.update(b"real test hash");
    let hash: [u8; 32] = hasher.finalize().into();

    let result = client.submit(&hash).await;
    assert!(result.is_ok(), "Submit should succeed: {result:?}");

    let (_calendar_url, proof) = result.unwrap();
    assert!(!proof.is_empty(), "Proof should not be empty");
}

#[tokio::test]
#[ignore] // Requires network
async fn test_async_ots_upgrade_real() {
    let client = AsyncOtsClient::with_config(OtsConfig::default()).expect("create client");

    // Submit a hash first
    let mut hasher = Sha256::new();
    hasher.update(b"test upgrade hash");
    let hash: [u8; 32] = hasher.finalize().into();

    let (_calendar_url, proof) = client.submit(&hash).await.expect("submit");

    // Try to upgrade (will be pending)
    let result = client.upgrade(&proof).await;
    assert!(result.is_ok(), "Upgrade should not fail");

    // Should be None (too soon for confirmation)
    let upgrade_opt = result.unwrap();
    if let Some(upgrade_result) = upgrade_opt {
        match upgrade_result.status {
            OtsStatus::Pending { .. } => {
                // Expected: still pending
            }
            OtsStatus::Confirmed { .. } => {
                panic!("Unexpected confirmation for fresh submission");
            }
        }
    }
}

#[tokio::test]
#[ignore] // Requires network
async fn test_async_ots_custom_config() {
    let config = OtsConfig {
        calendar_urls: vec!["https://alice.btc.calendar.opentimestamps.org".to_string()],
        timeout_secs: 10,
        min_confirmations: 6,
    };
    let client = AsyncOtsClient::with_config(config).expect("create client");

    let mut hasher = Sha256::new();
    hasher.update(b"custom config test");
    let hash: [u8; 32] = hasher.finalize().into();

    let result = client.submit(&hash).await;
    assert!(result.is_ok(), "Submit with custom config should succeed");
}

// ============================================================================
// OTS JOB TESTS (with sqlite feature)
// ============================================================================

#[cfg(feature = "sqlite")]
mod ots_job_tests {
    use super::*;
    use atl_server::background::ots_job::{poll_pending_anchors, submit_unanchored_trees};
    use atl_server::storage::{SqliteStore, TreeStatus};

    #[tokio::test]
    async fn test_submit_finds_unanchored_trees() {
        let storage = Arc::new(SqliteStore::new(":memory:").expect("storage"));
        storage.initialize().expect("init");

        // Create and close a tree
        storage.create_active_tree(0).expect("create");
        storage
            .close_tree_and_create_new(10, &[1u8; 32])
            .expect("close");

        // Verify tree exists without anchor
        let trees = storage
            .get_trees_without_bitcoin_anchor()
            .expect("get trees");
        assert_eq!(trees.len(), 1);

        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        // Submit trees
        let result = submit_unanchored_trees(&storage, &client, 10).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_submit_creates_anchor() {
        let storage = Arc::new(SqliteStore::new(":memory:").expect("storage"));
        storage.initialize().expect("init");

        // Create closed tree (simulating tree_closer)
        storage.create_active_tree(0).expect("create");
        storage
            .close_tree_and_create_new(10, &[1u8; 32])
            .expect("close");

        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());

        // Run submit phase
        let result = submit_unanchored_trees(&storage, &client, 10).await;
        assert!(result.is_ok());

        // Verify tree now has anchor (no more unanchored)
        let trees = storage
            .get_trees_without_bitcoin_anchor()
            .expect("get trees");
        assert_eq!(trees.len(), 0);
    }

    #[tokio::test]
    async fn test_submit_handles_failure_gracefully() {
        let storage = Arc::new(SqliteStore::new(":memory:").expect("storage"));
        storage.initialize().expect("init");

        storage.create_active_tree(0).expect("create");
        storage
            .close_tree_and_create_new(10, &[1u8; 32])
            .expect("close");

        let client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_failing());

        // Submit phase should not panic on client failure
        let result = submit_unanchored_trees(&storage, &client, 10).await;
        assert!(result.is_ok());

        // Tree should remain unanchored
        let trees = storage
            .get_trees_without_bitcoin_anchor()
            .expect("get trees");
        assert_eq!(trees.len(), 1);
    }

    #[tokio::test]
    async fn test_poll_confirms_anchor_atomically() {
        let storage = Arc::new(SqliteStore::new(":memory:").expect("storage"));
        storage.initialize().expect("init");

        // Create closed tree
        storage.create_active_tree(0).expect("create");
        storage
            .close_tree_and_create_new(10, &[1u8; 32])
            .expect("close");

        // Submit with success mock
        let submit_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());
        submit_unanchored_trees(&storage, &submit_client, 10)
            .await
            .expect("submit");

        // Poll with confirmation mock
        let poll_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_with_confirmation());
        let result = poll_pending_anchors(&storage, &poll_client, 10).await;
        assert!(result.is_ok());

        // Verify anchor is confirmed
        let pending = storage.get_pending_ots_anchors().expect("get pending");
        assert_eq!(pending.len(), 0, "No pending anchors should remain");
    }

    #[tokio::test]
    async fn test_poll_handles_no_upgrade() {
        let storage = Arc::new(SqliteStore::new(":memory:").expect("storage"));
        storage.initialize().expect("init");

        // Create closed tree
        storage.create_active_tree(0).expect("create");
        storage
            .close_tree_and_create_new(10, &[1u8; 32])
            .expect("close");

        // Submit
        let submit_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());
        submit_unanchored_trees(&storage, &submit_client, 10)
            .await
            .expect("submit");

        // Poll with no-confirmation mock
        let poll_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());
        let result = poll_pending_anchors(&storage, &poll_client, 10).await;
        assert!(result.is_ok());

        // Verify anchor remains pending
        let pending = storage.get_pending_ots_anchors().expect("get pending");
        assert_eq!(pending.len(), 1, "Anchor should still be pending");
    }

    #[tokio::test]
    async fn test_full_tree_lifecycle() {
        let storage = Arc::new(SqliteStore::new(":memory:").expect("storage"));
        storage.initialize().expect("init");

        // 1. Create active tree
        storage.create_active_tree(0).expect("create");

        // 2. Close tree (tree_closer)
        storage
            .close_tree_and_create_new(10, &[1u8; 32])
            .expect("close");

        let trees_before = storage
            .get_trees_without_bitcoin_anchor()
            .expect("get trees");
        assert_eq!(trees_before.len(), 1);
        assert_eq!(trees_before[0].status, TreeStatus::PendingBitcoin);

        // 3. Submit phase
        let submit_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_success());
        submit_unanchored_trees(&storage, &submit_client, 10)
            .await
            .expect("submit");

        let pending_before = storage.get_pending_ots_anchors().expect("get pending");
        assert_eq!(pending_before.len(), 1);

        // 4. Poll phase
        let poll_client: Arc<dyn OtsClient> = Arc::new(MockOtsClient::new_with_confirmation());
        poll_pending_anchors(&storage, &poll_client, 10)
            .await
            .expect("poll");

        // 5. Verify final state
        let pending_after = storage.get_pending_ots_anchors().expect("get pending");
        assert_eq!(pending_after.len(), 0);

        let trees_after = storage
            .get_trees_without_bitcoin_anchor()
            .expect("get trees");
        assert_eq!(trees_after.len(), 0);
    }

    #[tokio::test]
    async fn test_tree_closer_no_ots_interaction() {
        let storage = Arc::new(SqliteStore::new(":memory:").expect("storage"));
        storage.initialize().expect("init");

        // Tree closer operates without OTS client
        storage.create_active_tree(0).expect("create");
        storage
            .close_tree_and_create_new(10, &[1u8; 32])
            .expect("close");

        // Verify tree is in correct state
        let trees = storage
            .get_trees_without_bitcoin_anchor()
            .expect("get trees");
        assert_eq!(trees.len(), 1);
        assert_eq!(trees[0].status, TreeStatus::PendingBitcoin);
        assert!(trees[0].root_hash.is_some());
    }
}
