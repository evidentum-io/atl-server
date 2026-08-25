// File: src/background/tsa_job/request.rs

use crate::error::{ServerError, ServerResult};
use crate::storage::index::{IndexStore, TreeRecord};
use crate::traits::{Anchor, AnchorType};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Try to get TSA timestamp from a specific server
///
/// Returns anchor_id on success.
pub async fn try_tsa_timestamp(
    tree: &TreeRecord,
    tsa_url: &str,
    index: &Arc<Mutex<IndexStore>>,
    timeout_ms: u64,
) -> ServerResult<i64> {
    use crate::anchoring::rfc3161::{AsyncRfc3161Client, TsaClient};

    // Get root hash from tree
    let root_hash = tree
        .root_hash
        .ok_or_else(|| ServerError::Internal(format!("Tree {} has no root_hash", tree.id)))?;

    // Check if TSA anchor already exists for this root_hash
    let existing_id = {
        let idx = index.lock().await;
        idx.get_tsa_anchor_for_hash(&root_hash).map_err(|e| {
            ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
        })?
    };

    if let Some(existing_id) = existing_id {
        tracing::info!(
            tree_id = tree.id,
            anchor_id = existing_id,
            "TSA anchor already exists, linking to tree"
        );
        let idx = index.lock().await;
        idx.update_tree_tsa_anchor(tree.id, existing_id)
            .map_err(|e| {
                ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
            })?;
        return Ok(existing_id);
    }

    // Create TSA client and request timestamp
    let client = AsyncRfc3161Client::new()
        .map_err(|e| ServerError::Internal(format!("Failed to create TSA client: {}", e)))?;

    let response = client
        .timestamp(tsa_url, &root_hash, timeout_ms)
        .await
        .map_err(|e| ServerError::ServiceUnavailable(format!("TSA request failed: {}", e)))?;

    // Verify the token's messageImprint matches the hash we requested before
    // trusting (and storing) anything the TSA sent back. A response that does
    // not match is treated the same as a failed request so the round-robin
    // selector moves on to the next configured TSA.
    if let Err(e) = client.verify(&response, &root_hash) {
        tracing::warn!(
            tree_id = tree.id,
            tsa_url = tsa_url,
            error = %e,
            "TSA response failed messageImprint verification, rejecting"
        );
        return Err(ServerError::ServiceUnavailable(format!(
            "TSA response verification failed: {}",
            e
        )));
    }

    tracing::info!(
        tree_id = tree.id,
        tsa_url = tsa_url,
        timestamp = response.timestamp,
        "TSA timestamp received and verified"
    );

    let anchor = Anchor {
        anchor_type: AnchorType::Rfc3161,
        target: "data_tree_root".to_string(),
        anchored_hash: root_hash,
        tree_size: tree.end_size.unwrap_or(tree.start_size),
        super_tree_size: None,
        timestamp: response.timestamp,
        token: response.token_der,
        metadata: serde_json::json!({
            "tsa_url": tsa_url,
        }),
    };

    // Store anchor with status='confirmed' (TSA anchors are immediately confirmed)
    let anchor_id = {
        let idx = index.lock().await;
        idx.store_anchor_returning_id(
            tree.end_size.unwrap_or(tree.start_size),
            &anchor,
            "confirmed",
        )
        .map_err(|e| ServerError::Storage(crate::error::StorageError::Database(e.to_string())))?
    };

    // Update tree with TSA anchor reference
    {
        let idx = index.lock().await;
        idx.update_tree_tsa_anchor(tree.id, anchor_id)
            .map_err(|e| {
                ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
            })?
    };

    Ok(anchor_id)
}

/// Create TSA anchor for current tree head (active tree periodic anchoring)
///
/// This function creates a TSA timestamp for the active tree's root hash
/// without linking it to any specific tree record. It's used for periodic
/// anchoring of the active tree state.
///
/// # Arguments
/// * `root_hash` - Current tree root hash
/// * `tree_size` - Current tree size
/// * `tsa_url` - TSA server URL
/// * `timeout_ms` - Request timeout in milliseconds
/// * `index` - IndexStore reference
///
/// # Returns
/// * `anchor_id` - ID of the created anchor record
pub async fn create_tsa_anchor_for_tree_head(
    root_hash: [u8; 32],
    tree_size: u64,
    tsa_url: &str,
    timeout_ms: u64,
    index: &Arc<Mutex<IndexStore>>,
) -> ServerResult<i64> {
    use crate::anchoring::rfc3161::{AsyncRfc3161Client, TsaClient};

    // Check if TSA anchor already exists for this root_hash
    let existing_id = {
        let idx = index.lock().await;
        idx.get_tsa_anchor_for_hash(&root_hash).map_err(|e| {
            ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
        })?
    };

    if let Some(existing_id) = existing_id {
        tracing::debug!(
            tree_size = tree_size,
            anchor_id = existing_id,
            root_hash = hex::encode(root_hash),
            "TSA anchor already exists for this root hash"
        );
        return Ok(existing_id);
    }

    // Create TSA client and request timestamp
    let client = AsyncRfc3161Client::new()
        .map_err(|e| ServerError::Internal(format!("Failed to create TSA client: {}", e)))?;

    let response = client
        .timestamp(tsa_url, &root_hash, timeout_ms)
        .await
        .map_err(|e| ServerError::ServiceUnavailable(format!("TSA request failed: {}", e)))?;

    // Verify the token's messageImprint matches the requested root hash before
    // storing anything. A mismatched or malformed token is treated the same as
    // a failed request (the periodic job retries on the next interval, possibly
    // against a different round-robin server).
    if let Err(e) = client.verify(&response, &root_hash) {
        tracing::warn!(
            tree_size = tree_size,
            root_hash = hex::encode(root_hash),
            tsa_url = tsa_url,
            error = %e,
            "TSA response failed messageImprint verification, rejecting"
        );
        return Err(ServerError::ServiceUnavailable(format!(
            "TSA response verification failed: {}",
            e
        )));
    }

    tracing::info!(
        tree_size = tree_size,
        root_hash = hex::encode(root_hash),
        tsa_url = tsa_url,
        timestamp = response.timestamp,
        "TSA timestamp received and verified for active tree"
    );

    let anchor = Anchor {
        anchor_type: AnchorType::Rfc3161,
        target: "data_tree_root".to_string(),
        anchored_hash: root_hash,
        tree_size,
        super_tree_size: None,
        timestamp: response.timestamp,
        token: response.token_der,
        metadata: serde_json::json!({
            "tsa_url": tsa_url,
        }),
    };

    // Store anchor with status='confirmed' (TSA anchors are immediately confirmed)
    let anchor_id = {
        let idx = index.lock().await;
        idx.store_anchor_returning_id(tree_size, &anchor, "confirmed")
            .map_err(|e| {
                ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
            })?
    };

    Ok(anchor_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::index::IndexStore;

    fn create_test_index_store() -> IndexStore {
        use rusqlite::Connection;
        let conn = Connection::open_in_memory().expect("Failed to create in-memory DB");
        let store = IndexStore::from_connection(conn);
        store.initialize().expect("Failed to initialize schema");
        store
    }

    fn create_test_tree_record(
        id: i64,
        root_hash: Option<[u8; 32]>,
        start_size: u64,
        end_size: Option<u64>,
    ) -> TreeRecord {
        use crate::storage::index::lifecycle::TreeStatus;

        TreeRecord {
            id,
            origin_id: [0u8; 32],
            root_hash,
            start_size,
            end_size,
            status: TreeStatus::PendingBitcoin,
            tsa_anchor_id: None,
            created_at: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            first_entry_at: None,
            closed_at: Some(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)),
            bitcoin_anchor_id: None,
        }
    }

    #[tokio::test]
    async fn test_try_tsa_timestamp_missing_root_hash() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let tree = create_test_tree_record(1, None, 0, Some(100));

        let result = try_tsa_timestamp(&tree, "https://freetsa.org/tsr", &index, 5000).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("has no root_hash"));
    }

    #[tokio::test]
    async fn test_try_tsa_timestamp_existing_anchor() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [1u8; 32];
        let origin_id = [0u8; 32];

        // Create anchor first
        let anchor = crate::traits::Anchor {
            anchor_type: crate::traits::AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: root_hash,
            tree_size: 100,
            super_tree_size: None,
            timestamp: 1234567890,
            token: vec![1, 2, 3],
            metadata: serde_json::json!({"tsa_url": "https://test.com"}),
        };

        let (anchor_id, tree_id) = {
            let idx = index.lock().await;
            let aid = idx
                .store_anchor_returning_id(100, &anchor, "confirmed")
                .expect("Failed to store anchor");

            // Create actual tree in database
            let tid = idx
                .create_active_tree(&origin_id, 0)
                .expect("Failed to create tree");

            // Close it with the root hash we want to test
            idx.connection()
                .execute(
                    "UPDATE trees SET status = 'pending_bitcoin', end_size = ?1, root_hash = ?2 WHERE id = ?3",
                    rusqlite::params![100i64, root_hash.as_slice(), tid],
                )
                .expect("Failed to update tree");

            (aid, tid)
        };

        // Create tree record that matches what we inserted
        let tree = create_test_tree_record(tree_id, Some(root_hash), 0, Some(100));

        let result = try_tsa_timestamp(&tree, "https://freetsa.org/tsr", &index, 5000).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), anchor_id);

        // Verify tree was linked to anchor
        let idx = index.lock().await;
        let tree_record = idx.get_tree(tree.id).unwrap().unwrap();
        assert_eq!(tree_record.tsa_anchor_id, Some(anchor_id));
    }

    #[tokio::test]
    async fn test_create_tsa_anchor_for_tree_head_existing_anchor() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [2u8; 32];

        // Create anchor first
        let anchor = crate::traits::Anchor {
            anchor_type: crate::traits::AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: root_hash,
            tree_size: 200,
            super_tree_size: None,
            timestamp: 1234567890,
            token: vec![1, 2, 3],
            metadata: serde_json::json!({"tsa_url": "https://test.com"}),
        };

        let anchor_id = {
            let idx = index.lock().await;
            idx.store_anchor_returning_id(200, &anchor, "confirmed")
                .expect("Failed to store anchor")
        };

        let result =
            create_tsa_anchor_for_tree_head(root_hash, 200, "https://test.com/tsr", 5000, &index)
                .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), anchor_id);
    }

    // Note: We don't test actual TSA network calls here as that would require
    // network access and real TSA servers. Those tests should be in integration tests.
    // Unit tests focus on the logic around anchor lookup and tree linking.

    #[tokio::test]
    async fn test_try_tsa_timestamp_uses_tree_end_size() {
        let _index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [3u8; 32];
        let tree = create_test_tree_record(1, Some(root_hash), 100, Some(500));

        // Should use end_size (500) not start_size (100)
        // We can't test network call, but verify tree structure is correct
        assert_eq!(tree.end_size, Some(500));
        assert_eq!(tree.start_size, 100);
    }

    #[tokio::test]
    async fn test_try_tsa_timestamp_fallback_to_start_size() {
        let _index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [4u8; 32];
        let tree = create_test_tree_record(1, Some(root_hash), 200, None);

        // When end_size is None, should use start_size
        assert_eq!(tree.end_size, None);
        assert_eq!(tree.start_size, 200);
    }

    #[tokio::test]
    async fn test_create_tsa_anchor_with_different_tree_sizes() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash1 = [5u8; 32];
        let root_hash2 = [6u8; 32];

        // Create anchors for different tree sizes
        let anchor1 = crate::traits::Anchor {
            anchor_type: crate::traits::AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: root_hash1,
            tree_size: 100,
            super_tree_size: None,
            timestamp: 1000,
            token: vec![1],
            metadata: serde_json::json!({}),
        };

        let anchor2 = crate::traits::Anchor {
            anchor_type: crate::traits::AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: root_hash2,
            tree_size: 200,
            super_tree_size: None,
            timestamp: 2000,
            token: vec![2],
            metadata: serde_json::json!({}),
        };

        let (id1, id2) = {
            let idx = index.lock().await;
            let i1 = idx
                .store_anchor_returning_id(100, &anchor1, "confirmed")
                .unwrap();
            let i2 = idx
                .store_anchor_returning_id(200, &anchor2, "confirmed")
                .unwrap();
            (i1, i2)
        };

        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn test_try_tsa_timestamp_storage_error_on_get() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [7u8; 32];

        // Close the database connection to cause storage errors
        {
            let idx = index.lock().await;
            let conn = idx.connection();
            conn.execute("DROP TABLE anchors", [])
                .expect("Failed to drop table");
        }

        let tree = create_test_tree_record(1, Some(root_hash), 0, Some(100));
        let result = try_tsa_timestamp(&tree, "https://test.com/tsr", &index, 5000).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ServerError::Storage(_)));
    }

    #[tokio::test]
    async fn test_create_tsa_anchor_for_tree_head_storage_error() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [8u8; 32];

        // Close the database to cause storage errors
        {
            let idx = index.lock().await;
            let conn = idx.connection();
            conn.execute("DROP TABLE anchors", [])
                .expect("Failed to drop table");
        }

        let result =
            create_tsa_anchor_for_tree_head(root_hash, 300, "https://test.com/tsr", 5000, &index)
                .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ServerError::Storage(_)));
    }

    #[tokio::test]
    async fn test_try_tsa_timestamp_metadata_format() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [9u8; 32];

        // Create anchor with specific metadata
        let tsa_url = "https://custom-tsa.example.com/tsr";
        let anchor = crate::traits::Anchor {
            anchor_type: crate::traits::AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: root_hash,
            tree_size: 400,
            super_tree_size: None,
            timestamp: 1705000000,
            token: vec![0xAA, 0xBB, 0xCC],
            metadata: serde_json::json!({"tsa_url": tsa_url}),
        };

        let _anchor_id = {
            let idx = index.lock().await;
            idx.store_anchor_returning_id(400, &anchor, "confirmed")
                .expect("Failed to store anchor")
        };

        // Verify that anchor was stored with correct metadata
        let idx = index.lock().await;
        let stored_anchors = idx.get_anchors(400).expect("Failed to get anchors");

        assert!(!stored_anchors.is_empty());
        let stored = &stored_anchors[0];
        assert_eq!(stored.metadata["tsa_url"], tsa_url);
        assert_eq!(stored.anchor_type, crate::traits::AnchorType::Rfc3161);
        assert_eq!(stored.target, "data_tree_root");
    }

    #[tokio::test]
    async fn test_create_tsa_anchor_tree_head_multiple_calls_same_hash() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [10u8; 32];

        // Create anchor
        let anchor = crate::traits::Anchor {
            anchor_type: crate::traits::AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: root_hash,
            tree_size: 500,
            super_tree_size: None,
            timestamp: 1705100000,
            token: vec![0xFF],
            metadata: serde_json::json!({"tsa_url": "https://test.com"}),
        };

        let first_id = {
            let idx = index.lock().await;
            idx.store_anchor_returning_id(500, &anchor, "confirmed")
                .expect("Failed to store anchor")
        };

        // Call multiple times with same hash
        let result1 =
            create_tsa_anchor_for_tree_head(root_hash, 500, "https://test.com/tsr", 5000, &index)
                .await;
        let result2 =
            create_tsa_anchor_for_tree_head(root_hash, 500, "https://test.com/tsr", 5000, &index)
                .await;

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_eq!(result1.unwrap(), first_id);
        assert_eq!(result2.unwrap(), first_id);
    }

    #[tokio::test]
    async fn test_try_tsa_timestamp_tree_without_end_size() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [11u8; 32];

        // Create anchor with start_size
        let anchor = crate::traits::Anchor {
            anchor_type: crate::traits::AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: root_hash,
            tree_size: 150,
            super_tree_size: None,
            timestamp: 1705200000,
            token: vec![0x11, 0x22],
            metadata: serde_json::json!({"tsa_url": "https://test.com"}),
        };

        let anchor_id = {
            let idx = index.lock().await;
            idx.store_anchor_returning_id(150, &anchor, "confirmed")
                .expect("Failed to store anchor")
        };

        // Tree with no end_size should use start_size
        let tree = create_test_tree_record(1, Some(root_hash), 150, None);

        let result = try_tsa_timestamp(&tree, "https://test.com/tsr", &index, 5000).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), anchor_id);
    }

    #[tokio::test]
    async fn test_anchor_target_is_data_tree_root() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [12u8; 32];

        // Create and store anchor
        let anchor = crate::traits::Anchor {
            anchor_type: crate::traits::AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: root_hash,
            tree_size: 600,
            super_tree_size: None,
            timestamp: 1705300000,
            token: vec![0x33, 0x44, 0x55],
            metadata: serde_json::json!({"tsa_url": "https://test.com"}),
        };

        let _anchor_id = {
            let idx = index.lock().await;
            idx.store_anchor_returning_id(600, &anchor, "confirmed")
                .expect("Failed to store anchor")
        };

        // Retrieve and verify target
        let idx = index.lock().await;
        let stored_anchors = idx.get_anchors(600).expect("Failed to get anchors");

        assert!(!stored_anchors.is_empty());
        let stored = &stored_anchors[0];
        assert_eq!(stored.target, "data_tree_root");
    }

    #[tokio::test]
    async fn test_try_tsa_timestamp_zero_tree_size() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [13u8; 32];

        // Create anchor with zero tree size
        let anchor = crate::traits::Anchor {
            anchor_type: crate::traits::AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: root_hash,
            tree_size: 0,
            super_tree_size: None,
            timestamp: 1705400000,
            token: vec![0x66],
            metadata: serde_json::json!({"tsa_url": "https://test.com"}),
        };

        let anchor_id = {
            let idx = index.lock().await;
            idx.store_anchor_returning_id(0, &anchor, "confirmed")
                .expect("Failed to store anchor")
        };

        let tree = create_test_tree_record(1, Some(root_hash), 0, Some(0));
        let result = try_tsa_timestamp(&tree, "https://test.com/tsr", &index, 5000).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), anchor_id);
    }

    #[tokio::test]
    async fn test_create_tsa_anchor_tree_head_zero_tree_size() {
        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [14u8; 32];

        // Create anchor with zero tree size
        let anchor = crate::traits::Anchor {
            anchor_type: crate::traits::AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: root_hash,
            tree_size: 0,
            super_tree_size: None,
            timestamp: 1705500000,
            token: vec![0x77, 0x88],
            metadata: serde_json::json!({"tsa_url": "https://test.com"}),
        };

        let anchor_id = {
            let idx = index.lock().await;
            idx.store_anchor_returning_id(0, &anchor, "confirmed")
                .expect("Failed to store anchor")
        };

        let result =
            create_tsa_anchor_for_tree_head(root_hash, 0, "https://test.com/tsr", 5000, &index)
                .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), anchor_id);
    }

    // -------------------------------------------------------------------
    // messageImprint verification tests (regression for unverified TSA
    // tokens being trusted and stored).
    //
    // The DER blob below is a real captured FreeTSA response whose
    // TSTInfo.messageImprint.hashedMessage equals
    // FREETSA_EXPECTED_HASH_HEX (see also asn1.rs tests, which parse the
    // same fixture).
    // -------------------------------------------------------------------

    const FREETSA_RESPONSE_HEX: &str = "3082155d30030201003082155406092a864886f70d010702a082154530821541020103310f300d060960864801650304020305003082018f060b2a864886f70d0109100104a082017e0482017a3082017602010106042a0304013031300d060960864801650304020105000420954d5a49fd70d9b8bcdb35d252267829957f7ef7fa6c74f88419bdc5e82209f40204026eb423180f32303236303131313030303131325a0101ff02090081c082603883d30da0820111a482010d308201093111300f060355040a13084672656520545341310c300a060355040b130354534131763074060355040d136d54686973206365727469666963617465206469676974616c6c79207369676e7320646f63756d656e747320616e642074696d65207374616d70207265717565737473206d616465207573696e672074686520667265657473612e6f7267206f6e6c696e65207365727669636573311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310b3009060355040613024445310f300d0603550408130642617965726ea082100830820801308205e9a003020102020900c1e986160da8e982300d06092a864886f70d01010d05003081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b3009060355040613024445301e170d3136303331333031353733395a170d3236303331313031353733395a308201093111300f060355040a13084672656520545341310c300a060355040b130354534131763074060355040d136d54686973206365727469666963617465206469676974616c6c79207369676e7320646f63756d656e747320616e642074696d65207374616d70207265717565737473206d616465207573696e672074686520667265657473612e6f7267206f6e6c696e65207365727669636573311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310b3009060355040613024445310f300d0603550408130642617965726e30820222300d06092a864886f70d01010105000382020f003082020a0282020100b591048c4e486f34e9dc08627fc2375162236984b82cb130beff517cfc38f84bce5c65a874dab2621ae0bce7e33563e0ede934fd5f8823159f07848808227460c1ed88261706f4281334359dfbb81bd1353fc179610af1a8c8c865dc00ea23b3a89be6bd03ba85a9ec827d60565905e22d6a584ed1380ae150280cee397e98a012f380464007862443bc077cb95f421af31712d9683cdb6dffbaf3c8ba5ba566ae523d459d6177346d4d840e27886b7c01c5b890d78a2e27bba8dd2f9a2812e157d62f921c65962548069dcdb7d06de181de0e9570d66f87220ce28b628ab55906f3ee0c210f7051e8f4858af8b9a92d09e46af2d9cba5bfcfad168cdf604491a4b06603b114caf7031f065e7eeefa53c575f3490c059d2e32ddc76ac4d4c4c710683b97fd1be591bc61055186d88f9a0391b307b6f91ed954daa36f9acd6a1e14aa2e4adf17464b54db18dbb6ffe30080246547370436ce4e77bae5de6fe0f3f9d6e7ffbeb461e794e92fb0951f8aae61a412cce9b21074635c8be327ae1a0f6b4a646eb0f8463bc63bf845530435d19e802511ec9f66c3496952d8becb69b0aa4d4c41f60515fe7dcbb89319cdda59ba6aea4be3ceae718e6fcb6ccd7db9fc50bb15b12f3665b0aa307289c2e6dd4b111ce48ba2d9efdb5a6b9a506069334fb34f6fc7ae330f0b34208aac80df3266fdd90465876ba2cb898d9505315b6e7b0203010001a38201db308201d730090603551d1304023000301d0603551d0e041604146e760b7b4e4f9ce160ca6d2ce927a2a294b37737301f0603551d23041830168014fa550d8c346651434cf7e7b3a76c95af7ae6a497300b0603551d0f0404030206c030160603551d250101ff040c300a06082b06010505070308306306082b0601050507010104573055302a06082b06010505073002861e687474703a2f2f7777772e667265657473612e6f72672f7473612e637274302706082b06010505073001861b687474703a2f2f7777772e667265657473612e6f72673a3235363030370603551d1f0430302e302ca02aa0288626687474703a2f2f7777772e667265657473612e6f72672f63726c2f726f6f745f63612e63726c3081c60603551d200481be3081bb3081b80601003081b2303306082b060105050702011627687474703a2f2f7777772e667265657473612e6f72672f667265657473615f6370732e68746d6c303206082b060105050702011626687474703a2f2f7777772e667265657473612e6f72672f667265657473615f6370732e706466304706082b06010505070202303b1a394672656554534120747275737465642074696d657374616d70696e6720536f6674776172652061732061205365727669636520285361615329300d06092a864886f70d01010d05000382020100a5c944e2c6fac0a14d930a7fd0a0b172b41fc1483c3e957c68a2bcd9b9764f1a950161fd72472d41a5eed277786203b5422240fb3a26cde176087b6fb1011df4cc19e2571aa4a051109665e94c46f50bd2adee6ac4137e251b25a39dabda451515d8ff9e07209e8ec20b7874f7e1a0ede7c00937fe84a334f8b3265ced2d8ed9df61396583677feb382c1ee3b23e6ea5f05df30de7b9f89005d25266f612f39c8b4f6daba6d7bfbac19632b90637329f52a6f066a10e43eaa81f849a6c5fe3fe8b5ea23275f687f2052e502ea6c30762a668cce07871dd8e97e315bba929e25589977a0a312ce96c5106b1437c779f2b361b182888f3ee8a234374fa063e956192627f7c431073965d1260928eba009e803429ae324cf96f042354f37bca5afddc79f79346ab388bfc79f01dc9861254ea6cc129941076b83d20556f3be51326837f2876f7833b370e7c3d410523827d4f53400c72218d75229ff10c6f8893a9a3a1c0c42bb4c898c13df41c7f6573b4fc56515971a610a7b0d2857c8225a9fb204eaceca2e8971aa1af87886a2ae3c72fe0a0aae842980a77bef16b92115458090d982b5946603764e75a0ad3d11454b9986f678b9ab6afe8497033ae3abfd4eb43b7bc9dee68815949e6481582a82e785277f2282107efe390200e0508acb8ea82ea2505276f3c9da2a3d3b4ad38bbf8842bda36fc2448291f558dc02dd1e0308207ff308205e7a003020102020900c1e986160da8e980300d06092a864886f70d01010d05003081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b3009060355040613024445301e170d3136303331333031353231335a170d3431303330373031353231335a3081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b300906035504061302444530820222300d06092a864886f70d01010105000382020f003082020a0282020100b6028e0e3032f11110d964cda94b9d0278e1942ae913aaa59907cda69793995bd9ac7e33bad9fe3704da1c01a98d21afe3f591a59d7067705167998f5016722e0ab462b21f439171d2cfcc4593f3735af794a5ab311f6c010c7898de33d75c4510ee76f4bd1d1498cf17d303f06a5dd9f796cc6ca9b657a56fe3ea4fefbe7ce6b6a18d3e35a30cee5ff170d1cf39a333d3fda8964d22db685b29e561be890f0aa845873b2e84ab26ab839ffe8fade9d23bb31e61d273cc9b880649185fabecfa0534600aba901b614e2e854582dea2226fc19cd7df52bed50d8777cd9988c053a3fc7dc3287a068a4ff12b713cd9803666e955385456ff38f80298cf6b93856e9224774a66cf1cdd11c2f8efd85203d7458b25664b13ed639cded4ff8113d6cc5353d2729473c3c307157c722aa5b5dd0bfb2d6c38b1b93749c881ec60026d08951b3824bd71bacbce473aebd636f0b918b4a2c8ff4694f07457af2d6f1cf82554d1770fd79ff5d314dcd104cddcabc94138056dfcf017e7eb8572fd52f70144f188da05f5823f58dd06297e7387bed2d772c13da8266601045fe412dd70986c0c987ba7344b9037387516d258e7885b51f8968b7f2601213bc4cb4c85f8ff0b84af6a988337cdfb81868f7ecf31dca6716d7ec2dd802c1672629e5c0052cb357dd29aafc43f615b3b1ff9d4e1ce08c71c73e1febb7dc56a33621329e9ed6c230203010001a382024e3082024a300c0603551d13040530030101ff300e0603551d0f0101ff0404030201c6301d0603551d0e04160414fa550d8c346651434cf7e7b3a76c95af7ae6a4973081ca0603551d230481c23081bf8014fa550d8c346651434cf7e7b3a76c95af7ae6a497a1819ba481983081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b3009060355040613024445820900c1e986160da8e98030330603551d1f042c302a3028a026a0248622687474703a2f2f7777772e667265657473612e6f72672f726f6f745f63612e63726c3081cf0603551d200481c73081c43081c1060a2b0601040181f22401013081b2303306082b060105050702011627687474703a2f2f7777772e667265657473612e6f72672f667265657473615f6370732e68746d6c303206082b060105050702011626687474703a2f2f7777772e667265657473612e6f72672f667265657473615f6370732e706466304706082b06010505070202303b1a394672656554534120747275737465642074696d657374616d70696e6720536f6674776172652061732061205365727669636520285361615329303706082b06010505070101042b3029302706082b06010505073001861b687474703a2f2f7777772e667265657473612e6f72673a32353630300d06092a864886f70d01010d0500038202010068af7ebf938562ef4ceb3b580be2faf6cc35a26772962f3d95901fa5630c87d09198984ce8a06a33f8a9c282ed9f1cb11ac6c23e17108ee4efce6fb294de95c133262255725522ca61971d4a3b7f78250dfb8d4aeec0fb1959b164100520b9c10e64c62662e4ad4d0abae2298fc948fc4e99e8d9e6b8fdbe4404121ec7c1422eacb2c9d7328e07396e60b4f3bb803ad4a555c80fefb53f85e7764a0a9fb4afc399f4cd2f5fbf587105c6081cf3d05337b6bb7d1b010b749f4888c912f3696ba1b6902d77b7dfc046c04a0cc1ec4f8d185e2da55dfb7bc2a2036c6219246a4f99ddbb6f1f829398f3b803dc0ad90dcb59bef4c27c77404b99043b78271867991152c399f12cbfc4c625adc096355ae44e342100ec517a502e2f06f940b8d43599bbc1154f8ae761a0b0d555fb4a1391d4f3420af8dbf12f2d7ddb9d77dce1537804074af175e4f2d6d55b34b5d6f7dcbdd31730af56480d4c0cff143f9e83bc151866d0ba0f0bbdc47fe27864176bbd6c1ab85df325edf777889bc4471bf3fa73e56cc591e8b160cda7b0786a1ec04ac3b24fa2e28d5d19e5e48004d5e166a83c82ec6fd54fb385ebaf7133a85b52de46db5244e1c34ae8d36e712f9fce0d493d7d3edd586c6198e3ec3e6e96346f417ac9f221e0aff33a8f6a0b1ef4c023630b76adaa8d91433825ecc41c49a5b98b181c7da30e997ab954c73c2cd805afda993182038a308203860201013081a33081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b3009060355040613024445020900c1e986160da8e982300d06096086480165030402030500a081b8301a06092a864886f70d010903310d060b2a864886f70d0109100104301c06092a864886f70d010905310f170d3236303131313030303131325a302b060b2a864886f70d010910020c311c301a301830160414916da3d860ecca82e34bc59d1793e7e968875f14304f06092a864886f70d01090431420440044f479a6d1240954966f51b86e85ef72ea23c12a0a3dc502901adca336334339ebf18f39b7708cd60780514841423adf531b44b3795699ca8ecf9a9325d3c53300d06092a864886f70d0101010500048202003f09255516f561f9f09786cb3abd61aca90f6217f01145b1e9107be8fcd17c0d3334f056c4e3c9871ef58b63c8e52c008aecf8bc345fc7242b22091c9b0020ed37626450c93a65d130a57a8f473709dbf458b0a9fea1e5564d4efd04bf0c935759be75543c6a3e59c9c42eb25cc89bb8491794b5aeaa598c03023118e07b71ae1e1b6236c2f9e8f252b73e993de3a6c9a99622d919b2abfedc77ffb0d40d85641ef40054f2fc1f15d8bf24b4d02801f16fc1a8c4d9b4cd0806b02b0270225d022fcc6453d17e55123a3fa1144cb6aa6e4652fecfb1290105b198a3bd98f9d2da733dbb5d4d31accd89a1feb065f9fba28458c883adba81dbb665299ffccdba1845e620854866939b84ad76f4dd76f1bcc45b5ae0316802c24d4cc3d0244fcf41bfe23d9edd8272d86c5ed1560cd7f7ed1314c6d7f78d9b31ea0df30f5a5784e59a876f1e0a389c9a16010e5f4ddb91874c9699517bf34016ae64e20fbddc8dd6db1ff68608d14ce073bc4725dfe70ea22306fa11dd0547b1b3a4ef78bea22682f63d907e3a508346ef4f2f0a9af84aa6b6ac833ca9d6e386a9a811b251a9ea3c774eaffae9a90aa75ae5af75cff823cbe65a999b99cfc3088445e9a011d164da17f1b6862dd2ce1cd95237b56fbe780e13e8833504899d8062fa2423f990e3361ee702d75c5e0a061fce4acf390a3423397365902d86ac4c77644b575415ebc5";
    const FREETSA_EXPECTED_HASH_HEX: &str =
        "954d5a49fd70d9b8bcdb35d252267829957f7ef7fa6c74f88419bdc5e82209f4";

    fn freetsa_response_der() -> Vec<u8> {
        hex::decode(FREETSA_RESPONSE_HEX).expect("valid hex fixture")
    }

    fn freetsa_expected_hash() -> [u8; 32] {
        let bytes = hex::decode(FREETSA_EXPECTED_HASH_HEX).expect("valid hex fixture");
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes);
        hash
    }

    #[tokio::test]
    async fn test_try_tsa_timestamp_rejects_wrong_message_imprint() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(freetsa_response_der())
            .create_async()
            .await;

        let index = Arc::new(Mutex::new(create_test_index_store()));
        // Deliberately does NOT match the fixture's embedded messageImprint.
        let wrong_root_hash = [0xFFu8; 32];
        let tree = create_test_tree_record(1, Some(wrong_root_hash), 0, Some(100));

        let result = try_tsa_timestamp(&tree, &server.url(), &index, 5000).await;

        mock.assert_async().await;
        assert!(
            result.is_err(),
            "mismatched messageImprint must be rejected"
        );
        let err = result.unwrap_err();
        assert!(matches!(err, ServerError::ServiceUnavailable(_)));
        assert!(err.to_string().to_lowercase().contains("verif"));

        // Nothing should have been stored for the wrong hash.
        let idx = index.lock().await;
        let stored = idx
            .get_tsa_anchor_for_hash(&wrong_root_hash)
            .expect("query should succeed");
        assert!(stored.is_none(), "rejected token must not be persisted");
    }

    #[tokio::test]
    async fn test_try_tsa_timestamp_accepts_correct_message_imprint() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(freetsa_response_der())
            .create_async()
            .await;

        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = freetsa_expected_hash();
        let tree = create_test_tree_record(1, Some(root_hash), 0, Some(100));

        let result = try_tsa_timestamp(&tree, &server.url(), &index, 5000).await;

        mock.assert_async().await;
        assert!(
            result.is_ok(),
            "matching messageImprint must be accepted: {:?}",
            result
        );
        let anchor_id = result.unwrap();

        let idx = index.lock().await;
        let stored = idx
            .get_tsa_anchor_for_hash(&root_hash)
            .expect("query should succeed")
            .expect("anchor must be persisted");
        assert_eq!(stored, anchor_id);
    }

    #[tokio::test]
    async fn test_try_tsa_timestamp_rejects_malformed_response() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(&b"not a valid RFC 3161 DER response"[..])
            .create_async()
            .await;

        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = [7u8; 32];
        let tree = create_test_tree_record(1, Some(root_hash), 0, Some(100));

        let result = try_tsa_timestamp(&tree, &server.url(), &index, 5000).await;

        mock.assert_async().await;
        assert!(result.is_err(), "malformed response must not be accepted");

        let idx = index.lock().await;
        let stored = idx
            .get_tsa_anchor_for_hash(&root_hash)
            .expect("query should succeed");
        assert!(stored.is_none(), "malformed response must not be persisted");
    }

    #[tokio::test]
    async fn test_create_tsa_anchor_for_tree_head_rejects_wrong_message_imprint() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(freetsa_response_der())
            .create_async()
            .await;

        let index = Arc::new(Mutex::new(create_test_index_store()));
        let wrong_root_hash = [0xEEu8; 32];

        let result =
            create_tsa_anchor_for_tree_head(wrong_root_hash, 100, &server.url(), 5000, &index)
                .await;

        mock.assert_async().await;
        assert!(
            result.is_err(),
            "mismatched messageImprint must be rejected"
        );

        let idx = index.lock().await;
        let stored = idx
            .get_tsa_anchor_for_hash(&wrong_root_hash)
            .expect("query should succeed");
        assert!(stored.is_none(), "rejected token must not be persisted");
    }

    #[tokio::test]
    async fn test_create_tsa_anchor_for_tree_head_accepts_correct_message_imprint() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(freetsa_response_der())
            .create_async()
            .await;

        let index = Arc::new(Mutex::new(create_test_index_store()));
        let root_hash = freetsa_expected_hash();

        let result =
            create_tsa_anchor_for_tree_head(root_hash, 100, &server.url(), 5000, &index).await;

        mock.assert_async().await;
        assert!(
            result.is_ok(),
            "matching messageImprint must be accepted: {:?}",
            result
        );
        let anchor_id = result.unwrap();

        let idx = index.lock().await;
        let stored = idx
            .get_tsa_anchor_for_hash(&root_hash)
            .expect("query should succeed")
            .expect("anchor must be persisted");
        assert_eq!(stored, anchor_id);
    }
}
