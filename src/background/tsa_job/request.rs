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

    tracing::info!(
        tree_id = tree.id,
        tsa_url = tsa_url,
        timestamp = response.timestamp,
        "TSA timestamp received"
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

    tracing::info!(
        tree_size = tree_size,
        root_hash = hex::encode(root_hash),
        tsa_url = tsa_url,
        timestamp = response.timestamp,
        "TSA timestamp received for active tree"
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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("has no root_hash"));
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
            let mut idx = index.lock().await;
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
}
