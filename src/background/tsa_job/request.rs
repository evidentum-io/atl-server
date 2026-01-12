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
        anchored_hash: root_hash,
        tree_size: tree.end_size.unwrap_or(tree.start_size),
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
        anchored_hash: root_hash,
        tree_size,
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
