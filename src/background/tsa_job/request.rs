// File: src/background/tsa_job/request.rs

use crate::error::{ServerError, ServerResult};
use crate::storage::TreeRecord;
use crate::traits::{Anchor, AnchorType};

#[cfg(feature = "sqlite")]
use crate::storage::SqliteStore;

/// Try to get TSA timestamp from a specific server
///
/// Returns anchor_id on success.
#[cfg(feature = "sqlite")]
pub async fn try_tsa_timestamp(
    tree: &TreeRecord,
    tsa_url: &str,
    storage: &SqliteStore,
    timeout_ms: u64,
) -> ServerResult<i64> {
    use crate::anchoring::rfc3161::{AsyncRfc3161Client, TsaClient};

    // Get root hash from tree
    let root_hash = tree
        .root_hash
        .ok_or_else(|| ServerError::Internal(format!("Tree {} has no root_hash", tree.id)))?;

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
    let anchor_id = storage.store_anchor_returning_id(
        tree.end_size.unwrap_or(tree.start_size),
        &anchor,
        "confirmed",
    )?;

    // Update tree with TSA anchor reference
    storage.update_tree_tsa_anchor(tree.id, anchor_id)?;

    Ok(anchor_id)
}
