// File: src/background/tsa_job/request.rs

use crate::error::{ServerError, ServerResult};
use crate::storage::TreeRecord;
use crate::traits::{Anchor, AnchorType, Storage};

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

/// TSA timestamp for active tree (takes current snapshot)
///
/// Unlike closed trees, active trees don't have end_size/root_hash set.
/// We compute current tree head and anchor that.
#[cfg(feature = "sqlite")]
pub async fn try_tsa_timestamp_active(
    tree: &TreeRecord,
    selector: &super::round_robin::RoundRobinSelector,
    storage: &SqliteStore,
    timeout_ms: u64,
) -> ServerResult<i64> {
    use crate::anchoring::rfc3161::{AsyncRfc3161Client, TsaClient};

    // Get CURRENT tree head (not tree.root_hash which is NULL for active)
    let tree_head = storage.get_tree_head()?;

    if tree_head.tree_size == 0 {
        return Err(ServerError::Internal("Cannot anchor empty tree".into()));
    }

    let root_hash = tree_head.root_hash;
    let tree_size = tree_head.tree_size;

    // Try TSA servers with round-robin
    let num_servers = selector.urls_count();
    if num_servers == 0 {
        return Err(ServerError::Internal("No TSA URLs configured".into()));
    }

    // Start from last_index + 1 for true round-robin
    let start_index = (selector.last_index() + 1) % num_servers;

    let client = AsyncRfc3161Client::new()
        .map_err(|e| ServerError::Internal(format!("Failed to create TSA client: {}", e)))?;

    let mut last_error: Option<ServerError> = None;

    for i in 0..num_servers {
        let current_index = (start_index + i) % num_servers;
        let tsa_url = selector.get_url(current_index);

        tracing::debug!(
            tree_id = tree.id,
            tree_size = tree_size,
            tsa_url = %tsa_url,
            attempt = i + 1,
            "Attempting periodic TSA timestamp for active tree"
        );

        match client.timestamp(tsa_url, &root_hash, timeout_ms).await {
            Ok(response) => {
                tracing::info!(
                    tree_id = tree.id,
                    tree_size = tree_size,
                    tsa_url = tsa_url,
                    timestamp = response.timestamp,
                    "Periodic TSA timestamp received for active tree"
                );

                let anchor = Anchor {
                    anchor_type: AnchorType::Rfc3161,
                    anchored_hash: root_hash,
                    tree_size, // Current snapshot size
                    timestamp: response.timestamp,
                    token: response.token_der,
                    metadata: serde_json::json!({
                        "tsa_url": tsa_url,
                        "periodic": true, // Mark as periodic anchor
                    }),
                };

                // Store anchor (do NOT update tree.tsa_anchor_id for active trees)
                let anchor_id =
                    storage.store_anchor_returning_id(tree_size, &anchor, "confirmed")?;

                selector.update_last_index(current_index);
                return Ok(anchor_id);
            }
            Err(e) => {
                tracing::warn!(
                    tree_id = tree.id,
                    tsa_url = %tsa_url,
                    error = %e,
                    "TSA server failed, trying next"
                );
                last_error = Some(e.into());
            }
        }
    }

    Err(last_error.unwrap_or_else(|| ServerError::Internal("All TSA servers failed".into())))
}
