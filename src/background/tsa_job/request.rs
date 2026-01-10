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
    _timeout_ms: u64,
) -> ServerResult<i64> {
    // Get root hash from tree
    let root_hash = tree
        .root_hash
        .ok_or_else(|| ServerError::Internal(format!("Tree {} has no root_hash", tree.id)))?;

    // For now, return stub implementation
    // ANCHOR-1 spec will implement the actual TSA client
    tracing::warn!(
        tree_id = tree.id,
        tsa_url = tsa_url,
        "TSA client not implemented yet (ANCHOR-1 pending), using stub anchor"
    );

    // Create stub TSA anchor
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let anchor = Anchor {
        anchor_type: AnchorType::Rfc3161,
        anchored_hash: root_hash,
        tree_size: tree.end_size.unwrap_or(tree.start_size),
        timestamp,
        token: create_stub_tsa_token(&root_hash, timestamp),
        metadata: serde_json::json!({
            "tsa_url": tsa_url,
            "stub": true,
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

/// Create stub TSA token for testing
///
/// In production, this will be replaced with actual RFC 3161 TimeStampToken.
fn create_stub_tsa_token(hash: &[u8; 32], timestamp: u64) -> Vec<u8> {
    let mut token = Vec::new();
    token.extend_from_slice(b"TSA-STUB-");
    token.extend_from_slice(&timestamp.to_be_bytes());
    token.extend_from_slice(hash);
    token
}
