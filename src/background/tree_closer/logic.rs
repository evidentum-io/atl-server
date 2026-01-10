// File: src/background/tree_closer/logic.rs

use crate::error::ServerResult;
use crate::traits::{Anchor, AnchorType, Storage};
use std::sync::Arc;

#[cfg(feature = "sqlite")]
use crate::storage::SqliteStore;

/// Check if active tree should be closed and close it if needed
///
/// This function contains the core tree closing logic:
/// 1. Get active tree (or create one if missing)
/// 2. Check if tree is old enough (based on first_entry_at, NOT created_at)
/// 3. Empty trees (first_entry_at = NULL) are NEVER closed
/// 4. Submit root to OTS calendar
/// 5. Close tree and create new one atomically
#[cfg(feature = "sqlite")]
pub async fn check_and_close_if_needed(
    storage: &Arc<SqliteStore>,
    tree_lifetime_secs: u64,
    ots_calendar_url: &str,
) -> ServerResult<()> {
    // Get active tree
    let active_tree = match storage.get_active_tree()? {
        Some(tree) => tree,
        None => {
            // No active tree - create one (first run or recovery)
            let tree_head = storage.get_tree_head()?;
            let tree_id = storage.create_active_tree(tree_head.tree_size)?;
            tracing::info!(
                tree_id = tree_id,
                start_size = tree_head.tree_size,
                "Created initial active tree"
            );
            return Ok(());
        }
    };

    // Edge case 1: Empty tree (first_entry_at = NULL) - timer not started, wait for first entry
    let first_entry_at = match active_tree.first_entry_at {
        Some(ts) => ts,
        None => {
            tracing::debug!(
                tree_id = active_tree.id,
                "Tree has no entries yet (first_entry_at = NULL), timer not started"
            );
            return Ok(());
        }
    };

    // Check if tree has lived long enough since FIRST ENTRY (not creation)
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let tree_age_secs = (now - first_entry_at) / 1_000_000_000;

    if tree_age_secs < tree_lifetime_secs as i64 {
        tracing::debug!(
            tree_id = active_tree.id,
            age_secs = tree_age_secs,
            lifetime_secs = tree_lifetime_secs,
            "Tree not old enough to close (timer started from first entry)"
        );
        return Ok(());
    }

    // Get current tree head (may have new entries since tree was created)
    let tree_head = storage.get_tree_head()?;

    // Double-check: tree has entries (should always be true if first_entry_at is set)
    if tree_head.tree_size <= active_tree.start_size {
        tracing::warn!(
            tree_id = active_tree.id,
            start_size = active_tree.start_size,
            current_size = tree_head.tree_size,
            "Tree has first_entry_at but no entries in tree - inconsistent state"
        );
        return Ok(());
    }

    tracing::info!(
        tree_id = active_tree.id,
        start_size = active_tree.start_size,
        end_size = tree_head.tree_size,
        age_secs = tree_age_secs,
        "Closing tree and submitting to OTS (timer started from first entry)"
    );

    // STEP 1: Submit to OTS calendar and get pending proof
    let ots_proof = submit_to_ots_calendar(&tree_head.root_hash, ots_calendar_url).await?;

    // STEP 2: Store anchor with status='pending'
    let anchor = Anchor {
        anchor_type: AnchorType::BitcoinOts,
        anchored_hash: tree_head.root_hash,
        tree_size: tree_head.tree_size,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
        token: ots_proof,
        metadata: serde_json::json!({
            "calendar_url": ots_calendar_url,
        }),
    };

    let anchor_id = storage.store_anchor_returning_id(
        tree_head.tree_size,
        &anchor,
        "pending", // OTS anchors start as pending
    )?;

    // STEP 3: Close tree and create new one ATOMICALLY
    let (closed_tree_id, new_tree_id) =
        storage.close_tree_and_create_new(tree_head.tree_size, &tree_head.root_hash, anchor_id)?;

    tracing::info!(
        closed_tree_id = closed_tree_id,
        new_tree_id = new_tree_id,
        end_size = tree_head.tree_size,
        anchor_id = anchor_id,
        "Tree closed, new active tree created, OTS pending"
    );

    Ok(())
}

/// Submit hash to OTS calendar server
///
/// Returns the pending OTS proof (incomplete until Bitcoin confirmation).
async fn submit_to_ots_calendar(hash: &[u8; 32], calendar_url: &str) -> ServerResult<Vec<u8>> {
    // For now, return a stub proof
    // ANCHOR-1 spec will implement the actual OTS client
    tracing::warn!(
        calendar_url = calendar_url,
        "OTS client not implemented yet (ANCHOR-1 pending), using stub proof"
    );

    // Create minimal stub proof that can be parsed by OTS poll job
    // Format: magic bytes + version + hash
    let mut stub_proof = Vec::new();
    stub_proof.extend_from_slice(&[0x00, 0x4f, 0x50]); // OTS magic
    stub_proof.push(0x01); // Version
    stub_proof.extend_from_slice(hash);

    Ok(stub_proof)
}
