// File: src/background/tree_closer/logic.rs

use crate::error::ServerResult;
use crate::storage::chain_index::ChainIndex;
use crate::storage::index::IndexStore;
use crate::traits::{Storage, TreeRotator};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Check if active tree should be closed and close it if needed
///
/// This function contains the core tree closing logic:
/// 1. Get active tree (or create one if missing)
/// 2. Check if tree is old enough (based on first_entry_at, NOT created_at)
/// 3. Empty trees (first_entry_at = NULL) are NEVER closed
/// 4. Close tree and create new one with genesis leaf (via TreeRotator)
/// 5. Genesis leaf is inserted into BOTH Slab and SQLite
/// 6. Record closed tree in Chain Index
/// 7. OTS anchoring will be done by ots_job separately
pub async fn check_and_close_if_needed(
    index: &Arc<Mutex<IndexStore>>,
    storage: &Arc<dyn Storage>,
    rotator: &Arc<dyn TreeRotator>,
    chain_index: &Arc<Mutex<ChainIndex>>,
    tree_lifetime_secs: u64,
) -> ServerResult<()> {
    let tree_head = storage.tree_head();
    let origin_id = storage.origin_id();

    // Get active tree
    let active_tree = {
        let idx = index.lock().await;
        idx.get_active_tree().map_err(|e| {
            crate::error::ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
        })?
    };

    let active_tree = match active_tree {
        Some(tree) => tree,
        None => {
            // No active tree - create one (first run or recovery)
            let tree_id = {
                let idx = index.lock().await;
                idx.create_active_tree(&origin_id, tree_head.tree_size)
                    .map_err(|e| {
                        crate::error::ServerError::Storage(crate::error::StorageError::Database(
                            e.to_string(),
                        ))
                    })?
            };
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
        "Closing tree (timer started from first entry), OTS anchoring will be done by ots_job"
    );

    // Rotate tree: close current tree and create new one with genesis leaf
    // This uses TreeRotator trait which ensures genesis leaf is inserted into BOTH Slab and SQLite
    let result = rotator
        .rotate_tree(&origin_id, tree_head.tree_size, &tree_head.root_hash)
        .await
        .map_err(crate::error::ServerError::Storage)?;

    tracing::info!(
        closed_tree_id = result.closed_tree_id,
        new_tree_id = result.new_tree_id,
        data_tree_index = result.data_tree_index,
        super_root = %hex::encode(result.super_root),
        new_tree_size = result.new_tree_head.tree_size,
        "Tree rotated with Super-Tree append, pending OTS anchoring by ots_job"
    );

    {
        let ci = chain_index.lock().await;
        ci.record_closed_tree(&result.closed_tree_metadata)
            .map_err(|e| {
                crate::error::ServerError::Storage(crate::error::StorageError::Database(
                    e.to_string(),
                ))
            })?;
    }

    tracing::info!(
        tree_id = result.closed_tree_metadata.tree_id,
        data_tree_index = result.data_tree_index,
        "Recorded closed tree in Chain Index"
    );

    Ok(())
}
