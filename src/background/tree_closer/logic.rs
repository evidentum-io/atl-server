// File: src/background/tree_closer/logic.rs

use crate::error::ServerResult;
use crate::traits::Storage;
use std::sync::Arc;

#[cfg(feature = "sqlite")]
use crate::storage::SqliteStore;

/// Check if active tree should be closed and close it if needed
///
/// This function contains the core tree closing logic:
/// 1. Get active tree (or create one if missing)
/// 2. Check if tree is old enough (based on first_entry_at, NOT created_at)
/// 3. Empty trees (first_entry_at = NULL) are NEVER closed
/// 4. Close tree with status='pending_bitcoin' (OTS anchoring will be done by ots_job)
/// 5. Create new active tree atomically
#[cfg(feature = "sqlite")]
pub async fn check_and_close_if_needed(
    storage: &Arc<SqliteStore>,
    tree_lifetime_secs: u64,
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
        "Closing tree (timer started from first entry), OTS anchoring will be done by ots_job"
    );

    // Close tree and create new one ATOMICALLY
    // Tree is marked as 'pending_bitcoin', bitcoin_anchor_id will be set by ots_job
    let (closed_tree_id, new_tree_id) =
        storage.close_tree_and_create_new(tree_head.tree_size, &tree_head.root_hash)?;

    tracing::info!(
        closed_tree_id = closed_tree_id,
        new_tree_id = new_tree_id,
        end_size = tree_head.tree_size,
        "Tree closed, new active tree created, pending OTS anchoring by ots_job"
    );

    Ok(())
}
