//! OTS submission phase
//!
//! Submits Super Root to OTS calendar when Super-Tree grows.

#[cfg(feature = "ots")]
use std::sync::Arc;

#[cfg(feature = "ots")]
use crate::error::{ServerError, ServerResult};

#[cfg(feature = "ots")]
use crate::anchoring::ots::OtsClient;

#[cfg(feature = "ots")]
use crate::storage::index::IndexStore;

#[cfg(feature = "ots")]
use crate::traits::Storage;

#[cfg(feature = "ots")]
use tokio::sync::Mutex;

/// Submit Super Root to OTS calendar (v2.0)
///
/// Checks if Super-Tree has grown since last OTS submission.
/// If yes, submits the current Super Root to OTS calendar.
#[cfg(feature = "ots")]
pub async fn submit_unanchored_trees(
    index: &Arc<Mutex<IndexStore>>,
    storage: &Arc<dyn Storage>,
    client: &Arc<dyn OtsClient>,
    _max_batch_size: usize,
) -> ServerResult<()> {
    // Get current Super-Tree size and last submitted size
    let (current_super_tree_size, last_submitted_size) = {
        let idx = index.lock().await;
        let current = idx.get_super_tree_size().map_err(|e| {
            ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
        })?;
        let last = idx.get_last_ots_submitted_super_tree_size().map_err(|e| {
            ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
        })?;
        (current, last)
    };

    // If no new trees closed since last submission, nothing to do
    if current_super_tree_size <= last_submitted_size {
        tracing::debug!(
            current = current_super_tree_size,
            last_submitted = last_submitted_size,
            "No new Super Root to submit"
        );
        return Ok(());
    }

    // Get Super Root from storage
    let super_root = storage
        .get_super_root(current_super_tree_size)
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to get super_root");
            e
        })?;

    // Submit to OTS calendar
    match client.submit(&super_root).await {
        Ok((calendar_url, proof)) => {
            let anchor_id = {
                let mut idx = index.lock().await;
                idx.submit_super_root_ots_anchor(
                    &proof,
                    &calendar_url,
                    &super_root,
                    current_super_tree_size,
                )
                .map_err(|e| {
                    ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
                })?
            };

            // Update last submitted size
            {
                let idx = index.lock().await;
                idx.set_last_ots_submitted_super_tree_size(current_super_tree_size)
                    .map_err(|e| {
                        ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
                    })?;
            }

            tracing::info!(
                anchor_id = anchor_id,
                super_tree_size = current_super_tree_size,
                calendar_url = %calendar_url,
                "Super Root OTS proof submitted (pending Bitcoin)"
            );
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "Super Root OTS submit failed, will retry"
            );
        }
    }

    Ok(())
}
