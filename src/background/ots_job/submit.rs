//! OTS submission phase
//!
//! Finds trees that need Bitcoin anchoring and submits them to OTS calendar.

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

/// Submit unanchored trees to OTS calendar
///
/// Finds trees with status='pending_bitcoin' and bitcoin_anchor_id=NULL.
/// Submits root hash to OTS calendar and creates anchor record atomically.
#[cfg(feature = "ots")]
pub async fn submit_unanchored_trees(
    index: &Arc<Mutex<IndexStore>>,
    _storage: &Arc<dyn Storage>,
    client: &Arc<dyn OtsClient>,
    max_batch_size: usize,
) -> ServerResult<()> {
    let trees = {
        let idx = index.lock().await;
        idx.get_trees_without_bitcoin_anchor()
            .map_err(|e| ServerError::Storage(crate::error::StorageError::Database(e.to_string())))?
    };

    if trees.is_empty() {
        tracing::debug!("No trees need OTS submission");
        return Ok(());
    }

    let trees_to_submit: Vec<_> = trees.into_iter().take(max_batch_size).collect();

    tracing::info!(
        count = trees_to_submit.len(),
        "Submitting trees to OTS calendar"
    );

    let mut submitted = 0;
    for tree in trees_to_submit {
        let root_hash = match tree.root_hash {
            Some(h) => h,
            None => {
                tracing::warn!(tree_id = tree.id, "Tree missing root_hash, skipping");
                continue;
            }
        };

        match client.submit(&root_hash).await {
            Ok((calendar_url, proof)) => {
                let anchor_id = {
                    let mut idx = index.lock().await;
                    idx.submit_ots_anchor_atomic(
                        tree.id,
                        &proof,
                        &calendar_url,
                        &root_hash,
                        tree.end_size.unwrap_or(0),
                    )
                    .map_err(|e| ServerError::Storage(crate::error::StorageError::Database(e.to_string())))?
                };

                tracing::info!(
                    tree_id = tree.id,
                    anchor_id = anchor_id,
                    calendar_url = %calendar_url,
                    "OTS proof submitted (pending Bitcoin)"
                );
                submitted += 1;
            }
            Err(e) => {
                tracing::warn!(
                    tree_id = tree.id,
                    error = %e,
                    "OTS submit failed, will retry"
                );
            }
        }
    }

    if submitted > 0 {
        tracing::info!(submitted = submitted, "OTS submit phase completed");
    }

    Ok(())
}
