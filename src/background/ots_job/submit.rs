//! OTS submission phase
//!
//! Finds trees that need Bitcoin anchoring and submits them to OTS calendar.

#[cfg(all(feature = "sqlite", feature = "ots"))]
use std::sync::Arc;

#[cfg(all(feature = "sqlite", feature = "ots"))]
use crate::error::ServerResult;

#[cfg(feature = "ots")]
use crate::anchoring::ots::OtsClient;

#[cfg(all(feature = "sqlite", feature = "ots"))]
use crate::storage::SqliteStore;

/// Submit unanchored trees to OTS calendar
///
/// Finds trees with status='pending_bitcoin' and bitcoin_anchor_id=NULL.
/// Submits root hash to OTS calendar and creates anchor record atomically.
#[cfg(all(feature = "sqlite", feature = "ots"))]
pub async fn submit_unanchored_trees(
    storage: &Arc<SqliteStore>,
    client: &Arc<dyn OtsClient>,
    max_batch_size: usize,
) -> ServerResult<()> {
    let trees = storage.get_trees_without_bitcoin_anchor()?;

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
                let anchor_id = storage.submit_ots_anchor_atomic(
                    tree.id,
                    &proof,
                    &calendar_url,
                    &root_hash,
                    tree.end_size.unwrap_or(0),
                )?;

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
