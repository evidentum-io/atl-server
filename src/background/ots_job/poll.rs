//! OTS polling phase
//!
//! Polls pending OTS anchors for Bitcoin confirmation and updates them atomically.

#[cfg(feature = "ots")]
use std::sync::Arc;

#[cfg(feature = "ots")]
use crate::error::{ServerError, ServerResult};

#[cfg(feature = "ots")]
use crate::anchoring::ots::{OtsClient, OtsStatus};

#[cfg(feature = "ots")]
use crate::storage::index::IndexStore;

#[cfg(feature = "ots")]
use tokio::sync::Mutex;

/// Poll pending OTS anchors for Bitcoin confirmation
///
/// Upgrades pending proofs via OTS calendar and updates database atomically
/// when Bitcoin confirmation is detected.
#[cfg(feature = "ots")]
pub async fn poll_pending_anchors(
    index: &Arc<Mutex<IndexStore>>,
    client: &Arc<dyn OtsClient>,
    max_batch_size: usize,
) -> ServerResult<()> {
    let pending = {
        let idx = index.lock().await;
        idx.get_pending_ots_anchors().map_err(|e| {
            ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
        })?
    };

    if pending.is_empty() {
        tracing::debug!("No pending OTS anchors");
        return Ok(());
    }

    let anchors: Vec<_> = pending.into_iter().take(max_batch_size).collect();

    tracing::info!(
        count = anchors.len(),
        "Polling OTS anchors for Bitcoin confirmation"
    );

    let mut confirmed = 0;
    for anchor in anchors {
        let result = match client.upgrade(&anchor.anchor.token).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    anchor_id = anchor.id,
                    error = %e,
                    "OTS upgrade failed, will retry"
                );
                continue;
            }
        };

        let upgrade_result = match result {
            None => {
                tracing::debug!(anchor_id = anchor.id, "No OTS upgrade available yet");
                continue;
            }
            Some(r) => r,
        };

        match upgrade_result.status {
            OtsStatus::Confirmed {
                block_height,
                block_time,
            } => {
                {
                    let mut idx = index.lock().await;
                    idx.confirm_ots_anchor_atomic(
                        anchor.id,
                        &upgrade_result.proof,
                        block_height,
                        block_time,
                    )
                    .map_err(|e| {
                        ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
                    })?
                };

                tracing::info!(
                    anchor_id = anchor.id,
                    block_height = block_height,
                    "OTS anchor confirmed in Bitcoin"
                );
                confirmed += 1;
            }
            OtsStatus::Pending { calendar_url } => {
                {
                    let idx = index.lock().await;
                    idx.update_anchor_token(anchor.id, &upgrade_result.proof)
                        .map_err(|e| {
                            ServerError::Storage(crate::error::StorageError::Database(
                                e.to_string(),
                            ))
                        })?
                };
                tracing::debug!(
                    anchor_id = anchor.id,
                    calendar_url = %calendar_url,
                    "OTS proof upgraded but still pending"
                );
            }
        }
    }

    if confirmed > 0 {
        tracing::info!(
            confirmed = confirmed,
            "OTS poll phase completed with confirmations"
        );
    }

    Ok(())
}
