//! OTS polling phase
//!
//! Polls pending OTS anchors for Bitcoin confirmation and updates them atomically.

#[cfg(all(feature = "sqlite", feature = "ots"))]
use std::sync::Arc;

#[cfg(all(feature = "sqlite", feature = "ots"))]
use crate::error::ServerResult;

#[cfg(feature = "ots")]
use crate::anchoring::ots::{OtsClient, OtsStatus};

#[cfg(all(feature = "sqlite", feature = "ots"))]
use crate::storage::SqliteStore;

/// Poll pending OTS anchors for Bitcoin confirmation
///
/// Upgrades pending proofs via OTS calendar and updates database atomically
/// when Bitcoin confirmation is detected.
#[cfg(all(feature = "sqlite", feature = "ots"))]
pub async fn poll_pending_anchors(
    storage: &Arc<SqliteStore>,
    client: &Arc<dyn OtsClient>,
    max_batch_size: usize,
) -> ServerResult<()> {
    let pending = storage.get_pending_ots_anchors()?;

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
                storage.confirm_ots_anchor_atomic(
                    anchor.id,
                    &upgrade_result.proof,
                    block_height,
                    block_time,
                )?;

                tracing::info!(
                    anchor_id = anchor.id,
                    block_height = block_height,
                    "OTS anchor confirmed in Bitcoin"
                );
                confirmed += 1;
            }
            OtsStatus::Pending { calendar_url } => {
                storage.update_anchor_token(anchor.id, &upgrade_result.proof)?;
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
