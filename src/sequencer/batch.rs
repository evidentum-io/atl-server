//! Batch flushing logic with retry

use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, warn};

use crate::error::ServerError;
use crate::traits::{AppendResult, BatchResult, Storage};

use super::buffer::AppendRequest;
use super::config::SequencerConfig;

/// Flush accumulated batch to storage
///
/// Uses async Storage trait - no spawn_blocking needed.
/// Maps BatchResult entries to individual AppendResults for each handler.
pub async fn flush_batch(
    batch: &mut Vec<AppendRequest>,
    storage: &Arc<dyn Storage>,
    config: &SequencerConfig,
) {
    if batch.is_empty() {
        return;
    }

    let batch_size = batch.len();
    let requests: Vec<AppendRequest> = std::mem::take(batch);

    // Extract params and response channels
    let (params, response_txs): (Vec<_>, Vec<_>) = requests
        .into_iter()
        .map(|r| (r.params, r.response_tx))
        .unzip();

    // Attempt batch write with retry (fully async)
    let result = write_batch_with_retry(storage, &params, config).await;

    match result {
        Ok(batch_result) => {
            // Map BatchResult entries to individual AppendResults
            for (entry_result, tx) in batch_result.entries.into_iter().zip(response_txs) {
                let append_result = AppendResult {
                    id: entry_result.id,
                    leaf_index: entry_result.leaf_index,
                    tree_head: batch_result.tree_head.clone(),
                    inclusion_proof: vec![], // Proofs generated lazily on GET
                    timestamp: batch_result.committed_at,
                };
                let _ = tx.send(Ok(append_result));
            }
            debug!(batch_size, "Batch committed successfully");
        }
        Err(e) => {
            error!(batch_size, error = %e, "Batch failed after retries");
            let error_msg = e.to_string();
            for tx in response_txs {
                let _ = tx.send(Err(ServerError::Internal(error_msg.clone())));
            }
        }
    }
}

/// Write batch with exponential backoff retry
///
/// Direct async call - no spawn_blocking wrapper needed!
async fn write_batch_with_retry(
    storage: &Arc<dyn Storage>,
    params: &[crate::traits::AppendParams],
    config: &SequencerConfig,
) -> Result<BatchResult, ServerError> {
    let mut attempt = 0;
    let mut delay_ms = config.retry_base_ms;

    loop {
        // Direct async call to storage
        let result = storage.append_batch(params.to_vec()).await;

        match result {
            Ok(batch_result) => return Ok(batch_result),
            Err(e) => {
                attempt += 1;
                if attempt >= config.retry_count {
                    return Err(ServerError::Storage(e));
                }

                warn!(
                    attempt,
                    max_attempts = config.retry_count,
                    delay_ms,
                    error = %e,
                    "Batch write failed, retrying"
                );

                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                delay_ms *= 2;
            }
        }
    }
}
