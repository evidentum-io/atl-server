//! Batch flushing logic with retry

use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, warn};

use crate::error::ServerError;
use crate::traits::{AppendResult, Storage};

use super::buffer::AppendRequest;
use super::config::SequencerConfig;

/// Flush accumulated batch to storage
///
/// Note: TSA anchoring is handled by background jobs (BACKGROUND-1).
/// POST response returns anchors: [] which is correct.
/// GET response returns anchors from database (populated by background job).
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

    // Attempt batch write with retry
    let result = write_batch_with_retry(storage, &params, config).await;

    match result {
        Ok(results) => {
            // Send individual results back to handlers
            for (result, tx) in results.into_iter().zip(response_txs) {
                let _ = tx.send(Ok(result));
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
async fn write_batch_with_retry(
    storage: &Arc<dyn Storage>,
    params: &[crate::traits::AppendParams],
    config: &SequencerConfig,
) -> Result<Vec<AppendResult>, ServerError> {
    let mut attempt = 0;
    let mut delay_ms = config.retry_base_ms;

    loop {
        let storage = Arc::clone(storage);
        let params_clone = params.to_vec();

        let result = tokio::task::spawn_blocking(move || storage.append_batch(params_clone))
            .await
            .map_err(|e| ServerError::Internal(format!("spawn_blocking failed: {}", e)))?;

        match result {
            Ok(results) => return Ok(results),
            Err(e) => {
                attempt += 1;
                if attempt >= config.retry_count {
                    return Err(e);
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
