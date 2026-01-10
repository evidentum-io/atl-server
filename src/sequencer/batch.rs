//! Batch flushing logic with retry and TSA integration

use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, warn};

use crate::error::ServerError;
use crate::traits::{AppendResult, Storage};

use super::buffer::AppendRequest;
use super::config::SequencerConfig;

/// Flush accumulated batch to storage with TSA timestamping
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
        Ok(mut results) => {
            // Get TSA timestamp on the batch root hash (Tier-1 evidence)
            if !config.tsa_urls.is_empty() {
                let tsa_result = super::tsa::get_tsa_timestamp(&results, config).await;

                match tsa_result {
                    Ok(tsa_anchor) => {
                        // Attach TSA anchor to all results in batch
                        for result in results.iter_mut() {
                            result.tsa_anchor = Some(tsa_anchor.clone());
                        }
                        debug!(
                            batch_size,
                            tsa_url = %tsa_anchor.tsa_url,
                            "Batch committed with TSA"
                        );
                    }
                    Err(e) => {
                        // TSA failed - behavior depends on ATL_STRICT_TSA
                        if config.tsa_strict {
                            // Strict mode: fail the entire batch
                            error!(batch_size, error = %e, "TSA failed in strict mode");
                            for tx in response_txs {
                                let _ = tx.send(Err(ServerError::ServiceUnavailable(format!(
                                    "TSA unavailable: {}",
                                    e
                                ))));
                            }
                            return;
                        } else {
                            // Non-strict mode: proceed without TSA
                            warn!(
                                batch_size,
                                error = %e,
                                "TSA failed, proceeding without timestamp"
                            );
                        }
                    }
                }
            }

            // Send individual results back to handlers
            for (result, tx) in results.into_iter().zip(response_txs) {
                // Ignore send error (client may have disconnected)
                let _ = tx.send(Ok(result));
            }
            debug!(batch_size, "Batch committed successfully");
        }
        Err(e) => {
            // All requests in batch get the same error
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
        // Use spawn_blocking for sync SQLite operation
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
                delay_ms *= 2; // Exponential backoff
            }
        }
    }
}
