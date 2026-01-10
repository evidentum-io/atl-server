//! Bounded channel buffer for append requests

#![allow(dead_code)]

use tokio::sync::oneshot;

use crate::error::ServerError;
use crate::traits::{AppendParams, AppendResult};

/// A single append request with response channel
#[derive(Debug)]
pub struct AppendRequest {
    /// Parameters for the append operation
    pub params: AppendParams,

    /// Channel to send the result back to the API handler
    pub response_tx: oneshot::Sender<Result<AppendResult, ServerError>>,
}

/// Handle for submitting requests to the Sequencer
#[derive(Clone)]
pub struct SequencerHandle {
    /// Sender side of the bounded channel
    tx: tokio::sync::mpsc::Sender<AppendRequest>,
}

impl SequencerHandle {
    /// Create a new handle from a sender channel
    pub(crate) fn new(tx: tokio::sync::mpsc::Sender<AppendRequest>) -> Self {
        Self { tx }
    }

    /// Submit an append request and wait for result
    ///
    /// Returns error if:
    /// - Buffer is full (backpressure)
    /// - Sequencer has shut down
    /// - Database operation failed
    pub async fn append(&self, params: AppendParams) -> Result<AppendResult, ServerError> {
        let (response_tx, response_rx) = oneshot::channel();

        let request = AppendRequest {
            params,
            response_tx,
        };

        // Try to send, return 503 if buffer full
        self.tx.send(request).await.map_err(|_| {
            ServerError::ServiceUnavailable("sequencer buffer full, try again later".into())
        })?;

        // Wait for result from Sequencer
        response_rx
            .await
            .map_err(|_| ServerError::Internal("sequencer dropped response channel".into()))?
    }

    /// Check if buffer has capacity (for health checks)
    pub fn has_capacity(&self) -> bool {
        self.tx.capacity() > 0
    }

    /// Get current buffer utilization (0.0 - 1.0)
    #[allow(dead_code)]
    pub fn buffer_utilization(&self) -> f64 {
        let capacity = self.tx.max_capacity();
        let available = self.tx.capacity();
        1.0 - (available as f64 / capacity as f64)
    }

    /// Submit a batch of append requests and wait for results
    ///
    /// Returns error if:
    /// - Buffer is full (backpressure)
    /// - Sequencer has shut down
    /// - Database operation failed
    pub async fn append_batch(
        &self,
        params_batch: Vec<AppendParams>,
    ) -> Result<Vec<AppendResult>, ServerError> {
        let mut result_rxs = Vec::with_capacity(params_batch.len());

        // Submit all requests to the sequencer
        for params in params_batch {
            let (response_tx, response_rx) = oneshot::channel();

            let request = AppendRequest {
                params,
                response_tx,
            };

            // Try to send, return 503 if buffer full
            self.tx.send(request).await.map_err(|_| {
                ServerError::ServiceUnavailable("sequencer buffer full, try again later".into())
            })?;

            result_rxs.push(response_rx);
        }

        // Wait for all results
        let mut results = Vec::with_capacity(result_rxs.len());
        for rx in result_rxs {
            let result = rx.await.map_err(|_| {
                ServerError::Internal("sequencer dropped response channel".into())
            })??;
            results.push(result);
        }

        Ok(results)
    }
}
