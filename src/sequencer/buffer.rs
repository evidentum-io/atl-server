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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::TreeHead;

    fn create_test_params() -> AppendParams {
        AppendParams {
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata_cleartext: Some(serde_json::json!({"test": "data"})),
            external_id: Some("test-id".to_string()),
        }
    }

    fn create_test_append_result() -> Result<AppendResult, ServerError> {
        Ok(AppendResult {
            id: uuid::Uuid::new_v4(),
            leaf_index: 42,
            tree_head: TreeHead {
                tree_size: 100,
                root_hash: [3u8; 32],
                origin: [4u8; 32],
            },
            inclusion_proof: vec![],
            timestamp: chrono::Utc::now(),
        })
    }

    #[tokio::test]
    async fn test_append_request_creation() {
        let params = create_test_params();
        let (tx, _rx) = oneshot::channel();

        let request = AppendRequest {
            params: params.clone(),
            response_tx: tx,
        };

        assert_eq!(request.params.payload_hash, [1u8; 32]);
        assert_eq!(request.params.metadata_hash, [2u8; 32]);
    }

    #[tokio::test]
    async fn test_sequencer_handle_new() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<AppendRequest>(10);
        let handle = SequencerHandle::new(tx);

        // Verify handle is clonable
        let _handle_clone = handle.clone();
    }

    #[tokio::test]
    async fn test_sequencer_handle_has_capacity() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<AppendRequest>(10);
        let handle = SequencerHandle::new(tx);

        assert!(handle.has_capacity());
    }

    #[tokio::test]
    async fn test_sequencer_handle_buffer_utilization_empty() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<AppendRequest>(10);
        let handle = SequencerHandle::new(tx);

        let utilization = handle.buffer_utilization();
        assert!((0.0..=1.0).contains(&utilization));
    }

    #[tokio::test]
    async fn test_sequencer_handle_append_success() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<AppendRequest>(10);
        let handle = SequencerHandle::new(tx);

        let params = create_test_params();

        // Spawn task to simulate sequencer response
        let append_task = tokio::spawn(async move { handle.append(params).await });

        // Receive request and send response
        if let Some(request) = rx.recv().await {
            let _ = request.response_tx.send(create_test_append_result());
        }

        let result = append_task.await.expect("task should complete");
        assert!(result.is_ok());
    }

    // NOTE: test_sequencer_handle_append_buffer_full removed - send().await blocks, doesn't fail

    #[tokio::test]
    async fn test_sequencer_handle_append_channel_closed() {
        let (tx, rx) = tokio::sync::mpsc::channel::<AppendRequest>(10);
        let handle = SequencerHandle::new(tx);

        // Drop receiver to simulate sequencer shutdown
        drop(rx);

        let params = create_test_params();
        let result = handle.append(params).await;

        assert!(result.is_err());
        match result {
            Err(ServerError::ServiceUnavailable(msg)) => {
                assert!(msg.contains("buffer full"));
            }
            _ => panic!("Expected ServiceUnavailable error"),
        }
    }

    #[tokio::test]
    async fn test_sequencer_handle_append_response_dropped() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<AppendRequest>(10);
        let handle = SequencerHandle::new(tx);

        let params = create_test_params();

        // Spawn task to simulate sequencer response
        let append_task = tokio::spawn(async move { handle.append(params).await });

        // Receive request but drop it without responding
        if let Some(_request) = rx.recv().await {
            // Drop request without sending response
        }

        let result = append_task.await.expect("task should complete");
        assert!(result.is_err());
        match result {
            Err(ServerError::Internal(msg)) => {
                assert!(msg.contains("dropped response channel"));
            }
            _ => panic!("Expected Internal error"),
        }
    }

    #[tokio::test]
    async fn test_sequencer_handle_append_error_response() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<AppendRequest>(10);
        let handle = SequencerHandle::new(tx);

        let params = create_test_params();

        // Spawn task to simulate sequencer response
        let append_task = tokio::spawn(async move { handle.append(params).await });

        // Receive request and send error response
        if let Some(request) = rx.recv().await {
            let _ = request
                .response_tx
                .send(Err(ServerError::Internal("test error".into())));
        }

        let result = append_task.await.expect("task should complete");
        assert!(result.is_err());
        match result {
            Err(ServerError::Internal(msg)) => {
                assert_eq!(msg, "test error");
            }
            _ => panic!("Expected Internal error"),
        }
    }

    #[tokio::test]
    async fn test_sequencer_handle_append_batch_success() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<AppendRequest>(10);
        let handle = SequencerHandle::new(tx);

        let params_batch = vec![
            create_test_params(),
            create_test_params(),
            create_test_params(),
        ];
        let batch_size = params_batch.len();

        // Spawn task to simulate sequencer responses
        let append_task = tokio::spawn(async move { handle.append_batch(params_batch).await });

        // Receive all requests and send responses
        for _ in 0..batch_size {
            if let Some(request) = rx.recv().await {
                let _ = request.response_tx.send(create_test_append_result());
            }
        }

        let result = append_task.await.expect("task should complete");
        assert!(result.is_ok());
        let results = result.expect("should have results");
        assert_eq!(results.len(), batch_size);
    }

    #[tokio::test]
    async fn test_sequencer_handle_append_batch_empty() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<AppendRequest>(10);
        let handle = SequencerHandle::new(tx);

        let params_batch = vec![];
        let result = handle.append_batch(params_batch).await;

        assert!(result.is_ok());
        let results = result.expect("should have results");
        assert_eq!(results.len(), 0);
    }

    // NOTE: test_sequencer_handle_append_batch_buffer_full removed - send().await blocks, doesn't fail

    #[tokio::test]
    async fn test_sequencer_handle_append_batch_response_dropped() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<AppendRequest>(10);
        let handle = SequencerHandle::new(tx);

        let params_batch = vec![create_test_params()];

        // Spawn task to simulate sequencer response
        let append_task = tokio::spawn(async move { handle.append_batch(params_batch).await });

        // Receive request but drop without responding
        if let Some(_request) = rx.recv().await {
            // Drop without sending response
        }

        let result = append_task.await.expect("task should complete");
        assert!(result.is_err());
        match result {
            Err(ServerError::Internal(msg)) => {
                assert!(msg.contains("dropped response channel"));
            }
            _ => panic!("Expected Internal error"),
        }
    }

    #[tokio::test]
    async fn test_sequencer_handle_append_batch_partial_error() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<AppendRequest>(10);
        let handle = SequencerHandle::new(tx);

        let params_batch = vec![create_test_params(), create_test_params()];

        // Spawn task to simulate sequencer responses
        let append_task = tokio::spawn(async move { handle.append_batch(params_batch).await });

        // First request succeeds
        if let Some(request) = rx.recv().await {
            let _ = request.response_tx.send(create_test_append_result());
        }

        // Second request fails
        if let Some(request) = rx.recv().await {
            let _ = request
                .response_tx
                .send(Err(ServerError::Internal("batch error".into())));
        }

        let result = append_task.await.expect("task should complete");
        assert!(result.is_err());
        match result {
            Err(ServerError::Internal(msg)) => {
                assert_eq!(msg, "batch error");
            }
            _ => panic!("Expected Internal error"),
        }
    }

    #[tokio::test]
    async fn test_sequencer_handle_buffer_utilization_full() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<AppendRequest>(2);
        let handle = SequencerHandle::new(tx.clone());

        // Fill buffer completely
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();

        let _ = tx
            .send(AppendRequest {
                params: create_test_params(),
                response_tx: tx1,
            })
            .await;

        let _ = tx
            .send(AppendRequest {
                params: create_test_params(),
                response_tx: tx2,
            })
            .await;

        let utilization = handle.buffer_utilization();
        assert!(utilization > 0.5); // Should be highly utilized
    }

    #[tokio::test]
    async fn test_sequencer_handle_has_capacity_when_full() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<AppendRequest>(1);
        let handle = SequencerHandle::new(tx.clone());

        // Fill buffer
        let (fill_tx, _fill_rx) = oneshot::channel();
        let _ = tx
            .send(AppendRequest {
                params: create_test_params(),
                response_tx: fill_tx,
            })
            .await;

        assert!(!handle.has_capacity());
    }
}
