//! Health check handler

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};

use crate::api::dto::HealthResponse;
use crate::api::state::AppState;
use crate::config::ServerMode;

/// GET /health - Health check (NODE/SEQUENCER modes only)
///
/// Returns 200 OK if the server is healthy, 503 Service Unavailable if not.
#[allow(dead_code)]
pub async fn health_check(
    State(state): State<Arc<AppState>>,
) -> Result<Json<HealthResponse>, (StatusCode, Json<HealthResponse>)> {
    match state.mode {
        ServerMode::Node => check_node_health(state).await,
        ServerMode::Sequencer => check_sequencer_health(state).await,
        ServerMode::Standalone => {
            // This should never be reached - router doesn't add health endpoint for Standalone
            unreachable!("Health endpoint not available in STANDALONE mode")
        }
    }
}

/// Check health of NODE mode (gRPC connection to Sequencer)
#[allow(dead_code)]
async fn check_node_health(
    state: Arc<AppState>,
) -> Result<Json<HealthResponse>, (StatusCode, Json<HealthResponse>)> {
    match state.dispatcher.health_check().await {
        Ok(()) => Ok(Json(HealthResponse {
            status: "healthy".to_string(),
            mode: "NODE".to_string(),
            sequencer_connected: Some(true),
            error: None,
        })),
        Err(_) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "unhealthy".to_string(),
                mode: "NODE".to_string(),
                sequencer_connected: Some(false),
                error: Some("Cannot connect to sequencer".to_string()),
            }),
        )),
    }
}

/// Check health of SEQUENCER mode (storage accessible)
#[allow(dead_code)]
async fn check_sequencer_health(
    state: Arc<AppState>,
) -> Result<Json<HealthResponse>, (StatusCode, Json<HealthResponse>)> {
    // Check if storage is available
    let storage_healthy = state
        .storage
        .as_ref()
        .map(|s| s.is_initialized())
        .unwrap_or(false);

    if storage_healthy {
        Ok(Json(HealthResponse {
            status: "healthy".to_string(),
            mode: "SEQUENCER".to_string(),
            sequencer_connected: None,
            error: None,
        }))
    } else {
        Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "unhealthy".to_string(),
                mode: "SEQUENCER".to_string(),
                sequencer_connected: None,
                error: Some("Storage unavailable".to_string()),
            }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ServerError, ServerResult};
    use crate::traits::Storage;
    use async_trait::async_trait;
    use axum::extract::State;

    struct MockSequencerClientHealthy;

    #[async_trait]
    impl crate::traits::SequencerClient for MockSequencerClientHealthy {
        async fn dispatch(
            &self,
            _params: crate::traits::AppendParams,
        ) -> ServerResult<crate::traits::DispatchResult> {
            unimplemented!()
        }

        async fn dispatch_batch(
            &self,
            _params: Vec<crate::traits::AppendParams>,
        ) -> ServerResult<crate::traits::BatchDispatchResult> {
            unimplemented!()
        }

        async fn get_receipt(
            &self,
            _request: crate::traits::GetReceiptRequest,
        ) -> ServerResult<crate::traits::ReceiptResponse> {
            unimplemented!()
        }

        async fn get_tree_head(&self) -> ServerResult<crate::traits::TreeHead> {
            unimplemented!()
        }

        async fn get_consistency_proof(
            &self,
            _from_size: u64,
            _to_size: u64,
        ) -> ServerResult<crate::traits::ConsistencyProofResponse> {
            unimplemented!()
        }

        async fn get_public_keys(&self) -> ServerResult<Vec<crate::traits::PublicKeyInfo>> {
            unimplemented!()
        }

        async fn trigger_anchoring(
            &self,
            _request: crate::traits::TriggerAnchoringRequest,
        ) -> ServerResult<Vec<crate::traits::AnchoringStatus>> {
            unimplemented!()
        }

        async fn health_check(&self) -> ServerResult<()> {
            Ok(())
        }
    }

    struct MockSequencerClientUnhealthy;

    #[async_trait]
    impl crate::traits::SequencerClient for MockSequencerClientUnhealthy {
        async fn dispatch(
            &self,
            _params: crate::traits::AppendParams,
        ) -> ServerResult<crate::traits::DispatchResult> {
            unimplemented!()
        }

        async fn dispatch_batch(
            &self,
            _params: Vec<crate::traits::AppendParams>,
        ) -> ServerResult<crate::traits::BatchDispatchResult> {
            unimplemented!()
        }

        async fn get_receipt(
            &self,
            _request: crate::traits::GetReceiptRequest,
        ) -> ServerResult<crate::traits::ReceiptResponse> {
            unimplemented!()
        }

        async fn get_tree_head(&self) -> ServerResult<crate::traits::TreeHead> {
            unimplemented!()
        }

        async fn get_consistency_proof(
            &self,
            _from_size: u64,
            _to_size: u64,
        ) -> ServerResult<crate::traits::ConsistencyProofResponse> {
            unimplemented!()
        }

        async fn get_public_keys(&self) -> ServerResult<Vec<crate::traits::PublicKeyInfo>> {
            unimplemented!()
        }

        async fn trigger_anchoring(
            &self,
            _request: crate::traits::TriggerAnchoringRequest,
        ) -> ServerResult<Vec<crate::traits::AnchoringStatus>> {
            unimplemented!()
        }

        async fn health_check(&self) -> ServerResult<()> {
            Err(ServerError::ServiceUnavailable(
                "connection lost".to_string(),
            ))
        }
    }

    struct MockStorageInitialized;

    #[async_trait]
    impl Storage for MockStorageInitialized {
        async fn append_batch(
            &self,
            _params: Vec<crate::traits::AppendParams>,
        ) -> Result<crate::traits::BatchResult, crate::error::StorageError> {
            unimplemented!()
        }

        async fn flush(&self) -> Result<(), crate::error::StorageError> {
            unimplemented!()
        }

        fn tree_head(&self) -> crate::traits::TreeHead {
            unimplemented!()
        }

        fn origin_id(&self) -> [u8; 32] {
            unimplemented!()
        }

        fn is_healthy(&self) -> bool {
            unimplemented!()
        }

        fn get_entry(&self, _id: &uuid::Uuid) -> ServerResult<crate::traits::Entry> {
            unimplemented!()
        }

        fn get_inclusion_proof(
            &self,
            _entry_id: &uuid::Uuid,
            _tree_size: Option<u64>,
        ) -> ServerResult<crate::traits::InclusionProof> {
            unimplemented!()
        }

        fn get_consistency_proof(
            &self,
            _from_size: u64,
            _to_size: u64,
        ) -> ServerResult<crate::traits::ConsistencyProof> {
            unimplemented!()
        }

        fn get_anchors(
            &self,
            _tree_size: u64,
        ) -> ServerResult<Vec<crate::traits::anchor::Anchor>> {
            unimplemented!()
        }

        fn get_latest_anchored_size(&self) -> ServerResult<Option<u64>> {
            unimplemented!()
        }

        fn get_anchors_covering(
            &self,
            _target_tree_size: u64,
            _limit: usize,
        ) -> ServerResult<Vec<crate::traits::anchor::Anchor>> {
            unimplemented!()
        }

        fn get_root_at_size(&self, _tree_size: u64) -> ServerResult<[u8; 32]> {
            unimplemented!()
        }

        fn get_super_root(&self, _super_tree_size: u64) -> ServerResult<[u8; 32]> {
            unimplemented!()
        }

        fn is_initialized(&self) -> bool {
            true
        }
    }

    struct MockStorageUninitialized;

    #[async_trait]
    impl Storage for MockStorageUninitialized {
        async fn append_batch(
            &self,
            _params: Vec<crate::traits::AppendParams>,
        ) -> Result<crate::traits::BatchResult, crate::error::StorageError> {
            unimplemented!()
        }

        async fn flush(&self) -> Result<(), crate::error::StorageError> {
            unimplemented!()
        }

        fn tree_head(&self) -> crate::traits::TreeHead {
            unimplemented!()
        }

        fn origin_id(&self) -> [u8; 32] {
            unimplemented!()
        }

        fn is_healthy(&self) -> bool {
            unimplemented!()
        }

        fn get_entry(&self, _id: &uuid::Uuid) -> ServerResult<crate::traits::Entry> {
            unimplemented!()
        }

        fn get_inclusion_proof(
            &self,
            _entry_id: &uuid::Uuid,
            _tree_size: Option<u64>,
        ) -> ServerResult<crate::traits::InclusionProof> {
            unimplemented!()
        }

        fn get_consistency_proof(
            &self,
            _from_size: u64,
            _to_size: u64,
        ) -> ServerResult<crate::traits::ConsistencyProof> {
            unimplemented!()
        }

        fn get_anchors(
            &self,
            _tree_size: u64,
        ) -> ServerResult<Vec<crate::traits::anchor::Anchor>> {
            unimplemented!()
        }

        fn get_latest_anchored_size(&self) -> ServerResult<Option<u64>> {
            unimplemented!()
        }

        fn get_anchors_covering(
            &self,
            _target_tree_size: u64,
            _limit: usize,
        ) -> ServerResult<Vec<crate::traits::anchor::Anchor>> {
            unimplemented!()
        }

        fn get_root_at_size(&self, _tree_size: u64) -> ServerResult<[u8; 32]> {
            unimplemented!()
        }

        fn get_super_root(&self, _super_tree_size: u64) -> ServerResult<[u8; 32]> {
            unimplemented!()
        }

        fn is_initialized(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn test_health_check_node_healthy() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;

        assert!(result.is_ok());
        let response = result.unwrap().0;
        assert_eq!(response.status, "healthy");
        assert_eq!(response.mode, "NODE");
        assert_eq!(response.sequencer_connected, Some(true));
        assert!(response.error.is_none());
    }

    #[tokio::test]
    async fn test_health_check_node_unhealthy() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientUnhealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;

        assert!(result.is_err());
        let (status_code, response) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.status, "unhealthy");
        assert_eq!(response.mode, "NODE");
        assert_eq!(response.sequencer_connected, Some(false));
        assert_eq!(
            response.error,
            Some("Cannot connect to sequencer".to_string())
        );
    }

    #[tokio::test]
    async fn test_health_check_sequencer_healthy() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageInitialized)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;

        assert!(result.is_ok());
        let response = result.unwrap().0;
        assert_eq!(response.status, "healthy");
        assert_eq!(response.mode, "SEQUENCER");
        assert!(response.sequencer_connected.is_none());
        assert!(response.error.is_none());
    }

    #[tokio::test]
    async fn test_health_check_sequencer_unhealthy_no_storage() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;

        assert!(result.is_err());
        let (status_code, response) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.status, "unhealthy");
        assert_eq!(response.mode, "SEQUENCER");
        assert!(response.sequencer_connected.is_none());
        assert_eq!(response.error, Some("Storage unavailable".to_string()));
    }

    #[tokio::test]
    async fn test_health_check_sequencer_unhealthy_uninitialized_storage() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageUninitialized)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;

        assert!(result.is_err());
        let (status_code, response) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.status, "unhealthy");
        assert_eq!(response.mode, "SEQUENCER");
        assert!(response.sequencer_connected.is_none());
        assert_eq!(response.error, Some("Storage unavailable".to_string()));
    }

    #[tokio::test]
    async fn test_check_node_health_healthy() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_node_health(state).await;

        assert!(result.is_ok());
        let response = result.unwrap().0;
        assert_eq!(response.status, "healthy");
        assert_eq!(response.mode, "NODE");
        assert_eq!(response.sequencer_connected, Some(true));
        assert!(response.error.is_none());
    }

    #[tokio::test]
    async fn test_check_node_health_unhealthy() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientUnhealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_node_health(state).await;

        assert!(result.is_err());
        let (status_code, response) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.status, "unhealthy");
        assert_eq!(response.mode, "NODE");
        assert_eq!(response.sequencer_connected, Some(false));
        assert_eq!(
            response.error,
            Some("Cannot connect to sequencer".to_string())
        );
    }

    #[tokio::test]
    async fn test_check_sequencer_health_healthy() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageInitialized)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_sequencer_health(state).await;

        assert!(result.is_ok());
        let response = result.unwrap().0;
        assert_eq!(response.status, "healthy");
        assert_eq!(response.mode, "SEQUENCER");
        assert!(response.sequencer_connected.is_none());
        assert!(response.error.is_none());
    }

    #[tokio::test]
    async fn test_check_sequencer_health_unhealthy_no_storage() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_sequencer_health(state).await;

        assert!(result.is_err());
        let (status_code, response) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.status, "unhealthy");
        assert_eq!(response.mode, "SEQUENCER");
        assert!(response.sequencer_connected.is_none());
        assert_eq!(response.error, Some("Storage unavailable".to_string()));
    }

    #[tokio::test]
    async fn test_check_sequencer_health_unhealthy_uninitialized() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageUninitialized)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_sequencer_health(state).await;

        assert!(result.is_err());
        let (status_code, response) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.status, "unhealthy");
        assert_eq!(response.mode, "SEQUENCER");
        assert!(response.sequencer_connected.is_none());
        assert_eq!(response.error, Some("Storage unavailable".to_string()));
    }

    #[tokio::test]
    #[should_panic(expected = "Health endpoint not available in STANDALONE mode")]
    async fn test_health_check_standalone_unreachable() {
        let state = Arc::new(AppState {
            mode: ServerMode::Standalone,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let _result = health_check(State(state)).await;
    }

    #[tokio::test]
    async fn test_node_health_with_different_base_url() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "https://example.com:8443".to_string(),
        });

        let result = check_node_health(state).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sequencer_health_storage_is_healthy_method_not_called() {
        // This test verifies that is_initialized is used, not is_healthy
        struct MockStorageHealthyButNotInitialized;

        #[async_trait]
        impl Storage for MockStorageHealthyButNotInitialized {
            async fn append_batch(
                &self,
                _params: Vec<crate::traits::AppendParams>,
            ) -> Result<crate::traits::BatchResult, crate::error::StorageError> {
                unimplemented!()
            }

            async fn flush(&self) -> Result<(), crate::error::StorageError> {
                unimplemented!()
            }

            fn tree_head(&self) -> crate::traits::TreeHead {
                unimplemented!()
            }

            fn origin_id(&self) -> [u8; 32] {
                unimplemented!()
            }

            fn is_healthy(&self) -> bool {
                true // Reports healthy
            }

            fn get_entry(&self, _id: &uuid::Uuid) -> ServerResult<crate::traits::Entry> {
                unimplemented!()
            }

            fn get_inclusion_proof(
                &self,
                _entry_id: &uuid::Uuid,
                _tree_size: Option<u64>,
            ) -> ServerResult<crate::traits::InclusionProof> {
                unimplemented!()
            }

            fn get_consistency_proof(
                &self,
                _from_size: u64,
                _to_size: u64,
            ) -> ServerResult<crate::traits::ConsistencyProof> {
                unimplemented!()
            }

            fn get_anchors(
                &self,
                _tree_size: u64,
            ) -> ServerResult<Vec<crate::traits::anchor::Anchor>> {
                unimplemented!()
            }

            fn get_latest_anchored_size(&self) -> ServerResult<Option<u64>> {
                unimplemented!()
            }

            fn get_anchors_covering(
                &self,
                _target_tree_size: u64,
                _limit: usize,
            ) -> ServerResult<Vec<crate::traits::anchor::Anchor>> {
                unimplemented!()
            }

            fn get_root_at_size(&self, _tree_size: u64) -> ServerResult<[u8; 32]> {
                unimplemented!()
            }

            fn get_super_root(&self, _super_tree_size: u64) -> ServerResult<[u8; 32]> {
                unimplemented!()
            }

            fn is_initialized(&self) -> bool {
                false // Not initialized despite being healthy
            }
        }

        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageHealthyButNotInitialized)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_sequencer_health(state).await;
        // Should be unhealthy because is_initialized returns false
        assert!(result.is_err());
        let (status_code, response) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.status, "unhealthy");
    }

    #[tokio::test]
    async fn test_health_response_serialization_node_healthy() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;
        assert!(result.is_ok());

        let json_response = result.unwrap();
        assert_eq!(json_response.0.status, "healthy");
        assert_eq!(json_response.0.mode, "NODE");
        assert_eq!(json_response.0.sequencer_connected, Some(true));
        assert!(json_response.0.error.is_none());
    }

    #[tokio::test]
    async fn test_health_response_serialization_node_unhealthy() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientUnhealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;
        assert!(result.is_err());

        let (status, json_response) = result.unwrap_err();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json_response.0.status, "unhealthy");
        assert_eq!(json_response.0.mode, "NODE");
        assert_eq!(json_response.0.sequencer_connected, Some(false));
        assert!(json_response.0.error.is_some());
    }

    #[tokio::test]
    async fn test_health_response_serialization_sequencer_healthy() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageInitialized)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;
        assert!(result.is_ok());

        let json_response = result.unwrap();
        assert_eq!(json_response.0.status, "healthy");
        assert_eq!(json_response.0.mode, "SEQUENCER");
        assert!(json_response.0.sequencer_connected.is_none());
        assert!(json_response.0.error.is_none());
    }

    #[tokio::test]
    async fn test_health_response_serialization_sequencer_unhealthy() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;
        assert!(result.is_err());

        let (status, json_response) = result.unwrap_err();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json_response.0.status, "unhealthy");
        assert_eq!(json_response.0.mode, "SEQUENCER");
        assert!(json_response.0.sequencer_connected.is_none());
        assert_eq!(
            json_response.0.error,
            Some("Storage unavailable".to_string())
        );
    }

    #[tokio::test]
    async fn test_check_node_health_with_all_app_state_fields() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://test.example.com:9999".to_string(),
        });

        let result = check_node_health(state).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_sequencer_health_with_initialized_storage() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageInitialized)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_sequencer_health(state).await;
        assert!(result.is_ok());
        let response = result.unwrap().0;
        assert_eq!(response.status, "healthy");
        assert_eq!(response.mode, "SEQUENCER");
    }

    #[tokio::test]
    async fn test_multiple_health_checks_node_consistent() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        // Multiple calls should return consistent results
        for _ in 0..5 {
            let result = check_node_health(state.clone()).await;
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_multiple_health_checks_sequencer_consistent() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageInitialized)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        // Multiple calls should return consistent results
        for _ in 0..5 {
            let result = check_sequencer_health(state.clone()).await;
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_health_check_error_message_content() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientUnhealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_node_health(state).await;
        assert!(result.is_err());

        let (_, response) = result.unwrap_err();
        let error_msg = response.0.error.expect("error should be present");
        assert_eq!(error_msg, "Cannot connect to sequencer");
        assert!(error_msg.len() > 0);
    }

    #[tokio::test]
    async fn test_health_check_node_healthy_response_structure() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;
        assert!(result.is_ok());

        let json = result.unwrap();
        assert_eq!(json.0.status, "healthy");
        assert_eq!(json.0.mode, "NODE");
        assert_eq!(json.0.sequencer_connected, Some(true));
        assert!(json.0.error.is_none());
    }

    #[tokio::test]
    async fn test_health_check_sequencer_storage_initialized_boundary() {
        struct MockStorageEdgeCase;

        #[async_trait]
        impl Storage for MockStorageEdgeCase {
            async fn append_batch(
                &self,
                _params: Vec<crate::traits::AppendParams>,
            ) -> Result<crate::traits::BatchResult, crate::error::StorageError> {
                unimplemented!()
            }

            async fn flush(&self) -> Result<(), crate::error::StorageError> {
                unimplemented!()
            }

            fn tree_head(&self) -> crate::traits::TreeHead {
                unimplemented!()
            }

            fn origin_id(&self) -> [u8; 32] {
                unimplemented!()
            }

            fn is_healthy(&self) -> bool {
                unimplemented!()
            }

            fn get_entry(&self, _id: &uuid::Uuid) -> ServerResult<crate::traits::Entry> {
                unimplemented!()
            }

            fn get_inclusion_proof(
                &self,
                _entry_id: &uuid::Uuid,
                _tree_size: Option<u64>,
            ) -> ServerResult<crate::traits::InclusionProof> {
                unimplemented!()
            }

            fn get_consistency_proof(
                &self,
                _from_size: u64,
                _to_size: u64,
            ) -> ServerResult<crate::traits::ConsistencyProof> {
                unimplemented!()
            }

            fn get_anchors(
                &self,
                _tree_size: u64,
            ) -> ServerResult<Vec<crate::traits::anchor::Anchor>> {
                unimplemented!()
            }

            fn get_latest_anchored_size(&self) -> ServerResult<Option<u64>> {
                unimplemented!()
            }

            fn get_anchors_covering(
                &self,
                _target_tree_size: u64,
                _limit: usize,
            ) -> ServerResult<Vec<crate::traits::anchor::Anchor>> {
                unimplemented!()
            }

            fn get_root_at_size(&self, _tree_size: u64) -> ServerResult<[u8; 32]> {
                unimplemented!()
            }

            fn get_super_root(&self, _super_tree_size: u64) -> ServerResult<[u8; 32]> {
                unimplemented!()
            }

            fn is_initialized(&self) -> bool {
                true
            }
        }

        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageEdgeCase)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_sequencer_health(state).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_node_health_error_status_code() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientUnhealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_node_health(state).await;
        assert!(result.is_err());

        let (status_code, _) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_check_sequencer_health_error_status_code() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_sequencer_health(state).await;
        assert!(result.is_err());

        let (status_code, _) = result.unwrap_err();
        assert_eq!(status_code, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_health_check_response_json_structure_node() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;
        assert!(result.is_ok());

        let json_response = result.unwrap();
        let response = json_response.0;

        assert!(!response.status.is_empty());
        assert!(!response.mode.is_empty());
        assert!(response.sequencer_connected.is_some());
    }

    #[tokio::test]
    async fn test_health_check_response_json_structure_sequencer() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageInitialized)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;
        assert!(result.is_ok());

        let json_response = result.unwrap();
        let response = json_response.0;

        assert!(!response.status.is_empty());
        assert!(!response.mode.is_empty());
        assert!(response.sequencer_connected.is_none());
        assert!(response.error.is_none());
    }

    #[tokio::test]
    async fn test_node_health_with_empty_base_url() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "".to_string(),
        });

        let result = check_node_health(state).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sequencer_health_with_various_storage_states() {
        struct MockStorageVariant {
            initialized: bool,
        }

        #[async_trait]
        impl Storage for MockStorageVariant {
            async fn append_batch(
                &self,
                _params: Vec<crate::traits::AppendParams>,
            ) -> Result<crate::traits::BatchResult, crate::error::StorageError> {
                unimplemented!()
            }

            async fn flush(&self) -> Result<(), crate::error::StorageError> {
                unimplemented!()
            }

            fn tree_head(&self) -> crate::traits::TreeHead {
                unimplemented!()
            }

            fn origin_id(&self) -> [u8; 32] {
                unimplemented!()
            }

            fn is_healthy(&self) -> bool {
                unimplemented!()
            }

            fn get_entry(&self, _id: &uuid::Uuid) -> ServerResult<crate::traits::Entry> {
                unimplemented!()
            }

            fn get_inclusion_proof(
                &self,
                _entry_id: &uuid::Uuid,
                _tree_size: Option<u64>,
            ) -> ServerResult<crate::traits::InclusionProof> {
                unimplemented!()
            }

            fn get_consistency_proof(
                &self,
                _from_size: u64,
                _to_size: u64,
            ) -> ServerResult<crate::traits::ConsistencyProof> {
                unimplemented!()
            }

            fn get_anchors(
                &self,
                _tree_size: u64,
            ) -> ServerResult<Vec<crate::traits::anchor::Anchor>> {
                unimplemented!()
            }

            fn get_latest_anchored_size(&self) -> ServerResult<Option<u64>> {
                unimplemented!()
            }

            fn get_anchors_covering(
                &self,
                _target_tree_size: u64,
                _limit: usize,
            ) -> ServerResult<Vec<crate::traits::anchor::Anchor>> {
                unimplemented!()
            }

            fn get_root_at_size(&self, _tree_size: u64) -> ServerResult<[u8; 32]> {
                unimplemented!()
            }

            fn get_super_root(&self, _super_tree_size: u64) -> ServerResult<[u8; 32]> {
                unimplemented!()
            }

            fn is_initialized(&self) -> bool {
                self.initialized
            }
        }

        // Test with initialized=true
        let state_initialized = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageVariant { initialized: true })),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result_initialized = check_sequencer_health(state_initialized).await;
        assert!(result_initialized.is_ok());

        // Test with initialized=false
        let state_not_initialized = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageVariant {
                initialized: false,
            })),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result_not_initialized = check_sequencer_health(state_not_initialized).await;
        assert!(result_not_initialized.is_err());
    }

    #[tokio::test]
    async fn test_health_check_concurrent_calls() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let mut handles = vec![];
        for _ in 0..10 {
            let state_clone = Arc::clone(&state);
            handles.push(tokio::spawn(async move {
                health_check(State(state_clone)).await
            }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_health_check_mode_matching() {
        let node_state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let node_result = health_check(State(node_state)).await;
        assert!(node_result.is_ok());
        assert_eq!(node_result.unwrap().0.mode, "NODE");

        let sequencer_state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageInitialized)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let sequencer_result = health_check(State(sequencer_state)).await;
        assert!(sequencer_result.is_ok());
        assert_eq!(sequencer_result.unwrap().0.mode, "SEQUENCER");
    }

    #[tokio::test]
    async fn test_node_health_error_exact_message() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientUnhealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_node_health(state).await;
        assert!(result.is_err());

        let (status, response) = result.unwrap_err();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.0.status, "unhealthy");
        assert_eq!(response.0.error, Some("Cannot connect to sequencer".to_string()));
    }

    #[tokio::test]
    async fn test_sequencer_health_error_exact_message() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = check_sequencer_health(state).await;
        assert!(result.is_err());

        let (status, response) = result.unwrap_err();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.0.status, "unhealthy");
        assert_eq!(response.0.error, Some("Storage unavailable".to_string()));
    }

    #[tokio::test]
    async fn test_health_check_all_app_state_fields_populated() {
        // Test with all optional fields set to Some
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageInitialized)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "https://production.example.com".to_string(),
        });

        let result = health_check(State(state)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_node_health_dispatcher_call() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        // Call directly to ensure dispatcher health_check is invoked
        let result = check_node_health(state).await;

        assert!(result.is_ok());
        let response = result.unwrap().0;
        assert_eq!(response.sequencer_connected, Some(true));
    }

    #[tokio::test]
    async fn test_check_sequencer_health_storage_check() {
        let state = Arc::new(AppState {
            mode: ServerMode::Sequencer,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: Some(Arc::new(MockStorageInitialized)),
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        // Call directly to ensure storage is_initialized is checked
        let result = check_sequencer_health(state).await;

        assert!(result.is_ok());
        let response = result.unwrap().0;
        assert_eq!(response.status, "healthy");
        assert!(response.sequencer_connected.is_none());
    }

    #[tokio::test]
    async fn test_health_response_fields_not_empty() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientHealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;
        assert!(result.is_ok());

        let response = result.unwrap().0;
        assert!(!response.status.is_empty());
        assert!(!response.mode.is_empty());
    }

    #[tokio::test]
    async fn test_error_response_fields_populated() {
        let state = Arc::new(AppState {
            mode: ServerMode::Node,
            dispatcher: Arc::new(MockSequencerClientUnhealthy),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens: None,
            base_url: "http://localhost:3000".to_string(),
        });

        let result = health_check(State(state)).await;
        assert!(result.is_err());

        let (_, response) = result.unwrap_err();
        assert!(!response.0.status.is_empty());
        assert!(!response.0.mode.is_empty());
        assert!(response.0.error.is_some());
        assert!(!response.0.error.unwrap().is_empty());
    }
}
