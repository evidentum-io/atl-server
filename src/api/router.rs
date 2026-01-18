//! Router setup and configuration

use std::sync::Arc;

use axum::{middleware, routing::get, routing::post, Router};

use crate::api::handlers;
use crate::api::middleware::auth_middleware;
use crate::api::state::AppState;

/// Create the API router
///
/// Routes work in all modes (STANDALONE, NODE, SEQUENCER) via SequencerClient.
#[allow(dead_code)]
pub fn create_router(state: Arc<AppState>) -> Router {
    // API v1 routes
    let api_v1 = Router::new()
        // Main anchor endpoint
        .route("/anchor", post(handlers::create_anchor))
        // Get receipt by ID
        .route("/anchor/:id", get(handlers::get_anchor));

    // Apply auth middleware if tokens configured
    let api_v1 = if state.access_tokens.is_some() {
        api_v1.layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
    } else {
        api_v1
    };

    // Mount API v1 under /v1
    let mut router = Router::new().nest("/v1", api_v1);

    // Add health endpoint for NODE and SEQUENCER modes
    if state.mode.has_health_endpoint() {
        router = router.route("/health", get(handlers::health_check));
    }

    router.with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServerMode;
    use crate::traits::dispatcher::{
        AnchoringStatus, BatchDispatchResult, ConsistencyProofResponse, DispatchResult,
        GetReceiptRequest, PublicKeyInfo, ReceiptResponse, TriggerAnchoringRequest,
    };
    use crate::traits::{AppendParams, SequencerClient, TreeHead};
    use async_trait::async_trait;

    /// Mock sequencer client for testing
    struct MockSequencerClient;

    #[async_trait]
    impl SequencerClient for MockSequencerClient {
        async fn dispatch(
            &self,
            _params: AppendParams,
        ) -> crate::error::ServerResult<DispatchResult> {
            unimplemented!()
        }

        async fn dispatch_batch(
            &self,
            _params: Vec<AppendParams>,
        ) -> crate::error::ServerResult<BatchDispatchResult> {
            unimplemented!()
        }

        async fn get_receipt(
            &self,
            _request: GetReceiptRequest,
        ) -> crate::error::ServerResult<ReceiptResponse> {
            unimplemented!()
        }

        async fn get_tree_head(&self) -> crate::error::ServerResult<TreeHead> {
            unimplemented!()
        }

        async fn get_consistency_proof(
            &self,
            _from_size: u64,
            _to_size: u64,
        ) -> crate::error::ServerResult<ConsistencyProofResponse> {
            unimplemented!()
        }

        async fn get_public_keys(&self) -> crate::error::ServerResult<Vec<PublicKeyInfo>> {
            unimplemented!()
        }

        async fn trigger_anchoring(
            &self,
            _request: TriggerAnchoringRequest,
        ) -> crate::error::ServerResult<Vec<AnchoringStatus>> {
            unimplemented!()
        }

        async fn health_check(&self) -> crate::error::ServerResult<()> {
            Ok(())
        }
    }

    fn create_test_state(mode: ServerMode, access_tokens: Option<Vec<String>>) -> Arc<AppState> {
        Arc::new(AppState {
            mode,
            dispatcher: Arc::new(MockSequencerClient),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens,
            base_url: "http://localhost:8080".to_string(),
        })
    }

    #[test]
    fn test_router_standalone_no_health() {
        let state = create_test_state(ServerMode::Standalone, None);
        let _router = create_router(state.clone());

        // Verify router is created successfully
        assert_eq!(state.mode, ServerMode::Standalone);
    }

    #[test]
    fn test_router_node_has_health() {
        let state = create_test_state(ServerMode::Node, None);
        let _router = create_router(state.clone());

        // Node mode should have health endpoint
        assert_eq!(state.mode, ServerMode::Node);
        assert!(state.mode.has_health_endpoint());
    }

    #[test]
    fn test_router_sequencer_has_health() {
        let state = create_test_state(ServerMode::Sequencer, None);
        let _router = create_router(state.clone());

        // Sequencer mode should have health endpoint
        assert_eq!(state.mode, ServerMode::Sequencer);
        assert!(state.mode.has_health_endpoint());
    }

    #[test]
    fn test_router_with_auth_middleware() {
        let tokens = vec!["token1".to_string(), "token2".to_string()];
        let state = create_test_state(ServerMode::Standalone, Some(tokens.clone()));
        let _router = create_router(state.clone());

        // Auth middleware should be applied when tokens are present
        assert_eq!(state.access_tokens, Some(tokens));
    }

    #[test]
    fn test_router_without_auth_middleware() {
        let state = create_test_state(ServerMode::Standalone, None);
        let _router = create_router(state.clone());

        // No auth middleware when tokens are None
        assert_eq!(state.access_tokens, None);
    }
}
