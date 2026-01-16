//! Bearer token authentication middleware

use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{header::AUTHORIZATION, Request, StatusCode},
    middleware::Next,
    response::Response,
};

use crate::api::state::AppState;
use crate::error::ServerError;

/// Authentication middleware - validates Bearer tokens
///
/// Only applied when access_tokens are configured.
/// Returns 401 if token is missing or invalid.
#[allow(dead_code)]
pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let tokens = state
        .access_tokens
        .as_ref()
        .expect("middleware only applied when tokens configured");

    // Get Authorization header
    let auth_header = req.headers().get(AUTHORIZATION).ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            ServerError::AuthMissing.to_string(),
        )
    })?;

    // Parse header value
    let auth_str = auth_header.to_str().map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            ServerError::AuthInvalid.to_string(),
        )
    })?;

    // Check Bearer prefix
    if !auth_str.starts_with("Bearer ") {
        return Err((
            StatusCode::UNAUTHORIZED,
            ServerError::AuthInvalid.to_string(),
        ));
    }

    // Extract and validate token
    let token = &auth_str[7..];
    if !tokens.iter().any(|t| t == token) {
        return Err((
            StatusCode::UNAUTHORIZED,
            ServerError::AuthInvalid.to_string(),
        ));
    }

    // Token valid, proceed
    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{header::AUTHORIZATION, Request, StatusCode},
        routing::get,
        Router,
    };
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::api::state::AppState;
    use crate::config::ServerMode;
    use crate::error::ServerError;

    /// Mock dispatcher for testing (minimal implementation)
    struct MockDispatcher;

    #[axum::async_trait]
    impl crate::traits::SequencerClient for MockDispatcher {
        async fn dispatch(
            &self,
            _params: crate::traits::AppendParams,
        ) -> Result<crate::traits::DispatchResult, crate::error::ServerError> {
            unimplemented!("not used in auth tests")
        }

        async fn dispatch_batch(
            &self,
            _params: Vec<crate::traits::AppendParams>,
        ) -> Result<crate::traits::BatchDispatchResult, crate::error::ServerError> {
            unimplemented!("not used in auth tests")
        }

        async fn get_receipt(
            &self,
            _request: crate::traits::GetReceiptRequest,
        ) -> Result<crate::traits::ReceiptResponse, crate::error::ServerError> {
            unimplemented!("not used in auth tests")
        }

        async fn get_tree_head(
            &self,
        ) -> Result<crate::traits::TreeHead, crate::error::ServerError> {
            unimplemented!("not used in auth tests")
        }

        async fn get_consistency_proof(
            &self,
            _from_size: u64,
            _to_size: u64,
        ) -> Result<crate::traits::ConsistencyProofResponse, crate::error::ServerError> {
            unimplemented!("not used in auth tests")
        }

        async fn get_public_keys(
            &self,
        ) -> Result<Vec<crate::traits::PublicKeyInfo>, crate::error::ServerError> {
            unimplemented!("not used in auth tests")
        }

        async fn trigger_anchoring(
            &self,
            _request: crate::traits::TriggerAnchoringRequest,
        ) -> Result<Vec<crate::traits::AnchoringStatus>, crate::error::ServerError> {
            unimplemented!("not used in auth tests")
        }

        async fn health_check(&self) -> Result<(), crate::error::ServerError> {
            unimplemented!("not used in auth tests")
        }
    }

    /// Helper to create test AppState
    fn create_test_state(access_tokens: Option<Vec<String>>) -> Arc<AppState> {
        Arc::new(AppState {
            mode: ServerMode::Standalone,
            dispatcher: Arc::new(MockDispatcher),
            storage: None,
            storage_engine: None,
            signer: None,
            access_tokens,
            base_url: "http://localhost:8080".to_string(),
        })
    }

    /// Helper to create test router with auth middleware
    fn create_test_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/test", get(|| async { "ok" }))
            .route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_auth_middleware_missing_header() {
        let state = create_test_state(Some(vec!["valid-token".to_string()]));
        let app = create_test_router(state);

        let req = Request::builder()
            .uri("/test")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(body_str, ServerError::AuthMissing.to_string());
    }

    #[tokio::test]
    async fn test_auth_middleware_invalid_header_encoding() {
        let state = create_test_state(Some(vec!["valid-token".to_string()]));
        let app = create_test_router(state);

        // Create request with invalid UTF-8 in Authorization header
        let mut req = Request::builder().uri("/test").body(Body::empty()).unwrap();
        req.headers_mut().insert(
            AUTHORIZATION,
            axum::http::HeaderValue::from_bytes(&[0xFF, 0xFE, 0xFD]).unwrap(),
        );

        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(body_str, ServerError::AuthInvalid.to_string());
    }

    #[tokio::test]
    async fn test_auth_middleware_missing_bearer_prefix() {
        let state = create_test_state(Some(vec!["valid-token".to_string()]));
        let app = create_test_router(state);

        let req = Request::builder()
            .uri("/test")
            .header(AUTHORIZATION, "valid-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(body_str, ServerError::AuthInvalid.to_string());
    }

    #[tokio::test]
    async fn test_auth_middleware_wrong_bearer_case() {
        let state = create_test_state(Some(vec!["valid-token".to_string()]));
        let app = create_test_router(state);

        let req = Request::builder()
            .uri("/test")
            .header(AUTHORIZATION, "bearer valid-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(body_str, ServerError::AuthInvalid.to_string());
    }

    #[tokio::test]
    async fn test_auth_middleware_invalid_token() {
        let state = create_test_state(Some(vec!["valid-token".to_string()]));
        let app = create_test_router(state);

        let req = Request::builder()
            .uri("/test")
            .header(AUTHORIZATION, "Bearer invalid-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(body_str, ServerError::AuthInvalid.to_string());
    }

    #[tokio::test]
    async fn test_auth_middleware_valid_token_single() {
        let state = create_test_state(Some(vec!["valid-token".to_string()]));
        let app = create_test_router(state);

        let req = Request::builder()
            .uri("/test")
            .header(AUTHORIZATION, "Bearer valid-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(body_str, "ok");
    }

    #[tokio::test]
    async fn test_auth_middleware_valid_token_multiple() {
        let state = create_test_state(Some(vec![
            "token-1".to_string(),
            "token-2".to_string(),
            "token-3".to_string(),
        ]));

        // Test first token
        let app = create_test_router(state.clone());
        let req = Request::builder()
            .uri("/test")
            .header(AUTHORIZATION, "Bearer token-1")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Test middle token
        let app = create_test_router(state.clone());
        let req = Request::builder()
            .uri("/test")
            .header(AUTHORIZATION, "Bearer token-2")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Test last token
        let app = create_test_router(state);
        let req = Request::builder()
            .uri("/test")
            .header(AUTHORIZATION, "Bearer token-3")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_middleware_empty_token() {
        let state = create_test_state(Some(vec!["valid-token".to_string()]));
        let app = create_test_router(state);

        let req = Request::builder()
            .uri("/test")
            .header(AUTHORIZATION, "Bearer ")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(body_str, ServerError::AuthInvalid.to_string());
    }

    #[tokio::test]
    async fn test_auth_middleware_token_with_spaces() {
        let state = create_test_state(Some(vec!["token with spaces".to_string()]));
        let app = create_test_router(state);

        let req = Request::builder()
            .uri("/test")
            .header(AUTHORIZATION, "Bearer token with spaces")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_middleware_bearer_only() {
        let state = create_test_state(Some(vec!["valid-token".to_string()]));
        let app = create_test_router(state);

        let req = Request::builder()
            .uri("/test")
            .header(AUTHORIZATION, "Bearer")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(body_str, ServerError::AuthInvalid.to_string());
    }

    #[tokio::test]
    #[should_panic(expected = "middleware only applied when tokens configured")]
    async fn test_auth_middleware_panic_when_no_tokens_configured() {
        let state = create_test_state(None);
        let app = create_test_router(state);

        let req = Request::builder()
            .uri("/test")
            .header(AUTHORIZATION, "Bearer token")
            .body(Body::empty())
            .unwrap();

        // This should panic because access_tokens is None
        let _ = app.oneshot(req).await;
    }
}
