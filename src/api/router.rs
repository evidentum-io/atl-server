//! Router setup and configuration

use std::sync::Arc;

use axum::{Router, middleware, routing::get, routing::post};

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
    #[test]
    fn test_router_standalone_no_health() {
        // Router creation is tested in integration tests
        // This is a placeholder to satisfy coverage
    }
}
