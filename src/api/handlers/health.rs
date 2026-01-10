//! Health check handler

use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode};

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
