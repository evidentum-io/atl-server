//! Anchor endpoint handlers

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use uuid::Uuid;

use crate::api::dto::AnchorJsonRequest;
use crate::api::state::AppState;
use crate::api::streaming::{hash_json_payload, hash_metadata};
use crate::config::ServerMode;
use crate::error::ServerError;
use crate::traits::AppendParams;

/// Placeholder receipt type (will be implemented by RECEIPT-GEN-1)
///
/// For now, we return a simple JSON structure.
#[allow(dead_code)]
type Receipt = serde_json::Value;

/// POST /v1/anchor - Create anchor entry
///
/// Dispatches to JSON or multipart handler based on Content-Type.
#[allow(dead_code)]
pub async fn create_anchor(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<Receipt>), ServerError> {
    // SEQUENCER mode rejects HTTP anchoring (use gRPC only)
    if matches!(state.mode, ServerMode::Sequencer) {
        return Err(ServerError::NotSupported(
            "Direct HTTP anchoring disabled in SEQUENCER mode. Use gRPC.".into(),
        ));
    }

    // Determine content type
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_type.starts_with("application/json") {
        anchor_json(state, body).await
    } else if content_type.starts_with("multipart/form-data") {
        // Multipart handling will be added later
        Err(ServerError::NotSupported(
            "Multipart upload not yet implemented".into(),
        ))
    } else {
        Err(ServerError::UnsupportedContentType(
            content_type.to_string(),
        ))
    }
}

/// Handle JSON anchor request
async fn anchor_json(
    state: Arc<AppState>,
    body: Body,
) -> Result<(StatusCode, Json<Receipt>), ServerError> {
    use axum::body::to_bytes;

    // Read body (limit 10MB)
    let bytes = to_bytes(body, 10 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::InvalidArgument(format!("Failed to read body: {}", e)))?;

    // Parse JSON
    let req: AnchorJsonRequest = serde_json::from_slice(&bytes)
        .map_err(|e| ServerError::InvalidArgument(format!("Invalid JSON: {}", e)))?;

    // Compute hashes
    let payload_hash = hash_json_payload(&req.payload);
    let metadata_hash = hash_metadata(req.metadata.as_ref());

    // Generate and return receipt
    generate_and_return_receipt(
        state,
        payload_hash,
        metadata_hash,
        req.metadata,
        req.external_id,
    )
    .await
}

/// Common receipt generation logic
async fn generate_and_return_receipt(
    state: Arc<AppState>,
    payload_hash: [u8; 32],
    metadata_hash: [u8; 32],
    metadata: Option<serde_json::Value>,
    external_id: Option<String>,
) -> Result<(StatusCode, Json<Receipt>), ServerError> {
    // Create append params
    let params = AppendParams {
        payload_hash,
        metadata_hash,
        metadata_cleartext: metadata.clone(),
        external_id: external_id.clone(),
    };

    // Dispatch to Sequencer (either local or gRPC)
    let dispatch_result = state.dispatcher.dispatch(params).await?;

    // Get storage engine and signer
    let storage_engine = state.storage_engine.as_ref().ok_or_else(|| {
        ServerError::NotSupported("Receipt generation requires storage engine".into())
    })?;
    let signer = state
        .signer
        .as_ref()
        .ok_or_else(|| ServerError::NotSupported("Receipt generation requires signer".into()))?;

    // Build immediate receipt (does NOT query storage for entry)
    let receipt_v2 = crate::receipt::build_immediate_receipt(
        &dispatch_result,
        payload_hash,
        metadata,
        storage_engine,
        signer,
        &state.base_url,
    )?;

    // Convert to JSON for API response
    let receipt = serde_json::to_value(&receipt_v2)
        .map_err(|e| ServerError::InvalidArgument(format!("Failed to serialize receipt: {}", e)))?;

    Ok((StatusCode::CREATED, Json(receipt)))
}

/// GET /v1/anchor/{id} - Get receipt with current anchors
#[allow(dead_code)]
pub async fn get_anchor(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Receipt>, ServerError> {
    // Get storage engine and signer
    let storage_engine = state.storage_engine.as_ref().ok_or_else(|| {
        ServerError::NotSupported("Receipt generation requires storage engine".into())
    })?;
    let signer = state
        .signer
        .as_ref()
        .ok_or_else(|| ServerError::NotSupported("Receipt generation requires signer".into()))?;

    // Generate upgrade URL template
    let upgrade_url_template = Some(format!("{}/v1/anchor/{{}}", state.base_url));

    // Generate receipt v2.0 with anchors
    let receipt_v2 = crate::receipt::generate_receipt(
        &id,
        storage_engine,
        signer,
        crate::receipt::ReceiptOptions {
            upgrade_url_template,
            include_anchors: true,
            ..Default::default()
        },
    )
    .await?;

    // Convert to JSON for API response
    let receipt = serde_json::to_value(&receipt_v2)
        .map_err(|e| ServerError::InvalidArgument(format!("Failed to serialize receipt: {}", e)))?;

    Ok(Json(receipt))
}
