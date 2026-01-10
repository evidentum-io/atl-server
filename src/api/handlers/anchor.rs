//! Anchor endpoint handlers

use std::sync::Arc;

use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use uuid::Uuid;

use crate::api::dto::AnchorJsonRequest;
use crate::api::state::AppState;
use crate::api::streaming::{hash_json_payload, hash_metadata};
use crate::config::ServerMode;
use crate::error::ServerError;
use crate::traits::{AppendParams, GetReceiptRequest};

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

    // Generate upgrade URL
    let entry_id = dispatch_result.result.id;
    let upgrade_url = format!("{}/v1/anchor/{}", state.base_url, entry_id);

    // Build receipt (STUB - will be replaced by RECEIPT-GEN-1)
    let receipt = build_receipt_stub(&dispatch_result, &upgrade_url, metadata, external_id);

    Ok((StatusCode::CREATED, Json(receipt)))
}

/// GET /v1/anchor/{id} - Get receipt with current anchors
#[allow(dead_code)]
pub async fn get_anchor(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Receipt>, ServerError> {
    // Request receipt from dispatcher
    let request = GetReceiptRequest {
        entry_id: id,
        include_anchors: true,
    };

    let response = state.dispatcher.get_receipt(request).await?;

    // Generate upgrade URL
    let upgrade_url = format!("{}/v1/anchor/{}", state.base_url, id);

    // Build receipt with anchors (STUB - will be replaced by RECEIPT-GEN-1)
    let receipt = build_receipt_with_anchors_stub(&response, &upgrade_url);

    Ok(Json(receipt))
}

// ========== STUBS (to be replaced by RECEIPT-GEN-1) ==========

/// Build receipt stub for POST response
///
/// This is a placeholder. RECEIPT-GEN-1 will implement proper receipt generation.
fn build_receipt_stub(
    dispatch_result: &crate::traits::DispatchResult,
    upgrade_url: &str,
    metadata: Option<serde_json::Value>,
    external_id: Option<String>,
) -> Receipt {
    use crate::api::handlers::helpers::{format_hash, format_signature};

    let result = &dispatch_result.result;
    let checkpoint = &dispatch_result.checkpoint;

    let mut entry_json = serde_json::json!({
        "id": result.id.to_string(),
        "payload_hash": format_hash(&result.tree_head.root_hash), // STUB: should be entry's payload hash
        "metadata_hash": format_hash(&result.tree_head.root_hash), // STUB: should be entry's metadata hash
        "metadata": metadata.unwrap_or_else(|| serde_json::json!({}))
    });

    // Add external_id if present
    if let Some(ext_id) = external_id {
        entry_json["external_id"] = serde_json::json!(ext_id);
    }

    serde_json::json!({
        "spec_version": "1.0.0",
        "upgrade_url": upgrade_url,
        "entry": entry_json,
        "proof": {
            "tree_size": checkpoint.tree_size,
            "root_hash": format_hash(&checkpoint.root_hash),
            "path": result.inclusion_proof.iter().map(format_hash).collect::<Vec<_>>(),
            "leaf_index": result.leaf_index,
            "checkpoint": {
                "origin": format_hash(&checkpoint.origin),
                "tree_size": checkpoint.tree_size,
                "root_hash": format_hash(&checkpoint.root_hash),
                "timestamp": checkpoint.timestamp,
                "signature": format_signature(&checkpoint.signature),
                "key_id": format_hash(&checkpoint.origin), // STUB: should be key_id
            }
        },
        "anchors": []
    })
}

/// Build receipt stub for GET response with anchors
///
/// This is a placeholder. RECEIPT-GEN-1 will implement proper receipt generation.
#[allow(dead_code)]
fn build_receipt_with_anchors_stub(
    response: &crate::traits::ReceiptResponse,
    upgrade_url: &str,
) -> Receipt {
    use crate::api::handlers::helpers::{format_hash, format_signature};

    let entry = &response.entry;
    let checkpoint = &response.checkpoint;

    let mut entry_json = serde_json::json!({
        "id": entry.id.to_string(),
        "payload_hash": format_hash(&entry.payload_hash),
        "metadata_hash": format_hash(&entry.metadata_hash),
        "metadata": entry.metadata_cleartext.as_ref().unwrap_or(&serde_json::json!({}))
    });

    // Add external_id if present
    if let Some(ref ext_id) = entry.external_id {
        entry_json["external_id"] = serde_json::json!(ext_id);
    }

    serde_json::json!({
        "spec_version": "1.0.0",
        "upgrade_url": upgrade_url,
        "entry": entry_json,
        "proof": {
            "tree_size": checkpoint.tree_size,
            "root_hash": format_hash(&checkpoint.root_hash),
            "path": response.inclusion_proof.iter().map(format_hash).collect::<Vec<_>>(),
            "leaf_index": entry.leaf_index.unwrap_or(0),
            "checkpoint": {
                "origin": format_hash(&checkpoint.origin),
                "tree_size": checkpoint.tree_size,
                "root_hash": format_hash(&checkpoint.root_hash),
                "timestamp": checkpoint.timestamp,
                "signature": format_signature(&checkpoint.signature),
                "key_id": format_hash(&checkpoint.origin),
            }
        },
        "anchors": [] // STUB: will be populated from response.anchors
    })
}
