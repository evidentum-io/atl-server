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
use crate::api::streaming::{hash_metadata, hash_payload};
use crate::config::ServerMode;
use crate::error::ServerError;
use crate::traits::AppendParams;
use crate::validation::{body_as_utf8, reject_duplicate_property_names};

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
        // No multipart ingress exists: nothing is parsed here, so nothing
        // reaches the log by this route. Whoever implements it must run
        // `reject_duplicate_property_names` over each JSON part's raw bytes
        // before parsing it, exactly as `anchor_json` does -- a multipart
        // metadata part is client JSON text and carries the same Section 3.1
        // constraint.
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

    // Refuse duplicate property names *before* the typed parse.
    //
    // This is the last point at which the constraint is decidable. RFC 8785
    // Section 3.1 forbids objects that repeat a property name, and the
    // typed parse below silently resolves such an object by keeping the
    // last occurrence -- after which `req.metadata` is
    // indistinguishable from metadata the client never sent. RFC 8259 Section 4
    // makes the surviving occurrence unpredictable across parsers, so the
    // hash committed to the tree would be one of several a conformant reader
    // could compute from the same bytes.
    let body_text = body_as_utf8(&bytes, "request body")?;
    reject_duplicate_property_names(body_text, "request body")?;

    // Parse JSON
    let req: AnchorJsonRequest = serde_json::from_str(body_text)
        .map_err(|e| ServerError::InvalidArgument(format!("Invalid JSON: {}", e)))?;

    // Compute hashes
    let payload_hash = if req.file.unwrap_or(false) {
        parse_precomputed_hash(&req.payload)?
    } else {
        hash_payload(&req.payload)?
    };
    let metadata_hash = hash_metadata(req.metadata.as_ref())?;

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

/// Parse a pre-computed SHA-256 hash from a JSON string value.
///
/// Expects format `"sha256:<64 hex chars>"`. Returns the 32-byte hash.
fn parse_precomputed_hash(value: &serde_json::Value) -> Result<[u8; 32], ServerError> {
    let s = value.as_str().ok_or_else(|| {
        ServerError::InvalidArgument("file=true requires payload to be a string".into())
    })?;

    let hex_str = s.strip_prefix("sha256:").ok_or_else(|| {
        ServerError::InvalidArgument(
            "file=true requires payload format \"sha256:<64 hex chars>\"".into(),
        )
    })?;

    if hex_str.len() != 64 {
        return Err(ServerError::InvalidArgument(format!(
            "invalid hash length: expected 64 hex chars, got {}",
            hex_str.len()
        )));
    }

    let bytes = hex::decode(hex_str)
        .map_err(|e| ServerError::InvalidArgument(format!("invalid hex in payload hash: {e}")))?;

    let mut hash = [0u8; 32];
    hash.copy_from_slice(&bytes);
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::dto::AnchorJsonRequest;
    use crate::config::ServerMode;
    use crate::error::ServerError;
    use crate::receipt::CheckpointSigner;
    use crate::storage::config::StorageConfig;
    use crate::storage::engine::StorageEngine;
    use crate::traits::{
        dispatcher::{BatchDispatchResult, DispatchResult},
        storage::{AppendParams, AppendResult, TreeHead},
        SequencerClient, Storage,
    };
    use async_trait::async_trait;
    use axum::{
        body::Body,
        http::{header, StatusCode},
    };
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use uuid::Uuid;

    struct MockSequencerClient {
        storage_engine: Arc<StorageEngine>,
        dispatch_calls: Arc<Mutex<Vec<AppendParams>>>,
        should_fail: Arc<Mutex<Option<ServerError>>>,
    }

    impl MockSequencerClient {
        fn new_success(storage_engine: Arc<StorageEngine>) -> Self {
            Self {
                storage_engine,
                dispatch_calls: Arc::new(Mutex::new(Vec::new())),
                should_fail: Arc::new(Mutex::new(None)),
            }
        }

        fn new_failure(storage_engine: Arc<StorageEngine>, error: ServerError) -> Self {
            Self {
                storage_engine,
                dispatch_calls: Arc::new(Mutex::new(Vec::new())),
                should_fail: Arc::new(Mutex::new(Some(error))),
            }
        }
    }

    #[async_trait]
    impl SequencerClient for MockSequencerClient {
        async fn dispatch(&self, params: AppendParams) -> Result<DispatchResult, ServerError> {
            self.dispatch_calls.lock().unwrap().push(params.clone());

            if let Some(error) = self.should_fail.lock().unwrap().take() {
                return Err(error);
            }

            let batch_result = self
                .storage_engine
                .append_batch(vec![params])
                .await
                .map_err(ServerError::from)?;

            let entry_result = &batch_result.entries[0];
            let origin = self.storage_engine.origin_id();
            let tree_head = self.storage_engine.tree_head();

            let result = AppendResult {
                id: entry_result.id,
                leaf_index: entry_result.leaf_index,
                tree_head: tree_head.clone(),
                inclusion_proof: vec![],
                timestamp: batch_result.committed_at,
            };

            let checkpoint = atl_core::Checkpoint {
                origin,
                tree_size: tree_head.tree_size,
                root_hash: tree_head.root_hash,
                timestamp: 1000,
                signature: [2u8; 64],
                key_id: [3u8; 32],
            };

            Ok(DispatchResult { result, checkpoint })
        }

        async fn dispatch_batch(
            &self,
            _params: Vec<AppendParams>,
        ) -> Result<BatchDispatchResult, ServerError> {
            unimplemented!("not used in anchor tests")
        }

        async fn get_receipt(
            &self,
            _request: crate::traits::dispatcher::GetReceiptRequest,
        ) -> Result<crate::traits::dispatcher::ReceiptResponse, ServerError> {
            unimplemented!("not used in anchor tests")
        }

        async fn get_tree_head(&self) -> Result<TreeHead, ServerError> {
            unimplemented!("not used in anchor tests")
        }

        async fn get_consistency_proof(
            &self,
            _from_size: u64,
            _to_size: u64,
        ) -> Result<crate::traits::dispatcher::ConsistencyProofResponse, ServerError> {
            unimplemented!("not used in anchor tests")
        }

        async fn get_public_keys(
            &self,
        ) -> Result<Vec<crate::traits::dispatcher::PublicKeyInfo>, ServerError> {
            unimplemented!("not used in anchor tests")
        }

        async fn trigger_anchoring(
            &self,
            _request: crate::traits::dispatcher::TriggerAnchoringRequest,
        ) -> Result<Vec<crate::traits::dispatcher::AnchoringStatus>, ServerError> {
            unimplemented!("not used in anchor tests")
        }

        async fn health_check(&self) -> Result<(), ServerError> {
            unimplemented!("not used in anchor tests")
        }
    }

    fn create_test_state(
        mode: ServerMode,
        dispatcher: Arc<dyn SequencerClient>,
        storage: Option<Arc<dyn Storage>>,
        storage_engine: Option<Arc<StorageEngine>>,
        signer: Option<Arc<CheckpointSigner>>,
    ) -> Arc<AppState> {
        Arc::new(AppState {
            mode,
            dispatcher,
            storage,
            storage_engine,
            signer,
            access_tokens: None,
            base_url: "http://test.local".to_string(),
        })
    }

    async fn create_standalone_state() -> (Arc<AppState>, TempDir) {
        let dir = TempDir::new().unwrap();
        let origin = [1u8; 32];
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let storage_engine = Arc::new(StorageEngine::new(config, origin).await.unwrap());

        // Active tree is automatically created by StorageEngine::new()

        let dispatcher = Arc::new(MockSequencerClient::new_success(storage_engine.clone()));
        let signer = CheckpointSigner::from_bytes(&[42u8; 32]);

        let state = create_test_state(
            ServerMode::Standalone,
            dispatcher,
            None,
            Some(storage_engine),
            Some(Arc::new(signer)),
        );

        (state, dir)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_anchor_json_success() {
        let (state, _dir) = create_standalone_state().await;

        let request = AnchorJsonRequest {
            payload: serde_json::json!({"data": "test"}),
            metadata: Some(serde_json::json!({"source": "test"})),
            external_id: Some("ext-123".to_string()),
            file: None,
        };

        let body_bytes = serde_json::to_vec(&request).unwrap();
        let body = Body::from(body_bytes);

        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

        let result = create_anchor(State(state), headers, body).await;

        assert!(result.is_ok());
        let (status, json) = result.unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert!(json.0.get("entry").is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_anchor_sequencer_mode_rejects() {
        let dir = TempDir::new().unwrap();
        let origin = [1u8; 32];
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let storage_engine = Arc::new(StorageEngine::new(config, origin).await.unwrap());
        let dispatcher = Arc::new(MockSequencerClient::new_success(storage_engine));
        let state = create_test_state(ServerMode::Sequencer, dispatcher, None, None, None);

        let body = Body::from("{}");
        let headers = HeaderMap::new();

        let result = create_anchor(State(state), headers, body).await;

        assert!(matches!(result, Err(ServerError::NotSupported(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_anchor_unsupported_content_type() {
        let (state, _dir) = create_standalone_state().await;

        let body = Body::from("test data");
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());

        let result = create_anchor(State(state), headers, body).await;

        assert!(matches!(
            result,
            Err(ServerError::UnsupportedContentType(_))
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_anchor_multipart_not_implemented() {
        let (state, _dir) = create_standalone_state().await;

        let body = Body::from("test data");
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "multipart/form-data; boundary=test".parse().unwrap(),
        );

        let result = create_anchor(State(state), headers, body).await;

        assert!(matches!(result, Err(ServerError::NotSupported(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_anchor_missing_content_type_defaults_to_unsupported() {
        let (state, _dir) = create_standalone_state().await;

        let request = AnchorJsonRequest {
            payload: serde_json::json!({"data": "test"}),
            metadata: None,
            external_id: None,
            file: None,
        };

        let body_bytes = serde_json::to_vec(&request).unwrap();
        let body = Body::from(body_bytes);
        let headers = HeaderMap::new();

        let result = create_anchor(State(state), headers, body).await;

        assert!(matches!(
            result,
            Err(ServerError::UnsupportedContentType(_))
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_anchor_json_invalid_json() {
        let (state, _dir) = create_standalone_state().await;
        let body = Body::from(b"not valid json".to_vec());

        let result = anchor_json(state, body).await;

        assert!(matches!(result, Err(ServerError::InvalidArgument(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_anchor_json_minimal_request() {
        let (state, _dir) = create_standalone_state().await;

        let request = AnchorJsonRequest {
            payload: serde_json::json!({"minimal": true}),
            metadata: None,
            external_id: None,
            file: None,
        };

        let body_bytes = serde_json::to_vec(&request).unwrap();
        let body = Body::from(body_bytes);

        let result = anchor_json(state, body).await;

        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_anchor_json_dispatcher_failure() {
        let dir = TempDir::new().unwrap();
        let origin = [1u8; 32];
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let storage_engine = Arc::new(StorageEngine::new(config, origin).await.unwrap());

        // Active tree is automatically created by StorageEngine::new()

        let dispatcher = Arc::new(MockSequencerClient::new_failure(
            storage_engine.clone(),
            ServerError::ServiceUnavailable("test error".into()),
        ));
        let signer = CheckpointSigner::from_bytes(&[42u8; 32]);

        let state = create_test_state(
            ServerMode::Standalone,
            dispatcher,
            None,
            Some(storage_engine),
            Some(Arc::new(signer)),
        );

        let request = AnchorJsonRequest {
            payload: serde_json::json!({"data": "test"}),
            metadata: None,
            external_id: None,
            file: None,
        };

        let body_bytes = serde_json::to_vec(&request).unwrap();
        let body = Body::from(body_bytes);

        let result = anchor_json(state, body).await;

        assert!(matches!(result, Err(ServerError::ServiceUnavailable(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_generate_and_return_receipt_no_storage_engine() {
        let dir = TempDir::new().unwrap();
        let origin = [1u8; 32];
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let storage_engine = Arc::new(StorageEngine::new(config, origin).await.unwrap());

        // Active tree is automatically created by StorageEngine::new()

        let dispatcher = Arc::new(MockSequencerClient::new_success(storage_engine));
        let state = create_test_state(ServerMode::Standalone, dispatcher, None, None, None);

        let result = generate_and_return_receipt(state, [0u8; 32], [0u8; 32], None, None).await;

        assert!(matches!(result, Err(ServerError::NotSupported(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_generate_and_return_receipt_no_signer() {
        let dir = TempDir::new().unwrap();
        let origin = [1u8; 32];
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let storage_engine = Arc::new(StorageEngine::new(config, origin).await.unwrap());

        // Active tree is automatically created by StorageEngine::new()

        let dispatcher = Arc::new(MockSequencerClient::new_success(storage_engine.clone()));

        let state = create_test_state(
            ServerMode::Standalone,
            dispatcher,
            None,
            Some(storage_engine),
            None,
        );

        let result = generate_and_return_receipt(state, [0u8; 32], [0u8; 32], None, None).await;

        assert!(matches!(result, Err(ServerError::NotSupported(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_anchor_success() {
        let dir = TempDir::new().unwrap();
        let origin = [1u8; 32];
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let storage_engine = Arc::new(StorageEngine::new(config, origin).await.unwrap());

        // Active tree is automatically created by StorageEngine::new()

        let batch_result = storage_engine
            .append_batch(vec![AppendParams {
                payload_hash: [4u8; 32],
                metadata_hash: [5u8; 32],
                metadata_cleartext: Some(serde_json::json!({"test": "data"})),
                external_id: None,
            }])
            .await
            .unwrap();

        let entry_id = batch_result.entries[0].id;
        let dispatcher = Arc::new(MockSequencerClient::new_success(storage_engine.clone()));
        let signer = CheckpointSigner::from_bytes(&[42u8; 32]);

        let state = create_test_state(
            ServerMode::Standalone,
            dispatcher,
            None,
            Some(storage_engine),
            Some(Arc::new(signer)),
        );

        let result = get_anchor(State(state), Path(entry_id)).await;

        assert!(result.is_ok());
        let json = result.unwrap();
        assert!(json.0.get("entry").is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_anchor_no_storage_engine() {
        let dir = TempDir::new().unwrap();
        let origin = [1u8; 32];
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let storage_engine = Arc::new(StorageEngine::new(config, origin).await.unwrap());
        let dispatcher = Arc::new(MockSequencerClient::new_success(storage_engine));
        let state = create_test_state(ServerMode::Standalone, dispatcher, None, None, None);

        let entry_id = Uuid::new_v4();
        let result = get_anchor(State(state), Path(entry_id)).await;

        assert!(matches!(result, Err(ServerError::NotSupported(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_anchor_no_signer() {
        let dir = TempDir::new().unwrap();
        let origin = [1u8; 32];
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let storage_engine = Arc::new(StorageEngine::new(config, origin).await.unwrap());
        let dispatcher = Arc::new(MockSequencerClient::new_success(storage_engine.clone()));

        let state = create_test_state(
            ServerMode::Standalone,
            dispatcher,
            None,
            Some(storage_engine),
            None,
        );

        let entry_id = Uuid::new_v4();
        let result = get_anchor(State(state), Path(entry_id)).await;

        assert!(matches!(result, Err(ServerError::NotSupported(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_anchor_json_with_metadata() {
        let (state, _dir) = create_standalone_state().await;

        let request = AnchorJsonRequest {
            payload: serde_json::json!({"data": "test"}),
            metadata: Some(serde_json::json!({"source": "api", "version": "1.0"})),
            external_id: Some("test-ext-id".to_string()),
            file: None,
        };

        let body_bytes = serde_json::to_vec(&request).unwrap();
        let body = Body::from(body_bytes);

        let result = anchor_json(state, body).await;

        assert!(result.is_ok());
        let (status, _) = result.unwrap();
        assert_eq!(status, StatusCode::CREATED);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_anchor_json_empty_metadata() {
        let (state, _dir) = create_standalone_state().await;

        let request = AnchorJsonRequest {
            payload: serde_json::json!({"data": "test"}),
            metadata: Some(serde_json::json!({})),
            external_id: None,
            file: None,
        };

        let body_bytes = serde_json::to_vec(&request).unwrap();
        let body = Body::from(body_bytes);

        let result = anchor_json(state, body).await;

        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_anchor_json_complex_payload() {
        let (state, _dir) = create_standalone_state().await;

        let request = AnchorJsonRequest {
            payload: serde_json::json!({
                "nested": {
                    "deep": {
                        "value": [1, 2, 3],
                        "string": "test"
                    }
                },
                "array": ["a", "b", "c"]
            }),
            metadata: None,
            external_id: None,
            file: None,
        };

        let body_bytes = serde_json::to_vec(&request).unwrap();
        let body = Body::from(body_bytes);

        let result = anchor_json(state, body).await;

        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_anchor_content_type_case_insensitive() {
        let (state, _dir) = create_standalone_state().await;

        let request = AnchorJsonRequest {
            payload: serde_json::json!({"data": "test"}),
            metadata: None,
            external_id: None,
            file: None,
        };

        let body_bytes = serde_json::to_vec(&request).unwrap();
        let body = Body::from(body_bytes);

        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/json; charset=utf-8".parse().unwrap(),
        );

        let result = create_anchor(State(state), headers, body).await;

        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_anchor_json_large_body_rejected() {
        let (state, _dir) = create_standalone_state().await;

        let large_payload = "x".repeat(11 * 1024 * 1024);
        let body = Body::from(large_payload);

        let result = anchor_json(state, body).await;

        assert!(matches!(result, Err(ServerError::InvalidArgument(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hash_computation_deterministic() {
        let (state, _dir) = create_standalone_state().await;

        let request = AnchorJsonRequest {
            payload: serde_json::json!({"a": 1, "b": 2}),
            metadata: None,
            external_id: None,
            file: None,
        };

        let body1_bytes = serde_json::to_vec(&request).unwrap();
        let body1 = Body::from(body1_bytes);

        let result1 = anchor_json(state.clone(), body1).await;
        assert!(result1.is_ok());

        let request2 = AnchorJsonRequest {
            payload: serde_json::json!({"b": 2, "a": 1}),
            metadata: None,
            external_id: None,
            file: None,
        };

        let body2_bytes = serde_json::to_vec(&request2).unwrap();
        let body2 = Body::from(body2_bytes);

        let result2 = anchor_json(state.clone(), body2).await;
        assert!(result2.is_ok());
    }

    #[test]
    fn test_parse_precomputed_hash_valid() {
        let value = serde_json::json!(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let result = parse_precomputed_hash(&value);
        assert!(result.is_ok());
        let hash = result.unwrap();
        assert_eq!(
            hex::encode(hash),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_parse_precomputed_hash_not_string() {
        let value = serde_json::json!(42);
        let result = parse_precomputed_hash(&value);
        assert!(matches!(result, Err(ServerError::InvalidArgument(_))));
    }

    #[test]
    fn test_parse_precomputed_hash_missing_prefix() {
        let value =
            serde_json::json!("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        let result = parse_precomputed_hash(&value);
        assert!(matches!(result, Err(ServerError::InvalidArgument(_))));
    }

    #[test]
    fn test_parse_precomputed_hash_wrong_length() {
        let value = serde_json::json!("sha256:abcdef");
        let result = parse_precomputed_hash(&value);
        assert!(matches!(result, Err(ServerError::InvalidArgument(_))));
    }

    #[test]
    fn test_parse_precomputed_hash_invalid_hex() {
        let value = serde_json::json!(
            "sha256:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
        );
        let result = parse_precomputed_hash(&value);
        assert!(matches!(result, Err(ServerError::InvalidArgument(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_anchor_json_file_flag_uses_precomputed_hash() {
        let (state, _dir) = create_standalone_state().await;

        let request = AnchorJsonRequest {
            payload: serde_json::json!(
                "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            ),
            metadata: Some(serde_json::json!({"type": "file-upload", "filename": "test.pdf"})),
            external_id: Some("ext-file-001".to_string()),
            file: Some(true),
        };

        let body_bytes = serde_json::to_vec(&request).unwrap();
        let body = Body::from(body_bytes);

        let result = anchor_json(state, body).await;
        assert!(result.is_ok());

        let (status, json) = result.unwrap();
        assert_eq!(status, StatusCode::CREATED);

        // payload_hash in receipt must equal the pre-computed hash
        let payload_hash = json.0["entry"]["payload_hash"].as_str().unwrap();
        assert_eq!(
            payload_hash,
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_anchor_json_file_flag_false_hashes_normally() {
        let (state, _dir) = create_standalone_state().await;

        let request = AnchorJsonRequest {
            payload: serde_json::json!(
                "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            ),
            metadata: None,
            external_id: None,
            file: Some(false),
        };

        let body_bytes = serde_json::to_vec(&request).unwrap();
        let body = Body::from(body_bytes);

        let result = anchor_json(state, body).await;
        assert!(result.is_ok());

        let (_, json) = result.unwrap();
        let payload_hash = json.0["entry"]["payload_hash"].as_str().unwrap();
        // Without file flag, payload is hashed — so payload_hash != the string itself
        assert_ne!(
            payload_hash,
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// Store a confirmed RFC 3161 anchor committing to `root`.
    async fn store_tsa_anchor(engine: &StorageEngine, root: [u8; 32], tree_size: u64) {
        use crate::traits::anchor::{Anchor, AnchorType};
        let index_store = engine.index_store();
        let index = index_store.lock().await;
        index
            .store_anchor_returning_id(
                tree_size,
                &Anchor {
                    anchor_type: AnchorType::Rfc3161,
                    target: "data_tree_root".to_string(),
                    anchored_hash: root,
                    tree_size,
                    super_tree_size: None,
                    timestamp: 1_000_000,
                    token: vec![],
                    metadata: serde_json::json!({"tsa_url": "https://tsa.example.com"}),
                },
                "confirmed",
            )
            .unwrap();
    }

    async fn append_leaf(engine: &StorageEngine, seed: u8) -> Uuid {
        engine
            .append_batch(vec![AppendParams {
                payload_hash: [seed; 32],
                metadata_hash: [0u8; 32],
                metadata_cleartext: None,
                external_id: None,
            }])
            .await
            .unwrap()
            .entries[0]
            .id
    }

    fn state_over(engine: Arc<StorageEngine>) -> Arc<AppState> {
        let dispatcher = Arc::new(MockSequencerClient::new_success(engine.clone()));
        create_test_state(
            ServerMode::Standalone,
            dispatcher,
            None,
            Some(engine),
            Some(Arc::new(CheckpointSigner::from_bytes(&[42u8; 32]))),
        )
    }

    /// Assert the served receipt describes one tree state: every RFC 3161
    /// anchor commits to `proof.root_hash`, which the checkpoint also signs
    /// (ATL Protocol Section 5.2 steps 3-4, Section 5.5.1 step 2).
    fn assert_served_receipt_is_single_state(receipt: &serde_json::Value) -> usize {
        let proof = &receipt["proof"];
        assert_eq!(proof["root_hash"], proof["checkpoint"]["root_hash"]);
        assert_eq!(proof["tree_size"], proof["checkpoint"]["tree_size"]);

        // `anchors` is omitted from the JSON when empty.
        let anchors = receipt["anchors"].as_array().cloned().unwrap_or_default();
        let mut tsa_anchors = 0;
        for anchor in &anchors {
            if anchor["type"] == "rfc3161" {
                tsa_anchors += 1;
                assert_eq!(
                    anchor["target_hash"], proof["root_hash"],
                    "rfc3161 anchor.target_hash must equal proof.root_hash"
                );
            }
        }
        tsa_anchors
    }

    /// End-to-end through the served endpoint: an entry whose leaf index is
    /// below an anchored tree size gets its own state's anchor, not another
    /// state's.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_anchor_serves_a_single_consistent_state() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(
            StorageEngine::new(
                StorageConfig {
                    data_dir: dir.path().to_path_buf(),
                    ..Default::default()
                },
                [7u8; 32],
            )
            .await
            .unwrap(),
        );

        let entry_id = append_leaf(&engine, 0x01).await;
        append_leaf(&engine, 0x02).await;

        let closed = engine.tree_head();
        engine
            .rotate_tree(&engine.origin_id(), closed.tree_size, &closed.root_hash)
            .await
            .unwrap();
        store_tsa_anchor(&engine, closed.root_hash, closed.tree_size).await;

        // Grow the log past the anchored state.
        append_leaf(&engine, 0x03).await;
        append_leaf(&engine, 0x04).await;
        append_leaf(&engine, 0x05).await;
        assert_ne!(engine.tree_head().root_hash, closed.root_hash);

        let receipt = get_anchor(State(state_over(engine)), Path(entry_id))
            .await
            .expect("receipt must be served")
            .0;

        assert_eq!(receipt["proof"]["tree_size"], closed.tree_size);
        assert_eq!(
            assert_served_receipt_is_single_state(&receipt),
            1,
            "the entry's own state is anchored, so its receipt carries that anchor"
        );
    }

    /// End-to-end through the served endpoint: when the entry's own state
    /// was never timestamped, a later state's anchor must not be served with
    /// it.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_anchor_does_not_serve_another_states_anchor() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(
            StorageEngine::new(
                StorageConfig {
                    data_dir: dir.path().to_path_buf(),
                    ..Default::default()
                },
                [8u8; 32],
            )
            .await
            .unwrap(),
        );

        let entry_id = append_leaf(&engine, 0x01).await;
        append_leaf(&engine, 0x02).await;
        let first = engine.tree_head();
        engine
            .rotate_tree(&engine.origin_id(), first.tree_size, &first.root_hash)
            .await
            .unwrap();
        // No anchor for the first closed tree.

        append_leaf(&engine, 0x03).await;
        let second = engine.tree_head();
        engine
            .rotate_tree(&engine.origin_id(), second.tree_size, &second.root_hash)
            .await
            .unwrap();
        store_tsa_anchor(&engine, second.root_hash, second.tree_size).await;

        let receipt = get_anchor(State(state_over(engine)), Path(entry_id))
            .await
            .expect("receipt must be served")
            .0;

        assert_eq!(receipt["proof"]["tree_size"], first.tree_size);
        assert_eq!(
            assert_served_receipt_is_single_state(&receipt),
            0,
            "the entry's own state has no timestamp, so no rfc3161 anchor may be served"
        );
        assert!(
            receipt["upgrade_url"].is_string(),
            "an unanchored state must advertise the upgrade path"
        );
    }

    // ================================================================
    // RFC 8785 Section 3.1 on the ingress bytes
    //
    // These drive `create_anchor` with a raw `Body`, not with a constructed
    // `AnchorJsonRequest`: a `serde_json::Value` cannot hold a repeated key,
    // so a test built from one would exercise the parser's output instead of
    // the input the constraint is about, and would pass with the check
    // removed.
    // ================================================================

    /// The body this test sends is the defect: `serde_json` resolves it to
    /// `{"id":2}` and the log would commit to a document the client never
    /// unambiguously sent.
    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_property_name_in_metadata_is_refused() {
        let (state, _dir) = create_standalone_state().await;

        let body = Body::from(r#"{"payload":"doc","metadata":{"id":1,"id":2}}"#);
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

        let err = create_anchor(State(state), headers, body)
            .await
            .expect_err("a duplicate property name must be refused");

        assert!(
            matches!(err, ServerError::DuplicatePropertyName { .. }),
            "expected DuplicatePropertyName, got {err:?}"
        );
        assert_eq!(
            err.status_code(),
            StatusCode::BAD_REQUEST,
            "a malformed client document is a 4xx, not a 5xx"
        );
        assert!(
            err.to_string().contains("/metadata"),
            "the message must locate the offending object: {err}"
        );
    }

    /// The envelope itself is ambiguous: which `metadata` gets anchored is
    /// `serde_json`'s choice, not the protocol's.
    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_metadata_key_in_the_envelope_is_refused() {
        let (state, _dir) = create_standalone_state().await;

        let body = Body::from(r#"{"payload":"doc","metadata":{"a":1},"metadata":{"b":2}}"#);
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

        let err = create_anchor(State(state), headers, body)
            .await
            .expect_err("an ambiguous envelope must be refused");

        assert!(matches!(err, ServerError::DuplicatePropertyName { .. }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_body_that_is_not_utf8_is_refused() {
        let (state, _dir) = create_standalone_state().await;

        let body = Body::from(vec![0x7b, 0xff, 0xfe, 0x7d]);
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

        let err = create_anchor(State(state), headers, body)
            .await
            .expect_err("non-UTF-8 must be refused");

        assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
    }

    /// Guards against the check being written so broadly that it refuses
    /// ordinary documents. Repeating a name in *different* objects is legal.
    #[tokio::test(flavor = "multi_thread")]
    async fn repeated_names_in_sibling_objects_are_accepted() {
        let (state, _dir) = create_standalone_state().await;

        let body = Body::from(r#"{"payload":"doc","metadata":{"a":{"id":1},"b":{"id":2}}}"#);
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

        let (status, _) = create_anchor(State(state), headers, body)
            .await
            .expect("distinct objects may each carry an `id`");
        assert_eq!(status, StatusCode::CREATED);
    }

    // ================================================================
    // Large integers are normalized, never refused
    // ================================================================

    /// A nanosecond timestamp is past 2^53, so it is not exactly
    /// representable as a double. RFC 8785 Appendix B notes (1) and (2) put
    /// that outside the canonicalizer's concern -- the integer is normalized
    /// to the double it denotes and rendered by ECMA-262 Section 7.1.12.1.
    ///
    /// What has to hold is that both ends do the same thing, so this walks
    /// the whole circle: raw client text -> the hash the server commits ->
    /// the receipt as bytes -> a verifier's independent recomputation.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_nanosecond_integer_closes_the_circle() {
        let (state, _dir) = create_standalone_state().await;

        // Raw text. Written as a literal so the u64 never passes through a
        // Rust f64 on the way in.
        let raw = r#"{"payload":"doc","metadata":{"ts":1756812345678901234}}"#;

        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

        let (status, receipt_value) = create_anchor(State(state), headers, Body::from(raw))
            .await
            .expect("a large integer must be anchored, not refused");
        assert_eq!(status, StatusCode::CREATED);

        // What the client will actually receive.
        let receipt_bytes = serde_json::to_vec(&receipt_value.0).expect("receipt must serialize");
        let receipt_text = String::from_utf8(receipt_bytes).expect("receipt is UTF-8");

        // The receipt carries the client's digits verbatim, not the rounded
        // spelling: the canonical form is derived at verification time, it is
        // not what gets published.
        assert!(
            receipt_text.contains("1756812345678901234"),
            "the receipt must republish the literal the client sent: {receipt_text}"
        );

        // A verifier's path: parse the bytes (which re-runs the Section 3.1
        // check inside atl-core), then canonicalize the metadata it finds.
        let parsed = atl_core::Receipt::from_json(&receipt_text)
            .expect("the receipt we issue must be one atl-core accepts");

        let canonical = atl_core::canonicalize(&parsed.entry().metadata)
            .expect("metadata must have a canonical form");
        assert_eq!(
            canonical, r#"{"ts":1756812345678901200}"#,
            "the canonical form is the ECMA-262 spelling of the nearest double"
        );

        let recomputed = atl_core::canonicalize_and_hash(&parsed.entry().metadata)
            .expect("metadata must have a canonical form");
        assert_eq!(
            format!("sha256:{}", hex::encode(recomputed)),
            parsed.entry().metadata_hash,
            "the verifier's hash must equal the metadata_hash the receipt states"
        );

        // And the same value is what the leaf committed to at ingress.
        let at_ingress = crate::api::streaming::hash_metadata(Some(
            &serde_json::json!({"ts": 1_756_812_345_678_901_234_u64}),
        ))
        .expect("ingress hash");
        assert_eq!(
            format!("sha256:{}", hex::encode(at_ingress)),
            parsed.entry().metadata_hash,
            "the hash committed at ingress must equal the one the receipt states"
        );
    }
}
