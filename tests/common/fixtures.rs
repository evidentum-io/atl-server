//! Test fixtures and app setup utilities

use async_trait::async_trait;
use atl_server::api::{AppState, create_router};
use atl_server::config::ServerMode;
use atl_server::traits::dispatcher::SequencerClient;
use atl_server::traits::storage::{AppendParams, Storage, TreeHead};
use atl_server::{ServerResult, SqliteStore};
use axum::Router;
use std::sync::{Arc, Mutex};

/// Create a test app with in-memory storage and no authentication
pub async fn test_app() -> Router {
    let mut storage = SqliteStore::in_memory().expect("Failed to create in-memory storage");
    // Call inherent method (not trait method) - must be called before Arc::new
    SqliteStore::initialize(&mut storage).expect("Failed to initialize storage");
    let storage = Arc::new(storage);

    let dispatcher = Arc::new(LocalDispatcher::new(storage.clone() as Arc<dyn Storage>));

    let state = Arc::new(AppState {
        mode: ServerMode::Standalone,
        dispatcher: dispatcher as Arc<dyn SequencerClient>,
        storage: Some(storage as Arc<dyn Storage>),
        access_tokens: None,
        base_url: "http://localhost:3000".to_string(),
    });

    create_router(state)
}

/// Create a test app with authentication enabled
pub async fn test_app_with_auth(tokens: Vec<String>) -> Router {
    let mut storage = SqliteStore::in_memory().expect("Failed to create in-memory storage");
    // Call inherent method (not trait method) - must be called before Arc::new
    SqliteStore::initialize(&mut storage).expect("Failed to initialize storage");
    let storage = Arc::new(storage);

    let dispatcher = Arc::new(LocalDispatcher::new(storage.clone() as Arc<dyn Storage>));

    let state = Arc::new(AppState {
        mode: ServerMode::Standalone,
        dispatcher: dispatcher as Arc<dyn SequencerClient>,
        storage: Some(storage as Arc<dyn Storage>),
        access_tokens: Some(tokens),
        base_url: "http://localhost:3000".to_string(),
    });

    create_router(state)
}

/// Local dispatcher for testing (wraps storage directly)
pub struct LocalDispatcher {
    storage: Arc<dyn Storage>,
    signer: atl_server::receipt::CheckpointSigner,
    last_checkpoint: Mutex<Option<atl_core::Checkpoint>>,
}

impl LocalDispatcher {
    /// Create new local dispatcher with the given storage
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            signer: atl_server::receipt::CheckpointSigner::from_bytes(&[1u8; 32]),
            last_checkpoint: Mutex::new(None),
        }
    }
}

#[async_trait]
impl SequencerClient for LocalDispatcher {
    async fn dispatch(
        &self,
        params: AppendParams,
    ) -> ServerResult<atl_server::traits::dispatcher::DispatchResult> {
        let result = self.storage.append(params)?;
        let tree_head = self.storage.get_tree_head()?;

        let checkpoint = atl_core::Checkpoint {
            origin: tree_head.origin,
            tree_size: tree_head.tree_size,
            timestamp: chrono::Utc::now().timestamp() as u64,
            root_hash: tree_head.root_hash,
            signature: [0u8; 64], // Dummy signature for tests
            key_id: *self.signer.key_id(),
        };

        *self.last_checkpoint.lock().unwrap() = Some(checkpoint.clone());

        Ok(atl_server::traits::dispatcher::DispatchResult { result, checkpoint })
    }

    async fn get_receipt(
        &self,
        request: atl_server::traits::dispatcher::GetReceiptRequest,
    ) -> ServerResult<atl_server::traits::dispatcher::ReceiptResponse> {
        let entry_id = request.entry_id;
        use atl_server::traits::dispatcher::ReceiptResponse;

        let entry = self.storage.get_entry(&entry_id)?;
        let proof = self.storage.get_inclusion_proof(&entry_id, None)?;
        let tree_head = self.storage.get_tree_head()?;

        let checkpoint = atl_core::Checkpoint {
            origin: tree_head.origin,
            tree_size: tree_head.tree_size,
            timestamp: chrono::Utc::now().timestamp() as u64,
            root_hash: tree_head.root_hash,
            signature: [0u8; 64], // Dummy signature for tests
            key_id: *self.signer.key_id(),
        };

        Ok(ReceiptResponse {
            entry,
            inclusion_proof: proof.path,
            checkpoint,
            consistency_proof: None,
            anchors: vec![],
        })
    }

    async fn get_tree_head(&self) -> ServerResult<TreeHead> {
        self.storage.get_tree_head()
    }

    async fn get_public_keys(
        &self,
    ) -> ServerResult<Vec<atl_server::traits::dispatcher::PublicKeyInfo>> {
        Ok(vec![atl_server::traits::dispatcher::PublicKeyInfo {
            key_id: *self.signer.key_id(),
            public_key: self.signer.public_key_bytes(),
            algorithm: "Ed25519".to_string(),
            created_at: chrono::Utc::now().timestamp() as u64,
        }])
    }

    async fn dispatch_batch(
        &self,
        params: Vec<AppendParams>,
    ) -> ServerResult<atl_server::traits::dispatcher::BatchDispatchResult> {
        let results = self.storage.append_batch(params)?;
        let tree_head = self.storage.get_tree_head()?;
        let checkpoint = atl_core::Checkpoint {
            origin: tree_head.origin,
            tree_size: tree_head.tree_size,
            timestamp: chrono::Utc::now().timestamp() as u64,
            root_hash: tree_head.root_hash,
            signature: [0u8; 64],
            key_id: *self.signer.key_id(),
        };
        Ok(atl_server::traits::dispatcher::BatchDispatchResult {
            results,
            checkpoint,
        })
    }

    async fn get_consistency_proof(
        &self,
        from_size: u64,
        to_size: u64,
    ) -> ServerResult<atl_server::traits::dispatcher::ConsistencyProofResponse> {
        let proof = self.storage.get_consistency_proof(from_size, to_size)?;
        let tree_head = self.storage.get_tree_head()?;
        Ok(atl_server::traits::dispatcher::ConsistencyProofResponse {
            from_size: proof.from_size,
            to_size: proof.to_size,
            path: proof.path,
            from_root: [0u8; 32], // Stub
            to_root: tree_head.root_hash,
        })
    }

    async fn trigger_anchoring(
        &self,
        _request: atl_server::traits::dispatcher::TriggerAnchoringRequest,
    ) -> ServerResult<Vec<atl_server::traits::dispatcher::AnchoringStatus>> {
        Ok(vec![])
    }

    async fn health_check(&self) -> ServerResult<()> {
        Ok(())
    }
}
