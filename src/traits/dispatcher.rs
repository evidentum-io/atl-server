//! Sequencer client trait and dispatcher implementations

use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;

use crate::error::ServerResult;
use crate::traits::{
    anchor::Anchor, storage::AppendParams, storage::AppendResult, storage::Entry, storage::TreeHead,
};

/// Response from Sequencer containing entry result and signed checkpoint
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DispatchResult {
    /// The append result (id, leaf_index, proof, etc.)
    pub result: AppendResult,

    /// Signed checkpoint from Sequencer (includes signature)
    pub checkpoint: atl_core::Checkpoint,
}

/// Response for batch dispatch
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BatchDispatchResult {
    /// Results for each entry (same order as input)
    pub results: Vec<AppendResult>,

    /// Single signed checkpoint covering the entire batch
    pub checkpoint: atl_core::Checkpoint,
}

/// Request to get a receipt by entry ID
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct GetReceiptRequest {
    /// Entry ID to retrieve
    pub entry_id: Uuid,

    /// Include anchors in response
    pub include_anchors: bool,
}

/// Receipt response from Sequencer
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ReceiptResponse {
    /// The entry
    pub entry: Entry,

    /// Inclusion proof path
    pub inclusion_proof: Vec<[u8; 32]>,

    /// Signed checkpoint
    pub checkpoint: atl_core::Checkpoint,

    /// Consistency proof for split-view protection
    pub consistency_proof: Option<ConsistencyProofInfo>,

    /// Anchors for the tree size
    pub anchors: Vec<Anchor>,
}

/// Consistency proof info for split-view protection
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ConsistencyProofInfo {
    /// From tree size
    pub from_tree_size: u64,

    /// Consistency proof path
    pub path: Vec<[u8; 32]>,
}

/// Consistency proof response from dispatcher
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ConsistencyProofResponse {
    /// From tree size
    pub from_size: u64,

    /// To tree size
    pub to_size: u64,

    /// Consistency proof path
    pub path: Vec<[u8; 32]>,

    /// Root hash at from_size
    pub from_root: [u8; 32],

    /// Root hash at to_size
    pub to_root: [u8; 32],
}

/// Public key info for verification
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PublicKeyInfo {
    /// Key ID (hash of public key)
    pub key_id: [u8; 32],

    /// Public key bytes (Ed25519)
    pub public_key: [u8; 32],

    /// Algorithm identifier
    pub algorithm: String,

    /// When the key was created (Unix epoch seconds)
    pub created_at: u64,
}

/// Anchoring trigger request
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TriggerAnchoringRequest {
    /// Anchor types to trigger
    pub anchor_types: Vec<String>,
}

/// Anchoring trigger response
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AnchoringStatus {
    /// Anchor type
    pub anchor_type: String,

    /// Status: "completed" or "pending"
    pub status: String,

    /// Timestamp (Unix epoch nanoseconds)
    pub timestamp: Option<u64>,

    /// Estimated seconds until finality
    pub estimated_finality_secs: Option<u64>,
}

/// Sequencer client for dispatching entries
///
/// Implementations:
/// - `LocalDispatcher`: Direct dispatch to local SequencerCore (STANDALONE/SEQUENCER)
/// - `GrpcDispatcher`: Remote dispatch via gRPC (NODE)
#[allow(dead_code)]
#[async_trait]
pub trait SequencerClient: Send + Sync {
    // ========== Entry Operations ==========

    /// Dispatch a single entry to the Sequencer
    ///
    /// Returns the append result with signed checkpoint.
    async fn dispatch(&self, params: AppendParams) -> ServerResult<DispatchResult>;

    /// Dispatch a batch of entries to the Sequencer
    ///
    /// Returns results for all entries with a single signed checkpoint.
    async fn dispatch_batch(&self, params: Vec<AppendParams>) -> ServerResult<BatchDispatchResult>;

    // ========== Query Operations ==========

    /// Get receipt for an entry by ID
    async fn get_receipt(&self, request: GetReceiptRequest) -> ServerResult<ReceiptResponse>;

    /// Get current tree head
    async fn get_tree_head(&self) -> ServerResult<TreeHead>;

    /// Get consistency proof between two tree sizes
    async fn get_consistency_proof(
        &self,
        from_size: u64,
        to_size: u64,
    ) -> ServerResult<ConsistencyProofResponse>;

    /// Get public keys for verification
    async fn get_public_keys(&self) -> ServerResult<Vec<PublicKeyInfo>>;

    // ========== Anchoring Operations ==========

    /// Trigger external anchoring
    async fn trigger_anchoring(
        &self,
        request: TriggerAnchoringRequest,
    ) -> ServerResult<Vec<AnchoringStatus>>;

    // ========== Health ==========

    /// Check if the dispatcher is healthy/connected
    async fn health_check(&self) -> ServerResult<()>;
}

/// Local dispatcher for STANDALONE and SEQUENCER modes
///
/// Sends entries directly to the local SequencerCore via mpsc channel.
pub struct LocalDispatcher {
    /// Handle to the sequencer for sending append requests
    handle: crate::sequencer::SequencerHandle,
    /// Checkpoint signer for Ed25519 signatures
    signer: crate::receipt::CheckpointSigner,
    /// Storage backend for read operations
    #[allow(dead_code)]
    storage: Arc<dyn crate::traits::Storage>,
}

impl LocalDispatcher {
    /// Create a new local dispatcher with sequencer handle, signer, and storage
    ///
    /// # Arguments
    /// * `handle` - Sequencer handle for append operations
    /// * `signer` - Checkpoint signing key
    /// * `storage` - Storage backend for read operations
    pub fn new(
        handle: crate::sequencer::SequencerHandle,
        signer: crate::receipt::CheckpointSigner,
        storage: Arc<dyn crate::traits::Storage>,
    ) -> Self {
        Self {
            handle,
            signer,
            storage,
        }
    }
}

#[async_trait]
impl SequencerClient for LocalDispatcher {
    async fn dispatch(&self, params: AppendParams) -> ServerResult<DispatchResult> {
        // Send append request through the sequencer handle
        let result = self.handle.append(params).await?;

        // Create and sign checkpoint
        let checkpoint = self.signer.sign_checkpoint_struct(
            result.tree_head.origin,
            result.tree_head.tree_size,
            &result.tree_head.root_hash,
        );

        Ok(DispatchResult { result, checkpoint })
    }

    async fn dispatch_batch(&self, params: Vec<AppendParams>) -> ServerResult<BatchDispatchResult> {
        // Send batch through sequencer handle (sends requests individually through channel)
        let results = self.handle.append_batch(params).await?;

        // Get tree head after batch
        let tree_head = self.storage.tree_head();

        // Create and sign checkpoint
        let checkpoint = self.signer.sign_checkpoint_struct(
            tree_head.origin,
            tree_head.tree_size,
            &tree_head.root_hash,
        );

        Ok(BatchDispatchResult {
            results,
            checkpoint,
        })
    }

    async fn get_receipt(&self, request: GetReceiptRequest) -> ServerResult<ReceiptResponse> {
        let entry_id = request.entry_id;

        // 1. Get entry to find its leaf_index
        let entry = self.storage.get_entry(&entry_id)?;

        let leaf_index = entry.leaf_index.ok_or_else(|| {
            crate::error::ServerError::Internal(format!("Entry {} has no leaf_index", entry_id))
        })?;

        // 2. Get covering TSA anchor
        //    OTS anchor is not queried here: data_tree_index is not available
        //    in this context (LocalDispatcher does not have super_proof info).
        let entry_tree_size = leaf_index + 1;
        let anchors = if request.include_anchors {
            let mut result = Vec::new();
            if let Some(tsa) = self.storage.get_tsa_anchor_covering(entry_tree_size)? {
                result.push(tsa);
            }
            result
        } else {
            vec![]
        };

        // 3. Determine target tree_size for receipt
        //    - If TSA anchor exists: use anchor's tree_size
        //    - If no anchors: use current tree_size (fallback)
        let (target_tree_size, target_root_hash) = if let Some(anchor) = anchors.first() {
            // Build receipt at anchor's tree_size
            let root = self.storage.get_root_at_size(anchor.tree_size)?;

            // ATL Protocol Section 5.5.1 step 2 requires the anchor to commit
            // to the very root the rest of the receipt is built against. The
            // anchor's own `tree_size` column decides which root that is, so
            // the anchor is not trusted on that column alone: a row whose
            // `tree_size` and `anchored_hash` disagree would otherwise yield a
            // receipt that cannot verify. Refuse instead of issuing one.
            if anchor.anchored_hash != root {
                return Err(crate::error::ServerError::ReceiptStateMismatch {
                    tree_size: anchor.tree_size,
                    source_name: "rfc3161 anchor",
                    found: hex::encode(anchor.anchored_hash),
                    expected: hex::encode(root),
                });
            }

            (anchor.tree_size, root)
        } else {
            // No anchors - use current tree state
            let tree_head = self.storage.tree_head();
            (tree_head.tree_size, tree_head.root_hash)
        };

        // 4. Generate inclusion proof at target tree_size
        let proof = self
            .storage
            .get_inclusion_proof(&entry_id, Some(target_tree_size))?;

        // 5. Sign checkpoint at target tree_size with target root
        let origin = self.storage.origin_id();
        let checkpoint =
            self.signer
                .sign_checkpoint_struct(origin, target_tree_size, &target_root_hash);

        // 6. No consistency proof needed (receipt tree_size == anchor tree_size)
        Ok(ReceiptResponse {
            entry,
            inclusion_proof: proof.path,
            checkpoint,
            consistency_proof: None,
            anchors,
        })
    }

    async fn get_tree_head(&self) -> ServerResult<TreeHead> {
        Ok(self.storage.tree_head())
    }

    async fn get_consistency_proof(
        &self,
        from_size: u64,
        to_size: u64,
    ) -> ServerResult<ConsistencyProofResponse> {
        let proof = self.storage.get_consistency_proof(from_size, to_size)?;
        let tree_head = self.storage.tree_head();

        // Get root hash at from_size (requires storage query or recomputation)
        // For now, use zero bytes - proper historical root lookup is a separate feature
        let from_root = [0u8; 32];

        Ok(ConsistencyProofResponse {
            from_size: proof.from_size,
            to_size: proof.to_size,
            path: proof.path,
            from_root,
            to_root: tree_head.root_hash,
        })
    }

    async fn get_public_keys(&self) -> ServerResult<Vec<PublicKeyInfo>> {
        Ok(vec![PublicKeyInfo {
            key_id: *self.signer.key_id(),
            public_key: self.signer.public_key_bytes(),
            algorithm: "Ed25519".to_string(),
            created_at: 0, // TODO: track key creation time if needed
        }])
    }

    async fn trigger_anchoring(
        &self,
        _request: TriggerAnchoringRequest,
    ) -> ServerResult<Vec<AnchoringStatus>> {
        // Anchoring is handled by background tasks, not dispatcher
        // Return empty array to indicate no immediate anchoring occurred
        Ok(vec![])
    }

    async fn health_check(&self) -> ServerResult<()> {
        // Check if sequencer handle has capacity
        if self.handle.has_capacity() {
            Ok(())
        } else {
            Err(crate::error::ServerError::ServiceUnavailable(
                "sequencer buffer full".into(),
            ))
        }
    }
}

/// gRPC dispatcher for NODE mode
///
/// Sends entries to remote Sequencer via gRPC.
/// Maintains persistent connection with keep-alive.
/// Implementation will be added in GRPC-1.
#[allow(dead_code)]
pub struct GrpcDispatcher {
    // Implementation details will be added in GRPC-1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Storage;

    #[test]
    fn test_dispatch_result_creation() {
        let result = crate::traits::AppendResult {
            id: uuid::Uuid::new_v4(),
            leaf_index: 0,
            tree_head: TreeHead {
                tree_size: 1,
                root_hash: [1u8; 32],
                origin: [2u8; 32],
            },
            inclusion_proof: vec![],
            timestamp: chrono::Utc::now(),
        };

        let checkpoint = atl_core::Checkpoint {
            origin: [2u8; 32],
            tree_size: 1,
            timestamp: 1234567890,
            root_hash: [1u8; 32],
            key_id: [3u8; 32],
            signature: [0u8; 64],
        };

        let dispatch_result = DispatchResult {
            result: result.clone(),
            checkpoint: checkpoint.clone(),
        };

        assert_eq!(dispatch_result.result.leaf_index, 0);
        assert_eq!(dispatch_result.checkpoint.tree_size, 1);
    }

    #[test]
    fn test_batch_dispatch_result_creation() {
        let results = vec![crate::traits::AppendResult {
            id: uuid::Uuid::new_v4(),
            leaf_index: 0,
            tree_head: TreeHead {
                tree_size: 1,
                root_hash: [1u8; 32],
                origin: [2u8; 32],
            },
            inclusion_proof: vec![],
            timestamp: chrono::Utc::now(),
        }];

        let checkpoint = atl_core::Checkpoint {
            origin: [2u8; 32],
            tree_size: 1,
            timestamp: 1234567890,
            root_hash: [1u8; 32],
            key_id: [3u8; 32],
            signature: [0u8; 64],
        };

        let batch_result = BatchDispatchResult {
            results,
            checkpoint: checkpoint.clone(),
        };

        assert_eq!(batch_result.results.len(), 1);
        assert_eq!(batch_result.checkpoint.tree_size, 1);
    }

    #[test]
    fn test_get_receipt_request_creation() {
        let entry_id = uuid::Uuid::new_v4();
        let request = GetReceiptRequest {
            entry_id,
            include_anchors: true,
        };

        assert_eq!(request.entry_id, entry_id);
        assert!(request.include_anchors);
    }

    #[test]
    fn test_consistency_proof_info_creation() {
        let info = ConsistencyProofInfo {
            from_tree_size: 10,
            path: vec![[1u8; 32], [2u8; 32]],
        };

        assert_eq!(info.from_tree_size, 10);
        assert_eq!(info.path.len(), 2);
    }

    #[test]
    fn test_consistency_proof_response_creation() {
        let response = ConsistencyProofResponse {
            from_size: 10,
            to_size: 20,
            path: vec![[1u8; 32]],
            from_root: [3u8; 32],
            to_root: [4u8; 32],
        };

        assert_eq!(response.from_size, 10);
        assert_eq!(response.to_size, 20);
        assert_eq!(response.path.len(), 1);
    }

    #[test]
    fn test_public_key_info_creation() {
        let key_info = PublicKeyInfo {
            key_id: [1u8; 32],
            public_key: [2u8; 32],
            algorithm: "Ed25519".to_string(),
            created_at: 1234567890,
        };

        assert_eq!(key_info.algorithm, "Ed25519");
        assert_eq!(key_info.created_at, 1234567890);
    }

    #[test]
    fn test_trigger_anchoring_request_creation() {
        let request = TriggerAnchoringRequest {
            anchor_types: vec!["ots".to_string(), "tsa".to_string()],
        };

        assert_eq!(request.anchor_types.len(), 2);
        assert!(request.anchor_types.contains(&"ots".to_string()));
    }

    #[test]
    fn test_anchoring_status_creation() {
        let status = AnchoringStatus {
            anchor_type: "ots".to_string(),
            status: "completed".to_string(),
            timestamp: Some(1234567890),
            estimated_finality_secs: Some(600),
        };

        assert_eq!(status.anchor_type, "ots");
        assert_eq!(status.status, "completed");
        assert_eq!(status.timestamp, Some(1234567890));
        assert_eq!(status.estimated_finality_secs, Some(600));
    }

    #[test]
    fn test_receipt_response_creation() {
        let entry = Entry {
            id: uuid::Uuid::new_v4(),
            leaf_index: Some(0),
            payload_hash: [0u8; 32],
            metadata_hash: [1u8; 32],
            metadata_cleartext: None,
            external_id: None,
            created_at: chrono::Utc::now(),
        };

        let checkpoint = atl_core::Checkpoint {
            origin: [2u8; 32],
            tree_size: 1,
            timestamp: 1234567890,
            root_hash: [1u8; 32],
            key_id: [3u8; 32],
            signature: [0u8; 64],
        };

        let response = ReceiptResponse {
            entry,
            inclusion_proof: vec![[4u8; 32]],
            checkpoint: checkpoint.clone(),
            consistency_proof: None,
            anchors: vec![],
        };

        assert_eq!(response.inclusion_proof.len(), 1);
        assert!(response.consistency_proof.is_none());
        assert_eq!(response.anchors.len(), 0);
    }

    /// Build a `LocalDispatcher` over a real storage engine.
    ///
    /// `get_receipt` never touches the sequencer handle, so the sequencer is
    /// created but not run.
    async fn make_local_dispatcher(
        origin: [u8; 32],
    ) -> (
        LocalDispatcher,
        std::sync::Arc<crate::storage::engine::StorageEngine>,
        tempfile::TempDir,
    ) {
        use crate::sequencer::{Sequencer, SequencerConfig};
        use crate::storage::config::StorageConfig;
        use crate::storage::engine::StorageEngine;

        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(
            StorageEngine::new(
                StorageConfig {
                    data_dir: dir.path().to_path_buf(),
                    ..Default::default()
                },
                origin,
            )
            .await
            .unwrap(),
        );

        let storage: Arc<dyn Storage> = engine.clone();
        let (_sequencer, handle) = Sequencer::new(storage.clone(), SequencerConfig::default());
        let signer = crate::receipt::CheckpointSigner::from_bytes(&origin);

        (LocalDispatcher::new(handle, signer, storage), engine, dir)
    }

    async fn append_leaf(engine: &crate::storage::engine::StorageEngine, seed: u8) -> uuid::Uuid {
        let batch = engine
            .append_batch(vec![AppendParams {
                payload_hash: [seed; 32],
                metadata_hash: [0u8; 32],
                metadata_cleartext: None,
                external_id: None,
            }])
            .await
            .unwrap();
        batch.entries[0].id
    }

    async fn store_tsa_anchor(
        engine: &crate::storage::engine::StorageEngine,
        anchored_hash: [u8; 32],
        tree_size: u64,
    ) {
        use crate::traits::anchor::{Anchor, AnchorType};
        let index_store = engine.index_store();
        let index = index_store.lock().await;
        index
            .store_anchor_returning_id(
                tree_size,
                &Anchor {
                    anchor_type: AnchorType::Rfc3161,
                    target: "data_tree_root".to_string(),
                    anchored_hash,
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

    /// The receipt is built at the anchor's tree size, so the anchor must
    /// commit to the root at that size (ATL Protocol Section 5.5.1 step 2).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_local_get_receipt_binds_checkpoint_to_the_anchored_root() {
        let (dispatcher, engine, _dir) = make_local_dispatcher([40u8; 32]).await;

        let entry_id = append_leaf(&engine, 0x01).await;
        append_leaf(&engine, 0x02).await;
        let head = engine.tree_head();
        store_tsa_anchor(&engine, head.root_hash, head.tree_size).await;

        let response = dispatcher
            .get_receipt(GetReceiptRequest {
                entry_id,
                include_anchors: true,
            })
            .await
            .expect("receipt generation must succeed");

        assert_eq!(response.anchors.len(), 1);
        assert_eq!(response.checkpoint.tree_size, head.tree_size);
        assert_eq!(response.checkpoint.root_hash, head.root_hash);
        assert_eq!(
            response.anchors[0].anchored_hash,
            response.checkpoint.root_hash
        );
    }

    /// An anchor row whose `tree_size` and `anchored_hash` name different
    /// states must be refused, not turned into an unverifiable receipt.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_local_get_receipt_refuses_anchor_that_does_not_match_root() {
        let (dispatcher, engine, _dir) = make_local_dispatcher([41u8; 32]).await;

        let entry_id = append_leaf(&engine, 0x01).await;
        append_leaf(&engine, 0x02).await;
        let head = engine.tree_head();
        store_tsa_anchor(&engine, [0x66u8; 32], head.tree_size).await;

        let err = dispatcher
            .get_receipt(GetReceiptRequest {
                entry_id,
                include_anchors: true,
            })
            .await
            .expect_err("a foreign anchored hash must be refused");

        match err {
            crate::error::ServerError::ReceiptStateMismatch { source_name, .. } => {
                assert_eq!(source_name, "rfc3161 anchor");
            }
            other => panic!("expected ReceiptStateMismatch, got {other:?}"),
        }
    }
}
