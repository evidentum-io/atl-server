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

        // 2. Get covering anchors (from ANCHOR-FIX-2)
        let entry_tree_size = leaf_index + 1;
        let anchors = if request.include_anchors {
            self.storage.get_anchors_covering(entry_tree_size, 10)?
        } else {
            vec![]
        };

        // 3. Determine target tree_size for receipt
        //    - If anchors exist: use closest anchor's tree_size
        //    - If no anchors: use current tree_size (fallback)
        let (target_tree_size, target_root_hash) = if let Some(anchor) = anchors.first() {
            // Build receipt at anchor's tree_size
            let root = self.storage.get_root_at_size(anchor.tree_size)?;
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
