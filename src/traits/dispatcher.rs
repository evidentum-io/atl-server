//! Sequencer client trait and dispatcher implementations

use async_trait::async_trait;
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
}

impl LocalDispatcher {
    /// Create a new local dispatcher with a sequencer handle
    pub fn new(handle: crate::sequencer::SequencerHandle) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl SequencerClient for LocalDispatcher {
    async fn dispatch(&self, params: AppendParams) -> ServerResult<DispatchResult> {
        // Send append request through the sequencer handle
        let result = self.handle.append(params).await?;

        // Create checkpoint (TODO: implement proper signing in checkpoint module)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let checkpoint = atl_core::Checkpoint {
            origin: result.tree_head.origin,
            tree_size: result.tree_head.tree_size,
            root_hash: result.tree_head.root_hash,
            timestamp,
            signature: [0u8; 64], // TODO: implement signing
            key_id: [0u8; 32],    // TODO: derive from signing key
        };

        Ok(DispatchResult { result, checkpoint })
    }

    async fn dispatch_batch(
        &self,
        _params: Vec<AppendParams>,
    ) -> ServerResult<BatchDispatchResult> {
        Err(crate::error::ServerError::NotSupported(
            "batch dispatch not yet implemented".into(),
        ))
    }

    async fn get_receipt(&self, _request: GetReceiptRequest) -> ServerResult<ReceiptResponse> {
        Err(crate::error::ServerError::NotSupported(
            "get_receipt not yet implemented".into(),
        ))
    }

    async fn get_tree_head(&self) -> ServerResult<TreeHead> {
        Err(crate::error::ServerError::NotSupported(
            "get_tree_head not yet implemented".into(),
        ))
    }

    async fn get_consistency_proof(
        &self,
        _from_size: u64,
        _to_size: u64,
    ) -> ServerResult<ConsistencyProofResponse> {
        Err(crate::error::ServerError::NotSupported(
            "get_consistency_proof not yet implemented".into(),
        ))
    }

    async fn get_public_keys(&self) -> ServerResult<Vec<PublicKeyInfo>> {
        Err(crate::error::ServerError::NotSupported(
            "get_public_keys not yet implemented".into(),
        ))
    }

    async fn trigger_anchoring(
        &self,
        _request: TriggerAnchoringRequest,
    ) -> ServerResult<Vec<AnchoringStatus>> {
        Err(crate::error::ServerError::NotSupported(
            "trigger_anchoring not yet implemented".into(),
        ))
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
