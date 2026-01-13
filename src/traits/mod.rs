//! Trait definitions for storage and anchoring

pub mod anchor;
pub mod checkpoint;
pub mod dispatcher;
pub mod proof;
pub mod storage;
pub mod tree_rotator;

// Re-export all types
#[allow(unused_imports)]
pub use anchor::{
    Anchor, AnchorRequest, AnchorResult, AnchorType, Anchorer, OtsConfig, Rfc3161Config,
};

#[allow(unused_imports)]
pub use checkpoint::CheckpointStore;

#[allow(unused_imports)]
pub use dispatcher::{
    AnchoringStatus, BatchDispatchResult, ConsistencyProofInfo, ConsistencyProofResponse,
    DispatchResult, GetReceiptRequest, GrpcDispatcher, LocalDispatcher, PublicKeyInfo,
    ReceiptResponse, SequencerClient, TriggerAnchoringRequest,
};

#[allow(unused_imports)]
pub use proof::ProofProvider;

#[allow(unused_imports)]
pub use storage::{
    AppendParams, AppendResult, BatchResult, ConsistencyProof, Entry, EntryResult, InclusionProof,
    Storage, TreeHead,
};

#[allow(unused_imports)]
pub use tree_rotator::TreeRotator;
