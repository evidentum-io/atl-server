//! Trait definitions for storage and anchoring

pub mod anchor;
pub mod dispatcher;
pub mod storage;

// Re-export all types
#[allow(unused_imports)]
pub use anchor::{
    Anchor, AnchorRequest, AnchorResult, AnchorType, Anchorer, OtsConfig, Rfc3161Config,
};

#[allow(unused_imports)]
pub use dispatcher::{
    AnchoringStatus, BatchDispatchResult, ConsistencyProofInfo, ConsistencyProofResponse,
    DispatchResult, GetReceiptRequest, GrpcDispatcher, LocalDispatcher, PublicKeyInfo,
    ReceiptResponse, SequencerClient, TriggerAnchoringRequest,
};

#[allow(unused_imports)]
pub use storage::{
    AppendParams, AppendResult, ConsistencyProof, Entry, InclusionProof, Storage, TreeHead,
};
