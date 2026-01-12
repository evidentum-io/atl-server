//! Handlers for tree operations
//!
//! This module implements:
//! - GetTreeHead: Get current tree state
//! - GetPublicKeys: Get verification keys

use tonic::{Request, Response, Status};

use crate::grpc::proto::*;
use crate::grpc::server::service::SequencerGrpcServer;

/// Handle GetTreeHead request
///
/// Returns the current state of the Merkle tree.
pub async fn handle_get_tree_head(
    server: &SequencerGrpcServer,
    request: Request<GetTreeHeadRequest>,
) -> Result<Response<TreeHeadResponse>, Status> {
    server.check_auth(&request)?;

    let head = server.storage().tree_head();

    // Create checkpoint on the fly using current tree head and signer
    let cp = server
        .signer()
        .sign_checkpoint_struct(head.origin, head.tree_size, &head.root_hash);

    Ok(Response::new(TreeHeadResponse {
        tree_size: head.tree_size,
        root_hash: head.root_hash.to_vec(),
        origin: server.storage().origin_id().to_vec(),
        latest_checkpoint: Some(Checkpoint {
            origin: cp.origin.to_vec(),
            tree_size: cp.tree_size,
            timestamp: cp.timestamp,
            root_hash: cp.root_hash.to_vec(),
            signature: cp.signature.to_vec(),
            key_id: cp.key_id.to_vec(),
        }),
    }))
}

/// Handle GetPublicKeys request
///
/// Returns the public keys used for checkpoint signing.
pub async fn handle_get_public_keys(
    server: &SequencerGrpcServer,
    request: Request<GetPublicKeysRequest>,
) -> Result<Response<PublicKeysResponse>, Status> {
    server.check_auth(&request)?;

    let (key_id, public_key_bytes) = server.signer().public_key_info();

    Ok(Response::new(PublicKeysResponse {
        keys: vec![PublicKey {
            key_id: key_id.to_vec(),
            public_key: public_key_bytes.to_vec(),
            algorithm: "ed25519".to_string(),
            created_at: 0, // TODO: track key creation time
        }],
    }))
}
