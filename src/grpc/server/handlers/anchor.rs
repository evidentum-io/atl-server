//! Handlers for anchoring operations
//!
//! This module implements:
//! - AnchorEntry: Submit a single entry
//! - AnchorBatch: Submit multiple entries in one request

use tonic::{Request, Response, Status};

use crate::error::ServerError;
use crate::grpc::proto::*;
use crate::grpc::server::service::SequencerGrpcServer;
use crate::traits::AppendParams;

/// Handle AnchorEntry request
///
/// Submits a single entry to the Sequencer and returns a signed checkpoint.
pub async fn handle_anchor_entry(
    server: &SequencerGrpcServer,
    request: Request<AnchorRequest>,
) -> Result<Response<AnchorResponse>, Status> {
    server.check_auth(&request)?;

    let req = request.into_inner();

    // Validate hashes
    let payload_hash: [u8; 32] = req
        .payload_hash
        .try_into()
        .map_err(|_| Status::invalid_argument("payload_hash must be 32 bytes"))?;
    let metadata_hash: [u8; 32] = req
        .metadata_hash
        .try_into()
        .map_err(|_| Status::invalid_argument("metadata_hash must be 32 bytes"))?;

    // Parse metadata JSON
    let metadata_cleartext = if req.metadata_json.is_empty() {
        None
    } else {
        Some(
            serde_json::from_str(&req.metadata_json)
                .map_err(|e| Status::invalid_argument(format!("invalid metadata JSON: {e}")))?,
        )
    };

    let params = AppendParams {
        payload_hash,
        metadata_hash,
        metadata_cleartext,
        external_id: if req.external_id.is_empty() {
            None
        } else {
            Some(req.external_id)
        },
    };

    // Send to SequencerCore
    let result = server
        .sequencer_handle()
        .append(params)
        .await
        .map_err(|e| match e {
            ServerError::ServiceUnavailable(msg) => Status::unavailable(msg),
            _ => Status::internal(e.to_string()),
        })?;

    // Sign checkpoint
    let checkpoint = sign_checkpoint(
        server,
        result.tree_head.tree_size,
        &result.tree_head.root_hash,
    )?;

    // Get consistency proof for split-view protection
    let (consistency_proof, consistency_from) =
        get_consistency_proof_from_anchor(server, result.tree_head.tree_size)?;

    // TODO: Add TSA anchor support when TSA module is integrated
    let tsa_anchor = None;

    Ok(Response::new(AnchorResponse {
        entry_id: result.id.to_string(),
        leaf_index: result.leaf_index,
        inclusion_path: result.inclusion_proof.iter().map(|h| h.to_vec()).collect(),
        checkpoint: Some(checkpoint),
        consistency_proof,
        consistency_from,
        tsa_anchor,
    }))
}

/// Handle AnchorBatch request
///
/// Submits multiple entries to the Sequencer in one request for maximum throughput.
pub async fn handle_anchor_batch(
    server: &SequencerGrpcServer,
    request: Request<AnchorBatchRequest>,
) -> Result<Response<AnchorBatchResponse>, Status> {
    server.check_auth(&request)?;

    let req = request.into_inner();

    if req.entries.is_empty() {
        return Err(Status::invalid_argument("empty batch"));
    }

    // Convert requests to params
    let params: Result<Vec<AppendParams>, Status> =
        req.entries
            .into_iter()
            .map(|entry| {
                let payload_hash: [u8; 32] = entry
                    .payload_hash
                    .try_into()
                    .map_err(|_| Status::invalid_argument("payload_hash must be 32 bytes"))?;
                let metadata_hash: [u8; 32] = entry
                    .metadata_hash
                    .try_into()
                    .map_err(|_| Status::invalid_argument("metadata_hash must be 32 bytes"))?;

                let metadata_cleartext = if entry.metadata_json.is_empty() {
                    None
                } else {
                    Some(serde_json::from_str(&entry.metadata_json).map_err(|e| {
                        Status::invalid_argument(format!("invalid metadata JSON: {e}"))
                    })?)
                };

                Ok(AppendParams {
                    payload_hash,
                    metadata_hash,
                    metadata_cleartext,
                    external_id: if entry.external_id.is_empty() {
                        None
                    } else {
                        Some(entry.external_id)
                    },
                })
            })
            .collect();

    let params = params?;

    // Send batch to SequencerCore
    let results = server
        .sequencer_handle()
        .append_batch(params)
        .await
        .map_err(|e| match e {
            ServerError::ServiceUnavailable(msg) => Status::unavailable(msg),
            _ => Status::internal(e.to_string()),
        })?;

    // Get tree head from first result
    let tree_head = results
        .first()
        .map(|r| &r.tree_head)
        .ok_or_else(|| Status::internal("empty results"))?;

    // Sign checkpoint
    let checkpoint = sign_checkpoint(server, tree_head.tree_size, &tree_head.root_hash)?;

    let entry_results: Vec<AnchorEntryResult> = results
        .iter()
        .map(|r| AnchorEntryResult {
            entry_id: r.id.to_string(),
            leaf_index: r.leaf_index,
            inclusion_path: r.inclusion_proof.iter().map(|h| h.to_vec()).collect(),
        })
        .collect();

    Ok(Response::new(AnchorBatchResponse {
        results: entry_results,
        checkpoint: Some(checkpoint),
    }))
}

/// Sign a checkpoint for the given tree state
#[allow(clippy::result_large_err)]
fn sign_checkpoint(
    server: &SequencerGrpcServer,
    tree_size: u64,
    root_hash: &[u8; 32],
) -> Result<Checkpoint, Status> {
    let checkpoint =
        server
            .signer()
            .sign_checkpoint_struct(server.storage().origin_id(), tree_size, root_hash);

    Ok(Checkpoint {
        origin: checkpoint.origin.to_vec(),
        tree_size: checkpoint.tree_size,
        timestamp: checkpoint.timestamp,
        root_hash: checkpoint.root_hash.to_vec(),
        signature: checkpoint.signature.to_vec(),
        key_id: checkpoint.key_id.to_vec(),
    })
}

/// Get consistency proof from the last anchored tree size to current.
///
/// This provides split-view protection for fresh receipts.
#[allow(clippy::result_large_err)]
fn get_consistency_proof_from_anchor(
    server: &SequencerGrpcServer,
    current_tree_size: u64,
) -> Result<(Vec<Vec<u8>>, u64), Status> {
    // Get the most recent externally anchored tree size
    let anchored_size = server
        .storage()
        .get_latest_anchored_size()
        .map_err(|e| Status::internal(e.to_string()))?;

    match anchored_size {
        Some(from_size) if from_size < current_tree_size => {
            // Generate consistency proof from anchored size to current
            let proof = server
                .storage()
                .get_consistency_proof(from_size, current_tree_size)
                .map_err(|e| Status::internal(e.to_string()))?;

            Ok((proof.path.iter().map(|h| h.to_vec()).collect(), from_size))
        }
        _ => {
            // No anchor yet or tree hasn't grown since last anchor
            Ok((vec![], 0))
        }
    }
}
