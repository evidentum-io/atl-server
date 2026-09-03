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
use crate::traits::Storage;
use crate::validation::reject_duplicate_property_names;

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

    // Parse metadata JSON.
    //
    // The duplicate-property-name check runs on `metadata_json` itself,
    // before `from_str`: that string *is* the raw text, and after the parse
    // the losing occurrence of a repeated name is gone (RFC 8785 Section 3.1,
    // RFC 8259 Section 4). Unlike the HTTP path there is no envelope to scan,
    // so the scope is exactly the metadata document.
    let metadata_cleartext = if req.metadata_json.is_empty() {
        None
    } else {
        reject_duplicate_property_names(&req.metadata_json, "metadata_json")
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
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
// `Status` is 176 bytes, over the `result_large_err` threshold. It is tonic's
// own error type on a trait-shaped signature, so it cannot be boxed here; the
// helpers below carry the same allowance.
#[allow(clippy::result_large_err)]
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

                // Same check as the single-entry path: on the raw text, before
                // the parse. One bad entry fails the whole batch, which is the
                // existing behaviour for a malformed `metadata_json` too.
                let metadata_cleartext = if entry.metadata_json.is_empty() {
                    None
                } else {
                    reject_duplicate_property_names(&entry.metadata_json, "metadata_json")
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receipt::CheckpointSigner;
    use crate::sequencer::{Sequencer, SequencerConfig};
    use crate::storage::config::StorageConfig;
    use crate::storage::engine::StorageEngine;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn test_server() -> (SequencerGrpcServer, TempDir) {
        let dir = TempDir::new().expect("tempdir");
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let storage = Arc::new(
            StorageEngine::new(config, [7u8; 32])
                .await
                .expect("storage engine"),
        );
        let (sequencer, handle) = Sequencer::new(storage.clone(), SequencerConfig::default());
        tokio::spawn(sequencer.run());

        let signer = Arc::new(CheckpointSigner::from_bytes(&[42u8; 32]));
        (SequencerGrpcServer::new(handle, storage, signer, None), dir)
    }

    fn anchor_request(metadata_json: &str) -> Request<AnchorRequest> {
        Request::new(AnchorRequest {
            payload_hash: vec![1u8; 32],
            metadata_hash: vec![2u8; 32],
            metadata_json: metadata_json.to_string(),
            external_id: String::new(),
        })
    }

    /// The text is the defect: `serde_json::from_str` resolves it to
    /// `{"id":2}` and the entry is stored as a document the client never
    /// unambiguously sent. Driving the handler rather than
    /// `reject_duplicate_property_names` is the point -- a test of the helper
    /// alone would still pass with the call removed from here.
    #[tokio::test(flavor = "multi_thread")]
    async fn anchor_entry_refuses_a_duplicate_property_name() {
        let (server, _dir) = test_server().await;

        let status = handle_anchor_entry(&server, anchor_request(r#"{"id":1,"id":2}"#))
            .await
            .expect_err("a duplicate property name must be refused");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status.message().contains("duplicate property name"),
            "message should say what was wrong: {}",
            status.message()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anchor_entry_accepts_repeated_names_in_sibling_objects() {
        let (server, _dir) = test_server().await;

        handle_anchor_entry(&server, anchor_request(r#"{"a":{"id":1},"b":{"id":2}}"#))
            .await
            .expect("distinct objects may each carry an `id`");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anchor_batch_refuses_a_duplicate_property_name() {
        let (server, _dir) = test_server().await;

        let request = Request::new(AnchorBatchRequest {
            entries: vec![
                AnchorRequest {
                    payload_hash: vec![1u8; 32],
                    metadata_hash: vec![2u8; 32],
                    metadata_json: r#"{"ok":1}"#.to_string(),
                    external_id: String::new(),
                },
                AnchorRequest {
                    payload_hash: vec![3u8; 32],
                    metadata_hash: vec![4u8; 32],
                    metadata_json: r#"{"id":1,"id":2}"#.to_string(),
                    external_id: String::new(),
                },
            ],
        });

        let status = handle_anchor_batch(&server, request)
            .await
            .expect_err("a duplicate property name must fail the batch");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }
}
