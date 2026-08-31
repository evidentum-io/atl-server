//! Handlers for receipt operations
//!
//! This module implements:
//! - GetReceipt: Retrieve a receipt by entry ID
//! - UpgradeReceipt: Upgrade receipt with Bitcoin proof (Tier-2 evidence)

use tonic::{Request, Response, Status};

use crate::error::ServerError;
use crate::grpc::proto::*;
use crate::grpc::server::service::SequencerGrpcServer;
use crate::traits::Storage;

/// Handle GetReceipt request
///
/// Retrieves a receipt for the given entry ID with optional external anchors.
pub async fn handle_get_receipt(
    server: &SequencerGrpcServer,
    request: Request<GetReceiptRequest>,
) -> Result<Response<ReceiptResponse>, Status> {
    server.check_auth(&request)?;

    let req = request.into_inner();

    let entry_id: uuid::Uuid = req
        .entry_id
        .parse()
        .map_err(|_| Status::invalid_argument("invalid UUID"))?;

    // Get entry from storage
    let entry = server.storage().get_entry(&entry_id).map_err(|e| match e {
        ServerError::EntryNotFound(_) => Status::not_found("entry not found"),
        _ => Status::internal(e.to_string()),
    })?;

    // Get inclusion proof
    let proof = server
        .storage()
        .get_inclusion_proof(&entry_id, None)
        .map_err(|e| Status::internal(e.to_string()))?;

    // Create checkpoint on the fly using current tree head and signer
    let head = server.storage().tree_head();
    let checkpoint =
        server
            .signer()
            .sign_checkpoint_struct(head.origin, head.tree_size, &head.root_hash);

    // Get anchors if requested
    let anchors = if req.include_anchors {
        server
            .storage()
            .get_anchors(checkpoint.tree_size)
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(|a| ExternalAnchor {
                anchor_type: a.anchor_type.to_string(),
                anchored_hash: a.anchored_hash.to_vec(),
                timestamp: a.timestamp,
                token: a.token,
                metadata_json: serde_json::to_string(&a.metadata).unwrap_or_default(),
            })
            .collect()
    } else {
        vec![]
    };

    // Get consistency proof for split-view protection
    let consistency_proof = server
        .storage()
        .get_latest_anchored_size()
        .ok()
        .flatten()
        .and_then(|size| {
            if size < checkpoint.tree_size {
                server
                    .storage()
                    .get_consistency_proof(size, checkpoint.tree_size)
                    .ok()
                    .map(|cp| ConsistencyProof {
                        from_tree_size: size,
                        path: cp.path.iter().map(|h| h.to_vec()).collect(),
                    })
            } else {
                None
            }
        });

    Ok(Response::new(ReceiptResponse {
        entry: Some(Entry {
            id: entry.id.to_string(),
            payload_hash: entry.payload_hash.to_vec(),
            metadata_hash: entry.metadata_hash.to_vec(),
            metadata_json: entry
                .metadata_cleartext
                .map(|v| serde_json::to_string(&v).unwrap_or_default())
                .unwrap_or_default(),
            leaf_index: entry.leaf_index.unwrap_or(0),
            created_at: entry.created_at.timestamp_nanos_opt().unwrap_or(0) as u64,
            external_id: entry.external_id.unwrap_or_default(),
        }),
        inclusion_path: proof.path.iter().map(|h| h.to_vec()).collect(),
        checkpoint: Some(Checkpoint {
            origin: checkpoint.origin.to_vec(),
            tree_size: checkpoint.tree_size,
            timestamp: checkpoint.timestamp,
            root_hash: checkpoint.root_hash.to_vec(),
            signature: checkpoint.signature.to_vec(),
            key_id: checkpoint.key_id.to_vec(),
        }),
        consistency_proof,
        anchors,
    }))
}

/// Handle UpgradeReceipt request
///
/// Re-issues the receipt for the entry. When the entry's Data Tree has been
/// closed, appended to the Super-Tree and that Super Root has a confirmed
/// Bitcoin anchor, the re-issued receipt carries `super_proof` plus both
/// anchors (Receipt-Full, ATL Protocol Section 5.5.3); otherwise the caller
/// is told the upgrade is still pending.
///
/// The receipt is produced entirely by `generate_receipt`, which binds
/// every part of it to one tree state. Anchors are never appended here:
/// an anchor added after the fact is not bound to the state the proof was
/// built for, and Section 5.5.1 step 2 / Section 5.5.2 step 2 compare
/// exactly that binding.
pub async fn handle_upgrade_receipt(
    server: &SequencerGrpcServer,
    request: Request<UpgradeReceiptRequest>,
) -> Result<Response<UpgradeReceiptResponse>, Status> {
    server.check_auth(&request)?;

    let req = request.into_inner();

    let entry_id: uuid::Uuid = req
        .entry_id
        .parse()
        .map_err(|_| Status::invalid_argument("invalid UUID"))?;

    let entry = server.storage().get_entry(&entry_id).map_err(|e| match e {
        ServerError::EntryNotFound(_) => Status::not_found("entry not found"),
        _ => Status::internal(e.to_string()),
    })?;

    let leaf_index = entry
        .leaf_index
        .ok_or_else(|| Status::internal("entry has no leaf_index"))?;

    let options = crate::receipt::ReceiptOptions {
        include_anchors: true,
        ..Default::default()
    };

    let receipt =
        crate::receipt::generate_receipt(&entry_id, server.storage(), server.signer(), options)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

    let has_bitcoin_anchor = receipt
        .anchors
        .iter()
        .any(|a| matches!(a, atl_core::ReceiptAnchor::BitcoinOts { .. }));

    if has_bitcoin_anchor {
        let receipt_json =
            serde_json::to_vec(&receipt).map_err(|e| Status::internal(e.to_string()))?;

        return Ok(Response::new(UpgradeReceiptResponse {
            status: UpgradeStatus::Upgraded as i32,
            upgraded_receipt_json: receipt_json,
            receipt_tree_size: receipt.proof.tree_size,
            last_anchor_tree_size: 0,
            estimated_completion_unix: 0,
        }));
    }

    let last_anchor_tree_size = server
        .storage()
        .get_latest_anchored_size()
        .map_err(|e| Status::internal(e.to_string()))?
        .unwrap_or(0);

    let estimated_completion = chrono::Utc::now() + chrono::Duration::hours(1);
    let estimated_completion_unix = estimated_completion.timestamp_nanos_opt().unwrap_or(0) as u64;

    Ok(Response::new(UpgradeReceiptResponse {
        status: UpgradeStatus::Pending as i32,
        upgraded_receipt_json: vec![],
        receipt_tree_size: leaf_index + 1,
        last_anchor_tree_size,
        estimated_completion_unix,
    }))
}
