//! Conversion helpers between protobuf and trait types
//!
//! This module provides functions to convert between gRPC proto types
//! and the internal trait types used by the application.

use crate::error::{ServerError, ServerResult};
use crate::grpc::proto;
use crate::traits::{
    Anchor, AnchorType, AppendParams, ConsistencyProofInfo, Entry, ReceiptResponse,
};

/// Convert trait AppendParams to proto AnchorRequest
pub fn trait_append_params_to_proto(params: &AppendParams) -> proto::AnchorRequest {
    proto::AnchorRequest {
        payload_hash: params.payload_hash.to_vec(),
        metadata_hash: params.metadata_hash.to_vec(),
        metadata_json: params
            .metadata_cleartext
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap_or_default())
            .unwrap_or_default(),
        external_id: params.external_id.clone().unwrap_or_default(),
    }
}

/// Convert proto Checkpoint to atl_core::Checkpoint
pub fn proto_checkpoint_to_core(cp: proto::Checkpoint) -> ServerResult<atl_core::Checkpoint> {
    Ok(atl_core::Checkpoint {
        origin: cp
            .origin
            .try_into()
            .map_err(|_| ServerError::Internal("invalid origin".into()))?,
        tree_size: cp.tree_size,
        timestamp: cp.timestamp,
        root_hash: cp
            .root_hash
            .try_into()
            .map_err(|_| ServerError::Internal("invalid root_hash".into()))?,
        signature: cp
            .signature
            .try_into()
            .map_err(|_| ServerError::Internal("invalid signature".into()))?,
        key_id: cp
            .key_id
            .try_into()
            .map_err(|_| ServerError::Internal("invalid key_id".into()))?,
    })
}

/// Convert proto ReceiptResponse to trait ReceiptResponse
pub fn proto_receipt_to_trait(response: proto::ReceiptResponse) -> ServerResult<ReceiptResponse> {
    let entry = response
        .entry
        .ok_or_else(|| ServerError::Internal("missing entry".into()))?;
    let checkpoint = response
        .checkpoint
        .ok_or_else(|| ServerError::Internal("missing checkpoint".into()))?;

    Ok(ReceiptResponse {
        entry: Entry {
            id: entry
                .id
                .parse()
                .map_err(|_| ServerError::Internal("invalid UUID".into()))?,
            payload_hash: entry
                .payload_hash
                .try_into()
                .map_err(|_| ServerError::Internal("invalid payload_hash".into()))?,
            metadata_hash: entry
                .metadata_hash
                .try_into()
                .map_err(|_| ServerError::Internal("invalid metadata_hash".into()))?,
            metadata_cleartext: if entry.metadata_json.is_empty() {
                None
            } else {
                serde_json::from_str(&entry.metadata_json).ok()
            },
            leaf_index: Some(entry.leaf_index),
            created_at: chrono::DateTime::from_timestamp_nanos(entry.created_at as i64),
            external_id: if entry.external_id.is_empty() {
                None
            } else {
                Some(entry.external_id)
            },
        },
        inclusion_proof: response
            .inclusion_path
            .into_iter()
            .map(|h| {
                h.try_into()
                    .map_err(|_| ServerError::Internal("invalid hash".into()))
            })
            .collect::<Result<Vec<[u8; 32]>, _>>()?,
        checkpoint: proto_checkpoint_to_core(checkpoint)?,
        consistency_proof: response.consistency_proof.map(|cp| ConsistencyProofInfo {
            from_tree_size: cp.from_tree_size,
            path: cp
                .path
                .into_iter()
                .filter_map(|h| h.try_into().ok())
                .collect(),
        }),
        anchors: response
            .anchors
            .into_iter()
            .map(proto_external_anchor_to_trait)
            .collect(),
    })
}

/// Convert proto ExternalAnchor to trait Anchor
fn proto_external_anchor_to_trait(a: proto::ExternalAnchor) -> Anchor {
    let metadata: serde_json::Value = serde_json::from_str(&a.metadata_json).unwrap_or_default();

    let tree_size = metadata
        .get("tree_size")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    Anchor {
        anchor_type: match a.anchor_type.as_str() {
            "rfc3161" => AnchorType::Rfc3161,
            "ots" | "bitcoin_ots" => AnchorType::BitcoinOts,
            _ => AnchorType::Other,
        },
        target: metadata
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("data_tree_root")
            .to_string(),
        anchored_hash: a.anchored_hash.try_into().unwrap_or([0u8; 32]),
        tree_size,
        super_tree_size: metadata.get("super_tree_size").and_then(|v| v.as_u64()),
        timestamp: a.timestamp,
        token: a.token,
        metadata,
    }
}
