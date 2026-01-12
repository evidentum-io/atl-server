//! SequencerClient trait implementation for GrpcDispatcher
//!
//! This module implements all operations defined by the SequencerClient trait.

use async_trait::async_trait;
use tonic::Request;

use crate::error::ServerResult;
use crate::grpc::client::convert::{
    proto_checkpoint_to_core, proto_receipt_to_trait, trait_append_params_to_proto,
};
use crate::grpc::client::dispatcher::GrpcDispatcher;
use crate::grpc::proto::*;
use crate::traits::{
    AnchoringStatus, AppendParams, BatchDispatchResult, ConsistencyProofResponse, DispatchResult,
    GetReceiptRequest, PublicKeyInfo, ReceiptResponse, SequencerClient, TreeHead,
    TriggerAnchoringRequest,
};

#[async_trait]
impl SequencerClient for GrpcDispatcher {
    async fn dispatch(&self, params: AppendParams) -> ServerResult<DispatchResult> {
        let request = trait_append_params_to_proto(&params);

        let response = self
            .with_retry(|| {
                let req = self.add_auth(Request::new(request.clone()));
                let mut c = self.client();
                async move { c.anchor_entry(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        let checkpoint = response.checkpoint.ok_or_else(|| {
            crate::error::ServerError::Internal("missing checkpoint in response".into())
        })?;

        Ok(DispatchResult {
            result: crate::traits::AppendResult {
                id: response.entry_id.parse().map_err(|_| {
                    crate::error::ServerError::Internal("invalid UUID in response".into())
                })?,
                leaf_index: response.leaf_index,
                tree_head: crate::traits::TreeHead {
                    tree_size: checkpoint.tree_size,
                    root_hash: checkpoint.root_hash.clone().try_into().map_err(|_| {
                        crate::error::ServerError::Internal("invalid root_hash length".into())
                    })?,
                    origin: checkpoint.origin.clone().try_into().map_err(|_| {
                        crate::error::ServerError::Internal("invalid origin length".into())
                    })?,
                },
                inclusion_proof: response
                    .inclusion_path
                    .into_iter()
                    .map(|h| {
                        h.try_into().map_err(|_| {
                            crate::error::ServerError::Internal("invalid hash length".into())
                        })
                    })
                    .collect::<Result<Vec<[u8; 32]>, _>>()?,
                timestamp: chrono::Utc::now(),
            },
            checkpoint: proto_checkpoint_to_core(checkpoint)?,
        })
    }

    async fn dispatch_batch(&self, params: Vec<AppendParams>) -> ServerResult<BatchDispatchResult> {
        let entries: Vec<AnchorRequest> = params.iter().map(trait_append_params_to_proto).collect();

        let request = AnchorBatchRequest { entries };

        let response = self
            .with_retry(|| {
                let req = self.add_auth(Request::new(request.clone()));
                let mut c = self.client();
                async move { c.anchor_batch(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        let checkpoint = response.checkpoint.ok_or_else(|| {
            crate::error::ServerError::Internal("missing checkpoint in response".into())
        })?;

        let results: Vec<crate::traits::AppendResult> = response
            .results
            .into_iter()
            .map(|r| {
                Ok(crate::traits::AppendResult {
                    id: r
                        .entry_id
                        .parse()
                        .map_err(|_| crate::error::ServerError::Internal("invalid UUID".into()))?,
                    leaf_index: r.leaf_index,
                    tree_head: crate::traits::TreeHead {
                        tree_size: checkpoint.tree_size,
                        root_hash: checkpoint.root_hash.clone().try_into().map_err(|_| {
                            crate::error::ServerError::Internal("invalid root_hash".into())
                        })?,
                        origin: checkpoint.origin.clone().try_into().map_err(|_| {
                            crate::error::ServerError::Internal("invalid origin".into())
                        })?,
                    },
                    inclusion_proof: r
                        .inclusion_path
                        .into_iter()
                        .map(|h| {
                            h.try_into().map_err(|_| {
                                crate::error::ServerError::Internal("invalid hash".into())
                            })
                        })
                        .collect::<Result<Vec<[u8; 32]>, _>>()?,
                    timestamp: chrono::Utc::now(),
                })
            })
            .collect::<ServerResult<Vec<_>>>()?;

        Ok(BatchDispatchResult {
            results,
            checkpoint: proto_checkpoint_to_core(checkpoint)?,
        })
    }

    async fn get_receipt(&self, request: GetReceiptRequest) -> ServerResult<ReceiptResponse> {
        let grpc_request = crate::grpc::proto::GetReceiptRequest {
            entry_id: request.entry_id.to_string(),
            include_anchors: request.include_anchors,
        };

        let response = self
            .with_retry(|| {
                let req = self.add_auth(Request::new(grpc_request.clone()));
                let mut c = self.client();
                async move { c.get_receipt(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        proto_receipt_to_trait(response)
    }

    async fn get_tree_head(&self) -> ServerResult<TreeHead> {
        let response = self
            .with_retry(|| {
                let req = self.add_auth(Request::new(GetTreeHeadRequest {}));
                let mut c = self.client();
                async move { c.get_tree_head(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        Ok(TreeHead {
            tree_size: response.tree_size,
            root_hash: response
                .root_hash
                .try_into()
                .map_err(|_| crate::error::ServerError::Internal("invalid root_hash".into()))?,
            origin: response
                .origin
                .try_into()
                .map_err(|_| crate::error::ServerError::Internal("invalid origin".into()))?,
        })
    }

    async fn get_consistency_proof(
        &self,
        _from_size: u64,
        _to_size: u64,
    ) -> ServerResult<ConsistencyProofResponse> {
        // Note: This operation is not directly supported by the gRPC proto
        // It would need to be implemented via GetTreeHead calls
        // For now, return NotSupported
        Err(crate::error::ServerError::NotSupported(
            "get_consistency_proof via gRPC not yet implemented".into(),
        ))
    }

    async fn get_public_keys(&self) -> ServerResult<Vec<PublicKeyInfo>> {
        let response = self
            .with_retry(|| {
                let req = self.add_auth(Request::new(GetPublicKeysRequest {}));
                let mut c = self.client();
                async move { c.get_public_keys(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        response
            .keys
            .into_iter()
            .map(|k| {
                Ok(PublicKeyInfo {
                    key_id: k.key_id.try_into().map_err(|_| {
                        crate::error::ServerError::Internal("invalid key_id".into())
                    })?,
                    public_key: k.public_key.try_into().map_err(|_| {
                        crate::error::ServerError::Internal("invalid public_key".into())
                    })?,
                    algorithm: k.algorithm,
                    created_at: k.created_at,
                })
            })
            .collect()
    }

    async fn trigger_anchoring(
        &self,
        request: TriggerAnchoringRequest,
    ) -> ServerResult<Vec<AnchoringStatus>> {
        let grpc_request = crate::grpc::proto::TriggerAnchoringRequest {
            anchor_types: request.anchor_types,
        };

        let response = self
            .with_retry(|| {
                let req = self.add_auth(Request::new(grpc_request.clone()));
                let mut c = self.client();
                async move { c.trigger_anchoring(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        Ok(response
            .statuses
            .into_iter()
            .map(|s| AnchoringStatus {
                anchor_type: s.anchor_type,
                status: s.status,
                timestamp: if s.timestamp > 0 {
                    Some(s.timestamp)
                } else {
                    None
                },
                estimated_finality_secs: if s.estimated_finality_secs > 0 {
                    Some(s.estimated_finality_secs)
                } else {
                    None
                },
            })
            .collect())
    }

    async fn health_check(&self) -> ServerResult<()> {
        let response = self
            .with_retry(|| {
                let req = self.add_auth(Request::new(HealthCheckRequest {}));
                let mut c = self.client();
                async move { c.health_check(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if response.healthy {
            Ok(())
        } else {
            Err(crate::error::ServerError::ServiceUnavailable(
                response.message,
            ))
        }
    }
}
