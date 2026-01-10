//! gRPC server service implementation
//!
//! This module implements the SequencerGrpcServer struct and the core gRPC service trait.

use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::grpc::proto::sequencer_service_server::{SequencerService, SequencerServiceServer};
use crate::grpc::proto::*;
use crate::receipt::CheckpointSigner;
use crate::sequencer::SequencerHandle;
use crate::traits::Storage;

/// gRPC server implementation for SEQUENCER role
pub struct SequencerGrpcServer {
    /// Handle to the local SequencerCore
    sequencer_handle: SequencerHandle,

    /// Storage for query operations
    storage: Arc<dyn Storage>,

    /// Checkpoint signer
    signer: Arc<CheckpointSigner>,

    /// Server start time (for uptime)
    start_time: Instant,
}

impl SequencerGrpcServer {
    /// Create a new gRPC server
    ///
    /// # Arguments
    ///
    /// * `sequencer_handle` - Handle to the SequencerCore for submitting requests
    /// * `storage` - Storage backend for queries
    /// * `signer` - Checkpoint signer for creating signed tree heads
    pub fn new(
        sequencer_handle: SequencerHandle,
        storage: Arc<dyn Storage>,
        signer: Arc<CheckpointSigner>,
    ) -> Self {
        Self {
            sequencer_handle,
            storage,
            signer,
            start_time: Instant::now(),
        }
    }

    /// Create tonic service with optional authentication interceptor
    ///
    /// # Arguments
    ///
    /// * `token` - Optional shared secret for authentication. If None, no auth is required.
    ///
    /// # Returns
    ///
    /// A configured `SequencerServiceServer` ready to be served by tonic.
    pub fn into_service(self, token: Option<String>) -> SequencerServiceServer<Self> {
        if token.is_some() {
            info!("gRPC server configured with authentication");
        } else {
            info!("gRPC server running without authentication (for dev/testing only)");
        }

        // TODO: Implement proper interceptor when tonic supports it
        // For now, authentication must be done inside handler methods
        SequencerServiceServer::new(self)
    }

    /// Helper: Get the sequencer handle
    pub(super) fn sequencer_handle(&self) -> &SequencerHandle {
        &self.sequencer_handle
    }

    /// Helper: Get the storage
    pub(super) fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    /// Helper: Get the signer
    pub(super) fn signer(&self) -> &Arc<CheckpointSigner> {
        &self.signer
    }

    /// Helper: Get server uptime in seconds
    pub(super) fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

#[tonic::async_trait]
impl SequencerService for SequencerGrpcServer {
    async fn anchor_entry(
        &self,
        request: Request<AnchorRequest>,
    ) -> Result<Response<AnchorResponse>, Status> {
        super::handlers::anchor::handle_anchor_entry(self, request).await
    }

    async fn anchor_batch(
        &self,
        request: Request<AnchorBatchRequest>,
    ) -> Result<Response<AnchorBatchResponse>, Status> {
        super::handlers::anchor::handle_anchor_batch(self, request).await
    }

    async fn get_receipt(
        &self,
        request: Request<GetReceiptRequest>,
    ) -> Result<Response<ReceiptResponse>, Status> {
        super::handlers::receipt::handle_get_receipt(self, request).await
    }

    async fn get_tree_head(
        &self,
        request: Request<GetTreeHeadRequest>,
    ) -> Result<Response<TreeHeadResponse>, Status> {
        super::handlers::tree::handle_get_tree_head(self, request).await
    }

    async fn get_public_keys(
        &self,
        request: Request<GetPublicKeysRequest>,
    ) -> Result<Response<PublicKeysResponse>, Status> {
        super::handlers::tree::handle_get_public_keys(self, request).await
    }

    async fn trigger_anchoring(
        &self,
        request: Request<TriggerAnchoringRequest>,
    ) -> Result<Response<TriggerAnchoringResponse>, Status> {
        super::handlers::health::handle_trigger_anchoring(self, request).await
    }

    async fn upgrade_receipt(
        &self,
        request: Request<UpgradeReceiptRequest>,
    ) -> Result<Response<UpgradeReceiptResponse>, Status> {
        super::handlers::receipt::handle_upgrade_receipt(self, request).await
    }

    async fn health_check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        super::handlers::health::handle_health_check(self, request).await
    }
}
