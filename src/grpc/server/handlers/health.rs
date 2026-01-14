//! Handlers for health and admin operations
//!
//! This module implements:
//! - HealthCheck: Server health status
//! - TriggerAnchoring: Manually trigger external anchoring

use tonic::{Request, Response, Status};

use crate::grpc::proto::*;
use crate::grpc::server::service::SequencerGrpcServer;
use crate::traits::Storage;

/// Handle HealthCheck request
///
/// Returns the current health status of the server.
pub async fn handle_health_check(
    server: &SequencerGrpcServer,
    request: Request<HealthCheckRequest>,
) -> Result<Response<HealthCheckResponse>, Status> {
    server.check_auth(&request)?;

    let head = server.storage().tree_head();

    Ok(Response::new(HealthCheckResponse {
        healthy: true,
        message: "OK".to_string(),
        tree_size: head.tree_size,
        uptime_secs: server.uptime_secs(),
    }))
}

/// Handle TriggerAnchoring request
///
/// Triggers external anchoring for the current tree head.
/// TODO: Implement actual anchoring trigger when background anchoring is integrated.
pub async fn handle_trigger_anchoring(
    server: &SequencerGrpcServer,
    request: Request<TriggerAnchoringRequest>,
) -> Result<Response<TriggerAnchoringResponse>, Status> {
    server.check_auth(&request)?;

    let req = request.into_inner();

    let head = server.storage().tree_head();

    // TODO: Implement actual anchoring trigger
    // For now, return placeholder response
    let statuses: Vec<AnchoringStatus> = req
        .anchor_types
        .iter()
        .map(|t| AnchoringStatus {
            anchor_type: t.clone(),
            status: "pending".to_string(),
            timestamp: 0,
            estimated_finality_secs: if t == "ots" { 3600 } else { 0 },
            error_message: String::new(),
        })
        .collect();

    Ok(Response::new(TriggerAnchoringResponse {
        tree_size: head.tree_size,
        root_hash: head.root_hash.to_vec(),
        statuses,
    }))
}
