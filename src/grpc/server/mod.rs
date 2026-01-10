//! gRPC server implementation for SEQUENCER role
//!
//! This module implements the SequencerService gRPC interface.
//! The SEQUENCER exposes both gRPC (port 50051) and HTTP admin endpoints.

mod service;

pub mod handlers;

pub use service::SequencerGrpcServer;
