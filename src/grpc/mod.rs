//! gRPC module for distributed Node-Sequencer architecture
//!
//! This module implements the gRPC protocol for communication between NODE and SEQUENCER roles.
//! NODEs are stateless HTTP servers that forward requests to a master SEQUENCER via gRPC.
//!
//! # Architecture
//!
//! - **SEQUENCER:** Runs both gRPC server (port 50051) and HTTP admin endpoints
//! - **NODE:** Runs HTTP server and uses GrpcDispatcher to forward to SEQUENCER
//!
//! # Authentication
//!
//! All gRPC requests must include `x-sequencer-token` header with a pre-shared secret.

#[allow(clippy::all)]
#[allow(clippy::pedantic)]
#[allow(clippy::nursery)]
pub mod proto;

pub mod auth;

#[cfg(feature = "grpc")]
pub mod server;

#[cfg(feature = "grpc")]
pub mod client;

// Re-export key types for convenience
pub use auth::AuthInterceptor;

#[cfg(feature = "grpc")]
pub use client::{GrpcClientConfig, GrpcDispatcher};

#[cfg(feature = "grpc")]
pub use server::SequencerGrpcServer;
