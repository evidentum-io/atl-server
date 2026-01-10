//! gRPC client implementation for NODE role
//!
//! This module implements the GrpcDispatcher that NODEs use to forward
//! requests to a master SEQUENCER via gRPC.

mod config;
mod convert;
mod dispatcher;
mod operations;

pub use config::GrpcClientConfig;
pub use dispatcher::GrpcDispatcher;
