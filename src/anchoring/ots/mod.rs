//! OpenTimestamps client implementation
//!
//! Bitcoin-backed timestamping using the OpenTimestamps protocol.
//! Uses atl_core::ots for proof serialization and verification.

mod calendar;
mod client;
mod proof;
mod types;

pub use client::OpenTimestampsClient;
pub use types::{OtsConfig, OtsStatus};

// Re-export proof utilities for advanced use cases
pub use proof::{detect_status, is_finalized, parse_proof, verify_timestamp};
