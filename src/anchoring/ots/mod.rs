//! OpenTimestamps client implementation
//!
//! Bitcoin-backed timestamping using the OpenTimestamps protocol.
//! Uses atl_core::ots for proof serialization and verification.

#![allow(dead_code)]

mod calendar;
mod client;
mod proof;
mod types;

#[allow(unused_imports)]
pub use client::OpenTimestampsClient;
#[allow(unused_imports)]
pub use types::{OtsConfig, OtsStatus};

// Re-export proof utilities for advanced use cases
#[allow(unused_imports)]
pub use proof::{detect_status, is_finalized, parse_proof, verify_timestamp};
