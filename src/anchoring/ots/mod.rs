//! OpenTimestamps client implementation
//!
//! Bitcoin-backed timestamping using the OpenTimestamps protocol.
//! Uses atl_core::ots for proof serialization and verification.

// Allow dead_code for public API functions that may be used by external consumers
#![allow(dead_code)]

mod async_client;
mod bitcoin;
mod calendar;
mod client;
mod proof;
mod types;

pub use async_client::{AsyncOtsClient, OtsClient};
#[allow(unused_imports)] // Used by future tasks
pub use bitcoin::get_block_timestamp;
pub use types::{OtsConfig, OtsStatus};

// Re-exported for integration tests in tests/ (not used in src/)
#[allow(unused_imports)]
pub use async_client::UpgradeResult;

// Internal use only - not part of public API
#[doc(hidden)]
#[allow(unused_imports)]
pub use client::OpenTimestampsClient;

// Test utilities (available in tests)
#[cfg(test)]
pub mod fixtures;
#[cfg(test)]
pub mod mock;
