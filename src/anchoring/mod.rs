//! Anchoring implementations
//!
//! This module provides clients for external timestamp services:
//! - RFC 3161 Time-Stamp Authority (TSA) - Tier-1 Evidence
//! - OpenTimestamps (Bitcoin) - Tier-2 Evidence

pub mod error;

#[cfg(feature = "rfc3161")]
pub mod rfc3161;

#[cfg(feature = "ots")]
pub mod ots;

// Re-exported for external API and tests (not used in src/, internal code uses anchoring::error::AnchorError)
#[allow(unused_imports)]
pub use error::AnchorError;

// Re-exported for integration tests in tests/ (not used in src/)
#[cfg(feature = "rfc3161")]
#[allow(unused_imports)]
pub use rfc3161::{
    AsyncRfc3161Client, Rfc3161Client, TsaAnchor, TsaClient, TsaConfig, TsaResponse, TsaService,
};

#[cfg(feature = "ots")]
#[allow(unused_imports)]
pub use ots::{OtsConfig, OtsStatus};
