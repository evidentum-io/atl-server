//! Unified OTS job (submit + poll)
//!
//! Phase 1: Submit closed trees to OTS calendar
//! Phase 2: Poll pending anchors for Bitcoin confirmation

pub mod config;
pub mod job;
pub mod poll;
pub mod submit;

pub use config::OtsJobConfig;
#[cfg(feature = "ots")]
pub use job::OtsJob;

// Re-exported for integration tests in tests/ (not used in src/)
#[cfg(all(feature = "sqlite", feature = "ots"))]
#[allow(unused_imports)]
pub use poll::poll_pending_anchors;
#[cfg(all(feature = "sqlite", feature = "ots"))]
#[allow(unused_imports)]
pub use submit::submit_unanchored_trees;
