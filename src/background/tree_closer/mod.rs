// File: src/background/tree_closer/mod.rs

//! Tree closer job
//!
//! Closes trees after their lifetime expires. OTS anchoring is handled by ots_job separately.

pub mod config;
pub mod job;
pub mod logic;

#[cfg(test)]
mod tests;

pub use config::TreeCloserConfig;
pub use job::TreeCloser;
