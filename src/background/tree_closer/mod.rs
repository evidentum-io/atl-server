// File: src/background/tree_closer/mod.rs

//! Tree closer job
//!
//! Closes trees after their lifetime expires and submits them to OTS.

pub mod config;
pub mod job;
pub mod logic;

pub use config::TreeCloserConfig;
pub use job::TreeCloser;
