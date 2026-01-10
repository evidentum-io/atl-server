// File: src/background/ots_poll_job/mod.rs

//! OTS poll job
//!
//! Polls pending OpenTimestamps anchors for Bitcoin confirmation.

pub mod config;
pub mod job;
pub mod upgrade;

pub use config::OtsPollJobConfig;
pub use job::OtsPollJob;
