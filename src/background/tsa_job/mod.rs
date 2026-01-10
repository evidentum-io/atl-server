// File: src/background/tsa_job/mod.rs

//! TSA anchoring job
//!
//! Anchors closed/pending trees with RFC 3161 timestamp authorities.
//! Uses round-robin load distribution across multiple TSA servers.

pub mod config;
pub mod job;
pub mod request;
pub mod round_robin;

pub use config::TsaJobConfig;
pub use job::TsaAnchoringJob;
