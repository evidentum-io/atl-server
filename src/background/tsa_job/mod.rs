// File: src/background/tsa_job/mod.rs

//! TSA anchoring job
//!
//! Anchors closed/pending trees with RFC 3161 timestamp authorities.
//! Uses round-robin load distribution across multiple TSA servers.

pub mod audit;
pub mod config;
pub mod job;
pub mod request;
pub mod round_robin;

// Only consumed by the separate `tsa_anchor_audit` admin binary (via the
// `atl_server` library crate), never by the `atl-server` server binary
// itself (which redeclares this whole module tree inline from
// `src/main.rs` rather than depending on the library crate) -- hence
// `unused_imports` firing specifically for that compilation target.
#[allow(unused_imports)]
pub use audit::{audit_tsa_anchors, AuditError, AuditReport, BadAnchor};
pub use config::TsaJobConfig;
pub use job::TsaAnchoringJob;
