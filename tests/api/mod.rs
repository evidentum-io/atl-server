//! API integration tests
//!
//! Tests for HTTP endpoints:
//! - GET /health (NODE/SEQUENCER modes only)
//! - POST /v1/anchor (all modes)
//! - GET /v1/anchor/{id} (all modes)
//! - Authentication middleware

pub mod anchor_test;
pub mod auth_test;
pub mod health_test;
