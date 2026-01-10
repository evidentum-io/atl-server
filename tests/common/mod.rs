//! Common test utilities and fixtures
//!
//! This module provides shared test infrastructure including:
//! - Test app setup with in-memory storage
//! - Mock builders for test data
//! - Custom assertions for receipt validation

pub mod assertions;
pub mod fixtures;

// Re-export commonly used items
pub use assertions::*;
pub use fixtures::*;

// Re-export frequently used external types for convenience
pub use axum::body::Body;
pub use axum::http::{Request, StatusCode};
pub use std::sync::Arc;
pub use tower::ServiceExt;
