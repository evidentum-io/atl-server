//! Request DTOs

use serde::{Deserialize, Serialize};

/// Request body for POST /v1/anchor (JSON mode)
#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub struct AnchorJsonRequest {
    /// The payload to notarize (any JSON value)
    pub payload: serde_json::Value,

    /// Optional metadata (descriptive, included in receipt)
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,

    /// Optional client-provided correlation ID
    #[serde(default)]
    pub external_id: Option<String>,
}
