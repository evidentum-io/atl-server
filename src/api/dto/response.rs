//! Response DTOs

use serde::Serialize;

/// TSA anchor in receipt.anchors (added by background job)
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize)]
pub struct TsaAnchorDto {
    /// Always "rfc3161"
    #[serde(rename = "type")]
    pub anchor_type: String,

    /// TSA URL
    pub tsa_url: String,

    /// ISO 8601 timestamp
    pub timestamp: String,

    /// Raw RFC 3161 TimeStampResp in base64
    pub token_der: String,
}

/// Bitcoin OTS anchor in receipt.anchors
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize)]
pub struct BitcoinAnchorDto {
    /// Always "bitcoin_ots"
    #[serde(rename = "type")]
    pub anchor_type: String,

    /// ISO 8601 timestamp (same as bitcoin_block_time)
    pub timestamp: String,

    /// Bitcoin block height
    pub bitcoin_block_height: u64,

    /// ISO 8601 timestamp of Bitcoin block
    pub bitcoin_block_time: String,

    /// Tree size at the time of Bitcoin anchoring
    pub tree_size: u64,

    /// OpenTimestamps proof in base64
    pub ots_proof: String,
}

/// Consistency proof (in receipt.proof when anchors exist)
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize)]
pub struct ConsistencyProofDto {
    /// From tree size
    pub from_tree_size: u64,

    /// Consistency proof path
    pub path: Vec<String>,
}

/// Health check response
#[allow(dead_code)]
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    /// "healthy" or "unhealthy"
    pub status: String,

    /// Server mode: "NODE", "SEQUENCER", "STANDALONE"
    pub mode: String,

    /// NODE only: true if gRPC connection to Sequencer is alive
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequencer_connected: Option<bool>,

    /// Error message if unhealthy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Error response body
#[allow(dead_code)]
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Human-readable error message
    pub error: String,

    /// Machine-readable error code
    pub code: String,

    /// Additional details (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}
