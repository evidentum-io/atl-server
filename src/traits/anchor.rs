//! Anchorer trait definition

use serde::{Deserialize, Serialize};

use crate::error::ServerResult;

/// Anchor type identifier
///
/// **IMPORTANT (IETF ATL Protocol v1.0.0):**
/// - Wire format (JSON) uses snake_case: `"rfc3161"`, `"bitcoin_ots"`
/// - Rust code uses CamelCase: `Rfc3161`, `BitcoinOts`
/// - Use `#[serde(rename = "...")]` to ensure correct JSON serialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AnchorType {
    /// RFC 3161 Time-Stamp Protocol
    #[serde(rename = "rfc3161")]
    Rfc3161,

    /// Bitcoin via OpenTimestamps
    /// Wire format: "bitcoin_ots" (NOT "ots" or "bitcoin")
    #[serde(rename = "bitcoin_ots")]
    BitcoinOts,

    /// Other (for extensibility)
    #[serde(rename = "other")]
    Other,
}

impl std::fmt::Display for AnchorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Display matches wire format per IETF ATL Protocol v1.0.0
        match self {
            AnchorType::Rfc3161 => write!(f, "rfc3161"),
            AnchorType::BitcoinOts => write!(f, "bitcoin_ots"),
            AnchorType::Other => write!(f, "other"),
        }
    }
}

/// An external timestamp anchor
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Anchor {
    /// Type of anchor
    pub anchor_type: AnchorType,

    /// Target of the anchor: "data_tree_root" or "super_root"
    pub target: String,

    /// Hash that was anchored (target_hash)
    pub anchored_hash: [u8; 32],

    /// Tree size at the time of anchoring (for TSA anchors)
    pub tree_size: u64,

    /// Super-Tree size at the time of anchoring (for OTS v2.0 anchors)
    pub super_tree_size: Option<u64>,

    /// Timestamp (nanoseconds since Unix epoch)
    pub timestamp: u64,

    /// The anchor token/proof (format depends on type)
    /// - RFC 3161: DER-encoded TimeStampToken
    /// - OTS: Serialized OTS proof (via atl_core::ots::Serializer)
    pub token: Vec<u8>,

    /// Additional metadata (type-specific)
    pub metadata: serde_json::Value,
}

/// Request to create an anchor
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AnchorRequest {
    /// Hash to anchor (typically tree root)
    pub hash: [u8; 32],

    /// Tree size at anchoring (for correlation)
    pub tree_size: u64,

    /// Optional metadata to include
    pub metadata: Option<serde_json::Value>,
}

/// Result of an anchor operation
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AnchorResult {
    /// The created anchor
    pub anchor: Anchor,

    /// Whether the anchor is finalized
    /// - RFC 3161: always true
    /// - OTS: false until Bitcoin confirmation
    pub is_final: bool,

    /// Estimated seconds until finality (if not final)
    pub estimated_finality_secs: Option<u64>,
}

/// RFC 3161 TSA configuration
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Rfc3161Config {
    /// TSA URL
    pub url: String,

    /// Request timeout in seconds
    pub timeout_secs: u64,

    /// Optional HTTP basic auth username
    pub username: Option<String>,

    /// Optional HTTP basic auth password
    pub password: Option<String>,

    /// Optional CA certificate (PEM)
    pub ca_cert: Option<String>,
}

/// OpenTimestamps configuration
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct OtsConfig {
    /// Calendar server URLs
    pub calendar_urls: Vec<String>,

    /// Request timeout in seconds
    pub timeout_secs: u64,

    /// Minimum Bitcoin confirmations for finality
    pub min_confirmations: u64,
}

/// External timestamp service client
///
/// Implementations provide integration with specific timestamping services
/// (RFC 3161 TSA, OpenTimestamps, etc.)
#[allow(dead_code)]
pub trait Anchorer: Send + Sync {
    /// Get the type of anchors this client produces
    fn anchor_type(&self) -> AnchorType;

    /// Create an anchor for the given hash
    ///
    /// This typically involves:
    /// - RFC 3161: HTTP request to TSA
    /// - OTS: HTTP request to calendar server
    fn anchor(&self, request: &AnchorRequest) -> ServerResult<AnchorResult>;

    /// Verify an existing anchor
    ///
    /// Checks that the anchor token is valid and matches the hash.
    fn verify(&self, anchor: &Anchor) -> ServerResult<()>;

    /// Check if an anchor is finalized
    ///
    /// - RFC 3161: Always true
    /// - OTS: Check if Bitcoin transaction is confirmed
    fn is_final(&self, anchor: &Anchor) -> ServerResult<bool>;

    /// Get service identifier (URL, name, etc.)
    fn service_id(&self) -> &str;
}
