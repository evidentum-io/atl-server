//! Anchoring-specific error types

use thiserror::Error;

/// Anchoring operation errors
#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum AnchorError {
    /// Network communication error
    #[error("network error: {0}")]
    Network(String),

    /// Service returned an error status
    #[error("service error: {0}")]
    ServiceError(String),

    /// Invalid response from service
    #[error("invalid response: {0}")]
    InvalidResponse(String),

    /// Token or proof verification failed
    #[error("token invalid: {0}")]
    TokenInvalid(String),

    /// Request timeout
    #[error("timeout after {0} seconds")]
    Timeout(u64),

    /// Untrusted TSA certificate
    #[error("untrusted TSA: {0}")]
    UntrustedTsa(String),

    /// Bitcoin block not confirmed
    #[error("block not confirmed: height {0}")]
    BlockNotConfirmed(u64),

    /// Feature not enabled
    #[error("feature not enabled: {0}")]
    FeatureNotEnabled(String),

    /// Service not configured
    #[error("not configured: {0}")]
    NotConfigured(String),

    /// ASN.1 encoding/decoding error
    #[error("ASN.1 error: {0}")]
    Asn1Error(String),
}

// Conversion from ASN.1 errors
#[cfg(feature = "rfc3161")]
impl From<rasn::error::DecodeError> for AnchorError {
    fn from(e: rasn::error::DecodeError) -> Self {
        AnchorError::Asn1Error(format!("decode error: {}", e))
    }
}

#[cfg(feature = "rfc3161")]
impl From<rasn::error::EncodeError> for AnchorError {
    fn from(e: rasn::error::EncodeError) -> Self {
        AnchorError::Asn1Error(format!("encode error: {}", e))
    }
}
