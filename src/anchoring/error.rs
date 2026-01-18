//! Anchoring-specific error types

use thiserror::Error;

/// Anchoring operation errors
#[allow(dead_code)]
#[derive(Debug, Clone, Error)]
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

    /// Failed to fetch block timestamp from all Bitcoin API providers
    #[error("Failed to fetch block timestamp for height {height}: {details}")]
    BlockTimeFetchFailed { height: u64, details: String },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_error_display() {
        let err = AnchorError::Network("connection refused".to_string());
        assert_eq!(err.to_string(), "network error: connection refused");
    }

    #[test]
    fn test_service_error_display() {
        let err = AnchorError::ServiceError("internal error".to_string());
        assert_eq!(err.to_string(), "service error: internal error");
    }

    #[test]
    fn test_invalid_response_display() {
        let err = AnchorError::InvalidResponse("malformed JSON".to_string());
        assert_eq!(err.to_string(), "invalid response: malformed JSON");
    }

    #[test]
    fn test_token_invalid_display() {
        let err = AnchorError::TokenInvalid("signature mismatch".to_string());
        assert_eq!(err.to_string(), "token invalid: signature mismatch");
    }

    #[test]
    fn test_timeout_display() {
        let err = AnchorError::Timeout(30);
        assert_eq!(err.to_string(), "timeout after 30 seconds");
    }

    #[test]
    fn test_untrusted_tsa_display() {
        let err = AnchorError::UntrustedTsa("example.com".to_string());
        assert_eq!(err.to_string(), "untrusted TSA: example.com");
    }

    #[test]
    fn test_block_not_confirmed_display() {
        let err = AnchorError::BlockNotConfirmed(123456);
        assert_eq!(err.to_string(), "block not confirmed: height 123456");
    }

    #[test]
    fn test_feature_not_enabled_display() {
        let err = AnchorError::FeatureNotEnabled("rfc3161".to_string());
        assert_eq!(err.to_string(), "feature not enabled: rfc3161");
    }

    #[test]
    fn test_not_configured_display() {
        let err = AnchorError::NotConfigured("TSA endpoint".to_string());
        assert_eq!(err.to_string(), "not configured: TSA endpoint");
    }

    #[test]
    fn test_asn1_error_display() {
        let err = AnchorError::Asn1Error("invalid DER encoding".to_string());
        assert_eq!(err.to_string(), "ASN.1 error: invalid DER encoding");
    }

    #[test]
    fn test_block_time_fetch_failed_display() {
        let err = AnchorError::BlockTimeFetchFailed {
            height: 800000,
            details: "all providers unreachable".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Failed to fetch block timestamp for height 800000: all providers unreachable"
        );
    }

    #[test]
    #[cfg(feature = "rfc3161")]
    fn test_from_decode_error() {
        use rasn::error::{BerDecodeErrorKind, DecodeError};
        let decode_err = DecodeError::from(BerDecodeErrorKind::InvalidDate {
            msg: "invalid date format".to_string(),
        });
        let anchor_err: AnchorError = decode_err.into();
        let error_msg = anchor_err.to_string();
        assert!(error_msg.contains("ASN.1 error"));
        assert!(error_msg.contains("decode error"));
    }

    #[test]
    #[cfg(feature = "rfc3161")]
    fn test_from_encode_error() {
        use rasn::error::{BerEncodeErrorKind, EncodeError};
        let encode_err =
            EncodeError::from(BerEncodeErrorKind::InvalidObjectIdentifier { oid: vec![1, 2] });
        let anchor_err: AnchorError = encode_err.into();
        let error_msg = anchor_err.to_string();
        assert!(error_msg.contains("ASN.1 error"));
        assert!(error_msg.contains("encode error"));
    }
}
