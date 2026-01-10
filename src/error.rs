//! Server error types

use axum::http::StatusCode;
use thiserror::Error;

/// Main server error type
#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum ServerError {
    // ========== Entry Errors ==========
    /// Entry not found
    #[error("entry not found: {0}")]
    EntryNotFound(String),

    /// Duplicate entry
    #[error("duplicate entry: {0}")]
    DuplicateEntry(String),

    /// Entry not yet in tree
    #[error("entry not in tree: {0}")]
    EntryNotInTree(String),

    // ========== Tree Errors ==========
    /// Leaf index out of bounds
    #[error("leaf index {index} out of bounds for tree size {tree_size}")]
    LeafIndexOutOfBounds { index: u64, tree_size: u64 },

    /// Tree size mismatch
    #[error("tree size mismatch: expected {expected}, got {actual}")]
    TreeSizeMismatch { expected: u64, actual: u64 },

    // ========== Validation Errors ==========
    /// Invalid argument
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Invalid hash format
    #[error("invalid hash: {0}")]
    InvalidHash(String),

    /// Invalid signature format
    #[error("invalid signature: {0}")]
    InvalidSignature(String),

    /// Invalid UUID format
    #[error("invalid UUID: {0}")]
    InvalidUuid(String),

    /// Unsupported content type
    #[error("unsupported content type: {0}")]
    UnsupportedContentType(String),

    // ========== Authentication Errors ==========
    /// Missing authorization header
    #[error("authorization required")]
    AuthMissing,

    /// Invalid authorization token
    #[error("invalid authorization token")]
    AuthInvalid,

    // ========== Storage Errors ==========
    /// Storage operation failed
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    // ========== Anchoring Errors ==========
    /// Anchoring operation failed
    #[error("anchoring error: {0}")]
    Anchoring(AnchorError),

    // ========== Server Errors ==========
    /// Operation not supported
    #[error("not supported: {0}")]
    NotSupported(String),

    /// Service unavailable (e.g., sequencer buffer full)
    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),

    /// Configuration error
    #[error("configuration error: {0}")]
    Config(String),

    // ========== Core Error Wrapper ==========
    /// Wrapped error from atl-core
    #[error("core error: {0}")]
    Core(#[from] atl_core::AtlError),
}

/// Storage-specific errors
#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum StorageError {
    /// Database connection failed
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Query execution failed
    #[error("query failed: {0}")]
    QueryFailed(String),

    /// Transaction failed
    #[error("transaction failed: {0}")]
    TransactionFailed(String),

    /// Schema migration failed
    #[error("migration failed: {0}")]
    MigrationFailed(String),

    /// Data corruption detected
    #[error("data corruption: {0}")]
    Corruption(String),

    /// Storage not initialized
    #[error("storage not initialized")]
    NotInitialized,

    /// Storage is read-only
    #[error("storage is read-only")]
    ReadOnly,

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// SQLite error
    #[cfg(feature = "sqlite")]
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

// Re-export AnchorError from anchoring module
#[cfg(any(feature = "rfc3161", feature = "ots"))]
pub use crate::anchoring::error::AnchorError;

// Placeholder for when anchoring features are disabled
#[cfg(not(any(feature = "rfc3161", feature = "ots")))]
#[derive(Debug, Error)]
#[error("anchoring not available")]
pub struct AnchorError;

/// Server result type alias
pub type ServerResult<T> = Result<T, ServerError>;

impl ServerError {
    /// Get the HTTP status code for this error
    pub fn status_code(&self) -> StatusCode {
        match self {
            // 400 Bad Request
            ServerError::InvalidArgument(_)
            | ServerError::InvalidHash(_)
            | ServerError::InvalidSignature(_)
            | ServerError::InvalidUuid(_)
            | ServerError::LeafIndexOutOfBounds { .. }
            | ServerError::TreeSizeMismatch { .. }
            | ServerError::EntryNotInTree(_) => StatusCode::BAD_REQUEST,

            // 401 Unauthorized
            ServerError::AuthMissing | ServerError::AuthInvalid => StatusCode::UNAUTHORIZED,

            // 404 Not Found
            ServerError::EntryNotFound(_) => StatusCode::NOT_FOUND,

            // 409 Conflict
            ServerError::DuplicateEntry(_) => StatusCode::CONFLICT,

            // 415 Unsupported Media Type
            ServerError::UnsupportedContentType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,

            // 501 Not Implemented
            ServerError::NotSupported(_) => StatusCode::NOT_IMPLEMENTED,

            // 503 Service Unavailable
            ServerError::ServiceUnavailable(_)
            | ServerError::Anchoring(AnchorError::Network(_))
            | ServerError::Anchoring(AnchorError::Timeout(_))
            | ServerError::Anchoring(AnchorError::ServiceError(_)) => {
                StatusCode::SERVICE_UNAVAILABLE
            }

            // 500 Internal Server Error
            ServerError::Storage(_)
            | ServerError::Internal(_)
            | ServerError::Config(_)
            | ServerError::Core(_)
            | ServerError::Anchoring(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Get error code for API response
    pub fn error_code(&self) -> &'static str {
        match self {
            ServerError::EntryNotFound(_) => "ENTRY_NOT_FOUND",
            ServerError::DuplicateEntry(_) => "DUPLICATE_ENTRY",
            ServerError::EntryNotInTree(_) => "ENTRY_NOT_IN_TREE",
            ServerError::LeafIndexOutOfBounds { .. } => "INDEX_OUT_OF_BOUNDS",
            ServerError::TreeSizeMismatch { .. } => "TREE_SIZE_MISMATCH",
            ServerError::InvalidArgument(_) => "INVALID_ARGUMENT",
            ServerError::InvalidHash(_) => "INVALID_HASH",
            ServerError::InvalidSignature(_) => "INVALID_SIGNATURE",
            ServerError::InvalidUuid(_) => "INVALID_UUID",
            ServerError::UnsupportedContentType(_) => "UNSUPPORTED_CONTENT_TYPE",
            ServerError::AuthMissing => "AUTH_MISSING",
            ServerError::AuthInvalid => "AUTH_INVALID",
            ServerError::Storage(_) => "STORAGE_ERROR",
            ServerError::Anchoring(_) => "ANCHORING_ERROR",
            ServerError::NotSupported(_) => "NOT_SUPPORTED",
            ServerError::ServiceUnavailable(_) => "SERVICE_UNAVAILABLE",
            ServerError::Internal(_) => "INTERNAL_ERROR",
            ServerError::Config(_) => "CONFIG_ERROR",
            ServerError::Core(_) => "CORE_ERROR",
        }
    }

    /// Check if error is recoverable (client can retry)
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            ServerError::Anchoring(AnchorError::Network(_))
                | ServerError::Anchoring(AnchorError::Timeout(_))
                | ServerError::Storage(StorageError::ConnectionFailed(_))
        )
    }
}

// Conversions from external errors

impl From<serde_json::Error> for ServerError {
    fn from(e: serde_json::Error) -> Self {
        ServerError::InvalidArgument(e.to_string())
    }
}

impl From<uuid::Error> for ServerError {
    fn from(e: uuid::Error) -> Self {
        ServerError::InvalidUuid(e.to_string())
    }
}

impl From<hex::FromHexError> for ServerError {
    fn from(e: hex::FromHexError) -> Self {
        ServerError::InvalidHash(e.to_string())
    }
}

impl From<base64::DecodeError> for ServerError {
    fn from(e: base64::DecodeError) -> Self {
        ServerError::InvalidArgument(format!("base64 decode: {}", e))
    }
}

#[cfg(feature = "sqlite")]
impl From<rusqlite::Error> for ServerError {
    fn from(e: rusqlite::Error) -> Self {
        ServerError::Storage(StorageError::Sqlite(e))
    }
}

// HTTP client errors (for anchoring)
#[cfg(feature = "rfc3161")]
impl From<reqwest::Error> for AnchorError {
    fn from(e: reqwest::Error) -> Self {
        if e.is_timeout() {
            AnchorError::Timeout(30)
        } else if e.is_connect() {
            AnchorError::Network(format!("connection failed: {}", e))
        } else {
            AnchorError::Network(e.to_string())
        }
    }
}

// Conversion from AnchorError to ServerError
#[cfg(any(feature = "rfc3161", feature = "ots"))]
impl From<AnchorError> for ServerError {
    fn from(e: AnchorError) -> Self {
        ServerError::Anchoring(e)
    }
}

#[cfg(feature = "rfc3161")]
impl From<reqwest::Error> for ServerError {
    fn from(e: reqwest::Error) -> Self {
        ServerError::Anchoring(e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_status_codes() {
        assert_eq!(
            ServerError::EntryNotFound("x".into()).status_code(),
            StatusCode::NOT_FOUND
        );

        assert_eq!(
            ServerError::InvalidHash("x".into()).status_code(),
            StatusCode::BAD_REQUEST
        );

        assert_eq!(
            ServerError::Storage(StorageError::Corruption("x".into())).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    #[cfg(any(feature = "rfc3161", feature = "ots"))]
    fn test_is_recoverable() {
        assert!(ServerError::Anchoring(AnchorError::Timeout(30)).is_recoverable());
        assert!(!ServerError::EntryNotFound("x".into()).is_recoverable());
    }

    #[test]
    fn test_error_codes() {
        assert_eq!(
            ServerError::EntryNotFound("x".into()).error_code(),
            "ENTRY_NOT_FOUND"
        );
        assert_eq!(
            ServerError::InvalidHash("x".into()).error_code(),
            "INVALID_HASH"
        );
    }

    #[test]
    fn test_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ServerError>();
    }
}
