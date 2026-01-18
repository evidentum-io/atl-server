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

    /// Super-Tree not initialized
    #[error("Super-Tree not initialized - no trees have been closed yet")]
    SuperTreeNotInitialized,

    /// Entry's tree not yet in Super-Tree
    #[error("Entry's tree is still active, not yet in Super-Tree")]
    TreeNotClosed,

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
    /// Storage operation failed (NOT NotFound - that becomes EntryNotFound)
    #[error("storage error: {0}")]
    Storage(StorageError),

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

    /// SQLite database error
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// Database operation failed
    #[error("Database error: {0}")]
    Database(String),

    /// Entity not found
    #[error("Not found: {0}")]
    NotFound(String),

    /// Invalid range specified
    #[error("Invalid range: {0}")]
    InvalidRange(String),

    /// Invalid index specified
    #[error("Invalid index: {0}")]
    InvalidIndex(String),
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
            | ServerError::EntryNotInTree(_)
            | ServerError::SuperTreeNotInitialized
            | ServerError::TreeNotClosed => StatusCode::BAD_REQUEST,

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
            ServerError::SuperTreeNotInitialized => "SUPER_TREE_NOT_INITIALIZED",
            ServerError::TreeNotClosed => "TREE_NOT_CLOSED",
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

impl From<rusqlite::Error> for ServerError {
    fn from(e: rusqlite::Error) -> Self {
        ServerError::Storage(StorageError::Sqlite(e))
    }
}

impl From<StorageError> for ServerError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::NotFound(msg) => ServerError::EntryNotFound(msg),
            other => ServerError::Storage(other),
        }
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

    // ========== Status Code Tests ==========

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
    fn test_status_code_bad_request_variants() {
        assert_eq!(
            ServerError::InvalidArgument("test".into()).status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ServerError::InvalidSignature("test".into()).status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ServerError::InvalidUuid("test".into()).status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ServerError::LeafIndexOutOfBounds {
                index: 10,
                tree_size: 5
            }
            .status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ServerError::TreeSizeMismatch {
                expected: 100,
                actual: 50
            }
            .status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ServerError::EntryNotInTree("test".into()).status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ServerError::SuperTreeNotInitialized.status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ServerError::TreeNotClosed.status_code(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn test_status_code_unauthorized_variants() {
        assert_eq!(
            ServerError::AuthMissing.status_code(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            ServerError::AuthInvalid.status_code(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn test_status_code_conflict() {
        assert_eq!(
            ServerError::DuplicateEntry("test".into()).status_code(),
            StatusCode::CONFLICT
        );
    }

    #[test]
    fn test_status_code_unsupported_media_type() {
        assert_eq!(
            ServerError::UnsupportedContentType("test".into()).status_code(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
    }

    #[test]
    fn test_status_code_not_implemented() {
        assert_eq!(
            ServerError::NotSupported("test".into()).status_code(),
            StatusCode::NOT_IMPLEMENTED
        );
    }

    #[test]
    #[cfg(any(feature = "rfc3161", feature = "ots"))]
    fn test_status_code_service_unavailable_anchoring() {
        assert_eq!(
            ServerError::ServiceUnavailable("test".into()).status_code(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            ServerError::Anchoring(AnchorError::Network("test".into())).status_code(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            ServerError::Anchoring(AnchorError::Timeout(30)).status_code(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            ServerError::Anchoring(AnchorError::ServiceError("test".into())).status_code(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn test_status_code_internal_server_error_variants() {
        assert_eq!(
            ServerError::Internal("test".into()).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            ServerError::Config("test".into()).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    // ========== Error Code Tests ==========

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
    fn test_all_error_code_variants() {
        assert_eq!(
            ServerError::DuplicateEntry("x".into()).error_code(),
            "DUPLICATE_ENTRY"
        );
        assert_eq!(
            ServerError::EntryNotInTree("x".into()).error_code(),
            "ENTRY_NOT_IN_TREE"
        );
        assert_eq!(
            ServerError::LeafIndexOutOfBounds {
                index: 10,
                tree_size: 5
            }
            .error_code(),
            "INDEX_OUT_OF_BOUNDS"
        );
        assert_eq!(
            ServerError::TreeSizeMismatch {
                expected: 100,
                actual: 50
            }
            .error_code(),
            "TREE_SIZE_MISMATCH"
        );
        assert_eq!(
            ServerError::SuperTreeNotInitialized.error_code(),
            "SUPER_TREE_NOT_INITIALIZED"
        );
        assert_eq!(ServerError::TreeNotClosed.error_code(), "TREE_NOT_CLOSED");
        assert_eq!(
            ServerError::InvalidArgument("x".into()).error_code(),
            "INVALID_ARGUMENT"
        );
        assert_eq!(
            ServerError::InvalidSignature("x".into()).error_code(),
            "INVALID_SIGNATURE"
        );
        assert_eq!(
            ServerError::InvalidUuid("x".into()).error_code(),
            "INVALID_UUID"
        );
        assert_eq!(
            ServerError::UnsupportedContentType("x".into()).error_code(),
            "UNSUPPORTED_CONTENT_TYPE"
        );
        assert_eq!(ServerError::AuthMissing.error_code(), "AUTH_MISSING");
        assert_eq!(ServerError::AuthInvalid.error_code(), "AUTH_INVALID");
        assert_eq!(
            ServerError::Storage(StorageError::Corruption("x".into())).error_code(),
            "STORAGE_ERROR"
        );
        assert_eq!(
            ServerError::NotSupported("x".into()).error_code(),
            "NOT_SUPPORTED"
        );
        assert_eq!(
            ServerError::ServiceUnavailable("x".into()).error_code(),
            "SERVICE_UNAVAILABLE"
        );
        assert_eq!(
            ServerError::Internal("x".into()).error_code(),
            "INTERNAL_ERROR"
        );
        assert_eq!(ServerError::Config("x".into()).error_code(), "CONFIG_ERROR");
    }

    #[test]
    #[cfg(any(feature = "rfc3161", feature = "ots"))]
    fn test_error_code_anchoring() {
        assert_eq!(
            ServerError::Anchoring(AnchorError::Timeout(30)).error_code(),
            "ANCHORING_ERROR"
        );
    }

    // ========== Recoverability Tests ==========

    #[test]
    #[cfg(any(feature = "rfc3161", feature = "ots"))]
    fn test_is_recoverable() {
        assert!(ServerError::Anchoring(AnchorError::Timeout(30)).is_recoverable());
        assert!(!ServerError::EntryNotFound("x".into()).is_recoverable());
    }

    #[test]
    #[cfg(any(feature = "rfc3161", feature = "ots"))]
    fn test_is_recoverable_anchoring_variants() {
        assert!(ServerError::Anchoring(AnchorError::Network("test".into())).is_recoverable());
        assert!(ServerError::Anchoring(AnchorError::Timeout(60)).is_recoverable());
        assert!(!ServerError::Anchoring(AnchorError::TokenInvalid("test".into())).is_recoverable());
    }

    #[test]
    fn test_is_recoverable_storage_variants() {
        assert!(
            ServerError::Storage(StorageError::ConnectionFailed("test".into())).is_recoverable()
        );
        assert!(!ServerError::Storage(StorageError::Corruption("test".into())).is_recoverable());
        assert!(!ServerError::Storage(StorageError::NotInitialized).is_recoverable());
    }

    #[test]
    fn test_is_recoverable_non_recoverable_errors() {
        assert!(!ServerError::InvalidHash("x".into()).is_recoverable());
        assert!(!ServerError::DuplicateEntry("x".into()).is_recoverable());
        assert!(!ServerError::AuthInvalid.is_recoverable());
        assert!(!ServerError::Internal("x".into()).is_recoverable());
    }

    // ========== Trait Tests ==========

    #[test]
    fn test_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ServerError>();
    }

    // ========== Conversion Tests ==========

    #[test]
    fn test_storage_error_not_found_converts_to_entry_not_found() {
        let storage_err = StorageError::NotFound("test entry".into());
        let server_err: ServerError = storage_err.into();

        assert!(matches!(server_err, ServerError::EntryNotFound(_)));
        assert_eq!(server_err.status_code(), StatusCode::NOT_FOUND);
        assert_eq!(server_err.error_code(), "ENTRY_NOT_FOUND");
    }

    #[test]
    fn test_storage_error_database_converts_to_storage() {
        let storage_err = StorageError::Database("connection lost".into());
        let server_err: ServerError = storage_err.into();

        assert!(matches!(server_err, ServerError::Storage(_)));
        assert_eq!(server_err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(server_err.error_code(), "STORAGE_ERROR");
    }

    #[test]
    fn test_storage_error_io_converts_to_storage() {
        let io_err = std::io::Error::other("disk full");
        let storage_err = StorageError::Io(io_err);
        let server_err: ServerError = storage_err.into();

        assert!(matches!(
            server_err,
            ServerError::Storage(StorageError::Io(_))
        ));
        assert_eq!(server_err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_serde_json_error_conversion() {
        let json_err = serde_json::from_str::<i32>("invalid json").unwrap_err();
        let server_err: ServerError = json_err.into();

        assert!(matches!(server_err, ServerError::InvalidArgument(_)));
        assert_eq!(server_err.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_uuid_error_conversion() {
        let uuid_err = uuid::Uuid::parse_str("invalid-uuid").unwrap_err();
        let server_err: ServerError = uuid_err.into();

        assert!(matches!(server_err, ServerError::InvalidUuid(_)));
        assert_eq!(server_err.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_hex_error_conversion() {
        let hex_err = hex::decode("invalid_hex").unwrap_err();
        let server_err: ServerError = hex_err.into();

        assert!(matches!(server_err, ServerError::InvalidHash(_)));
        assert_eq!(server_err.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_base64_error_conversion() {
        use base64::Engine;
        let b64_err = base64::engine::general_purpose::STANDARD
            .decode("!@#$%")
            .unwrap_err();
        let server_err: ServerError = b64_err.into();

        assert!(matches!(server_err, ServerError::InvalidArgument(_)));
        assert_eq!(server_err.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_rusqlite_error_conversion() {
        let sql_err = rusqlite::Error::InvalidQuery;
        let server_err: ServerError = sql_err.into();

        assert!(matches!(
            server_err,
            ServerError::Storage(StorageError::Sqlite(_))
        ));
    }

    // ========== StorageError Display Tests ==========

    #[test]
    fn test_storage_error_display() {
        assert_eq!(
            StorageError::ConnectionFailed("timeout".into()).to_string(),
            "connection failed: timeout"
        );
        assert_eq!(
            StorageError::QueryFailed("syntax error".into()).to_string(),
            "query failed: syntax error"
        );
        assert_eq!(
            StorageError::TransactionFailed("deadlock".into()).to_string(),
            "transaction failed: deadlock"
        );
        assert_eq!(
            StorageError::MigrationFailed("v2 to v3".into()).to_string(),
            "migration failed: v2 to v3"
        );
        assert_eq!(
            StorageError::Corruption("checksum mismatch".into()).to_string(),
            "data corruption: checksum mismatch"
        );
        assert_eq!(
            StorageError::NotInitialized.to_string(),
            "storage not initialized"
        );
        assert_eq!(StorageError::ReadOnly.to_string(), "storage is read-only");
        assert_eq!(
            StorageError::Database("connection lost".into()).to_string(),
            "Database error: connection lost"
        );
        assert_eq!(
            StorageError::NotFound("entry".into()).to_string(),
            "Not found: entry"
        );
        assert_eq!(
            StorageError::InvalidRange("0..10".into()).to_string(),
            "Invalid range: 0..10"
        );
        assert_eq!(
            StorageError::InvalidIndex("5".into()).to_string(),
            "Invalid index: 5"
        );
    }

    // ========== ServerError Display Tests ==========

    #[test]
    fn test_server_error_display() {
        assert_eq!(
            ServerError::EntryNotFound("abc".into()).to_string(),
            "entry not found: abc"
        );
        assert_eq!(
            ServerError::DuplicateEntry("abc".into()).to_string(),
            "duplicate entry: abc"
        );
        assert_eq!(
            ServerError::EntryNotInTree("abc".into()).to_string(),
            "entry not in tree: abc"
        );
        assert_eq!(
            ServerError::LeafIndexOutOfBounds {
                index: 10,
                tree_size: 5
            }
            .to_string(),
            "leaf index 10 out of bounds for tree size 5"
        );
        assert_eq!(
            ServerError::TreeSizeMismatch {
                expected: 100,
                actual: 50
            }
            .to_string(),
            "tree size mismatch: expected 100, got 50"
        );
        assert_eq!(
            ServerError::SuperTreeNotInitialized.to_string(),
            "Super-Tree not initialized - no trees have been closed yet"
        );
        assert_eq!(
            ServerError::TreeNotClosed.to_string(),
            "Entry's tree is still active, not yet in Super-Tree"
        );
        assert_eq!(
            ServerError::InvalidArgument("test".into()).to_string(),
            "invalid argument: test"
        );
        assert_eq!(
            ServerError::InvalidHash("test".into()).to_string(),
            "invalid hash: test"
        );
        assert_eq!(
            ServerError::InvalidSignature("test".into()).to_string(),
            "invalid signature: test"
        );
        assert_eq!(
            ServerError::InvalidUuid("test".into()).to_string(),
            "invalid UUID: test"
        );
        assert_eq!(
            ServerError::UnsupportedContentType("test".into()).to_string(),
            "unsupported content type: test"
        );
        assert_eq!(
            ServerError::AuthMissing.to_string(),
            "authorization required"
        );
        assert_eq!(
            ServerError::AuthInvalid.to_string(),
            "invalid authorization token"
        );
        assert_eq!(
            ServerError::NotSupported("test".into()).to_string(),
            "not supported: test"
        );
        assert_eq!(
            ServerError::ServiceUnavailable("test".into()).to_string(),
            "service unavailable: test"
        );
        assert_eq!(
            ServerError::Internal("test".into()).to_string(),
            "internal error: test"
        );
        assert_eq!(
            ServerError::Config("test".into()).to_string(),
            "configuration error: test"
        );
    }

    // ========== Storage Error Conversion Tests ==========

    #[test]
    fn test_storage_error_other_variants_convert_to_storage() {
        let variants = vec![
            StorageError::QueryFailed("test".into()),
            StorageError::TransactionFailed("test".into()),
            StorageError::MigrationFailed("test".into()),
            StorageError::Corruption("test".into()),
            StorageError::NotInitialized,
            StorageError::ReadOnly,
            StorageError::Database("test".into()),
            StorageError::InvalidRange("test".into()),
            StorageError::InvalidIndex("test".into()),
        ];

        for storage_err in variants {
            let server_err: ServerError = storage_err.into();
            assert!(matches!(server_err, ServerError::Storage(_)));
        }
    }
}
