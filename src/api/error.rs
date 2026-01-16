//! API error response types

use crate::error::ServerError;
use axum::{
    response::{IntoResponse, Response},
    Json,
};

/// API error response body
#[derive(Debug, serde::Serialize)]
pub struct ErrorResponse {
    /// Human-readable error message
    pub error: String,

    /// Machine-readable error code
    pub code: String,

    /// Whether the error is recoverable (client can retry)
    pub recoverable: bool,

    /// Additional error details (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let status = self.status_code();

        let body = ErrorResponse {
            error: self.to_string(),
            code: self.error_code().to_string(),
            recoverable: self.is_recoverable(),
            details: None,
        };

        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{AnchorError, StorageError};
    use axum::{body::to_bytes, http::StatusCode};

    #[test]
    fn test_error_response_serialization_without_details() {
        let response = ErrorResponse {
            error: "test error message".to_string(),
            code: "TEST_ERROR".to_string(),
            recoverable: false,
            details: None,
        };

        let json = serde_json::to_value(&response).unwrap();

        assert_eq!(json["error"], "test error message");
        assert_eq!(json["code"], "TEST_ERROR");
        assert_eq!(json["recoverable"], false);
        assert!(json.get("details").is_none());
    }

    #[test]
    fn test_error_response_serialization_with_details() {
        let details = serde_json::json!({
            "field": "value",
            "nested": {
                "key": "data"
            }
        });

        let response = ErrorResponse {
            error: "validation failed".to_string(),
            code: "VALIDATION_ERROR".to_string(),
            recoverable: true,
            details: Some(details.clone()),
        };

        let json = serde_json::to_value(&response).unwrap();

        assert_eq!(json["error"], "validation failed");
        assert_eq!(json["code"], "VALIDATION_ERROR");
        assert_eq!(json["recoverable"], true);
        assert_eq!(json["details"], details);
    }

    #[tokio::test]
    async fn test_into_response_not_found() {
        let error = ServerError::EntryNotFound("test-id".to_string());
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "entry not found: test-id");
        assert_eq!(json["code"], "ENTRY_NOT_FOUND");
        assert_eq!(json["recoverable"], false);
        assert!(json.get("details").is_none());
    }

    #[tokio::test]
    async fn test_into_response_invalid_hash() {
        let error = ServerError::InvalidHash("bad hex".to_string());
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "invalid hash: bad hex");
        assert_eq!(json["code"], "INVALID_HASH");
        assert_eq!(json["recoverable"], false);
    }

    #[tokio::test]
    async fn test_into_response_duplicate_entry() {
        let error = ServerError::DuplicateEntry("entry-123".to_string());
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "duplicate entry: entry-123");
        assert_eq!(json["code"], "DUPLICATE_ENTRY");
        assert_eq!(json["recoverable"], false);
    }

    #[tokio::test]
    async fn test_into_response_auth_missing() {
        let error = ServerError::AuthMissing;
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "authorization required");
        assert_eq!(json["code"], "AUTH_MISSING");
        assert_eq!(json["recoverable"], false);
    }

    #[tokio::test]
    async fn test_into_response_unsupported_content_type() {
        let error = ServerError::UnsupportedContentType("text/plain".to_string());
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "unsupported content type: text/plain");
        assert_eq!(json["code"], "UNSUPPORTED_CONTENT_TYPE");
    }

    #[tokio::test]
    async fn test_into_response_not_supported() {
        let error = ServerError::NotSupported("feature-x".to_string());
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "not supported: feature-x");
        assert_eq!(json["code"], "NOT_SUPPORTED");
    }

    #[tokio::test]
    async fn test_into_response_storage_error() {
        let error = ServerError::Storage(StorageError::Database("connection lost".to_string()));
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            json["error"],
            "storage error: Database error: connection lost"
        );
        assert_eq!(json["code"], "STORAGE_ERROR");
        assert_eq!(json["recoverable"], false);
    }

    #[tokio::test]
    #[cfg(any(feature = "rfc3161", feature = "ots"))]
    async fn test_into_response_recoverable_anchoring_error() {
        let error = ServerError::Anchoring(AnchorError::Network("timeout".to_string()));
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "anchoring error: network error: timeout");
        assert_eq!(json["code"], "ANCHORING_ERROR");
        assert_eq!(json["recoverable"], true);
    }

    #[tokio::test]
    async fn test_into_response_service_unavailable() {
        let error = ServerError::ServiceUnavailable("buffer full".to_string());
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "service unavailable: buffer full");
        assert_eq!(json["code"], "SERVICE_UNAVAILABLE");
        assert_eq!(json["recoverable"], false);
    }

    #[tokio::test]
    async fn test_into_response_internal_error() {
        let error = ServerError::Internal("unexpected state".to_string());
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "internal error: unexpected state");
        assert_eq!(json["code"], "INTERNAL_ERROR");
        assert_eq!(json["recoverable"], false);
    }

    #[tokio::test]
    async fn test_into_response_config_error() {
        let error = ServerError::Config("invalid port".to_string());
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "configuration error: invalid port");
        assert_eq!(json["code"], "CONFIG_ERROR");
    }

    #[tokio::test]
    async fn test_into_response_leaf_index_out_of_bounds() {
        let error = ServerError::LeafIndexOutOfBounds {
            index: 100,
            tree_size: 50,
        };
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            json["error"],
            "leaf index 100 out of bounds for tree size 50"
        );
        assert_eq!(json["code"], "INDEX_OUT_OF_BOUNDS");
    }

    #[tokio::test]
    async fn test_into_response_storage_recoverable() {
        let error = ServerError::Storage(StorageError::ConnectionFailed("retry".to_string()));
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            json["error"],
            "storage error: connection failed: retry"
        );
        assert_eq!(json["recoverable"], true);
    }
}
