//! API error response types

use crate::error::ServerError;
use axum::{
    Json,
    response::{IntoResponse, Response},
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
