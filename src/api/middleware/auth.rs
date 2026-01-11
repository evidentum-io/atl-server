//! Bearer token authentication middleware

use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{header::AUTHORIZATION, Request, StatusCode},
    middleware::Next,
    response::Response,
};

use crate::api::state::AppState;
use crate::error::ServerError;

/// Authentication middleware - validates Bearer tokens
///
/// Only applied when access_tokens are configured.
/// Returns 401 if token is missing or invalid.
#[allow(dead_code)]
pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let tokens = state
        .access_tokens
        .as_ref()
        .expect("middleware only applied when tokens configured");

    // Get Authorization header
    let auth_header = req.headers().get(AUTHORIZATION).ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            ServerError::AuthMissing.to_string(),
        )
    })?;

    // Parse header value
    let auth_str = auth_header.to_str().map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            ServerError::AuthInvalid.to_string(),
        )
    })?;

    // Check Bearer prefix
    if !auth_str.starts_with("Bearer ") {
        return Err((
            StatusCode::UNAUTHORIZED,
            ServerError::AuthInvalid.to_string(),
        ));
    }

    // Extract and validate token
    let token = &auth_str[7..];
    if !tokens.iter().any(|t| t == token) {
        return Err((
            StatusCode::UNAUTHORIZED,
            ServerError::AuthInvalid.to_string(),
        ));
    }

    // Token valid, proceed
    Ok(next.run(req).await)
}
