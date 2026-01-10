//! Shared secret authentication for gRPC requests
//!
//! This module implements token-based authentication via the `x-sequencer-token` header.
//! All NODEs must include this header to authenticate with the SEQUENCER.

use tonic::{Request, Status};

/// Authentication interceptor for gRPC server
///
/// Validates that incoming requests contain a valid `x-sequencer-token` header
/// matching the expected shared secret.
pub struct AuthInterceptor {
    /// Expected token value
    expected_token: String,
}

impl AuthInterceptor {
    /// Create a new authentication interceptor
    ///
    /// # Arguments
    ///
    /// * `expected_token` - The shared secret that clients must provide
    pub fn new(expected_token: String) -> Self {
        Self { expected_token }
    }

    /// Check authentication on a request
    ///
    /// # Errors
    ///
    /// Returns `Status::Unauthenticated` if:
    /// - The `x-sequencer-token` header is missing
    /// - The token value doesn't match the expected token
    /// - The token value contains invalid characters
    pub fn check<T>(&self, req: &Request<T>) -> Result<(), Status> {
        match req.metadata().get("x-sequencer-token") {
            Some(token) => {
                let token_str = token
                    .to_str()
                    .map_err(|_| Status::unauthenticated("invalid token encoding"))?;

                if token_str == self.expected_token {
                    Ok(())
                } else {
                    Err(Status::unauthenticated("invalid token"))
                }
            }
            None => Err(Status::unauthenticated("missing x-sequencer-token header")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[test]
    fn test_auth_success() {
        let interceptor = AuthInterceptor::new("test-token".to_string());
        let mut request = Request::new(());

        request.metadata_mut().insert(
            "x-sequencer-token",
            MetadataValue::from_static("test-token"),
        );

        assert!(interceptor.check(&request).is_ok());
    }

    #[test]
    fn test_auth_missing_token() {
        let interceptor = AuthInterceptor::new("test-token".to_string());
        let request = Request::new(());

        let result = interceptor.check(&request);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("missing"));
    }

    #[test]
    fn test_auth_invalid_token() {
        let interceptor = AuthInterceptor::new("test-token".to_string());
        let mut request = Request::new(());

        request.metadata_mut().insert(
            "x-sequencer-token",
            MetadataValue::from_static("wrong-token"),
        );

        let result = interceptor.check(&request);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("invalid token"));
    }
}
