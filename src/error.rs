//! Server error types

use thiserror::Error;

/// Server error type
#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum ServerError {
    // TODO: Add error variants (see ERROR-1.md)
    #[error("not implemented")]
    NotImplemented,
}

/// Server result type
#[allow(dead_code)]
pub type ServerResult<T> = Result<T, ServerError>;
