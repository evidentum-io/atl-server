//! Server error types

use thiserror::Error;

/// Server error type
#[derive(Debug, Error)]
pub enum ServerError {
    // TODO: Add error variants (see ERROR-1.md)

    #[error("not implemented")]
    NotImplemented,
}

/// Server result type
pub type ServerResult<T> = Result<T, ServerError>;
