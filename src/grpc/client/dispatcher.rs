//! gRPC dispatcher for NODE mode
//!
//! This module implements the GrpcDispatcher struct that implements the
//! SequencerClient trait by forwarding requests to a remote SEQUENCER.

use std::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use tracing::{info, warn};

use crate::error::{ServerError, ServerResult};
use crate::grpc::client::config::GrpcClientConfig;
use crate::grpc::proto::sequencer_service_client::SequencerServiceClient;

/// gRPC dispatcher for NODE mode
pub struct GrpcDispatcher {
    /// gRPC client
    client: SequencerServiceClient<Channel>,

    /// Configuration
    config: GrpcClientConfig,
}

impl GrpcDispatcher {
    /// Create a new GrpcDispatcher with the given configuration
    ///
    /// # Arguments
    ///
    /// * `config` - Client configuration including URL and authentication
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - The sequencer URL is invalid
    /// - Cannot connect to the sequencer
    pub async fn new(config: GrpcClientConfig) -> ServerResult<Self> {
        let endpoint = Endpoint::from_shared(config.sequencer_url.clone())
            .map_err(|e| ServerError::InvalidArgument(format!("invalid sequencer URL: {e}")))?
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .http2_keep_alive_interval(Duration::from_secs(config.keep_alive_secs))
            .keep_alive_timeout(Duration::from_secs(config.keep_alive_secs))
            .keep_alive_while_idle(true)
            .concurrency_limit(config.concurrency_limit);

        let channel = endpoint.connect().await.map_err(|e| {
            ServerError::ServiceUnavailable(format!("failed to connect to sequencer: {e}"))
        })?;

        info!(url = %config.sequencer_url, "Connected to Sequencer");

        Ok(Self {
            client: SequencerServiceClient::new(channel),
            config,
        })
    }

    /// Add authentication token to request if configured
    pub(super) fn add_auth<T>(&self, mut request: Request<T>) -> Request<T> {
        if let Some(ref token) = self.config.token {
            if let Ok(value) = MetadataValue::try_from(token) {
                request.metadata_mut().insert("x-sequencer-token", value);
            }
        }
        request
    }

    /// Execute with retry logic
    ///
    /// Only retries on transient errors (Unavailable, DeadlineExceeded, Aborted).
    /// Other errors are returned immediately.
    pub(super) async fn with_retry<T, F, Fut>(&self, operation: F) -> ServerResult<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        let mut attempt = 0;
        let mut delay_ms = self.config.retry_backoff_ms;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(status) => {
                    attempt += 1;

                    // Only retry on transient errors
                    let should_retry = matches!(
                        status.code(),
                        tonic::Code::Unavailable
                            | tonic::Code::DeadlineExceeded
                            | tonic::Code::Aborted
                    );

                    if !should_retry || attempt >= self.config.retry_count {
                        return Err(grpc_status_to_error(status));
                    }

                    warn!(
                        attempt = attempt,
                        max_attempts = self.config.retry_count,
                        delay_ms = delay_ms,
                        code = ?status.code(),
                        "gRPC request failed, retrying"
                    );

                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    delay_ms *= 2; // Exponential backoff
                }
            }
        }
    }

    /// Get a clone of the client for use in operations
    pub(super) fn client(&self) -> SequencerServiceClient<Channel> {
        self.client.clone()
    }
}

/// Convert gRPC Status to ServerError
pub(super) fn grpc_status_to_error(status: Status) -> ServerError {
    match status.code() {
        tonic::Code::NotFound => ServerError::EntryNotFound(status.message().to_string()),
        tonic::Code::InvalidArgument => ServerError::InvalidArgument(status.message().to_string()),
        tonic::Code::Unauthenticated => ServerError::AuthInvalid,
        tonic::Code::Unavailable => ServerError::ServiceUnavailable(status.message().to_string()),
        _ => ServerError::Internal(status.message().to_string()),
    }
}
