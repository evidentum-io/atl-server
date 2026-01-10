//! TSA integration for batch timestamping

use tracing::warn;

use crate::anchoring::rfc3161::TsaAnchor;
use crate::error::ServerError;
use crate::traits::AppendResult;

use super::config::SequencerConfig;

/// Get TSA timestamp on the batch root hash
///
/// Tries all configured TSA URLs until one succeeds.
/// Returns error if all TSAs fail.
pub async fn get_tsa_timestamp(
    results: &[AppendResult],
    config: &SequencerConfig,
) -> Result<TsaAnchor, ServerError> {
    if config.tsa_urls.is_empty() {
        return Err(ServerError::NotSupported("TSA not configured".into()));
    }

    // Use the root hash from the first result (all have same root)
    let root_hash = results
        .first()
        .map(|r| r.tree_head.root_hash)
        .ok_or_else(|| ServerError::Internal("empty batch results".into()))?;

    // For now, we'll use a stub implementation
    // Full RFC 3161 integration will use AsyncRfc3161Client
    #[cfg(feature = "rfc3161")]
    {
        use crate::anchoring::rfc3161::{AsyncRfc3161Client, TsaClient};

        let client = AsyncRfc3161Client::new()
            .map_err(|e| ServerError::Internal(format!("Failed to create TSA client: {}", e)))?;

        // Try each TSA URL until one succeeds
        for tsa_url in &config.tsa_urls {
            match client
                .timestamp(tsa_url, &root_hash, config.tsa_timeout_ms)
                .await
            {
                Ok(response) => {
                    return Ok(TsaAnchor {
                        tsa_url: tsa_url.clone(),
                        tsa_response: response.token_der,
                        timestamp: response.timestamp,
                    });
                }
                Err(e) => {
                    warn!(tsa_url = %tsa_url, error = %e, "TSA request failed, trying next");
                    continue;
                }
            }
        }
    }

    // When rfc3161 feature is disabled, TSA is not supported
    #[cfg(not(feature = "rfc3161"))]
    {
        return Err(ServerError::NotSupported(
            "TSA requires 'rfc3161' feature to be enabled".into(),
        ));
    }

    Err(ServerError::ServiceUnavailable(
        "all TSA servers failed".into(),
    ))
}
