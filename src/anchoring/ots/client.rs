//! OpenTimestamps client implementation

use crate::anchoring::error::AnchorError;
use crate::anchoring::ots::calendar::CalendarClient;
use crate::anchoring::ots::proof::{detect_status, is_finalized};
use crate::anchoring::ots::types::{OtsConfig, OtsStatus};
use atl_core::ots::DetachedTimestampFile;

/// OpenTimestamps client
///
/// Uses atl_core::ots for proof building and serialization.
/// Only handles HTTP communication with calendar servers.
pub struct OpenTimestampsClient {
    /// Configuration
    config: OtsConfig,

    /// Calendar client for HTTP communication
    calendar: CalendarClient,
}

impl OpenTimestampsClient {
    /// Create a new OTS client with default calendars
    pub fn new() -> Result<Self, AnchorError> {
        Self::with_config(OtsConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(config: OtsConfig) -> Result<Self, AnchorError> {
        let calendar = CalendarClient::new(config.timeout_secs)?;

        Ok(Self { config, calendar })
    }

    /// Submit hash to calendar and build initial timestamp
    async fn submit_to_calendar(
        &self,
        hash: &[u8; 32],
        calendar_url: &str,
    ) -> Result<DetachedTimestampFile, AnchorError> {
        let response_bytes = self.calendar.submit(calendar_url, hash).await?;

        DetachedTimestampFile::from_calendar_response(*hash, &response_bytes).map_err(|e| {
            AnchorError::InvalidResponse(format!("failed to parse calendar response: {}", e))
        })
    }

    /// Try to get timestamp with fallback to multiple calendars
    pub async fn timestamp_with_fallback(
        &self,
        hash: &[u8; 32],
    ) -> Result<(String, DetachedTimestampFile), AnchorError> {
        if self.config.calendar_urls.is_empty() {
            return Err(AnchorError::NotConfigured(
                "no calendar URLs configured".into(),
            ));
        }

        let mut last_error = None;

        for calendar_url in &self.config.calendar_urls {
            match self.submit_to_calendar(hash, calendar_url).await {
                Ok(timestamp) => return Ok((calendar_url.clone(), timestamp)),
                Err(e) => {
                    tracing::warn!(
                        calendar_url = %calendar_url,
                        error = %e,
                        "Calendar request failed, trying next"
                    );
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| AnchorError::ServiceError("all calendars failed".into())))
    }

    /// Upgrade a pending proof by querying calendar
    pub async fn upgrade_proof(
        &self,
        file: &DetachedTimestampFile,
    ) -> Result<Option<DetachedTimestampFile>, AnchorError> {
        let status = detect_status(&file.timestamp);

        if is_finalized(&status) {
            return Ok(None);
        }

        if let OtsStatus::Pending { calendar_url } = status {
            if calendar_url.is_empty() {
                return Ok(None);
            }

            let current_bytes = file
                .to_bytes()
                .map_err(|e| AnchorError::InvalidResponse(e.to_string()))?;

            if let Some(upgraded_bytes) =
                self.calendar.upgrade(&calendar_url, &current_bytes).await?
            {
                let upgraded = DetachedTimestampFile::from_bytes(&upgraded_bytes)
                    .map_err(|e| AnchorError::InvalidResponse(e.to_string()))?;

                return Ok(Some(upgraded));
            }
        }

        Ok(None)
    }
}

impl Default for OpenTimestampsClient {
    fn default() -> Self {
        Self::new().expect("failed to create default OTS client")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;

    #[test]
    fn test_client_creation() {
        let result = OpenTimestampsClient::new();
        assert!(result.is_ok());
    }

    #[test]
    fn test_client_creation_in_tokio() {
        // Verify creation works inside tokio runtime
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let result = OpenTimestampsClient::new();
            assert!(result.is_ok());
        });
    }

    #[tokio::test]
    #[ignore] // Requires network
    async fn test_ots_submit() {
        let client = OpenTimestampsClient::new().unwrap();
        let hash = sha2::Sha256::digest(b"test ots data").into();

        let result = client.timestamp_with_fallback(&hash).await;
        assert!(result.is_ok());
    }
}
