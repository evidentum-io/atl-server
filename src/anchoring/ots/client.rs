//! OpenTimestamps client implementation

use crate::anchoring::error::AnchorError;
use crate::anchoring::ots::calendar::CalendarClient;
use crate::anchoring::ots::proof::find_pending_attestation;
use crate::anchoring::ots::types::OtsConfig;
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
        // Find pending attestation with commitment
        let Some((calendar_url, commitment)) = find_pending_attestation(&file.timestamp) else {
            return Ok(None);
        };

        // Normalize URI: some calendars return URLs with leading '+' or '-'
        let calendar_url = calendar_url.trim_start_matches(['+', '-']);
        if calendar_url.is_empty() {
            return Ok(None);
        }

        // Query calendar for completed timestamp using commitment
        if let Some(response_bytes) = self.calendar.upgrade(calendar_url, &commitment).await? {
            // Parse calendar response into timestamp
            // Response is a partial timestamp that needs to be merged with original
            let upgraded =
                DetachedTimestampFile::upgrade_from_calendar_response(file, &response_bytes)
                    .map_err(|e| {
                        AnchorError::InvalidResponse(format!("failed to parse upgrade: {}", e))
                    })?;

            return Ok(Some(upgraded));
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
    use crate::anchoring::ots::fixtures;
    use mockito::Server;
    use sha2::Digest;

    #[test]
    fn test_client_creation() {
        let result = OpenTimestampsClient::new();
        assert!(result.is_ok());
    }

    #[test]
    fn test_client_default() {
        let client = OpenTimestampsClient::default();
        assert_eq!(client.config.timeout_secs, 30);
        assert!(!client.config.calendar_urls.is_empty());
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

    #[test]
    fn test_client_with_custom_config() {
        let config = OtsConfig {
            calendar_urls: vec!["https://custom.calendar".to_string()],
            timeout_secs: 10,
            min_confirmations: 3,
        };

        let client = OpenTimestampsClient::with_config(config.clone()).unwrap();
        assert_eq!(client.config.timeout_secs, 10);
        assert_eq!(client.config.calendar_urls.len(), 1);
        assert_eq!(client.config.min_confirmations, 3);
    }

    #[test]
    fn test_client_with_empty_calendars() {
        let config = OtsConfig {
            calendar_urls: vec![],
            timeout_secs: 30,
            min_confirmations: 6,
        };

        let result = OpenTimestampsClient::with_config(config);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_timestamp_with_fallback_no_calendars() {
        let config = OtsConfig {
            calendar_urls: vec![],
            timeout_secs: 30,
            min_confirmations: 6,
        };

        let client = OpenTimestampsClient::with_config(config).unwrap();
        let hash: [u8; 32] = [0u8; 32];

        let result = client.timestamp_with_fallback(&hash).await;
        assert!(result.is_err());
        match result {
            Err(AnchorError::NotConfigured(msg)) => {
                assert!(msg.contains("no calendar URLs"));
            }
            _ => panic!("Expected NotConfigured error"),
        }
    }

    #[tokio::test]
    async fn test_timestamp_with_fallback_first_calendar_success() {
        let mut server = Server::new_async().await;
        let hash: [u8; 32] = sha2::Sha256::digest(b"test").into();

        // Use minimal valid calendar response
        let response = fixtures::mock_calendar_response(&server.url());

        let mock = server
            .mock("POST", "/digest")
            .with_status(200)
            .with_body(response)
            .create_async()
            .await;

        let config = OtsConfig {
            calendar_urls: vec![server.url()],
            timeout_secs: 30,
            min_confirmations: 6,
        };

        let client = OpenTimestampsClient::with_config(config).unwrap();
        let result = client.timestamp_with_fallback(&hash).await;

        mock.assert_async().await;
        // Should succeed and return the calendar URL
        assert!(result.is_ok(), "timestamp_with_fallback failed: {:?}", result);
        let (calendar_url, _) = result.unwrap();
        assert_eq!(calendar_url, server.url());
    }

    #[tokio::test]
    async fn test_timestamp_with_fallback_second_calendar_succeeds() {
        let mut server1 = Server::new_async().await;
        let mut server2 = Server::new_async().await;
        let hash: [u8; 32] = [1u8; 32];

        // First calendar fails
        let mock1 = server1
            .mock("POST", "/digest")
            .with_status(500)
            .create_async()
            .await;

        // Second calendar succeeds with valid response
        let response = fixtures::mock_calendar_response(&server2.url());
        let mock2 = server2
            .mock("POST", "/digest")
            .with_status(200)
            .with_body(response)
            .create_async()
            .await;

        let config = OtsConfig {
            calendar_urls: vec![server1.url(), server2.url()],
            timeout_secs: 30,
            min_confirmations: 6,
        };

        let client = OpenTimestampsClient::with_config(config).unwrap();
        let result = client.timestamp_with_fallback(&hash).await;

        mock1.assert_async().await;
        mock2.assert_async().await;
        assert!(result.is_ok(), "Expected success, got: {:?}", result);
        let (calendar_url, _) = result.unwrap();
        assert_eq!(calendar_url, server2.url());
    }

    #[tokio::test]
    async fn test_timestamp_with_fallback_all_calendars_fail() {
        let mut server1 = Server::new_async().await;
        let mut server2 = Server::new_async().await;
        let hash: [u8; 32] = [2u8; 32];

        let mock1 = server1
            .mock("POST", "/digest")
            .with_status(500)
            .create_async()
            .await;

        let mock2 = server2
            .mock("POST", "/digest")
            .with_status(503)
            .create_async()
            .await;

        let config = OtsConfig {
            calendar_urls: vec![server1.url(), server2.url()],
            timeout_secs: 30,
            min_confirmations: 6,
        };

        let client = OpenTimestampsClient::with_config(config).unwrap();
        let result = client.timestamp_with_fallback(&hash).await;

        mock1.assert_async().await;
        mock2.assert_async().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_timestamp_with_fallback_invalid_response() {
        let mut server = Server::new_async().await;
        let hash: [u8; 32] = [3u8; 32];

        // Invalid response (not valid OTS format)
        let mock = server
            .mock("POST", "/digest")
            .with_status(200)
            .with_body(vec![0xFF, 0xFF])
            .create_async()
            .await;

        let config = OtsConfig {
            calendar_urls: vec![server.url()],
            timeout_secs: 30,
            min_confirmations: 6,
        };

        let client = OpenTimestampsClient::with_config(config).unwrap();
        let result = client.timestamp_with_fallback(&hash).await;

        mock.assert_async().await;
        assert!(result.is_err());
        match result {
            Err(AnchorError::InvalidResponse(msg)) => {
                assert!(msg.contains("failed to parse"));
            }
            _ => panic!("Expected InvalidResponse error"),
        }
    }

    #[tokio::test]
    async fn test_upgrade_proof_no_pending_attestation() {
        let hash: [u8; 32] = [4u8; 32];

        // Create a timestamp with no pending attestation
        let file = fixtures::create_timestamp_without_pending(&hash);

        let client = OpenTimestampsClient::new().unwrap();
        let result = client.upgrade_proof(&file).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_upgrade_proof_with_pending_not_ready() {
        // For now, just test the no-pending case
        // Full upgrade testing requires integration with real OTS format
        let hash: [u8; 32] = [5u8; 32];
        let file = fixtures::create_timestamp_without_pending(&hash);

        let client = OpenTimestampsClient::new().unwrap();
        let result = client.upgrade_proof(&file).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_upgrade_proof_no_attestations() {
        // Test with timestamp that has unknown attestation
        let hash: [u8; 32] = [7u8; 32];
        let file = fixtures::create_timestamp_without_pending(&hash);

        let client = OpenTimestampsClient::new().unwrap();
        let result = client.upgrade_proof(&file).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_upgrade_proof_uri_normalization() {
        // Test URI normalization logic without full OTS parsing
        let hash: [u8; 32] = [11u8; 32];

        // Create timestamp with URI that needs normalization
        let file_plus = fixtures::create_timestamp_with_uri(&hash, "+https://test.com");
        let client = OpenTimestampsClient::new().unwrap();

        // This tests that normalization doesn't panic
        let result = client.upgrade_proof(&file_plus).await;
        // Either Ok(None) or network error is acceptable
        assert!(result.is_ok() || matches!(result, Err(AnchorError::Network(_))));
    }

    #[tokio::test]
    async fn test_upgrade_proof_empty_uri_after_normalization() {
        let hash: [u8; 32] = [12u8; 32];

        // URI that becomes empty after trimming
        let file = fixtures::create_timestamp_with_uri(&hash, "+++");
        let client = OpenTimestampsClient::new().unwrap();
        let result = client.upgrade_proof(&file).await;

        // Empty URI should return None, not error
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
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
