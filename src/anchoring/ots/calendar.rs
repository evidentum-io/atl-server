//! Calendar server communication

use crate::anchoring::error::AnchorError;
use std::time::Duration;

/// Calendar client for HTTP communication with OTS servers
pub struct CalendarClient {
    /// HTTP client
    client: reqwest::Client,

    /// Timeout
    timeout: Duration,
}

impl CalendarClient {
    /// Create a new calendar client
    pub fn new(timeout_secs: u64) -> Result<Self, AnchorError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .map_err(|e| AnchorError::Network(e.to_string()))?;

        Ok(Self {
            client,
            timeout: Duration::from_secs(timeout_secs),
        })
    }

    /// Submit a hash to calendar and get initial timestamp
    pub async fn submit(
        &self,
        calendar_url: &str,
        hash: &[u8; 32],
    ) -> Result<Vec<u8>, AnchorError> {
        tracing::debug!(calendar_url = %calendar_url, "Submitting to calendar");

        let url = format!("{}/digest", calendar_url);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/x-opentimestamps")
            .body(hash.to_vec())
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    AnchorError::Timeout(self.timeout.as_secs())
                } else {
                    AnchorError::Network(e.to_string())
                }
            })?;

        if !response.status().is_success() {
            return Err(AnchorError::ServiceError(format!(
                "Calendar returned status {}",
                response.status()
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| AnchorError::Network(e.to_string()))?
            .to_vec();

        tracing::info!(calendar_url = %calendar_url, "Timestamp submitted successfully");

        Ok(bytes)
    }

    /// Upgrade a pending timestamp by querying calendar
    ///
    /// Sends GET request to `{calendar_url}/timestamp/{hex_commitment}`
    pub async fn upgrade(
        &self,
        calendar_url: &str,
        commitment: &[u8],
    ) -> Result<Option<Vec<u8>>, AnchorError> {
        let hex_commitment = hex::encode(commitment);
        let url = format!("{}/timestamp/{}", calendar_url, hex_commitment);

        tracing::debug!(url = %url, "Fetching timestamp from calendar");

        let response = self.client.get(&url).send().await.map_err(|e| {
            if e.is_timeout() {
                AnchorError::Timeout(self.timeout.as_secs())
            } else {
                AnchorError::Network(e.to_string())
            }
        })?;

        // 404 = timestamp not yet available
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            tracing::debug!("Timestamp not yet available at calendar");
            return Ok(None);
        }

        if !response.status().is_success() {
            return Err(AnchorError::ServiceError(format!(
                "Calendar returned status {}",
                response.status()
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| AnchorError::Network(e.to_string()))?
            .to_vec();

        tracing::info!(calendar_url = %calendar_url, "Timestamp upgraded successfully");

        Ok(Some(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;
    use sha2::Digest;

    #[test]
    fn test_calendar_client_creation() {
        let result = CalendarClient::new(30);
        assert!(result.is_ok());
    }

    #[test]
    fn test_calendar_client_creation_stores_timeout() {
        let client = CalendarClient::new(15).unwrap();
        assert_eq!(client.timeout, Duration::from_secs(15));
    }

    #[test]
    fn test_calendar_client_creation_different_timeouts() {
        let client_5 = CalendarClient::new(5).unwrap();
        assert_eq!(client_5.timeout.as_secs(), 5);

        let client_60 = CalendarClient::new(60).unwrap();
        assert_eq!(client_60.timeout.as_secs(), 60);
    }

    #[tokio::test]
    async fn test_calendar_client_creation_in_async() {
        let result = CalendarClient::new(30);
        assert!(
            result.is_ok(),
            "CalendarClient::new() must work in async context"
        );
    }

    #[tokio::test]
    async fn test_submit_success() {
        let mut server = Server::new_async().await;
        let hash: [u8; 32] = sha2::Sha256::digest(b"test data").into();

        let mock = server
            .mock("POST", "/digest")
            .match_header("Content-Type", "application/x-opentimestamps")
            .with_status(200)
            .with_body(vec![0x00, 0x4f, 0x54, 0x01])
            .create_async()
            .await;

        let client = CalendarClient::new(30).unwrap();
        let result = client.submit(&server.url(), &hash).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response, vec![0x00, 0x4f, 0x54, 0x01]);
    }

    #[tokio::test]
    async fn test_submit_non_success_status() {
        let mut server = Server::new_async().await;
        let hash: [u8; 32] = [0u8; 32];

        let mock = server
            .mock("POST", "/digest")
            .with_status(500)
            .create_async()
            .await;

        let client = CalendarClient::new(30).unwrap();
        let result = client.submit(&server.url(), &hash).await;

        mock.assert_async().await;
        assert!(result.is_err());
        match result {
            Err(AnchorError::ServiceError(msg)) => {
                assert!(msg.contains("500"));
            }
            _ => panic!("Expected ServiceError"),
        }
    }

    #[tokio::test]
    async fn test_submit_404_status() {
        let mut server = Server::new_async().await;
        let hash: [u8; 32] = [0u8; 32];

        let mock = server
            .mock("POST", "/digest")
            .with_status(404)
            .create_async()
            .await;

        let client = CalendarClient::new(30).unwrap();
        let result = client.submit(&server.url(), &hash).await;

        mock.assert_async().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(AnchorError::ServiceError(_))));
    }

    #[tokio::test]
    async fn test_submit_network_error() {
        let client = CalendarClient::new(1).unwrap();
        let hash: [u8; 32] = [0u8; 32];

        // Invalid URL to trigger network error
        let result = client.submit("http://invalid.local:9999", &hash).await;

        assert!(result.is_err());
        // Network error or timeout are both acceptable
        assert!(matches!(
            result,
            Err(AnchorError::Network(_)) | Err(AnchorError::Timeout(_))
        ));
    }

    #[tokio::test]
    async fn test_submit_empty_response() {
        let mut server = Server::new_async().await;
        let hash: [u8; 32] = [0u8; 32];

        // Mock successful status with empty body
        let mock = server
            .mock("POST", "/digest")
            .with_status(200)
            .with_body(vec![])
            .create_async()
            .await;

        let client = CalendarClient::new(30).unwrap();
        let result = client.submit(&server.url(), &hash).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response, Vec::<u8>::new());
    }

    #[tokio::test]
    async fn test_upgrade_success() {
        let mut server = Server::new_async().await;
        let commitment = vec![1u8; 32];
        let hex_commitment = hex::encode(&commitment);

        let mock = server
            .mock("GET", format!("/timestamp/{}", hex_commitment).as_str())
            .with_status(200)
            .with_body(vec![0x00, 0x4f, 0x54, 0x02])
            .create_async()
            .await;

        let client = CalendarClient::new(30).unwrap();
        let result = client.upgrade(&server.url(), &commitment).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.is_some());
        assert_eq!(response.unwrap(), vec![0x00, 0x4f, 0x54, 0x02]);
    }

    #[tokio::test]
    async fn test_upgrade_not_found_returns_none() {
        let mut server = Server::new_async().await;
        let commitment = vec![2u8; 32];
        let hex_commitment = hex::encode(&commitment);

        let mock = server
            .mock("GET", format!("/timestamp/{}", hex_commitment).as_str())
            .with_status(404)
            .create_async()
            .await;

        let client = CalendarClient::new(30).unwrap();
        let result = client.upgrade(&server.url(), &commitment).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.is_none());
    }

    #[tokio::test]
    async fn test_upgrade_non_success_non_404_status() {
        let mut server = Server::new_async().await;
        let commitment = vec![3u8; 32];
        let hex_commitment = hex::encode(&commitment);

        let mock = server
            .mock("GET", format!("/timestamp/{}", hex_commitment).as_str())
            .with_status(503)
            .create_async()
            .await;

        let client = CalendarClient::new(30).unwrap();
        let result = client.upgrade(&server.url(), &commitment).await;

        mock.assert_async().await;
        assert!(result.is_err());
        match result {
            Err(AnchorError::ServiceError(msg)) => {
                assert!(msg.contains("503"));
            }
            _ => panic!("Expected ServiceError"),
        }
    }

    #[tokio::test]
    async fn test_upgrade_network_error() {
        let client = CalendarClient::new(1).unwrap();
        let commitment = vec![4u8; 32];

        // Invalid URL to trigger network error
        let result = client
            .upgrade("http://invalid.local:9999", &commitment)
            .await;

        assert!(result.is_err());
        // Network error or timeout are both acceptable
        assert!(matches!(
            result,
            Err(AnchorError::Network(_)) | Err(AnchorError::Timeout(_))
        ));
    }

    #[tokio::test]
    async fn test_upgrade_hex_encoding() {
        let mut server = Server::new_async().await;
        let commitment = vec![0xAB, 0xCD, 0xEF];
        let expected_path = "/timestamp/abcdef";

        let mock = server
            .mock("GET", expected_path)
            .with_status(200)
            .with_body(vec![0xFF])
            .create_async()
            .await;

        let client = CalendarClient::new(30).unwrap();
        let result = client.upgrade(&server.url(), &commitment).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_upgrade_empty_commitment() {
        let mut server = Server::new_async().await;
        let commitment = vec![];

        let mock = server
            .mock("GET", "/timestamp/")
            .with_status(200)
            .with_body(vec![0xFF])
            .create_async()
            .await;

        let client = CalendarClient::new(30).unwrap();
        let result = client.upgrade(&server.url(), &commitment).await;

        mock.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_submit_with_different_hash_values() {
        let mut server = Server::new_async().await;

        // Test with all zeros
        let hash_zeros: [u8; 32] = [0u8; 32];
        let mock1 = server
            .mock("POST", "/digest")
            .with_status(200)
            .with_body(vec![0x01])
            .create_async()
            .await;

        let client = CalendarClient::new(30).unwrap();
        let result = client.submit(&server.url(), &hash_zeros).await;
        mock1.assert_async().await;
        assert!(result.is_ok());

        // Test with all ones
        let hash_ones: [u8; 32] = [0xFFu8; 32];
        let mock2 = server
            .mock("POST", "/digest")
            .with_status(200)
            .with_body(vec![0x02])
            .create_async()
            .await;

        let result = client.submit(&server.url(), &hash_ones).await;
        mock2.assert_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_upgrade_empty_response() {
        let mut server = Server::new_async().await;
        let commitment = vec![5u8; 32];
        let hex_commitment = hex::encode(&commitment);

        let mock = server
            .mock("GET", format!("/timestamp/{}", hex_commitment).as_str())
            .with_status(200)
            .with_body(vec![])
            .create_async()
            .await;

        let client = CalendarClient::new(30).unwrap();
        let result = client.upgrade(&server.url(), &commitment).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response, Some(Vec::<u8>::new()));
    }

    #[tokio::test]
    #[ignore]
    async fn test_calendar_submit() {
        let client = CalendarClient::new(30).unwrap();
        let hash = sha2::Sha256::digest(b"test calendar").into();

        let result = client
            .submit("https://a.pool.opentimestamps.org", &hash)
            .await;
        assert!(result.is_ok());
    }
}
