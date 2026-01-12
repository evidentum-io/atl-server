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
    use sha2::Digest;

    #[test]
    fn test_calendar_client_creation() {
        let result = CalendarClient::new(30);
        assert!(result.is_ok());
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
