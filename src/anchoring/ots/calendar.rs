//! Calendar server communication

use crate::anchoring::error::AnchorError;
use std::time::Duration;

/// Calendar client for HTTP communication with OTS servers
pub struct CalendarClient {
    /// HTTP client
    client: reqwest::blocking::Client,

    /// Timeout
    timeout: Duration,
}

impl CalendarClient {
    /// Create a new calendar client
    pub fn new(timeout_secs: u64) -> Result<Self, AnchorError> {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .map_err(|e| AnchorError::Network(e.to_string()))?;

        Ok(Self {
            client,
            timeout: Duration::from_secs(timeout_secs),
        })
    }

    /// Submit a hash to calendar and get initial timestamp
    pub fn submit(&self, calendar_url: &str, hash: &[u8; 32]) -> Result<Vec<u8>, AnchorError> {
        tracing::debug!(calendar_url = %calendar_url, "Submitting to calendar");

        let url = format!("{}/digest", calendar_url);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/x-opentimestamps")
            .body(hash.to_vec())
            .send()
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
            .map_err(|e| AnchorError::Network(e.to_string()))?
            .to_vec();

        tracing::info!(calendar_url = %calendar_url, "Timestamp submitted successfully");

        Ok(bytes)
    }

    /// Upgrade a pending timestamp by querying calendar
    pub fn upgrade(
        &self,
        calendar_url: &str,
        current_timestamp: &[u8],
    ) -> Result<Option<Vec<u8>>, AnchorError> {
        tracing::debug!(calendar_url = %calendar_url, "Attempting upgrade");

        let url = format!("{}/timestamp", calendar_url);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/x-opentimestamps")
            .body(current_timestamp.to_vec())
            .send()
            .map_err(|e| {
                if e.is_timeout() {
                    AnchorError::Timeout(self.timeout.as_secs())
                } else {
                    AnchorError::Network(e.to_string())
                }
            })?;

        if !response.status().is_success() {
            tracing::debug!("Upgrade not available yet");
            return Ok(None);
        }

        let bytes = response
            .bytes()
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

    #[test]
    #[ignore]
    fn test_calendar_submit() {
        let client = CalendarClient::new(30).unwrap();
        let hash = sha2::Sha256::digest(b"test calendar").into();

        let result = client.submit("https://a.pool.opentimestamps.org", &hash);
        assert!(result.is_ok());
    }
}
