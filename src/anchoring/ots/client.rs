//! OpenTimestamps client implementation

use crate::anchoring::error::AnchorError;
use crate::anchoring::ots::calendar::CalendarClient;
use crate::anchoring::ots::proof::{detect_status, is_finalized, parse_proof, verify_timestamp};
use crate::anchoring::ots::types::{OtsConfig, OtsStatus};
use crate::error::ServerResult;
use crate::traits::anchor::{Anchor, AnchorRequest, AnchorResult, AnchorType, Anchorer};
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
    fn submit_to_calendar(
        &self,
        hash: &[u8; 32],
        calendar_url: &str,
    ) -> Result<DetachedTimestampFile, AnchorError> {
        let proof_bytes = self.calendar.submit(calendar_url, hash)?;

        DetachedTimestampFile::from_bytes(&proof_bytes)
            .map_err(|e| AnchorError::InvalidResponse(format!("failed to parse OTS proof: {}", e)))
    }

    /// Try to get timestamp with fallback to multiple calendars
    fn timestamp_with_fallback(
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
            match self.submit_to_calendar(hash, calendar_url) {
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
    pub fn upgrade_proof(
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

            if let Some(upgraded_bytes) = self.calendar.upgrade(&calendar_url, &current_bytes)? {
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

impl Anchorer for OpenTimestampsClient {
    fn anchor_type(&self) -> AnchorType {
        AnchorType::BitcoinOts
    }

    fn anchor(&self, request: &AnchorRequest) -> ServerResult<AnchorResult> {
        let (calendar_url, file) = self.timestamp_with_fallback(&request.hash)?;

        let status = detect_status(&file.timestamp);

        let (is_final, block_height, block_time) = match &status {
            OtsStatus::Confirmed {
                block_height,
                block_time,
            } => (true, Some(*block_height), Some(*block_time)),
            OtsStatus::Pending { .. } => (false, None, None),
        };

        let proof_bytes = file
            .to_bytes()
            .map_err(|e| AnchorError::InvalidResponse(e.to_string()))?;

        let timestamp_nanos = block_time.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });

        Ok(AnchorResult {
            anchor: Anchor {
                anchor_type: AnchorType::BitcoinOts,
                anchored_hash: request.hash,
                tree_size: request.tree_size,
                timestamp: timestamp_nanos,
                token: proof_bytes,
                metadata: serde_json::json!({
                    "calendar_url": calendar_url,
                    "status": if is_final { "confirmed" } else { "pending" },
                    "bitcoin_block_height": block_height,
                    "bitcoin_block_time": block_time,
                }),
            },
            is_final,
            estimated_finality_secs: if is_final { None } else { Some(3600) },
        })
    }

    fn verify(&self, anchor: &Anchor) -> ServerResult<()> {
        if anchor.anchor_type != AnchorType::BitcoinOts {
            return Err(AnchorError::TokenInvalid("not an OTS anchor".into()).into());
        }

        let file = parse_proof(&anchor.token)?;

        verify_timestamp(&file.timestamp, &anchor.anchored_hash)?;

        Ok(())
    }

    fn is_final(&self, anchor: &Anchor) -> ServerResult<bool> {
        let file = parse_proof(&anchor.token)?;

        let status = detect_status(&file.timestamp);

        Ok(is_finalized(&status))
    }

    fn service_id(&self) -> &str {
        "opentimestamps"
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
    #[ignore]
    fn test_ots_submit() {
        let client = OpenTimestampsClient::new().unwrap();
        let hash = sha2::Sha256::digest(b"test ots data").into();

        let request = AnchorRequest {
            hash,
            tree_size: 1,
            metadata: None,
        };

        let result = client.anchor(&request);
        assert!(result.is_ok());
    }
}
