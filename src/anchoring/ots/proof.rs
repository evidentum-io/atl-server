//! OTS proof status detection and verification

use crate::anchoring::error::AnchorError;
use crate::anchoring::ots::bitcoin;
use crate::anchoring::ots::types::OtsStatus;
use atl_core::ots::{Attestation, DetachedTimestampFile, Timestamp};
use std::time::Duration;

/// Detect proof status from timestamp attestations
pub async fn detect_status(
    timestamp: &Timestamp,
    timeout: Duration,
) -> Result<OtsStatus, AnchorError> {
    // Walk the timestamp tree to find attestations
    let attestations = collect_attestations(&timestamp.first_step);

    for attestation in attestations {
        match attestation {
            Attestation::Bitcoin { height } => {
                // Fetch real timestamp from Bitcoin blockchain APIs
                let block_time = bitcoin::get_block_timestamp(*height, timeout).await?;
                return Ok(OtsStatus::Confirmed {
                    block_height: *height,
                    block_time,
                });
            }
            Attestation::Pending { uri } => {
                return Ok(OtsStatus::Pending {
                    calendar_url: uri.clone(),
                });
            }
            _ => continue,
        }
    }

    // Default to pending if no recognized attestations
    Ok(OtsStatus::Pending {
        calendar_url: String::new(),
    })
}

/// Collect all attestations from a timestamp tree
fn collect_attestations(step: &atl_core::ots::Step) -> Vec<&Attestation> {
    use atl_core::ots::StepData;

    let mut attestations = Vec::new();

    match &step.data {
        StepData::Attestation(att) => {
            attestations.push(att);
        }
        StepData::Op(_) | StepData::Fork => {
            // Recursively collect from children
            for child in &step.next {
                attestations.extend(collect_attestations(child));
            }
        }
    }

    attestations
}

/// Find pending attestation and return (calendar_url, commitment)
///
/// Commitment is the step.output for the pending attestation - this is what
/// the calendar server uses to look up the completed timestamp.
pub fn find_pending_attestation(timestamp: &Timestamp) -> Option<(String, Vec<u8>)> {
    find_pending_in_step(&timestamp.first_step)
}

fn find_pending_in_step(step: &atl_core::ots::Step) -> Option<(String, Vec<u8>)> {
    use atl_core::ots::StepData;

    match &step.data {
        StepData::Attestation(Attestation::Pending { uri }) => {
            Some((uri.clone(), step.output.clone()))
        }
        StepData::Op(_) | StepData::Fork => {
            for child in &step.next {
                if let Some(result) = find_pending_in_step(child) {
                    return Some(result);
                }
            }
            None
        }
        _ => None,
    }
}

/// Check if a timestamp is finalized (Bitcoin confirmed)
#[must_use]
pub fn is_finalized(status: &OtsStatus) -> bool {
    matches!(status, OtsStatus::Confirmed { .. })
}

/// Verify timestamp structure and hash commitment
pub fn verify_timestamp(
    timestamp: &Timestamp,
    expected_hash: &[u8; 32],
) -> Result<(), AnchorError> {
    // Verify start digest matches
    if timestamp.start_digest.as_slice() != expected_hash {
        return Err(AnchorError::TokenInvalid(
            "timestamp digest does not match expected hash".into(),
        ));
    }

    // Basic structural verification (tree depth, etc.)
    // Full cryptographic verification would require re-executing all operations
    // For now, just check that we have attestations
    let attestations = collect_attestations(&timestamp.first_step);

    if attestations.is_empty() {
        return Err(AnchorError::TokenInvalid(
            "timestamp has no attestations".into(),
        ));
    }

    Ok(())
}

/// Parse OTS proof bytes into DetachedTimestampFile
pub fn parse_proof(proof_bytes: &[u8]) -> Result<DetachedTimestampFile, AnchorError> {
    DetachedTimestampFile::from_bytes(proof_bytes)
        .map_err(|e| AnchorError::InvalidResponse(format!("failed to parse OTS proof: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anchoring::ots::bitcoin::BLOCK_TIME_CACHE;
    use atl_core::ots::{Step, StepData};

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    #[tokio::test]
    async fn test_detect_pending_status() {
        let hash = vec![1u8; 32];
        let pending_att = Attestation::Pending {
            uri: "https://test.calendar".to_string(),
        };

        let step = Step {
            data: StepData::Attestation(pending_att),
            output: hash.clone(),
            next: vec![],
        };

        let timestamp = Timestamp {
            start_digest: hash,
            first_step: step,
        };

        let status = detect_status(&timestamp, TEST_TIMEOUT).await.unwrap();
        assert!(matches!(status, OtsStatus::Pending { .. }));
    }

    #[tokio::test]
    async fn test_detect_confirmed_status() {
        let hash = vec![2u8; 32];
        let test_height = 700_000u64;
        let test_block_time = 1_631_318_400u64; // Real timestamp for block 700000

        // Pre-populate cache to avoid API call in test
        {
            let mut cache = BLOCK_TIME_CACHE.write().unwrap();
            cache.insert(test_height, test_block_time);
        }

        let bitcoin_att = Attestation::Bitcoin {
            height: test_height,
        };

        let step = Step {
            data: StepData::Attestation(bitcoin_att),
            output: hash.clone(),
            next: vec![],
        };

        let timestamp = Timestamp {
            start_digest: hash,
            first_step: step,
        };

        let status = detect_status(&timestamp, TEST_TIMEOUT).await.unwrap();
        match status {
            OtsStatus::Confirmed {
                block_height,
                block_time,
            } => {
                assert_eq!(block_height, test_height);
                assert_eq!(block_time, test_block_time);
            }
            _ => panic!("Expected Confirmed status"),
        }

        // Cleanup
        BLOCK_TIME_CACHE.write().unwrap().remove(&test_height);
    }

    #[tokio::test]
    async fn test_detect_status_propagates_error() {
        let hash = vec![3u8; 32];
        // Use a height that doesn't exist - should cause error (no fallback!)
        let impossible_height = 999_999_999u64;

        let bitcoin_att = Attestation::Bitcoin {
            height: impossible_height,
        };

        let step = Step {
            data: StepData::Attestation(bitcoin_att),
            output: hash.clone(),
            next: vec![],
        };

        let timestamp = Timestamp {
            start_digest: hash,
            first_step: step,
        };

        // Should return error, NOT a fallback estimate!
        let result = detect_status(&timestamp, Duration::from_secs(5)).await;
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(AnchorError::BlockTimeFetchFailed { .. })
        ));
    }

    #[test]
    fn test_is_finalized() {
        let pending = OtsStatus::Pending {
            calendar_url: "test".to_string(),
        };
        assert!(!is_finalized(&pending));

        let confirmed = OtsStatus::Confirmed {
            block_height: 700000,
            block_time: 1640000000,
        };
        assert!(is_finalized(&confirmed));
    }
}
