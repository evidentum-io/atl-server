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
    use atl_core::ots::{Step, StepData};

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

        let timeout = Duration::from_secs(30);
        let status = detect_status(&timestamp, timeout).await.unwrap();
        assert!(matches!(status, OtsStatus::Pending { .. }));
    }

    #[tokio::test]
    #[ignore] // Requires network access - will be covered in BBT-3 with pre-populated cache
    async fn test_detect_confirmed_status() {
        let hash = vec![2u8; 32];
        let bitcoin_att = Attestation::Bitcoin { height: 700000 };

        let step = Step {
            data: StepData::Attestation(bitcoin_att),
            output: hash.clone(),
            next: vec![],
        };

        let timestamp = Timestamp {
            start_digest: hash,
            first_step: step,
        };

        let timeout = Duration::from_secs(10);
        let status = detect_status(&timestamp, timeout).await.unwrap();
        assert!(matches!(status, OtsStatus::Confirmed { .. }));
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
