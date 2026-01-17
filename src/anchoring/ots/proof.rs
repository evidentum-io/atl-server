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

    #[test]
    fn test_collect_attestations_empty() {
        let hash = vec![4u8; 32];
        let step = Step {
            data: StepData::Fork,
            output: hash.clone(),
            next: vec![],
        };

        let attestations = collect_attestations(&step);
        assert!(attestations.is_empty());
    }

    #[test]
    fn test_collect_attestations_single() {
        let hash = vec![5u8; 32];
        let pending_att = Attestation::Pending {
            uri: "https://test.calendar".to_string(),
        };

        let step = Step {
            data: StepData::Attestation(pending_att),
            output: hash.clone(),
            next: vec![],
        };

        let attestations = collect_attestations(&step);
        assert_eq!(attestations.len(), 1);
    }

    #[test]
    fn test_collect_attestations_nested() {
        use atl_core::ots::Op;

        let hash = vec![6u8; 32];
        let pending_att = Attestation::Pending {
            uri: "https://calendar1.com".to_string(),
        };
        let bitcoin_att = Attestation::Bitcoin { height: 700000 };

        let child1 = Step {
            data: StepData::Attestation(pending_att),
            output: vec![7u8; 32],
            next: vec![],
        };

        let child2 = Step {
            data: StepData::Attestation(bitcoin_att),
            output: vec![8u8; 32],
            next: vec![],
        };

        let parent = Step {
            data: StepData::Op(Op::Sha256),
            output: hash.clone(),
            next: vec![child1, child2],
        };

        let attestations = collect_attestations(&parent);
        assert_eq!(attestations.len(), 2);
    }

    #[test]
    fn test_collect_attestations_deeply_nested() {
        use atl_core::ots::Op;

        let bitcoin_att = Attestation::Bitcoin { height: 800000 };

        let leaf = Step {
            data: StepData::Attestation(bitcoin_att),
            output: vec![9u8; 32],
            next: vec![],
        };

        let level2 = Step {
            data: StepData::Op(Op::Append(vec![1, 2])),
            output: vec![10u8; 32],
            next: vec![leaf],
        };

        let level1 = Step {
            data: StepData::Fork,
            output: vec![11u8; 32],
            next: vec![level2],
        };

        let attestations = collect_attestations(&level1);
        assert_eq!(attestations.len(), 1);
    }

    #[test]
    fn test_collect_attestations_multiple_branches() {
        use atl_core::ots::Op;

        let att1 = Attestation::Bitcoin { height: 700000 };
        let att2 = Attestation::Pending {
            uri: "https://cal1.com".to_string(),
        };
        let att3 = Attestation::Pending {
            uri: "https://cal2.com".to_string(),
        };

        let branch1 = Step {
            data: StepData::Attestation(att1),
            output: vec![12u8; 32],
            next: vec![],
        };

        let branch2 = Step {
            data: StepData::Attestation(att2),
            output: vec![13u8; 32],
            next: vec![],
        };

        let branch3 = Step {
            data: StepData::Attestation(att3),
            output: vec![14u8; 32],
            next: vec![],
        };

        let root = Step {
            data: StepData::Op(Op::Sha256),
            output: vec![15u8; 32],
            next: vec![branch1, branch2, branch3],
        };

        let attestations = collect_attestations(&root);
        assert_eq!(attestations.len(), 3);
    }

    #[test]
    fn test_find_pending_attestation_found() {
        let hash = vec![16u8; 32];
        let pending_att = Attestation::Pending {
            uri: "https://found.calendar".to_string(),
        };

        let commitment = vec![99u8; 32];

        let step = Step {
            data: StepData::Attestation(pending_att),
            output: commitment.clone(),
            next: vec![],
        };

        let timestamp = Timestamp {
            start_digest: hash,
            first_step: step,
        };

        let result = find_pending_attestation(&timestamp);
        assert!(result.is_some());
        let (url, output) = result.unwrap();
        assert_eq!(url, "https://found.calendar");
        assert_eq!(output, commitment);
    }

    #[test]
    fn test_find_pending_attestation_not_found() {
        let hash = vec![17u8; 32];
        let bitcoin_att = Attestation::Bitcoin { height: 700000 };

        let step = Step {
            data: StepData::Attestation(bitcoin_att),
            output: vec![18u8; 32],
            next: vec![],
        };

        let timestamp = Timestamp {
            start_digest: hash,
            first_step: step,
        };

        let result = find_pending_attestation(&timestamp);
        assert!(result.is_none());
    }

    #[test]
    fn test_find_pending_attestation_nested() {
        use atl_core::ots::Op;

        let hash = vec![19u8; 32];
        let pending_att = Attestation::Pending {
            uri: "https://nested.calendar".to_string(),
        };

        let commitment = vec![77u8; 32];

        let child = Step {
            data: StepData::Attestation(pending_att),
            output: commitment.clone(),
            next: vec![],
        };

        let parent = Step {
            data: StepData::Op(Op::Sha256),
            output: vec![20u8; 32],
            next: vec![child],
        };

        let timestamp = Timestamp {
            start_digest: hash,
            first_step: parent,
        };

        let result = find_pending_attestation(&timestamp);
        assert!(result.is_some());
        let (url, output) = result.unwrap();
        assert_eq!(url, "https://nested.calendar");
        assert_eq!(output, commitment);
    }

    #[test]
    fn test_find_pending_attestation_multiple_returns_first() {
        use atl_core::ots::Op;

        let hash = vec![21u8; 32];
        let pending1 = Attestation::Pending {
            uri: "https://first.calendar".to_string(),
        };
        let pending2 = Attestation::Pending {
            uri: "https://second.calendar".to_string(),
        };

        let commitment1 = vec![88u8; 32];

        let branch1 = Step {
            data: StepData::Attestation(pending1),
            output: commitment1.clone(),
            next: vec![],
        };

        let branch2 = Step {
            data: StepData::Attestation(pending2),
            output: vec![89u8; 32],
            next: vec![],
        };

        let root = Step {
            data: StepData::Op(Op::Sha256),
            output: vec![22u8; 32],
            next: vec![branch1, branch2],
        };

        let timestamp = Timestamp {
            start_digest: hash,
            first_step: root,
        };

        let result = find_pending_attestation(&timestamp);
        assert!(result.is_some());
        let (url, output) = result.unwrap();
        assert_eq!(url, "https://first.calendar");
        assert_eq!(output, commitment1);
    }

    #[test]
    fn test_verify_timestamp_success() {
        let expected_hash = [23u8; 32];
        let pending_att = Attestation::Pending {
            uri: "https://test.calendar".to_string(),
        };

        let step = Step {
            data: StepData::Attestation(pending_att),
            output: expected_hash.to_vec(),
            next: vec![],
        };

        let timestamp = Timestamp {
            start_digest: expected_hash.to_vec(),
            first_step: step,
        };

        let result = verify_timestamp(&timestamp, &expected_hash);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_timestamp_wrong_digest() {
        let expected_hash = [24u8; 32];
        let wrong_hash = [25u8; 32];
        let pending_att = Attestation::Pending {
            uri: "https://test.calendar".to_string(),
        };

        let step = Step {
            data: StepData::Attestation(pending_att),
            output: wrong_hash.to_vec(),
            next: vec![],
        };

        let timestamp = Timestamp {
            start_digest: wrong_hash.to_vec(),
            first_step: step,
        };

        let result = verify_timestamp(&timestamp, &expected_hash);
        assert!(result.is_err());
        assert!(matches!(result, Err(AnchorError::TokenInvalid(_))));
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("does not match expected hash"));
    }

    #[test]
    fn test_verify_timestamp_no_attestations() {
        let expected_hash = [26u8; 32];

        let step = Step {
            data: StepData::Fork,
            output: expected_hash.to_vec(),
            next: vec![],
        };

        let timestamp = Timestamp {
            start_digest: expected_hash.to_vec(),
            first_step: step,
        };

        let result = verify_timestamp(&timestamp, &expected_hash);
        assert!(result.is_err());
        assert!(matches!(result, Err(AnchorError::TokenInvalid(_))));
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("has no attestations"));
    }

    #[test]
    fn test_verify_timestamp_with_bitcoin_attestation() {
        let expected_hash = [27u8; 32];
        let bitcoin_att = Attestation::Bitcoin { height: 700000 };

        let step = Step {
            data: StepData::Attestation(bitcoin_att),
            output: expected_hash.to_vec(),
            next: vec![],
        };

        let timestamp = Timestamp {
            start_digest: expected_hash.to_vec(),
            first_step: step,
        };

        let result = verify_timestamp(&timestamp, &expected_hash);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_proof_invalid_bytes() {
        let invalid_proof = vec![0xFF, 0xFF, 0xFF];

        let result = parse_proof(&invalid_proof);
        assert!(result.is_err());
        assert!(matches!(result, Err(AnchorError::InvalidResponse(_))));
    }

    #[test]
    fn test_parse_proof_empty_bytes() {
        let empty_proof = vec![];

        let result = parse_proof(&empty_proof);
        assert!(result.is_err());
        assert!(matches!(result, Err(AnchorError::InvalidResponse(_))));
    }

    #[tokio::test]
    async fn test_detect_status_with_unknown_attestation() {
        use atl_core::ots::Op;

        let hash = vec![28u8; 32];
        // Use an operation node with no attestation children
        let step = Step {
            data: StepData::Op(Op::Sha256),
            output: hash.clone(),
            next: vec![],
        };

        let timestamp = Timestamp {
            start_digest: hash,
            first_step: step,
        };

        let status = detect_status(&timestamp, TEST_TIMEOUT).await.unwrap();
        // Should default to Pending with empty URL
        assert!(matches!(status, OtsStatus::Pending { .. }));
    }

    #[tokio::test]
    async fn test_detect_status_fork_with_nested_attestation() {
        let hash = vec![29u8; 32];
        let pending_att = Attestation::Pending {
            uri: "https://nested.calendar".to_string(),
        };

        let child = Step {
            data: StepData::Attestation(pending_att),
            output: vec![30u8; 32],
            next: vec![],
        };

        let parent = Step {
            data: StepData::Fork,
            output: hash.clone(),
            next: vec![child],
        };

        let timestamp = Timestamp {
            start_digest: hash,
            first_step: parent,
        };

        let status = detect_status(&timestamp, TEST_TIMEOUT).await.unwrap();
        match status {
            OtsStatus::Pending { calendar_url } => {
                assert_eq!(calendar_url, "https://nested.calendar");
            }
            _ => panic!("Expected Pending status"),
        }
    }

    #[tokio::test]
    async fn test_detect_status_bitcoin_takes_priority() {
        use atl_core::ots::Op;

        let hash = vec![31u8; 32];
        let test_height = 700001u64;
        let test_block_time = 1_631_320_000u64;

        // Pre-populate cache
        {
            let mut cache = BLOCK_TIME_CACHE.write().unwrap();
            cache.insert(test_height, test_block_time);
        }

        let bitcoin_att = Attestation::Bitcoin {
            height: test_height,
        };
        let pending_att = Attestation::Pending {
            uri: "https://calendar.com".to_string(),
        };

        let bitcoin_branch = Step {
            data: StepData::Attestation(bitcoin_att),
            output: vec![32u8; 32],
            next: vec![],
        };

        let pending_branch = Step {
            data: StepData::Attestation(pending_att),
            output: vec![33u8; 32],
            next: vec![],
        };

        // Bitcoin comes first in the tree
        let root = Step {
            data: StepData::Op(Op::Sha256),
            output: hash.clone(),
            next: vec![bitcoin_branch, pending_branch],
        };

        let timestamp = Timestamp {
            start_digest: hash,
            first_step: root,
        };

        let status = detect_status(&timestamp, TEST_TIMEOUT).await.unwrap();
        assert!(matches!(status, OtsStatus::Confirmed { .. }));

        // Cleanup
        BLOCK_TIME_CACHE.write().unwrap().remove(&test_height);
    }

    #[test]
    fn test_is_finalized_edge_cases() {
        let pending_empty = OtsStatus::Pending {
            calendar_url: String::new(),
        };
        assert!(!is_finalized(&pending_empty));

        let confirmed_zero_height = OtsStatus::Confirmed {
            block_height: 0,
            block_time: 0,
        };
        assert!(is_finalized(&confirmed_zero_height));

        let confirmed_max = OtsStatus::Confirmed {
            block_height: u64::MAX,
            block_time: u64::MAX,
        };
        assert!(is_finalized(&confirmed_max));
    }
}
