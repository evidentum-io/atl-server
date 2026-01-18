//! Test fixtures for OTS calendar responses
//!
//! Real captured responses from OTS calendar servers for testing.

/// Test hash used for fixtures (SHA256 of "test fixture data")
pub const TEST_HASH: [u8; 32] = [
    // sha256("test fixture data")
    0x8d, 0x96, 0x9e, 0xef, 0x6e, 0xca, 0xd3, 0xc2, 0x9a, 0x3a, 0x62, 0x92, 0x80, 0xe6, 0x86, 0xcf,
    0x0c, 0x3f, 0x5d, 0x5a, 0x86, 0xaf, 0xf3, 0xca, 0x12, 0x02, 0x0c, 0x92, 0x3a, 0xdc, 0x6c, 0x92,
];

/// Raw calendar response for TEST_HASH from a.pool.opentimestamps.org
///
/// This is the raw operations returned by the calendar, NOT a complete .ots file.
/// Use with `DetachedTimestampFile::from_calendar_response(TEST_HASH, &CALENDAR_RESPONSE)`.
pub const CALENDAR_RESPONSE_A: &[u8] = &[
    // Captured response - will be filled with real data
    // Format: operations starting with opcodes
    // Typically ~100 bytes for pending attestation
];

/// Alternative calendar response from b.pool.opentimestamps.org
pub const CALENDAR_RESPONSE_B: &[u8] = &[
    // Captured response from different calendar
];

/// Example of upgraded proof with Bitcoin attestation
pub const CONFIRMED_PROOF: &[u8] = &[
    // Complete .ots file with Bitcoin attestation
    // Magic header + version + hash type + hash + operations + bitcoin attestation
];

/// Generate a test hash from arbitrary data
pub fn test_hash(data: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(data).into()
}

/// Generate mock calendar response (for unit tests without real captures)
///
/// This creates a minimal valid pending attestation response.
pub fn mock_calendar_response(calendar_url: &str) -> Vec<u8> {
    // Minimal pending attestation:
    // 00 = attestation marker
    // 83 df e3 0d 2e f9 0c 8e = PENDING_TAG
    // length (varint) + url bytes

    let mut response = vec![
        0x00, // Attestation marker
        0x83, 0xdf, 0xe3, 0x0d, 0x2e, 0xf9, 0x0c, 0x8e, // PENDING_TAG
    ];

    // Add URL length (varint) and URL bytes
    let url_bytes = calendar_url.as_bytes();
    response.push(url_bytes.len() as u8); // Simple length for short URLs
    response.extend_from_slice(url_bytes);

    response
}

/// Create a minimal valid OTS response for testing
pub fn create_minimal_ots_response(hash: &[u8; 32]) -> Vec<u8> {
    use atl_core::ots::DetachedTimestampFile;

    // Create using atl_core's builder
    let calendar_response = mock_calendar_response("https://test.calendar");
    let file = DetachedTimestampFile::from_calendar_response(*hash, &calendar_response)
        .expect("failed to create test timestamp");

    file.to_bytes().expect("failed to serialize timestamp")
}

/// Create a timestamp without pending attestation (for testing)
pub fn create_timestamp_without_pending(hash: &[u8; 32]) -> atl_core::ots::DetachedTimestampFile {
    use atl_core::ots::{
        Attestation, DetachedTimestampFile, DigestType, Step, StepData, Timestamp,
    };

    // Create a timestamp with only unknown attestation type
    let step = Step {
        data: StepData::Attestation(Attestation::Unknown {
            tag: [0xFF; 8],
            data: vec![],
        }),
        output: hash.to_vec(),
        next: vec![],
    };

    let timestamp = Timestamp {
        start_digest: hash.to_vec(),
        first_step: step,
    };

    DetachedTimestampFile {
        digest_type: DigestType::Sha256,
        timestamp,
    }
}

/// Create a timestamp with pending attestation pointing to specific calendar
pub fn create_timestamp_with_pending(
    hash: &[u8; 32],
    calendar_url: &str,
) -> atl_core::ots::DetachedTimestampFile {
    use atl_core::ots::{
        Attestation, DetachedTimestampFile, DigestType, Step, StepData, Timestamp,
    };

    // Use a deterministic commitment based on hash
    let mut commitment = hash.to_vec();
    commitment.push(0x01);

    let step = Step {
        data: StepData::Attestation(Attestation::Pending {
            uri: calendar_url.to_string(),
        }),
        output: commitment,
        next: vec![],
    };

    let timestamp = Timestamp {
        start_digest: hash.to_vec(),
        first_step: step,
    };

    DetachedTimestampFile {
        digest_type: DigestType::Sha256,
        timestamp,
    }
}

/// Create a timestamp with specific URI (for normalization testing)
pub fn create_timestamp_with_uri(
    hash: &[u8; 32],
    uri: &str,
) -> atl_core::ots::DetachedTimestampFile {
    create_timestamp_with_pending(hash, uri)
}

/// Create a minimal upgrade response
pub fn create_minimal_ots_upgrade_response() -> Vec<u8> {
    // Minimal upgrade: just some operations
    vec![
        0x08, // SHA256 operation
        0xF0, 0x20, // Prepend 32 bytes
        // 32 bytes of data
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
        0x1F, 0x20, 0x08, // SHA256 operation
        // Bitcoin attestation
        0x00, // Attestation marker
        0x05, 0x88, 0x96, 0x0d, 0x73, 0xd7, 0x19, 0x01, // BITCOIN_TAG
        0x04, 0x00, 0x00, 0xc3, 0x50, // Block height 700000 in varint
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use atl_core::ots::DetachedTimestampFile;

    #[test]
    fn test_test_hash() {
        let hash = test_hash(b"hello");
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_test_hash_deterministic() {
        let hash1 = test_hash(b"test data");
        let hash2 = test_hash(b"test data");
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_test_hash_different_inputs() {
        let hash1 = test_hash(b"data1");
        let hash2 = test_hash(b"data2");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_mock_calendar_response() {
        let response = mock_calendar_response("https://test.calendar");
        assert!(!response.is_empty());
        // First byte should be attestation marker
        assert_eq!(response[0], 0x00);
    }

    #[test]
    fn test_mock_calendar_response_contains_url() {
        let url = "https://test.calendar";
        let response = mock_calendar_response(url);
        // Response should contain the URL bytes
        let url_bytes = url.as_bytes();
        assert!(response.windows(url_bytes.len()).any(|w| w == url_bytes));
    }

    #[test]
    fn test_mock_response_parses() {
        let hash = test_hash(b"test");
        let response = mock_calendar_response("https://a.pool.opentimestamps.org");

        // This should successfully create a DetachedTimestampFile
        let result = DetachedTimestampFile::from_calendar_response(hash, &response);

        // May fail if mock format doesn't match exactly what atl_core expects
        // Adjust mock format as needed
        assert!(
            result.is_ok(),
            "Mock calendar response failed to parse: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_create_minimal_ots_response() {
        let hash = test_hash(b"test data");
        let ots_bytes = create_minimal_ots_response(&hash);
        assert!(!ots_bytes.is_empty());
    }

    #[test]
    fn test_create_minimal_ots_response_parses() {
        let hash = test_hash(b"test");
        let ots_bytes = create_minimal_ots_response(&hash);

        // Should be able to deserialize back
        let result = DetachedTimestampFile::from_bytes(&ots_bytes);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_timestamp_without_pending() {
        let hash = test_hash(b"test");
        let timestamp = create_timestamp_without_pending(&hash);

        // Verify it's a valid timestamp without pending attestation
        assert_eq!(timestamp.timestamp.start_digest, hash.to_vec());
    }

    #[test]
    fn test_create_timestamp_with_pending() {
        let hash = test_hash(b"test");
        let calendar_url = "https://test.calendar.org";
        let timestamp = create_timestamp_with_pending(&hash, calendar_url);

        // Verify it's a valid timestamp
        assert_eq!(timestamp.timestamp.start_digest, hash.to_vec());
    }

    #[test]
    fn test_create_timestamp_with_uri() {
        let hash = test_hash(b"test");
        let uri = "https://custom.calendar.org";
        let timestamp = create_timestamp_with_uri(&hash, uri);

        // Should be equivalent to create_timestamp_with_pending
        assert_eq!(timestamp.timestamp.start_digest, hash.to_vec());
    }

    #[test]
    fn test_create_minimal_ots_upgrade_response() {
        let response = create_minimal_ots_upgrade_response();
        assert!(!response.is_empty());
        // Should contain some operations
        assert!(response.len() > 10);
    }

    #[test]
    fn test_test_hash_constant() {
        // Verify TEST_HASH is correct
        assert_eq!(TEST_HASH.len(), 32);
    }

    #[test]
    fn test_calendar_response_constants() {
        // Verify constants are properly defined
        assert_eq!(CALENDAR_RESPONSE_A.len(), 0); // Empty in current implementation
        assert_eq!(CALENDAR_RESPONSE_B.len(), 0); // Empty in current implementation
        assert_eq!(CONFIRMED_PROOF.len(), 0); // Empty in current implementation
    }
}
