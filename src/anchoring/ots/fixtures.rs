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
    fn test_mock_calendar_response() {
        let response = mock_calendar_response("https://test.calendar");
        assert!(!response.is_empty());
        // First byte should be attestation marker
        assert_eq!(response[0], 0x00);
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
}
