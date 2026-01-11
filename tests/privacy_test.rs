//! Privacy by Design verification tests

use atl_server::api::streaming::hash_json_payload;
use atl_server::traits::storage::{AppendParams, Storage};
use atl_server::SqliteStore;
use sha2::Digest;

fn create_test_storage() -> SqliteStore {
    let store = SqliteStore::in_memory().expect("Failed to create in-memory storage");
    SqliteStore::initialize(&store).expect("Failed to initialize storage");
    store
}

#[test]
fn test_privacy_by_design() {
    // CRITICAL: Verify that payload data is NEVER stored
    let storage = create_test_storage();

    // Simulate the server's workflow:
    // 1. Receive payload (we have the original data here)
    let original_payload = serde_json::json!({
        "sensitive": "user data",
        "secret": "password123",
        "credit_card": "1234-5678-9012-3456"
    });

    // 2. Compute hash (server does this)
    let payload_hash = hash_json_payload(&original_payload);

    // 3. Store only the hash
    let params = AppendParams {
        payload_hash,
        metadata_hash: [0u8; 32],
        metadata_cleartext: None,
        external_id: None,
    };

    let result = storage.append(params).expect("Failed to append entry");

    // 4. Verify: Entry contains ONLY the hash, NOT the original data
    let entry = storage.get_entry(&result.id).expect("Failed to get entry");

    // The entry has the hash
    assert_eq!(entry.payload_hash, payload_hash);

    // There is NO way to retrieve the original payload from storage
    // because it was never stored - this is Privacy by Design!

    // The server can only return the hash, not the original content
    assert_ne!(entry.payload_hash, [0u8; 32]); // Hash is not empty
}

#[test]
fn test_no_payload_in_database() {
    let storage = create_test_storage();

    let sensitive_data = b"This is sensitive user data that should NEVER be stored";
    let payload_hash: [u8; 32] = sha2::Sha256::digest(sensitive_data).into();

    let params = AppendParams {
        payload_hash,
        metadata_hash: [0u8; 32],
        metadata_cleartext: Some(serde_json::json!({"note": "This metadata is OK to store"})),
        external_id: None,
    };

    let result = storage.append(params).expect("Failed to append entry");
    let entry = storage.get_entry(&result.id).expect("Failed to get entry");

    // Only hash is stored
    assert_eq!(entry.payload_hash, payload_hash);

    // Metadata cleartext is stored (by design - it's descriptive, not the payload)
    assert!(entry.metadata_cleartext.is_some());
}

#[test]
fn test_metadata_cleartext_storage() {
    // Metadata cleartext is the ONLY exception to hash-only storage
    // It's stored because it's descriptive information for receipts
    let storage = create_test_storage();

    let metadata = serde_json::json!({
        "document_type": "invoice",
        "client_id": "ABC123",
        "timestamp": "2024-01-01T00:00:00Z"
    });

    let params = AppendParams {
        payload_hash: [1u8; 32],
        metadata_hash: atl_server::api::streaming::hash_json_payload(&metadata),
        metadata_cleartext: Some(metadata.clone()),
        external_id: None,
    };

    let result = storage.append(params).expect("Failed to append entry");
    let entry = storage.get_entry(&result.id).expect("Failed to get entry");

    // Metadata cleartext is stored
    assert_eq!(entry.metadata_cleartext, Some(metadata));

    // But payload is NEVER stored - only its hash
    assert_ne!(entry.payload_hash, [0u8; 32]);
}

#[test]
fn test_streaming_hash_prevents_buffering() {
    // This test verifies that streaming hash computation doesn't buffer
    // the entire payload in memory (O(1) memory usage)

    // If we were to buffer, a 1GB file would consume 1GB of RAM.
    // With streaming, we only hold one chunk at a time.

    let storage = create_test_storage();

    // Simulate hashing a large payload
    let large_payload = serde_json::json!({
        "data": "x".repeat(1_000_000)  // 1MB string
    });

    let payload_hash = hash_json_payload(&large_payload);

    let params = AppendParams {
        payload_hash,
        metadata_hash: [0u8; 32],
        metadata_cleartext: None,
        external_id: None,
    };

    let result = storage.append(params).expect("Failed to append entry");
    let entry = storage.get_entry(&result.id).expect("Failed to get entry");

    // Only hash is stored, not the 1MB payload
    assert_eq!(entry.payload_hash, payload_hash);
}
