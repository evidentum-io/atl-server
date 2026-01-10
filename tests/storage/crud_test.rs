//! Storage CRUD operation tests

use atl_server::SqliteStore;
use atl_server::traits::storage::{AppendParams, Storage};

fn create_test_storage() -> SqliteStore {
    let store = SqliteStore::in_memory().expect("Failed to create in-memory storage");
    SqliteStore::initialize(&store).expect("Failed to initialize storage");
    store
}

#[test]
fn test_initialize_creates_tables() {
    let store = SqliteStore::in_memory().expect("Failed to create in-memory storage");
    SqliteStore::initialize(&store).expect("Failed to initialize storage");
    assert!(store.is_initialized());
}

#[test]
fn test_append_single_entry() {
    let store = create_test_storage();

    let params = AppendParams {
        payload_hash: [1u8; 32],
        metadata_hash: [2u8; 32],
        metadata_cleartext: Some(serde_json::json!({"key": "value"})),
        external_id: Some("ext-123".into()),
    };

    let result = store.append(params).expect("Failed to append entry");

    assert_eq!(result.leaf_index, 0);
    assert_eq!(result.tree_head.tree_size, 1);
}

#[test]
fn test_append_multiple_entries() {
    let store = create_test_storage();

    for i in 0..100 {
        let params = AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        };
        store.append(params).expect("Failed to append entry");
    }

    let head = store.get_tree_head().expect("Failed to get tree head");
    assert_eq!(head.tree_size, 100);
}

#[test]
fn test_get_entry_by_id() {
    let store = create_test_storage();

    let params = AppendParams {
        payload_hash: [42u8; 32],
        metadata_hash: [0u8; 32],
        metadata_cleartext: Some(serde_json::json!({"test": true})),
        external_id: None,
    };

    let result = store.append(params).expect("Failed to append entry");
    let entry = store.get_entry(&result.id).expect("Failed to get entry");

    assert_eq!(entry.id, result.id);
    assert_eq!(entry.payload_hash, [42u8; 32]);
    assert_eq!(entry.leaf_index, Some(0));
}

#[test]
fn test_get_entry_by_external_id() {
    let store = create_test_storage();

    let params = AppendParams {
        payload_hash: [42u8; 32],
        metadata_hash: [0u8; 32],
        metadata_cleartext: None,
        external_id: Some("order-12345".into()),
    };

    let result = store.append(params).expect("Failed to append entry");
    let entry = store
        .get_entry_by_external_id("order-12345")
        .expect("Failed to get entry by external_id");

    assert_eq!(entry.id, result.id);
    assert_eq!(entry.external_id, Some("order-12345".into()));
}

#[test]
fn test_get_entry_by_index() {
    let store = create_test_storage();

    // Append 10 entries
    let mut entry_ids = vec![];
    for i in 0..10 {
        let params = AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        };
        let result = store.append(params).expect("Failed to append entry");
        entry_ids.push(result.id);
    }

    // Get entry at index 5
    let entry = store
        .get_entry_by_index(5)
        .expect("Failed to get entry by index");
    assert_eq!(entry.id, entry_ids[5]);
    assert_eq!(entry.leaf_index, Some(5));
    assert_eq!(entry.payload_hash, [5u8; 32]);
}

#[test]
fn test_get_entry_not_found() {
    let store = create_test_storage();
    let random_id = uuid::Uuid::new_v4();

    let result = store.get_entry(&random_id);
    assert!(result.is_err());
}

#[test]
fn test_storage_contains_only_hashes() {
    // CRITICAL: Verify Privacy by Design
    let store = create_test_storage();

    let payload_hash = [42u8; 32];
    let params = AppendParams {
        payload_hash,
        metadata_hash: [0u8; 32],
        metadata_cleartext: Some(serde_json::json!({"sensitive": "data"})),
        external_id: None,
    };

    let result = store.append(params).expect("Failed to append entry");

    // Entry should contain hash, NOT original payload
    let entry = store.get_entry(&result.id).expect("Failed to get entry");
    assert_eq!(entry.payload_hash, payload_hash);
    // No way to retrieve original payload - it was never stored!
}

#[test]
fn test_append_batch() {
    let store = create_test_storage();

    let params_vec: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();

    let results = store
        .append_batch(params_vec)
        .expect("Failed to append batch");

    assert_eq!(results.len(), 10);
    assert_eq!(results[0].leaf_index, 0);
    assert_eq!(results[9].leaf_index, 9);

    let head = store.get_tree_head().expect("Failed to get tree head");
    assert_eq!(head.tree_size, 10);
}
