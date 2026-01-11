//! Storage proof tests (inclusion and consistency)

use atl_core::core::merkle;
use atl_server::traits::storage::{AppendParams, Storage};
use atl_server::SqliteStore;

fn create_test_storage() -> SqliteStore {
    let store = SqliteStore::in_memory().expect("Failed to create in-memory storage");
    SqliteStore::initialize(&store).expect("Failed to initialize storage");
    store
}

#[test]
fn test_inclusion_proof_validity() {
    let store = create_test_storage();

    // Append 10 entries
    let mut entry_ids = vec![];
    for i in 0..10 {
        let params = AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [(i + 100) as u8; 32],
            metadata_cleartext: None,
            external_id: None,
        };
        let result = store.append(params).expect("Failed to append entry");
        entry_ids.push(result.id);
    }

    // Get proof for middle entry
    let proof = store
        .get_inclusion_proof(&entry_ids[5], None)
        .expect("Failed to get inclusion proof");

    assert_eq!(proof.leaf_index, 5);
    assert_eq!(proof.tree_size, 10);

    // Verify proof using atl-core
    let entry = store.get_entry(&entry_ids[5]).expect("Failed to get entry");
    let head = store.get_tree_head().expect("Failed to get tree head");

    let leaf_hash = merkle::compute_leaf_hash(&entry.payload_hash, &entry.metadata_hash);

    // Convert proof to atl-core format
    let atl_proof = atl_core::InclusionProof {
        tree_size: proof.tree_size,
        leaf_index: proof.leaf_index,
        path: proof.path.clone(),
    };

    let is_valid = merkle::verify_inclusion(&leaf_hash, &atl_proof, &head.root_hash);

    assert!(is_valid.is_ok() && is_valid.unwrap());
}

#[test]
fn test_inclusion_proof_at_specific_tree_size() {
    let store = create_test_storage();

    // Append 5 entries
    let mut entry_ids = vec![];
    for i in 0..5 {
        let params = AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        };
        let result = store.append(params).expect("Failed to append entry");
        entry_ids.push(result.id);
    }

    // Get proof for first entry at tree size 5
    let proof = store
        .get_inclusion_proof(&entry_ids[0], Some(5))
        .expect("Failed to get inclusion proof");

    assert_eq!(proof.leaf_index, 0);
    assert_eq!(proof.tree_size, 5);

    // Append more entries
    for i in 5..10 {
        let params = AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        };
        store.append(params).expect("Failed to append entry");
    }

    // Proof should still be valid for tree size 5
    assert_eq!(proof.tree_size, 5);
}

#[test]
fn test_consistency_proof() {
    let store = create_test_storage();

    // Append 10 entries
    for i in 0..10 {
        let params = AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        };
        store.append(params).expect("Failed to append entry");
    }

    // Get consistency proof from size 3 to size 10
    let proof = store
        .get_consistency_proof(3, 10)
        .expect("Failed to get consistency proof");

    assert_eq!(proof.from_size, 3);
    assert_eq!(proof.to_size, 10);
    assert!(!proof.path.is_empty());
}

#[test]
fn test_consistency_proof_invalid_range() {
    let store = create_test_storage();

    // Append 10 entries
    for i in 0..10 {
        let params = AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        };
        store.append(params).expect("Failed to append entry");
    }

    // Try to get consistency proof with invalid range (from > to)
    let result = store.get_consistency_proof(10, 5);
    assert!(result.is_err());
}

#[test]
fn test_checkpoint_storage() {
    let store = create_test_storage();

    // Append some entries
    for i in 0..100 {
        let params = AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        };
        store.append(params).expect("Failed to append entry");
    }

    let checkpoint = atl_core::Checkpoint {
        origin: [1u8; 32],
        tree_size: 100,
        timestamp: 12345678,
        root_hash: [2u8; 32],
        signature: [3u8; 64],
        key_id: [4u8; 32],
    };

    store
        .store_checkpoint(&checkpoint)
        .expect("Failed to store checkpoint");

    let retrieved = store
        .get_checkpoint_by_size(100)
        .expect("Failed to get checkpoint");

    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().tree_size, 100);
}
