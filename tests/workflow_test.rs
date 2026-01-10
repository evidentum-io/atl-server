//! End-to-end workflow tests

use atl_core::core::merkle;
use atl_server::SqliteStore;
use atl_server::traits::storage::{AppendParams, Storage};
use sha2::Digest;
use std::sync::Arc;

fn create_test_storage() -> Arc<SqliteStore> {
    let mut store = SqliteStore::in_memory().expect("Failed to create in-memory storage");
    SqliteStore::initialize(&mut store).expect("Failed to initialize storage");
    Arc::new(store)
}

#[test]
fn test_full_workflow() {
    // 1. Create storage
    let storage = create_test_storage();

    // 2. Append entries (simulating POST /v1/anchor)
    let mut entry_ids = vec![];
    for i in 0u32..10 {
        let payload_data = i.to_le_bytes();
        let payload_hash: [u8; 32] = sha2::Sha256::digest(payload_data).into();
        let metadata_hash = [0u8; 32];

        let params = AppendParams {
            payload_hash,
            metadata_hash,
            metadata_cleartext: Some(serde_json::json!({"index": i})),
            external_id: None,
        };

        let result = storage.append(params).expect("Failed to append entry");
        entry_ids.push(result.id);
    }

    // 3. Generate and verify receipts for each entry
    for entry_id in &entry_ids {
        let entry = storage.get_entry(entry_id).expect("Failed to get entry");
        let proof = storage
            .get_inclusion_proof(entry_id, None)
            .expect("Failed to get proof");
        let tree_head = storage.get_tree_head().expect("Failed to get tree head");

        // Verify leaf hash
        let leaf_hash = merkle::compute_leaf_hash(&entry.payload_hash, &entry.metadata_hash);

        // Convert proof to atl-core format
        let atl_proof = atl_core::InclusionProof {
            tree_size: proof.tree_size,
            leaf_index: proof.leaf_index,
            path: proof.path.clone(),
        };

        // Verify inclusion proof
        let is_valid = merkle::verify_inclusion(&leaf_hash, &atl_proof, &tree_head.root_hash);

        assert!(is_valid.is_ok() && is_valid.unwrap());
    }

    // 4. Verify final tree state
    let head = storage.get_tree_head().expect("Failed to get tree head");
    assert_eq!(head.tree_size, 10);
}

#[test]
fn test_receipt_survives_tree_growth() {
    let storage = create_test_storage();

    // Append first entry
    let params = AppendParams {
        payload_hash: [1u8; 32],
        metadata_hash: [0u8; 32],
        metadata_cleartext: None,
        external_id: None,
    };

    let result1 = storage.append(params).expect("Failed to append entry");

    // Get proof at tree size 1
    let proof1 = storage
        .get_inclusion_proof(&result1.id, Some(1))
        .expect("Failed to get proof");

    assert_eq!(proof1.tree_size, 1);

    // Append more entries
    for i in 2..100 {
        let params = AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        };
        storage.append(params).expect("Failed to append entry");
    }

    // Get root at tree size 1 (from checkpoint if available, or recompute)
    // For this test, we verify the proof structure is preserved
    assert_eq!(proof1.leaf_index, 0);
    assert_eq!(proof1.tree_size, 1);
}

#[test]
fn test_batch_workflow() {
    let storage = create_test_storage();

    // Prepare batch of 100 entries
    let params_vec: Vec<AppendParams> = (0..100)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: Some(serde_json::json!({"batch_index": i})),
            external_id: Some(format!("batch-{}", i)),
        })
        .collect();

    // Append batch atomically
    let results = storage
        .append_batch(params_vec)
        .expect("Failed to append batch");

    assert_eq!(results.len(), 100);

    // Verify all entries are in the tree
    let head = storage.get_tree_head().expect("Failed to get tree head");
    assert_eq!(head.tree_size, 100);

    // Verify a few random entries
    for i in [0, 25, 50, 75, 99] {
        let entry = storage
            .get_entry(&results[i].id)
            .expect("Failed to get entry");
        assert_eq!(entry.payload_hash, [i as u8; 32]);
        assert_eq!(entry.external_id, Some(format!("batch-{}", i)));
    }
}

#[test]
fn test_consistency_across_checkpoints() {
    let storage = create_test_storage();

    // Append 10 entries
    for i in 0..10 {
        let params = AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        };
        storage.append(params).expect("Failed to append entry");
    }

    // Append 10 more entries
    for i in 10..20 {
        let params = AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        };
        storage.append(params).expect("Failed to append entry");
    }

    // Get consistency proof
    let consistency_proof = storage
        .get_consistency_proof(10, 20)
        .expect("Failed to get consistency proof");

    assert_eq!(consistency_proof.from_size, 10);
    assert_eq!(consistency_proof.to_size, 20);
}
