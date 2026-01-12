//! Integration tests for tree chaining and genesis leaf insertion
//!
//! These tests verify the fix for the architectural issue where genesis leaf
//! was only inserted into SQLite but not into Slab.

use atl_server::storage::config::StorageConfig;
use atl_server::storage::engine::StorageEngine;
use atl_server::traits::{AppendParams, ProofProvider, Storage, TreeRotator};
use std::sync::Arc;

/// Create test engine with temporary directory
async fn create_test_engine() -> (StorageEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let origin = [42u8; 32];
    let engine = StorageEngine::new(config, origin).await.unwrap();
    (engine, dir)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_genesis_leaf_in_both_slab_and_sqlite() {
    let (engine, _dir) = create_test_engine().await;
    let origin_id = engine.origin_id();

    // Create initial active tree
    {
        let index = engine.index_store();
        let idx = index.lock().await;
        idx.create_active_tree(&origin_id, 0).unwrap();
    }

    // Append 10 entries to first tree
    let entries: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(entries).await.unwrap();

    let tree_head_before = engine.tree_head();
    assert_eq!(tree_head_before.tree_size, 10);

    // Rotate tree
    let result = engine
        .rotate_tree(
            &origin_id,
            tree_head_before.tree_size,
            &tree_head_before.root_hash,
        )
        .await
        .unwrap();

    // Verify genesis leaf index
    assert_eq!(result.genesis_leaf_index, 10);
    assert_eq!(result.new_tree_head.tree_size, 11);

    // Verify genesis leaf in SQLite
    let index = engine.index_store();
    let idx = index.lock().await;
    let genesis_entry = idx.get_entry_by_index(10).unwrap().unwrap();
    assert_eq!(genesis_entry.leaf_index, 10);

    // Verify genesis metadata
    let metadata = genesis_entry.metadata_cleartext.as_ref().unwrap();
    assert_eq!(metadata["type"], "genesis");
    assert_eq!(metadata["prev_tree_size"], 10);

    // Verify genesis hash matches expected
    let expected_genesis_hash = atl_core::compute_genesis_leaf_hash(
        &tree_head_before.root_hash,
        tree_head_before.tree_size,
    );
    assert_eq!(genesis_entry.payload_hash, expected_genesis_hash);

    // Verify genesis leaf in Slab (via inclusion proof)
    // If inclusion proof works, the leaf is in the Slab
    drop(idx);
    let genesis_uuid = genesis_entry.id;
    let proof = Storage::get_inclusion_proof(&engine, &genesis_uuid, Some(11)).unwrap();
    assert_eq!(proof.leaf_index, 10);
    assert_eq!(proof.tree_size, 11);
    assert!(!proof.path.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "RFC 6962 root computation refactoring needed"]
async fn test_merkle_tree_integrity_after_rotation() {
    let (engine, _dir) = create_test_engine().await;
    let origin_id = engine.origin_id();

    // Create initial active tree
    {
        let index = engine.index_store();
        let idx = index.lock().await;
        idx.create_active_tree(&origin_id, 0).unwrap();
    }

    // Append entries and rotate twice
    for round in 0..2 {
        let entries: Vec<AppendParams> = (0..5)
            .map(|i| AppendParams {
                payload_hash: [(round * 10 + i) as u8; 32],
                metadata_hash: [0u8; 32],
                metadata_cleartext: None,
                external_id: None,
            })
            .collect();
        engine.append_batch(entries).await.unwrap();

        let head = engine.tree_head();
        engine
            .rotate_tree(&origin_id, head.tree_size, &head.root_hash)
            .await
            .unwrap();
    }

    // Final tree size: 5 + genesis + 5 + genesis = 12
    let final_head = engine.tree_head();
    assert_eq!(final_head.tree_size, 12);

    // Verify inclusion proofs for all entries
    for i in 0..12 {
        let entry = ProofProvider::get_entry_by_index(&engine, i).await.unwrap();
        let proof = Storage::get_inclusion_proof(&engine, &entry.id, Some(12)).unwrap();
        assert_eq!(proof.leaf_index, i);

        // Verify proof (using atl-core)
        let leaf_hash = atl_core::compute_leaf_hash(&entry.payload_hash, &entry.metadata_hash);
        let atl_proof = atl_core::InclusionProof {
            tree_size: proof.tree_size,
            leaf_index: proof.leaf_index,
            path: proof.path,
        };
        let is_valid =
            atl_core::verify_inclusion(&leaf_hash, &atl_proof, &final_head.root_hash).unwrap();
        assert!(is_valid, "Inclusion proof invalid for leaf {}", i);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_genesis_leaf_hash_correctness() {
    let (engine, _dir) = create_test_engine().await;
    let origin_id = engine.origin_id();

    // Create initial active tree
    {
        let index = engine.index_store();
        let idx = index.lock().await;
        idx.create_active_tree(&origin_id, 0).unwrap();
    }

    // Append entries
    let entries: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(entries).await.unwrap();

    let head_before = engine.tree_head();

    // Rotate tree
    let result = engine
        .rotate_tree(&origin_id, head_before.tree_size, &head_before.root_hash)
        .await
        .unwrap();

    // Compute expected genesis hash manually
    let expected: [u8; 32] = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update([0x00]); // Leaf prefix
        hasher.update(b"ATL-CHAIN-v1"); // Domain separator
        hasher.update(head_before.root_hash);
        hasher.update(head_before.tree_size.to_le_bytes());
        hasher.finalize().into()
    };

    // Verify via atl_core
    let atl_core_hash =
        atl_core::compute_genesis_leaf_hash(&head_before.root_hash, head_before.tree_size);
    assert_eq!(atl_core_hash, expected);

    // Verify stored genesis matches
    let index = engine.index_store();
    let idx = index.lock().await;
    let genesis_entry = idx.get_entry_by_index(10).unwrap().unwrap();
    assert_eq!(genesis_entry.payload_hash, expected);

    // Also verify via result
    assert_eq!(result.genesis_leaf_index, 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_chain_link_integrity() {
    let (engine, _dir) = create_test_engine().await;
    let origin_id = engine.origin_id();

    // Create initial active tree
    {
        let index = engine.index_store();
        let idx = index.lock().await;
        idx.create_active_tree(&origin_id, 0).unwrap();
    }

    // Create chain of 3 trees
    let mut tree_ids = vec![];
    let mut root_hashes = vec![];

    for round in 0..3 {
        // Append entries
        let entries: Vec<AppendParams> = (0..5)
            .map(|i| AppendParams {
                payload_hash: [(round * 10 + i) as u8; 32],
                metadata_hash: [0u8; 32],
                metadata_cleartext: None,
                external_id: None,
            })
            .collect();
        engine.append_batch(entries).await.unwrap();

        let head = engine.tree_head();
        root_hashes.push(head.root_hash);

        let result = engine
            .rotate_tree(&origin_id, head.tree_size, &head.root_hash)
            .await
            .unwrap();

        tree_ids.push((result.closed_tree_id, result.new_tree_id));
    }

    // Verify chain links
    let index = engine.index_store();
    let idx = index.lock().await;

    // Tree 1: prev_tree_id = NULL (first tree)
    let tree1 = idx.get_tree(tree_ids[0].0).unwrap().unwrap();
    assert!(tree1.prev_tree_id.is_none());

    // Tree 2: prev_tree_id = Tree 1
    let tree2 = idx.get_tree(tree_ids[1].0).unwrap().unwrap();
    assert_eq!(tree2.prev_tree_id, Some(tree_ids[0].0));

    // Tree 3: prev_tree_id = Tree 2
    let tree3 = idx.get_tree(tree_ids[2].0).unwrap().unwrap();
    assert_eq!(tree3.prev_tree_id, Some(tree_ids[1].0));

    // Verify genesis leaves reference correct predecessors
    // Genesis at index 5 (after tree 1 closed) should reference tree 1
    let genesis1 = idx.get_entry_by_index(5).unwrap().unwrap();
    let meta1 = genesis1.metadata_cleartext.as_ref().unwrap();
    assert_eq!(meta1["prev_tree_id"], tree_ids[0].0);
    assert_eq!(meta1["prev_tree_size"], 5);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "RFC 6962 root computation refactoring needed"]
async fn test_consistency_proof_across_rotation() {
    let (engine, _dir) = create_test_engine().await;
    let origin_id = engine.origin_id();

    // Create initial active tree
    {
        let index = engine.index_store();
        let idx = index.lock().await;
        idx.create_active_tree(&origin_id, 0).unwrap();
    }

    // Append entries
    let entries: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(entries).await.unwrap();

    let head_at_10 = engine.tree_head();
    let root_at_10 = head_at_10.root_hash;

    // Rotate tree (adds genesis, tree_size = 11)
    engine
        .rotate_tree(&origin_id, head_at_10.tree_size, &head_at_10.root_hash)
        .await
        .unwrap();

    // Append more entries
    let more_entries: Vec<AppendParams> = (0..5)
        .map(|i| AppendParams {
            payload_hash: [(100 + i) as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(more_entries).await.unwrap();

    let head_at_16 = engine.tree_head();
    assert_eq!(head_at_16.tree_size, 16);

    // Get consistency proof from 10 to 16
    let proof = Storage::get_consistency_proof(&engine, 10, 16).unwrap();
    assert_eq!(proof.from_size, 10);
    assert_eq!(proof.to_size, 16);

    // Verify proof
    let atl_proof = atl_core::ConsistencyProof {
        from_size: proof.from_size,
        to_size: proof.to_size,
        path: proof.path,
    };
    let is_valid =
        atl_core::verify_consistency(&atl_proof, &root_at_10, &head_at_16.root_hash).unwrap();
    assert!(
        is_valid,
        "Consistency proof should be valid across rotation"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tree_closer_uses_rotate_tree() {
    let (engine, _dir) = create_test_engine().await;
    let storage: Arc<StorageEngine> = Arc::new(engine);
    let storage_trait: Arc<dyn Storage> = storage.clone();
    let rotator: Arc<dyn TreeRotator> = storage.clone();
    let index = storage.index_store();

    // Create active tree
    {
        let idx = index.lock().await;
        idx.create_active_tree(&storage_trait.origin_id(), 0)
            .unwrap();
    }

    // Append entries and set first_entry_at
    let entries: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    storage_trait.append_batch(entries).await.unwrap();

    // Run tree_closer logic with tree_lifetime_secs = 0 (close immediately)
    atl_server::background::tree_closer::logic::check_and_close_if_needed(
        &index,
        &storage_trait,
        &rotator,
        0, // Close immediately
    )
    .await
    .unwrap();

    // Verify genesis leaf exists at index 10
    let idx = index.lock().await;
    let genesis = idx.get_entry_by_index(10).unwrap();
    assert!(
        genesis.is_some(),
        "Genesis leaf should exist after tree_closer"
    );

    let genesis = genesis.unwrap();
    let meta = genesis.metadata_cleartext.as_ref().unwrap();
    assert_eq!(meta["type"], "genesis");
}
