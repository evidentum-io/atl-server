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

    // Verify data_tree_index (first rotation should be 0)
    assert_eq!(result.data_tree_index, 0);
    assert_eq!(result.new_tree_head.tree_size, 10); // NO +1 for genesis!

    // Verify Super-Tree contains the closed tree root
    let super_slab = engine.super_slab().read().await;
    let super_leaf = super_slab.get_node(0, 0).unwrap();
    assert_eq!(super_leaf, tree_head_before.root_hash);

    // Verify Super-Tree size is 1
    let index = engine.index_store();
    let idx = index.lock().await;
    let super_size = idx.get_super_tree_size().unwrap();
    assert_eq!(super_size, 1);

    // Verify Super genesis root is set
    let super_genesis = idx.get_super_genesis_root().unwrap();
    assert_eq!(super_genesis, Some(result.super_root));
}

#[tokio::test(flavor = "multi_thread")]
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

    // Final tree size: 5 + 5 = 10 (NO genesis leaves anymore)
    let final_head = engine.tree_head();
    assert_eq!(final_head.tree_size, 10);

    // Verify inclusion proofs for all entries (NO genesis entries)
    for i in 0..10 {
        let entry = ProofProvider::get_entry_by_index(&engine, i).await.unwrap();
        let proof = Storage::get_inclusion_proof(&engine, &entry.id, Some(10)).unwrap();
        assert_eq!(proof.leaf_index, i);

        // All entries are regular entries (no genesis)
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
async fn test_super_tree_stores_data_tree_root() {
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

    // Verify Super-Tree contains the closed tree root (NOT genesis leaf hash)
    let super_slab = engine.super_slab().read().await;
    let super_leaf = super_slab.get_node(0, 0).unwrap();
    assert_eq!(
        super_leaf, head_before.root_hash,
        "Super-Tree should store the data tree root directly"
    );

    // Verify data_tree_index is correct
    assert_eq!(
        result.data_tree_index, 0,
        "First rotation should have data_tree_index=0"
    );

    // Verify Super-Tree metadata
    let index = engine.index_store();
    let idx = index.lock().await;
    let super_size = idx.get_super_tree_size().unwrap();
    assert_eq!(super_size, 1);
    let super_genesis = idx.get_super_genesis_root().unwrap();
    assert_eq!(super_genesis, Some(result.super_root));
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

    // Verify chain links via Super-Tree (prev_tree_id removed in v2.0)
    // Chain tracking is now implicit through data_tree_index in Super-Tree
    let index = engine.index_store();
    let idx = index.lock().await;

    // Verify trees exist and have correct data_tree_index
    let tree1 = idx.get_tree(tree_ids[0].0).unwrap().unwrap();
    let tree2 = idx.get_tree(tree_ids[1].0).unwrap().unwrap();
    let tree3 = idx.get_tree(tree_ids[2].0).unwrap().unwrap();

    // Verify data_tree_index is sequential (implicit chain)
    let index1 = idx.get_tree_data_tree_index(tree1.id).unwrap().unwrap();
    let index2 = idx.get_tree_data_tree_index(tree2.id).unwrap().unwrap();
    let index3 = idx.get_tree_data_tree_index(tree3.id).unwrap().unwrap();

    assert_eq!(index1, 0);
    assert_eq!(index2, 1);
    assert_eq!(index3, 2);

    // Verify Super-Tree contains all three tree roots in order
    let super_slab = engine.super_slab().read().await;
    for (i, root_hash) in root_hashes.iter().enumerate() {
        let super_leaf = super_slab.get_node(0, i as u64).unwrap();
        assert_eq!(super_leaf, *root_hash, "Super-Tree leaf {} mismatch", i);
    }

    // Chain tracking moved to Super-Tree (no more prev_tree_id)
}

#[tokio::test(flavor = "multi_thread")]
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

    // Rotate tree (NO genesis, tree_size stays 10)
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

    let head_at_15 = engine.tree_head();
    assert_eq!(head_at_15.tree_size, 15); // 10 + 5, NO genesis

    // Get consistency proof from 10 to 15
    let proof = Storage::get_consistency_proof(&engine, 10, 15).unwrap();
    assert_eq!(proof.from_size, 10);
    assert_eq!(proof.to_size, 15);

    // Verify proof
    let atl_proof = atl_core::ConsistencyProof {
        from_size: proof.from_size,
        to_size: proof.to_size,
        path: proof.path,
    };
    let is_valid =
        atl_core::verify_consistency(&atl_proof, &root_at_10, &head_at_15.root_hash).unwrap();
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

    // Create mock Chain Index
    let chain_index_dir = tempfile::tempdir().unwrap();
    let chain_index_path = chain_index_dir.path().join("chain_index.db");
    let chain_index =
        atl_server::storage::chain_index::ChainIndex::open(&chain_index_path).unwrap();
    let chain_index = std::sync::Arc::new(tokio::sync::Mutex::new(chain_index));

    // Run tree_closer logic with tree_lifetime_secs = 0 (close immediately)
    atl_server::background::tree_closer::logic::check_and_close_if_needed(
        &index,
        &storage_trait,
        &rotator,
        &chain_index,
        0, // Close immediately
    )
    .await
    .unwrap();

    // Verify NO genesis leaf at index 10 (genesis removed in SUPER-2)
    let idx = index.lock().await;
    let maybe_genesis = idx.get_entry_by_index(10).unwrap();
    assert!(
        maybe_genesis.is_none(),
        "Genesis leaf should NOT exist after tree_closer (removed in SUPER-2)"
    );

    // Verify Super-Tree was updated
    let super_size = idx.get_super_tree_size().unwrap();
    assert_eq!(
        super_size, 1,
        "Super-Tree should have 1 entry (the closed data tree root)"
    );
}
