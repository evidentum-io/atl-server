//! Tests for StorageEngine

use super::*;
use tempfile::{tempdir, TempDir};

/// Returns (engine, _tempdir) - tempdir must be kept alive for engine to work
async fn create_test_engine(origin: [u8; 32]) -> (StorageEngine, TempDir) {
    let dir = tempdir().unwrap();
    let config = StorageConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let engine = StorageEngine::new(config, origin).await.unwrap();
    (engine, dir)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_engine_initialization() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    assert_eq!(engine.tree_head().tree_size, 0);
    assert!(engine.is_healthy());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_append_batch() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let params: Vec<AppendParams> = (0..100)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();

    let result = engine.append_batch(params).await.unwrap();

    assert_eq!(result.entries.len(), 100);
    assert_eq!(result.tree_head.tree_size, 100);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_crash_recovery() {
    let dir = tempdir().unwrap();
    let origin = [42u8; 32];

    // First engine: write some data
    {
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let engine = StorageEngine::new(config, origin).await.unwrap();

        let params = vec![AppendParams {
            payload_hash: [42u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];
        engine.append_batch(params).await.unwrap();
    }
    // Engine dropped, simulating crash

    // Second engine: should recover
    let config = StorageConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let engine = StorageEngine::new(config, origin).await.unwrap();

    assert_eq!(engine.tree_head().tree_size, 1);

    // Entry should be queryable
    let entry = engine.get_entry_by_index(0).await.unwrap();
    assert_eq!(entry.payload_hash, [42u8; 32]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_inclusion_proof_verification() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Append 10 entries
    let params: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    // Get proof for entry 5 (using explicit trait qualification to avoid ambiguity)
    use crate::traits::storage::Storage;
    let entry_uuid = engine.get_entry_by_index(5).await.unwrap().id;
    let proof = Storage::get_inclusion_proof(&engine, &entry_uuid, None).unwrap();
    let entry = Storage::get_entry(&engine, &entry_uuid).unwrap();
    let head = engine.tree_head();

    // Verify with atl-core
    let leaf_hash = atl_core::compute_leaf_hash(&entry.payload_hash, &entry.metadata_hash);

    // Debug output before moving
    let path_len = proof.path.len();
    eprintln!("leaf_hash: {:02x?}", &leaf_hash[..8]);
    eprintln!("root_hash: {:02x?}", &head.root_hash[..8]);
    eprintln!("tree_size: {}", head.tree_size);
    eprintln!("proof.tree_size: {}", proof.tree_size);
    eprintln!("proof.leaf_index: {}", proof.leaf_index);
    eprintln!("proof.path len: {}", path_len);

    let atl_proof = atl_core::InclusionProof {
        tree_size: proof.tree_size,
        leaf_index: proof.leaf_index,
        path: proof.path,
    };

    let result = atl_core::verify_inclusion(&leaf_hash, &atl_proof, &head.root_hash);
    eprintln!("result: {:?}", result);

    assert!(result.is_ok() && result.unwrap());
}

// Tests for StorageEngine::rotate_tree()

#[tokio::test(flavor = "multi_thread")]
async fn test_rotate_tree_inserts_genesis_into_slab() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Create initial active tree (normally done by tree_closer)
    let origin_id = engine.origin_id();
    {
        let index = engine.index.lock().await;
        index.create_active_tree(&origin_id, 0).unwrap();
    }

    // Append some entries first
    let params: Vec<AppendParams> = (0..100)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    let head_before = engine.tree_head();
    let end_size = head_before.tree_size;
    let root_hash = head_before.root_hash;

    // Get slab tree size before rotation
    let tree_size_before = {
        let slabs = engine.slabs.read().await;
        slabs.tree_size()
    };

    // Rotate tree
    let result = engine
        .rotate_tree(&origin_id, end_size, &root_hash)
        .await
        .unwrap();

    // Get slab tree size after rotation
    let tree_size_after = {
        let slabs = engine.slabs.read().await;
        slabs.tree_size()
    };

    // Assert: slab tree_size increased by 1 (genesis leaf inserted)
    assert_eq!(tree_size_after, tree_size_before + 1);
    assert_eq!(result.new_tree_head.tree_size, end_size + 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rotate_tree_inserts_genesis_into_sqlite() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Create initial active tree (normally done by tree_closer)
    let origin_id = engine.origin_id();
    {
        let index = engine.index.lock().await;
        index.create_active_tree(&origin_id, 0).unwrap();
    }

    // Append some entries first
    let params: Vec<AppendParams> = (0..50)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    let head_before = engine.tree_head();
    let end_size = head_before.tree_size;
    let root_hash = head_before.root_hash;

    // Rotate tree
    engine
        .rotate_tree(&origin_id, end_size, &root_hash)
        .await
        .unwrap();

    // Query genesis entry from SQLite
    let genesis_entry = engine.get_entry_by_index(end_size).await.unwrap();

    // Assert: entry exists at leaf_index = end_size
    assert_eq!(genesis_entry.leaf_index, Some(end_size));

    // Assert: entry has type="genesis" metadata
    let metadata = genesis_entry.metadata_cleartext.unwrap();
    let meta_type = metadata.get("type").unwrap().as_str().unwrap();
    assert_eq!(meta_type, "genesis");

    // Assert: metadata contains prev_tree info
    assert!(metadata.get("prev_tree_id").is_some());
    assert!(metadata.get("prev_root_hash").is_some());
    assert!(metadata.get("prev_tree_size").is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rotate_tree_updates_cache() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Create initial active tree (normally done by tree_closer)
    let origin_id = engine.origin_id();
    {
        let index = engine.index.lock().await;
        index.create_active_tree(&origin_id, 0).unwrap();
    }

    // Append some entries first
    let params: Vec<AppendParams> = (0..25)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    let head_before = engine.tree_head();
    let end_size = head_before.tree_size;
    let root_hash = head_before.root_hash;
    let old_root = head_before.root_hash;

    // Rotate tree
    engine
        .rotate_tree(&origin_id, end_size, &root_hash)
        .await
        .unwrap();

    let head_after = engine.tree_head();

    // Assert: tree_size increased by 1
    assert_eq!(head_after.tree_size, end_size + 1);

    // Assert: root_hash changed (genesis leaf changes the tree structure)
    assert_ne!(head_after.root_hash, old_root);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rotate_tree_returns_correct_metadata() {
    let (engine, _dir) = create_test_engine([42u8; 32]).await;

    // Create initial active tree (normally done by tree_closer)
    let origin_id = engine.origin_id();
    {
        let index = engine.index.lock().await;
        index.create_active_tree(&origin_id, 0).unwrap();
    }

    // Append some entries first
    let params: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    let head_before = engine.tree_head();
    let end_size = head_before.tree_size;
    let root_hash = head_before.root_hash;

    // Rotate tree
    let result = engine
        .rotate_tree(&origin_id, end_size, &root_hash)
        .await
        .unwrap();

    // Assert: genesis_leaf_index == end_size
    assert_eq!(result.genesis_leaf_index, end_size);

    // Assert: closed_tree_metadata has correct values
    assert_eq!(result.closed_tree_metadata.tree_size, end_size);
    assert_eq!(result.closed_tree_metadata.root_hash, root_hash);
    assert_eq!(result.closed_tree_metadata.origin_id, origin_id);

    // Assert: new_tree_head matches current tree_head()
    let head_after = engine.tree_head();
    assert_eq!(result.new_tree_head.tree_size, head_after.tree_size);
    assert_eq!(result.new_tree_head.root_hash, head_after.root_hash);
    assert_eq!(result.new_tree_head.origin, head_after.origin);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_genesis_hash_matches_atl_core() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Create initial active tree (normally done by tree_closer)
    let origin_id = engine.origin_id();
    {
        let index = engine.index.lock().await;
        index.create_active_tree(&origin_id, 0).unwrap();
    }

    // Append some entries first
    let params: Vec<AppendParams> = (0..5)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    let head_before = engine.tree_head();
    let end_size = head_before.tree_size;
    let root_hash = head_before.root_hash;

    // Compute expected genesis hash using atl-core
    let expected_genesis_hash = atl_core::compute_genesis_leaf_hash(&root_hash, end_size);

    // Rotate tree
    engine
        .rotate_tree(&origin_id, end_size, &root_hash)
        .await
        .unwrap();

    // Get genesis entry from SQLite
    let genesis_entry = engine.get_entry_by_index(end_size).await.unwrap();

    // Assert: entry.payload_hash matches computed genesis hash
    assert_eq!(genesis_entry.payload_hash, expected_genesis_hash);
    assert_eq!(genesis_entry.metadata_hash, expected_genesis_hash);
}
