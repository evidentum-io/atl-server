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

    // Get data tree size before rotation
    let tree_size_before = {
        let slabs = engine.slabs.read().await;
        slabs.tree_size()
    };

    // Rotate tree
    let result = engine
        .rotate_tree(&origin_id, end_size, &root_hash)
        .await
        .unwrap();

    // Get data tree size after rotation
    let tree_size_after = {
        let slabs = engine.slabs.read().await;
        slabs.tree_size()
    };

    // Assert: data tree size unchanged (NO genesis leaf anymore)
    assert_eq!(tree_size_after, tree_size_before);
    assert_eq!(result.new_tree_head.tree_size, end_size); // NO +1!

    // Assert: Super-Tree has grown by 1
    let super_size = engine.index.lock().await.get_super_tree_size().unwrap();
    assert_eq!(super_size, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rotate_tree_does_not_insert_genesis() {
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
    let result = engine
        .rotate_tree(&origin_id, end_size, &root_hash)
        .await
        .unwrap();

    // Assert: NO entry exists at leaf_index = end_size (no genesis)
    let entry_result = engine.get_entry_by_index(end_size).await;
    assert!(
        entry_result.is_err(),
        "No genesis entry should exist at end_size"
    );

    // Assert: Super-Tree contains the data tree root
    let mut super_slabs = engine.super_slabs.write().await;
    let super_leaf = super_slabs.get_node(0, 0).unwrap().unwrap();
    assert_eq!(super_leaf, root_hash);

    // Assert: data_tree_index is correct
    assert_eq!(result.data_tree_index, 0);
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

    // Assert: tree_size stays the same (NO genesis leaf)
    assert_eq!(head_after.tree_size, end_size);

    // Assert: root_hash stays the same (no genesis, new tree starts empty)
    assert_eq!(head_after.root_hash, old_root);
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

    // Assert: data_tree_index is set (first rotation should be 0)
    assert_eq!(result.data_tree_index, 0);

    // Assert: super_root is computed
    assert_ne!(result.super_root, [0u8; 32]);

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
async fn test_rotate_tree_appends_to_super_tree() {
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

    // Rotate tree
    let result = engine
        .rotate_tree(&origin_id, end_size, &root_hash)
        .await
        .unwrap();

    // Assert: Super-Tree contains the closed tree root
    let mut super_slabs = engine.super_slabs.write().await;
    let super_leaf = super_slabs.get_node(0, 0).unwrap().unwrap();
    assert_eq!(super_leaf, root_hash);

    // Assert: Super-Tree size is 1
    let super_size = engine.index.lock().await.get_super_tree_size().unwrap();
    assert_eq!(super_size, 1);

    // Assert: Super genesis root is set
    let super_genesis = engine.index.lock().await.get_super_genesis_root().unwrap();
    assert_eq!(super_genesis, Some(result.super_root));
}

// Tests for SUPER-1: Super-Tree Storage Integration

#[tokio::test(flavor = "multi_thread")]
async fn test_super_slabs_directory_created_on_startup() {
    let dir = tempdir().unwrap();
    let config = StorageConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let expected_path = dir.path().join("super_tree").join("slabs");

    let _storage = StorageEngine::new(config, [0u8; 32]).await.unwrap();

    assert!(
        expected_path.exists(),
        "Super-Tree slab directory should exist"
    );
    assert!(expected_path.is_dir(), "Should be a directory");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_super_genesis_root_initially_none() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let result = engine
        .index_store()
        .lock()
        .await
        .get_super_genesis_root()
        .unwrap();

    assert!(
        result.is_none(),
        "Genesis root should be None before first rotation"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_super_genesis_root_once_only() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;
    let first_root = [1u8; 32];
    let second_root = [2u8; 32];

    {
        let index_store = engine.index_store();
        let index = index_store.lock().await;
        index.set_super_genesis_root(&first_root).unwrap();
        index.set_super_genesis_root(&second_root).unwrap();
    }
    let result = engine
        .index_store()
        .lock()
        .await
        .get_super_genesis_root()
        .unwrap();

    assert_eq!(
        result,
        Some(first_root),
        "Genesis should be first value, not overwritten"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_super_tree_size_initially_zero() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let size = engine
        .index_store()
        .lock()
        .await
        .get_super_tree_size()
        .unwrap();

    assert_eq!(size, 0, "Size should be 0 before any rotations");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_super_tree_size_increments_correctly() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    {
        let index_store = engine.index_store();
        let index = index_store.lock().await;
        index.set_super_tree_size(1).unwrap();
        index.set_super_tree_size(2).unwrap();
        index.set_super_tree_size(3).unwrap();
    }
    let result = engine
        .index_store()
        .lock()
        .await
        .get_super_tree_size()
        .unwrap();

    assert_eq!(result, 3, "Size should be last set value");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_storage_engine_has_super_slabs_accessor() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let super_slabs = engine.super_slabs();

    assert!(
        Arc::strong_count(super_slabs) >= 1,
        "super_slabs should be accessible"
    );
}
