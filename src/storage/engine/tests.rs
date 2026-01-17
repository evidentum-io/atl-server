//! Tests for StorageEngine

use super::*;
use std::sync::atomic::Ordering;
use tempfile::{tempdir, TempDir};

/// Returns (engine, _tempdir) - tempdir must be kept alive for engine to work
async fn create_test_engine(origin: [u8; 32]) -> (StorageEngine, TempDir) {
    let dir = tempdir().unwrap();
    let config = StorageConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let engine = StorageEngine::new(config, origin).await.unwrap();

    // Create initial active tree (normally done by tree_closer)
    {
        let index = engine.index_store();
        let index_lock = index.lock().await;
        index_lock.create_active_tree(&origin, 0).unwrap();
    }

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

        // Create initial active tree (normally done by tree_closer)
        {
            let index = engine.index_store();
            let index_lock = index.lock().await;
            index_lock.create_active_tree(&origin, 0).unwrap();
        }

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
        let _index = engine.index.lock().await;
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
        let _index = engine.index.lock().await;
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
    let super_slab = engine.super_slab.read().await;
    let super_leaf = super_slab.get_node(0, 0).unwrap();
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
        let _index = engine.index.lock().await;
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
        let _index = engine.index.lock().await;
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
        let _index = engine.index.lock().await;
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
    let super_slab = engine.super_slab.read().await;
    let super_leaf = super_slab.get_node(0, 0).unwrap();
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
    let expected_dir = dir.path().join("super_tree");
    let expected_file = expected_dir.join("super_tree.slab");

    let _storage = StorageEngine::new(config, [0u8; 32]).await.unwrap();

    assert!(expected_dir.exists(), "Super-Tree directory should exist");
    assert!(expected_dir.is_dir(), "Should be a directory");
    assert!(expected_file.exists(), "Super-Tree slab file should exist");
    assert!(expected_file.is_file(), "Should be a file");
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
async fn test_storage_engine_has_super_slab_accessor() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let super_slab = engine.super_slab();

    assert!(
        Arc::strong_count(super_slab) >= 1,
        "super_slab should be accessible"
    );
}

// Additional tests for better coverage

#[tokio::test(flavor = "multi_thread")]
async fn test_append_batch_empty() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let params = vec![];
    let result = engine.append_batch(params).await.unwrap();

    assert_eq!(result.entries.len(), 0);
    assert_eq!(result.tree_head.tree_size, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_append_batch_with_metadata() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let metadata = serde_json::json!({"source": "test", "timestamp": 12345});
    let params = vec![AppendParams {
        payload_hash: [1u8; 32],
        metadata_hash: [2u8; 32],
        metadata_cleartext: Some(metadata.clone()),
        external_id: Some("ext-123".to_string()),
    }];

    let result = engine.append_batch(params).await.unwrap();

    assert_eq!(result.entries.len(), 1);
    assert_eq!(result.tree_head.tree_size, 1);

    // Verify entry can be retrieved with metadata
    use crate::traits::storage::Storage;
    let entry = Storage::get_entry(&engine, &result.entries[0].id).unwrap();
    assert_eq!(entry.payload_hash, [1u8; 32]);
    assert_eq!(entry.metadata_hash, [2u8; 32]);
    assert_eq!(entry.metadata_cleartext, Some(metadata));
    assert_eq!(entry.external_id, Some("ext-123".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_append_batch_with_external_id() {
    use crate::traits::ProofProvider;

    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let params = vec![AppendParams {
        payload_hash: [10u8; 32],
        metadata_hash: [20u8; 32],
        metadata_cleartext: None,
        external_id: Some("my-external-id".to_string()),
    }];

    let result = engine.append_batch(params).await.unwrap();
    assert_eq!(result.entries.len(), 1);

    // Retrieve by external ID
    let entry = ProofProvider::get_entry_by_external_id(&engine, "my-external-id")
        .await
        .unwrap();
    assert_eq!(entry.payload_hash, [10u8; 32]);
    assert_eq!(entry.external_id, Some("my-external-id".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_entry_by_external_id_not_found() {
    use crate::traits::ProofProvider;

    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let result = ProofProvider::get_entry_by_external_id(&engine, "non-existent").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), StorageError::NotFound(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flush() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Append some data
    let params = vec![AppendParams {
        payload_hash: [1u8; 32],
        metadata_hash: [0u8; 32],
        metadata_cleartext: None,
        external_id: None,
    }];
    engine.append_batch(params).await.unwrap();

    // Flush should succeed
    let result = engine.flush().await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_is_healthy() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;
    assert!(engine.is_healthy());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_is_initialized() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;
    assert!(engine.is_initialized());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_origin_id() {
    let origin = [42u8; 32];
    let (engine, _dir) = create_test_engine(origin).await;
    assert_eq!(engine.origin_id(), origin);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tree_head() {
    let origin = [99u8; 32];
    let (engine, _dir) = create_test_engine(origin).await;

    let head = engine.tree_head();
    assert_eq!(head.tree_size, 0);
    assert_eq!(head.origin, origin);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_index_store_accessor() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let index = engine.index_store();
    assert!(Arc::strong_count(&index) >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_entry_by_index() {
    use crate::traits::ProofProvider;

    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Append entries
    let params: Vec<AppendParams> = (0..5)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    // Retrieve by index
    for i in 0..5 {
        let entry = ProofProvider::get_entry_by_index(&engine, i).await.unwrap();
        assert_eq!(entry.payload_hash, [i as u8; 32]);
        assert_eq!(entry.leaf_index, Some(i));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_entry_by_index_not_found() {
    use crate::traits::ProofProvider;

    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let result = ProofProvider::get_entry_by_index(&engine, 999).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), StorageError::NotFound(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_consistency_proof_invalid_range() {
    use crate::traits::ProofProvider;

    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Append some entries
    let params: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    // from_size > to_size
    let result = ProofProvider::get_consistency_proof(&engine, 5, 3).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), StorageError::InvalidRange(_)));

    // to_size > current tree size
    let result = ProofProvider::get_consistency_proof(&engine, 5, 100).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), StorageError::InvalidRange(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_consistency_proof_valid() {
    use crate::traits::ProofProvider;

    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Append entries
    let params: Vec<AppendParams> = (0..20)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    // Valid range
    let proof = ProofProvider::get_consistency_proof(&engine, 5, 15)
        .await
        .unwrap();
    assert_eq!(proof.from_size, 5);
    assert_eq!(proof.to_size, 15);
    assert!(!proof.path.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_inclusion_proof_invalid_index() {
    use crate::traits::ProofProvider;

    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Append some entries
    let params: Vec<AppendParams> = (0..5)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    // Try to get proof for leaf_index >= tree_size
    let result = ProofProvider::get_inclusion_proof(&engine, 10, None).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        StorageError::InvalidIndex(_)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_inclusion_proof_with_tree_size() {
    use crate::traits::ProofProvider;

    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Append entries
    let params: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    // Get proof at specific tree size
    let proof = ProofProvider::get_inclusion_proof(&engine, 3, Some(8))
        .await
        .unwrap();
    assert_eq!(proof.leaf_index, 3);
    assert_eq!(proof.tree_size, 8);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_anchors_empty() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    use crate::traits::storage::Storage;
    let anchors = Storage::get_anchors(&engine, 0).unwrap();
    assert_eq!(anchors.len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_latest_anchored_size_empty() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    use crate::traits::storage::Storage;
    let size = Storage::get_latest_anchored_size(&engine).unwrap();
    assert_eq!(size, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_anchors_covering_empty() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    use crate::traits::storage::Storage;
    let anchors = Storage::get_anchors_covering(&engine, 10, 5).unwrap();
    assert_eq!(anchors.len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_root_at_size() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Append entries
    let params: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    use crate::traits::storage::Storage;
    // Get root at various sizes
    for size in 1..=10 {
        let root = Storage::get_root_at_size(&engine, size).unwrap();
        assert_eq!(root.len(), 32);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_super_root_zero() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    use crate::traits::storage::Storage;
    let root = Storage::get_super_root(&engine, 0).unwrap();
    assert_eq!(root, [0u8; 32]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_batches_sequential() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // First batch
    let params1 = vec![AppendParams {
        payload_hash: [1u8; 32],
        metadata_hash: [0u8; 32],
        metadata_cleartext: None,
        external_id: None,
    }];
    let result1 = engine.append_batch(params1).await.unwrap();
    assert_eq!(result1.tree_head.tree_size, 1);

    // Second batch
    let params2 = vec![AppendParams {
        payload_hash: [2u8; 32],
        metadata_hash: [0u8; 32],
        metadata_cleartext: None,
        external_id: None,
    }];
    let result2 = engine.append_batch(params2).await.unwrap();
    assert_eq!(result2.tree_head.tree_size, 2);

    // Third batch
    let params3 = vec![AppendParams {
        payload_hash: [3u8; 32],
        metadata_hash: [0u8; 32],
        metadata_cleartext: None,
        external_id: None,
    }];
    let result3 = engine.append_batch(params3).await.unwrap();
    assert_eq!(result3.tree_head.tree_size, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tree_head_after_rotation() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let origin_id = engine.origin_id();

    // Append entries
    let params: Vec<AppendParams> = (0..20)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    let head_before = engine.tree_head();

    // Rotate
    engine
        .rotate_tree(&origin_id, head_before.tree_size, &head_before.root_hash)
        .await
        .unwrap();

    // Tree head should be updated
    let head_after = engine.tree_head();
    assert_eq!(head_after.tree_size, head_before.tree_size);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_params_to_wal_entry_no_metadata() {
    let params = AppendParams {
        payload_hash: [1u8; 32],
        metadata_hash: [2u8; 32],
        metadata_cleartext: None,
        external_id: None,
    };

    let id = Uuid::new_v4();
    let wal_entry = StorageEngine::params_to_wal_entry(id, &params);

    assert_eq!(wal_entry.id, *id.as_bytes());
    assert_eq!(wal_entry.payload_hash, [1u8; 32]);
    assert_eq!(wal_entry.metadata_hash, [2u8; 32]);
    assert!(wal_entry.metadata.is_empty());
    assert!(wal_entry.external_id.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_params_to_wal_entry_with_metadata() {
    let metadata = serde_json::json!({"key": "value"});
    let params = AppendParams {
        payload_hash: [1u8; 32],
        metadata_hash: [2u8; 32],
        metadata_cleartext: Some(metadata.clone()),
        external_id: Some("ext-id".to_string()),
    };

    let id = Uuid::new_v4();
    let wal_entry = StorageEngine::params_to_wal_entry(id, &params);

    assert_eq!(wal_entry.id, *id.as_bytes());
    assert_eq!(wal_entry.payload_hash, [1u8; 32]);
    assert_eq!(wal_entry.metadata_hash, [2u8; 32]);
    assert!(!wal_entry.metadata.is_empty());
    assert_eq!(wal_entry.external_id, b"ext-id");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_large_batch() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Large batch (1000 entries)
    let params: Vec<AppendParams> = (0..1000)
        .map(|i| AppendParams {
            payload_hash: [(i % 256) as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();

    let result = engine.append_batch(params).await.unwrap();
    assert_eq!(result.entries.len(), 1000);
    assert_eq!(result.tree_head.tree_size, 1000);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_rotations() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let origin_id = engine.origin_id();

    // First rotation
    let params1: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params1).await.unwrap();

    let head1 = engine.tree_head();
    let result1 = engine
        .rotate_tree(&origin_id, head1.tree_size, &head1.root_hash)
        .await
        .unwrap();
    assert_eq!(result1.data_tree_index, 0);

    // Second rotation
    let params2: Vec<AppendParams> = (10..20)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params2).await.unwrap();

    let head2 = engine.tree_head();
    let result2 = engine
        .rotate_tree(&origin_id, head2.tree_size, &head2.root_hash)
        .await
        .unwrap();
    assert_eq!(result2.data_tree_index, 1);

    // Verify Super-Tree has 2 leaves
    let super_size = engine.index.lock().await.get_super_tree_size().unwrap();
    assert_eq!(super_size, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_append_batch_with_all_fields() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let metadata1 = serde_json::json!({"type": "document", "id": 123});
    let metadata2 = serde_json::json!({"type": "image", "size": 456});

    let params = vec![
        AppendParams {
            payload_hash: [10u8; 32],
            metadata_hash: [20u8; 32],
            metadata_cleartext: Some(metadata1.clone()),
            external_id: Some("ext-001".to_string()),
        },
        AppendParams {
            payload_hash: [11u8; 32],
            metadata_hash: [21u8; 32],
            metadata_cleartext: Some(metadata2.clone()),
            external_id: Some("ext-002".to_string()),
        },
        AppendParams {
            payload_hash: [12u8; 32],
            metadata_hash: [22u8; 32],
            metadata_cleartext: None,
            external_id: None,
        },
    ];

    let result = engine.append_batch(params).await.unwrap();
    assert_eq!(result.entries.len(), 3);

    use crate::traits::storage::Storage;

    // Verify first entry
    let entry1 = Storage::get_entry(&engine, &result.entries[0].id).unwrap();
    assert_eq!(entry1.metadata_cleartext, Some(metadata1));
    assert_eq!(entry1.external_id, Some("ext-001".to_string()));

    // Verify second entry
    let entry2 = Storage::get_entry(&engine, &result.entries[1].id).unwrap();
    assert_eq!(entry2.metadata_cleartext, Some(metadata2));
    assert_eq!(entry2.external_id, Some("ext-002".to_string()));

    // Verify third entry (no metadata/external_id)
    let entry3 = Storage::get_entry(&engine, &result.entries[2].id).unwrap();
    assert_eq!(entry3.metadata_cleartext, None);
    assert_eq!(entry3.external_id, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_super_root_after_rotation() {
    use crate::traits::{storage::Storage, TreeRotator};

    let (engine, _dir) = create_test_engine([1u8; 32]).await;
    let origin_id = engine.origin_id();

    // Append entries and rotate
    let params: Vec<AppendParams> = (0..5)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    let head = engine.tree_head();
    TreeRotator::rotate_tree(&engine, &origin_id, head.tree_size, &head.root_hash)
        .await
        .unwrap();

    // Now Super-Tree has size 1
    let super_root = Storage::get_super_root(&engine, 1).unwrap();
    assert_eq!(super_root.len(), 32);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consistency_proof_same_size() {
    use crate::traits::ProofProvider;

    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Append entries
    let params: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params).await.unwrap();

    // from_size == to_size
    let proof = ProofProvider::get_consistency_proof(&engine, 5, 5)
        .await
        .unwrap();
    assert_eq!(proof.from_size, 5);
    assert_eq!(proof.to_size, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_entry_leaf_hash_computation() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    let payload_hash = [42u8; 32];
    let metadata_hash = [99u8; 32];

    let params = vec![AppendParams {
        payload_hash,
        metadata_hash,
        metadata_cleartext: None,
        external_id: None,
    }];

    let result = engine.append_batch(params).await.unwrap();
    assert_eq!(result.entries.len(), 1);

    // Verify leaf_hash matches atl-core computation
    let expected_leaf_hash = atl_core::compute_leaf_hash(&payload_hash, &metadata_hash);
    assert_eq!(result.entries[0].leaf_hash, expected_leaf_hash);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tree_state_cache_consistency() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Initial state
    let head1 = engine.tree_head();
    assert_eq!(head1.tree_size, 0);

    // Append first batch
    let params1 = vec![AppendParams {
        payload_hash: [1u8; 32],
        metadata_hash: [0u8; 32],
        metadata_cleartext: None,
        external_id: None,
    }];
    engine.append_batch(params1).await.unwrap();

    let head2 = engine.tree_head();
    assert_eq!(head2.tree_size, 1);

    // Append second batch
    let params2 = vec![AppendParams {
        payload_hash: [2u8; 32],
        metadata_hash: [0u8; 32],
        metadata_cleartext: None,
        external_id: None,
    }];
    engine.append_batch(params2).await.unwrap();

    let head3 = engine.tree_head();
    assert_eq!(head3.tree_size, 2);

    // All reads should be consistent
    assert_ne!(head1.root_hash, head2.root_hash);
    assert_ne!(head2.root_hash, head3.root_hash);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_params_to_wal_entry_empty_external_id() {
    let params = AppendParams {
        payload_hash: [5u8; 32],
        metadata_hash: [6u8; 32],
        metadata_cleartext: None,
        external_id: Some("".to_string()), // Empty string
    };

    let id = Uuid::new_v4();
    let wal_entry = StorageEngine::params_to_wal_entry(id, &params);

    assert_eq!(wal_entry.external_id, b"");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_params_to_wal_entry_complex_metadata() {
    let metadata = serde_json::json!({
        "nested": {
            "key": "value",
            "array": [1, 2, 3]
        },
        "unicode": "Привет мир 🌍"
    });

    let params = AppendParams {
        payload_hash: [7u8; 32],
        metadata_hash: [8u8; 32],
        metadata_cleartext: Some(metadata.clone()),
        external_id: Some("unicode-test".to_string()),
    };

    let id = Uuid::new_v4();
    let wal_entry = StorageEngine::params_to_wal_entry(id, &params);

    assert!(!wal_entry.metadata.is_empty());
    assert_eq!(wal_entry.external_id, b"unicode-test");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_slab_flush_idempotency() {
    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Append data
    let params = vec![AppendParams {
        payload_hash: [1u8; 32],
        metadata_hash: [0u8; 32],
        metadata_cleartext: None,
        external_id: None,
    }];
    engine.append_batch(params).await.unwrap();

    // Multiple flushes should succeed
    assert!(engine.flush().await.is_ok());
    assert!(engine.flush().await.is_ok());
    assert!(engine.flush().await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tree_rotation_updates_super_genesis_once() {
    use crate::traits::TreeRotator;

    let (engine, _dir) = create_test_engine([1u8; 32]).await;
    let origin_id = engine.origin_id();

    // First rotation
    let params1: Vec<AppendParams> = (0..3)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params1).await.unwrap();

    let head1 = engine.tree_head();
    let result1 = TreeRotator::rotate_tree(&engine, &origin_id, head1.tree_size, &head1.root_hash)
        .await
        .unwrap();
    let super_genesis1 = result1.super_root;

    // Second rotation
    let params2: Vec<AppendParams> = (3..6)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    engine.append_batch(params2).await.unwrap();

    let head2 = engine.tree_head();
    TreeRotator::rotate_tree(&engine, &origin_id, head2.tree_size, &head2.root_hash)
        .await
        .unwrap();

    // Super genesis should still be from first rotation
    let super_genesis_final = engine
        .index
        .lock()
        .await
        .get_super_genesis_root()
        .unwrap()
        .unwrap();
    assert_eq!(super_genesis_final, super_genesis1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_reads() {
    use crate::traits::storage::Storage;

    let (engine, _dir) = create_test_engine([1u8; 32]).await;

    // Append entries
    let params: Vec<AppendParams> = (0..20)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();
    let result = engine.append_batch(params).await.unwrap();

    // Spawn multiple concurrent read tasks
    let mut handles = vec![];
    for i in 0..10 {
        let engine_clone = std::sync::Arc::new(engine.clone_for_test());
        let entry_id = result.entries[i].id;

        let handle = tokio::spawn(async move {
            Storage::get_entry(&*engine_clone, &entry_id).unwrap()
        });
        handles.push(handle);
    }

    // All reads should succeed
    for handle in handles {
        let entry = handle.await.unwrap();
        assert_eq!(entry.payload_hash[0], entry.leaf_index.unwrap() as u8);
    }
}

// Helper trait to clone engine for testing
trait CloneForTest {
    fn clone_for_test(&self) -> Self;
}

impl CloneForTest for StorageEngine {
    fn clone_for_test(&self) -> Self {
        // This is a test-only clone that shares the same underlying Arc<>s
        // NOT a real deep clone - just for testing concurrent access
        Self {
            config: self.config.clone(),
            wal: Arc::clone(&self.wal),
            slabs: Arc::clone(&self.slabs),
            super_slab: Arc::clone(&self.super_slab),
            index: Arc::clone(&self.index),
            tree_state: StdRwLock::new(
                self.tree_state
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone(),
            ),
            healthy: AtomicBool::new(self.healthy.load(Ordering::Relaxed)),
        }
    }
}

impl Clone for TreeState {
    fn clone(&self) -> Self {
        Self {
            tree_size: self.tree_size,
            root_hash: self.root_hash,
            origin: self.origin,
        }
    }
}
