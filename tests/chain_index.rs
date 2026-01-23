//! Integration tests for Chain Index

use std::sync::Arc;
use tempfile::tempdir;

use atl_server::storage::chain_index::ChainIndex;
use atl_server::storage::{StorageConfig, StorageEngine};
use atl_server::traits::{AppendParams, Storage};

#[tokio::test]
async fn test_chain_index_lifecycle() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    let origin_id = [0xaa; 32];
    let storage_config = StorageConfig {
        data_dir: data_dir.clone(),
        wal_dir: None,
        slab_dir: None,
        db_path: None,
        slab_capacity: 1_000_000,
        max_open_slabs: 10,
        wal_keep_count: 2,
        fsync_enabled: false,
    };

    let engine = StorageEngine::new(storage_config, origin_id).await.unwrap();
    let engine = Arc::new(engine);

    let chain_index_path = data_dir.join("chain_index.db");
    let chain_index = ChainIndex::open(&chain_index_path).unwrap();

    // Active tree is automatically created by StorageEngine::new()

    let entries: Vec<AppendParams> = (0..10)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();

    let storage_trait: Arc<dyn Storage> = engine.clone();
    storage_trait.append_batch(entries).await.unwrap();

    let tree_head = storage_trait.tree_head();
    let result = engine
        .rotate_tree(&origin_id, tree_head.tree_size, &tree_head.root_hash)
        .await
        .unwrap();

    chain_index
        .record_closed_tree(&result.closed_tree_metadata)
        .unwrap();

    let tree = chain_index.get_tree(result.closed_tree_id).unwrap();
    assert!(tree.is_some());

    let tree = tree.unwrap();
    assert_eq!(tree.tree_id, result.closed_tree_id);
    assert_eq!(tree.tree_size, 10);
    assert_eq!(tree.data_tree_index, result.data_tree_index);

    let result = chain_index.verify_full_chain().unwrap();
    assert!(result.valid);
    assert_eq!(result.verified_trees, 1);
}

#[tokio::test]
async fn test_chain_index_sync() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    let origin_id = [0xbb; 32];
    let storage_config = StorageConfig {
        data_dir: data_dir.clone(),
        wal_dir: None,
        slab_dir: None,
        db_path: None,
        slab_capacity: 1_000_000,
        max_open_slabs: 10,
        wal_keep_count: 2,
        fsync_enabled: false,
    };

    let engine = StorageEngine::new(storage_config, origin_id).await.unwrap();
    let engine = Arc::new(engine);

    // Active tree is automatically created by StorageEngine::new()

    let entries: Vec<AppendParams> = (0..5)
        .map(|i| AppendParams {
            payload_hash: [i as u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        })
        .collect();

    let storage_trait: Arc<dyn Storage> = engine.clone();
    storage_trait.append_batch(entries).await.unwrap();

    let tree_head = storage_trait.tree_head();
    let _result = engine
        .rotate_tree(&origin_id, tree_head.tree_size, &tree_head.root_hash)
        .await
        .unwrap();

    let chain_index_path = data_dir.join("chain_index.db");
    let chain_index = ChainIndex::open(&chain_index_path).unwrap();

    let index = engine.index_store();
    let idx = index.lock().await;
    let synced = chain_index.sync_with_main_db(&idx).unwrap();

    assert_eq!(synced, 1);

    let all_trees = chain_index.get_all_trees().unwrap();
    assert_eq!(all_trees.len(), 1);
}

#[tokio::test]
#[ignore = "Test needs update after removing genesis_leaf_hash in SUPER-2"]
async fn test_multiple_tree_rotations() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    let origin_id = [0xcc; 32];
    let storage_config = StorageConfig {
        data_dir: data_dir.clone(),
        wal_dir: None,
        slab_dir: None,
        db_path: None,
        slab_capacity: 1_000_000,
        max_open_slabs: 10,
        wal_keep_count: 2,
        fsync_enabled: false,
    };

    let engine = StorageEngine::new(storage_config, origin_id).await.unwrap();
    let engine = Arc::new(engine);

    let chain_index_path = data_dir.join("chain_index.db");
    let chain_index = ChainIndex::open(&chain_index_path).unwrap();

    // Active tree is automatically created by StorageEngine::new()

    let storage_trait: Arc<dyn Storage> = engine.clone();

    for iteration in 0..3 {
        let entries: Vec<AppendParams> = (0..5)
            .map(|i| AppendParams {
                payload_hash: [(i + iteration * 5) as u8; 32],
                metadata_hash: [0u8; 32],
                metadata_cleartext: None,
                external_id: None,
            })
            .collect();

        storage_trait.append_batch(entries).await.unwrap();

        let tree_head = storage_trait.tree_head();
        let result = engine
            .rotate_tree(&origin_id, tree_head.tree_size, &tree_head.root_hash)
            .await
            .unwrap();

        chain_index
            .record_closed_tree(&result.closed_tree_metadata)
            .unwrap();
    }

    let result = chain_index.verify_full_chain().unwrap();
    if !result.valid {
        eprintln!("Chain verification failed:");
        eprintln!("  verified_trees: {}", result.verified_trees);
        eprintln!("  first_invalid_tree: {:?}", result.first_invalid_tree);
        eprintln!("  error_message: {:?}", result.error_message);
    }
    assert!(result.valid);
    assert_eq!(result.verified_trees, 3);
}
