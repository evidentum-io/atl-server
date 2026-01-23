//! Integration tests for Super-Tree architecture fix
//!
//! These tests verify that the "missing node" bug is fixed by testing:
//! - Concurrent rotation + proof generation
//! - Crash recovery with partial Super-Tree state
//! - High-frequency rotation stress test
//! - Regression test for the original bug scenario

use atl_server::storage::config::StorageConfig;
use atl_server::storage::engine::StorageEngine;
use atl_server::traits::{AppendParams, Storage};
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

/// Create test configuration
fn create_test_config(dir: &Path) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_path_buf(),
        slab_capacity: 1024,
        max_open_slabs: 2,
        wal_keep_count: 5,
        ..Default::default()
    }
}

/// Create test engine with active tree initialized
async fn create_test_engine() -> (Arc<StorageEngine>, TempDir) {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(dir.path());
    let origin = [0u8; 32];
    let engine = StorageEngine::new(config, origin)
        .await
        .expect("failed to create engine");

    // Active tree is automatically created by StorageEngine::new()

    (Arc::new(engine), dir)
}

/// Helper to rotate tree with some entries
async fn rotate_tree_helper(engine: &StorageEngine, tree_num: u64) {
    // Append some entries to create a tree worth closing
    let params = vec![AppendParams {
        payload_hash: [tree_num as u8; 32],
        metadata_hash: [0u8; 32],
        metadata_cleartext: None,
        external_id: None,
    }];
    engine
        .append_batch(params)
        .await
        .expect("failed to append batch");

    // Rotate
    let origin = engine.origin_id();
    let tree_head = engine.tree_head();
    engine
        .rotate_tree(&origin, tree_head.tree_size, &tree_head.root_hash)
        .await
        .expect("failed to rotate tree");
}

#[tokio::test(flavor = "multi_thread")]
async fn should_generate_valid_proof_during_concurrent_rotation() {
    // Arrange
    let (engine, _dir) = create_test_engine().await;

    // Pre-populate with some closed trees
    for i in 0..5 {
        rotate_tree_helper(&engine, i).await;
    }

    // Act: Start rotation in background, immediately request proof
    let rotation_handle = tokio::spawn({
        let engine = Arc::clone(&engine);
        async move {
            for i in 5..10 {
                rotate_tree_helper(&engine, i).await;
            }
        }
    });

    // Concurrent proof requests
    let proof_handle = tokio::spawn({
        let engine = Arc::clone(&engine);
        async move {
            for _ in 0..100 {
                // Get current size (may be changing)
                let size = engine.super_slab().read().await.leaf_count();
                if size > 0 {
                    // This should NEVER fail with "missing node"
                    let result = engine.get_super_root(size);
                    assert!(result.is_ok(), "get_super_root failed: {:?}", result);
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
        }
    });

    // Wait for both
    rotation_handle.await.expect("rotation task failed");
    proof_handle.await.expect("proof task failed");

    // Assert: Final state is consistent
    let final_size = engine.super_slab().read().await.leaf_count();
    assert_eq!(final_size, 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn should_recover_after_crash_during_rotation() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");

    // Phase 1: Create engine, rotate 3 trees, simulate crash
    {
        let config = create_test_config(dir.path());
        let origin = [0u8; 32];
        let engine = StorageEngine::new(config, origin)
            .await
            .expect("failed to create engine");

        // Active tree is automatically created by StorageEngine::new()

        for i in 0..3 {
            rotate_tree_helper(&engine, i).await;
        }

        // Verify SQLite and slab are in sync
        let slab_count = engine.super_slab().read().await.leaf_count();
        assert_eq!(slab_count, 3);

        // Don't call shutdown - simulate crash by just dropping
        drop(engine);
    }

    // Phase 2: Reopen (recovery runs)
    {
        let config = create_test_config(dir.path());
        let origin = [0u8; 32];
        let engine = StorageEngine::new(config, origin)
            .await
            .expect("failed to reopen engine");

        // Assert: State is recovered
        let slab_count = engine.super_slab().read().await.leaf_count();
        assert_eq!(slab_count, 3);

        // Assert: Proofs work
        let root = engine.get_super_root(3).expect("failed to get super root");
        assert_ne!(root, [0u8; 32]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn should_handle_rapid_rotations_without_missing_node() {
    let (engine, _dir) = create_test_engine().await;

    // Rapid-fire rotations
    for i in 0..100 {
        rotate_tree_helper(&engine, i).await;

        // Immediately verify proof works
        let size = engine.super_slab().read().await.leaf_count();
        let root = engine
            .get_super_root(size)
            .unwrap_or_else(|e| panic!("Root computation failed at rotation {}: {:?}", i, e));
        assert_ne!(root, [0u8; 32], "Root is empty at rotation {}", i);
    }

    // Final verification
    let final_size = engine.super_slab().read().await.leaf_count();
    assert_eq!(final_size, 100);
}

#[tokio::test(flavor = "multi_thread")]
async fn regression_missing_node_after_append() {
    // This test replicates the exact bug scenario:
    // 1. Append to Super-Tree
    // 2. tree_state updated
    // 3. Proof requested immediately
    // 4. LRU eviction happened (in old code)
    // 5. Slab reopened with stale header
    // 6. "missing node" error

    let (engine, _dir) = create_test_engine().await;

    // Setup: Create first tree
    rotate_tree_helper(&engine, 0).await;

    // The bug: immediately after rotation, request inclusion proof
    // In old code, this could fail if LRU evicted the slab
    let slab = engine.super_slab().read().await;
    let size = slab.leaf_count();

    // This MUST NOT fail
    let path = slab.get_inclusion_path(0, size);
    assert!(path.is_ok(), "get_inclusion_path failed: {:?}", path);

    // Verify the proof is valid
    let root = slab.get_root(size).expect("failed to get root");
    let leaf = slab.get_node(0, 0).expect("leaf must exist");

    // Verify with atl_core
    let proof = atl_core::InclusionProof {
        leaf_index: 0,
        tree_size: size,
        path: path.expect("path should exist"),
    };

    let valid =
        atl_core::verify_inclusion(&leaf, &proof, &root).expect("verification should not error");

    assert!(valid, "Proof verification failed - THIS WAS THE BUG");
}

#[tokio::test(flavor = "multi_thread")]
async fn should_maintain_consistency_across_multiple_rotations() {
    let (engine, _dir) = create_test_engine().await;

    // Perform multiple rotations and verify each one
    for i in 0..10 {
        rotate_tree_helper(&engine, i).await;

        // Verify Super-Tree size increments correctly
        let super_size = engine.super_slab().read().await.leaf_count();
        assert_eq!(
            super_size,
            i + 1,
            "Super-Tree size mismatch at rotation {}",
            i
        );

        // Verify we can get root for current size
        let root = engine
            .get_super_root(super_size)
            .expect("failed to get super root");
        assert_ne!(
            root, [0u8; 32],
            "Root should not be empty at rotation {}",
            i
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn should_verify_all_super_tree_proofs_after_rotations() {
    let (engine, _dir) = create_test_engine().await;

    // Create 5 rotations
    for i in 0..5 {
        rotate_tree_helper(&engine, i).await;
    }

    // Verify inclusion proofs for all leaves
    let slab = engine.super_slab().read().await;
    let size = slab.leaf_count();
    assert_eq!(size, 5);

    for leaf_index in 0..size {
        // Get inclusion path
        let path = slab
            .get_inclusion_path(leaf_index, size)
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to get inclusion path for leaf {}: {:?}",
                    leaf_index, e
                )
            });

        // Get leaf and root
        let leaf = slab
            .get_node(0, leaf_index)
            .unwrap_or_else(|| panic!("Failed to get leaf {}", leaf_index));
        let root = slab
            .get_root(size)
            .unwrap_or_else(|e| panic!("Failed to get root: {:?}", e));

        // Verify with atl_core
        let proof = atl_core::InclusionProof {
            leaf_index,
            tree_size: size,
            path,
        };

        let valid = atl_core::verify_inclusion(&leaf, &proof, &root)
            .expect("verification should not error");

        assert!(valid, "Proof verification failed for leaf {}", leaf_index);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn should_handle_concurrent_append_and_super_root_access() {
    let (engine, _dir) = create_test_engine().await;

    // Pre-populate with one tree
    rotate_tree_helper(&engine, 0).await;

    // Spawn tasks: one appending to data tree, one accessing super root
    let append_handle = tokio::spawn({
        let engine = Arc::clone(&engine);
        async move {
            for i in 0..50 {
                let params = vec![AppendParams {
                    payload_hash: [i as u8; 32],
                    metadata_hash: [0u8; 32],
                    metadata_cleartext: None,
                    external_id: None,
                }];
                engine
                    .append_batch(params)
                    .await
                    .expect("failed to append batch");
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
        }
    });

    let super_root_handle = tokio::spawn({
        let engine = Arc::clone(&engine);
        async move {
            for _ in 0..100 {
                let size = engine.super_slab().read().await.leaf_count();
                if size > 0 {
                    let result = engine.get_super_root(size);
                    assert!(result.is_ok(), "get_super_root failed: {:?}", result);
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
        }
    });

    // Wait for both
    append_handle.await.expect("append task failed");
    super_root_handle.await.expect("super root task failed");
}
