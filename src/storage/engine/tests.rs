//! Tests for StorageEngine

use super::*;
use tempfile::tempdir;

async fn create_test_engine(origin: [u8; 32]) -> StorageEngine {
    let dir = tempdir().unwrap();
    let config = StorageConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    StorageEngine::new(config, origin).await.unwrap()
}

#[tokio::test]
async fn test_engine_initialization() {
    let engine = create_test_engine([1u8; 32]).await;

    assert_eq!(engine.tree_head().tree_size, 0);
    assert!(engine.is_healthy());
}

#[tokio::test]
async fn test_append_batch() {
    let engine = create_test_engine([1u8; 32]).await;

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

#[tokio::test]
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

#[tokio::test]
async fn test_inclusion_proof_verification() {
    let engine = create_test_engine([1u8; 32]).await;

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

    let atl_proof = atl_core::InclusionProof {
        tree_size: proof.tree_size,
        leaf_index: proof.leaf_index,
        path: proof.path,
    };

    let result = atl_core::verify_inclusion(&leaf_hash, &atl_proof, &head.root_hash);

    assert!(result.is_ok() && result.unwrap());
}
