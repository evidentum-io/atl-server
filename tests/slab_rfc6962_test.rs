//! Integration tests for slab-based Merkle tree storage
//!
//! Verifies RFC 6962 compliance and cross-slab operations.

use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tempfile::tempdir;

// Mock types for testing (since we can't import from main lib due to compilation issues)
#[path = "../src/storage/high_throughput/slab/format.rs"]
mod format;

#[path = "../src/storage/high_throughput/slab/mmap.rs"]
mod mmap;

#[path = "../src/storage/high_throughput/slab/manager.rs"]
mod manager;

use format::*;
use manager::*;
use mmap::*;

/// Compute leaf hash (RFC 6962: 0x00 prefix)
fn leaf_hash(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([0x00]);
    hasher.update(data);
    hasher.finalize().into()
}

/// Verify RFC 6962 inclusion proof (simplified version for testing)
fn verify_inclusion_proof(
    leaf_hash: &[u8; 32],
    leaf_index: u64,
    tree_size: u64,
    proof_path: &[[u8; 32]],
    root: &[u8; 32],
) -> bool {
    let mut current = *leaf_hash;
    let mut index = leaf_index;

    for sibling in proof_path {
        let mut hasher = Sha256::new();
        hasher.update([0x01]); // Internal node prefix

        // Determine left/right order based on index
        if index % 2 == 0 {
            hasher.update(current);
            hasher.update(sibling);
        } else {
            hasher.update(sibling);
            hasher.update(current);
        }

        current = hasher.finalize().into();
        index /= 2;
    }

    &current == root
}

#[test]
fn test_slab_header_creation() {
    let header = SlabHeader::new(1, 0, 1000);
    assert_eq!(header.magic, SLAB_MAGIC);
    assert_eq!(header.version, SLAB_VERSION);
    assert!(header.validate());
}

#[test]
fn test_slab_file_basic_operations() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_slab.dat");

    // Create and write
    let mut slab = SlabFile::create(&path, 1, 0, 100).unwrap();
    let hash = [42u8; 32];
    slab.set_node(0, 0, &hash);
    slab.flush().unwrap();
    drop(slab);

    // Reopen and verify
    let slab = SlabFile::open(&path).unwrap();
    assert_eq!(slab.get_node(0, 0).unwrap(), hash);
}

#[test]
fn test_slab_manager_single_slab() {
    let dir = tempdir().unwrap();
    let mut manager = SlabManager::new(
        dir.path().to_path_buf(),
        SlabConfig {
            max_leaves: 100,
            max_open_slabs: 2,
        },
    )
    .unwrap();

    // Append leaves
    let leaves: Vec<[u8; 32]> = (0..10)
        .map(|i| leaf_hash(&i.to_le_bytes()))
        .collect();

    let root = manager.append_leaves(&leaves).unwrap();
    assert_ne!(root, [0u8; 32]);
    assert_eq!(manager.tree_size(), 10);
}

#[test]
fn test_slab_manager_rotation() {
    let dir = tempdir().unwrap();
    let mut manager = SlabManager::new(
        dir.path().to_path_buf(),
        SlabConfig {
            max_leaves: 50,
            max_open_slabs: 3,
        },
    )
    .unwrap();

    // Append 120 leaves (should create 3 slabs: 50 + 50 + 20)
    let leaves: Vec<[u8; 32]> = (0..120)
        .map(|i| leaf_hash(&(i as u64).to_le_bytes()))
        .collect();

    let root = manager.append_leaves(&leaves).unwrap();
    assert_ne!(root, [0u8; 32]);
    assert_eq!(manager.tree_size(), 120);

    // Verify slab files exist
    assert!(dir.path().join("slab_0001.dat").exists());
    assert!(dir.path().join("slab_0002.dat").exists());
    assert!(dir.path().join("slab_0003.dat").exists());
}

#[test]
fn test_inclusion_proof_single_slab() {
    let dir = tempdir().unwrap();
    let mut manager = SlabManager::new(
        dir.path().to_path_buf(),
        SlabConfig {
            max_leaves: 100,
            max_open_slabs: 2,
        },
    )
    .unwrap();

    // Append 16 leaves
    let leaves: Vec<[u8; 32]> = (0..16)
        .map(|i| leaf_hash(&(i as u64).to_le_bytes()))
        .collect();

    let root = manager.append_leaves(&leaves).unwrap();

    // Get proof for leaf at index 5
    let proof_path = manager.get_inclusion_path(5, 16).unwrap();
    assert!(!proof_path.is_empty());

    // Verify proof
    let verified = verify_inclusion_proof(&leaves[5], 5, 16, &proof_path, &root);
    assert!(verified, "RFC 6962 inclusion proof verification failed");
}

#[test]
fn test_cross_slab_proof() {
    let dir = tempdir().unwrap();
    let mut manager = SlabManager::new(
        dir.path().to_path_buf(),
        SlabConfig {
            max_leaves: 100,
            max_open_slabs: 3,
        },
    )
    .unwrap();

    // Append 250 leaves (3 slabs: 100 + 100 + 50)
    let leaves: Vec<[u8; 32]> = (0..250)
        .map(|i| leaf_hash(&(i as u64).to_le_bytes()))
        .collect();

    let root = manager.append_leaves(&leaves).unwrap();

    // Get proof for leaf in middle slab (index 150)
    let proof_path = manager.get_inclusion_path(150, 250).unwrap();
    assert!(!proof_path.is_empty());

    // Verify cross-slab proof
    let verified = verify_inclusion_proof(&leaves[150], 150, 250, &proof_path, &root);
    assert!(
        verified,
        "Cross-slab RFC 6962 proof verification failed"
    );
}

#[test]
fn test_proof_at_slab_boundary() {
    let dir = tempdir().unwrap();
    let mut manager = SlabManager::new(
        dir.path().to_path_buf(),
        SlabConfig {
            max_leaves: 100,
            max_open_slabs: 2,
        },
    )
    .unwrap();

    // Append exactly 100 leaves (fills first slab)
    let leaves: Vec<[u8; 32]> = (0..100)
        .map(|i| leaf_hash(&(i as u64).to_le_bytes()))
        .collect();

    let root1 = manager.append_leaves(&leaves).unwrap();

    // Get proof for last leaf of first slab
    let proof = manager.get_inclusion_proof(99, 100).unwrap();
    let verified = verify_inclusion_proof(&leaves[99], 99, 100, &proof, &root1);
    assert!(verified, "Boundary proof (last leaf) failed");

    // Add one more leaf (triggers second slab)
    let new_leaf = leaf_hash(&100u64.to_le_bytes());
    let root2 = manager.append_leaves(&[new_leaf]).unwrap();
    assert_ne!(root1, root2);

    // Get proof for first leaf of second slab
    let proof = manager.get_inclusion_path(100, 101).unwrap();
    let verified = verify_inclusion_proof(&new_leaf, 100, 101, &proof, &root2);
    assert!(
        verified,
        "Boundary proof (first leaf of new slab) failed"
    );
}

#[test]
fn test_persistence_across_open_close() {
    let dir = tempdir().unwrap();
    let tree_size;
    let root;
    let test_leaf;

    // Create and populate
    {
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig::default(),
        )
        .unwrap();

        let leaves: Vec<[u8; 32]> = (0..100)
            .map(|i| leaf_hash(&(i as u64).to_le_bytes()))
            .collect();

        root = manager.append_leaves(&leaves).unwrap();
        tree_size = manager.tree_size();
        test_leaf = leaves[42];
        manager.flush().unwrap();
    }

    // Reopen and verify
    {
        let mut manager = SlabManager::open(
            dir.path().to_path_buf(),
            SlabConfig::default(),
        )
        .unwrap();

        assert_eq!(manager.tree_size(), tree_size);
        let restored_root = manager.get_root(tree_size).unwrap();
        assert_eq!(restored_root, root);

        // Verify proof still works
        let proof = manager.get_inclusion_path(42, tree_size).unwrap();
        let verified = verify_inclusion_proof(&test_leaf, 42, tree_size, &proof, &root);
        assert!(verified, "Proof after reopen failed");
    }
}
