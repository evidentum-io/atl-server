//! Pinned Super-Tree slab implementation
//!
//! This module provides a dedicated storage layer for the Super-Tree that:
//! - Owns a single SlabFile that is never evicted from memory
//! - Uses SlabFile.leaf_count as the single source of truth
//! - Guarantees atomic append operations (flush before state update)
//! - Propagates all errors without silent ignores

#![allow(dead_code)] // New functionality, not yet integrated

use std::io;
use std::path::{Path, PathBuf};

use atl_core::core::merkle::{compute_root, generate_inclusion_proof, Hash};

use super::format::SlabHeader;
use super::mmap::SlabFile;

/// Pinned Super-Tree slab storage
///
/// This type manages a single memory-mapped slab file for the Super-Tree
/// that remains pinned in memory for the server's lifetime. Each leaf in
/// this tree represents a closed Data Tree root hash.
///
/// # Invariants
///
/// 1. **Single SlabFile**: Owns exactly one SlabFile, created on construction
/// 2. **No eviction**: SlabFile remains in memory for entire server lifetime
/// 3. **Atomic append**: `flush()` MUST succeed before `leaf_count` is updated
/// 4. **Source of truth**: `leaf_count()` reads from SlabFile header
pub struct PinnedSuperTreeSlab {
    /// The underlying slab file (always in memory)
    slab: SlabFile,

    /// Path for error messages and reopening after crash
    path: PathBuf,
}

impl PinnedSuperTreeSlab {
    /// Create or open the Super-Tree slab file
    ///
    /// If file exists, opens and validates header.
    /// If file does not exist, creates with given capacity.
    ///
    /// # Arguments
    /// * `path` - Path to slab file
    /// * `max_leaves` - Maximum capacity (only used on create)
    ///
    /// # Errors
    /// Returns IO error if file operations fail or header is invalid
    pub fn open_or_create(path: &Path, max_leaves: u32) -> io::Result<Self> {
        let slab = if path.exists() {
            // Open existing file
            SlabFile::open(path)?
        } else {
            // Create new file with slab_id=0 and start_index=0
            // These are not used for Super-Tree, but required by SlabFile API
            SlabFile::create(path, 0, 0, max_leaves)?
        };

        Ok(Self {
            slab,
            path: path.to_path_buf(),
        })
    }

    /// Append a single leaf hash to the Super-Tree
    ///
    /// This operation is ATOMIC:
    /// 1. Write leaf to mmap
    /// 2. Update parent hashes up the tree
    /// 3. Flush to disk
    /// 4. Return new root
    ///
    /// If flush fails, the leaf is NOT considered appended.
    ///
    /// # Arguments
    /// * `leaf_hash` - 32-byte hash (closed Data Tree root)
    ///
    /// # Returns
    /// New Super-Tree root hash after append
    ///
    /// # Errors
    /// Returns IO error if write or flush fails
    pub fn append_leaf(&mut self, leaf_hash: &[u8; 32]) -> io::Result<[u8; 32]> {
        let leaf_index = self.leaf_count();

        // Write leaf at level 0
        self.slab.set_node(0, leaf_index, leaf_hash)?;

        // Update parent hashes up to root
        self.update_tree_after_append(leaf_index)?;

        // CRITICAL: Flush to disk BEFORE considering the append successful
        self.slab.flush()?;

        // Compute and return new root
        self.get_root(leaf_index + 1)
    }

    /// Get current leaf count (number of closed Data Trees)
    ///
    /// Reads directly from SlabFile header - this is THE source of truth.
    #[must_use]
    pub fn leaf_count(&self) -> u64 {
        u64::from(self.slab.leaf_count())
    }

    /// Get Super-Tree root for given size
    ///
    /// # Arguments
    /// * `tree_size` - Size to compute root for (must be <= leaf_count)
    ///
    /// # Errors
    /// Returns IO error if tree_size > leaf_count
    pub fn get_root(&self, tree_size: u64) -> io::Result<[u8; 32]> {
        if tree_size > self.leaf_count() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "tree_size {} exceeds leaf_count {}",
                    tree_size,
                    self.leaf_count()
                ),
            ));
        }

        if tree_size == 0 {
            // Empty tree: SHA-256 of empty string
            return Ok(compute_root(&[]));
        }

        // Collect all leaf hashes up to tree_size
        let mut leaves = Vec::with_capacity(tree_size as usize);
        for i in 0..tree_size {
            if let Some(hash) = self.slab.get_node(0, i) {
                leaves.push(hash);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("missing leaf at index {}", i),
                ));
            }
        }

        Ok(compute_root(&leaves))
    }

    /// Get inclusion proof for a Data Tree in Super-Tree
    ///
    /// # Arguments
    /// * `data_tree_index` - Index of Data Tree (0-based)
    /// * `tree_size` - Super-Tree size for proof
    ///
    /// # Errors
    /// Returns IO error if index out of bounds
    pub fn get_inclusion_path(
        &self,
        data_tree_index: u64,
        tree_size: u64,
    ) -> io::Result<Vec<[u8; 32]>> {
        if tree_size > self.leaf_count() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "tree_size {} exceeds leaf_count {}",
                    tree_size,
                    self.leaf_count()
                ),
            ));
        }

        if data_tree_index >= tree_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "data_tree_index {} >= tree_size {}",
                    data_tree_index, tree_size
                ),
            ));
        }

        // Generate inclusion proof using atl_core
        let proof = generate_inclusion_proof(data_tree_index, tree_size, |level, index| {
            self.get_node(level, index)
        })
        .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(proof.path)
    }

    /// Get node at (level, index) for proof generation
    ///
    /// # Arguments
    /// * `level` - Tree level (0 = leaves)
    /// * `index` - Node index at that level
    pub fn get_node(&self, level: u32, index: u64) -> Option<[u8; 32]> {
        self.slab.get_node(level, index)
    }

    /// Explicit flush (for shutdown)
    ///
    /// # Errors
    /// Returns IO error if flush fails
    pub fn flush(&self) -> io::Result<()> {
        self.slab.flush()
    }

    /// Get path for debugging/logging
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Update parent hashes after appending a leaf
    ///
    /// Traverses from the newly added leaf up to the root, updating all
    /// affected parent hashes.
    ///
    /// # Arguments
    /// * `leaf_index` - Index of the newly added leaf
    ///
    /// # Errors
    /// Returns IO error if any write fails
    fn update_tree_after_append(&mut self, leaf_index: u64) -> io::Result<()> {
        let mut current_index = leaf_index;
        let mut current_level = 0u32;

        // Traverse up the tree, computing and storing parent hashes
        loop {
            // Check if we're at root (no siblings)
            let parent_index = current_index / 2;
            let is_left_child = current_index.is_multiple_of(2);

            // Get sibling node
            let sibling_index = if is_left_child {
                current_index + 1
            } else {
                current_index - 1
            };

            // Check if sibling exists at this level
            let sibling = self.slab.get_node(current_level, sibling_index);

            // If no sibling, we're at the edge of the tree - stop here
            // The parent will be computed when the sibling is added
            if sibling.is_none() {
                break;
            }

            // Get current node
            let current = self
                .slab
                .get_node(current_level, current_index)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "missing node at level {} index {}",
                            current_level, current_index
                        ),
                    )
                })?;

            // Compute parent hash
            let parent_hash = if is_left_child {
                atl_core::core::merkle::hash_children(&current, &sibling.unwrap())
            } else {
                atl_core::core::merkle::hash_children(&sibling.unwrap(), &current)
            };

            // Write parent hash at next level
            self.slab
                .set_node(current_level + 1, parent_index, &parent_hash)?;

            // Move up to parent
            current_index = parent_index;
            current_level += 1;

            // If we're at the root (index 0 at this level), stop
            if current_index == 0 {
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atl_core::core::merkle::verify_inclusion;
    use tempfile::tempdir;

    #[test]
    fn should_create_new_slab_when_file_not_exists() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("super_tree.slab");

        let slab = PinnedSuperTreeSlab::open_or_create(&path, 1000).unwrap();
        assert_eq!(slab.leaf_count(), 0);
        assert!(path.exists());
    }

    #[test]
    fn should_open_existing_slab_with_correct_count() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("super_tree.slab");

        // Create and append leaves (use non-zero hashes)
        {
            let mut slab = PinnedSuperTreeSlab::open_or_create(&path, 1000).unwrap();
            for i in 0..5 {
                let mut leaf = [0u8; 32];
                leaf[0] = i as u8 + 1; // +1 to avoid all-zero hash
                slab.append_leaf(&leaf).unwrap();
            }
        }

        // Reopen and verify count
        let slab = PinnedSuperTreeSlab::open_or_create(&path, 1000).unwrap();
        assert_eq!(slab.leaf_count(), 5);
    }

    #[test]
    fn should_append_leaf_and_return_root() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("super_tree.slab");

        let mut slab = PinnedSuperTreeSlab::open_or_create(&path, 1000).unwrap();
        let leaf = [1u8; 32];
        let root = slab.append_leaf(&leaf).unwrap();

        // For single leaf, root == leaf
        assert_eq!(root, leaf);
        assert_eq!(slab.leaf_count(), 1);
    }

    #[test]
    fn should_compute_correct_root_for_multiple_leaves() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("super_tree.slab");

        let mut slab = PinnedSuperTreeSlab::open_or_create(&path, 1000).unwrap();
        let mut leaves = Vec::new();

        // Append 10 leaves (use non-zero hashes)
        for i in 0..10 {
            let mut leaf = [0u8; 32];
            leaf[0] = i as u8 + 1; // +1 to avoid all-zero hash
            leaves.push(leaf);
            slab.append_leaf(&leaf).unwrap();
        }

        // Get root from slab
        let root = slab.get_root(10).unwrap();

        // Compute expected root using atl_core
        let expected = compute_root(&leaves);

        assert_eq!(root, expected);
    }

    #[test]
    fn should_generate_valid_inclusion_proof() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("super_tree.slab");

        let mut slab = PinnedSuperTreeSlab::open_or_create(&path, 1000).unwrap();
        let mut leaves = Vec::new();

        // Append 10 leaves (use non-zero hashes)
        for i in 0..10 {
            let mut leaf = [0u8; 32];
            leaf[0] = i as u8 + 1; // +1 to avoid all-zero hash
            leaves.push(leaf);
            slab.append_leaf(&leaf).unwrap();
        }

        // Generate proof for leaf 5
        let proof_path = slab.get_inclusion_path(5, 10).unwrap();
        let root = slab.get_root(10).unwrap();

        // Verify proof using atl_core
        let proof = atl_core::core::merkle::InclusionProof {
            leaf_index: 5,
            tree_size: 10,
            path: proof_path,
        };

        let valid = verify_inclusion(&leaves[5], &proof, &root).unwrap();
        assert!(valid);
    }

    #[test]
    fn should_return_error_when_index_out_of_bounds() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("super_tree.slab");

        let mut slab = PinnedSuperTreeSlab::open_or_create(&path, 1000).unwrap();

        // Append 5 leaves (use non-zero hashes)
        for i in 0..5 {
            let mut leaf = [0u8; 32];
            leaf[0] = i as u8 + 1; // +1 to avoid all-zero hash
            slab.append_leaf(&leaf).unwrap();
        }

        // Try to get proof for out-of-bounds index
        let result = slab.get_inclusion_path(10, 5);
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("data_tree_index"));
    }

    #[test]
    fn should_persist_leaf_count_after_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("super_tree.slab");

        // Append 3 leaves (use non-zero hashes)
        {
            let mut slab = PinnedSuperTreeSlab::open_or_create(&path, 1000).unwrap();
            for i in 0..3 {
                let mut leaf = [0u8; 32];
                leaf[0] = i as u8 + 1; // +1 to avoid all-zero hash
                slab.append_leaf(&leaf).unwrap();
            }
            slab.flush().unwrap();
        }

        // Reopen and verify count
        let slab = PinnedSuperTreeSlab::open_or_create(&path, 1000).unwrap();
        assert_eq!(slab.leaf_count(), 3);
    }

    #[test]
    fn should_compute_empty_tree_root() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("super_tree.slab");

        let slab = PinnedSuperTreeSlab::open_or_create(&path, 1000).unwrap();
        let root = slab.get_root(0).unwrap();

        // Empty tree root is SHA-256 of empty string
        let expected = compute_root(&[]);
        assert_eq!(root, expected);
    }
}
