//! Multi-slab coordination and management
//!
//! Manages multiple slab files and coordinates cross-slab operations.

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use super::format::{SlabHeader, DEFAULT_SLAB_CAPACITY};
use super::mmap::SlabFile;

/// Configuration for slab manager
#[derive(Debug, Clone)]
pub struct SlabConfig {
    /// Maximum leaves per slab
    pub max_leaves: u32,

    /// Maximum open slabs in memory
    pub max_open_slabs: usize,
}

impl Default for SlabConfig {
    fn default() -> Self {
        Self {
            max_leaves: DEFAULT_SLAB_CAPACITY,
            max_open_slabs: 10,
        }
    }
}

/// Manages multiple slab files for large Merkle trees
///
/// Coordinates cross-slab operations and maintains LRU eviction policy.
pub struct SlabManager {
    /// Directory containing slab files
    slab_dir: PathBuf,

    /// Open slab files (slab_id -> SlabFile)
    slabs: HashMap<u32, SlabFile>,

    /// LRU tracking (slab_id -> last access order)
    lru_order: Vec<u32>,

    /// Current active slab for writes
    active_slab: Option<u32>,

    /// Configuration
    config: SlabConfig,

    /// Total tree size across all slabs
    tree_size: u64,
}

impl SlabManager {
    /// Create new manager for a fresh log
    ///
    /// # Arguments
    /// * `slab_dir` - Directory to store slab files
    /// * `config` - Slab configuration
    ///
    /// # Errors
    /// Returns IO error if directory creation fails
    pub fn new(slab_dir: PathBuf, config: SlabConfig) -> io::Result<Self> {
        std::fs::create_dir_all(&slab_dir)?;

        Ok(Self {
            slab_dir,
            slabs: HashMap::new(),
            lru_order: Vec::new(),
            active_slab: None,
            config,
            tree_size: 0,
        })
    }

    /// Open existing manager and load slab metadata
    ///
    /// Scans directory for existing slab files and reconstructs state.
    ///
    /// # Arguments
    /// * `slab_dir` - Directory containing slab files
    /// * `config` - Slab configuration
    ///
    /// # Errors
    /// Returns IO error if directory doesn't exist or files are corrupted
    #[allow(dead_code)]
    pub fn open(slab_dir: PathBuf, config: SlabConfig) -> io::Result<Self> {
        let mut manager = Self::new(slab_dir, config)?;

        // Scan for existing slab files
        let entries = std::fs::read_dir(&manager.slab_dir)?;
        let mut slab_ids = Vec::new();

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("dat") {
                if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Some(id_str) = name.strip_prefix("slab_") {
                        if let Ok(id) = id_str.parse::<u32>() {
                            slab_ids.push(id);
                        }
                    }
                }
            }
        }

        // Sort and find active slab
        slab_ids.sort_unstable();

        if let Some(&last_id) = slab_ids.last() {
            // Open last slab to check if it's full
            let slab = manager.get_or_open_slab(last_id)?;
            let is_full = slab.is_full();
            let leaf_count = slab.leaf_count();
            let max_leaves = manager.config.max_leaves;

            if !is_full {
                manager.active_slab = Some(last_id);
            }

            // Compute total tree size
            manager.tree_size = (last_id as u64 - 1) * (max_leaves as u64) + (leaf_count as u64);
        }

        Ok(manager)
    }

    /// Get node hash by global coordinates
    ///
    /// Automatically selects the correct slab.
    ///
    /// # Arguments
    /// * `level` - Tree level (0 = leaves)
    /// * `leaf_index` - Global leaf index
    ///
    /// # Errors
    /// Returns IO error if slab cannot be opened
    pub fn get_node(&mut self, level: u32, leaf_index: u64) -> io::Result<Option<[u8; 32]>> {
        let slab_id = self.leaf_index_to_slab_id(leaf_index);
        let local_index = leaf_index % (self.config.max_leaves as u64);

        let slab = self.get_or_open_slab(slab_id)?;
        Ok(slab.get_node(level, local_index))
    }

    /// Append leaves and update tree
    ///
    /// Computes parent hashes bottom-up using RFC 6962 algorithm.
    ///
    /// # Arguments
    /// * `leaf_hashes` - Precomputed leaf hashes to append
    ///
    /// # Returns
    /// New global root hash after all leaves are appended
    ///
    /// # Errors
    /// Returns IO error if slab operations fail
    pub fn append_leaves(&mut self, leaf_hashes: &[[u8; 32]]) -> io::Result<[u8; 32]> {
        for leaf_hash in leaf_hashes {
            let slab_id = self.ensure_active_slab()?;
            let slab = self.slabs.get_mut(&slab_id).expect("active slab missing");

            let local_index = slab.leaf_count() as u64;
            slab.set_node(0, local_index, leaf_hash);

            // Update parents bottom-up
            self.update_tree_after_append(slab_id, local_index)?;

            self.tree_size += 1;
        }

        self.get_root(self.tree_size)
    }

    /// Get root hash for given tree size
    ///
    /// Combines slab roots if tree spans multiple slabs.
    ///
    /// # Arguments
    /// * `tree_size` - Tree size to compute root for
    ///
    /// # Errors
    /// Returns IO error if slab operations fail
    pub fn get_root(&mut self, tree_size: u64) -> io::Result<[u8; 32]> {
        if tree_size == 0 {
            return Ok([0u8; 32]);
        }

        let num_slabs = self.num_slabs_for_size(tree_size);

        if num_slabs == 1 {
            // Single slab - return its root
            self.get_slab_root_by_id(1, tree_size as u32)
        } else {
            // Multi-slab - combine roots using RFC 6962
            self.compute_cross_slab_root(tree_size)
        }
    }

    /// Get inclusion proof path
    ///
    /// Generates RFC 6962 compliant inclusion proof.
    ///
    /// # Arguments
    /// * `leaf_index` - Global leaf index to prove
    /// * `tree_size` - Tree size for proof
    ///
    /// # Errors
    /// Returns IO error if slab operations fail or index out of bounds
    pub fn get_inclusion_path(
        &mut self,
        leaf_index: u64,
        tree_size: u64,
    ) -> io::Result<Vec<[u8; 32]>> {
        if leaf_index >= tree_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "leaf_index >= tree_size",
            ));
        }

        let slab_id = self.leaf_index_to_slab_id(leaf_index);
        let local_index = leaf_index % (self.config.max_leaves as u64);

        // Get local path within slab (limited by actual tree size)
        let mut path = self.get_local_inclusion_path(slab_id, local_index, tree_size)?;

        // Add cross-slab path if needed
        if self.num_slabs_for_size(tree_size) > 1 {
            let cross_path = self.get_cross_slab_path(slab_id, tree_size)?;
            path.extend(cross_path);
        }

        Ok(path)
    }

    /// Flush all open slabs to disk
    ///
    /// # Errors
    /// Returns IO error if any flush fails
    pub fn flush(&mut self) -> io::Result<()> {
        for slab in self.slabs.values() {
            slab.flush()?;
        }
        Ok(())
    }

    /// Get current tree size
    #[allow(dead_code)]
    #[must_use]
    pub fn tree_size(&self) -> u64 {
        self.tree_size
    }

    /// Update tree after appending a leaf
    ///
    /// Recomputes all affected parent nodes bottom-up.
    fn update_tree_after_append(&mut self, slab_id: u32, local_index: u64) -> io::Result<()> {
        let slab = self.slabs.get_mut(&slab_id).expect("slab missing");
        let max_height = SlabHeader::tree_height(self.config.max_leaves);

        let mut current_index = local_index;
        for level in 0..max_height {
            let parent_index = current_index / 2;

            // Compute parent hash
            let left_child = slab.get_node(level, parent_index * 2).unwrap_or([0u8; 32]);
            let right_child = slab
                .get_node(level, parent_index * 2 + 1)
                .unwrap_or(left_child); // RFC 6962: duplicate if odd

            let parent_hash = Self::hash_internal_node(&left_child, &right_child);
            slab.set_node(level + 1, parent_index, &parent_hash);

            current_index = parent_index;
        }

        Ok(())
    }

    /// Get or open a slab by ID
    fn get_or_open_slab(&mut self, slab_id: u32) -> io::Result<&mut SlabFile> {
        if !self.slabs.contains_key(&slab_id) {
            let path = self.slab_path(slab_id);
            let slab = SlabFile::open(&path)?;
            self.slabs.insert(slab_id, slab);
            self.touch_slab(slab_id);
            self.evict_lru();
        } else {
            self.touch_slab(slab_id);
        }

        Ok(self.slabs.get_mut(&slab_id).expect("slab just inserted"))
    }

    /// Ensure active slab exists and is not full
    fn ensure_active_slab(&mut self) -> io::Result<u32> {
        if let Some(id) = self.active_slab {
            if !self.slabs.get(&id).is_some_and(SlabFile::is_full) {
                return Ok(id);
            }
        }

        // Create new slab
        self.create_new_slab()
    }

    /// Create new slab file
    fn create_new_slab(&mut self) -> io::Result<u32> {
        let new_id = self.active_slab.map_or(1, |id| id + 1);
        let start_index = (new_id as u64 - 1) * (self.config.max_leaves as u64);
        let path = self.slab_path(new_id);

        let slab = SlabFile::create(&path, new_id, start_index, self.config.max_leaves)?;
        self.slabs.insert(new_id, slab);
        self.active_slab = Some(new_id);
        self.touch_slab(new_id);
        self.evict_lru();

        Ok(new_id)
    }

    /// Close least-recently-used slabs to stay under limit
    fn evict_lru(&mut self) {
        while self.slabs.len() > self.config.max_open_slabs && !self.lru_order.is_empty() {
            let oldest = self.lru_order.remove(0);
            if Some(oldest) != self.active_slab {
                self.slabs.remove(&oldest);
            }
        }
    }

    /// Update LRU tracking
    fn touch_slab(&mut self, slab_id: u32) {
        self.lru_order.retain(|&id| id != slab_id);
        self.lru_order.push(slab_id);
    }

    /// Convert global leaf index to slab ID
    fn leaf_index_to_slab_id(&self, leaf_index: u64) -> u32 {
        (leaf_index / (self.config.max_leaves as u64)) as u32 + 1
    }

    /// Calculate number of slabs needed for tree size
    fn num_slabs_for_size(&self, tree_size: u64) -> u32 {
        tree_size.div_ceil(self.config.max_leaves as u64) as u32
    }

    /// Get slab file path
    fn slab_path(&self, slab_id: u32) -> PathBuf {
        self.slab_dir.join(format!("slab_{slab_id:04}.dat"))
    }

    /// Get root hash for a slab by ID
    fn get_slab_root_by_id(&mut self, slab_id: u32, leaf_count: u32) -> io::Result<[u8; 32]> {
        if leaf_count == 0 {
            return Ok([0u8; 32]);
        }

        let slab = self.get_or_open_slab(slab_id)?;

        let leaves: Vec<[u8; 32]> = (0..leaf_count)
            .map(|i| slab.get_node(0, u64::from(i)).unwrap_or([0u8; 32]))
            .collect();

        Ok(atl_core::core::merkle::compute_root(&leaves))
    }

    /// Get root hash for a single slab
    #[allow(dead_code)]
    fn get_slab_root(&self, slab: &SlabFile, leaf_count: u32) -> io::Result<[u8; 32]> {
        if leaf_count == 0 {
            return Ok([0u8; 32]);
        }

        let leaves: Vec<[u8; 32]> = (0..leaf_count)
            .map(|i| slab.get_node(0, u64::from(i)).unwrap_or([0u8; 32]))
            .collect();

        Ok(atl_core::core::merkle::compute_root(&leaves))
    }

    /// Compute cross-slab root by combining slab roots
    fn compute_cross_slab_root(&mut self, tree_size: u64) -> io::Result<[u8; 32]> {
        let num_slabs = self.num_slabs_for_size(tree_size);
        let max_leaves = self.config.max_leaves;
        let mut roots = Vec::new();

        for slab_id in 1..=num_slabs {
            let leaves_in_slab = if slab_id < num_slabs {
                max_leaves
            } else {
                ((tree_size - 1) % (max_leaves as u64) + 1) as u32
            };
            let root = self.get_slab_root_by_id(slab_id, leaves_in_slab)?;
            roots.push(root);
        }

        // Combine using binary tree (RFC 6962)
        Ok(Self::combine_roots(&roots))
    }

    /// Get local inclusion path within a slab
    ///
    /// The path is limited by the actual tree_size, not the slab's max capacity,
    /// to produce RFC 6962 compliant proofs.
    fn get_local_inclusion_path(
        &mut self,
        slab_id: u32,
        local_index: u64,
        tree_size: u64,
    ) -> io::Result<Vec<[u8; 32]>> {
        // Calculate actual tree height from tree_size (not max_leaves)
        let actual_height = if tree_size <= 1 {
            0
        } else {
            64 - (tree_size - 1).leading_zeros()
        };

        let slab = self.get_or_open_slab(slab_id)?;
        let mut path = Vec::new();
        let mut current_index = local_index;

        for level in 0..actual_height {
            let sibling_index = current_index ^ 1;
            if let Some(sibling) = slab.get_node(level, sibling_index) {
                path.push(sibling);
            }
            current_index /= 2;
        }

        Ok(path)
    }

    /// Get cross-slab path (sibling slab roots)
    fn get_cross_slab_path(
        &mut self,
        target_slab: u32,
        tree_size: u64,
    ) -> io::Result<Vec<[u8; 32]>> {
        let num_slabs = self.num_slabs_for_size(tree_size);
        let max_leaves = self.config.max_leaves;
        let mut cross_path = Vec::new();

        for slab_id in 1..=num_slabs {
            if slab_id != target_slab {
                let leaves = if slab_id < num_slabs {
                    max_leaves
                } else {
                    ((tree_size - 1) % (max_leaves as u64) + 1) as u32
                };
                let root = self.get_slab_root_by_id(slab_id, leaves)?;
                cross_path.push(root);
            }
        }

        Ok(cross_path)
    }

    /// Hash internal node using RFC 6962 (0x01 prefix)
    fn hash_internal_node(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update([0x01]); // Internal node prefix
        hasher.update(left);
        hasher.update(right);
        hasher.finalize().into()
    }

    /// Combine roots into single hash
    fn combine_roots(roots: &[[u8; 32]]) -> [u8; 32] {
        if roots.is_empty() {
            return [0u8; 32];
        }
        if roots.len() == 1 {
            return roots[0];
        }

        // Build binary tree
        let mut current = roots.to_vec();
        while current.len() > 1 {
            let mut next = Vec::new();
            for chunk in current.chunks(2) {
                if chunk.len() == 2 {
                    next.push(Self::hash_internal_node(&chunk[0], &chunk[1]));
                } else {
                    next.push(chunk[0]);
                }
            }
            current = next;
        }

        current[0]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_manager_create() {
        let dir = tempdir().unwrap();
        let manager = SlabManager::new(dir.path().to_path_buf(), SlabConfig::default()).unwrap();
        assert_eq!(manager.tree_size(), 0);
    }

    #[test]
    fn test_manager_append_single_slab() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 100,
                max_open_slabs: 2,
            },
        )
        .unwrap();

        let leaves: Vec<[u8; 32]> = (0..10).map(|i| [i as u8; 32]).collect();
        let _root = manager.append_leaves(&leaves).unwrap();

        assert_eq!(manager.tree_size(), 10);
    }

    #[test]
    fn test_manager_slab_rotation() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 100,
                max_open_slabs: 2,
            },
        )
        .unwrap();

        let leaves: Vec<[u8; 32]> = (0..150).map(|i| [i as u8; 32]).collect();
        let _root = manager.append_leaves(&leaves).unwrap();

        assert_eq!(manager.tree_size(), 150);
        assert!(dir.path().join("slab_0001.dat").exists());
        assert!(dir.path().join("slab_0002.dat").exists());
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

        let leaves: Vec<[u8; 32]> = (0..10).map(|i| [i as u8; 32]).collect();
        manager.append_leaves(&leaves).unwrap();

        let path = manager.get_inclusion_path(5, 10).unwrap();
        assert!(!path.is_empty());
    }

    #[test]
    fn test_root_matches_atl_core() {
        for size in [1, 2, 3, 5, 7, 10, 15, 31, 50] {
            let dir = tempdir().unwrap();
            let mut manager = SlabManager::new(
                dir.path().to_path_buf(),
                SlabConfig {
                    max_leaves: 100,
                    max_open_slabs: 2,
                },
            )
            .unwrap();

            let leaves: Vec<[u8; 32]> = (0..size).map(|i| [i as u8; 32]).collect();
            let root_from_manager = manager.append_leaves(&leaves).unwrap();

            let leaf_hashes: Vec<[u8; 32]> = (0..size).map(|i| [i as u8; 32]).collect();
            let expected_root = atl_core::core::merkle::compute_root(&leaf_hashes);

            assert_eq!(
                root_from_manager, expected_root,
                "Root mismatch for size {size}"
            );
        }
    }
}
