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

    /// In-memory cache of active slab leaves for O(1) root computation
    ///
    /// Contains all leaves of the currently active slab.
    /// At 5000 RPS for 10 minutes = max 3M entries = ~96 MB.
    /// Cleared on slab rotation.
    active_slab_leaves: Vec<[u8; 32]>,

    /// Cached root hash of active slab
    ///
    /// Updated incrementally after each append via `update_tree_after_append()`.
    /// `None` if no leaves in active slab.
    cached_active_root: Option<[u8; 32]>,
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
            active_slab_leaves: Vec::new(),
            cached_active_root: None,
        })
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
            slab.set_node(0, local_index, leaf_hash)?;

            // Push to in-memory cache (still needed for proof generation)
            self.active_slab_leaves.push(*leaf_hash);

            // O(log n) incremental tree update
            self.update_tree_after_append(local_index)?;

            self.tree_size += 1;
        }

        // Return cached root (O(1)) or compute for multi-slab case
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

    /// Get inclusion proof path using only in-memory cache (no mutation)
    ///
    /// This method takes `&self` instead of `&mut self`, allowing use with READ lock.
    /// Returns `None` if data is not available in cache (caller should fallback to mutable path).
    ///
    /// # Arguments
    /// * `leaf_index` - Global leaf index to prove
    /// * `tree_size` - Tree size for proof
    ///
    /// # Returns
    /// * `Some(path)` - Proof path if data available in cache
    /// * `None` - Data not in cache (e.g., multi-slab or cache miss)
    ///
    /// # Requirements
    /// * Only works for single-slab trees where leaf is in active slab
    /// * Requires `active_slab_leaves` to be populated
    pub fn get_inclusion_path_readonly(
        &self,
        leaf_index: u64,
        tree_size: u64,
    ) -> Option<Vec<[u8; 32]>> {
        // Validate inputs
        if tree_size == 0 || leaf_index >= tree_size {
            return None;
        }

        // Check if single-slab tree (most common case)
        let num_slabs = self.num_slabs_for_size(tree_size);
        if num_slabs != 1 {
            return None; // Multi-slab requires mutable path for LRU
        }

        // Check if we have data in cache
        if self.active_slab_leaves.len() < tree_size as usize {
            return None; // Cache miss - need mutable path
        }

        // OPTIMIZED: Reference the cache directly - NO ALLOCATION
        let leaves: &[[u8; 32]] = &self.active_slab_leaves[..tree_size as usize];

        // Closure borrows the slice (zero-copy)
        let get_node = |level: u32, index: u64| -> Option<[u8; 32]> {
            if level == 0 && (index as usize) < leaves.len() {
                Some(leaves[index as usize])
            } else {
                None
            }
        };

        // Generate proof - only allocates the result path (small: log2(tree_size) hashes)
        atl_core::core::merkle::generate_inclusion_proof(leaf_index, tree_size, get_node)
            .ok()
            .map(|p| p.path)
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

        let num_slabs = self.num_slabs_for_size(tree_size);

        if num_slabs == 1 {
            // Single slab - delegate directly to atl-core
            return self.get_local_inclusion_path(1, leaf_index, tree_size);
        }

        // Multi-slab case
        let slab_id = self.leaf_index_to_slab_id(leaf_index);
        let local_index = leaf_index % (self.config.max_leaves as u64);

        // Calculate leaves in target slab
        let max_leaves = self.config.max_leaves as u64;
        let leaves_in_slab = if slab_id < num_slabs {
            max_leaves
        } else {
            ((tree_size - 1) % max_leaves) + 1
        };

        // Get local path within slab
        let mut path = self.get_local_inclusion_path(slab_id, local_index, leaves_in_slab)?;

        // Add cross-slab path
        let cross_path = self.get_cross_slab_path(slab_id, tree_size)?;
        path.extend(cross_path);

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
    #[must_use]
    #[allow(dead_code)]
    pub fn tree_size(&self) -> u64 {
        self.tree_size
    }

    /// Initialize the in-memory leaf cache for the active slab
    ///
    /// Should be called after crash recovery if an active slab exists.
    /// Loads all existing leaves from the slab file into memory.
    ///
    /// # Arguments
    /// * `tree_size` - Total tree size (to calculate leaves in active slab)
    ///
    /// # Errors
    /// Returns IO error if slab cannot be read
    pub fn initialize_leaf_cache(&mut self, tree_size: u64) -> io::Result<()> {
        // Calculate which slab should be active and how many leaves it has
        if tree_size == 0 {
            self.active_slab_leaves.clear();
            self.cached_active_root = None;
            return Ok(());
        }

        let max_leaves = self.config.max_leaves as u64;
        let slab_id = ((tree_size - 1) / max_leaves) as u32 + 1;

        // Ensure slab is open and active
        self.get_or_open_slab(slab_id)?;
        self.active_slab = Some(slab_id);

        // Load leaves into cache
        self.load_active_slab_leaves(slab_id)?;

        // Compute initial root from loaded leaves
        if !self.active_slab_leaves.is_empty() {
            self.cached_active_root =
                Some(atl_core::core::merkle::compute_root(&self.active_slab_leaves));
        } else {
            self.cached_active_root = None;
        }

        // Update tree_size
        self.tree_size = tree_size;

        Ok(())
    }

    /// Load leaves from a slab file into the in-memory cache
    ///
    /// Called during recovery when an active slab already has data.
    fn load_active_slab_leaves(&mut self, slab_id: u32) -> io::Result<()> {
        let slab = self.get_or_open_slab(slab_id)?;
        let leaf_count = slab.leaf_count();

        // Collect all leaves first to avoid borrowing conflicts
        let mut leaves = Vec::with_capacity(leaf_count as usize);
        for i in 0..leaf_count {
            if let Some(hash) = slab.get_node(0, u64::from(i)) {
                leaves.push(hash);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("missing leaf {i} in slab {slab_id}"),
                ));
            }
        }

        // Now update the cache
        self.active_slab_leaves = leaves;

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

        // Clear leaf cache for new slab
        self.active_slab_leaves.clear();
        // Pre-allocate for expected capacity
        self.active_slab_leaves.reserve(self.config.max_leaves as usize);
        // Clear cached root for new slab
        self.cached_active_root = None;

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
    ///
    /// Uses cached root for active slab (O(1)), reads from disk for closed slabs.
    ///
    /// # Arguments
    /// * `slab_id` - Slab identifier
    /// * `leaf_count` - Number of leaves to consider (may be less than slab capacity)
    ///
    /// # Returns
    /// Root hash for the specified leaf count
    fn get_slab_root_by_id(&mut self, slab_id: u32, leaf_count: u32) -> io::Result<[u8; 32]> {
        if leaf_count == 0 {
            return Ok([0u8; 32]);
        }

        if leaf_count == 1 {
            // Single leaf case: check cache first, then slab
            if self.active_slab == Some(slab_id) && !self.active_slab_leaves.is_empty() {
                return Ok(self.active_slab_leaves[0]);
            }
            let slab = self.get_or_open_slab(slab_id)?;
            return Ok(slab.get_node(0, 0).unwrap_or([0u8; 32]));
        }

        // Fast path: Use cached root for active slab (O(1))
        if self.active_slab == Some(slab_id)
            && self.active_slab_leaves.len() == leaf_count as usize
        {
            if let Some(root) = self.cached_active_root {
                return Ok(root);
            }
            // Cache miss - compute and cache (happens after recovery)
            let root = atl_core::core::merkle::compute_root(&self.active_slab_leaves);
            self.cached_active_root = Some(root);
            return Ok(root);
        }

        // Fallback: read leaves from slab file (closed slabs or cache miss)
        let slab = self.get_or_open_slab(slab_id)?;
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

    /// Get local inclusion path within a slab using atl-core
    ///
    /// Generates RFC 6962 compliant proof by delegating to atl-core.
    ///
    /// # Arguments
    /// * `slab_id` - Slab ID to generate proof from
    /// * `local_index` - Leaf index within the slab
    /// * `local_tree_size` - Tree size for proof (limited to this slab)
    ///
    /// # Errors
    /// Returns IO error if tree_size is 0, index out of bounds, or proof generation fails
    fn get_local_inclusion_path(
        &mut self,
        slab_id: u32,
        local_index: u64,
        local_tree_size: u64,
    ) -> io::Result<Vec<[u8; 32]>> {
        if local_tree_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tree_size cannot be 0",
            ));
        }
        if local_index >= local_tree_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "local_index >= local_tree_size",
            ));
        }

        // OPTIMIZATION: Use active_slab_leaves cache for active slab
        // This avoids mmap reads which may see stale data without flush
        let leaves: Vec<[u8; 32]> = if self.active_slab == Some(slab_id)
            && self.active_slab_leaves.len() >= local_tree_size as usize
        {
            // Fast path: read from in-memory cache
            self.active_slab_leaves[..local_tree_size as usize].to_vec()
        } else {
            // Slow path: read from mmap (closed slabs or cache miss)
            let slab = self.get_or_open_slab(slab_id)?;
            (0..local_tree_size)
                .map(|i| slab.get_node(0, i).unwrap_or([0u8; 32]))
                .collect()
        };

        // Create callback for atl-core to fetch leaf nodes
        let get_node = |level: u32, index: u64| -> Option<[u8; 32]> {
            // atl-core's compute_subtree_root only requests level 0 (leaves)
            if level == 0 && (index as usize) < leaves.len() {
                Some(leaves[index as usize])
            } else {
                None
            }
        };

        // Generate RFC 6962 compliant proof
        let proof = atl_core::core::merkle::generate_inclusion_proof(
            local_index,
            local_tree_size,
            get_node,
        )
        .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(proof.path)
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

    /// Update parent hashes after appending a leaf (O(log n) incremental update)
    ///
    /// Traverses from the newly added leaf up to the root, computing and storing
    /// parent hashes. Also updates `cached_active_root` with the new root.
    ///
    /// This is the RFC 6962 incremental Merkle tree update algorithm,
    /// ported from `PinnedSuperTreeSlab`.
    ///
    /// # Arguments
    /// * `leaf_index` - Local index within active slab (0-based)
    ///
    /// # Errors
    /// Returns IO error if any write fails
    fn update_tree_after_append(&mut self, leaf_index: u64) -> io::Result<()> {
        let slab_id = self.active_slab.expect("must have active slab");
        let slab = self.slabs.get_mut(&slab_id).expect("active slab missing");

        let mut current_index = leaf_index;
        let mut current_level = 0u32;

        // Traverse up the tree, computing and storing parent hashes
        loop {
            let parent_index = current_index / 2;
            let is_left_child = current_index.is_multiple_of(2);

            // Get sibling node index
            let sibling_index = if is_left_child {
                current_index + 1
            } else {
                current_index - 1
            };

            // Check if sibling exists at this level
            let sibling = slab.get_node(current_level, sibling_index);

            // If no sibling, we're at the edge of the tree - stop here
            if sibling.is_none() {
                break;
            }

            // Get current node
            let current = slab.get_node(current_level, current_index).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("missing node at level {current_level} index {current_index}"),
                )
            })?;

            // Compute parent hash using RFC 6962 algorithm
            let parent_hash = if is_left_child {
                atl_core::core::merkle::hash_children(&current, &sibling.unwrap())
            } else {
                atl_core::core::merkle::hash_children(&sibling.unwrap(), &current)
            };

            // Write parent hash at next level
            slab.set_node(current_level + 1, parent_index, &parent_hash)?;

            // Move up to parent
            current_index = parent_index;
            current_level += 1;

            // If we're at index 0 at this level, we've computed the subtree root
            if current_index == 0 {
                break;
            }
        }

        // Update cached root from the tree structure
        self.update_cached_root()?;

        Ok(())
    }

    /// Update cached active root from stored intermediate nodes
    ///
    /// Reads the current root from the tree structure in the slab file.
    /// For a tree with N leaves, the root is computed from the highest stored nodes.
    fn update_cached_root(&mut self) -> io::Result<()> {
        let leaf_count = self.active_slab_leaves.len();

        if leaf_count == 0 {
            self.cached_active_root = None;
            return Ok(());
        }

        if leaf_count == 1 {
            self.cached_active_root = Some(self.active_slab_leaves[0]);
            return Ok(());
        }

        // For multiple leaves, compute root using RFC 6962 algorithm
        let root = atl_core::core::merkle::compute_root(&self.active_slab_leaves);
        self.cached_active_root = Some(root);

        Ok(())
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

    #[test]
    fn test_inclusion_proof_verifies_with_atl_core() {
        // Test various tree sizes
        for size in [1u64, 2, 3, 5, 7, 10, 15, 31, 50] {
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
            let root = manager.append_leaves(&leaves).unwrap();

            // Test inclusion proof for each leaf
            for leaf_idx in 0..size {
                let path = manager.get_inclusion_path(leaf_idx, size).unwrap();

                // Verify with atl-core
                let proof = atl_core::core::merkle::InclusionProof {
                    leaf_index: leaf_idx,
                    tree_size: size,
                    path: path.clone(),
                };

                let verified = atl_core::core::merkle::verify_inclusion(
                    &leaves[leaf_idx as usize],
                    &proof,
                    &root,
                )
                .expect("verification should not error");

                assert!(
                    verified,
                    "Proof failed for leaf {} in tree of size {}",
                    leaf_idx, size
                );
            }
        }
    }

    #[test]
    fn test_single_leaf_proof() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 100,
                max_open_slabs: 2,
            },
        )
        .unwrap();

        let leaves = vec![[42u8; 32]];
        let root = manager.append_leaves(&leaves).unwrap();

        let path = manager.get_inclusion_path(0, 1).unwrap();
        assert!(
            path.is_empty(),
            "Single leaf tree should have empty proof path"
        );

        // Root should equal the single leaf
        assert_eq!(root, leaves[0]);
    }

    #[test]
    fn test_get_root_zero_size() {
        let dir = tempdir().unwrap();
        let mut manager =
            SlabManager::new(dir.path().to_path_buf(), SlabConfig::default()).unwrap();

        let root = manager.get_root(0).unwrap();
        assert_eq!(root, [0u8; 32]);
    }

    #[test]
    fn test_get_node_basic() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 100,
                max_open_slabs: 2,
            },
        )
        .unwrap();

        let leaves: Vec<[u8; 32]> = vec![[1u8; 32], [2u8; 32], [3u8; 32]];
        manager.append_leaves(&leaves).unwrap();

        let node = manager.get_node(0, 0).unwrap();
        assert_eq!(node, Some([1u8; 32]));

        let node = manager.get_node(0, 1).unwrap();
        assert_eq!(node, Some([2u8; 32]));
    }

    #[test]
    fn test_flush_operation() {
        let dir = tempdir().unwrap();
        let mut manager =
            SlabManager::new(dir.path().to_path_buf(), SlabConfig::default()).unwrap();

        let leaves: Vec<[u8; 32]> = vec![[1u8; 32], [2u8; 32]];
        manager.append_leaves(&leaves).unwrap();

        let result = manager.flush();
        assert!(result.is_ok());
    }

    #[test]
    fn test_tree_size_getter() {
        let dir = tempdir().unwrap();
        let mut manager =
            SlabManager::new(dir.path().to_path_buf(), SlabConfig::default()).unwrap();

        assert_eq!(manager.tree_size(), 0);

        let leaves: Vec<[u8; 32]> = vec![[1u8; 32], [2u8; 32], [3u8; 32]];
        manager.append_leaves(&leaves).unwrap();

        assert_eq!(manager.tree_size(), 3);
    }

    #[test]
    fn test_lru_eviction() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 2,
                max_open_slabs: 2,
            },
        )
        .unwrap();

        // Fill first slab
        manager.append_leaves(&[[1u8; 32], [2u8; 32]]).unwrap();

        // Fill second slab (will create slab 2 as active)
        manager.append_leaves(&[[3u8; 32], [4u8; 32]]).unwrap();

        // Fill third slab - should trigger eviction
        manager.append_leaves(&[[5u8; 32], [6u8; 32]]).unwrap();

        // Should have evicted one slab, keeping active and one more
        assert!(manager.slabs.len() <= manager.config.max_open_slabs);

        // Active slab (3) should still be present
        assert_eq!(manager.active_slab, Some(3));
        assert!(manager.slabs.contains_key(&3));
    }

    #[test]
    fn test_inclusion_proof_out_of_bounds() {
        let dir = tempdir().unwrap();
        let mut manager =
            SlabManager::new(dir.path().to_path_buf(), SlabConfig::default()).unwrap();

        let leaves: Vec<[u8; 32]> = vec![[1u8; 32], [2u8; 32], [3u8; 32]];
        manager.append_leaves(&leaves).unwrap();

        let result = manager.get_inclusion_path(5, 3);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_multi_slab_spanning() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 10,
                max_open_slabs: 3,
            },
        )
        .unwrap();

        let leaves: Vec<[u8; 32]> = (0..25).map(|i| [i as u8; 32]).collect();
        manager.append_leaves(&leaves).unwrap();

        assert_eq!(manager.tree_size(), 25);

        // Should span 3 slabs
        let num_slabs = manager.num_slabs_for_size(25);
        assert_eq!(num_slabs, 3);
    }

    #[test]
    fn test_multi_slab_root_computation() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 5,
                max_open_slabs: 5,
            },
        )
        .unwrap();

        let leaves: Vec<[u8; 32]> = (0..12).map(|i| [i as u8; 32]).collect();
        let root = manager.append_leaves(&leaves).unwrap();

        // Should compute cross-slab root
        let recomputed_root = manager.get_root(12).unwrap();
        assert_eq!(root, recomputed_root);
    }

    #[test]
    fn test_multi_slab_inclusion_proof() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 8,
                max_open_slabs: 3,
            },
        )
        .unwrap();

        let leaves: Vec<[u8; 32]> = (0..20).map(|i| [i as u8; 32]).collect();
        manager.append_leaves(&leaves).unwrap();

        // Test proof from first slab
        let path1 = manager.get_inclusion_path(3, 20).unwrap();
        assert!(!path1.is_empty());

        // Test proof from second slab
        let path2 = manager.get_inclusion_path(10, 20).unwrap();
        assert!(!path2.is_empty());
    }

    #[test]
    fn test_slab_config_default() {
        let config = SlabConfig::default();
        assert_eq!(config.max_leaves, DEFAULT_SLAB_CAPACITY);
        assert_eq!(config.max_open_slabs, 10);
    }

    #[test]
    fn test_slab_config_custom() {
        let config = SlabConfig {
            max_leaves: 50,
            max_open_slabs: 5,
        };
        assert_eq!(config.max_leaves, 50);
        assert_eq!(config.max_open_slabs, 5);
    }

    #[test]
    fn test_combine_roots_empty() {
        let roots: Vec<[u8; 32]> = vec![];
        let result = SlabManager::combine_roots(&roots);
        assert_eq!(result, [0u8; 32]);
    }

    #[test]
    fn test_combine_roots_single() {
        let root = [42u8; 32];
        let result = SlabManager::combine_roots(&[root]);
        assert_eq!(result, root);
    }

    #[test]
    fn test_combine_roots_multiple() {
        let roots = vec![[1u8; 32], [2u8; 32], [3u8; 32]];
        let result = SlabManager::combine_roots(&roots);
        assert_ne!(result, [0u8; 32]);
    }

    #[test]
    fn test_hash_internal_node() {
        let left = [1u8; 32];
        let right = [2u8; 32];

        let hash1 = SlabManager::hash_internal_node(&left, &right);
        let hash2 = SlabManager::hash_internal_node(&left, &right);

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, [0u8; 32]);
    }

    #[test]
    fn test_leaf_index_to_slab_id() {
        let dir = tempdir().unwrap();
        let manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 100,
                max_open_slabs: 2,
            },
        )
        .unwrap();

        assert_eq!(manager.leaf_index_to_slab_id(0), 1);
        assert_eq!(manager.leaf_index_to_slab_id(50), 1);
        assert_eq!(manager.leaf_index_to_slab_id(99), 1);
        assert_eq!(manager.leaf_index_to_slab_id(100), 2);
        assert_eq!(manager.leaf_index_to_slab_id(250), 3);
    }

    #[test]
    fn test_num_slabs_for_size() {
        let dir = tempdir().unwrap();
        let manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 100,
                max_open_slabs: 2,
            },
        )
        .unwrap();

        assert_eq!(manager.num_slabs_for_size(1), 1);
        assert_eq!(manager.num_slabs_for_size(50), 1);
        assert_eq!(manager.num_slabs_for_size(100), 1);
        assert_eq!(manager.num_slabs_for_size(101), 2);
        assert_eq!(manager.num_slabs_for_size(250), 3);
    }

    #[test]
    fn test_slab_path_formatting() {
        let dir = tempdir().unwrap();
        let manager = SlabManager::new(dir.path().to_path_buf(), SlabConfig::default()).unwrap();

        let path1 = manager.slab_path(1);
        assert!(path1.ends_with("slab_0001.dat"));

        let path99 = manager.slab_path(99);
        assert!(path99.ends_with("slab_0099.dat"));

        let path1000 = manager.slab_path(1000);
        assert!(path1000.ends_with("slab_1000.dat"));
    }

    #[test]
    fn test_append_empty_leaves() {
        let dir = tempdir().unwrap();
        let mut manager =
            SlabManager::new(dir.path().to_path_buf(), SlabConfig::default()).unwrap();

        let empty_leaves: Vec<[u8; 32]> = vec![];
        let root = manager.append_leaves(&empty_leaves).unwrap();

        assert_eq!(manager.tree_size(), 0);
        assert_eq!(root, [0u8; 32]);
    }

    #[test]
    fn test_get_node_nonexistent_slab() {
        let dir = tempdir().unwrap();
        let mut manager =
            SlabManager::new(dir.path().to_path_buf(), SlabConfig::default()).unwrap();

        // Try to get node from non-existent slab
        let result = manager.get_node(0, 1000);
        assert!(result.is_err());
    }

    #[test]
    fn test_touch_slab_updates_lru() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 5,
                max_open_slabs: 2,
            },
        )
        .unwrap();

        manager.append_leaves(&[[1u8; 32]]).unwrap();

        assert!(!manager.lru_order.is_empty());
        assert_eq!(manager.lru_order.last(), Some(&1));
    }

    #[test]
    fn test_ensure_active_slab_creates_new() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 2,
                max_open_slabs: 5,
            },
        )
        .unwrap();

        assert_eq!(manager.active_slab, None);

        manager.append_leaves(&[[1u8; 32], [2u8; 32]]).unwrap();
        assert_eq!(manager.active_slab, Some(1));

        // Fill first slab and trigger new one
        manager.append_leaves(&[[3u8; 32]]).unwrap();
        assert_eq!(manager.active_slab, Some(2));
    }

    #[test]
    fn test_get_slab_root_by_id_zero_leaves() {
        let dir = tempdir().unwrap();
        let mut manager =
            SlabManager::new(dir.path().to_path_buf(), SlabConfig::default()).unwrap();

        let root = manager.get_slab_root_by_id(1, 0).unwrap();
        assert_eq!(root, [0u8; 32]);
    }

    #[test]
    fn test_local_inclusion_path_errors() {
        let dir = tempdir().unwrap();
        let mut manager =
            SlabManager::new(dir.path().to_path_buf(), SlabConfig::default()).unwrap();

        manager.append_leaves(&[[1u8; 32], [2u8; 32]]).unwrap();

        // tree_size = 0
        let result = manager.get_local_inclusion_path(1, 0, 0);
        assert!(result.is_err());

        // local_index >= local_tree_size
        let result = manager.get_local_inclusion_path(1, 5, 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_cross_slab_path_generation() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 5,
                max_open_slabs: 5,
            },
        )
        .unwrap();

        let leaves: Vec<[u8; 32]> = (0..12).map(|i| [i as u8; 32]).collect();
        manager.append_leaves(&leaves).unwrap();

        let cross_path = manager.get_cross_slab_path(1, 12).unwrap();
        assert!(!cross_path.is_empty());
    }

    #[test]
    fn test_evict_lru_preserves_active() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 2,
                max_open_slabs: 2,
            },
        )
        .unwrap();

        // Create slab 1
        manager.append_leaves(&[[1u8; 32], [2u8; 32]]).unwrap();
        assert_eq!(manager.active_slab, Some(1));

        // Create slab 2 (now active)
        manager.append_leaves(&[[3u8; 32], [4u8; 32]]).unwrap();
        assert_eq!(manager.active_slab, Some(2));

        // Create slab 3 - should evict slab 1 (oldest non-active)
        manager.append_leaves(&[[5u8; 32], [6u8; 32]]).unwrap();
        let active_id = manager.active_slab.unwrap();

        // Active slab should not be evicted
        assert_eq!(active_id, 3);
        assert!(manager.slabs.contains_key(&active_id));
        assert!(manager.slabs.len() <= manager.config.max_open_slabs);
    }

    #[test]
    fn test_get_or_open_slab_reuses_existing() {
        let dir = tempdir().unwrap();
        let mut manager =
            SlabManager::new(dir.path().to_path_buf(), SlabConfig::default()).unwrap();

        manager.append_leaves(&[[1u8; 32]]).unwrap();

        let initial_count = manager.slabs.len();

        // Access same slab again
        let _slab = manager.get_or_open_slab(1).unwrap();

        assert_eq!(manager.slabs.len(), initial_count);
    }

    #[test]
    fn test_multi_slab_boundary_cases() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 10,
                max_open_slabs: 5,
            },
        )
        .unwrap();

        // Exactly one slab
        manager
            .append_leaves(&(0..10).map(|i| [i; 32]).collect::<Vec<_>>())
            .unwrap();
        let root1 = manager.get_root(10).unwrap();
        assert_ne!(root1, [0u8; 32]);

        // One leaf into second slab
        manager.append_leaves(&[[10u8; 32]]).unwrap();
        let root2 = manager.get_root(11).unwrap();
        assert_ne!(root2, [0u8; 32]);
        assert_ne!(root1, root2);
    }

    #[test]
    fn test_inclusion_proof_at_slab_boundary() {
        let dir = tempdir().unwrap();
        let mut manager = SlabManager::new(
            dir.path().to_path_buf(),
            SlabConfig {
                max_leaves: 10,
                max_open_slabs: 5,
            },
        )
        .unwrap();

        let leaves: Vec<[u8; 32]> = (0..15).map(|i| [i as u8; 32]).collect();
        manager.append_leaves(&leaves).unwrap();

        // Last leaf of first slab
        let path1 = manager.get_inclusion_path(9, 15).unwrap();
        assert!(!path1.is_empty());

        // First leaf of second slab
        let path2 = manager.get_inclusion_path(10, 15).unwrap();
        assert!(!path2.is_empty());
    }
}
