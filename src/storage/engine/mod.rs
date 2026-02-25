// File: src/storage/engine.rs

#![allow(dead_code)]

//! Storage engine coordinator
//!
//! Orchestrates WAL, Slab, and SQLite components to implement the Storage trait.
//! This is the ONLY concrete implementation - no fallback storage.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::error::StorageError;
use crate::storage::config::StorageConfig;
use crate::storage::index::lifecycle::TreeRotationResult;
use crate::storage::index::{BatchInsert, IndexStore};
use crate::storage::recovery;
use crate::storage::slab::{PinnedSuperTreeSlab, SlabConfig, SlabManager};
use crate::traits::{
    AppendParams, BatchResult, ConsistencyProof, Entry, EntryResult, InclusionProof, ProofProvider,
    Storage, TreeHead, TreeRotator,
};

/// Cached tree state
struct TreeState {
    /// Current tree size
    tree_size: u64,

    /// Current root hash
    root_hash: [u8; 32],

    /// Origin ID (hash of log's public key)
    origin: [u8; 32],
}

/// Storage engine (THE storage implementation)
///
/// Coordinates Slab and SQLite for high-throughput append operations.
pub struct StorageEngine {
    /// Configuration
    config: StorageConfig,

    /// Slab manager (Data Tree slabs)
    slabs: Arc<RwLock<SlabManager>>,

    /// Super-Tree pinned slab
    super_slab: Arc<RwLock<PinnedSuperTreeSlab>>,

    /// SQLite index (Mutex because Connection is not Sync)
    index: Arc<Mutex<IndexStore>>,

    /// Cached tree state (sync RwLock for fast access from sync trait methods)
    tree_state: StdRwLock<TreeState>,

    /// Cached ID of the currently active tree
    /// Updated only during tree rotation
    /// Eliminates per-request SQL query in append_batch()
    active_tree_id: AtomicI64,

    /// Health status
    healthy: AtomicBool,
}

impl StorageEngine {
    /// Create new engine with configuration
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if:
    /// - Directory creation fails
    /// - Component initialization fails
    /// - Recovery fails
    pub async fn new(config: StorageConfig, origin: [u8; 32]) -> Result<Self, StorageError> {
        // Create directories if needed
        std::fs::create_dir_all(config.slab_dir())?;
        std::fs::create_dir_all(&config.data_dir)?;

        // Create Super-Tree slab directory
        let super_slab_dir = config.data_dir.join("super_tree");
        std::fs::create_dir_all(&super_slab_dir)?;

        // Initialize components
        let mut slabs = SlabManager::new(
            config.slab_dir(),
            SlabConfig {
                max_leaves: config.slab_capacity,
                max_open_slabs: config.max_open_slabs,
            },
        )?;

        // Initialize Super-Tree PinnedSuperTreeSlab
        let super_slab_path = super_slab_dir.join("super_tree.slab");
        let mut super_slab =
            PinnedSuperTreeSlab::open_or_create(&super_slab_path, config.slab_capacity)?;

        tracing::debug!(
            "init: Super-Tree size from slab file: {}",
            super_slab.leaf_count()
        );

        let mut index = IndexStore::open(&config.db_path())?;
        index.initialize()?;

        // Run crash recovery
        recovery::recover(&mut slabs, &mut index, &mut super_slab).await?;

        // Get tree size from index
        let tree_size = index.get_tree_size()?;

        // Initialize in-memory leaf cache for active slab
        // This enables O(1) root computation instead of O(N) disk reads
        slabs.initialize_leaf_cache(tree_size)?;

        // Get root hash from slab (or compute if tree is empty)
        let root_hash = if tree_size > 0 {
            slabs.get_root(tree_size)?
        } else {
            [0u8; 32]
        };

        // After recovery, load active tree ID from SQLite
        // If no active tree exists yet (fresh database or first run), create one
        // This is normally done by tree_closer, but we need it for initialization
        let active_tree_id = match index.get_active_tree()? {
            Some(tree) => tree.id,
            None => {
                // No active tree exists - create one (first run or recovery)
                tracing::info!("No active tree found, creating initial tree");
                index.create_active_tree(&origin, tree_size)?
            }
        };

        let tree_state = TreeState {
            tree_size,
            root_hash,
            origin,
        };

        Ok(Self {
            config,
            slabs: Arc::new(RwLock::new(slabs)),
            super_slab: Arc::new(RwLock::new(super_slab)),
            index: Arc::new(Mutex::new(index)),
            tree_state: StdRwLock::new(tree_state),
            active_tree_id: AtomicI64::new(active_tree_id),
            healthy: AtomicBool::new(true),
        })
    }

    /// Get shared reference to IndexStore for internal use
    ///
    /// This is for background jobs that need direct database access
    /// for tree lifecycle and anchoring operations.
    ///
    /// NOT part of Storage trait — internal server mechanics only.
    pub fn index_store(&self) -> Arc<Mutex<IndexStore>> {
        Arc::clone(&self.index)
    }

    /// Get reference to Super-Tree PinnedSuperTreeSlab for proofs
    pub fn super_slab(&self) -> &Arc<RwLock<PinnedSuperTreeSlab>> {
        &self.super_slab
    }

    /// Rotate the tree: close active tree and create new one
    ///
    /// This method appends the closed tree root to the Super-Tree and creates
    /// a new active data tree WITHOUT genesis leaf (Super-Tree replaces genesis).
    ///
    /// # Flow
    /// 1. Compute data_tree_index from Super-Tree size
    /// 2. Append closed tree root to Super-Tree
    /// 3. Update Super-Tree metadata (size, genesis root on first append)
    /// 4. Close active tree and create new one (IndexStore)
    /// 5. Update tree_state cache
    ///
    /// # Arguments
    /// * `origin_id` - Origin ID of the log
    /// * `end_size` - Final size of the tree being closed
    /// * `root_hash` - Root hash of the tree being closed
    ///
    /// # Returns
    /// `TreeRotationResult` with closed tree metadata, new tree head, and Super-Tree info
    ///
    /// # Errors
    /// Returns `StorageError` if any step fails
    pub async fn rotate_tree(
        &self,
        origin_id: &[u8; 32],
        end_size: u64,
        root_hash: &[u8; 32],
    ) -> Result<TreeRotationResult, StorageError> {
        // 1. Append closed tree root to Super-Tree FIRST to get data_tree_index
        let (super_root, data_tree_index) = {
            let mut super_slab = self.super_slab.write().await;

            // Get current Super-Tree size (this becomes data_tree_index)
            // SOURCE OF TRUTH: SlabFile header, not SQLite
            let data_tree_index = super_slab.leaf_count();

            // Append to Super-Tree (atomic: write + flush)
            let super_root = super_slab.append_leaf(root_hash)?;

            (super_root, data_tree_index)
        };

        // Update SQLite for audit (after successful append)
        {
            let index = self.index.lock().await;

            // Best-effort audit write - failure doesn't break consistency
            if let Err(e) = index.set_super_tree_size(data_tree_index + 1) {
                tracing::warn!("Failed to update SQLite super_tree_size (audit): {}", e);
            }

            // Set genesis on first append
            if data_tree_index == 0 {
                if let Err(e) = index.set_super_genesis_root(&super_root) {
                    tracing::warn!("Failed to update SQLite super_genesis_root (audit): {}", e);
                }
            }
        };

        // 2. Close active tree and create new one
        let close_result = {
            let mut index = self.index.lock().await;
            index
                .close_tree_and_create_new(origin_id, end_size, root_hash, data_tree_index)
                .map_err(|e| StorageError::Database(e.to_string()))?
        };

        // After successful rotation, update cached active_tree_id
        self.active_tree_id
            .store(close_result.new_tree_id, Ordering::Relaxed);

        // 3. Update tree_state cache (SIMPLIFIED - no genesis)
        let new_tree_size = end_size; // NO +1 for genesis!
        let new_root_hash = if new_tree_size > 0 {
            let mut slabs = self.slabs.write().await;
            slabs.get_root(new_tree_size)?
        } else {
            [0u8; 32]
        };

        {
            let mut state = self
                .tree_state
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.tree_size = new_tree_size;
            state.root_hash = new_root_hash;
        }

        // 4. Return result
        let new_tree_head = TreeHead {
            tree_size: new_tree_size,
            root_hash: new_root_hash,
            origin: *origin_id,
        };

        Ok(TreeRotationResult {
            closed_tree_id: close_result.closed_tree_id,
            new_tree_id: close_result.new_tree_id,
            data_tree_index,
            super_root,
            closed_tree_metadata: close_result.closed_tree_metadata,
            new_tree_head,
        })
    }
}

#[async_trait::async_trait]
impl Storage for StorageEngine {
    async fn append_batch(&self, params: Vec<AppendParams>) -> Result<BatchResult, StorageError> {
        if params.is_empty() {
            return Ok(BatchResult {
                entries: vec![],
                tree_head: self.tree_head(),
                committed_at: chrono::Utc::now(),
            });
        }

        // 1. Prepare entries
        let tree_size = self
            .tree_state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .tree_size;
        let entries: Vec<_> = params
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let id = Uuid::new_v4();
                let leaf_index = tree_size + i as u64;
                let leaf_hash = atl_core::compute_leaf_hash(&p.payload_hash, &p.metadata_hash);
                (id, leaf_index, leaf_hash, p)
            })
            .collect();

        // 2. Update Slab (mmap, no flush)
        let leaf_hashes: Vec<[u8; 32]> = entries.iter().map(|(_, _, h, _)| *h).collect();
        let new_root = {
            let mut slabs = self.slabs.write().await;
            slabs.append_leaves(&leaf_hashes)?
            // NOTE: No flush needed - proof generation uses active_slab_leaves cache
            // for active slab, which is always up-to-date after append_leaves()
        };

        // 3. Update SQLite (single fsync via SQLite WAL)
        // Use cached active tree ID (no SQL query)
        let active_tree_id = self.active_tree_id.load(Ordering::Relaxed);

        let batch_inserts: Vec<BatchInsert> = entries
            .iter()
            .map(|(id, leaf_index, _, p)| {
                let slab_id = (*leaf_index / u64::from(self.config.slab_capacity)) as u32 + 1;
                let slab_offset = (*leaf_index % u64::from(self.config.slab_capacity)) * 32;

                BatchInsert {
                    id: *id,
                    leaf_index: *leaf_index,
                    slab_id,
                    slab_offset,
                    payload_hash: p.payload_hash,
                    metadata_hash: p.metadata_hash,
                    metadata_cleartext: p.metadata_cleartext.as_ref().map(|v| v.to_string()),
                    external_id: p.external_id.clone(),
                    tree_id: active_tree_id,
                }
            })
            .collect();

        {
            let mut index = self.index.lock().await;
            index.insert_batch(&batch_inserts)?;
            let new_tree_size = tree_size + params.len() as u64;
            index.set_tree_size(new_tree_size)?;
            index.update_first_entry_at_for_active_tree()?;
        }

        // 4. Update cached state
        {
            let mut state = self
                .tree_state
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.tree_size = tree_size + params.len() as u64;
            state.root_hash = new_root;
        }

        // 5. Build result
        let entry_results: Vec<EntryResult> = entries
            .iter()
            .map(|(id, leaf_index, leaf_hash, _)| EntryResult {
                id: *id,
                leaf_index: *leaf_index,
                leaf_hash: *leaf_hash,
            })
            .collect();

        Ok(BatchResult {
            entries: entry_results,
            tree_head: self.tree_head(),
            committed_at: chrono::Utc::now(),
        })
    }

    async fn flush(&self) -> Result<(), StorageError> {
        let mut slabs = self.slabs.write().await;
        slabs.flush()?;
        Ok(())
    }

    fn tree_head(&self) -> TreeHead {
        let state = self
            .tree_state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        TreeHead {
            tree_size: state.tree_size,
            root_hash: state.root_hash,
            origin: state.origin,
        }
    }

    fn origin_id(&self) -> [u8; 32] {
        self.tree_state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .origin
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    fn get_entry(&self, id: &Uuid) -> crate::error::ServerResult<Entry> {
        // Delegate to ProofProvider async method via blocking
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { ProofProvider::get_entry(self, id).await.map_err(Into::into) })
        })
    }

    fn get_inclusion_proof(
        &self,
        entry_id: &Uuid,
        tree_size: Option<u64>,
    ) -> crate::error::ServerResult<InclusionProof> {
        // First get the entry to find its leaf_index
        let entry = Storage::get_entry(self, entry_id)?;
        let leaf_index = entry
            .leaf_index
            .ok_or_else(|| crate::error::ServerError::EntryNotInTree(entry_id.to_string()))?;

        // Delegate to ProofProvider async method via blocking
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                ProofProvider::get_inclusion_proof(self, leaf_index, tree_size)
                    .await
                    .map_err(Into::into)
            })
        })
    }

    fn get_inclusion_proof_by_leaf_index(
        &self,
        leaf_index: u64,
        tree_size: Option<u64>,
    ) -> crate::error::ServerResult<InclusionProof> {
        // Direct call to ProofProvider - no SQLite query needed
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                ProofProvider::get_inclusion_proof(self, leaf_index, tree_size)
                    .await
                    .map_err(Into::into)
            })
        })
    }

    fn get_consistency_proof(
        &self,
        from_size: u64,
        to_size: u64,
    ) -> crate::error::ServerResult<ConsistencyProof> {
        // Delegate to ProofProvider async method via blocking
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                ProofProvider::get_consistency_proof(self, from_size, to_size)
                    .await
                    .map_err(Into::into)
            })
        })
    }

    fn get_anchors(
        &self,
        tree_size: u64,
    ) -> crate::error::ServerResult<Vec<crate::traits::anchor::Anchor>> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let index = self.index.lock().await;
                index.get_anchors(tree_size).map_err(|e| {
                    crate::error::ServerError::Storage(StorageError::Database(e.to_string()))
                })
            })
        })
    }

    fn get_latest_anchored_size(&self) -> crate::error::ServerResult<Option<u64>> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let index = self.index.lock().await;
                index.get_latest_anchored_size().map_err(|e| {
                    crate::error::ServerError::Storage(StorageError::Database(e.to_string()))
                })
            })
        })
    }

    fn get_tsa_anchor_covering(
        &self,
        tree_size: u64,
    ) -> crate::error::ServerResult<Option<crate::traits::anchor::Anchor>> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let index = self.index.lock().await;
                index.get_tsa_anchor_covering(tree_size).map_err(|e| {
                    crate::error::ServerError::Storage(StorageError::Database(e.to_string()))
                })
            })
        })
    }

    fn get_ots_anchor_covering(
        &self,
        data_tree_index: u64,
    ) -> crate::error::ServerResult<Option<crate::traits::anchor::Anchor>> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let index = self.index.lock().await;
                index
                    .get_ots_anchor_covering(data_tree_index)
                    .map_err(|e| {
                        crate::error::ServerError::Storage(StorageError::Database(e.to_string()))
                    })
            })
        })
    }

    fn get_root_at_size(&self, tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut slabs = self.slabs.write().await;
                slabs.get_root(tree_size).map_err(|e| {
                    crate::error::ServerError::Storage(crate::error::StorageError::Io(e))
                })
            })
        })
    }

    fn get_super_root(&self, super_tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                if super_tree_size == 0 {
                    return Ok([0u8; 32]);
                }
                let super_slab = self.super_slab.read().await;
                super_slab.get_root(super_tree_size).map_err(|e| {
                    crate::error::ServerError::Storage(crate::error::StorageError::Io(e))
                })
            })
        })
    }

    fn is_initialized(&self) -> bool {
        // Storage is considered initialized if tree_state has been set
        true
    }
}

#[async_trait::async_trait]
impl TreeRotator for StorageEngine {
    async fn rotate_tree(
        &self,
        origin_id: &[u8; 32],
        end_size: u64,
        root_hash: &[u8; 32],
    ) -> Result<TreeRotationResult, StorageError> {
        // Delegate to the concrete implementation
        // This allows TreeRotator trait to be used polymorphically while
        // keeping the actual implementation in the StorageEngine method
        StorageEngine::rotate_tree(self, origin_id, end_size, root_hash).await
    }
}

#[async_trait::async_trait]
impl ProofProvider for StorageEngine {
    async fn get_entry(&self, id: &Uuid) -> Result<Entry, StorageError> {
        let index = self.index.lock().await;
        index
            .get_entry(id)?
            .map(Into::into)
            .ok_or(StorageError::NotFound("entry not found".into()))
    }

    async fn get_entry_by_index(&self, index: u64) -> Result<Entry, StorageError> {
        let idx = self.index.lock().await;
        idx.get_entry_by_index(index)?
            .map(Into::into)
            .ok_or(StorageError::NotFound("entry not found".into()))
    }

    async fn get_entry_by_external_id(&self, external_id: &str) -> Result<Entry, StorageError> {
        let index = self.index.lock().await;
        index
            .get_entry_by_external_id(external_id)?
            .map(Into::into)
            .ok_or(StorageError::NotFound("entry not found".into()))
    }

    async fn get_inclusion_proof(
        &self,
        leaf_index: u64,
        tree_size: Option<u64>,
    ) -> Result<InclusionProof, StorageError> {
        let tree_size = tree_size.unwrap_or_else(|| self.tree_head().tree_size);

        if leaf_index >= tree_size {
            return Err(StorageError::InvalidIndex("invalid leaf index".into()));
        }

        // Fast path: try READ lock for active slab (no contention)
        {
            let slabs = self.slabs.read().await;
            if let Some(path) = slabs.get_inclusion_path_readonly(leaf_index, tree_size) {
                return Ok(InclusionProof {
                    leaf_index,
                    tree_size,
                    path,
                });
            }
        }
        // READ lock released here

        // Slow path: WRITE lock for closed slabs or cache miss
        let path = {
            let mut slabs = self.slabs.write().await;
            slabs.get_inclusion_path(leaf_index, tree_size)?
        };

        Ok(InclusionProof {
            leaf_index,
            tree_size,
            path,
        })
    }

    async fn get_consistency_proof(
        &self,
        from_size: u64,
        to_size: u64,
    ) -> Result<ConsistencyProof, StorageError> {
        let current_tree_size = self.tree_head().tree_size;

        if from_size > to_size || to_size > current_tree_size {
            return Err(StorageError::InvalidRange("invalid range".into()));
        }

        // Compute consistency proof using atl-core algorithm
        // Use RefCell for interior mutability in Fn closure
        let proof = {
            let mut slabs = self.slabs.write().await;
            let slabs_cell = std::cell::RefCell::new(&mut *slabs);

            atl_core::generate_consistency_proof(from_size, to_size, |level, index| {
                slabs_cell
                    .borrow_mut()
                    .get_node(level, index)
                    .ok()
                    .flatten()
            })
            .map_err(|e| StorageError::Corruption(e.to_string()))?
        };

        Ok(ConsistencyProof {
            from_size,
            to_size,
            path: proof.path,
        })
    }
}

#[cfg(test)]
mod tests;
