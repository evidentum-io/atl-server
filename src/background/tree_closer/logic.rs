// File: src/background/tree_closer/logic.rs

use crate::error::ServerResult;
use crate::storage::chain_index::ChainIndex;
use crate::storage::index::IndexStore;
use crate::traits::{Storage, TreeRotator};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Check if active tree should be closed and close it if needed
///
/// This function contains the core tree closing logic:
/// 1. Get active tree (or create one if missing)
/// 2. Check if tree is old enough (based on first_entry_at, NOT created_at)
/// 3. Empty trees (first_entry_at = NULL) are NEVER closed
/// 4. Close tree and create new one with genesis leaf (via TreeRotator)
/// 5. Genesis leaf is inserted into BOTH Slab and SQLite
/// 6. Record closed tree in Chain Index
/// 7. OTS anchoring will be done by ots_job separately
pub async fn check_and_close_if_needed(
    index: &Arc<Mutex<IndexStore>>,
    storage: &Arc<dyn Storage>,
    rotator: &Arc<dyn TreeRotator>,
    chain_index: &Arc<Mutex<ChainIndex>>,
    tree_lifetime_secs: u64,
) -> ServerResult<()> {
    let tree_head = storage.tree_head();
    let origin_id = storage.origin_id();

    // Get active tree
    let active_tree = {
        let idx = index.lock().await;
        idx.get_active_tree().map_err(|e| {
            crate::error::ServerError::Storage(crate::error::StorageError::Database(e.to_string()))
        })?
    };

    let active_tree = match active_tree {
        Some(tree) => tree,
        None => {
            // No active tree - create one (first run or recovery)
            let tree_id = {
                let idx = index.lock().await;
                idx.create_active_tree(&origin_id, tree_head.tree_size)
                    .map_err(|e| {
                        crate::error::ServerError::Storage(crate::error::StorageError::Database(
                            e.to_string(),
                        ))
                    })?
            };
            tracing::info!(
                tree_id = tree_id,
                start_size = tree_head.tree_size,
                "Created initial active tree"
            );
            return Ok(());
        }
    };

    // Edge case 1: Empty tree (first_entry_at = NULL) - timer not started, wait for first entry
    let first_entry_at = match active_tree.first_entry_at {
        Some(ts) => ts,
        None => {
            tracing::debug!(
                tree_id = active_tree.id,
                "Tree has no entries yet (first_entry_at = NULL), timer not started"
            );
            return Ok(());
        }
    };

    // Check if tree has lived long enough since FIRST ENTRY (not creation)
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let tree_age_secs = (now - first_entry_at) / 1_000_000_000;

    if tree_age_secs < tree_lifetime_secs as i64 {
        tracing::debug!(
            tree_id = active_tree.id,
            age_secs = tree_age_secs,
            lifetime_secs = tree_lifetime_secs,
            "Tree not old enough to close (timer started from first entry)"
        );
        return Ok(());
    }

    // Double-check: tree has entries (should always be true if first_entry_at is set)
    if tree_head.tree_size <= active_tree.start_size {
        tracing::warn!(
            tree_id = active_tree.id,
            start_size = active_tree.start_size,
            current_size = tree_head.tree_size,
            "Tree has first_entry_at but no entries in tree - inconsistent state"
        );
        return Ok(());
    }

    tracing::info!(
        tree_id = active_tree.id,
        start_size = active_tree.start_size,
        end_size = tree_head.tree_size,
        age_secs = tree_age_secs,
        "Closing tree (timer started from first entry), OTS anchoring will be done by ots_job"
    );

    // Rotate tree: close current tree and create new one with genesis leaf
    // This uses TreeRotator trait which ensures genesis leaf is inserted into BOTH Slab and SQLite
    let result = rotator
        .rotate_tree(&origin_id, tree_head.tree_size, &tree_head.root_hash)
        .await
        .map_err(crate::error::ServerError::Storage)?;

    tracing::info!(
        closed_tree_id = result.closed_tree_id,
        new_tree_id = result.new_tree_id,
        data_tree_index = result.data_tree_index,
        super_root = %hex::encode(result.super_root),
        new_tree_size = result.new_tree_head.tree_size,
        "Tree rotated with Super-Tree append, pending OTS anchoring by ots_job"
    );

    {
        let ci = chain_index.lock().await;
        ci.record_closed_tree(&result.closed_tree_metadata)
            .map_err(|e| {
                crate::error::ServerError::Storage(crate::error::StorageError::Database(
                    e.to_string(),
                ))
            })?;
    }

    tracing::info!(
        tree_id = result.closed_tree_metadata.tree_id,
        data_tree_index = result.data_tree_index,
        "Recorded closed tree in Chain Index"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StorageError;
    use crate::storage::chain_index::ChainIndex;
    use crate::storage::index::lifecycle::{ClosedTreeMetadata, TreeRotationResult};
    use crate::storage::index::queries::IndexStore;
    use crate::traits::{InclusionProof, Storage, TreeHead, TreeRotator};
    use std::path::Path;
    use tempfile::tempdir;

    /// Mock storage for testing
    struct MockStorage {
        tree_head: TreeHead,
        origin_id: [u8; 32],
    }

    impl MockStorage {
        fn new(tree_size: u64, root_hash: [u8; 32], origin_id: [u8; 32]) -> Self {
            Self {
                tree_head: TreeHead {
                    tree_size,
                    root_hash,
                    origin: origin_id,
                },
                origin_id,
            }
        }
    }

    #[async_trait::async_trait]
    impl Storage for MockStorage {
        fn tree_head(&self) -> TreeHead {
            self.tree_head.clone()
        }

        fn origin_id(&self) -> [u8; 32] {
            self.origin_id
        }

        fn is_healthy(&self) -> bool {
            true
        }

        fn is_initialized(&self) -> bool {
            true
        }

        async fn append_batch(
            &self,
            _params: Vec<crate::traits::AppendParams>,
        ) -> Result<crate::traits::BatchResult, StorageError> {
            unimplemented!("Not needed for tree closer logic tests")
        }

        async fn flush(&self) -> Result<(), StorageError> {
            unimplemented!("Not needed for tree closer logic tests")
        }

        fn get_entry(&self, _id: &uuid::Uuid) -> crate::error::ServerResult<crate::traits::Entry> {
            unimplemented!("Not needed for tree closer logic tests")
        }

        fn get_inclusion_proof(
            &self,
            _entry_id: &uuid::Uuid,
            _tree_size: Option<u64>,
        ) -> crate::error::ServerResult<crate::traits::InclusionProof> {
            unimplemented!("Not needed for tree closer logic tests")
        }

        fn get_inclusion_proof_by_leaf_index(
            &self,
            _leaf_index: u64,
            _tree_size: Option<u64>,
        ) -> crate::error::ServerResult<InclusionProof> {
            unimplemented!()
        }

        fn get_consistency_proof(
            &self,
            _from_size: u64,
            _to_size: u64,
        ) -> crate::error::ServerResult<crate::traits::ConsistencyProof> {
            unimplemented!("Not needed for tree closer logic tests")
        }

        fn get_anchors(
            &self,
            _tree_size: u64,
        ) -> crate::error::ServerResult<Vec<crate::traits::Anchor>> {
            unimplemented!("Not needed for tree closer logic tests")
        }

        fn get_latest_anchored_size(&self) -> crate::error::ServerResult<Option<u64>> {
            unimplemented!("Not needed for tree closer logic tests")
        }

        fn get_anchors_covering(
            &self,
            _target_tree_size: u64,
            _limit: usize,
        ) -> crate::error::ServerResult<Vec<crate::traits::Anchor>> {
            unimplemented!("Not needed for tree closer logic tests")
        }

        fn get_root_at_size(&self, _tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
            unimplemented!("Not needed for tree closer logic tests")
        }

        fn get_super_root(&self, _super_tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
            unimplemented!("Not needed for tree closer logic tests")
        }
    }

    /// Mock tree rotator for testing
    struct MockTreeRotator {
        should_fail: bool,
        rotation_count: Arc<Mutex<usize>>,
    }

    impl MockTreeRotator {
        fn new() -> Self {
            Self {
                should_fail: false,
                rotation_count: Arc::new(Mutex::new(0)),
            }
        }

        fn with_failure() -> Self {
            Self {
                should_fail: true,
                rotation_count: Arc::new(Mutex::new(0)),
            }
        }

        async fn rotation_count(&self) -> usize {
            *self.rotation_count.lock().await
        }
    }

    #[async_trait::async_trait]
    impl TreeRotator for MockTreeRotator {
        async fn rotate_tree(
            &self,
            origin_id: &[u8; 32],
            end_size: u64,
            root_hash: &[u8; 32],
        ) -> Result<TreeRotationResult, StorageError> {
            *self.rotation_count.lock().await += 1;

            if self.should_fail {
                return Err(StorageError::Database("mock rotation failure".into()));
            }

            Ok(TreeRotationResult {
                closed_tree_id: 1,
                new_tree_id: 2,
                data_tree_index: 0,
                super_root: [42u8; 32],
                closed_tree_metadata: ClosedTreeMetadata {
                    tree_id: 1,
                    origin_id: *origin_id,
                    root_hash: *root_hash,
                    tree_size: end_size,
                    closed_at: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                    data_tree_index: 0,
                },
                new_tree_head: TreeHead {
                    tree_size: end_size,
                    root_hash: [1u8; 32],
                    origin: *origin_id,
                },
            })
        }
    }

    fn setup_index_with_active_tree(path: &Path, origin_id: &[u8; 32]) -> IndexStore {
        let db_path = path.join("atl.db");
        let index = IndexStore::open(&db_path).unwrap();
        index.initialize().unwrap();
        index.create_active_tree(origin_id, 0).unwrap();
        index
    }

    #[tokio::test]
    async fn test_no_active_tree_creates_new_one() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("atl.db");
        let index = IndexStore::open(&db_path).unwrap();
        index.initialize().unwrap();
        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        let origin_id = [1u8; 32];
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_ok());

        // Verify tree was created
        let idx = IndexStore::open(&db_path).unwrap();
        let active = idx.get_active_tree().unwrap();
        assert!(active.is_some());
    }

    #[tokio::test]
    async fn test_empty_tree_not_closed() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);
        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [0u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_ok());

        // Verify no rotation happened - tree should still be active
        let idx = IndexStore::open(&temp_dir.path().join("atl.db")).unwrap();
        let active = idx.get_active_tree().unwrap();
        assert!(active.is_some());
        let active_tree = active.unwrap();
        assert_eq!(active_tree.start_size, 0);
    }

    #[tokio::test]
    async fn test_young_tree_not_closed() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        // Set first_entry_at to NOW (tree just started)
        {
            let conn = index.connection();
            let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![now],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600, // Tree lifetime: 1 hour
        )
        .await;

        assert!(result.is_ok());

        // Verify no rotation happened - tree should still be active and young
        let idx = IndexStore::open(&temp_dir.path().join("atl.db")).unwrap();
        let active = idx.get_active_tree().unwrap();
        assert!(active.is_some());
    }

    #[tokio::test]
    async fn test_old_tree_gets_closed() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        // Set first_entry_at to 2 hours ago
        {
            let conn = index.connection();
            let two_hours_ago =
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - (2 * 3600 * 1_000_000_000);
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![two_hours_ago],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600, // Tree lifetime: 1 hour
        )
        .await;

        assert!(result.is_ok());

        // Verify rotation happened - new tree should have been created
        // We can't directly check if rotation happened, but the function should succeed
        // In real scenario, this would be visible in chain index
    }

    #[tokio::test]
    async fn test_tree_with_first_entry_but_no_size_increase_not_closed() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        // Set first_entry_at to 2 hours ago but tree size hasn't increased
        {
            let conn = index.connection();
            let two_hours_ago =
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - (2 * 3600 * 1_000_000_000);
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![two_hours_ago],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        // Tree size is same as start_size (0)
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_ok());

        // Verify no rotation happened due to inconsistent state
        // Tree still exists but hasn't grown
    }

    #[tokio::test]
    async fn test_rotation_failure_propagates_error() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        // Set first_entry_at to 2 hours ago
        {
            let conn = index.connection();
            let two_hours_ago =
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - (2 * 3600 * 1_000_000_000);
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![two_hours_ago],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::with_failure());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_err());
        match result {
            Err(crate::error::ServerError::Storage(StorageError::Database(msg))) => {
                assert_eq!(msg, "mock rotation failure");
            }
            _ => panic!("Expected StorageError::Database"),
        }
    }

    #[tokio::test]
    async fn test_tree_exactly_at_lifetime_threshold() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        let tree_lifetime = 3600u64;

        // Set first_entry_at to exactly tree_lifetime seconds ago
        {
            let conn = index.connection();
            let exactly_lifetime_ago = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                - (tree_lifetime as i64 * 1_000_000_000);
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![exactly_lifetime_ago],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            tree_lifetime,
        )
        .await;

        assert!(result.is_ok());
        // Tree should be closed when age >= lifetime
    }

    #[tokio::test]
    async fn test_tree_just_under_lifetime_threshold() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        let tree_lifetime = 3600u64;

        // Set first_entry_at to just 1 second less than threshold
        {
            let conn = index.connection();
            let just_under = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                - ((tree_lifetime - 1) as i64 * 1_000_000_000);
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![just_under],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            tree_lifetime,
        )
        .await;

        assert!(result.is_ok());

        // Tree should NOT be closed (just under threshold)
        let idx = IndexStore::open(&temp_dir.path().join("atl.db")).unwrap();
        let active = idx.get_active_tree().unwrap();
        assert!(active.is_some());
    }

    #[tokio::test]
    async fn test_different_origin_ids() {
        let temp_dir = tempdir().unwrap();
        let origin_id_1 = [1u8; 32];
        let origin_id_2 = [2u8; 32];

        let db_path = temp_dir.path().join("atl.db");
        let index = IndexStore::open(&db_path).unwrap();
        index.initialize().unwrap();

        // Create tree for origin_1
        index.create_active_tree(&origin_id_1, 0).unwrap();

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();

        // Process origin_1 - should work fine
        let storage1: Arc<dyn Storage> = Arc::new(MockStorage::new(50, [11u8; 32], origin_id_1));
        let rotator1 = MockTreeRotator::new();
        let rotator1_arc: Arc<dyn TreeRotator> = Arc::new(rotator1);

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage1,
            &rotator1_arc,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_ok());

        // Now test with origin_2 - should create new tree
        let db_path = temp_dir.path().join("atl2.db");
        let index2 = IndexStore::open(&db_path).unwrap();
        index2.initialize().unwrap();
        index2.create_active_tree(&origin_id_2, 100).unwrap();

        let chain_db_path2 = temp_dir.path().join("chain2.db");
        let chain_index2 = ChainIndex::open(&chain_db_path2).unwrap();

        let storage2: Arc<dyn Storage> = Arc::new(MockStorage::new(150, [22u8; 32], origin_id_2));
        let rotator2 = MockTreeRotator::new();
        let rotator2_arc: Arc<dyn TreeRotator> = Arc::new(rotator2);

        let result2 = check_and_close_if_needed(
            &Arc::new(Mutex::new(index2)),
            &storage2,
            &rotator2_arc,
            &Arc::new(Mutex::new(chain_index2)),
            3600,
        )
        .await;

        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_zero_lifetime_closes_immediately_if_old() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        // Set first_entry_at to 1 second ago
        {
            let conn = index.connection();
            let one_sec_ago = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - 1_000_000_000;
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![one_sec_ago],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            0, // Zero lifetime
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rotation_called_once() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        // Set first_entry_at to 2 hours ago
        {
            let conn = index.connection();
            let two_hours_ago =
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - (2 * 3600 * 1_000_000_000);
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![two_hours_ago],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator_impl = Arc::new(MockTreeRotator::new());
        let rotator: Arc<dyn TreeRotator> = rotator_impl.clone();

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_ok());

        // Verify rotation was called exactly once
        assert_eq!(rotator_impl.rotation_count().await, 1);
    }

    #[tokio::test]
    async fn test_chain_index_record_created() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        // Set first_entry_at to 2 hours ago
        {
            let conn = index.connection();
            let two_hours_ago =
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - (2 * 3600 * 1_000_000_000);
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![two_hours_ago],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = Arc::new(Mutex::new(ChainIndex::open(&chain_db_path).unwrap()));
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &chain_index,
            3600,
        )
        .await;

        assert!(result.is_ok());

        // Verify chain index has a record
        let ci = chain_index.lock().await;
        let tree = ci.get_tree(1).unwrap();
        assert!(tree.is_some());
    }

    #[tokio::test]
    async fn test_large_tree_size_values() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let db_path = temp_dir.path().join("atl.db");
        let index = IndexStore::open(&db_path).unwrap();
        index.initialize().unwrap();

        let large_size = u64::MAX / 2;
        index.create_active_tree(&origin_id, large_size).unwrap();

        // Set first_entry_at to 2 hours ago
        {
            let conn = index.connection();
            let two_hours_ago =
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - (2 * 3600 * 1_000_000_000);
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![two_hours_ago],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> =
            Arc::new(MockStorage::new(large_size + 1000, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_timestamp_arithmetic_edge_cases() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        // Set first_entry_at to very old timestamp (near i64 MIN)
        {
            let conn = index.connection();
            let very_old = 1_000_000_000i64; // Year 1970 + small offset
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![very_old],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_active_tree_database_error() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("atl.db");
        let origin_id = [1u8; 32];

        let index = IndexStore::open(&db_path).unwrap();
        index.initialize().unwrap();

        // Drop the trees table to cause database error
        {
            let conn = index.connection();
            conn.execute("DROP TABLE trees", []).unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(crate::error::ServerError::Storage(_))));
    }

    #[tokio::test]
    async fn test_create_active_tree_database_error() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("atl.db");
        let origin_id = [1u8; 32];

        let index = IndexStore::open(&db_path).unwrap();
        index.initialize().unwrap();

        // Drop the trees table to cause database error
        {
            let conn = index.connection();
            conn.execute("DROP TABLE trees", []).unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(crate::error::ServerError::Storage(_))));
    }

    #[tokio::test]
    async fn test_storage_healthy_check() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        // Verify storage is healthy
        assert!(storage.is_healthy());
        assert!(storage.is_initialized());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_tree_with_negative_age() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        // Set first_entry_at to future timestamp (shouldn't happen in practice)
        {
            let conn = index.connection();
            let future =
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) + (3600 * 1_000_000_000);
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![future],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_ok());
        // Tree should not be closed (negative age)
    }

    #[tokio::test]
    async fn test_very_large_lifetime() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        // Set first_entry_at to 1 second ago
        {
            let conn = index.connection();
            let one_sec_ago = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - 1_000_000_000;
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![one_sec_ago],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(100, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            u64::MAX, // Very large lifetime
        )
        .await;

        assert!(result.is_ok());
        // Tree should not be closed (lifetime too long)
    }

    #[tokio::test]
    async fn test_tree_size_exactly_at_start_size() {
        let temp_dir = tempdir().unwrap();
        let origin_id = [1u8; 32];
        let index = setup_index_with_active_tree(temp_dir.path(), &origin_id);

        // Set first_entry_at to 2 hours ago
        {
            let conn = index.connection();
            let two_hours_ago =
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - (2 * 3600 * 1_000_000_000);
            conn.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active'",
                rusqlite::params![two_hours_ago],
            )
            .unwrap();
        }

        let chain_db_path = temp_dir.path().join("chain.db");
        let chain_index = ChainIndex::open(&chain_db_path).unwrap();
        // Tree size equals start_size (no growth)
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new(0, [42u8; 32], origin_id));
        let rotator: Arc<dyn TreeRotator> = Arc::new(MockTreeRotator::new());

        let result = check_and_close_if_needed(
            &Arc::new(Mutex::new(index)),
            &storage,
            &rotator,
            &Arc::new(Mutex::new(chain_index)),
            3600,
        )
        .await;

        assert!(result.is_ok());
        // Should detect inconsistent state and not close
    }
}
