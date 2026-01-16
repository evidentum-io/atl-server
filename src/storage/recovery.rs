//! Storage engine crash recovery orchestration
//!
//! Coordinates WAL, Slab, and Index recovery after crashes.

use crate::error::StorageError;
use crate::storage::index::{BatchInsert, IndexStore};
use crate::storage::slab::{PinnedSuperTreeSlab, SlabManager};
use crate::storage::wal::{WalRecovery, WalWriter};
use uuid::Uuid;

/// Run crash recovery on storage components
///
/// This orchestrates the recovery process:
/// 1. Scan WAL directory for pending/committed batches
/// 2. Replay committed batches to Slab and Index
/// 3. Discard uncommitted batches
/// 4. Update WAL state with next batch ID
/// 5. Reconcile Super-Tree with closed trees
///
/// # Errors
///
/// Returns `StorageError` if recovery fails
pub async fn recover(
    wal: &mut WalWriter,
    slabs: &mut SlabManager,
    index: &mut IndexStore,
    super_slab: &mut PinnedSuperTreeSlab,
    slab_capacity: u64,
) -> Result<(), StorageError> {
    // Scan WAL directory for pending/committed batches
    let recovery = WalRecovery::new(wal.dir().to_path_buf());
    let result = recovery.scan()?;

    tracing::info!(
        "recovery: {} batches to replay, {} discarded",
        result.replay_needed.len(),
        result.discarded.len()
    );

    // Get current tree size from SQLite to skip already-replayed batches
    let current_tree_size = index.get_tree_size()?;

    // Replay committed batches (only those not yet in SQLite)
    for batch in result.replay_needed {
        // Skip if this batch was already applied to SQLite
        let batch_end_size = batch.tree_size_before + batch.entries.len() as u64;
        if batch_end_size <= current_tree_size {
            tracing::debug!(
                "skipping batch {} (already in SQLite: {} >= {})",
                batch.batch_id,
                current_tree_size,
                batch_end_size
            );
            continue;
        }
        tracing::debug!("replaying batch {}", batch.batch_id);

        // Get active tree ID for recovery entries
        let active_tree_id = index
            .get_active_tree()?
            .ok_or_else(|| {
                crate::error::StorageError::QueryFailed("No active tree during recovery".into())
            })?
            .id;

        // Convert WAL entries to BatchInsert format
        let batch_inserts: Vec<BatchInsert> = batch
            .entries
            .iter()
            .enumerate()
            .map(|(i, entry)| {
                let leaf_index = batch.tree_size_before + i as u64;
                let id = Uuid::from_bytes(entry.id);
                let slab_id = (leaf_index / slab_capacity) as u32 + 1;
                let slab_offset = (leaf_index % slab_capacity) * 32;

                let metadata_cleartext = if entry.metadata.is_empty() {
                    None
                } else {
                    String::from_utf8(entry.metadata.clone()).ok()
                };

                let external_id = if entry.external_id.is_empty() {
                    None
                } else {
                    String::from_utf8(entry.external_id.clone()).ok()
                };

                BatchInsert {
                    id,
                    leaf_index,
                    slab_id,
                    slab_offset,
                    payload_hash: entry.payload_hash,
                    metadata_hash: entry.metadata_hash,
                    metadata_cleartext,
                    external_id,
                    tree_id: active_tree_id,
                }
            })
            .collect();

        // Compute leaf hashes and update slab
        let leaf_hashes: Vec<[u8; 32]> = batch
            .entries
            .iter()
            .map(|entry| atl_core::compute_leaf_hash(&entry.payload_hash, &entry.metadata_hash))
            .collect();

        slabs.append_leaves(&leaf_hashes)?;

        // Insert into SQLite index
        index.insert_batch(&batch_inserts)?;
        let new_tree_size = batch.tree_size_before + batch.entries.len() as u64;
        index.set_tree_size(new_tree_size)?;
    }

    // Update WAL state
    wal.set_next_batch_id(result.next_batch_id);

    // Reconcile Super-Tree with closed trees
    reconcile_super_tree(index, super_slab).await?;

    Ok(())
}

/// Reconcile Super-Tree with closed trees from SQLite
///
/// If server crashed between tree close and Super-Tree append,
/// some closed trees may not be in Super-Tree. This function
/// detects and fixes such inconsistencies.
///
/// # Errors
///
/// Returns `StorageError::Corruption` if Super-Tree has more leaves than closed trees
async fn reconcile_super_tree(
    index: &mut IndexStore,
    super_slab: &mut PinnedSuperTreeSlab,
) -> Result<(), StorageError> {
    // 1. Get closed trees from SQLite (ordered by close time)
    let closed_trees = index
        .get_closed_trees_ordered()
        .map_err(|e| StorageError::Database(e.to_string()))?;
    let closed_count = closed_trees.len() as u64;

    // 2. Get Super-Tree size from PinnedSuperTreeSlab (SOURCE OF TRUTH)
    let super_size = super_slab.leaf_count();

    tracing::info!(
        "recovery: Super-Tree reconciliation - {} closed trees, {} in Super-Tree",
        closed_count,
        super_size
    );

    // 3. Check consistency
    if closed_count < super_size {
        return Err(StorageError::Corruption(format!(
            "Super-Tree has {} leaves but only {} closed trees in SQLite",
            super_size, closed_count
        )));
    }

    if closed_count == super_size {
        tracing::info!("recovery: Super-Tree is consistent");
        return Ok(());
    }

    // 4. Append missing trees to Super-Tree
    let missing_count = closed_count - super_size;
    tracing::warn!(
        "recovery: {} trees missing from Super-Tree, reconciling...",
        missing_count
    );

    for (i, tree) in closed_trees.iter().enumerate().skip(super_size as usize) {
        let expected_index = i as u64;

        // Append root to Super-Tree (atomic operation)
        super_slab.append_leaf(&tree.root_hash)?;

        tracing::info!(
            "recovery: Reconciled tree {} (root_hash={}) into Super-Tree at index {}",
            tree.id,
            hex::encode(&tree.root_hash[..8]),
            expected_index
        );
    }

    // 5. Verify final state
    let final_size = super_slab.leaf_count();
    if final_size != closed_count {
        return Err(StorageError::Corruption(format!(
            "Super-Tree reconciliation failed: expected size {}, got {}",
            closed_count, final_size
        )));
    }

    tracing::info!(
        "recovery: Super-Tree reconciliation complete, {} trees now consistent",
        final_size
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::index::IndexStore;
    use rusqlite::Connection;
    use tempfile::TempDir;

    fn create_test_index() -> (IndexStore, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let index = IndexStore::open(&db_path).unwrap();
        index.initialize().unwrap();
        (index, dir)
    }

    fn create_test_super_slab() -> (PinnedSuperTreeSlab, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let slab_path = dir.path().join("super_tree.slab");
        let slab = PinnedSuperTreeSlab::open_or_create(&slab_path, 1024).unwrap();
        (slab, dir)
    }

    fn create_active_tree(index: &IndexStore) -> i64 {
        let origin_id = [0u8; 32];
        index.create_active_tree(&origin_id, 0).unwrap()
    }

    fn close_tree(
        index: &mut IndexStore,
        end_size: u64,
        root_hash: [u8; 32],
        data_tree_index: u64,
    ) -> i64 {
        let origin_id = [0u8; 32];
        let result = index
            .close_tree_and_create_new(&origin_id, end_size, &root_hash, data_tree_index)
            .unwrap();
        result.closed_tree_id
    }

    #[tokio::test]
    async fn test_reconcile_when_consistent() {
        // Arrange: 3 closed trees, Super-Tree size = 3
        let (mut index, _index_dir) = create_test_index();
        let (mut super_slab, _slabs_dir) = create_test_super_slab();

        create_active_tree(&index);
        for i in 0..3 {
            let mut hash = [0u8; 32];
            hash[0] = i as u8 + 1; // Use non-zero hashes
            close_tree(&mut index, (i + 1) * 100, hash, i);
            super_slab.append_leaf(&hash).unwrap();
        }

        // Act
        let result = reconcile_super_tree(&mut index, &mut super_slab).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(super_slab.leaf_count(), 3);
    }

    #[tokio::test]
    async fn test_reconcile_appends_missing_trees() {
        // Arrange: 3 closed trees, Super-Tree size = 1
        let (mut index, _index_dir) = create_test_index();
        let (mut super_slab, _slabs_dir) = create_test_super_slab();

        create_active_tree(&index);
        for i in 0..3 {
            let mut hash = [0u8; 32];
            hash[0] = i as u8 + 1; // Use non-zero hashes
            close_tree(&mut index, (i + 1) * 100, hash, i);
        }
        // Only append first tree to Super-Tree
        let mut first_hash = [0u8; 32];
        first_hash[0] = 1;
        super_slab.append_leaf(&first_hash).unwrap();

        // Act
        let result = reconcile_super_tree(&mut index, &mut super_slab).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(super_slab.leaf_count(), 3);
    }

    #[tokio::test]
    async fn test_reconcile_fails_on_corruption() {
        // Arrange: 1 closed tree, Super-Tree size = 3 (impossible state)
        let (mut index, _index_dir) = create_test_index();
        let (mut super_slab, _slabs_dir) = create_test_super_slab();

        create_active_tree(&index);
        close_tree(&mut index, 100, [1u8; 32], 0);

        // Create Super-Tree with 3 leaves (more than closed trees)
        super_slab.append_leaf(&[1u8; 32]).unwrap();
        super_slab.append_leaf(&[2u8; 32]).unwrap();
        super_slab.append_leaf(&[3u8; 32]).unwrap();

        // Act
        let result = reconcile_super_tree(&mut index, &mut super_slab).await;

        // Assert
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StorageError::Corruption(_)));
    }

    #[tokio::test]
    async fn test_reconcile_preserves_order() {
        // Arrange: 3 closed trees with specific roots, Super-Tree empty
        let (mut index, _index_dir) = create_test_index();
        let (mut super_slab, _slabs_dir) = create_test_super_slab();

        create_active_tree(&index);
        let roots = [[1u8; 32], [2u8; 32], [3u8; 32]];
        for (i, root) in roots.iter().enumerate() {
            close_tree(&mut index, (i + 1) as u64 * 100, *root, i as u64);
        }

        // Act
        let result = reconcile_super_tree(&mut index, &mut super_slab).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(super_slab.leaf_count(), 3);

        // Verify roots were added in correct order
        for (i, expected_root) in roots.iter().enumerate() {
            let actual_root = super_slab.get_node(0, i as u64).unwrap();
            assert_eq!(&actual_root, expected_root);
        }
    }

    #[test]
    fn test_get_closed_trees_ordered_returns_correct_order() {
        // Arrange: Insert trees closed at different times
        let conn = Connection::open_in_memory().unwrap();

        // Create schema
        conn.execute_batch(crate::storage::index::schema::SCHEMA_V3)
            .unwrap();

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // Insert trees with different closed_at times
        for (i, closed_at_offset) in [100i64, 50, 150].iter().enumerate() {
            conn.execute(
                "INSERT INTO trees (origin_id, status, start_size, end_size, root_hash, created_at, closed_at, data_tree_index)
                 VALUES (?1, 'pending_bitcoin', ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    &[i as u8; 32],
                    i as i64 * 100,
                    (i + 1) as i64 * 100,
                    &[i as u8; 32],
                    now,
                    now + closed_at_offset,
                    i as i64,
                ],
            )
            .unwrap();
        }

        let index = IndexStore::from_connection(conn);

        // Act
        let trees = index.get_closed_trees_ordered().unwrap();

        // Assert: Should be ordered by closed_at ASC
        assert_eq!(trees.len(), 3);
        assert_eq!(trees[0].closed_at, now + 50);
        assert_eq!(trees[1].closed_at, now + 100);
        assert_eq!(trees[2].closed_at, now + 150);
    }

    #[tokio::test]
    async fn test_reconcile_with_no_closed_trees() {
        // Arrange: Active tree only, no closed trees
        let (mut index, _index_dir) = create_test_index();
        let (mut super_slab, _slabs_dir) = create_test_super_slab();

        create_active_tree(&index);

        // Act
        let result = reconcile_super_tree(&mut index, &mut super_slab).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(super_slab.leaf_count(), 0);
    }
}
