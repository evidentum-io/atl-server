// File: src/storage/index/lifecycle.rs

//! Tree lifecycle management for IndexStore
//!
//! This module contains methods for managing tree lifecycle:
//! - Active tree queries (get_active_tree, create_active_tree)
//! - Tree closure (close_tree_and_create_new)
//! - Tree status updates (mark_tree_closed, update_tree_tsa_anchor)
//! - Tree queries (get_trees_pending_tsa, get_trees_pending_bitcoin_confirmation)

use super::queries::IndexStore;
use rusqlite::{params, OptionalExtension};

/// Result of closing a tree and creating a new one
#[derive(Debug, Clone)]
pub struct TreeCloseResult {
    /// ID of the tree that was closed
    pub closed_tree_id: i64,
    /// ID of the newly created active tree
    pub new_tree_id: i64,
    /// Metadata for Chain Index and genesis insertion
    pub closed_tree_metadata: ClosedTreeMetadata,
}

/// Metadata for closed tree (for Chain Index recording)
#[derive(Debug, Clone)]
pub struct ClosedTreeMetadata {
    pub tree_id: i64,
    pub origin_id: [u8; 32],
    pub root_hash: [u8; 32],
    pub tree_size: u64,
    pub closed_at: i64,
    /// Position in Super-Tree (data tree index)
    pub data_tree_index: u64,
}

/// Result of rotating a tree (closing old + creating new)
#[derive(Debug, Clone)]
pub struct TreeRotationResult {
    /// ID of the tree that was closed
    pub closed_tree_id: i64,
    /// ID of the newly created active tree
    pub new_tree_id: i64,
    /// Position in Super-Tree (data tree index)
    pub data_tree_index: u64,
    /// Super-Tree root hash after append
    pub super_root: [u8; 32],
    /// Metadata for Chain Index
    pub closed_tree_metadata: ClosedTreeMetadata,
    /// New tree head after rotation
    pub new_tree_head: crate::traits::TreeHead,
}

/// Tree record from database
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TreeRecord {
    pub id: i64,
    pub origin_id: [u8; 32],
    pub status: TreeStatus,
    pub start_size: u64,
    pub end_size: Option<u64>,
    pub root_hash: Option<[u8; 32]>,
    pub created_at: i64,
    pub first_entry_at: Option<i64>,
    pub closed_at: Option<i64>,
    pub tsa_anchor_id: Option<i64>,
    pub bitcoin_anchor_id: Option<i64>,
}

/// Info about a closed tree for recovery
#[derive(Debug, Clone)]
pub struct ClosedTreeInfo {
    pub id: i64,
    pub root_hash: [u8; 32],
    #[allow(dead_code)] // Used for ordering, may be useful for logging
    pub closed_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeStatus {
    Active,
    PendingBitcoin,
    Closed,
}

impl TreeStatus {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            TreeStatus::Active => "active",
            TreeStatus::PendingBitcoin => "pending_bitcoin",
            TreeStatus::Closed => "closed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "active" => Some(TreeStatus::Active),
            "pending_bitcoin" => Some(TreeStatus::PendingBitcoin),
            "closed" => Some(TreeStatus::Closed),
            _ => None,
        }
    }
}

/// Convert database row to TreeRecord
fn row_to_tree(row: &rusqlite::Row) -> rusqlite::Result<TreeRecord> {
    let id: i64 = row.get(0)?;
    let origin_id: Vec<u8> = row.get(1)?;
    let status: String = row.get(2)?;
    let start_size: i64 = row.get(3)?;
    let end_size: Option<i64> = row.get(4)?;
    let root_hash: Option<Vec<u8>> = row.get(5)?;
    let created_at: i64 = row.get(6)?;
    let first_entry_at: Option<i64> = row.get(7)?;
    let closed_at: Option<i64> = row.get(8)?;
    let tsa_anchor_id: Option<i64> = row.get(9)?;
    let bitcoin_anchor_id: Option<i64> = row.get(10)?;

    Ok(TreeRecord {
        id,
        origin_id: origin_id.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(1, "origin_id".into(), rusqlite::types::Type::Blob)
        })?,
        status: TreeStatus::parse(&status).unwrap_or(TreeStatus::Active),
        start_size: start_size as u64,
        end_size: end_size.map(|s| s as u64),
        root_hash: root_hash.and_then(|h| h.try_into().ok()),
        created_at,
        first_entry_at,
        closed_at,
        tsa_anchor_id,
        bitcoin_anchor_id,
    })
}

impl IndexStore {
    /// Get the currently active tree
    pub fn get_active_tree(&self) -> rusqlite::Result<Option<TreeRecord>> {
        self.connection()
            .query_row(
                "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                        first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
                 FROM trees WHERE status = 'active' LIMIT 1",
                [],
                row_to_tree,
            )
            .optional()
    }

    /// Create a new active tree
    ///
    /// Returns the tree ID.
    pub fn create_active_tree(
        &self,
        origin_id: &[u8; 32],
        start_size: u64,
    ) -> rusqlite::Result<i64> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        self.connection().execute(
            "INSERT INTO trees (origin_id, status, start_size, created_at)
             VALUES (?1, 'active', ?2, ?3)",
            params![origin_id.as_slice(), start_size as i64, now],
        )?;

        Ok(self.connection().last_insert_rowid())
    }

    /// Close the active tree and create a new one atomically
    ///
    /// **IMPORTANT:** This method does NOT insert genesis leaf.
    /// The caller must use `StorageEngine::rotate_tree()` which coordinates
    /// genesis leaf insertion into both Slab and SQLite.
    ///
    /// The closed tree is marked as 'pending_bitcoin' with bitcoin_anchor_id = NULL.
    /// The ots_job will create the anchor and set the bitcoin_anchor_id later.
    ///
    /// Returns TreeCloseResult with metadata for genesis insertion.
    pub fn close_tree_and_create_new(
        &mut self,
        origin_id: &[u8; 32],
        end_size: u64,
        root_hash: &[u8; 32],
        data_tree_index: u64,
    ) -> rusqlite::Result<TreeCloseResult> {
        let mut conn = self.connection_mut();
        let tx = conn.transaction()?;
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // Get current active tree
        let active_tree_id: i64 =
            tx.query_row("SELECT id FROM trees WHERE status = 'active'", [], |row| {
                row.get(0)
            })?;

        // Update active tree to pending_bitcoin with data_tree_index
        tx.execute(
            "UPDATE trees SET status = 'pending_bitcoin', end_size = ?1, root_hash = ?2, closed_at = ?3, data_tree_index = ?4
             WHERE id = ?5",
            params![end_size as i64, root_hash.as_slice(), now, data_tree_index as i64, active_tree_id],
        )?;

        // Create new active tree
        // NOTE: start_size = end_size (genesis will be inserted by caller via rotate_tree)
        tx.execute(
            "INSERT INTO trees (origin_id, status, start_size, created_at)
             VALUES (?1, 'active', ?2, ?3)",
            params![origin_id.as_slice(), end_size as i64, now],
        )?;

        let new_tree_id = tx.last_insert_rowid();

        // NOTE: NO genesis leaf insertion here!
        // Genesis is inserted by StorageEngine::rotate_tree() which coordinates Slab + SQLite

        tx.commit()?;

        // Prepare metadata for caller (Chain Index + genesis insertion)
        let closed_tree_metadata = ClosedTreeMetadata {
            tree_id: active_tree_id,
            origin_id: *origin_id,
            root_hash: *root_hash,
            tree_size: end_size,
            closed_at: now,
            data_tree_index,
        };

        Ok(TreeCloseResult {
            closed_tree_id: active_tree_id,
            new_tree_id,
            closed_tree_metadata,
        })
    }

    /// Set bitcoin anchor for a tree (called by ots_job after anchor creation)
    #[allow(dead_code)]
    pub fn set_tree_bitcoin_anchor(&self, tree_id: i64, anchor_id: i64) -> rusqlite::Result<()> {
        self.connection().execute(
            "UPDATE trees SET bitcoin_anchor_id = ?1 WHERE id = ?2 AND status = 'pending_bitcoin'",
            params![anchor_id, tree_id],
        )?;
        Ok(())
    }

    /// Get trees pending TSA anchoring
    pub fn get_trees_pending_tsa(&self) -> rusqlite::Result<Vec<TreeRecord>> {
        let conn = self.connection();
        let mut stmt = conn.prepare(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                    first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
             FROM trees
             WHERE status IN ('pending_bitcoin', 'closed') AND tsa_anchor_id IS NULL
             ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map([], row_to_tree)?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    /// Get trees pending Bitcoin confirmation
    #[allow(dead_code)]
    pub fn get_trees_pending_bitcoin_confirmation(&self) -> rusqlite::Result<Vec<TreeRecord>> {
        let conn = self.connection();
        let mut stmt = conn.prepare(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                    first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
             FROM trees WHERE status = 'pending_bitcoin' ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map([], row_to_tree)?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    /// Mark tree as closed after Bitcoin confirmation
    #[allow(dead_code)]
    pub fn mark_tree_closed(&self, tree_id: i64) -> rusqlite::Result<()> {
        self.connection().execute(
            "UPDATE trees SET status = 'closed' WHERE id = ?1 AND status = 'pending_bitcoin'",
            params![tree_id],
        )?;
        Ok(())
    }

    /// Update tree TSA anchor
    pub fn update_tree_tsa_anchor(&self, tree_id: i64, anchor_id: i64) -> rusqlite::Result<()> {
        self.connection().execute(
            "UPDATE trees SET tsa_anchor_id = ?1 WHERE id = ?2",
            params![anchor_id, tree_id],
        )?;
        Ok(())
    }

    /// Get tree by ID
    #[allow(dead_code)]
    pub fn get_tree(&self, tree_id: i64) -> rusqlite::Result<Option<TreeRecord>> {
        self.connection()
            .query_row(
                "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                        first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
                 FROM trees WHERE id = ?1",
                params![tree_id],
                row_to_tree,
            )
            .optional()
    }

    /// Get tree covering a specific entry
    #[allow(dead_code)]
    pub fn get_tree_covering_entry(&self, leaf_index: u64) -> rusqlite::Result<Option<TreeRecord>> {
        self.connection()
            .query_row(
                "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                        first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
                 FROM trees
                 WHERE start_size <= ?1 AND end_size > ?1 AND status IN ('pending_bitcoin', 'closed')
                 LIMIT 1",
                params![leaf_index as i64],
                row_to_tree,
            )
            .optional()
    }

    /// Get tree by bitcoin anchor ID
    #[allow(dead_code)]
    pub fn get_tree_by_bitcoin_anchor_id(
        &self,
        anchor_id: i64,
    ) -> rusqlite::Result<Option<TreeRecord>> {
        self.connection()
            .query_row(
                "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                        first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
                 FROM trees WHERE bitcoin_anchor_id = ?1 LIMIT 1",
                params![anchor_id],
                row_to_tree,
            )
            .optional()
    }

    /// Get all closed trees ordered by close time
    ///
    /// Used by recovery to reconcile Super-Tree.
    /// Returns trees in the order they were closed (by closed_at ASC).
    pub fn get_closed_trees_ordered(&self) -> rusqlite::Result<Vec<ClosedTreeInfo>> {
        let conn = self.connection();
        let mut stmt = conn.prepare(
            "SELECT id, root_hash, closed_at
             FROM trees
             WHERE status != 'active'
             ORDER BY closed_at ASC",
        )?;

        let rows = stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let root_hash_vec: Vec<u8> = row.get(1)?;
            let closed_at: i64 = row.get(2)?;

            let root_hash: [u8; 32] = root_hash_vec.try_into().map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    1,
                    "root_hash".to_string(),
                    rusqlite::types::Type::Blob,
                )
            })?;

            Ok(ClosedTreeInfo {
                id,
                root_hash,
                closed_at,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
    }

    /// Update tree first_entry_at (called on first entry append)
    #[allow(dead_code)]
    pub fn update_tree_first_entry_at(&self, tree_id: i64) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        self.connection().execute(
            "UPDATE trees SET first_entry_at = ?1 WHERE id = ?2 AND first_entry_at IS NULL",
            params![now, tree_id],
        )?;

        Ok(())
    }

    /// Update first_entry_at for active tree (idempotent - only updates if NULL)
    ///
    /// Called on every append_batch, but only affects the first batch in a new tree.
    /// The WHERE clause ensures this is a no-op after the first batch.
    pub fn update_first_entry_at_for_active_tree(&self) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        self.connection().execute(
            "UPDATE trees SET first_entry_at = ?1 WHERE status = 'active' AND first_entry_at IS NULL",
            params![now],
        )?;
        Ok(())
    }

    /// Get all closed trees (for Chain Index sync)
    pub fn get_all_closed_trees(&self) -> rusqlite::Result<Vec<TreeRecord>> {
        let conn = self.connection();
        let mut stmt = conn.prepare(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                    first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
             FROM trees
             WHERE status IN ('pending_bitcoin', 'closed')
             ORDER BY id ASC",
        )?;

        let rows = stmt.query_map([], row_to_tree)?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    /// Get data_tree_index for a tree (used by RECEIPT-1)
    ///
    /// Returns None if tree is still active, not found, or has invalid (negative) value.
    #[allow(dead_code)]
    pub fn get_tree_data_tree_index(&self, tree_id: i64) -> rusqlite::Result<Option<u64>> {
        self.connection()
            .query_row(
                "SELECT data_tree_index FROM trees WHERE id = ?1",
                params![tree_id],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map(|opt| {
                opt.and_then(|v| {
                    if v >= 0 {
                        Some(v as u64)
                    } else {
                        // Defensive: negative values indicate data corruption
                        tracing::error!(
                            tree_id = tree_id,
                            value = v,
                            "Negative data_tree_index in database - possible data corruption"
                        );
                        None
                    }
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn create_test_index_store() -> IndexStore {
        let conn = Connection::open_in_memory().unwrap();

        // Create tables schema
        conn.execute(
            "CREATE TABLE trees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                origin_id BLOB NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                start_size INTEGER NOT NULL,
                end_size INTEGER,
                root_hash BLOB,
                data_tree_index INTEGER,
                created_at INTEGER NOT NULL,
                first_entry_at INTEGER,
                closed_at INTEGER,
                tsa_anchor_id INTEGER,
                bitcoin_anchor_id INTEGER
            )",
            [],
        )
        .unwrap();

        conn.execute(
            "CREATE UNIQUE INDEX idx_trees_active ON trees(status) WHERE status = 'active'",
            [],
        )
        .unwrap();

        conn.execute(
            "CREATE TABLE entries (
                id TEXT PRIMARY KEY,
                leaf_index INTEGER NOT NULL,
                slab_id INTEGER NOT NULL,
                slab_offset INTEGER NOT NULL,
                payload_hash BLOB NOT NULL,
                metadata_hash BLOB NOT NULL,
                metadata_cleartext TEXT,
                tree_id INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            )",
            [],
        )
        .unwrap();

        IndexStore::from_connection(conn)
    }

    #[test]
    fn test_close_tree_does_not_insert_genesis() {
        let mut store = create_test_index_store();
        let origin_id = [1u8; 32];

        // Create initial active tree
        store.create_active_tree(&origin_id, 0).unwrap();

        // Simulate some entries (we don't actually insert them, just for the test context)
        let end_size = 100u64;
        let root_hash = [2u8; 32];

        // Get entry count before close
        let count_before: i64 = store
            .connection()
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
            .unwrap();

        // Close tree and create new one
        let result = store
            .close_tree_and_create_new(&origin_id, end_size, &root_hash, 0)
            .unwrap();

        // Get entry count after close
        let count_after: i64 = store
            .connection()
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
            .unwrap();

        // Assert: no entry was inserted (genesis leaf not created)
        assert_eq!(count_before, count_after);

        // Assert: no entry exists at leaf_index = end_size (genesis position)
        let entry_at_genesis: Option<i64> = store
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM entries WHERE leaf_index = ?1",
                [end_size as i64],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(entry_at_genesis, Some(0));

        // Verify result structure
        assert!(result.closed_tree_id > 0);
        assert!(result.new_tree_id > 0);
        assert_ne!(result.closed_tree_id, result.new_tree_id);
    }

    #[test]
    fn test_close_tree_result_has_metadata() {
        let mut store = create_test_index_store();
        let origin_id = [1u8; 32];

        // Create and populate first tree
        let tree_id = store.create_active_tree(&origin_id, 0).unwrap();

        let end_size = 42u64;
        let root_hash = [3u8; 32];

        // Close tree
        let result = store
            .close_tree_and_create_new(&origin_id, end_size, &root_hash, 0)
            .unwrap();

        // Assert: metadata contains correct values
        assert_eq!(result.closed_tree_metadata.tree_id, tree_id);
        assert_eq!(result.closed_tree_metadata.origin_id, origin_id);
        assert_eq!(result.closed_tree_metadata.root_hash, root_hash);
        assert_eq!(result.closed_tree_metadata.tree_size, end_size);
        assert!(result.closed_tree_metadata.closed_at > 0);
        assert_eq!(result.closed_tree_metadata.data_tree_index, 0);
    }

    #[test]
    fn test_new_tree_first_entry_at_is_null() {
        let mut store = create_test_index_store();
        let origin_id = [1u8; 32];

        // Create first tree
        store.create_active_tree(&origin_id, 0).unwrap();

        let end_size = 10u64;
        let root_hash = [4u8; 32];

        // Close tree
        let result = store
            .close_tree_and_create_new(&origin_id, end_size, &root_hash, 0)
            .unwrap();

        // Query new tree
        let first_entry_at: Option<i64> = store
            .connection()
            .query_row(
                "SELECT first_entry_at FROM trees WHERE id = ?1",
                [result.new_tree_id],
                |row| row.get(0),
            )
            .unwrap();

        // Assert: new tree has first_entry_at = NULL (genesis not counted)
        assert_eq!(first_entry_at, None);
    }

    #[test]
    fn test_closed_tree_status_is_pending_bitcoin() {
        let mut store = create_test_index_store();
        let origin_id = [1u8; 32];

        // Create first tree
        store.create_active_tree(&origin_id, 0).unwrap();

        let end_size = 100u64;
        let root_hash = [5u8; 32];

        // Close tree
        let result = store
            .close_tree_and_create_new(&origin_id, end_size, &root_hash, 0)
            .unwrap();

        // Query closed tree status
        let status: String = store
            .connection()
            .query_row(
                "SELECT status FROM trees WHERE id = ?1",
                [result.closed_tree_id],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(status, "pending_bitcoin");
    }

    #[test]
    fn test_new_tree_start_size_equals_closed_tree_end_size() {
        let mut store = create_test_index_store();
        let origin_id = [1u8; 32];

        // Create first tree
        store.create_active_tree(&origin_id, 0).unwrap();

        let end_size = 123u64;
        let root_hash = [7u8; 32];

        // Close tree
        let result = store
            .close_tree_and_create_new(&origin_id, end_size, &root_hash, 0)
            .unwrap();

        // Query new tree's start_size
        let start_size: i64 = store
            .connection()
            .query_row(
                "SELECT start_size FROM trees WHERE id = ?1",
                [result.new_tree_id],
                |row| row.get(0),
            )
            .unwrap();

        // Assert: new tree starts where old tree ended
        assert_eq!(start_size as u64, end_size);
    }

    #[test]
    fn test_close_tree_stores_data_tree_index() {
        let mut store = create_test_index_store();
        let origin_id = [1u8; 32];
        store.create_active_tree(&origin_id, 0).unwrap();

        let end_size = 100u64;
        let root_hash = [8u8; 32];
        let data_tree_index = 0u64;

        let result = store
            .close_tree_and_create_new(&origin_id, end_size, &root_hash, data_tree_index)
            .unwrap();

        // Query closed tree's data_tree_index
        let stored_index: Option<i64> = store
            .connection()
            .query_row(
                "SELECT data_tree_index FROM trees WHERE id = ?1",
                [result.closed_tree_id],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(stored_index, Some(data_tree_index as i64));
    }

    #[test]
    fn test_get_tree_data_tree_index_returns_correct_value() {
        let mut store = create_test_index_store();
        let origin_id = [1u8; 32];
        store.create_active_tree(&origin_id, 0).unwrap();

        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[9u8; 32], 0)
            .unwrap();

        let index = store
            .get_tree_data_tree_index(result.closed_tree_id)
            .unwrap();
        assert_eq!(index, Some(0));
    }

    #[test]
    fn test_get_tree_data_tree_index_returns_none_for_active() {
        let store = create_test_index_store();
        let origin_id = [1u8; 32];
        let tree_id = store.create_active_tree(&origin_id, 0).unwrap();

        let index = store.get_tree_data_tree_index(tree_id).unwrap();
        assert_eq!(index, None, "Active tree should not have data_tree_index");
    }

    #[test]
    fn test_data_tree_index_increments_for_multiple_closes() {
        let mut store = create_test_index_store();
        let origin_id = [0u8; 32];
        store.create_active_tree(&origin_id, 0).unwrap();

        for i in 0..3 {
            let result = store
                .close_tree_and_create_new(&origin_id, (i + 1) * 100, &[i as u8; 32], i)
                .unwrap();

            let stored = store
                .get_tree_data_tree_index(result.closed_tree_id)
                .unwrap();
            assert_eq!(stored, Some(i));
        }
    }

    #[test]
    fn test_tree_status_as_str() {
        assert_eq!(TreeStatus::Active.as_str(), "active");
        assert_eq!(TreeStatus::PendingBitcoin.as_str(), "pending_bitcoin");
        assert_eq!(TreeStatus::Closed.as_str(), "closed");
    }

    #[test]
    fn test_tree_status_parse() {
        assert_eq!(TreeStatus::parse("active"), Some(TreeStatus::Active));
        assert_eq!(TreeStatus::parse("pending_bitcoin"), Some(TreeStatus::PendingBitcoin));
        assert_eq!(TreeStatus::parse("closed"), Some(TreeStatus::Closed));
        assert_eq!(TreeStatus::parse("invalid"), None);
        assert_eq!(TreeStatus::parse(""), None);
    }

    #[test]
    fn test_get_active_tree_empty() {
        let store = create_test_index_store();
        let tree = store.get_active_tree().unwrap();
        assert!(tree.is_none());
    }

    #[test]
    fn test_get_active_tree_exists() {
        let store = create_test_index_store();
        let origin_id = [0xaa; 32];
        let tree_id = store.create_active_tree(&origin_id, 0).unwrap();

        let active = store.get_active_tree().unwrap().unwrap();
        assert_eq!(active.id, tree_id);
        assert_eq!(active.status, TreeStatus::Active);
        assert_eq!(active.start_size, 0);
    }

    #[test]
    fn test_create_active_tree_with_non_zero_start() {
        let store = create_test_index_store();
        let origin_id = [0xbb; 32];
        let tree_id = store.create_active_tree(&origin_id, 100).unwrap();

        let tree = store.get_active_tree().unwrap().unwrap();
        assert_eq!(tree.id, tree_id);
        assert_eq!(tree.start_size, 100);
    }

    #[test]
    fn test_set_tree_bitcoin_anchor() {
        let mut store = create_test_index_store();
        let origin_id = [0xcc; 32];
        store.create_active_tree(&origin_id, 0).unwrap();
        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[0x11; 32], 0)
            .unwrap();

        store.set_tree_bitcoin_anchor(result.closed_tree_id, 42).unwrap();

        let tree = store.get_tree(result.closed_tree_id).unwrap().unwrap();
        assert_eq!(tree.bitcoin_anchor_id, Some(42));
    }

    #[test]
    fn test_set_tree_bitcoin_anchor_nonexistent() {
        let store = create_test_index_store();
        // Should not error, just no-op
        let result = store.set_tree_bitcoin_anchor(999, 42);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_trees_pending_tsa_empty() {
        let store = create_test_index_store();
        let trees = store.get_trees_pending_tsa().unwrap();
        assert_eq!(trees.len(), 0);
    }

    #[test]
    fn test_get_trees_pending_tsa_includes_pending_bitcoin() {
        let mut store = create_test_index_store();
        let origin_id = [0xdd; 32];
        store.create_active_tree(&origin_id, 0).unwrap();
        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[0x22; 32], 0)
            .unwrap();

        let pending = store.get_trees_pending_tsa().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, result.closed_tree_id);
        assert_eq!(pending[0].status, TreeStatus::PendingBitcoin);
    }

    #[test]
    fn test_get_trees_pending_tsa_filters_with_tsa_anchor() {
        let mut store = create_test_index_store();
        let origin_id = [0xee; 32];
        store.create_active_tree(&origin_id, 0).unwrap();
        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[0x33; 32], 0)
            .unwrap();

        // Set TSA anchor
        store.update_tree_tsa_anchor(result.closed_tree_id, 123).unwrap();

        let pending = store.get_trees_pending_tsa().unwrap();
        assert_eq!(pending.len(), 0); // Should be filtered out
    }

    #[test]
    fn test_get_trees_pending_bitcoin_confirmation_empty() {
        let store = create_test_index_store();
        let trees = store.get_trees_pending_bitcoin_confirmation().unwrap();
        assert_eq!(trees.len(), 0);
    }

    #[test]
    fn test_get_trees_pending_bitcoin_confirmation() {
        let mut store = create_test_index_store();
        let origin_id = [0xff; 32];
        store.create_active_tree(&origin_id, 0).unwrap();
        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[0x44; 32], 0)
            .unwrap();

        let pending = store.get_trees_pending_bitcoin_confirmation().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, result.closed_tree_id);
    }

    #[test]
    fn test_mark_tree_closed() {
        let mut store = create_test_index_store();
        let origin_id = [0xab; 32];
        store.create_active_tree(&origin_id, 0).unwrap();
        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[0x55; 32], 0)
            .unwrap();

        store.mark_tree_closed(result.closed_tree_id).unwrap();

        let tree = store.get_tree(result.closed_tree_id).unwrap().unwrap();
        assert_eq!(tree.status, TreeStatus::Closed);
    }

    #[test]
    fn test_mark_tree_closed_nonexistent() {
        let store = create_test_index_store();
        // Should not error, just no-op
        let result = store.mark_tree_closed(999);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_tree_tsa_anchor() {
        let mut store = create_test_index_store();
        let origin_id = [0xcd; 32];
        store.create_active_tree(&origin_id, 0).unwrap();
        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[0x66; 32], 0)
            .unwrap();

        store.update_tree_tsa_anchor(result.closed_tree_id, 789).unwrap();

        let tree = store.get_tree(result.closed_tree_id).unwrap().unwrap();
        assert_eq!(tree.tsa_anchor_id, Some(789));
    }

    #[test]
    fn test_get_tree_nonexistent() {
        let store = create_test_index_store();
        let tree = store.get_tree(999).unwrap();
        assert!(tree.is_none());
    }

    #[test]
    fn test_get_tree_covering_entry_no_match() {
        let store = create_test_index_store();
        let tree = store.get_tree_covering_entry(500).unwrap();
        assert!(tree.is_none());
    }

    #[test]
    fn test_get_tree_covering_entry_match() {
        let mut store = create_test_index_store();
        let origin_id = [0xef; 32];
        store.create_active_tree(&origin_id, 0).unwrap();
        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[0x77; 32], 0)
            .unwrap();

        // Query for entry within range [0, 100)
        let tree = store.get_tree_covering_entry(50).unwrap().unwrap();
        assert_eq!(tree.id, result.closed_tree_id);
        assert_eq!(tree.start_size, 0);
        assert_eq!(tree.end_size, Some(100));
    }

    #[test]
    fn test_get_tree_covering_entry_excludes_active() {
        let mut store = create_test_index_store();
        let origin_id = [0x12; 32];
        store.create_active_tree(&origin_id, 0).unwrap();

        // Active tree should not be returned
        let tree = store.get_tree_covering_entry(50).unwrap();
        assert!(tree.is_none());
    }

    #[test]
    fn test_get_tree_covering_entry_boundary() {
        let mut store = create_test_index_store();
        let origin_id = [0x34; 32];
        store.create_active_tree(&origin_id, 0).unwrap();
        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[0x88; 32], 0)
            .unwrap();

        // At start boundary
        let tree_start = store.get_tree_covering_entry(0).unwrap().unwrap();
        assert_eq!(tree_start.id, result.closed_tree_id);

        // At end boundary (exclusive)
        let tree_end = store.get_tree_covering_entry(100).unwrap();
        assert!(tree_end.is_none());

        // Just before end
        let tree_before_end = store.get_tree_covering_entry(99).unwrap().unwrap();
        assert_eq!(tree_before_end.id, result.closed_tree_id);
    }

    #[test]
    fn test_get_tree_by_bitcoin_anchor_id_nonexistent() {
        let store = create_test_index_store();
        let tree = store.get_tree_by_bitcoin_anchor_id(999).unwrap();
        assert!(tree.is_none());
    }

    #[test]
    fn test_get_tree_by_bitcoin_anchor_id_match() {
        let mut store = create_test_index_store();
        let origin_id = [0x56; 32];
        store.create_active_tree(&origin_id, 0).unwrap();
        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[0x99; 32], 0)
            .unwrap();

        store.set_tree_bitcoin_anchor(result.closed_tree_id, 555).unwrap();

        let tree = store.get_tree_by_bitcoin_anchor_id(555).unwrap().unwrap();
        assert_eq!(tree.id, result.closed_tree_id);
        assert_eq!(tree.bitcoin_anchor_id, Some(555));
    }

    #[test]
    fn test_get_closed_trees_ordered_empty() {
        let store = create_test_index_store();
        let trees = store.get_closed_trees_ordered().unwrap();
        assert_eq!(trees.len(), 0);
    }

    #[test]
    fn test_get_closed_trees_ordered_multiple() {
        let mut store = create_test_index_store();
        let origin_id = [0x78; 32];
        store.create_active_tree(&origin_id, 0).unwrap();

        for i in 0..3 {
            let _ = store
                .close_tree_and_create_new(&origin_id, (i + 1) * 100, &[i as u8; 32], i)
                .unwrap();
        }

        let trees = store.get_closed_trees_ordered().unwrap();
        assert_eq!(trees.len(), 3);

        // Verify ordering by closed_at
        for i in 0..2 {
            assert!(trees[i].closed_at <= trees[i + 1].closed_at);
        }
    }

    #[test]
    fn test_update_tree_first_entry_at() {
        let store = create_test_index_store();
        let origin_id = [0x9a; 32];
        let tree_id = store.create_active_tree(&origin_id, 0).unwrap();

        // Initially null
        let tree_before = store.get_tree(tree_id).unwrap().unwrap();
        assert!(tree_before.first_entry_at.is_none());

        store.update_tree_first_entry_at(tree_id).unwrap();

        let tree_after = store.get_tree(tree_id).unwrap().unwrap();
        assert!(tree_after.first_entry_at.is_some());
    }

    #[test]
    fn test_update_tree_first_entry_at_idempotent() {
        let store = create_test_index_store();
        let origin_id = [0xbc; 32];
        let tree_id = store.create_active_tree(&origin_id, 0).unwrap();

        store.update_tree_first_entry_at(tree_id).unwrap();
        let first_timestamp = store
            .get_tree(tree_id)
            .unwrap()
            .unwrap()
            .first_entry_at
            .unwrap();

        // Second update should not change timestamp
        store.update_tree_first_entry_at(tree_id).unwrap();
        let second_timestamp = store
            .get_tree(tree_id)
            .unwrap()
            .unwrap()
            .first_entry_at
            .unwrap();

        assert_eq!(first_timestamp, second_timestamp);
    }

    #[test]
    fn test_update_first_entry_at_for_active_tree() {
        let store = create_test_index_store();
        let origin_id = [0xde; 32];
        let tree_id = store.create_active_tree(&origin_id, 0).unwrap();

        store.update_first_entry_at_for_active_tree().unwrap();

        let tree = store.get_tree(tree_id).unwrap().unwrap();
        assert!(tree.first_entry_at.is_some());
    }

    #[test]
    fn test_update_first_entry_at_for_active_tree_no_active() {
        let store = create_test_index_store();
        // Should not error when no active tree exists
        let result = store.update_first_entry_at_for_active_tree();
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_all_closed_trees_empty() {
        let store = create_test_index_store();
        let trees = store.get_all_closed_trees().unwrap();
        assert_eq!(trees.len(), 0);
    }

    #[test]
    fn test_get_all_closed_trees_excludes_active() {
        let mut store = create_test_index_store();
        let origin_id = [0x21; 32];
        store.create_active_tree(&origin_id, 0).unwrap();

        // Close tree
        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[0xaa; 32], 0)
            .unwrap();

        let trees = store.get_all_closed_trees().unwrap();
        assert_eq!(trees.len(), 1);
        assert_eq!(trees[0].id, result.closed_tree_id);
    }

    #[test]
    fn test_get_tree_data_tree_index_negative_value() {
        let mut store = create_test_index_store();
        let origin_id = [0x43; 32];
        store.create_active_tree(&origin_id, 0).unwrap();
        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[0xbb; 32], 0)
            .unwrap();

        // Manually set negative value (simulating corruption)
        store
            .connection()
            .execute(
                "UPDATE trees SET data_tree_index = -1 WHERE id = ?1",
                [result.closed_tree_id],
            )
            .unwrap();

        let index = store.get_tree_data_tree_index(result.closed_tree_id).unwrap();
        assert!(index.is_none(), "Should return None for negative values");
    }

    #[test]
    fn test_row_to_tree_invalid_origin_id_size() {
        let store = create_test_index_store();

        // Manually insert invalid data
        store
            .connection()
            .execute(
                "INSERT INTO trees (id, origin_id, status, start_size, created_at)
                 VALUES (999, X'AA', 'active', 0, 0)",
                [],
            )
            .unwrap();

        let result = store.get_tree(999);
        assert!(result.is_err(), "Should fail with invalid origin_id size");
    }

    #[test]
    fn test_row_to_tree_invalid_status_fallback() {
        let store = create_test_index_store();

        // Manually insert invalid status
        store
            .connection()
            .execute(
                "INSERT INTO trees (id, origin_id, status, start_size, created_at)
                 VALUES (999, X'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'invalid_status', 0, 0)",
                [],
            )
            .unwrap();

        let tree = store.get_tree(999).unwrap().unwrap();
        // Should fallback to Active
        assert_eq!(tree.status, TreeStatus::Active);
    }

    #[test]
    fn test_tree_record_optional_fields_none() {
        let store = create_test_index_store();
        let origin_id = [0x65; 32];
        let tree_id = store.create_active_tree(&origin_id, 0).unwrap();

        let tree = store.get_tree(tree_id).unwrap().unwrap();
        assert!(tree.end_size.is_none());
        assert!(tree.root_hash.is_none());
        assert!(tree.first_entry_at.is_none());
        assert!(tree.closed_at.is_none());
        assert!(tree.tsa_anchor_id.is_none());
        assert!(tree.bitcoin_anchor_id.is_none());
    }

    #[test]
    fn test_tree_record_all_fields_populated() {
        let mut store = create_test_index_store();
        let origin_id = [0x87; 32];
        store.create_active_tree(&origin_id, 0).unwrap();
        let result = store
            .close_tree_and_create_new(&origin_id, 100, &[0xcc; 32], 0)
            .unwrap();

        store.update_tree_tsa_anchor(result.closed_tree_id, 111).unwrap();
        store.set_tree_bitcoin_anchor(result.closed_tree_id, 222).unwrap();
        store.mark_tree_closed(result.closed_tree_id).unwrap();

        let tree = store.get_tree(result.closed_tree_id).unwrap().unwrap();
        assert!(tree.end_size.is_some());
        assert!(tree.root_hash.is_some());
        assert!(tree.closed_at.is_some());
        assert_eq!(tree.tsa_anchor_id, Some(111));
        assert_eq!(tree.bitcoin_anchor_id, Some(222));
        assert_eq!(tree.status, TreeStatus::Closed);
    }
}
