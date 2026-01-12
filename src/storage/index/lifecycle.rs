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
#[allow(dead_code)]
pub struct ClosedTreeMetadata {
    pub tree_id: i64,
    pub origin_id: [u8; 32],
    pub root_hash: [u8; 32],
    pub tree_size: u64,
    pub prev_tree_id: Option<i64>,
    pub closed_at: i64,
}

/// Tree record from database
#[allow(dead_code)]
#[derive(Debug, Clone)]
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
    pub prev_tree_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeStatus {
    Active,
    PendingBitcoin,
    Closed,
}

#[allow(dead_code)]
impl TreeStatus {
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
    let prev_tree_id: Option<i64> = row.get(11)?;

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
        prev_tree_id,
    })
}

#[allow(dead_code)]
impl IndexStore {
    /// Get the currently active tree
    pub fn get_active_tree(&self) -> rusqlite::Result<Option<TreeRecord>> {
        self.connection()
            .query_row(
                "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                        first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id, prev_tree_id
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

    /// Close the active tree and create a new one atomically with chain link
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
    ) -> rusqlite::Result<TreeCloseResult> {
        let mut conn = self.connection_mut();
        let tx = conn.transaction()?;
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // Get current active tree (with prev_tree_id for chain continuity)
        let (active_tree_id, prev_tree_id_of_active): (i64, Option<i64>) = tx.query_row(
            "SELECT id, prev_tree_id FROM trees WHERE status = 'active'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        // Update active tree to pending_bitcoin
        tx.execute(
            "UPDATE trees SET status = 'pending_bitcoin', end_size = ?1, root_hash = ?2, closed_at = ?3
             WHERE id = ?4",
            params![end_size as i64, root_hash.as_slice(), now, active_tree_id],
        )?;

        // Create new active tree WITH chain link
        // NOTE: start_size = end_size (genesis will be inserted by caller via rotate_tree)
        tx.execute(
            "INSERT INTO trees (origin_id, status, start_size, created_at, prev_tree_id)
             VALUES (?1, 'active', ?2, ?3, ?4)",
            params![origin_id.as_slice(), end_size as i64, now, active_tree_id],
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
            prev_tree_id: prev_tree_id_of_active,
            closed_at: now,
        };

        Ok(TreeCloseResult {
            closed_tree_id: active_tree_id,
            new_tree_id,
            closed_tree_metadata,
        })
    }

    /// Set bitcoin anchor for a tree (called by ots_job after anchor creation)
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
                    first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id, prev_tree_id
             FROM trees
             WHERE status IN ('pending_bitcoin', 'closed') AND tsa_anchor_id IS NULL
             ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map([], row_to_tree)?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    /// Get trees pending Bitcoin confirmation
    pub fn get_trees_pending_bitcoin_confirmation(&self) -> rusqlite::Result<Vec<TreeRecord>> {
        let conn = self.connection();
        let mut stmt = conn.prepare(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                    first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id, prev_tree_id
             FROM trees WHERE status = 'pending_bitcoin' ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map([], row_to_tree)?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    /// Mark tree as closed after Bitcoin confirmation
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

    /// Get tree covering a specific entry
    pub fn get_tree_covering_entry(&self, leaf_index: u64) -> rusqlite::Result<Option<TreeRecord>> {
        self.connection()
            .query_row(
                "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                        first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id, prev_tree_id
                 FROM trees
                 WHERE start_size <= ?1 AND end_size > ?1 AND status IN ('pending_bitcoin', 'closed')
                 LIMIT 1",
                params![leaf_index as i64],
                row_to_tree,
            )
            .optional()
    }

    /// Get tree by bitcoin anchor ID
    pub fn get_tree_by_bitcoin_anchor_id(
        &self,
        anchor_id: i64,
    ) -> rusqlite::Result<Option<TreeRecord>> {
        self.connection()
            .query_row(
                "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                        first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id, prev_tree_id
                 FROM trees WHERE bitcoin_anchor_id = ?1 LIMIT 1",
                params![anchor_id],
                row_to_tree,
            )
            .optional()
    }

    /// Update tree first_entry_at (called on first entry append)
    pub fn update_tree_first_entry_at(&self, tree_id: i64) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        self.connection().execute(
            "UPDATE trees SET first_entry_at = ?1 WHERE id = ?2 AND first_entry_at IS NULL",
            params![now, tree_id],
        )?;

        Ok(())
    }

    /// Get trees that need OTS submission (pending_bitcoin but no anchor)
    pub fn get_trees_without_bitcoin_anchor(&self) -> rusqlite::Result<Vec<TreeRecord>> {
        let conn = self.connection();
        let mut stmt = conn.prepare(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                    first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id, prev_tree_id
             FROM trees
             WHERE status = 'pending_bitcoin' AND bitcoin_anchor_id IS NULL
             ORDER BY closed_at ASC",
        )?;

        let rows = stmt.query_map([], row_to_tree)?;
        rows.collect::<Result<Vec<_>, _>>()
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
                created_at INTEGER NOT NULL,
                first_entry_at INTEGER,
                closed_at INTEGER,
                tsa_anchor_id INTEGER,
                bitcoin_anchor_id INTEGER,
                prev_tree_id INTEGER
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
            .close_tree_and_create_new(&origin_id, end_size, &root_hash)
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
            .close_tree_and_create_new(&origin_id, end_size, &root_hash)
            .unwrap();

        // Assert: metadata contains correct values
        assert_eq!(result.closed_tree_metadata.tree_id, tree_id);
        assert_eq!(result.closed_tree_metadata.origin_id, origin_id);
        assert_eq!(result.closed_tree_metadata.root_hash, root_hash);
        assert_eq!(result.closed_tree_metadata.tree_size, end_size);
        assert_eq!(result.closed_tree_metadata.prev_tree_id, None); // First tree has no prev
        assert!(result.closed_tree_metadata.closed_at > 0);
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
            .close_tree_and_create_new(&origin_id, end_size, &root_hash)
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
            .close_tree_and_create_new(&origin_id, end_size, &root_hash)
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
    fn test_new_tree_has_prev_tree_id_chain_link() {
        let mut store = create_test_index_store();
        let origin_id = [1u8; 32];

        // Create first tree
        let first_tree_id = store.create_active_tree(&origin_id, 0).unwrap();

        let end_size = 50u64;
        let root_hash = [6u8; 32];

        // Close first tree
        let result = store
            .close_tree_and_create_new(&origin_id, end_size, &root_hash)
            .unwrap();

        // Query new tree's prev_tree_id
        let prev_tree_id: Option<i64> = store
            .connection()
            .query_row(
                "SELECT prev_tree_id FROM trees WHERE id = ?1",
                [result.new_tree_id],
                |row| row.get(0),
            )
            .unwrap();

        // Assert: new tree has chain link to closed tree
        assert_eq!(prev_tree_id, Some(first_tree_id));
        assert_eq!(prev_tree_id, Some(result.closed_tree_id));
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
            .close_tree_and_create_new(&origin_id, end_size, &root_hash)
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
}
