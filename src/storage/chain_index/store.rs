// File: src/storage/chain_index/store.rs

//! Chain Index store implementation

use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

use crate::storage::index::lifecycle::ClosedTreeMetadata;

/// Tree record in Chain Index
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ChainTreeRecord {
    pub tree_id: i64,
    pub origin_id: [u8; 32],
    pub root_hash: [u8; 32],
    pub tree_size: u64,
    pub data_tree_index: u64,
    pub status: ChainTreeStatus,
    pub bitcoin_txid: Option<String>,
    pub archive_location: Option<String>,
    pub created_at: String,
    pub closed_at: Option<String>,
    pub archived_at: Option<String>,
}

/// Tree status in Chain Index
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainTreeStatus {
    Active,
    Closed,
    Archived,
}

impl ChainTreeStatus {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            ChainTreeStatus::Active => "active",
            ChainTreeStatus::Closed => "closed",
            ChainTreeStatus::Archived => "archived",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "active" => Some(ChainTreeStatus::Active),
            "closed" => Some(ChainTreeStatus::Closed),
            "archived" => Some(ChainTreeStatus::Archived),
            _ => None,
        }
    }
}

/// Chain Index - separate SQLite database for tree metadata
pub struct ChainIndex {
    pub(super) conn: Connection,
}

impl ChainIndex {
    /// Open or create Chain Index database
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;

        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;

        // Check if this is a new database or needs migration
        let table_exists = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='chain_config'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;

        if table_exists.is_none() {
            // Fresh database - create v2 schema
            conn.execute_batch(super::schema::CHAIN_INDEX_SCHEMA)?;
            conn.execute(
                "INSERT OR IGNORE INTO chain_config (key, value, updated_at) VALUES ('schema_version', ?1, ?2)",
                params![
                    super::schema::CHAIN_INDEX_SCHEMA_VERSION.to_string(),
                    chrono::Utc::now().timestamp()
                ],
            )?;
        } else {
            // Existing database - check version
            let current_version = conn
                .query_row(
                    "SELECT value FROM chain_config WHERE key = 'schema_version'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(|v| v.parse::<u32>().unwrap_or(1))
                .unwrap_or(1);

            if current_version < 2 {
                // Apply migration from v1 to v2
                conn.execute_batch(super::schema::MIGRATE_CHAIN_V1_TO_V2)?;
            }
        }

        Ok(Self { conn })
    }

    /// Record a closed tree in the index
    ///
    /// Called by tree_closer job after `TreeRotator::rotate_tree()`.
    ///
    /// # Arguments
    /// * `metadata` - `ClosedTreeMetadata` from `TreeRotationResult`, contains data_tree_index
    pub fn record_closed_tree(&self, metadata: &ClosedTreeMetadata) -> rusqlite::Result<()> {
        let closed_at_secs = metadata.closed_at / 1_000_000_000;
        let closed_at = chrono::DateTime::from_timestamp(closed_at_secs, 0)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        self.conn.execute(
            r#"
            INSERT INTO trees (
                tree_id, origin_id, root_hash, tree_size,
                data_tree_index, status,
                created_at, closed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, 'closed', ?6, ?6)
            ON CONFLICT(tree_id) DO UPDATE SET
                root_hash = excluded.root_hash,
                tree_size = excluded.tree_size,
                data_tree_index = excluded.data_tree_index,
                status = 'closed',
                closed_at = excluded.closed_at
            "#,
            params![
                metadata.tree_id,
                metadata.origin_id.as_slice(),
                metadata.root_hash.as_slice(),
                metadata.tree_size as i64,
                metadata.data_tree_index as i64,
                closed_at,
            ],
        )?;

        Ok(())
    }

    /// Get tree by ID
    pub fn get_tree(&self, tree_id: i64) -> rusqlite::Result<Option<ChainTreeRecord>> {
        self.conn
            .query_row(
                "SELECT tree_id, origin_id, root_hash, tree_size, data_tree_index,
                        status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
                 FROM trees WHERE tree_id = ?1",
                params![tree_id],
                row_to_chain_tree,
            )
            .optional()
    }

    /// Get tree by data_tree_index (Super-Tree position)
    #[allow(dead_code)]
    pub fn get_tree_by_index(
        &self,
        data_tree_index: u64,
    ) -> rusqlite::Result<Option<ChainTreeRecord>> {
        self.conn
            .query_row(
                "SELECT tree_id, origin_id, root_hash, tree_size, data_tree_index,
                        status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
                 FROM trees WHERE data_tree_index = ?1",
                params![data_tree_index as i64],
                row_to_chain_tree,
            )
            .optional()
    }

    /// Get all trees in Super-Tree order (by data_tree_index)
    #[allow(dead_code)]
    pub fn get_trees_ordered(&self) -> rusqlite::Result<Vec<ChainTreeRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT tree_id, origin_id, root_hash, tree_size, data_tree_index,
                    status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
             FROM trees ORDER BY data_tree_index ASC",
        )?;
        let rows = stmt.query_map([], row_to_chain_tree)?;
        rows.collect()
    }

    /// Get all trees in chain order (oldest first)
    #[allow(dead_code)]
    pub fn get_all_trees(&self) -> rusqlite::Result<Vec<ChainTreeRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT tree_id, origin_id, root_hash, tree_size, data_tree_index,
                    status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
             FROM trees ORDER BY tree_id ASC",
        )?;
        let rows = stmt.query_map([], row_to_chain_tree)?;
        rows.collect()
    }

    /// Get trees by status
    #[allow(dead_code)]
    pub fn get_trees_by_status(
        &self,
        status: ChainTreeStatus,
    ) -> rusqlite::Result<Vec<ChainTreeRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT tree_id, origin_id, root_hash, tree_size, data_tree_index,
                    status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
             FROM trees WHERE status = ?1 ORDER BY tree_id ASC",
        )?;
        let rows = stmt.query_map(params![status.as_str()], row_to_chain_tree)?;
        rows.collect()
    }

    /// Update Bitcoin txid for a tree
    #[allow(dead_code)]
    pub fn set_bitcoin_txid(&self, tree_id: i64, txid: &str) -> rusqlite::Result<()> {
        self.conn.execute(
            "UPDATE trees SET bitcoin_txid = ?1 WHERE tree_id = ?2",
            params![txid, tree_id],
        )?;
        Ok(())
    }

    /// Get first tree (the one with data_tree_index = 0)
    #[allow(dead_code)]
    pub fn get_first_tree(&self) -> rusqlite::Result<Option<ChainTreeRecord>> {
        self.conn
            .query_row(
                "SELECT tree_id, origin_id, root_hash, tree_size, data_tree_index,
                        status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
                 FROM trees WHERE data_tree_index = 0 LIMIT 1",
                [],
                row_to_chain_tree,
            )
            .optional()
    }

    /// Get latest tree
    #[allow(dead_code)]
    pub fn get_latest_tree(&self) -> rusqlite::Result<Option<ChainTreeRecord>> {
        self.conn
            .query_row(
                "SELECT tree_id, origin_id, root_hash, tree_size, data_tree_index,
                        status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
                 FROM trees ORDER BY data_tree_index DESC LIMIT 1",
                [],
                row_to_chain_tree,
            )
            .optional()
    }

    /// Count total trees
    #[allow(dead_code)]
    pub fn count_trees(&self) -> rusqlite::Result<i64> {
        self.conn
            .query_row("SELECT COUNT(*) FROM trees", [], |row| row.get(0))
    }

    /// Sync Chain Index with main database
    ///
    /// Called on startup to ensure Chain Index is consistent.
    /// Inserts any trees from main DB that are missing from Chain Index.
    ///
    /// # Data Tree Index
    /// For synced trees, data_tree_index is derived from tree_id (tree_id - 1).
    /// This assumes sequential tree IDs starting from 1 (legacy behavior).
    pub fn sync_with_main_db(
        &self,
        index_store: &crate::storage::index::IndexStore,
    ) -> rusqlite::Result<usize> {
        let main_trees = index_store.get_all_closed_trees()?;
        let mut synced = 0;

        for tree in main_trees {
            if self.get_tree(tree.id)?.is_none() {
                let root_hash = tree.root_hash.unwrap_or([0u8; 32]);
                let tree_size = tree.end_size.unwrap_or(0);

                // Legacy: derive data_tree_index from tree_id (assuming sequential IDs)
                let data_tree_index = if tree.id > 0 { (tree.id - 1) as u64 } else { 0 };

                let metadata = ClosedTreeMetadata {
                    tree_id: tree.id,
                    origin_id: tree.origin_id,
                    root_hash,
                    tree_size,
                    closed_at: tree.closed_at.unwrap_or(0),
                    data_tree_index,
                };

                self.record_closed_tree(&metadata)?;
                synced += 1;

                tracing::info!(tree_id = tree.id, "Synced missing tree to Chain Index");
            }
        }

        Ok(synced)
    }
}

/// Convert database row to ChainTreeRecord
pub(super) fn row_to_chain_tree(row: &rusqlite::Row) -> rusqlite::Result<ChainTreeRecord> {
    let tree_id: i64 = row.get(0)?;
    let origin_id: Vec<u8> = row.get(1)?;
    let root_hash: Vec<u8> = row.get(2)?;
    let tree_size: i64 = row.get(3)?;
    let data_tree_index: i64 = row.get(4)?;
    let status: String = row.get(5)?;
    let bitcoin_txid: Option<String> = row.get(6)?;
    let archive_location: Option<String> = row.get(7)?;
    let created_at: String = row.get(8)?;
    let closed_at: Option<String> = row.get(9)?;
    let archived_at: Option<String> = row.get(10)?;

    Ok(ChainTreeRecord {
        tree_id,
        origin_id: origin_id.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(1, "origin_id".into(), rusqlite::types::Type::Blob)
        })?,
        root_hash: root_hash.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(2, "root_hash".into(), rusqlite::types::Type::Blob)
        })?,
        tree_size: tree_size as u64,
        data_tree_index: data_tree_index as u64,
        status: ChainTreeStatus::parse(&status).unwrap_or(ChainTreeStatus::Closed),
        bitcoin_txid,
        archive_location,
        created_at,
        closed_at,
        archived_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_chain_index_creation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("chain_index.db");
        let ci = ChainIndex::open(&path).unwrap();
        assert_eq!(ci.count_trees().unwrap(), 0);
    }

    #[test]
    fn test_record_and_get_tree() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let metadata = ClosedTreeMetadata {
            tree_id: 1,
            origin_id: [0xaa; 32],
            root_hash: [0xbb; 32],
            tree_size: 100,
            closed_at: 1_234_567_890_000_000_000,
            data_tree_index: 0,
        };

        ci.record_closed_tree(&metadata).unwrap();

        let tree = ci.get_tree(1).unwrap().unwrap();
        assert_eq!(tree.tree_id, 1);
        assert_eq!(tree.tree_size, 100);
        assert_eq!(tree.data_tree_index, 0);
    }

    #[test]
    fn test_chain_verification() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let m1 = ClosedTreeMetadata {
            tree_id: 1,
            origin_id: [0xaa; 32],
            root_hash: [0x11; 32],
            tree_size: 100,
            closed_at: 1_000_000_000_000,
            data_tree_index: 0,
        };
        ci.record_closed_tree(&m1).unwrap();

        let m2 = ClosedTreeMetadata {
            tree_id: 2,
            origin_id: [0xaa; 32],
            root_hash: [0x22; 32],
            tree_size: 200,
            closed_at: 2_000_000_000_000,
            data_tree_index: 1,
        };
        ci.record_closed_tree(&m2).unwrap();

        let result = ci.verify_full_chain().unwrap();
        assert!(result.valid);
        assert_eq!(result.verified_trees, 2);
    }

    #[test]
    fn test_broken_chain_detection() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let m1 = ClosedTreeMetadata {
            tree_id: 1,
            origin_id: [0xaa; 32],
            root_hash: [0x11; 32],
            tree_size: 100,
            closed_at: 1_000_000_000_000,
            data_tree_index: 0,
        };
        ci.record_closed_tree(&m1).unwrap();

        let m2 = ClosedTreeMetadata {
            tree_id: 2,
            origin_id: [0xaa; 32],
            root_hash: [0x22; 32],
            tree_size: 200,
            closed_at: 2_000_000_000_000,
            data_tree_index: 999, // Wrong index (should be 1)
        };
        ci.record_closed_tree(&m2).unwrap();

        let result = ci.verify_full_chain().unwrap();
        assert!(!result.valid);
        assert_eq!(result.first_invalid_tree, Some(2));
    }

    #[test]
    fn test_get_tree_by_index() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let root1 = [0x11; 32];
        let root2 = [0x22; 32];

        let m1 = ClosedTreeMetadata {
            tree_id: 1,
            origin_id: [0xaa; 32],
            root_hash: root1,
            tree_size: 50,
            closed_at: 1_000_000_000_000,
            data_tree_index: 0,
        };
        ci.record_closed_tree(&m1).unwrap();

        let m2 = ClosedTreeMetadata {
            tree_id: 2,
            origin_id: [0xaa; 32],
            root_hash: root2,
            tree_size: 100,
            closed_at: 2_000_000_000_000,
            data_tree_index: 1,
        };
        ci.record_closed_tree(&m2).unwrap();

        let tree0 = ci.get_tree_by_index(0).unwrap().unwrap();
        assert_eq!(tree0.data_tree_index, 0);
        assert_eq!(tree0.root_hash, root1);

        let tree1 = ci.get_tree_by_index(1).unwrap().unwrap();
        assert_eq!(tree1.data_tree_index, 1);
        assert_eq!(tree1.root_hash, root2);
    }

    #[test]
    fn test_get_trees_ordered_returns_super_tree_order() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        for i in 0..5u64 {
            let metadata = ClosedTreeMetadata {
                tree_id: (i + 1) as i64,
                origin_id: [0xaa; 32],
                root_hash: [i as u8; 32],
                tree_size: (i + 1) * 100,
                closed_at: ((i + 1) * 1_000_000_000_000) as i64,
                data_tree_index: i,
            };
            ci.record_closed_tree(&metadata).unwrap();
        }

        let trees = ci.get_trees_ordered().unwrap();
        assert_eq!(trees.len(), 5);
        for (i, tree) in trees.iter().enumerate() {
            assert_eq!(tree.data_tree_index, i as u64);
        }
    }

    #[test]
    fn test_new_database_has_v2_schema() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let schema_version: String = ci
            .conn
            .query_row(
                "SELECT value FROM chain_config WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(schema_version, "2");
    }

    #[test]
    fn test_data_tree_index_is_unique() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let m1 = ClosedTreeMetadata {
            tree_id: 1,
            origin_id: [0xaa; 32],
            root_hash: [0x11; 32],
            tree_size: 100,
            closed_at: 1_000_000_000_000,
            data_tree_index: 0,
        };
        ci.record_closed_tree(&m1).unwrap();

        let m2 = ClosedTreeMetadata {
            tree_id: 2,
            origin_id: [0xbb; 32],
            root_hash: [0x22; 32],
            tree_size: 200,
            closed_at: 2_000_000_000_000,
            data_tree_index: 0, // Duplicate index
        };

        let result = ci.record_closed_tree(&m2);
        assert!(
            result.is_err(),
            "Should fail due to unique constraint on data_tree_index"
        );
    }

    #[test]
    fn test_get_tree_nonexistent() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let tree = ci.get_tree(999).unwrap();
        assert!(tree.is_none());
    }

    #[test]
    fn test_get_tree_by_index_nonexistent() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let tree = ci.get_tree_by_index(999).unwrap();
        assert!(tree.is_none());
    }

    #[test]
    fn test_get_first_tree_empty_db() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let tree = ci.get_first_tree().unwrap();
        assert!(tree.is_none());
    }

    #[test]
    fn test_get_first_tree_multiple_trees() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let m1 = ClosedTreeMetadata {
            tree_id: 1,
            origin_id: [0xaa; 32],
            root_hash: [0x11; 32],
            tree_size: 100,
            closed_at: 1_000_000_000_000,
            data_tree_index: 0,
        };
        ci.record_closed_tree(&m1).unwrap();

        let m2 = ClosedTreeMetadata {
            tree_id: 2,
            origin_id: [0xbb; 32],
            root_hash: [0x22; 32],
            tree_size: 200,
            closed_at: 2_000_000_000_000,
            data_tree_index: 1,
        };
        ci.record_closed_tree(&m2).unwrap();

        let first = ci.get_first_tree().unwrap().unwrap();
        assert_eq!(first.data_tree_index, 0);
        assert_eq!(first.tree_id, 1);
    }

    #[test]
    fn test_get_latest_tree_empty_db() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let tree = ci.get_latest_tree().unwrap();
        assert!(tree.is_none());
    }

    #[test]
    fn test_get_latest_tree_multiple_trees() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        for i in 0..5 {
            let metadata = ClosedTreeMetadata {
                tree_id: (i + 1) as i64,
                origin_id: [0xaa; 32],
                root_hash: [i as u8; 32],
                tree_size: (i + 1) * 100,
                closed_at: ((i + 1) * 1_000_000_000_000) as i64,
                data_tree_index: i,
            };
            ci.record_closed_tree(&metadata).unwrap();
        }

        let latest = ci.get_latest_tree().unwrap().unwrap();
        assert_eq!(latest.data_tree_index, 4);
        assert_eq!(latest.tree_id, 5);
    }

    #[test]
    fn test_count_trees_empty() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        assert_eq!(ci.count_trees().unwrap(), 0);
    }

    #[test]
    fn test_count_trees_multiple() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        for i in 0..7 {
            let metadata = ClosedTreeMetadata {
                tree_id: (i + 1) as i64,
                origin_id: [0xaa; 32],
                root_hash: [i as u8; 32],
                tree_size: (i + 1) * 50,
                closed_at: ((i + 1) * 1_000_000_000_000) as i64,
                data_tree_index: i,
            };
            ci.record_closed_tree(&metadata).unwrap();
        }

        assert_eq!(ci.count_trees().unwrap(), 7);
    }

    #[test]
    fn test_get_all_trees_empty() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let trees = ci.get_all_trees().unwrap();
        assert_eq!(trees.len(), 0);
    }

    #[test]
    fn test_get_all_trees_ordered_by_tree_id() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        // Insert in reverse order
        for i in (0..4).rev() {
            let metadata = ClosedTreeMetadata {
                tree_id: (i + 1) as i64,
                origin_id: [0xaa; 32],
                root_hash: [i as u8; 32],
                tree_size: (i + 1) * 100,
                closed_at: ((i + 1) * 1_000_000_000_000) as i64,
                data_tree_index: i,
            };
            ci.record_closed_tree(&metadata).unwrap();
        }

        let trees = ci.get_all_trees().unwrap();
        assert_eq!(trees.len(), 4);

        // Should be ordered by tree_id ASC
        for (i, tree) in trees.iter().enumerate() {
            assert_eq!(tree.tree_id, (i + 1) as i64);
        }
    }

    #[test]
    fn test_get_trees_by_status_empty() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let trees = ci.get_trees_by_status(ChainTreeStatus::Closed).unwrap();
        assert_eq!(trees.len(), 0);
    }

    #[test]
    fn test_get_trees_by_status_filters_correctly() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        // Record trees with different statuses
        let m1 = ClosedTreeMetadata {
            tree_id: 1,
            origin_id: [0xaa; 32],
            root_hash: [0x11; 32],
            tree_size: 100,
            closed_at: 1_000_000_000_000,
            data_tree_index: 0,
        };
        ci.record_closed_tree(&m1).unwrap();

        // Manually update status to test filtering
        ci.conn
            .execute("UPDATE trees SET status = 'archived' WHERE tree_id = 1", [])
            .unwrap();

        let m2 = ClosedTreeMetadata {
            tree_id: 2,
            origin_id: [0xbb; 32],
            root_hash: [0x22; 32],
            tree_size: 200,
            closed_at: 2_000_000_000_000,
            data_tree_index: 1,
        };
        ci.record_closed_tree(&m2).unwrap();

        let closed = ci.get_trees_by_status(ChainTreeStatus::Closed).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].tree_id, 2);

        let archived = ci.get_trees_by_status(ChainTreeStatus::Archived).unwrap();
        assert_eq!(archived.len(), 1);
        assert_eq!(archived[0].tree_id, 1);
    }

    #[test]
    fn test_set_bitcoin_txid() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let metadata = ClosedTreeMetadata {
            tree_id: 1,
            origin_id: [0xaa; 32],
            root_hash: [0x11; 32],
            tree_size: 100,
            closed_at: 1_000_000_000_000,
            data_tree_index: 0,
        };
        ci.record_closed_tree(&metadata).unwrap();

        let txid = "abcd1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab";
        ci.set_bitcoin_txid(1, txid).unwrap();

        let tree = ci.get_tree(1).unwrap().unwrap();
        assert_eq!(tree.bitcoin_txid, Some(txid.to_string()));
    }

    #[test]
    fn test_set_bitcoin_txid_nonexistent_tree() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        // Should not error, just no-op
        let result = ci.set_bitcoin_txid(999, "txid123");
        assert!(result.is_ok());
    }

    #[test]
    fn test_record_closed_tree_updates_on_conflict() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        let m1 = ClosedTreeMetadata {
            tree_id: 1,
            origin_id: [0xaa; 32],
            root_hash: [0x11; 32],
            tree_size: 100,
            closed_at: 1_000_000_000_000,
            data_tree_index: 0,
        };
        ci.record_closed_tree(&m1).unwrap();

        // Record again with updated values
        let m2 = ClosedTreeMetadata {
            tree_id: 1,
            origin_id: [0xbb; 32],
            root_hash: [0x22; 32],
            tree_size: 200,
            closed_at: 2_000_000_000_000,
            data_tree_index: 5, // Different index should work with ON CONFLICT
        };
        ci.record_closed_tree(&m2).unwrap();

        let tree = ci.get_tree(1).unwrap().unwrap();
        assert_eq!(tree.root_hash, [0x22; 32]);
        assert_eq!(tree.tree_size, 200);
        assert_eq!(tree.data_tree_index, 5);
    }

    #[test]
    fn test_record_closed_tree_with_timestamp_overflow() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        // Use i64::MAX which would overflow timestamp conversion
        let metadata = ClosedTreeMetadata {
            tree_id: 1,
            origin_id: [0xaa; 32],
            root_hash: [0x11; 32],
            tree_size: 100,
            closed_at: i64::MAX,
            data_tree_index: 0,
        };

        // Should fallback to current time
        ci.record_closed_tree(&metadata).unwrap();

        let tree = ci.get_tree(1).unwrap().unwrap();
        assert!(tree.closed_at.is_some());
    }

    #[test]
    fn test_chain_tree_status_as_str() {
        assert_eq!(ChainTreeStatus::Active.as_str(), "active");
        assert_eq!(ChainTreeStatus::Closed.as_str(), "closed");
        assert_eq!(ChainTreeStatus::Archived.as_str(), "archived");
    }

    #[test]
    fn test_chain_tree_status_parse() {
        assert_eq!(
            ChainTreeStatus::parse("active"),
            Some(ChainTreeStatus::Active)
        );
        assert_eq!(
            ChainTreeStatus::parse("closed"),
            Some(ChainTreeStatus::Closed)
        );
        assert_eq!(
            ChainTreeStatus::parse("archived"),
            Some(ChainTreeStatus::Archived)
        );
        assert_eq!(ChainTreeStatus::parse("invalid"), None);
        assert_eq!(ChainTreeStatus::parse(""), None);
    }

    #[test]
    fn test_row_to_chain_tree_invalid_origin_id_size() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        // Manually insert invalid data
        ci.conn
            .execute(
                "INSERT INTO trees (tree_id, origin_id, root_hash, tree_size, data_tree_index, status, created_at, closed_at)
                 VALUES (1, X'AA', X'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB', 100, 0, 'closed', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')",
                [],
            )
            .unwrap();

        let result = ci.get_tree(1);
        assert!(result.is_err(), "Should fail with invalid origin_id size");
    }

    #[test]
    fn test_row_to_chain_tree_invalid_root_hash_size() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        // Manually insert invalid data
        ci.conn
            .execute(
                "INSERT INTO trees (tree_id, origin_id, root_hash, tree_size, data_tree_index, status, created_at, closed_at)
                 VALUES (1, X'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', X'BB', 100, 0, 'closed', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')",
                [],
            )
            .unwrap();

        let result = ci.get_tree(1);
        assert!(result.is_err(), "Should fail with invalid root_hash size");
    }

    #[test]
    fn test_row_to_chain_tree_invalid_status() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        // Manually insert invalid status
        ci.conn
            .execute(
                "INSERT INTO trees (tree_id, origin_id, root_hash, tree_size, data_tree_index, status, created_at, closed_at)
                 VALUES (1, X'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', X'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB', 100, 0, 'invalid_status', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')",
                [],
            )
            .unwrap();

        let tree = ci.get_tree(1).unwrap().unwrap();
        // Should fallback to Closed
        assert_eq!(tree.status, ChainTreeStatus::Closed);
    }

    #[test]
    fn test_sync_with_main_db_empty() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        // Create empty index store
        use rusqlite::Connection;
        let conn = Connection::open_in_memory().unwrap();
        let index_store = crate::storage::index::IndexStore::from_connection(conn);
        index_store.initialize().unwrap();

        let synced = ci.sync_with_main_db(&index_store).unwrap();
        assert_eq!(synced, 0);
    }

    #[test]
    fn test_sync_with_main_db_derives_data_tree_index() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        // Create index store with closed trees
        use rusqlite::Connection;
        let conn = Connection::open_in_memory().unwrap();
        let mut index_store = crate::storage::index::IndexStore::from_connection(conn);
        index_store.initialize().unwrap();

        // Create and close trees
        let origin_id = [0xaa; 32];
        index_store.create_active_tree(&origin_id, 0).unwrap();
        let result1 = index_store
            .close_tree_and_create_new(&origin_id, 100, &[0x11; 32], 0)
            .unwrap();

        let result2 = index_store
            .close_tree_and_create_new(&origin_id, 200, &[0x22; 32], 1)
            .unwrap();

        let synced = ci.sync_with_main_db(&index_store).unwrap();
        assert_eq!(synced, 2);

        // Verify synced trees
        let tree1 = ci.get_tree(result1.closed_tree_id).unwrap().unwrap();
        assert_eq!(tree1.data_tree_index, 0);

        let tree2 = ci.get_tree(result2.closed_tree_id).unwrap().unwrap();
        assert_eq!(tree2.data_tree_index, 1);
    }

    #[test]
    fn test_sync_with_main_db_skips_existing() {
        let dir = tempdir().unwrap();
        let ci = ChainIndex::open(&dir.path().join("ci.db")).unwrap();

        // Create index store with closed tree
        use rusqlite::Connection;
        let conn = Connection::open_in_memory().unwrap();
        let mut index_store = crate::storage::index::IndexStore::from_connection(conn);
        index_store.initialize().unwrap();

        let origin_id = [0xaa; 32];
        index_store.create_active_tree(&origin_id, 0).unwrap();
        let result = index_store
            .close_tree_and_create_new(&origin_id, 100, &[0x11; 32], 0)
            .unwrap();

        // First sync
        let synced1 = ci.sync_with_main_db(&index_store).unwrap();
        assert_eq!(synced1, 1);

        // Second sync should skip existing
        let synced2 = ci.sync_with_main_db(&index_store).unwrap();
        assert_eq!(synced2, 0);

        // Verify tree still exists
        let tree = ci.get_tree(result.closed_tree_id).unwrap();
        assert!(tree.is_some());
    }

    #[test]
    fn test_open_existing_database_with_v1_schema() {
        use rusqlite::Connection;
        let dir = tempdir().unwrap();
        let path = dir.path().join("ci.db");

        // Create v1 schema manually
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE trees (
                    tree_id INTEGER PRIMARY KEY,
                    origin_id BLOB NOT NULL,
                    root_hash BLOB NOT NULL,
                    tree_size INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'closed',
                    bitcoin_txid TEXT,
                    archive_location TEXT,
                    created_at TEXT NOT NULL,
                    closed_at TEXT,
                    archived_at TEXT
                );
                CREATE TABLE chain_config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                INSERT INTO chain_config (key, value, updated_at) VALUES ('schema_version', '1', 0);",
            )
            .unwrap();
        }

        // Open should trigger migration
        let ci = ChainIndex::open(&path).unwrap();

        // Check schema version is now 2
        let version: String = ci
            .conn
            .query_row(
                "SELECT value FROM chain_config WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, "2");

        // Check data_tree_index column exists
        let column_exists = ci
            .conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('trees') WHERE name = 'data_tree_index'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(column_exists, 1);
    }

    #[test]
    fn test_open_existing_database_with_v2_schema() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ci.db");

        // Create first instance (v2)
        {
            let _ci = ChainIndex::open(&path).unwrap();
        }

        // Reopen should not migrate
        let ci = ChainIndex::open(&path).unwrap();

        let version: String = ci
            .conn
            .query_row(
                "SELECT value FROM chain_config WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, "2");
    }
}
