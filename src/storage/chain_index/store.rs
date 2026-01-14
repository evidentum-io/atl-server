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
    pub prev_tree_id: Option<i64>,
    pub data_tree_index: Option<u64>,
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

        conn.execute_batch(super::schema::CHAIN_INDEX_SCHEMA)?;

        conn.execute(
            "INSERT OR IGNORE INTO chain_config (key, value, updated_at) VALUES ('schema_version', ?1, ?2)",
            params![
                super::schema::CHAIN_INDEX_SCHEMA_VERSION.to_string(),
                chrono::Utc::now().timestamp()
            ],
        )?;

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
                prev_tree_id, data_tree_index, status,
                created_at, closed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'closed', ?7, ?7)
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
                metadata.prev_tree_id,
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
                "SELECT tree_id, origin_id, root_hash, tree_size, prev_tree_id, data_tree_index,
                        status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
                 FROM trees WHERE tree_id = ?1",
                params![tree_id],
                row_to_chain_tree,
            )
            .optional()
    }

    /// Get all trees in chain order (oldest first)
    #[allow(dead_code)]
    pub fn get_all_trees(&self) -> rusqlite::Result<Vec<ChainTreeRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT tree_id, origin_id, root_hash, tree_size, prev_tree_id, data_tree_index,
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
            "SELECT tree_id, origin_id, root_hash, tree_size, prev_tree_id, data_tree_index,
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

    /// Get first tree (the one with prev_tree_id = NULL)
    #[allow(dead_code)]
    pub fn get_first_tree(&self) -> rusqlite::Result<Option<ChainTreeRecord>> {
        self.conn
            .query_row(
                "SELECT tree_id, origin_id, root_hash, tree_size, prev_tree_id, data_tree_index,
                        status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
                 FROM trees WHERE prev_tree_id IS NULL ORDER BY tree_id ASC LIMIT 1",
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
                "SELECT tree_id, origin_id, root_hash, tree_size, prev_tree_id, data_tree_index,
                        status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
                 FROM trees ORDER BY tree_id DESC LIMIT 1",
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
                    prev_tree_id: tree.prev_tree_id,
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
    let prev_tree_id: Option<i64> = row.get(4)?;
    let data_tree_index: Option<i64> = row.get(5)?;
    let status: String = row.get(6)?;
    let bitcoin_txid: Option<String> = row.get(7)?;
    let archive_location: Option<String> = row.get(8)?;
    let created_at: String = row.get(9)?;
    let closed_at: Option<String> = row.get(10)?;
    let archived_at: Option<String> = row.get(11)?;

    Ok(ChainTreeRecord {
        tree_id,
        origin_id: origin_id.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(1, "origin_id".into(), rusqlite::types::Type::Blob)
        })?,
        root_hash: root_hash.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(2, "root_hash".into(), rusqlite::types::Type::Blob)
        })?,
        tree_size: tree_size as u64,
        prev_tree_id,
        data_tree_index: data_tree_index.map(|i| i as u64),
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
            prev_tree_id: None,
            closed_at: 1_234_567_890_000_000_000,
            data_tree_index: 0,
        };

        ci.record_closed_tree(&metadata).unwrap();

        let tree = ci.get_tree(1).unwrap().unwrap();
        assert_eq!(tree.tree_id, 1);
        assert_eq!(tree.tree_size, 100);
        assert_eq!(tree.prev_tree_id, None);
        assert_eq!(tree.data_tree_index, Some(0));
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
            prev_tree_id: None,
            closed_at: 1_000_000_000_000,
            data_tree_index: 0,
        };
        ci.record_closed_tree(&m1).unwrap();

        let m2 = ClosedTreeMetadata {
            tree_id: 2,
            origin_id: [0xaa; 32],
            root_hash: [0x22; 32],
            tree_size: 200,
            prev_tree_id: Some(1),
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
            prev_tree_id: None,
            closed_at: 1_000_000_000_000,
            data_tree_index: 0,
        };
        ci.record_closed_tree(&m1).unwrap();

        let m2 = ClosedTreeMetadata {
            tree_id: 2,
            origin_id: [0xaa; 32],
            root_hash: [0x22; 32],
            tree_size: 200,
            prev_tree_id: Some(1),
            closed_at: 2_000_000_000_000,
            data_tree_index: 999, // Wrong index (should be 1)
        };
        ci.record_closed_tree(&m2).unwrap();

        let result = ci.verify_full_chain().unwrap();
        assert!(!result.valid);
        assert_eq!(result.first_invalid_tree, Some(2));
    }
}
