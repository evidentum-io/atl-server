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

/// Tree record from database
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeStatus {
    Active,
    PendingBitcoin,
    Closed,
}

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
    pub fn create_active_tree(&self, origin_id: &[u8; 32], start_size: u64) -> rusqlite::Result<i64> {
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
    /// The tree is marked as 'pending_bitcoin' with bitcoin_anchor_id = NULL.
    /// The ots_job will create the anchor and set the bitcoin_anchor_id later.
    ///
    /// Returns (closed_tree_id, new_tree_id).
    pub fn close_tree_and_create_new(
        &mut self,
        origin_id: &[u8; 32],
        end_size: u64,
        root_hash: &[u8; 32],
    ) -> rusqlite::Result<(i64, i64)> {
        let tx = self.connection().transaction()?;
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // Get current active tree ID
        let active_tree_id: i64 = tx
            .query_row("SELECT id FROM trees WHERE status = 'active'", [], |row| {
                row.get(0)
            })?;

        // Update active tree to pending_bitcoin (bitcoin_anchor_id will be set by ots_job)
        tx.execute(
            "UPDATE trees SET status = 'pending_bitcoin', end_size = ?1, root_hash = ?2, closed_at = ?3
             WHERE id = ?4",
            params![end_size as i64, root_hash.as_slice(), now, active_tree_id],
        )?;

        // Create new active tree
        tx.execute(
            "INSERT INTO trees (origin_id, status, start_size, created_at)
             VALUES (?1, 'active', ?2, ?3)",
            params![origin_id.as_slice(), end_size as i64, now],
        )?;

        let new_tree_id = tx.last_insert_rowid();

        tx.commit()?;

        Ok((active_tree_id, new_tree_id))
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
        let mut stmt = self.connection().prepare(
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
    pub fn get_trees_pending_bitcoin_confirmation(&self) -> rusqlite::Result<Vec<TreeRecord>> {
        let mut stmt = self.connection().prepare(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                    first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
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
    pub fn get_tree_by_bitcoin_anchor_id(&self, anchor_id: i64) -> rusqlite::Result<Option<TreeRecord>> {
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
        let mut stmt = self.connection().prepare(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                    first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
             FROM trees
             WHERE status = 'pending_bitcoin' AND bitcoin_anchor_id IS NULL
             ORDER BY closed_at ASC",
        )?;

        let rows = stmt.query_map([], row_to_tree)?;
        rows.collect::<Result<Vec<_>, _>>()
    }
}
