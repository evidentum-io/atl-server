// File: src/storage/sqlite/lifecycle.rs

use super::convert;
use super::store::SqliteStore;
use crate::error::{ServerError, ServerResult};
use crate::traits::Anchor;
use rusqlite::params;

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

/// Anchor with ID (for OTS poll job)
#[derive(Debug, Clone)]
pub struct AnchorWithId {
    pub id: i64,
    pub anchor: Anchor,
}

impl SqliteStore {
    /// Private helper: queries active tree using existing connection
    fn get_active_tree_impl(
        &self,
        conn: &rusqlite::Connection,
    ) -> ServerResult<Option<TreeRecord>> {
        let result = conn.query_row(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at, first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
             FROM trees WHERE status = 'active' LIMIT 1",
            [],
            convert::row_to_tree,
        );

        match result {
            Ok(tree) => Ok(Some(tree)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get the currently active tree
    pub fn get_active_tree(&self) -> ServerResult<Option<TreeRecord>> {
        let conn = self.get_conn()?;
        self.get_active_tree_impl(&conn)
    }

    /// Create a new active tree
    pub fn create_active_tree(&self, start_size: u64) -> ServerResult<i64> {
        let conn = self.get_conn()?;
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        conn.execute(
            "INSERT INTO trees (origin_id, status, start_size, created_at)
             VALUES (?1, 'active', ?2, ?3)",
            params![self.get_origin().as_slice(), start_size as i64, now],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// Close the active tree and create a new one atomically
    ///
    /// The tree is marked as 'pending_bitcoin' with bitcoin_anchor_id = NULL.
    /// The ots_job will create the anchor and set the bitcoin_anchor_id later.
    pub fn close_tree_and_create_new(
        &self,
        end_size: u64,
        root_hash: &[u8; 32],
    ) -> ServerResult<(i64, i64)> {
        let mut conn = self.get_conn()?;
        let tx = conn.transaction()?;
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // Get current active tree ID
        let active_tree_id: i64 = tx
            .query_row("SELECT id FROM trees WHERE status = 'active'", [], |row| {
                row.get(0)
            })
            .map_err(|_| ServerError::Internal("No active tree found".into()))?;

        // Update active tree to pending_bitcoin (bitcoin_anchor_id will be set by ots_job)
        tx.execute(
            "UPDATE trees SET status = 'pending_bitcoin', end_size = ?1, root_hash = ?2, closed_at = ?3
             WHERE id = ?4",
            params![end_size as i64, root_hash.as_slice(), now, active_tree_id,],
        )?;

        // Create new active tree
        tx.execute(
            "INSERT INTO trees (origin_id, status, start_size, created_at)
             VALUES (?1, 'active', ?2, ?3)",
            params![self.get_origin().as_slice(), end_size as i64, now],
        )?;

        let new_tree_id = tx.last_insert_rowid();

        tx.commit()?;

        Ok((active_tree_id, new_tree_id))
    }

    /// Set bitcoin anchor for a tree (called by ots_job after anchor creation)
    pub fn set_tree_bitcoin_anchor(&self, tree_id: i64, anchor_id: i64) -> ServerResult<()> {
        let conn = self.get_conn()?;
        conn.execute(
            "UPDATE trees SET bitcoin_anchor_id = ?1 WHERE id = ?2 AND status = 'pending_bitcoin'",
            params![anchor_id, tree_id],
        )?;
        Ok(())
    }

    /// Get trees pending TSA anchoring
    pub fn get_trees_pending_tsa(&self) -> ServerResult<Vec<TreeRecord>> {
        let conn = self.get_conn()?;

        let mut stmt = conn.prepare(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at, first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
             FROM trees WHERE status IN ('pending_bitcoin', 'closed') AND tsa_anchor_id IS NULL ORDER BY created_at ASC"
        )?;

        let rows = stmt.query_map([], convert::row_to_tree)?;
        rows.map(|r| r.map_err(|e| e.into())).collect()
    }

    /// Get trees pending Bitcoin confirmation
    pub fn get_trees_pending_bitcoin_confirmation(&self) -> ServerResult<Vec<TreeRecord>> {
        let conn = self.get_conn()?;

        let mut stmt = conn.prepare(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at, first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
             FROM trees WHERE status = 'pending_bitcoin' ORDER BY created_at ASC"
        )?;

        let rows = stmt.query_map([], convert::row_to_tree)?;
        rows.map(|r| r.map_err(|e| e.into())).collect()
    }

    /// Mark tree as closed after Bitcoin confirmation
    pub fn mark_tree_closed(&self, tree_id: i64) -> ServerResult<()> {
        let conn = self.get_conn()?;
        conn.execute(
            "UPDATE trees SET status = 'closed' WHERE id = ?1 AND status = 'pending_bitcoin'",
            params![tree_id],
        )?;
        Ok(())
    }

    /// Update tree TSA anchor
    pub fn update_tree_tsa_anchor(&self, tree_id: i64, anchor_id: i64) -> ServerResult<()> {
        let conn = self.get_conn()?;
        conn.execute(
            "UPDATE trees SET tsa_anchor_id = ?1 WHERE id = ?2",
            params![anchor_id, tree_id],
        )?;
        Ok(())
    }

    /// Get tree covering a specific entry
    pub fn get_tree_covering_entry(&self, leaf_index: u64) -> ServerResult<Option<TreeRecord>> {
        let conn = self.get_conn()?;

        let result = conn.query_row(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at, first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
             FROM trees WHERE start_size <= ?1 AND end_size > ?1 AND status IN ('pending_bitcoin', 'closed') LIMIT 1",
            params![leaf_index as i64],
            convert::row_to_tree,
        );

        match result {
            Ok(tree) => Ok(Some(tree)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get tree by bitcoin anchor ID
    pub fn get_tree_by_bitcoin_anchor_id(
        &self,
        anchor_id: i64,
    ) -> ServerResult<Option<TreeRecord>> {
        let conn = self.get_conn()?;

        let result = conn.query_row(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at, first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
             FROM trees WHERE bitcoin_anchor_id = ?1 LIMIT 1",
            params![anchor_id],
            convert::row_to_tree,
        );

        match result {
            Ok(tree) => Ok(Some(tree)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Update tree first_entry_at (called on first entry append)
    pub fn update_tree_first_entry_at(&self, tree_id: i64) -> ServerResult<()> {
        let conn = self.get_conn()?;
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        conn.execute(
            "UPDATE trees SET first_entry_at = ?1 WHERE id = ?2 AND first_entry_at IS NULL",
            params![now, tree_id],
        )?;

        Ok(())
    }

    /// Check if active tree needs TSA anchoring
    ///
    /// Returns the active tree if it exists AND:
    /// - Tree has grown (current size > last anchored size), AND
    /// - Enough time has passed since last anchor (elapsed >= interval_secs)
    ///
    /// Special cases:
    /// - No anchors at all → needs anchor (if tree not empty)
    /// - Tree empty (size 0) → no anchor needed
    /// - Tree hasn't grown since last anchor → no anchor needed
    pub fn get_active_tree_needing_tsa(
        &self,
        interval_secs: u64,
    ) -> ServerResult<Option<TreeRecord>> {
        let conn = self.get_conn()?;

        // First check if active tree exists
        let active_tree = match self.get_active_tree_impl(&conn)? {
            Some(tree) => tree,
            None => return Ok(None),
        };

        // Get local entry count for this tree (active tree has no checkpoints yet)
        let local_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM entries WHERE tree_id = ?1",
                params![active_tree.id],
                |row| row.get(0),
            )
            .unwrap_or(0);

        // If no new entries in this tree, no anchor needed
        if local_count == 0 {
            return Ok(None);
        }

        // Calculate GLOBAL tree size (start_size + local entries)
        let current_tree_size = active_tree.start_size as i64 + local_count;

        // Get last anchor info (tree_size and time)
        let last_anchor: Option<(i64, i64)> = conn
            .query_row(
                "SELECT tree_size, created_at FROM anchors WHERE anchor_type = 'rfc3161' ORDER BY tree_size DESC, created_at DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        let now = chrono::Utc::now().timestamp();

        let needs_anchor = match last_anchor {
            None => true,                                                    // No anchors at all
            Some((last_size, _)) if current_tree_size <= last_size => false, // Tree hasn't grown
            Some((_, last_time)) => {
                // Tree has grown, check if enough time passed
                let elapsed = now - (last_time / 1_000_000_000);
                elapsed >= interval_secs as i64
            }
        };

        if needs_anchor {
            Ok(Some(active_tree))
        } else {
            Ok(None)
        }
    }

    /// Get trees that need OTS submission (pending_bitcoin but no anchor)
    pub fn get_trees_without_bitcoin_anchor(&self) -> ServerResult<Vec<TreeRecord>> {
        let conn = self.get_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, origin_id, status, start_size, end_size, root_hash, created_at,
                    first_entry_at, closed_at, tsa_anchor_id, bitcoin_anchor_id
             FROM trees
             WHERE status = 'pending_bitcoin' AND bitcoin_anchor_id IS NULL
             ORDER BY closed_at ASC",
        )?;

        let rows = stmt.query_map([], convert::row_to_tree)?;
        rows.map(|r| r.map_err(|e| e.into())).collect()
    }

    /// Create OTS anchor and link to tree atomically
    pub fn submit_ots_anchor_atomic(
        &self,
        tree_id: i64,
        proof: &[u8],
        calendar_url: &str,
        root_hash: &[u8; 32],
        tree_size: u64,
    ) -> ServerResult<i64> {
        let mut conn = self.get_conn()?;
        let tx = conn.transaction()?;
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // Insert anchor
        tx.execute(
            "INSERT INTO anchors (anchor_type, anchored_hash, tree_size, timestamp, token, metadata, status, created_at)
             VALUES ('bitcoin_ots', ?1, ?2, ?3, ?4, ?5, 'pending', ?6)",
            params![
                root_hash.as_slice(),
                tree_size as i64,
                now,
                proof,
                serde_json::json!({"calendar_url": calendar_url}).to_string(),
                now,
            ],
        )?;
        let anchor_id = tx.last_insert_rowid();

        // Link to tree
        tx.execute(
            "UPDATE trees SET bitcoin_anchor_id = ?1 WHERE id = ?2",
            params![anchor_id, tree_id],
        )?;

        tx.commit()?;
        Ok(anchor_id)
    }

    /// Confirm OTS anchor and update tree status atomically
    ///
    /// All updates in ONE transaction
    pub fn confirm_ots_anchor_atomic(
        &self,
        anchor_id: i64,
        upgraded_proof: &[u8],
        block_height: u64,
        block_time: u64,
    ) -> ServerResult<()> {
        let mut conn = self.get_conn()?;
        let tx = conn.transaction()?;

        // Update anchor token
        tx.execute(
            "UPDATE anchors SET token = ?1 WHERE id = ?2",
            params![upgraded_proof, anchor_id],
        )?;

        // Update anchor status
        tx.execute(
            "UPDATE anchors SET status = 'confirmed' WHERE id = ?1",
            params![anchor_id],
        )?;

        // Update anchor metadata with Bitcoin block info
        tx.execute(
            "UPDATE anchors SET metadata = json_set(metadata,
                '$.bitcoin_block_height', ?1,
                '$.bitcoin_block_time', ?2,
                '$.status', 'confirmed')
             WHERE id = ?3",
            params![block_height as i64, block_time as i64, anchor_id],
        )?;

        // Update tree status
        tx.execute(
            "UPDATE trees SET status = 'closed'
             WHERE bitcoin_anchor_id = ?1 AND status = 'pending_bitcoin'",
            params![anchor_id],
        )?;

        tx.commit()?;
        Ok(())
    }
}
