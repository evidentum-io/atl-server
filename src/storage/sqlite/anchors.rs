// File: src/storage/sqlite/anchors.rs

use super::convert::{row_to_anchor, row_to_anchor_with_id};
use super::lifecycle::AnchorWithId;
use super::store::SqliteStore;
use crate::error::ServerResult;
use crate::traits::Anchor;
use rusqlite::params;

impl SqliteStore {
    /// Store an external anchor for a tree size
    pub(crate) fn store_anchor_impl(&self, tree_size: u64, anchor: &Anchor) -> ServerResult<()> {
        let conn = self.get_conn()?;
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        conn.execute(
            "INSERT INTO anchors (tree_size, anchor_type, anchored_hash, timestamp, token, metadata, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                tree_size as i64,
                anchor.anchor_type.to_string(),
                anchor.anchored_hash.as_slice(),
                anchor.timestamp as i64,
                anchor.token.as_slice(),
                serde_json::to_string(&anchor.metadata).ok(),
                now,
            ],
        )?;

        Ok(())
    }

    /// Get all anchors for a tree size
    pub(crate) fn get_anchors_impl(&self, tree_size: u64) -> ServerResult<Vec<Anchor>> {
        let conn = self.get_conn()?;

        let mut stmt = conn.prepare(
            "SELECT id, tree_size, anchor_type, anchored_hash, timestamp, token, metadata
             FROM anchors WHERE tree_size = ?1",
        )?;

        let rows = stmt.query_map(params![tree_size as i64], row_to_anchor)?;

        rows.map(|r| r.map_err(|e| e.into())).collect()
    }

    /// Get the most recent anchored tree size
    pub(crate) fn get_latest_anchored_size_impl(&self) -> ServerResult<Option<u64>> {
        let conn = self.get_conn()?;

        let result = conn.query_row("SELECT MAX(tree_size) FROM anchors", [], |row| {
            row.get::<_, Option<i64>>(0)
        });

        match result {
            Ok(Some(size)) => Ok(Some(size as u64)),
            Ok(None) => Ok(None),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Store anchor and return its ID (used by BACKGROUND-1)
    #[allow(dead_code)]
    pub fn store_anchor_returning_id(
        &self,
        tree_size: u64,
        anchor: &Anchor,
        status: &str,
    ) -> ServerResult<i64> {
        let conn = self.get_conn()?;
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        conn.execute(
            "INSERT INTO anchors (tree_size, anchor_type, anchored_hash, timestamp, token, metadata, status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                tree_size as i64,
                anchor.anchor_type.to_string(),
                anchor.anchored_hash.as_slice(),
                anchor.timestamp as i64,
                anchor.token.as_slice(),
                serde_json::to_string(&anchor.metadata).ok(),
                status,
                now,
            ],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// Get pending OTS anchors (used by BACKGROUND-1)
    #[allow(dead_code)]
    pub fn get_pending_ots_anchors(&self) -> ServerResult<Vec<AnchorWithId>> {
        let conn = self.get_conn()?;

        let mut stmt = conn.prepare(
            "SELECT id, tree_size, anchor_type, anchored_hash, timestamp, token, metadata, status
             FROM anchors WHERE anchor_type = 'bitcoin_ots' AND status = 'pending'",
        )?;

        let rows = stmt.query_map([], row_to_anchor_with_id)?;

        rows.map(|r| r.map_err(|e| e.into())).collect()
    }

    /// Update anchor status (used by BACKGROUND-1)
    #[allow(dead_code)]
    pub fn update_anchor_status(&self, anchor_id: i64, status: &str) -> ServerResult<()> {
        let conn = self.get_conn()?;
        conn.execute(
            "UPDATE anchors SET status = ?1 WHERE id = ?2",
            params![status, anchor_id],
        )?;
        Ok(())
    }

    /// Update anchor token (for OTS upgrade, used by BACKGROUND-1)
    #[allow(dead_code)]
    pub fn update_anchor_token(&self, anchor_id: i64, token: &[u8]) -> ServerResult<()> {
        let conn = self.get_conn()?;
        conn.execute(
            "UPDATE anchors SET token = ?1 WHERE id = ?2",
            params![token, anchor_id],
        )?;
        Ok(())
    }

    /// Update anchor metadata (used by BACKGROUND-1)
    #[allow(dead_code)]
    pub fn update_anchor_metadata(
        &self,
        anchor_id: i64,
        metadata: serde_json::Value,
    ) -> ServerResult<()> {
        let conn = self.get_conn()?;
        conn.execute(
            "UPDATE anchors SET metadata = ?1 WHERE id = ?2",
            params![serde_json::to_string(&metadata).ok(), anchor_id],
        )?;
        Ok(())
    }
}
