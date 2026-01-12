// File: src/storage/index/anchors.rs

//! Anchor management for IndexStore
//!
//! This module contains methods for managing anchors:
//! - Storing anchors (store_anchor, store_anchor_returning_id)
//! - Querying anchors (get_anchors, get_pending_ots_anchors, get_anchors_covering)
//! - Updating anchors (update_anchor_status, update_anchor_token, update_anchor_metadata)
//! - Atomic operations (submit_ots_anchor_atomic, confirm_ots_anchor_atomic)

use super::queries::IndexStore;
use crate::traits::{Anchor, AnchorType};
use rusqlite::{params, OptionalExtension};

/// Anchor with ID (for OTS poll job)
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AnchorWithId {
    pub id: i64,
    pub anchor: Anchor,
}

/// Convert database row to Anchor
fn row_to_anchor(row: &rusqlite::Row) -> rusqlite::Result<Anchor> {
    let _id: i64 = row.get(0)?;
    let tree_size: i64 = row.get(1)?;
    let anchor_type: String = row.get(2)?;
    let anchored_hash: Vec<u8> = row.get(3)?;
    let timestamp: i64 = row.get(4)?;
    let token: Vec<u8> = row.get(5)?;
    let metadata: Option<String> = row.get(6)?;

    let anchor_type = match anchor_type.as_str() {
        "rfc3161" => AnchorType::Rfc3161,
        "bitcoin_ots" => AnchorType::BitcoinOts,
        _ => AnchorType::Other,
    };

    Ok(Anchor {
        anchor_type,
        anchored_hash: anchored_hash.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                3,
                "anchored_hash".into(),
                rusqlite::types::Type::Blob,
            )
        })?,
        tree_size: tree_size as u64,
        timestamp: timestamp as u64,
        token,
        metadata: metadata
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default(),
    })
}

/// Convert database row to AnchorWithId
#[allow(dead_code)]
fn row_to_anchor_with_id(row: &rusqlite::Row) -> rusqlite::Result<AnchorWithId> {
    let id: i64 = row.get(0)?;
    let tree_size: i64 = row.get(1)?;
    let anchor_type: String = row.get(2)?;
    let anchored_hash: Vec<u8> = row.get(3)?;
    let timestamp: i64 = row.get(4)?;
    let token: Vec<u8> = row.get(5)?;
    let metadata: Option<String> = row.get(6)?;
    let _status: String = row.get(7)?;

    let anchor_type = match anchor_type.as_str() {
        "rfc3161" => AnchorType::Rfc3161,
        "bitcoin_ots" => AnchorType::BitcoinOts,
        _ => AnchorType::Other,
    };

    Ok(AnchorWithId {
        id,
        anchor: Anchor {
            anchor_type,
            anchored_hash: anchored_hash.try_into().map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    3,
                    "anchored_hash".into(),
                    rusqlite::types::Type::Blob,
                )
            })?,
            tree_size: tree_size as u64,
            timestamp: timestamp as u64,
            token,
            metadata: metadata
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
        },
    })
}

#[allow(dead_code)]
impl IndexStore {
    /// Store an external anchor for a tree size
    pub fn store_anchor(&self, tree_size: u64, anchor: &Anchor) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        self.connection().execute(
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

    /// Store anchor and return its ID
    pub fn store_anchor_returning_id(
        &self,
        tree_size: u64,
        anchor: &Anchor,
        status: &str,
    ) -> rusqlite::Result<i64> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        self.connection().execute(
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

        Ok(self.connection().last_insert_rowid())
    }

    /// Get all anchors for a tree size
    pub fn get_anchors(&self, tree_size: u64) -> rusqlite::Result<Vec<Anchor>> {
        let conn = self.connection();
        let mut stmt = conn.prepare(
            "SELECT id, tree_size, anchor_type, anchored_hash, timestamp, token, metadata
             FROM anchors WHERE tree_size = ?1",
        )?;

        let rows = stmt.query_map(params![tree_size as i64], row_to_anchor)?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    /// Get the most recent anchored tree size
    pub fn get_latest_anchored_size(&self) -> rusqlite::Result<Option<u64>> {
        let result =
            self.connection()
                .query_row("SELECT MAX(tree_size) FROM anchors", [], |row| {
                    row.get::<_, Option<i64>>(0)
                })?;

        Ok(result.map(|s| s as u64))
    }

    /// Get pending OTS anchors
    pub fn get_pending_ots_anchors(&self) -> rusqlite::Result<Vec<AnchorWithId>> {
        let conn = self.connection();
        let mut stmt = conn.prepare(
            "SELECT id, tree_size, anchor_type, anchored_hash, timestamp, token, metadata, status
             FROM anchors WHERE anchor_type = 'bitcoin_ots' AND status = 'pending'",
        )?;

        let rows = stmt.query_map([], row_to_anchor_with_id)?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    /// Update anchor status
    pub fn update_anchor_status(&self, anchor_id: i64, status: &str) -> rusqlite::Result<()> {
        self.connection().execute(
            "UPDATE anchors SET status = ?1 WHERE id = ?2",
            params![status, anchor_id],
        )?;
        Ok(())
    }

    /// Update anchor token (for OTS upgrade)
    pub fn update_anchor_token(&self, anchor_id: i64, token: &[u8]) -> rusqlite::Result<()> {
        self.connection().execute(
            "UPDATE anchors SET token = ?1 WHERE id = ?2",
            params![token, anchor_id],
        )?;
        Ok(())
    }

    /// Update anchor metadata
    pub fn update_anchor_metadata(
        &self,
        anchor_id: i64,
        metadata: serde_json::Value,
    ) -> rusqlite::Result<()> {
        self.connection().execute(
            "UPDATE anchors SET metadata = ?1 WHERE id = ?2",
            params![serde_json::to_string(&metadata).ok(), anchor_id],
        )?;
        Ok(())
    }

    /// Get anchors that cover a specific tree size
    ///
    /// Returns anchors where tree_size >= target_tree_size,
    /// ordered by tree_size ascending (closest first).
    /// Only returns 'confirmed' status anchors.
    /// For each anchor type, returns only the anchor with the minimum tree_size.
    pub fn get_anchors_covering(
        &self,
        target_tree_size: u64,
        limit: usize,
    ) -> rusqlite::Result<Vec<Anchor>> {
        let conn = self.connection();
        let mut stmt = conn.prepare(
            "SELECT a.id, a.tree_size, a.anchor_type, a.anchored_hash, a.timestamp, a.token, a.metadata
             FROM anchors a
             INNER JOIN (
                 SELECT anchor_type, MIN(tree_size) as min_tree_size
                 FROM anchors
                 WHERE tree_size >= ?1 AND status = 'confirmed'
                 GROUP BY anchor_type
             ) AS best ON a.anchor_type = best.anchor_type AND a.tree_size = best.min_tree_size
             WHERE a.status = 'confirmed'
             ORDER BY a.tree_size ASC
             LIMIT ?2",
        )?;

        let rows = stmt.query_map(
            params![target_tree_size as i64, limit as i64],
            row_to_anchor,
        )?;

        rows.collect::<Result<Vec<_>, _>>()
    }

    /// Create OTS anchor and link to tree atomically
    pub fn submit_ots_anchor_atomic(
        &mut self,
        tree_id: i64,
        proof: &[u8],
        calendar_url: &str,
        root_hash: &[u8; 32],
        tree_size: u64,
    ) -> rusqlite::Result<i64> {
        let mut conn = self.connection_mut();
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
    pub fn confirm_ots_anchor_atomic(
        &mut self,
        anchor_id: i64,
        upgraded_proof: &[u8],
        block_height: u64,
        block_time: u64,
    ) -> rusqlite::Result<()> {
        let mut conn = self.connection_mut();
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

    /// Get existing TSA anchor ID for a root hash (if any)
    pub fn get_tsa_anchor_for_hash(&self, root_hash: &[u8; 32]) -> rusqlite::Result<Option<i64>> {
        self.connection()
            .query_row(
                "SELECT id FROM anchors WHERE anchored_hash = ?1 AND anchor_type = 'rfc3161' LIMIT 1",
                [root_hash.as_slice()],
                |row| row.get(0),
            )
            .optional()
    }

    /// Get the latest anchored tree_size for TSA anchors (rfc3161)
    ///
    /// Returns the maximum tree_size that has been anchored via TSA.
    /// Used for periodic active tree anchoring.
    pub fn get_latest_tsa_anchored_size(&self) -> rusqlite::Result<Option<u64>> {
        let result = self.connection().query_row(
            "SELECT MAX(tree_size) FROM anchors WHERE anchor_type = 'rfc3161'",
            [],
            |row| row.get::<_, Option<i64>>(0),
        )?;

        Ok(result.map(|s| s as u64))
    }
}
