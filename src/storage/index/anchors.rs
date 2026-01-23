// File: src/storage/index/anchors.rs

//! Anchor management for IndexStore
//!
//! This module contains methods for managing anchors:
//! - Storing anchors (store_anchor, store_anchor_returning_id)
//! - Querying anchors (get_anchors, get_pending_ots_anchors, get_anchors_covering)
//! - Updating anchors (update_anchor_status, update_anchor_token, update_anchor_metadata)
//! - Atomic operations (confirm_ots_anchor_atomic)

use super::queries::IndexStore;
use crate::traits::{Anchor, AnchorType};
use rusqlite::{params, OptionalExtension};

/// Anchor with ID (for OTS poll job)
#[derive(Debug, Clone)]
pub struct AnchorWithId {
    pub id: i64,
    pub anchor: Anchor,
}

/// Convert database row to Anchor
fn row_to_anchor(row: &rusqlite::Row) -> rusqlite::Result<Anchor> {
    let _id: i64 = row.get(0)?;
    let tree_size: Option<i64> = row.get(1)?;
    let anchor_type: String = row.get(2)?;
    let target: String = row.get(3)?;
    let anchored_hash: Vec<u8> = row.get(4)?;
    let super_tree_size: Option<i64> = row.get(5)?;
    let timestamp: i64 = row.get(6)?;
    let token: Vec<u8> = row.get(7)?;
    let metadata: Option<String> = row.get(8)?;

    let anchor_type = match anchor_type.as_str() {
        "rfc3161" => AnchorType::Rfc3161,
        "bitcoin_ots" => AnchorType::BitcoinOts,
        _ => AnchorType::Other,
    };

    Ok(Anchor {
        anchor_type,
        target,
        anchored_hash: anchored_hash.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                4,
                "anchored_hash".into(),
                rusqlite::types::Type::Blob,
            )
        })?,
        tree_size: tree_size.map(|s| s as u64).unwrap_or(0),
        super_tree_size: super_tree_size.map(|s| s as u64),
        timestamp: timestamp as u64,
        token,
        metadata: metadata
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default(),
    })
}

/// Convert database row to AnchorWithId
fn row_to_anchor_with_id(row: &rusqlite::Row) -> rusqlite::Result<AnchorWithId> {
    let id: i64 = row.get(0)?;
    let tree_size: Option<i64> = row.get(1)?;
    let anchor_type: String = row.get(2)?;
    let target: String = row.get(3)?;
    let anchored_hash: Vec<u8> = row.get(4)?;
    let super_tree_size: Option<i64> = row.get(5)?;
    let timestamp: i64 = row.get(6)?;
    let token: Vec<u8> = row.get(7)?;
    let metadata: Option<String> = row.get(8)?;
    let _status: String = row.get(9)?;

    let anchor_type = match anchor_type.as_str() {
        "rfc3161" => AnchorType::Rfc3161,
        "bitcoin_ots" => AnchorType::BitcoinOts,
        _ => AnchorType::Other,
    };

    Ok(AnchorWithId {
        id,
        anchor: Anchor {
            anchor_type,
            target,
            anchored_hash: anchored_hash.try_into().map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    4,
                    "anchored_hash".into(),
                    rusqlite::types::Type::Blob,
                )
            })?,
            tree_size: tree_size.map(|s| s as u64).unwrap_or(0),
            super_tree_size: super_tree_size.map(|s| s as u64),
            timestamp: timestamp as u64,
            token,
            metadata: metadata
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
        },
    })
}

impl IndexStore {
    /// Store an external anchor for a tree size
    #[allow(dead_code)]
    pub fn store_anchor(&self, tree_size: u64, anchor: &Anchor) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        self.connection().execute(
            "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash, super_tree_size, timestamp, token, metadata, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                Some(tree_size as i64),
                anchor.anchor_type.to_string(),
                anchor.target,
                anchor.anchored_hash.as_slice(),
                anchor.super_tree_size.map(|s| s as i64),
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
            "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash, super_tree_size, timestamp, token, metadata, status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                Some(tree_size as i64),
                anchor.anchor_type.to_string(),
                anchor.target,
                anchor.anchored_hash.as_slice(),
                anchor.super_tree_size.map(|s| s as i64),
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
            "SELECT id, tree_size, anchor_type, target, anchored_hash, super_tree_size, timestamp, token, metadata
             FROM anchors WHERE tree_size = ?1 OR (target = 'super_root' AND status = 'confirmed')",
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
            "SELECT id, tree_size, anchor_type, target, anchored_hash, super_tree_size, timestamp, token, metadata, status
             FROM anchors WHERE anchor_type = 'bitcoin_ots' AND status = 'pending'",
        )?;

        let rows = stmt.query_map([], row_to_anchor_with_id)?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    /// Update anchor status
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
            "SELECT a.id, a.tree_size, a.anchor_type, a.target, a.anchored_hash, a.super_tree_size, a.timestamp, a.token, a.metadata
             FROM anchors a
             INNER JOIN (
                 SELECT anchor_type, MIN(tree_size) as min_tree_size
                 FROM anchors
                 WHERE (tree_size >= ?1 OR target = 'super_root') AND status = 'confirmed'
                 GROUP BY anchor_type
             ) AS best ON a.anchor_type = best.anchor_type AND (a.tree_size = best.min_tree_size OR (a.tree_size IS NULL AND best.min_tree_size IS NULL))
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

    /// Create OTS anchor for Super Root (v2.0)
    ///
    /// This method creates an OTS anchor that targets the Super Root instead of
    /// a Data Tree root. Used for batch anchoring multiple trees at once.
    pub fn submit_super_root_ots_anchor(
        &mut self,
        proof: &[u8],
        calendar_url: &str,
        super_root: &[u8; 32],
        super_tree_size: u64,
    ) -> rusqlite::Result<i64> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // Insert anchor (v2.0 style - targets super_root)
        self.connection().execute(
            "INSERT INTO anchors (anchor_type, target, anchored_hash, tree_size, super_tree_size, timestamp, token, metadata, status, created_at)
             VALUES ('bitcoin_ots', 'super_root', ?1, NULL, ?2, ?3, ?4, ?5, 'pending', ?6)",
            params![
                super_root.as_slice(),
                super_tree_size as i64,
                now,
                proof,
                serde_json::json!({"calendar_url": calendar_url}).to_string(),
                now,
            ],
        )?;

        Ok(self.connection().last_insert_rowid())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{Anchor, AnchorType};
    use rusqlite::Connection;

    /// Helper: create in-memory IndexStore for testing
    fn create_test_store() -> IndexStore {
        let conn = Connection::open_in_memory().expect("Failed to create in-memory DB");
        let store = IndexStore::from_connection(conn);
        store.initialize().expect("Failed to initialize schema");
        store
    }

    /// Helper: create test Anchor (RFC3161)
    fn create_test_anchor_rfc3161() -> Anchor {
        Anchor {
            anchor_type: AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: [1u8; 32],
            tree_size: 100,
            super_tree_size: None,
            timestamp: 1_234_567_890_000_000_000,
            token: vec![0xDE, 0xAD, 0xBE, 0xEF],
            metadata: serde_json::json!({"tsa_url": "https://example.com"}),
        }
    }

    /// Helper: create test Anchor (Bitcoin OTS)
    fn create_test_anchor_ots() -> Anchor {
        Anchor {
            anchor_type: AnchorType::BitcoinOts,
            target: "data_tree_root".to_string(),
            anchored_hash: [2u8; 32],
            tree_size: 200,
            super_tree_size: Some(50),
            timestamp: 1_234_567_890_000_000_000,
            token: vec![0xCA, 0xFE, 0xBA, 0xBE],
            metadata: serde_json::json!({"calendar_url": "https://ots.example.com"}),
        }
    }

    /// Helper: create test Anchor (Other type)
    fn create_test_anchor_other() -> Anchor {
        Anchor {
            anchor_type: AnchorType::Other,
            target: "data_tree_root".to_string(),
            anchored_hash: [3u8; 32],
            tree_size: 300,
            super_tree_size: None,
            timestamp: 1_234_567_890_000_000_000,
            token: vec![0x12, 0x34],
            metadata: serde_json::json!({}),
        }
    }

    #[test]
    fn test_store_anchor_success() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();

        let result = store.store_anchor(100, &anchor);
        assert!(result.is_ok(), "Failed to store anchor: {:?}", result.err());

        // Verify anchor was stored
        let anchors = store.get_anchors(100).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].anchor_type, AnchorType::Rfc3161);
        assert_eq!(anchors[0].tree_size, 100);
        assert_eq!(anchors[0].anchored_hash, [1u8; 32]);
    }

    #[test]
    fn test_store_anchor_returning_id_success() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();

        let id = store
            .store_anchor_returning_id(100, &anchor, "pending")
            .expect("Failed to store anchor");
        assert!(id > 0, "Expected positive anchor ID");

        // Verify anchor was stored with correct status
        let anchors = store
            .get_pending_ots_anchors()
            .expect("Failed to get pending anchors");
        // This anchor is RFC3161, so it shouldn't appear in pending OTS
        assert_eq!(anchors.len(), 0);
    }

    #[test]
    fn test_store_anchor_returning_id_ots_pending() {
        let store = create_test_store();
        let anchor = create_test_anchor_ots();

        let id = store
            .store_anchor_returning_id(200, &anchor, "pending")
            .expect("Failed to store OTS anchor");
        assert!(id > 0, "Expected positive anchor ID");

        // Verify it appears in pending OTS anchors
        let pending = store
            .get_pending_ots_anchors()
            .expect("Failed to get pending anchors");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id);
        assert_eq!(pending[0].anchor.anchor_type, AnchorType::BitcoinOts);
    }

    #[test]
    fn test_get_anchors_empty() {
        let store = create_test_store();
        let anchors = store.get_anchors(100).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 0);
    }

    #[test]
    fn test_get_anchors_multiple() {
        let store = create_test_store();

        let anchor1 = create_test_anchor_rfc3161();
        let anchor2 = create_test_anchor_ots();

        store
            .store_anchor(100, &anchor1)
            .expect("Failed to store anchor1");
        store
            .store_anchor(100, &anchor2)
            .expect("Failed to store anchor2");

        let anchors = store.get_anchors(100).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 2);
    }

    #[test]
    fn test_get_anchors_with_super_root() {
        let store = create_test_store();

        // Store a super_root anchor with confirmed status
        let mut super_anchor = create_test_anchor_ots();
        super_anchor.target = "super_root".to_string();
        store
            .store_anchor_returning_id(50, &super_anchor, "confirmed")
            .expect("Failed to store super_root anchor");

        // Store a regular anchor
        let anchor = create_test_anchor_rfc3161();
        store
            .store_anchor(100, &anchor)
            .expect("Failed to store regular anchor");

        // get_anchors should return both
        let anchors = store.get_anchors(100).expect("Failed to get anchors");
        assert!(anchors.len() >= 2);
    }

    #[test]
    fn test_get_latest_anchored_size_empty() {
        let store = create_test_store();
        let size = store
            .get_latest_anchored_size()
            .expect("Failed to get latest size");
        assert_eq!(size, None);
    }

    #[test]
    fn test_get_latest_anchored_size_with_anchors() {
        let store = create_test_store();

        let anchor1 = create_test_anchor_rfc3161();
        let anchor2 = create_test_anchor_ots();

        store
            .store_anchor(100, &anchor1)
            .expect("Failed to store anchor1");
        store
            .store_anchor(200, &anchor2)
            .expect("Failed to store anchor2");

        let size = store
            .get_latest_anchored_size()
            .expect("Failed to get latest size");
        assert_eq!(size, Some(200));
    }

    #[test]
    fn test_get_pending_ots_anchors_empty() {
        let store = create_test_store();
        let pending = store
            .get_pending_ots_anchors()
            .expect("Failed to get pending anchors");
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn test_get_pending_ots_anchors_filters_by_type_and_status() {
        let store = create_test_store();

        let ots_anchor = create_test_anchor_ots();
        let rfc_anchor = create_test_anchor_rfc3161();

        // Store OTS as pending
        store
            .store_anchor_returning_id(200, &ots_anchor, "pending")
            .expect("Failed to store OTS anchor");

        // Store OTS as confirmed
        store
            .store_anchor_returning_id(201, &ots_anchor, "confirmed")
            .expect("Failed to store confirmed OTS anchor");

        // Store RFC3161 as pending
        store
            .store_anchor_returning_id(100, &rfc_anchor, "pending")
            .expect("Failed to store RFC3161 anchor");

        // Should only return pending bitcoin_ots
        let pending = store
            .get_pending_ots_anchors()
            .expect("Failed to get pending anchors");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].anchor.anchor_type, AnchorType::BitcoinOts);
    }

    #[test]
    fn test_update_anchor_status() {
        let store = create_test_store();
        let anchor = create_test_anchor_ots();

        let id = store
            .store_anchor_returning_id(200, &anchor, "pending")
            .expect("Failed to store anchor");

        // Update status
        store
            .update_anchor_status(id, "confirmed")
            .expect("Failed to update status");

        // Verify status changed (pending list should be empty now)
        let pending = store
            .get_pending_ots_anchors()
            .expect("Failed to get pending anchors");
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn test_update_anchor_token() {
        let store = create_test_store();
        let anchor = create_test_anchor_ots();

        let id = store
            .store_anchor_returning_id(200, &anchor, "pending")
            .expect("Failed to store anchor");

        let new_token = vec![0xFF, 0xEE, 0xDD, 0xCC];
        store
            .update_anchor_token(id, &new_token)
            .expect("Failed to update token");

        // Verify token changed
        let pending = store
            .get_pending_ots_anchors()
            .expect("Failed to get pending anchors");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].anchor.token, new_token);
    }

    #[test]
    fn test_update_anchor_metadata() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();

        let id = store
            .store_anchor_returning_id(100, &anchor, "pending")
            .expect("Failed to store anchor");

        let new_metadata = serde_json::json!({"updated": true, "value": 42});
        store
            .update_anchor_metadata(id, new_metadata.clone())
            .expect("Failed to update metadata");

        // Verify metadata changed
        let anchors = store.get_anchors(100).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].metadata, new_metadata);
    }

    #[test]
    fn test_get_anchors_covering_empty() {
        let store = create_test_store();
        let anchors = store
            .get_anchors_covering(100, 10)
            .expect("Failed to get covering anchors");
        assert_eq!(anchors.len(), 0);
    }

    #[test]
    fn test_get_anchors_covering_exact_match() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();

        store
            .store_anchor_returning_id(100, &anchor, "confirmed")
            .expect("Failed to store anchor");

        let covering = store
            .get_anchors_covering(100, 10)
            .expect("Failed to get covering anchors");
        assert_eq!(covering.len(), 1);
        assert_eq!(covering[0].tree_size, 100);
    }

    #[test]
    fn test_get_anchors_covering_filters_by_status() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();

        // Store as pending
        store
            .store_anchor_returning_id(100, &anchor, "pending")
            .expect("Failed to store anchor");

        // Should not return pending anchors
        let covering = store
            .get_anchors_covering(100, 10)
            .expect("Failed to get covering anchors");
        assert_eq!(covering.len(), 0);
    }

    #[test]
    fn test_get_anchors_covering_orders_by_tree_size() {
        let store = create_test_store();

        let anchor1 = create_test_anchor_rfc3161();
        let anchor2 = create_test_anchor_rfc3161();

        store
            .store_anchor_returning_id(200, &anchor1, "confirmed")
            .expect("Failed to store anchor1");
        store
            .store_anchor_returning_id(150, &anchor2, "confirmed")
            .expect("Failed to store anchor2");

        // Looking for anchors covering size 100
        let covering = store
            .get_anchors_covering(100, 10)
            .expect("Failed to get covering anchors");
        assert!(!covering.is_empty());
        // Should get the closest (minimum) tree_size first
        assert_eq!(covering[0].tree_size, 150);
    }

    #[test]
    fn test_get_anchors_covering_respects_limit() {
        let store = create_test_store();

        let anchor1 = create_test_anchor_rfc3161();
        let mut anchor2 = create_test_anchor_ots();
        anchor2.tree_size = 150;

        store
            .store_anchor_returning_id(150, &anchor1, "confirmed")
            .expect("Failed to store anchor1");
        store
            .store_anchor_returning_id(150, &anchor2, "confirmed")
            .expect("Failed to store anchor2");

        let covering = store
            .get_anchors_covering(100, 1)
            .expect("Failed to get covering anchors");
        assert!(covering.len() <= 1);
    }

    #[test]
    fn test_get_anchors_covering_min_per_type() {
        let store = create_test_store();

        let rfc1 = create_test_anchor_rfc3161();
        let mut rfc2 = create_test_anchor_rfc3161();
        rfc2.tree_size = 200;

        let ots1 = create_test_anchor_ots();
        let mut ots2 = create_test_anchor_ots();
        ots2.tree_size = 250;

        // Store multiple anchors of same type
        store
            .store_anchor_returning_id(100, &rfc1, "confirmed")
            .expect("Failed to store rfc1");
        store
            .store_anchor_returning_id(200, &rfc2, "confirmed")
            .expect("Failed to store rfc2");
        store
            .store_anchor_returning_id(200, &ots1, "confirmed")
            .expect("Failed to store ots1");
        store
            .store_anchor_returning_id(250, &ots2, "confirmed")
            .expect("Failed to store ots2");

        // Should return only min tree_size per anchor type
        let covering = store
            .get_anchors_covering(100, 10)
            .expect("Failed to get covering anchors");
        assert_eq!(covering.len(), 2); // One per anchor type
        assert_eq!(covering[0].tree_size, 100); // Min RFC3161
        assert_eq!(covering[1].tree_size, 200); // Min OTS
    }

    #[test]
    fn test_submit_super_root_ots_anchor() {
        let mut store = create_test_store();

        let super_root = [0xABu8; 32];
        let proof = vec![0x01, 0x02, 0x03];
        let calendar_url = "https://ots.example.com";

        let id = store
            .submit_super_root_ots_anchor(&proof, calendar_url, &super_root, 42)
            .expect("Failed to submit super root anchor");

        assert!(id > 0);

        // Verify anchor was created with correct properties
        let pending = store
            .get_pending_ots_anchors()
            .expect("Failed to get pending anchors");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id);
        assert_eq!(pending[0].anchor.anchor_type, AnchorType::BitcoinOts);
        assert_eq!(pending[0].anchor.target, "super_root");
        assert_eq!(pending[0].anchor.anchored_hash, super_root);
        assert_eq!(pending[0].anchor.super_tree_size, Some(42));
        assert_eq!(pending[0].anchor.token, proof);
    }

    #[test]
    fn test_submit_super_root_ots_anchor_metadata() {
        let mut store = create_test_store();

        let super_root = [0xABu8; 32];
        let proof = vec![0x01, 0x02, 0x03];
        let calendar_url = "https://calendar.example.com/submit";

        store
            .submit_super_root_ots_anchor(&proof, calendar_url, &super_root, 100)
            .expect("Failed to submit super root anchor");

        let pending = store
            .get_pending_ots_anchors()
            .expect("Failed to get pending anchors");
        assert_eq!(pending.len(), 1);

        // Verify metadata contains calendar_url
        let metadata = &pending[0].anchor.metadata;
        assert_eq!(
            metadata.get("calendar_url").and_then(|v| v.as_str()),
            Some(calendar_url)
        );
    }

    #[test]
    fn test_confirm_ots_anchor_atomic_success() {
        let mut store = create_test_store();

        // Setup: Create a pending OTS anchor
        let anchor = create_test_anchor_ots();
        let anchor_id = store
            .store_anchor_returning_id(200, &anchor, "pending")
            .expect("Failed to store anchor");

        // Create a tree linked to this anchor
        store
            .connection()
            .execute(
                "INSERT INTO trees (origin_id, status, start_size, bitcoin_anchor_id, created_at)
                 VALUES (?1, 'pending_bitcoin', 0, ?2, ?3)",
                rusqlite::params![
                    [0u8; 32].as_slice(),
                    anchor_id,
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                ],
            )
            .expect("Failed to create tree");

        let upgraded_proof = vec![0xFF, 0xEE, 0xDD];
        let block_height = 700_000;
        let block_time = 1_600_000_000;

        // Confirm the anchor
        store
            .confirm_ots_anchor_atomic(anchor_id, &upgraded_proof, block_height, block_time)
            .expect("Failed to confirm anchor");

        // Verify anchor status changed
        let pending = store
            .get_pending_ots_anchors()
            .expect("Failed to get pending anchors");
        assert_eq!(pending.len(), 0);

        // Verify anchor token was updated
        let anchors = store.get_anchors(200).expect("Failed to get anchors");
        assert!(anchors.iter().any(|a| a.token == upgraded_proof));

        // Verify metadata was updated with Bitcoin info
        let metadata = &anchors
            .iter()
            .find(|a| a.token == upgraded_proof)
            .unwrap()
            .metadata;
        assert_eq!(
            metadata
                .get("bitcoin_block_height")
                .and_then(|v| v.as_i64()),
            Some(block_height as i64)
        );
        assert_eq!(
            metadata.get("bitcoin_block_time").and_then(|v| v.as_i64()),
            Some(block_time as i64)
        );
        assert_eq!(
            metadata.get("status").and_then(|v| v.as_str()),
            Some("confirmed")
        );
    }

    #[test]
    fn test_confirm_ots_anchor_atomic_updates_tree_status() {
        let mut store = create_test_store();

        let anchor = create_test_anchor_ots();
        let anchor_id = store
            .store_anchor_returning_id(200, &anchor, "pending")
            .expect("Failed to store anchor");

        // Create tree linked to anchor
        store
            .connection()
            .execute(
                "INSERT INTO trees (origin_id, status, start_size, bitcoin_anchor_id, created_at)
                 VALUES (?1, 'pending_bitcoin', 0, ?2, ?3)",
                rusqlite::params![
                    [0u8; 32].as_slice(),
                    anchor_id,
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                ],
            )
            .expect("Failed to create tree");

        store
            .confirm_ots_anchor_atomic(anchor_id, &[0xFF], 700_000, 1_600_000_000)
            .expect("Failed to confirm anchor");

        // Verify tree status changed to 'closed'
        let status: String = store
            .connection()
            .query_row(
                "SELECT status FROM trees WHERE bitcoin_anchor_id = ?1",
                [anchor_id],
                |row| row.get(0),
            )
            .expect("Failed to get tree status");
        assert_eq!(status, "closed");
    }

    #[test]
    fn test_get_tsa_anchor_for_hash_not_found() {
        let store = create_test_store();
        let hash = [0x42u8; 32];

        let result = store
            .get_tsa_anchor_for_hash(&hash)
            .expect("Failed to query TSA anchor");
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_tsa_anchor_for_hash_found() {
        let store = create_test_store();
        let hash = [0x42u8; 32];

        let mut anchor = create_test_anchor_rfc3161();
        anchor.anchored_hash = hash;

        let id = store
            .store_anchor_returning_id(100, &anchor, "confirmed")
            .expect("Failed to store anchor");

        let result = store
            .get_tsa_anchor_for_hash(&hash)
            .expect("Failed to query TSA anchor");
        assert_eq!(result, Some(id));
    }

    #[test]
    fn test_get_tsa_anchor_for_hash_ignores_ots() {
        let store = create_test_store();
        let hash = [0x42u8; 32];

        let mut anchor = create_test_anchor_ots();
        anchor.anchored_hash = hash;

        store
            .store_anchor_returning_id(100, &anchor, "confirmed")
            .expect("Failed to store OTS anchor");

        // Should not find OTS anchor
        let result = store
            .get_tsa_anchor_for_hash(&hash)
            .expect("Failed to query TSA anchor");
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_latest_tsa_anchored_size_empty() {
        let store = create_test_store();
        let size = store
            .get_latest_tsa_anchored_size()
            .expect("Failed to get latest TSA size");
        assert_eq!(size, None);
    }

    #[test]
    fn test_get_latest_tsa_anchored_size_with_anchors() {
        let store = create_test_store();

        let rfc1 = create_test_anchor_rfc3161();
        let mut rfc2 = create_test_anchor_rfc3161();
        rfc2.tree_size = 200;

        store
            .store_anchor(100, &rfc1)
            .expect("Failed to store rfc1");
        store
            .store_anchor(200, &rfc2)
            .expect("Failed to store rfc2");

        let size = store
            .get_latest_tsa_anchored_size()
            .expect("Failed to get latest TSA size");
        assert_eq!(size, Some(200));
    }

    #[test]
    fn test_get_latest_tsa_anchored_size_ignores_ots() {
        let store = create_test_store();

        let rfc = create_test_anchor_rfc3161();
        let ots = create_test_anchor_ots();

        store
            .store_anchor(100, &rfc)
            .expect("Failed to store RFC anchor");
        store
            .store_anchor(300, &ots)
            .expect("Failed to store OTS anchor");

        // Should return RFC size, not OTS
        let size = store
            .get_latest_tsa_anchored_size()
            .expect("Failed to get latest TSA size");
        assert_eq!(size, Some(100));
    }

    #[test]
    fn test_row_to_anchor_rfc3161() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();

        store
            .store_anchor(100, &anchor)
            .expect("Failed to store anchor");

        let anchors = store.get_anchors(100).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 1);

        let retrieved = &anchors[0];
        assert_eq!(retrieved.anchor_type, AnchorType::Rfc3161);
        assert_eq!(retrieved.target, "data_tree_root");
        assert_eq!(retrieved.anchored_hash, [1u8; 32]);
        assert_eq!(retrieved.tree_size, 100);
        assert_eq!(retrieved.super_tree_size, None);
        assert_eq!(retrieved.timestamp, 1_234_567_890_000_000_000);
        assert_eq!(retrieved.token, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_row_to_anchor_bitcoin_ots() {
        let store = create_test_store();
        let anchor = create_test_anchor_ots();

        store
            .store_anchor(200, &anchor)
            .expect("Failed to store anchor");

        let anchors = store.get_anchors(200).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 1);

        let retrieved = &anchors[0];
        assert_eq!(retrieved.anchor_type, AnchorType::BitcoinOts);
        assert_eq!(retrieved.super_tree_size, Some(50));
    }

    #[test]
    fn test_row_to_anchor_other_type() {
        let store = create_test_store();
        let anchor = create_test_anchor_other();

        store
            .store_anchor(300, &anchor)
            .expect("Failed to store anchor");

        let anchors = store.get_anchors(300).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].anchor_type, AnchorType::Other);
    }

    #[test]
    fn test_row_to_anchor_with_id() {
        let store = create_test_store();
        let anchor = create_test_anchor_ots();

        let id = store
            .store_anchor_returning_id(200, &anchor, "pending")
            .expect("Failed to store anchor");

        let pending = store
            .get_pending_ots_anchors()
            .expect("Failed to get pending anchors");
        assert_eq!(pending.len(), 1);

        let retrieved = &pending[0];
        assert_eq!(retrieved.id, id);
        assert_eq!(retrieved.anchor.anchor_type, AnchorType::BitcoinOts);
        assert_eq!(retrieved.anchor.tree_size, 200);
    }

    #[test]
    fn test_row_to_anchor_metadata_parsing() {
        let store = create_test_store();
        let mut anchor = create_test_anchor_rfc3161();
        anchor.metadata = serde_json::json!({
            "key": "value",
            "number": 42,
            "nested": {"inner": true}
        });

        store
            .store_anchor(100, &anchor)
            .expect("Failed to store anchor");

        let anchors = store.get_anchors(100).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 1);

        let metadata = &anchors[0].metadata;
        assert_eq!(metadata.get("key").and_then(|v| v.as_str()), Some("value"));
        assert_eq!(metadata.get("number").and_then(|v| v.as_i64()), Some(42));
        assert!(metadata
            .get("nested")
            .and_then(|v| v.get("inner"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false));
    }

    #[test]
    fn test_row_to_anchor_invalid_metadata_defaults_to_empty() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();

        store
            .store_anchor(100, &anchor)
            .expect("Failed to store anchor");

        // Manually corrupt metadata in DB
        store
            .connection()
            .execute(
                "UPDATE anchors SET metadata = 'invalid json' WHERE tree_size = 100",
                [],
            )
            .expect("Failed to corrupt metadata");

        // Should still parse successfully with default metadata
        let anchors = store.get_anchors(100).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 1);
        // Default metadata should be empty object or null
        assert!(anchors[0].metadata.is_null() || anchors[0].metadata == serde_json::json!({}));
    }

    #[test]
    fn test_anchor_with_id_structure() {
        let anchor = create_test_anchor_rfc3161();
        let anchor_with_id = AnchorWithId {
            id: 42,
            anchor: anchor.clone(),
        };

        assert_eq!(anchor_with_id.id, 42);
        assert_eq!(anchor_with_id.anchor.anchor_type, anchor.anchor_type);
        assert_eq!(anchor_with_id.anchor.anchored_hash, anchor.anchored_hash);
    }

    #[test]
    fn test_multiple_anchor_types_stored_correctly() {
        let store = create_test_store();

        let rfc = create_test_anchor_rfc3161();
        let ots = create_test_anchor_ots();
        let other = create_test_anchor_other();

        store.store_anchor(100, &rfc).expect("Failed to store RFC");
        store.store_anchor(200, &ots).expect("Failed to store OTS");
        store
            .store_anchor(300, &other)
            .expect("Failed to store Other");

        // Verify all types stored correctly
        let rfc_anchors = store.get_anchors(100).expect("Failed to get RFC anchors");
        assert_eq!(rfc_anchors[0].anchor_type, AnchorType::Rfc3161);

        let ots_anchors = store.get_anchors(200).expect("Failed to get OTS anchors");
        assert_eq!(ots_anchors[0].anchor_type, AnchorType::BitcoinOts);

        let other_anchors = store.get_anchors(300).expect("Failed to get Other anchors");
        assert_eq!(other_anchors[0].anchor_type, AnchorType::Other);
    }

    #[test]
    fn test_get_anchors_covering_below_min_tree_size() {
        let store = create_test_store();

        let anchor = create_test_anchor_rfc3161();
        store
            .store_anchor_returning_id(100, &anchor, "confirmed")
            .expect("Failed to store anchor");

        // Looking for anchors covering size 150, but only have size 100
        let covering = store
            .get_anchors_covering(150, 10)
            .expect("Failed to get covering anchors");
        assert_eq!(covering.len(), 0);
    }

    #[test]
    fn test_confirm_ots_anchor_atomic_no_tree() {
        let mut store = create_test_store();

        let anchor = create_test_anchor_ots();
        let anchor_id = store
            .store_anchor_returning_id(200, &anchor, "pending")
            .expect("Failed to store anchor");

        // Confirm without associated tree should still work
        let result = store.confirm_ots_anchor_atomic(anchor_id, &[0xFF], 700_000, 1_600_000_000);
        assert!(result.is_ok());

        // Verify anchor was updated
        let pending = store
            .get_pending_ots_anchors()
            .expect("Failed to get pending anchors");
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn test_row_to_anchor_invalid_hash_length() {
        let store = create_test_store();

        // Insert anchor with invalid hash length (not 32 bytes)
        store
            .connection()
            .execute(
                "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash, timestamp, token, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    100i64,
                    "rfc3161",
                    "data_tree_root",
                    vec![0x01u8, 0x02, 0x03], // Invalid: only 3 bytes instead of 32
                    1_234_567_890_000_000_000i64,
                    vec![0xDEu8, 0xAD],
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                ],
            )
            .expect("Failed to insert anchor");

        // Attempting to read should return an error due to invalid hash length
        let result = store.get_anchors(100);
        assert!(
            result.is_err(),
            "Expected error for invalid anchored_hash length"
        );

        // Verify the error is about InvalidColumnType
        if let Err(e) = result {
            let error_string = format!("{:?}", e);
            assert!(
                error_string.contains("InvalidColumnType")
                    || error_string.contains("anchored_hash"),
                "Error should mention InvalidColumnType or anchored_hash, got: {}",
                error_string
            );
        }
    }

    #[test]
    fn test_row_to_anchor_with_id_invalid_hash_length() {
        let store = create_test_store();

        // Insert OTS anchor with invalid hash length
        store
            .connection()
            .execute(
                "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash, timestamp, token, status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    200i64,
                    "bitcoin_ots",
                    "data_tree_root",
                    vec![0x01u8, 0x02], // Invalid: only 2 bytes
                    1_234_567_890_000_000_000i64,
                    vec![0xCAu8, 0xFE],
                    "pending",
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                ],
            )
            .expect("Failed to insert anchor");

        // Attempting to read should return an error
        let result = store.get_pending_ots_anchors();
        assert!(
            result.is_err(),
            "Expected error for invalid anchored_hash length in AnchorWithId"
        );
    }

    #[test]
    fn test_row_to_anchor_unknown_type_maps_to_other() {
        let store = create_test_store();

        // Insert anchor with unknown type
        store
            .connection()
            .execute(
                "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash, timestamp, token, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    100i64,
                    "unknown_future_type", // Unknown anchor type
                    "data_tree_root",
                    [0x42u8; 32].as_slice(),
                    1_234_567_890_000_000_000i64,
                    vec![0xDEu8, 0xAD],
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                ],
            )
            .expect("Failed to insert anchor");

        let anchors = store.get_anchors(100).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 1);
        // Unknown types should map to AnchorType::Other
        assert_eq!(anchors[0].anchor_type, AnchorType::Other);
    }

    #[test]
    fn test_row_to_anchor_with_id_unknown_type_maps_to_other() {
        let store = create_test_store();

        // Insert OTS-like anchor with unknown type
        store
            .connection()
            .execute(
                "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash, timestamp, token, status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    200i64,
                    "ethereum_anchor", // Unknown type
                    "data_tree_root",
                    [0x42u8; 32].as_slice(),
                    1_234_567_890_000_000_000i64,
                    vec![0xCAu8, 0xFE],
                    "confirmed",
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                ],
            )
            .expect("Failed to insert anchor");

        let anchors = store.get_anchors(200).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].anchor_type, AnchorType::Other);
    }

    #[test]
    fn test_store_anchor_with_metadata_serialization_edge_case() {
        let store = create_test_store();
        let mut anchor = create_test_anchor_rfc3161();

        // Test with complex nested metadata
        anchor.metadata = serde_json::json!({
            "array": [1, 2, 3],
            "null": null,
            "boolean": true,
            "nested": {
                "deeply": {
                    "nested": "value"
                }
            }
        });

        let result = store.store_anchor(100, &anchor);
        assert!(result.is_ok());

        let anchors = store.get_anchors(100).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].metadata, anchor.metadata);
    }

    #[test]
    fn test_update_operations_on_nonexistent_anchor() {
        let store = create_test_store();

        // Try to update status on non-existent anchor
        let result = store.update_anchor_status(99999, "confirmed");
        assert!(result.is_ok()); // SQLite doesn't error on UPDATE with no matches

        // Try to update token on non-existent anchor
        let result = store.update_anchor_token(99999, &[0xFF]);
        assert!(result.is_ok());

        // Try to update metadata on non-existent anchor
        let result = store.update_anchor_metadata(99999, serde_json::json!({"test": true}));
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_anchors_with_null_tree_size() {
        let store = create_test_store();

        // Insert anchor with NULL tree_size (e.g., Super Root anchor)
        store
            .connection()
            .execute(
                "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash, super_tree_size, timestamp, token, status, created_at)
                 VALUES (NULL, 'bitcoin_ots', 'super_root', ?1, ?2, ?3, ?4, 'confirmed', ?5)",
                rusqlite::params![
                    [0xABu8; 32].as_slice(),
                    100i64,
                    1_234_567_890_000_000_000i64,
                    vec![0x01u8, 0x02],
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                ],
            )
            .expect("Failed to insert super root anchor");

        // get_anchors should include super_root anchors with confirmed status
        let anchors = store.get_anchors(50).expect("Failed to get anchors");
        assert!(!anchors.is_empty());
        assert!(anchors.iter().any(|a| a.target == "super_root"));
    }
}
