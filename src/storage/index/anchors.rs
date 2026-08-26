// File: src/storage/index/anchors.rs

//! Anchor management for IndexStore
//!
//! This module contains methods for managing anchors:
//! - Storing anchors (store_anchor, store_anchor_returning_id)
//! - Querying anchors (get_anchors, get_pending_ots_anchors,
//!   get_tsa_anchor_covering, get_ots_anchor_covering)
//! - Updating anchors (update_anchor_status, update_anchor_token, update_anchor_metadata)
//! - Atomic operations (confirm_ots_anchor_atomic, reject_anchor_atomic)

use super::queries::IndexStore;
use crate::traits::{Anchor, AnchorType};
use rusqlite::{params, OptionalExtension};

/// Structured `metadata.rejection_reason` code recorded when a stored RFC
/// 3161 token is found to fail `messageImprint` verification (bound to a
/// hash other than its own `anchored_hash`).
pub const REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH: &str = "message_imprint_mismatch";

/// Anchor with ID (for OTS poll job)
#[derive(Debug, Clone)]
pub struct AnchorWithId {
    pub id: i64,
    pub anchor: Anchor,
}

/// A raw `rfc3161` anchor row for the admin audit tool.
///
/// See [`IndexStore::list_rfc3161_anchors_for_audit`].
#[derive(Debug, Clone)]
pub struct AuditableAnchor {
    pub id: i64,
    pub tree_size: Option<u64>,
    pub anchored_hash: [u8; 32],
    pub token: Vec<u8>,
    pub timestamp: u64,
    pub status: String,
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

/// Merge `new_fields` into an anchor's existing `metadata`, tolerant of
/// `existing_metadata` holding anything other than a well-formed JSON
/// object -- including bytes that are not even valid UTF-8.
///
/// This exists so that read-modify-write updates to the `metadata` column
/// (currently [`IndexStore::reject_anchor_atomic`] and
/// [`IndexStore::confirm_ots_anchor_atomic`]) never depend on SQL-side JSON
/// functions (`json_set` and friends), which require their input to
/// already be well-formed JSON and abort the whole enclosing transaction
/// with "malformed JSON" otherwise -- silently leaving whatever the
/// transaction was supposed to accomplish (rejecting a bad anchor,
/// confirming a good one) undone. `metadata` is user/network-influenced
/// data (it round-trips TSA/calendar URLs and, historically, was written
/// before some of today's invariants existed), so it must never be assumed
/// well-formed -- not even assumed to be text: the caller must read the
/// column as raw bytes (`CAST(metadata AS BLOB)`), not as `String`, since
/// `rusqlite` itself rejects invalid UTF-8 when a column is read as
/// `String`, which would abort the transaction exactly the same way
/// `json_set` did before this existed.
///
/// Three cases:
/// - `existing_metadata` is `None`: starts from an empty object.
/// - It parses (via [`serde_json::from_slice`]) as JSON and the top-level
///   value is an object: `new_fields` are merged into it, overwriting any
///   key they share with it and otherwise preserving every pre-existing
///   key.
/// - It fails to parse as JSON at all, or parses but isn't an object (a
///   bare number, array, string, bool, or null): neither can be merged
///   into structurally, but corrupted or unexpected metadata is itself
///   evidence and must not be discarded. If the bytes are valid UTF-8,
///   they are preserved verbatim as a string under `legacy_metadata_raw`
///   on a fresh object (note: JSON is required to be valid UTF-8, so a
///   successful parse of a non-object value always falls in this case).
///   If they are not even valid UTF-8, they are preserved losslessly as
///   base64 under `legacy_metadata_raw_base64` instead -- a distinctly
///   named key so a reader never has to guess which encoding a given
///   `legacy_metadata_*` field is in.
///
/// Returns the resulting object serialized back to a JSON string, ready
/// for a plain parameterized `UPDATE ... SET metadata = ?`.
fn merge_metadata_fields(
    existing_metadata: Option<&[u8]>,
    new_fields: impl IntoIterator<Item = (&'static str, serde_json::Value)>,
) -> String {
    use base64::Engine;

    let mut merged = match existing_metadata {
        None => serde_json::Map::new(),
        Some(raw_bytes) => match serde_json::from_slice::<serde_json::Value>(raw_bytes) {
            Ok(serde_json::Value::Object(map)) => map,
            Ok(_) | Err(_) => {
                let mut map = serde_json::Map::new();
                match std::str::from_utf8(raw_bytes) {
                    Ok(raw) => {
                        map.insert(
                            "legacy_metadata_raw".to_string(),
                            serde_json::Value::String(raw.to_string()),
                        );
                    }
                    Err(_) => {
                        map.insert(
                            "legacy_metadata_raw_base64".to_string(),
                            serde_json::Value::String(
                                base64::engine::general_purpose::STANDARD.encode(raw_bytes),
                            ),
                        );
                    }
                }
                map
            }
        },
    };

    for (key, value) in new_fields {
        merged.insert(key.to_string(), value);
    }

    serde_json::to_string(&serde_json::Value::Object(merged))
        .expect("serializing a serde_json::Map to a JSON string cannot fail")
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
    ///
    /// Only `confirmed` anchors are eligible: a valid TSA token has no
    /// intermediate state (both insertion paths write `confirmed`
    /// immediately), `pending` is exclusively an in-flight OTS state, and
    /// `rejected` marks a token that failed `messageImprint` verification.
    /// None of those should ever be served in a receipt.
    pub fn get_anchors(&self, tree_size: u64) -> rusqlite::Result<Vec<Anchor>> {
        let conn = self.connection();
        let mut stmt = conn.prepare(
            "SELECT id, tree_size, anchor_type, target, anchored_hash, super_tree_size, timestamp, token, metadata
             FROM anchors WHERE status = 'confirmed' AND (tree_size = ?1 OR target = 'super_root')",
        )?;

        let rows = stmt.query_map(params![tree_size as i64], row_to_anchor)?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    /// Get the most recent anchored tree size
    pub fn get_latest_anchored_size(&self) -> rusqlite::Result<Option<u64>> {
        let result = self.connection().query_row(
            "SELECT MAX(tree_size) FROM anchors WHERE status = 'confirmed'",
            [],
            |row| row.get::<_, Option<i64>>(0),
        )?;

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

    /// Get the minimum confirmed TSA anchor covering a data tree position.
    ///
    /// Returns the RFC 3161 anchor with the smallest tree_size >= `tree_size`,
    /// or None if no such anchor exists.
    pub fn get_tsa_anchor_covering(&self, tree_size: u64) -> rusqlite::Result<Option<Anchor>> {
        self.connection()
            .query_row(
                "SELECT id, tree_size, anchor_type, target, anchored_hash,
                        super_tree_size, timestamp, token, metadata
                 FROM anchors
                 WHERE status = 'confirmed'
                   AND anchor_type = 'rfc3161'
                   AND target = 'data_tree_root'
                   AND tree_size >= ?1
                 ORDER BY tree_size ASC
                 LIMIT 1",
                rusqlite::params![tree_size as i64],
                row_to_anchor,
            )
            .optional()
    }

    /// Get the minimum confirmed OTS anchor covering a data tree index.
    ///
    /// Returns the Bitcoin OTS anchor with the smallest super_tree_size
    /// that is > `data_tree_index`, or None if no such anchor exists.
    pub fn get_ots_anchor_covering(
        &self,
        data_tree_index: u64,
    ) -> rusqlite::Result<Option<Anchor>> {
        self.connection()
            .query_row(
                "SELECT id, tree_size, anchor_type, target, anchored_hash,
                        super_tree_size, timestamp, token, metadata
                 FROM anchors
                 WHERE status = 'confirmed'
                   AND anchor_type = 'bitcoin_ots'
                   AND target = 'super_root'
                   AND super_tree_size > ?1
                 ORDER BY super_tree_size ASC
                 LIMIT 1",
                rusqlite::params![data_tree_index as i64],
                row_to_anchor,
            )
            .optional()
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

        // Update anchor metadata with Bitcoin block info. Merged in Rust
        // (see `merge_metadata_fields`) rather than via SQLite's
        // `json_set`, which required `metadata` to already be a
        // well-formed JSON object and aborted this whole transaction with
        // "malformed JSON" otherwise. Unlike the `reject_anchor_atomic`
        // case (where a failure here leaves a bad anchor still in
        // circulation), a failure here is the opposite and arguably worse:
        // it silently, permanently prevents a *good* Bitcoin anchor from
        // ever confirming -- the token stays un-upgraded, status stays
        // non-`confirmed`, and the tree never advances to `closed` -- for
        // exactly this one row, until someone manually repairs its
        // metadata. Bitcoin confirmation is the one trust source here that
        // needs no trusted root of its own, so losing it silently is
        // costly.
        // Read as raw bytes, not `String`: `rusqlite` rejects invalid
        // UTF-8 when a column is fetched as `String`, which would fail
        // this query (and roll back the transaction) for exactly the kind
        // of corrupted `metadata` this whole merge exists to tolerate.
        let existing_metadata: Option<Vec<u8>> = tx
            .query_row(
                "SELECT CAST(metadata AS BLOB) FROM anchors WHERE id = ?1",
                params![anchor_id],
                |row| row.get(0),
            )
            .optional()?;

        let metadata_json = merge_metadata_fields(
            existing_metadata.as_deref(),
            [
                (
                    "bitcoin_block_height",
                    serde_json::Value::Number((block_height as i64).into()),
                ),
                (
                    "bitcoin_block_time",
                    serde_json::Value::Number((block_time as i64).into()),
                ),
                ("status", serde_json::Value::String("confirmed".to_string())),
            ],
        );

        tx.execute(
            "UPDATE anchors SET metadata = ?1 WHERE id = ?2",
            params![metadata_json, anchor_id],
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

    /// Get existing, confirmed TSA anchor ID for a root hash (if any)
    ///
    /// Excludes `rejected` rows (a token that failed `messageImprint`
    /// verification must never be found again for reuse) and anything
    /// that isn't a `data_tree_root` anchor.
    ///
    /// Production code should prefer
    /// [`get_tsa_anchor_with_token_for_hash`](Self::get_tsa_anchor_with_token_for_hash),
    /// which also loads the token so it can be verified before reuse; this
    /// lighter lookup is kept for tests and for callers that only need the
    /// id.
    #[allow(dead_code)]
    pub fn get_tsa_anchor_for_hash(&self, root_hash: &[u8; 32]) -> rusqlite::Result<Option<i64>> {
        self.connection()
            .query_row(
                "SELECT id FROM anchors
                 WHERE anchored_hash = ?1 AND anchor_type = 'rfc3161'
                   AND target = 'data_tree_root' AND status = 'confirmed'
                 LIMIT 1",
                [root_hash.as_slice()],
                |row| row.get(0),
            )
            .optional()
    }

    /// Get the existing, confirmed TSA anchor for a root hash, including
    /// its stored token, so the caller can verify it before trusting or
    /// reusing it.
    ///
    /// Excludes `rejected` rows for the same reason as
    /// [`get_tsa_anchor_for_hash`](Self::get_tsa_anchor_for_hash): once a
    /// token has been marked rejected it must never be surfaced again as
    /// something to reuse, only as an audit trail entry.
    ///
    /// Unlike `get_tsa_anchor_for_hash`, which only returns the row id,
    /// this loads the full anchor so a caller can re-run
    /// `TsaClient::verify()` on a token that was stored before
    /// verification-on-receipt existed.
    pub fn get_tsa_anchor_with_token_for_hash(
        &self,
        root_hash: &[u8; 32],
    ) -> rusqlite::Result<Option<AnchorWithId>> {
        self.connection()
            .query_row(
                "SELECT id, tree_size, anchor_type, target, anchored_hash, super_tree_size, timestamp, token, metadata, status
                 FROM anchors
                 WHERE anchored_hash = ?1 AND anchor_type = 'rfc3161'
                   AND target = 'data_tree_root' AND status = 'confirmed'
                 LIMIT 1",
                [root_hash.as_slice()],
                row_to_anchor_with_id,
            )
            .optional()
    }

    /// Mark an RFC 3161 anchor as rejected and atomically release any tree
    /// pointing to it via `tsa_anchor_id`.
    ///
    /// Used when a stored token is found to fail `messageImprint`
    /// verification. The row is never deleted: for a product whose premise
    /// is an immutable audit trail, permanently erasing the record that a
    /// bad root was once anchored under this id is a worse precedent than
    /// keeping it -- incident review needs it. Instead the row is flagged
    /// `status = 'rejected'` (a lifecycle state, distinct from the
    /// *reason*, which is recorded structurally as
    /// `metadata.rejection_reason` plus `metadata.rejected_at`), which
    /// every read path (`get_anchors`, `get_tsa_anchor_with_token_for_hash`,
    /// `get_tsa_anchor_for_hash`, `get_latest_anchored_size`,
    /// `get_latest_tsa_anchored_size`) now excludes.
    ///
    /// Releasing the tree in the SAME transaction is required, not
    /// optional: [`IndexStore::get_trees_pending_tsa`] only picks up trees
    /// with `tsa_anchor_id IS NULL`, so a tree left pointing at a rejected
    /// anchor would never be re-queued for anchoring -- worse than either
    /// rejecting cleanly or not rejecting at all.
    ///
    /// This must succeed regardless of what `metadata` currently holds --
    /// including a non-JSON string or JSON that isn't an object, both of
    /// which are preserved (not discarded) under `legacy_metadata_raw`.
    /// See the implementation for why.
    pub fn reject_anchor_atomic(&self, anchor_id: i64, reason: &str) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let mut conn = self.connection_mut();
        let tx = conn.transaction()?;

        // Merge into `metadata` in Rust (see `merge_metadata_fields`)
        // rather than via SQLite's `json_set`, which would abort this
        // whole transaction with "malformed JSON" if `metadata` holds
        // anything other than a well-formed JSON object -- leaving the
        // very row this method exists to reject sitting at
        // `status = 'confirmed'`, still servable and reusable.
        // Read as raw bytes, not `String`: `rusqlite` rejects invalid
        // UTF-8 when a column is fetched as `String`, which would fail
        // this query (and roll back the transaction) for exactly the kind
        // of corrupted `metadata` this whole merge exists to tolerate.
        let existing_metadata: Option<Vec<u8>> = tx
            .query_row(
                "SELECT CAST(metadata AS BLOB) FROM anchors WHERE id = ?1",
                params![anchor_id],
                |row| row.get(0),
            )
            .optional()?;

        let metadata_json = merge_metadata_fields(
            existing_metadata.as_deref(),
            [
                (
                    "rejection_reason",
                    serde_json::Value::String(reason.to_string()),
                ),
                ("rejected_at", serde_json::Value::Number(now.into())),
            ],
        );

        tx.execute(
            "UPDATE anchors SET status = 'rejected', metadata = ?1 WHERE id = ?2",
            params![metadata_json, anchor_id],
        )?;

        tx.execute(
            "UPDATE trees SET tsa_anchor_id = NULL WHERE tsa_anchor_id = ?1",
            params![anchor_id],
        )?;

        tx.commit()?;
        Ok(())
    }

    /// Get the latest anchored tree_size for TSA anchors (rfc3161)
    ///
    /// Returns the maximum tree_size that has been anchored via TSA, among
    /// `confirmed` anchors only (a `rejected` row's tree_size must not
    /// influence this).
    /// Used for periodic active tree anchoring.
    pub fn get_latest_tsa_anchored_size(&self) -> rusqlite::Result<Option<u64>> {
        let result = self.connection().query_row(
            "SELECT MAX(tree_size) FROM anchors WHERE anchor_type = 'rfc3161' AND status = 'confirmed'",
            [],
            |row| row.get::<_, Option<i64>>(0),
        )?;

        Ok(result.map(|s| s as u64))
    }

    /// List every `rfc3161` anchor row, regardless of status.
    ///
    /// This is for the one-off admin audit tool
    /// ([`crate::background::tsa_job::audit`]) only: unlike every other
    /// read path in this module, it deliberately does **not** filter by
    /// status, and exposes `status` on each row -- the audit needs to see
    /// `rejected` rows too (to skip them, for idempotency) as well as
    /// whatever `confirmed` rows may still be carrying a bad token from
    /// before verification-on-receipt existed.
    pub fn list_rfc3161_anchors_for_audit(&self) -> rusqlite::Result<Vec<AuditableAnchor>> {
        let conn = self.connection();
        let mut stmt = conn.prepare(
            "SELECT id, tree_size, anchored_hash, token, timestamp, status
             FROM anchors WHERE anchor_type = 'rfc3161' ORDER BY id",
        )?;

        let rows = stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let tree_size: Option<i64> = row.get(1)?;
            let anchored_hash: Vec<u8> = row.get(2)?;
            let token: Vec<u8> = row.get(3)?;
            let timestamp: i64 = row.get(4)?;
            let status: String = row.get(5)?;

            let anchored_hash: [u8; 32] = anchored_hash.try_into().map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    2,
                    "anchored_hash".into(),
                    rusqlite::types::Type::Blob,
                )
            })?;

            Ok(AuditableAnchor {
                id,
                tree_size: tree_size.map(|s| s as u64),
                anchored_hash,
                token,
                timestamp: timestamp as u64,
                status,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
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

        // `store_anchor` defaults to status='pending', but only 'confirmed'
        // anchors are ever served via `get_anchors`. Promote it directly so
        // this test keeps exercising `store_anchor`'s own field round-trip
        // rather than switching to a different insert method.
        store
            .connection()
            .execute(
                "UPDATE anchors SET status = 'confirmed' WHERE tree_size = 100",
                [],
            )
            .expect("Failed to promote anchor to confirmed");

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
            .store_anchor_returning_id(100, &anchor1, "confirmed")
            .expect("Failed to store anchor1");
        store
            .store_anchor_returning_id(100, &anchor2, "confirmed")
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
            .store_anchor_returning_id(100, &anchor, "confirmed")
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
            .store_anchor_returning_id(100, &anchor1, "confirmed")
            .expect("Failed to store anchor1");
        store
            .store_anchor_returning_id(200, &anchor2, "confirmed")
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
            .store_anchor_returning_id(100, &anchor, "confirmed")
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
        // Pre-existing metadata fields (a normal JSON object) must survive
        // the merge, not be discarded.
        assert_eq!(
            metadata.get("calendar_url").and_then(|v| v.as_str()),
            Some("https://ots.example.com")
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
            .store_anchor_returning_id(100, &rfc1, "confirmed")
            .expect("Failed to store rfc1");
        store
            .store_anchor_returning_id(200, &rfc2, "confirmed")
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
            .store_anchor_returning_id(100, &rfc, "confirmed")
            .expect("Failed to store RFC anchor");
        store
            .store_anchor_returning_id(300, &ots, "confirmed")
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
            .store_anchor_returning_id(100, &anchor, "confirmed")
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
            .store_anchor_returning_id(200, &anchor, "confirmed")
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
            .store_anchor_returning_id(300, &anchor, "confirmed")
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
            .store_anchor_returning_id(100, &anchor, "confirmed")
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

        // Manually corrupt metadata in DB, and promote to 'confirmed' since
        // that is now required for `get_anchors` to return the row at all.
        store
            .connection()
            .execute(
                "UPDATE anchors SET metadata = 'invalid json', status = 'confirmed' WHERE tree_size = 100",
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

        store
            .store_anchor_returning_id(100, &rfc, "confirmed")
            .expect("Failed to store RFC");
        store
            .store_anchor_returning_id(200, &ots, "confirmed")
            .expect("Failed to store OTS");
        store
            .store_anchor_returning_id(300, &other, "confirmed")
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

    // -------------------------------------------------------------------
    // Regression: `confirm_ots_anchor_atomic` must go through to
    // completion no matter what `metadata` currently holds. It used to
    // merge the Bitcoin block info via SQLite's `json_set`, which raises
    // "malformed JSON" (and rolls back the whole transaction -- token
    // upgrade, status, and metadata all revert) if `metadata` is not
    // itself well-formed JSON. Unlike the `reject_anchor_atomic` failure
    // mode (a bad anchor stays in circulation), this one silently and
    // permanently prevents a *good* Bitcoin anchor from ever confirming.
    // -------------------------------------------------------------------

    #[test]
    fn test_confirm_ots_anchor_atomic_survives_non_json_metadata() {
        let mut store = create_test_store();

        let anchor = create_test_anchor_ots();
        let anchor_id = store
            .store_anchor_returning_id(200, &anchor, "pending")
            .expect("Failed to store anchor");

        // Corrupt metadata to a string that is not valid JSON at all.
        let garbage = "not json at all {{{";
        store
            .connection()
            .execute(
                "UPDATE anchors SET metadata = ?1 WHERE id = ?2",
                rusqlite::params![garbage, anchor_id],
            )
            .expect("Failed to corrupt metadata");

        let upgraded_proof = vec![0xFF, 0xEE, 0xDD];
        let block_height = 700_000;
        let block_time = 1_600_000_000;

        store
            .confirm_ots_anchor_atomic(anchor_id, &upgraded_proof, block_height, block_time)
            .expect("confirmation must succeed even with unparseable metadata");

        let (status, token, metadata_json): (String, Vec<u8>, Option<String>) = store
            .connection()
            .query_row(
                "SELECT status, token, metadata FROM anchors WHERE id = ?1",
                rusqlite::params![anchor_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("row must still exist");
        assert_eq!(status, "confirmed", "status must become confirmed");
        assert_eq!(token, upgraded_proof, "token must be upgraded");

        let metadata: serde_json::Value =
            serde_json::from_str(&metadata_json.expect("metadata must be set"))
                .expect("resulting metadata must itself be valid JSON");
        assert_eq!(
            metadata["bitcoin_block_height"],
            serde_json::json!(block_height)
        );
        assert_eq!(
            metadata["bitcoin_block_time"],
            serde_json::json!(block_time)
        );
        assert_eq!(metadata["status"], "confirmed");
        assert_eq!(
            metadata["legacy_metadata_raw"], garbage,
            "the original unparseable content must be preserved, not discarded"
        );
    }

    #[test]
    fn test_confirm_ots_anchor_atomic_survives_non_object_json_metadata() {
        let mut store = create_test_store();

        let anchor = create_test_anchor_ots();
        let anchor_id = store
            .store_anchor_returning_id(200, &anchor, "pending")
            .expect("Failed to store anchor");

        // `metadata` is valid JSON, but an array, not an object.
        let non_object = "[1,2,3]";
        store
            .connection()
            .execute(
                "UPDATE anchors SET metadata = ?1 WHERE id = ?2",
                rusqlite::params![non_object, anchor_id],
            )
            .expect("Failed to corrupt metadata");

        let upgraded_proof = vec![0x11, 0x22];
        let block_height = 800_000;
        let block_time = 1_650_000_000;

        store
            .confirm_ots_anchor_atomic(anchor_id, &upgraded_proof, block_height, block_time)
            .expect("confirmation must succeed even with non-object JSON metadata");

        let (status, token, metadata_json): (String, Vec<u8>, Option<String>) = store
            .connection()
            .query_row(
                "SELECT status, token, metadata FROM anchors WHERE id = ?1",
                rusqlite::params![anchor_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("row must still exist");
        assert_eq!(status, "confirmed", "status must become confirmed");
        assert_eq!(token, upgraded_proof, "token must be upgraded");

        let metadata: serde_json::Value =
            serde_json::from_str(&metadata_json.expect("metadata must be set"))
                .expect("resulting metadata must itself be valid JSON");
        assert_eq!(
            metadata["bitcoin_block_height"],
            serde_json::json!(block_height)
        );
        assert_eq!(
            metadata["bitcoin_block_time"],
            serde_json::json!(block_time)
        );
        assert_eq!(metadata["status"], "confirmed");
        assert_eq!(
            metadata["legacy_metadata_raw"], non_object,
            "the original non-object JSON content must be preserved, not discarded"
        );
    }

    #[test]
    fn test_confirm_ots_anchor_atomic_survives_non_utf8_metadata() {
        let mut store = create_test_store();

        let anchor = create_test_anchor_ots();
        let anchor_id = store
            .store_anchor_returning_id(200, &anchor, "pending")
            .expect("Failed to store anchor");

        // Invalid UTF-8 bytes stored directly into the TEXT column. Before
        // this fix, `rusqlite` would refuse to fetch this as a `String`
        // and the whole confirmation transaction would roll back --
        // silently and permanently blocking this anchor's Bitcoin
        // confirmation.
        store
            .connection()
            .execute(
                "UPDATE anchors SET metadata = CAST(X'FF80' AS TEXT) WHERE id = ?1",
                rusqlite::params![anchor_id],
            )
            .expect("Failed to corrupt metadata");

        let upgraded_proof = vec![0x33, 0x44];
        let block_height = 900_000;
        let block_time = 1_700_000_000;

        store
            .confirm_ots_anchor_atomic(anchor_id, &upgraded_proof, block_height, block_time)
            .expect("confirmation must succeed even with non-UTF-8 metadata");

        let (status, token, metadata_json): (String, Vec<u8>, Option<String>) = store
            .connection()
            .query_row(
                "SELECT status, token, metadata FROM anchors WHERE id = ?1",
                rusqlite::params![anchor_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("row must still exist");
        assert_eq!(status, "confirmed", "status must become confirmed");
        assert_eq!(token, upgraded_proof, "token must be upgraded");

        let metadata: serde_json::Value =
            serde_json::from_str(&metadata_json.expect("metadata must be set"))
                .expect("resulting metadata must itself be valid JSON (and therefore valid UTF-8)");
        assert_eq!(
            metadata["bitcoin_block_height"],
            serde_json::json!(block_height)
        );
        assert_eq!(
            metadata["bitcoin_block_time"],
            serde_json::json!(block_time)
        );
        assert_eq!(metadata["status"], "confirmed");

        use base64::Engine;
        let expected_b64 = base64::engine::general_purpose::STANDARD.encode([0xFFu8, 0x80u8]);
        assert_eq!(
            metadata["legacy_metadata_raw_base64"], expected_b64,
            "non-UTF-8 original content must be preserved losslessly as base64"
        );
        assert!(
            metadata.get("legacy_metadata_raw").is_none(),
            "non-UTF-8 content must not be mixed into the plain-text legacy field"
        );
    }

    #[test]
    fn test_row_to_anchor_invalid_hash_length() {
        let store = create_test_store();

        // Insert anchor with invalid hash length (not 32 bytes). Must be
        // 'confirmed' or `get_anchors` would simply not find the row at all
        // (returning an empty, not erroring, result) instead of exercising
        // the row-parsing failure this test is about.
        store
            .connection()
            .execute(
                "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash, timestamp, token, status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    100i64,
                    "rfc3161",
                    "data_tree_root",
                    vec![0x01u8, 0x02, 0x03], // Invalid: only 3 bytes instead of 32
                    1_234_567_890_000_000_000i64,
                    vec![0xDEu8, 0xAD],
                    "confirmed",
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
                "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash, timestamp, token, status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    100i64,
                    "unknown_future_type", // Unknown anchor type
                    "data_tree_root",
                    [0x42u8; 32].as_slice(),
                    1_234_567_890_000_000_000i64,
                    vec![0xDEu8, 0xAD],
                    "confirmed",
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

        // `store_anchor` defaults to status='pending'; promote so
        // `get_anchors` (now 'confirmed'-only) can see it.
        store
            .connection()
            .execute(
                "UPDATE anchors SET status = 'confirmed' WHERE tree_size = 100",
                [],
            )
            .expect("Failed to promote anchor to confirmed");

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

    // Tests for get_tsa_anchor_covering

    #[test]
    fn should_return_none_when_no_tsa_anchors_exist() {
        let store = create_test_store();
        let result = store
            .get_tsa_anchor_covering(100)
            .expect("Query should not fail");
        assert_eq!(result, None);
    }

    #[test]
    fn should_return_exact_match_tsa_anchor() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();
        store
            .store_anchor_returning_id(100, &anchor, "confirmed")
            .expect("Failed to store anchor");

        let result = store
            .get_tsa_anchor_covering(100)
            .expect("Query should not fail");
        assert!(result.is_some());
        assert_eq!(result.unwrap().tree_size, 100);
    }

    #[test]
    fn should_return_minimum_covering_tsa_anchor() {
        let store = create_test_store();

        let mut a100 = create_test_anchor_rfc3161();
        a100.anchored_hash = [0x01u8; 32];
        store
            .store_anchor_returning_id(100, &a100, "confirmed")
            .expect("Failed to store anchor at 100");

        let mut a200 = create_test_anchor_rfc3161();
        a200.anchored_hash = [0x02u8; 32];
        store
            .store_anchor_returning_id(200, &a200, "confirmed")
            .expect("Failed to store anchor at 200");

        let mut a300 = create_test_anchor_rfc3161();
        a300.anchored_hash = [0x03u8; 32];
        store
            .store_anchor_returning_id(300, &a300, "confirmed")
            .expect("Failed to store anchor at 300");

        let result = store
            .get_tsa_anchor_covering(150)
            .expect("Query should not fail");
        assert!(result.is_some());
        assert_eq!(result.unwrap().tree_size, 200);
    }

    #[test]
    fn should_skip_pending_tsa_anchors() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();
        store
            .store_anchor_returning_id(100, &anchor, "pending")
            .expect("Failed to store pending anchor");

        let result = store
            .get_tsa_anchor_covering(50)
            .expect("Query should not fail");
        assert_eq!(result, None);
    }

    #[test]
    fn should_return_none_when_all_tsa_anchors_below_target() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();
        store
            .store_anchor_returning_id(50, &anchor, "confirmed")
            .expect("Failed to store anchor");

        let result = store
            .get_tsa_anchor_covering(100)
            .expect("Query should not fail");
        assert_eq!(result, None);
    }

    #[test]
    fn should_ignore_ots_anchors_in_tsa_query() {
        let store = create_test_store();
        let mut anchor = create_test_anchor_ots();
        anchor.target = "data_tree_root".to_string();
        store
            .store_anchor_returning_id(100, &anchor, "confirmed")
            .expect("Failed to store OTS anchor");

        let result = store
            .get_tsa_anchor_covering(50)
            .expect("Query should not fail");
        assert_eq!(result, None);
    }

    // Tests for get_ots_anchor_covering

    #[test]
    fn should_return_none_when_no_ots_anchors_exist() {
        let store = create_test_store();
        let result = store
            .get_ots_anchor_covering(0)
            .expect("Query should not fail");
        assert_eq!(result, None);
    }

    #[test]
    fn should_return_ots_anchor_covering_index() {
        let store = create_test_store();
        store
            .connection()
            .execute(
                "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash,
                        super_tree_size, timestamp, token, status, created_at)
                 VALUES (NULL, 'bitcoin_ots', 'super_root', ?1, 10, ?2, ?3, 'confirmed', ?4)",
                rusqlite::params![
                    [0x11u8; 32].as_slice(),
                    1_234_567_890_000_000_000i64,
                    vec![0xAAu8],
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                ],
            )
            .expect("Failed to insert OTS anchor");

        let result = store
            .get_ots_anchor_covering(5)
            .expect("Query should not fail");
        assert!(result.is_some());
        assert_eq!(result.unwrap().super_tree_size, Some(10));
    }

    #[test]
    fn should_return_minimum_covering_ots_anchor() {
        let store = create_test_store();

        for (hash_byte, super_size) in [(0x01u8, 5i64), (0x02, 10), (0x03, 20)] {
            store
                .connection()
                .execute(
                    "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash,
                            super_tree_size, timestamp, token, status, created_at)
                     VALUES (NULL, 'bitcoin_ots', 'super_root', ?1, ?2, ?3, ?4, 'confirmed', ?5)",
                    rusqlite::params![
                        [hash_byte; 32].as_slice(),
                        super_size,
                        1_234_567_890_000_000_000i64,
                        vec![hash_byte],
                        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                    ],
                )
                .expect("Failed to insert OTS anchor");
        }

        let result = store
            .get_ots_anchor_covering(7)
            .expect("Query should not fail");
        assert!(result.is_some());
        assert_eq!(result.unwrap().super_tree_size, Some(10));
    }

    #[test]
    fn should_not_cover_equal_index() {
        let store = create_test_store();
        store
            .connection()
            .execute(
                "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash,
                        super_tree_size, timestamp, token, status, created_at)
                 VALUES (NULL, 'bitcoin_ots', 'super_root', ?1, 5, ?2, ?3, 'confirmed', ?4)",
                rusqlite::params![
                    [0x22u8; 32].as_slice(),
                    1_234_567_890_000_000_000i64,
                    vec![0xBBu8],
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                ],
            )
            .expect("Failed to insert OTS anchor");

        let result = store
            .get_ots_anchor_covering(5)
            .expect("Query should not fail");
        assert_eq!(result, None);
    }

    #[test]
    fn should_skip_pending_ots_anchors() {
        let store = create_test_store();
        store
            .connection()
            .execute(
                "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash,
                        super_tree_size, timestamp, token, status, created_at)
                 VALUES (NULL, 'bitcoin_ots', 'super_root', ?1, 10, ?2, ?3, 'pending', ?4)",
                rusqlite::params![
                    [0x33u8; 32].as_slice(),
                    1_234_567_890_000_000_000i64,
                    vec![0xCCu8],
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                ],
            )
            .expect("Failed to insert pending OTS anchor");

        let result = store
            .get_ots_anchor_covering(5)
            .expect("Query should not fail");
        assert_eq!(result, None);
    }

    #[test]
    fn should_ignore_tsa_anchors_in_ots_query() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();
        store
            .store_anchor_returning_id(100, &anchor, "confirmed")
            .expect("Failed to store TSA anchor");

        let result = store
            .get_ots_anchor_covering(0)
            .expect("Query should not fail");
        assert_eq!(result, None);
    }

    #[test]
    fn should_only_match_super_root_target() {
        let store = create_test_store();
        // OTS anchor with data_tree_root target (wrong target)
        store
            .connection()
            .execute(
                "INSERT INTO anchors (tree_size, anchor_type, target, anchored_hash,
                        super_tree_size, timestamp, token, status, created_at)
                 VALUES (100, 'bitcoin_ots', 'data_tree_root', ?1, 10, ?2, ?3, 'confirmed', ?4)",
                rusqlite::params![
                    [0x44u8; 32].as_slice(),
                    1_234_567_890_000_000_000i64,
                    vec![0xDDu8],
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                ],
            )
            .expect("Failed to insert OTS anchor with wrong target");

        let result = store
            .get_ots_anchor_covering(5)
            .expect("Query should not fail");
        assert_eq!(result, None);
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

    // -------------------------------------------------------------------
    // Regression: rejecting an anchor must hide it from every read path
    // and atomically release the tree pointing at it, so the tree is
    // re-queued for anchoring instead of being stuck forever with a
    // non-NULL `tsa_anchor_id` that resolves to nothing usable.
    // -------------------------------------------------------------------

    #[test]
    fn test_reject_anchor_atomic_hides_row_and_requeues_tree() {
        let store = create_test_store();
        let hash = [9u8; 32];
        let origin_id = [0u8; 32];

        let mut anchor = create_test_anchor_rfc3161();
        anchor.anchored_hash = hash;
        anchor.tree_size = 100;

        let anchor_id = store
            .store_anchor_returning_id(100, &anchor, "confirmed")
            .expect("Failed to store anchor");

        let tree_id = store
            .create_active_tree(&origin_id, 0)
            .expect("Failed to create tree");
        store
            .connection()
            .execute(
                "UPDATE trees SET status = 'pending_bitcoin', end_size = ?1, root_hash = ?2, tsa_anchor_id = ?3 WHERE id = ?4",
                rusqlite::params![100i64, hash.as_slice(), anchor_id, tree_id],
            )
            .expect("Failed to link tree to anchor");

        // Sanity: everything is visible/linked before rejection, and the
        // tree is NOT in the anchoring queue (it already has an anchor).
        assert_eq!(store.get_anchors(100).unwrap().len(), 1);
        assert_eq!(store.get_latest_anchored_size().unwrap(), Some(100));
        assert_eq!(store.get_latest_tsa_anchored_size().unwrap(), Some(100));
        assert_eq!(
            store.get_tsa_anchor_for_hash(&hash).unwrap(),
            Some(anchor_id)
        );
        assert!(store
            .get_tsa_anchor_with_token_for_hash(&hash)
            .unwrap()
            .is_some());
        assert!(store
            .get_trees_pending_tsa()
            .unwrap()
            .iter()
            .all(|t| t.id != tree_id));

        store
            .reject_anchor_atomic(anchor_id, REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH)
            .expect("Failed to reject anchor");

        // The row must be gone from every read path...
        assert!(
            store.get_anchors(100).unwrap().is_empty(),
            "rejected anchor must not be served in a receipt"
        );
        assert_eq!(
            store.get_latest_anchored_size().unwrap(),
            None,
            "rejected anchor must not count toward the latest anchored size"
        );
        assert_eq!(
            store.get_latest_tsa_anchored_size().unwrap(),
            None,
            "rejected anchor must not count toward the latest TSA anchored size"
        );
        assert_eq!(
            store.get_tsa_anchor_for_hash(&hash).unwrap(),
            None,
            "rejected anchor must not be found for reuse by hash"
        );
        assert!(
            store
                .get_tsa_anchor_with_token_for_hash(&hash)
                .unwrap()
                .is_none(),
            "rejected anchor must not be found for reuse by hash (with token)"
        );

        // ...but the row itself must still exist, as an auditable record.
        let (status, metadata_json): (String, Option<String>) = store
            .connection()
            .query_row(
                "SELECT status, metadata FROM anchors WHERE id = ?1",
                rusqlite::params![anchor_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("rejected row must still exist");
        assert_eq!(status, "rejected");
        let metadata: serde_json::Value =
            serde_json::from_str(&metadata_json.expect("metadata must be set")).unwrap();
        assert_eq!(
            metadata["rejection_reason"],
            REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH
        );
        assert!(metadata["rejected_at"].is_number());
        // Pre-existing metadata fields (a normal JSON object) must survive
        // the merge, not be discarded.
        assert_eq!(metadata["tsa_url"], "https://example.com");

        // The tree must be detached and back in the anchoring queue.
        let tree = store
            .get_tree(tree_id)
            .expect("query should succeed")
            .expect("tree must still exist");
        assert!(
            tree.tsa_anchor_id.is_none(),
            "tree must be detached from the rejected anchor"
        );
        assert!(
            store
                .get_trees_pending_tsa()
                .unwrap()
                .iter()
                .any(|t| t.id == tree_id),
            "tree must be re-queued for TSA anchoring after its anchor is rejected"
        );
    }

    #[test]
    fn test_reject_anchor_atomic_is_idempotent() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();
        let anchor_id = store
            .store_anchor_returning_id(100, &anchor, "confirmed")
            .expect("Failed to store anchor");

        store
            .reject_anchor_atomic(anchor_id, REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH)
            .expect("first rejection should succeed");
        // Calling it again (e.g. the admin audit re-running) must not
        // error and must leave the row rejected.
        store
            .reject_anchor_atomic(anchor_id, REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH)
            .expect("second rejection should also succeed");

        let status: String = store
            .connection()
            .query_row(
                "SELECT status FROM anchors WHERE id = ?1",
                rusqlite::params![anchor_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "rejected");
    }

    // -------------------------------------------------------------------
    // Regression: `reject_anchor_atomic` must go through to completion no
    // matter what `metadata` currently holds. It used to build the merged
    // metadata via SQLite's `json_set`, which raises "malformed JSON" (and
    // rolls back the whole transaction, including the tree detach) if
    // `metadata` is not itself well-formed JSON -- leaving exactly the row
    // this method exists to reject sitting at `status = 'confirmed'`.
    // -------------------------------------------------------------------

    #[test]
    fn test_reject_anchor_atomic_survives_non_json_metadata() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();
        let anchor_id = store
            .store_anchor_returning_id(100, &anchor, "confirmed")
            .expect("Failed to store anchor");

        // Corrupt metadata to a string that is not valid JSON at all.
        let garbage = "not json at all {{{";
        store
            .connection()
            .execute(
                "UPDATE anchors SET metadata = ?1 WHERE id = ?2",
                rusqlite::params![garbage, anchor_id],
            )
            .expect("Failed to corrupt metadata");

        store
            .reject_anchor_atomic(anchor_id, REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH)
            .expect("rejection must succeed even with unparseable metadata");

        let (status, metadata_json): (String, Option<String>) = store
            .connection()
            .query_row(
                "SELECT status, metadata FROM anchors WHERE id = ?1",
                rusqlite::params![anchor_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row must still exist");
        assert_eq!(status, "rejected");

        let metadata: serde_json::Value =
            serde_json::from_str(&metadata_json.expect("metadata must be set"))
                .expect("resulting metadata must itself be valid JSON");
        assert_eq!(
            metadata["rejection_reason"],
            REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH
        );
        assert!(metadata["rejected_at"].is_number());
        assert_eq!(
            metadata["legacy_metadata_raw"], garbage,
            "the original unparseable content must be preserved, not discarded"
        );
    }

    #[test]
    fn test_reject_anchor_atomic_survives_non_object_json_metadata() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();
        let anchor_id = store
            .store_anchor_returning_id(100, &anchor, "confirmed")
            .expect("Failed to store anchor");

        // `metadata` is valid JSON, but a number, not an object.
        let non_object = "42";
        store
            .connection()
            .execute(
                "UPDATE anchors SET metadata = ?1 WHERE id = ?2",
                rusqlite::params![non_object, anchor_id],
            )
            .expect("Failed to corrupt metadata");

        store
            .reject_anchor_atomic(anchor_id, REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH)
            .expect("rejection must succeed even with non-object JSON metadata");

        let (status, metadata_json): (String, Option<String>) = store
            .connection()
            .query_row(
                "SELECT status, metadata FROM anchors WHERE id = ?1",
                rusqlite::params![anchor_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row must still exist");
        assert_eq!(status, "rejected");

        let metadata: serde_json::Value =
            serde_json::from_str(&metadata_json.expect("metadata must be set"))
                .expect("resulting metadata must itself be valid JSON");
        assert_eq!(
            metadata["rejection_reason"],
            REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH
        );
        assert!(metadata["rejected_at"].is_number());
        assert_eq!(
            metadata["legacy_metadata_raw"], non_object,
            "the original non-object JSON content must be preserved, not discarded"
        );
    }

    #[test]
    fn test_reject_anchor_atomic_survives_non_utf8_metadata() {
        let store = create_test_store();
        let anchor = create_test_anchor_rfc3161();
        let anchor_id = store
            .store_anchor_returning_id(100, &anchor, "confirmed")
            .expect("Failed to store anchor");

        // Invalid UTF-8 bytes stored directly into the TEXT column --
        // SQLite's TEXT affinity does not enforce valid UTF-8, so this can
        // happen from outside Rust entirely (a manual DB edit, a buggy
        // migration, etc). `rusqlite` itself would refuse to fetch this as
        // a `String`, which is exactly the failure mode this test guards
        // against.
        store
            .connection()
            .execute(
                "UPDATE anchors SET metadata = CAST(X'FF80' AS TEXT) WHERE id = ?1",
                rusqlite::params![anchor_id],
            )
            .expect("Failed to corrupt metadata");

        store
            .reject_anchor_atomic(anchor_id, REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH)
            .expect("rejection must succeed even with non-UTF-8 metadata");

        let (status, metadata_json): (String, Option<String>) = store
            .connection()
            .query_row(
                "SELECT status, metadata FROM anchors WHERE id = ?1",
                rusqlite::params![anchor_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row must still exist");
        assert_eq!(status, "rejected");

        let metadata: serde_json::Value =
            serde_json::from_str(&metadata_json.expect("metadata must be set"))
                .expect("resulting metadata must itself be valid JSON (and therefore valid UTF-8)");
        assert_eq!(
            metadata["rejection_reason"],
            REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH
        );
        assert!(metadata["rejected_at"].is_number());

        use base64::Engine;
        let expected_b64 = base64::engine::general_purpose::STANDARD.encode([0xFFu8, 0x80u8]);
        assert_eq!(
            metadata["legacy_metadata_raw_base64"], expected_b64,
            "non-UTF-8 original content must be preserved losslessly as base64"
        );
        assert!(
            metadata.get("legacy_metadata_raw").is_none(),
            "non-UTF-8 content must not be mixed into the plain-text legacy field"
        );
    }

    #[test]
    fn test_get_anchors_excludes_pending_ots() {
        let store = create_test_store();
        let ots_anchor = create_test_anchor_ots();

        // An in-flight (not yet Bitcoin-confirmed) OTS anchor.
        store
            .store_anchor_returning_id(200, &ots_anchor, "pending")
            .expect("Failed to store pending OTS anchor");

        let anchors = store.get_anchors(200).expect("Failed to get anchors");
        assert!(
            anchors.is_empty(),
            "an unconfirmed OTS anchor must never be served in a receipt"
        );

        // Once confirmed, it becomes visible.
        store
            .connection()
            .execute(
                "UPDATE anchors SET status = 'confirmed' WHERE tree_size = 200",
                [],
            )
            .expect("Failed to confirm anchor");
        let anchors = store.get_anchors(200).expect("Failed to get anchors");
        assert_eq!(anchors.len(), 1);
    }
}
