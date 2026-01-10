// File: src/storage/sqlite/entries.rs

use super::config::StorageStats;
use super::convert;
use super::store::SqliteStore;
use super::tree;
use crate::error::{ServerError, ServerResult};
use crate::traits::{AppendParams, AppendResult, Entry};
use rusqlite::{Connection, Transaction, params};
use uuid::Uuid;

impl SqliteStore {
    /// Append a new entry to the log
    pub(crate) fn append_impl(&self, params: AppendParams) -> ServerResult<AppendResult> {
        let mut conn = self.get_conn()?;
        let tx = conn.transaction()?;

        let result = append_single(&tx, &params, self.get_origin())?;

        tx.commit()?;
        Ok(result)
    }

    /// Atomically append a batch of entries to the log
    pub(crate) fn append_batch_impl(
        &self,
        params: Vec<AppendParams>,
    ) -> ServerResult<Vec<AppendResult>> {
        if params.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self.get_conn()?;
        let tx = conn.transaction()?;

        let results = append_batch_inner(&tx, &params, self.get_origin())?;

        tx.commit()?;
        Ok(results)
    }

    /// Get an entry by its ID
    pub(crate) fn get_entry_impl(&self, id: &Uuid) -> ServerResult<Entry> {
        let conn = self.get_conn()?;
        get_entry_by_id(&conn, id)
    }

    /// Get an entry by leaf index
    pub(crate) fn get_entry_by_index_impl(&self, index: u64) -> ServerResult<Entry> {
        let conn = self.get_conn()?;
        get_entry_by_leaf_index(&conn, index)
    }

    /// Get an entry by external ID
    pub(crate) fn get_entry_by_external_id_impl(&self, external_id: &str) -> ServerResult<Entry> {
        let conn = self.get_conn()?;
        get_entry_by_ext_id(&conn, external_id)
    }
}

/// Append a single entry within a transaction
fn append_single(
    tx: &Transaction,
    params: &AppendParams,
    origin: [u8; 32],
) -> ServerResult<AppendResult> {
    let entry_id = Uuid::new_v4();
    let timestamp = chrono::Utc::now();
    let timestamp_nanos = timestamp.timestamp_nanos_opt().unwrap_or(0);

    // Compute leaf hash
    let leaf_hash = atl_core::compute_leaf_hash(&params.payload_hash, &params.metadata_hash);

    // Get current tree size
    let current_size: i64 = tx.query_row(
        "SELECT COALESCE(MAX(leaf_index), -1) + 1 FROM entries WHERE leaf_index IS NOT NULL",
        [],
        |row| row.get(0),
    )?;

    let leaf_index = current_size as u64;

    // Get active tree ID and check if this is first entry
    let active_tree = get_active_tree_info(tx)?;

    // Insert entry
    tx.execute(
        "INSERT INTO entries (id, payload_hash, metadata_hash, metadata_cleartext, external_id, leaf_index, leaf_hash, tree_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            entry_id.to_string(),
            params.payload_hash.as_slice(),
            params.metadata_hash.as_slice(),
            params.metadata_cleartext.as_ref().map(|v| v.to_string()),
            params.external_id,
            leaf_index as i64,
            leaf_hash.as_slice(),
            active_tree.map(|(id, _)| id),
            timestamp_nanos,
        ],
    )?;

    // Update first_entry_at if this is the first entry in the tree
    if let Some((tree_id, first_entry_at)) = active_tree {
        if first_entry_at.is_none() {
            tx.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE id = ?2 AND first_entry_at IS NULL",
                params![timestamp_nanos, tree_id],
            )?;
        }
    }

    // Update tree nodes
    tree::update_tree_nodes(tx, leaf_index, &leaf_hash)?;

    // Compute tree head
    let new_tree_size = leaf_index + 1;
    let tree_head = tree::compute_tree_head(tx, new_tree_size, origin)?;

    // Update config
    update_tree_config(tx, new_tree_size, &tree_head.root_hash, timestamp_nanos)?;

    // Compute inclusion proof
    let inclusion_proof = tree::compute_inclusion_proof(tx, leaf_index, new_tree_size)?;

    Ok(AppendResult {
        id: entry_id,
        leaf_index,
        tree_head,
        inclusion_proof,
        timestamp,
        #[cfg(feature = "rfc3161")]
        tsa_anchor: None,
    })
}

/// Append a batch of entries within a transaction
fn append_batch_inner(
    tx: &Transaction,
    params: &[AppendParams],
    origin: [u8; 32],
) -> ServerResult<Vec<AppendResult>> {
    let batch_size = params.len();
    let timestamp = chrono::Utc::now();
    let timestamp_nanos = timestamp.timestamp_nanos_opt().unwrap_or(0);

    // Get starting tree size
    let start_size: i64 = tx.query_row(
        "SELECT COALESCE(MAX(leaf_index), -1) + 1 FROM entries WHERE leaf_index IS NOT NULL",
        [],
        |row| row.get(0),
    )?;

    // Get active tree ID and update first_entry_at if needed
    let active_tree = get_active_tree_info(tx)?;
    if let Some((tree_id, first_entry_at)) = &active_tree {
        if first_entry_at.is_none() {
            tx.execute(
                "UPDATE trees SET first_entry_at = ?1 WHERE id = ?2 AND first_entry_at IS NULL",
                params![timestamp_nanos, tree_id],
            )?;
        }
    }

    let mut results = Vec::with_capacity(batch_size);
    let mut leaf_hashes: Vec<(u64, [u8; 32])> = Vec::with_capacity(batch_size);

    // Phase 1: Insert all entries and compute leaf hashes
    for (i, p) in params.iter().enumerate() {
        let entry_id = Uuid::new_v4();
        let leaf_index = (start_size as u64) + (i as u64);
        let leaf_hash = atl_core::compute_leaf_hash(&p.payload_hash, &p.metadata_hash);

        tx.execute(
            "INSERT INTO entries (id, payload_hash, metadata_hash, metadata_cleartext, external_id, leaf_index, leaf_hash, tree_id, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                entry_id.to_string(),
                p.payload_hash.as_slice(),
                p.metadata_hash.as_slice(),
                p.metadata_cleartext.as_ref().map(|v| v.to_string()),
                p.external_id.as_ref(),
                leaf_index as i64,
                leaf_hash.as_slice(),
                active_tree.as_ref().map(|(id, _)| *id),
                timestamp_nanos,
            ],
        )?;

        // Store leaf at level 0
        tx.execute(
            "INSERT OR REPLACE INTO tree_nodes (level, idx, hash) VALUES (0, ?1, ?2)",
            params![leaf_index as i64, leaf_hash.as_slice()],
        )?;

        leaf_hashes.push((leaf_index, leaf_hash));
        results.push((entry_id, leaf_index));
    }

    // Phase 2: Batch update tree nodes
    tree::batch_update_tree_nodes(tx, &leaf_hashes, start_size as u64)?;

    // Phase 3: Compute final tree head
    let new_tree_size = (start_size as u64) + (batch_size as u64);
    let tree_head = tree::compute_tree_head(tx, new_tree_size, origin)?;

    // Update config
    update_tree_config(tx, new_tree_size, &tree_head.root_hash, timestamp_nanos)?;

    // Phase 4: Generate inclusion proofs
    results
        .into_iter()
        .map(|(id, leaf_index)| {
            let inclusion_proof = tree::compute_inclusion_proof(tx, leaf_index, new_tree_size)?;
            Ok(AppendResult {
                id,
                leaf_index,
                tree_head: tree_head.clone(),
                inclusion_proof,
                timestamp,
                #[cfg(feature = "rfc3161")]
                tsa_anchor: None,
            })
        })
        .collect()
}

fn get_active_tree_info(tx: &Transaction) -> ServerResult<Option<(i64, Option<i64>)>> {
    match tx.query_row(
        "SELECT id, first_entry_at FROM trees WHERE status = 'active'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ) {
        Ok(result) => Ok(Some(result)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn update_tree_config(
    tx: &Transaction,
    tree_size: u64,
    root_hash: &[u8; 32],
    timestamp: i64,
) -> ServerResult<()> {
    tx.execute(
        "INSERT OR REPLACE INTO atl_config (key, value, updated_at) VALUES ('tree_size', ?1, ?2)",
        params![tree_size.to_string(), timestamp],
    )?;
    tx.execute(
        "INSERT OR REPLACE INTO atl_config (key, value, updated_at) VALUES ('root_hash', ?1, ?2)",
        params![hex::encode(root_hash), timestamp],
    )?;
    Ok(())
}

fn get_entry_by_id(conn: &Connection, id: &Uuid) -> ServerResult<Entry> {
    let row = conn.query_row(
        "SELECT id, payload_hash, metadata_hash, metadata_cleartext, external_id, leaf_index, leaf_hash, created_at
         FROM entries WHERE id = ?1",
        params![id.to_string()],
        convert::row_to_entry,
    ).map_err(|_| ServerError::EntryNotFound(id.to_string()))?;
    Ok(row)
}

fn get_entry_by_leaf_index(conn: &Connection, index: u64) -> ServerResult<Entry> {
    let row = conn.query_row(
        "SELECT id, payload_hash, metadata_hash, metadata_cleartext, external_id, leaf_index, leaf_hash, created_at
         FROM entries WHERE leaf_index = ?1",
        params![index as i64],
        convert::row_to_entry,
    ).map_err(|_| ServerError::EntryNotFound(format!("index {}", index)))?;
    Ok(row)
}

fn get_entry_by_ext_id(conn: &Connection, external_id: &str) -> ServerResult<Entry> {
    let row = conn.query_row(
        "SELECT id, payload_hash, metadata_hash, metadata_cleartext, external_id, leaf_index, leaf_hash, created_at
         FROM entries WHERE external_id = ?1",
        params![external_id],
        convert::row_to_entry,
    ).map_err(|_| ServerError::EntryNotFound(format!("external_id {}", external_id)))?;
    Ok(row)
}

/// Get database statistics
pub fn get_stats(conn: &Connection) -> ServerResult<StorageStats> {
    let entry_count: u64 = conn.query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))?;
    let tree_size: u64 = conn
        .query_row(
            "SELECT COALESCE(value, '0') FROM atl_config WHERE key = 'tree_size'",
            [],
            |r| r.get::<_, String>(0).map(|s| s.parse().unwrap_or(0)),
        )
        .unwrap_or(0);
    let node_count: u64 = conn.query_row("SELECT COUNT(*) FROM tree_nodes", [], |r| r.get(0))?;
    let checkpoint_count: u64 =
        conn.query_row("SELECT COUNT(*) FROM checkpoints", [], |r| r.get(0))?;
    let anchor_count: u64 = conn.query_row("SELECT COUNT(*) FROM anchors", [], |r| r.get(0))?;

    Ok(StorageStats {
        entry_count,
        tree_size,
        node_count,
        checkpoint_count,
        anchor_count,
        file_size: 0, // TODO: implement
    })
}
