// File: src/storage/sqlite/checkpoints.rs

use super::convert::row_to_checkpoint;
use super::store::SqliteStore;
use crate::error::ServerResult;
use rusqlite::params;

impl SqliteStore {
    /// Store a signed checkpoint
    pub(crate) fn store_checkpoint_impl(
        &self,
        checkpoint: &atl_core::Checkpoint,
    ) -> ServerResult<()> {
        let conn = self.get_conn()?;
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        conn.execute(
            "INSERT OR REPLACE INTO checkpoints (tree_size, origin, timestamp, root_hash, signature, key_id, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                checkpoint.tree_size as i64,
                checkpoint.origin.as_slice(),
                checkpoint.timestamp as i64,
                checkpoint.root_hash.as_slice(),
                checkpoint.signature.as_slice(),
                checkpoint.key_id.as_slice(),
                now,
            ],
        )?;

        Ok(())
    }

    /// Get the most recent checkpoint
    pub(crate) fn get_latest_checkpoint_impl(&self) -> ServerResult<Option<atl_core::Checkpoint>> {
        let conn = self.get_conn()?;

        let result = conn.query_row(
            "SELECT tree_size, origin, timestamp, root_hash, signature, key_id
             FROM checkpoints ORDER BY tree_size DESC LIMIT 1",
            [],
            row_to_checkpoint,
        );

        match result {
            Ok(cp) => Ok(Some(cp)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get checkpoint by tree size
    pub(crate) fn get_checkpoint_by_size_impl(
        &self,
        tree_size: u64,
    ) -> ServerResult<Option<atl_core::Checkpoint>> {
        let conn = self.get_conn()?;

        let result = conn.query_row(
            "SELECT tree_size, origin, timestamp, root_hash, signature, key_id
             FROM checkpoints WHERE tree_size = ?1",
            params![tree_size as i64],
            row_to_checkpoint,
        );

        match result {
            Ok(cp) => Ok(Some(cp)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get checkpoint closest to (but not exceeding) a tree size
    pub(crate) fn get_checkpoint_at_or_before_impl(
        &self,
        tree_size: u64,
    ) -> ServerResult<Option<atl_core::Checkpoint>> {
        let conn = self.get_conn()?;

        let result = conn.query_row(
            "SELECT tree_size, origin, timestamp, root_hash, signature, key_id
             FROM checkpoints WHERE tree_size <= ?1 ORDER BY tree_size DESC LIMIT 1",
            params![tree_size as i64],
            row_to_checkpoint,
        );

        match result {
            Ok(cp) => Ok(Some(cp)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
