// File: src/storage/chain_index/archive.rs

//! Archival functionality for Chain Index
//!
//! **NOTE:** These are STUBS. Actual archival implementation is future work.
//! All functions return Ok(()) or placeholder values.

use rusqlite::params;

use super::store::{ChainIndex, ChainTreeRecord};

/// Archive location types (future use)
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ArchiveLocation {
    /// S3 bucket path
    S3 { bucket: String, key: String },
    /// Local disk path
    Disk { path: String },
}

impl ArchiveLocation {
    /// Serialize to string for storage
    #[allow(dead_code)]
    pub fn to_uri(&self) -> String {
        match self {
            ArchiveLocation::S3 { bucket, key } => format!("s3://{}/{}", bucket, key),
            ArchiveLocation::Disk { path } => format!("file://{}", path),
        }
    }

    /// Parse from URI string
    #[allow(dead_code)]
    pub fn from_uri(uri: &str) -> Option<Self> {
        if let Some(rest) = uri.strip_prefix("s3://") {
            let parts: Vec<&str> = rest.splitn(2, '/').collect();
            if parts.len() == 2 {
                return Some(ArchiveLocation::S3 {
                    bucket: parts[0].to_string(),
                    key: parts[1].to_string(),
                });
            }
        } else if let Some(rest) = uri.strip_prefix("file://") {
            return Some(ArchiveLocation::Disk {
                path: rest.to_string(),
            });
        }
        None
    }
}

impl ChainIndex {
    /// Export a tree to archive storage
    ///
    /// **STUB:** This function is a placeholder. In the future, it will:
    /// 1. Serialize tree data (entries, nodes) to a compact format
    /// 2. Upload to S3 or write to disk
    /// 3. Update Chain Index with archive_location
    /// 4. Mark tree as 'archived'
    ///
    /// # Arguments
    /// * `tree_id` - The tree to archive
    /// * `location` - Where to store the archive
    ///
    /// # Returns
    /// Ok(()) on success (currently always succeeds as it's a stub)
    #[allow(dead_code)]
    pub fn export_tree_to_archive(
        &self,
        tree_id: i64,
        _location: &ArchiveLocation,
    ) -> Result<(), String> {
        tracing::warn!(
            tree_id = tree_id,
            "export_tree_to_archive() is a STUB - not implemented yet"
        );

        // TODO: implement archival
        // 1. Get tree data from main DB
        // 2. Get all entries for this tree
        // 3. Serialize to archive format
        // 4. Upload to location
        // 5. Update Chain Index: status = 'archived', archive_location = uri

        Ok(())
    }

    /// Load a tree from archive storage
    ///
    /// **STUB:** This function is a placeholder. In the future, it will:
    /// 1. Download archive from S3/disk
    /// 2. Deserialize tree data
    /// 3. Return tree structure for verification
    ///
    /// # Arguments
    /// * `tree_id` - The tree to load
    ///
    /// # Returns
    /// Currently returns None (stub)
    #[allow(dead_code)]
    pub fn load_tree_from_archive(&self, tree_id: i64) -> Result<Option<ArchivedTree>, String> {
        tracing::warn!(
            tree_id = tree_id,
            "load_tree_from_archive() is a STUB - not implemented yet"
        );

        // TODO: implement archive loading
        // 1. Get archive_location from Chain Index
        // 2. Download from S3/disk
        // 3. Deserialize
        // 4. Return ArchivedTree structure

        Ok(None)
    }

    /// Verify an entry in an archived tree
    ///
    /// **STUB:** This function is a placeholder. In the future, it will:
    /// 1. Load tree from archive
    /// 2. Generate inclusion proof for the entry
    /// 3. Verify proof against stored root_hash
    ///
    /// # Arguments
    /// * `tree_id` - The archived tree
    /// * `leaf_index` - The entry to verify
    /// * `expected_hash` - Expected leaf hash
    ///
    /// # Returns
    /// Currently returns Ok(false) (stub)
    #[allow(dead_code)]
    pub fn verify_from_archive(
        &self,
        tree_id: i64,
        leaf_index: u64,
        _expected_hash: &[u8; 32],
    ) -> Result<bool, String> {
        tracing::warn!(
            tree_id = tree_id,
            leaf_index = leaf_index,
            "verify_from_archive() is a STUB - not implemented yet"
        );

        // TODO: implement archive verification
        // 1. Load tree from archive
        // 2. Get inclusion proof for leaf_index
        // 3. Verify: compute_root(leaf_hash, proof) == tree.root_hash
        // 4. Return verification result

        Ok(false)
    }

    /// Mark a tree as archived in the index
    ///
    /// Updates status and sets archive_location.
    /// This is a real operation (not a stub) - it updates the Chain Index.
    #[allow(dead_code)]
    pub fn mark_tree_archived(
        &self,
        tree_id: i64,
        location: &ArchiveLocation,
    ) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();

        self.conn.execute(
            "UPDATE trees SET status = 'archived', archive_location = ?1, archived_at = ?2 WHERE tree_id = ?3",
            params![location.to_uri(), now, tree_id],
        )?;

        Ok(())
    }

    /// Get trees that are ready for archival
    ///
    /// Returns closed trees that have Bitcoin confirmation but not yet archived.
    #[allow(dead_code)]
    pub fn get_trees_ready_for_archival(&self) -> rusqlite::Result<Vec<ChainTreeRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT tree_id, origin_id, root_hash, tree_size, prev_tree_id, genesis_leaf_hash,
                    status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
             FROM trees WHERE status = 'closed' AND bitcoin_txid IS NOT NULL ORDER BY tree_id ASC",
        )?;
        let rows = stmt.query_map([], super::store::row_to_chain_tree)?;
        rows.collect()
    }
}

/// Archived tree structure (placeholder)
///
/// **STUB:** This struct will hold deserialized tree data in the future.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ArchivedTree {
    pub tree_id: i64,
    pub root_hash: [u8; 32],
    pub tree_size: u64,
}
