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
            "SELECT tree_id, origin_id, root_hash, tree_size, data_tree_index,
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

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::store::ChainTreeStatus;
    use rusqlite::Connection;
    use tempfile::TempDir;

    /// Helper: create a test ChainIndex with temp database
    fn create_test_index() -> (ChainIndex, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let conn = Connection::open(&db_path).unwrap();

        // Initialize schema
        conn.execute(
            "CREATE TABLE trees (
                tree_id INTEGER PRIMARY KEY,
                origin_id TEXT NOT NULL,
                root_hash BLOB NOT NULL,
                tree_size INTEGER NOT NULL,
                data_tree_index INTEGER NOT NULL,
                status TEXT NOT NULL,
                bitcoin_txid TEXT,
                archive_location TEXT,
                created_at TEXT NOT NULL,
                closed_at TEXT,
                archived_at TEXT
            )",
            [],
        )
        .unwrap();

        let index = ChainIndex { conn };
        (index, temp_dir)
    }

    /// Helper: insert a test tree record
    fn insert_test_tree(
        index: &ChainIndex,
        tree_id: i64,
        status: &str,
        bitcoin_txid: Option<&str>,
        archive_location: Option<&str>,
    ) {
        let origin_id = vec![0u8; 32];
        let root_hash = vec![0u8; 32];
        let created_at = chrono::Utc::now().to_rfc3339();

        index
            .conn
            .execute(
                "INSERT INTO trees (tree_id, origin_id, root_hash, tree_size, data_tree_index,
                                   status, bitcoin_txid, archive_location, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    tree_id,
                    origin_id,
                    root_hash,
                    100i64,
                    0i64,
                    status,
                    bitcoin_txid,
                    archive_location,
                    created_at
                ],
            )
            .unwrap();
    }

    // --- ArchiveLocation Tests ---

    #[test]
    fn test_archive_location_s3_to_uri() {
        let location = ArchiveLocation::S3 {
            bucket: "my-bucket".to_string(),
            key: "path/to/tree.archive".to_string(),
        };
        assert_eq!(
            location.to_uri(),
            "s3://my-bucket/path/to/tree.archive"
        );
    }

    #[test]
    fn test_archive_location_disk_to_uri() {
        let location = ArchiveLocation::Disk {
            path: "/var/archives/tree.archive".to_string(),
        };
        assert_eq!(location.to_uri(), "file:///var/archives/tree.archive");
    }

    #[test]
    fn test_archive_location_from_uri_s3_valid() {
        let uri = "s3://my-bucket/path/to/archive";
        let location = ArchiveLocation::from_uri(uri).unwrap();

        match location {
            ArchiveLocation::S3 { bucket, key } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(key, "path/to/archive");
            }
            _ => panic!("Expected S3 variant"),
        }
    }

    #[test]
    fn test_archive_location_from_uri_s3_nested_path() {
        let uri = "s3://bucket/deep/nested/path/file.tar.gz";
        let location = ArchiveLocation::from_uri(uri).unwrap();

        match location {
            ArchiveLocation::S3 { bucket, key } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(key, "deep/nested/path/file.tar.gz");
            }
            _ => panic!("Expected S3 variant"),
        }
    }

    #[test]
    fn test_archive_location_from_uri_disk_valid() {
        let uri = "file:///mnt/archives/tree.bin";
        let location = ArchiveLocation::from_uri(uri).unwrap();

        match location {
            ArchiveLocation::Disk { path } => {
                assert_eq!(path, "/mnt/archives/tree.bin");
            }
            _ => panic!("Expected Disk variant"),
        }
    }

    #[test]
    fn test_archive_location_from_uri_invalid_scheme() {
        assert!(ArchiveLocation::from_uri("http://example.com").is_none());
        assert!(ArchiveLocation::from_uri("ftp://files.com").is_none());
        assert!(ArchiveLocation::from_uri("invalid").is_none());
    }

    #[test]
    fn test_archive_location_from_uri_s3_missing_key() {
        // Only bucket, no key after slash
        assert!(ArchiveLocation::from_uri("s3://bucket").is_none());
    }

    #[test]
    fn test_archive_location_from_uri_empty() {
        assert!(ArchiveLocation::from_uri("").is_none());
    }

    #[test]
    fn test_archive_location_roundtrip_s3() {
        let original = ArchiveLocation::S3 {
            bucket: "test-bucket".to_string(),
            key: "test/key.archive".to_string(),
        };
        let uri = original.to_uri();
        let parsed = ArchiveLocation::from_uri(&uri).unwrap();

        match parsed {
            ArchiveLocation::S3 { bucket, key } => {
                assert_eq!(bucket, "test-bucket");
                assert_eq!(key, "test/key.archive");
            }
            _ => panic!("Expected S3 variant"),
        }
    }

    #[test]
    fn test_archive_location_roundtrip_disk() {
        let original = ArchiveLocation::Disk {
            path: "/tmp/test.archive".to_string(),
        };
        let uri = original.to_uri();
        let parsed = ArchiveLocation::from_uri(&uri).unwrap();

        match parsed {
            ArchiveLocation::Disk { path } => {
                assert_eq!(path, "/tmp/test.archive");
            }
            _ => panic!("Expected Disk variant"),
        }
    }

    // --- ChainIndex::export_tree_to_archive (STUB) ---

    #[test]
    fn test_export_tree_to_archive_stub_success() {
        let (index, _temp) = create_test_index();
        let location = ArchiveLocation::Disk {
            path: "/tmp/test.archive".to_string(),
        };

        let result = index.export_tree_to_archive(1, &location);
        assert!(result.is_ok(), "Stub should always succeed");
    }

    #[test]
    fn test_export_tree_to_archive_stub_with_s3() {
        let (index, _temp) = create_test_index();
        let location = ArchiveLocation::S3 {
            bucket: "archive-bucket".to_string(),
            key: "tree_1.tar.gz".to_string(),
        };

        let result = index.export_tree_to_archive(42, &location);
        assert!(result.is_ok(), "Stub should succeed with S3 location");
    }

    // --- ChainIndex::load_tree_from_archive (STUB) ---

    #[test]
    fn test_load_tree_from_archive_stub_returns_none() {
        let (index, _temp) = create_test_index();

        let result = index.load_tree_from_archive(1).unwrap();
        assert!(
            result.is_none(),
            "Stub should return None (not implemented)"
        );
    }

    #[test]
    fn test_load_tree_from_archive_stub_different_ids() {
        let (index, _temp) = create_test_index();

        for tree_id in [0, 1, 100, i64::MAX] {
            let result = index.load_tree_from_archive(tree_id).unwrap();
            assert!(result.is_none(), "Stub should return None for tree_id={}", tree_id);
        }
    }

    // --- ChainIndex::verify_from_archive (STUB) ---

    #[test]
    fn test_verify_from_archive_stub_returns_false() {
        let (index, _temp) = create_test_index();
        let hash = [0u8; 32];

        let result = index.verify_from_archive(1, 0, &hash).unwrap();
        assert!(!result, "Stub should return false (not implemented)");
    }

    #[test]
    fn test_verify_from_archive_stub_various_params() {
        let (index, _temp) = create_test_index();
        let hash = [255u8; 32];

        for (tree_id, leaf_index) in [(0, 0), (1, 100), (42, 9999)] {
            let result = index.verify_from_archive(tree_id, leaf_index, &hash).unwrap();
            assert!(
                !result,
                "Stub should return false for tree={}, leaf={}",
                tree_id, leaf_index
            );
        }
    }

    // --- ChainIndex::mark_tree_archived (Real operation) ---

    #[test]
    fn test_mark_tree_archived_success() {
        let (index, _temp) = create_test_index();
        insert_test_tree(&index, 1, "closed", Some("btc_tx_123"), None);

        let location = ArchiveLocation::S3 {
            bucket: "archive".to_string(),
            key: "tree_1.tar".to_string(),
        };

        let result = index.mark_tree_archived(1, &location);
        assert!(result.is_ok(), "Should update tree successfully");

        // Verify database was updated
        let mut stmt = index
            .conn
            .prepare("SELECT status, archive_location, archived_at FROM trees WHERE tree_id = ?1")
            .unwrap();
        let mut rows = stmt.query(params![1]).unwrap();
        let row = rows.next().unwrap().unwrap();

        let status: String = row.get(0).unwrap();
        let archive_loc: Option<String> = row.get(1).unwrap();
        let archived_at: Option<String> = row.get(2).unwrap();

        assert_eq!(status, "archived");
        assert_eq!(archive_loc.unwrap(), "s3://archive/tree_1.tar");
        assert!(archived_at.is_some());
    }

    #[test]
    fn test_mark_tree_archived_with_disk_location() {
        let (index, _temp) = create_test_index();
        insert_test_tree(&index, 2, "closed", None, None);

        let location = ArchiveLocation::Disk {
            path: "/mnt/storage/tree2.bin".to_string(),
        };

        let result = index.mark_tree_archived(2, &location);
        assert!(result.is_ok());

        // Verify archive_location
        let mut stmt = index
            .conn
            .prepare("SELECT archive_location FROM trees WHERE tree_id = ?1")
            .unwrap();
        let loc: String = stmt.query_row(params![2], |row| row.get(0)).unwrap();

        assert_eq!(loc, "file:///mnt/storage/tree2.bin");
    }

    #[test]
    fn test_mark_tree_archived_nonexistent_tree() {
        let (index, _temp) = create_test_index();

        let location = ArchiveLocation::Disk {
            path: "/tmp/test.bin".to_string(),
        };

        // Should not fail (UPDATE just affects 0 rows)
        let result = index.mark_tree_archived(999, &location);
        assert!(result.is_ok(), "UPDATE on non-existent tree should not error");
    }

    #[test]
    fn test_mark_tree_archived_already_archived() {
        let (index, _temp) = create_test_index();
        insert_test_tree(
            &index,
            3,
            "archived",
            Some("btc_tx"),
            Some("s3://old-bucket/old.tar"),
        );

        let new_location = ArchiveLocation::S3 {
            bucket: "new-bucket".to_string(),
            key: "new.tar".to_string(),
        };

        // Should succeed (re-archiving)
        let result = index.mark_tree_archived(3, &new_location);
        assert!(result.is_ok());

        // Verify it was updated
        let mut stmt = index
            .conn
            .prepare("SELECT archive_location FROM trees WHERE tree_id = ?1")
            .unwrap();
        let loc: String = stmt.query_row(params![3], |row| row.get(0)).unwrap();

        assert_eq!(loc, "s3://new-bucket/new.tar");
    }

    // --- ChainIndex::get_trees_ready_for_archival ---

    #[test]
    fn test_get_trees_ready_for_archival_empty() {
        let (index, _temp) = create_test_index();

        let result = index.get_trees_ready_for_archival().unwrap();
        assert!(result.is_empty(), "No trees, should return empty vec");
    }

    #[test]
    fn test_get_trees_ready_for_archival_single_tree() {
        let (index, _temp) = create_test_index();
        insert_test_tree(&index, 1, "closed", Some("btc_tx_abc"), None);

        let result = index.get_trees_ready_for_archival().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].tree_id, 1);
        assert_eq!(result[0].status, ChainTreeStatus::Closed);
        assert_eq!(result[0].bitcoin_txid, Some("btc_tx_abc".to_string()));
    }

    #[test]
    fn test_get_trees_ready_for_archival_multiple_trees() {
        let (index, _temp) = create_test_index();
        insert_test_tree(&index, 1, "closed", Some("btc_1"), None);
        insert_test_tree(&index, 2, "closed", Some("btc_2"), None);
        insert_test_tree(&index, 3, "closed", Some("btc_3"), None);

        let result = index.get_trees_ready_for_archival().unwrap();
        assert_eq!(result.len(), 3);

        // Verify order (should be ASC by tree_id)
        assert_eq!(result[0].tree_id, 1);
        assert_eq!(result[1].tree_id, 2);
        assert_eq!(result[2].tree_id, 3);
    }

    #[test]
    fn test_get_trees_ready_for_archival_filters_open_trees() {
        let (index, _temp) = create_test_index();
        insert_test_tree(&index, 1, "open", Some("btc_1"), None);
        insert_test_tree(&index, 2, "closed", Some("btc_2"), None);

        let result = index.get_trees_ready_for_archival().unwrap();
        assert_eq!(result.len(), 1, "Should exclude open trees");
        assert_eq!(result[0].tree_id, 2);
    }

    #[test]
    fn test_get_trees_ready_for_archival_filters_no_bitcoin_txid() {
        let (index, _temp) = create_test_index();
        insert_test_tree(&index, 1, "closed", None, None);
        insert_test_tree(&index, 2, "closed", Some("btc_tx"), None);

        let result = index.get_trees_ready_for_archival().unwrap();
        assert_eq!(result.len(), 1, "Should exclude trees without bitcoin_txid");
        assert_eq!(result[0].tree_id, 2);
    }

    #[test]
    fn test_get_trees_ready_for_archival_filters_already_archived() {
        let (index, _temp) = create_test_index();
        insert_test_tree(&index, 1, "archived", Some("btc_1"), Some("s3://bucket/tree1"));
        insert_test_tree(&index, 2, "closed", Some("btc_2"), None);

        let result = index.get_trees_ready_for_archival().unwrap();
        assert_eq!(result.len(), 1, "Should exclude already archived trees");
        assert_eq!(result[0].tree_id, 2);
    }

    #[test]
    fn test_get_trees_ready_for_archival_ordered_by_tree_id() {
        let (index, _temp) = create_test_index();
        insert_test_tree(&index, 5, "closed", Some("btc_5"), None);
        insert_test_tree(&index, 1, "closed", Some("btc_1"), None);
        insert_test_tree(&index, 3, "closed", Some("btc_3"), None);

        let result = index.get_trees_ready_for_archival().unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].tree_id, 1);
        assert_eq!(result[1].tree_id, 3);
        assert_eq!(result[2].tree_id, 5);
    }

    #[test]
    fn test_get_trees_ready_for_archival_returns_all_fields() {
        let (index, _temp) = create_test_index();
        insert_test_tree(&index, 1, "closed", Some("btc_tx_test"), None);

        let result = index.get_trees_ready_for_archival().unwrap();
        let tree = &result[0];

        assert_eq!(tree.tree_id, 1);
        assert_eq!(tree.origin_id, [0u8; 32]);
        assert_eq!(tree.root_hash.len(), 32);
        assert_eq!(tree.tree_size, 100);
        assert_eq!(tree.data_tree_index, 0);
        assert_eq!(tree.status, ChainTreeStatus::Closed);
        assert_eq!(tree.bitcoin_txid, Some("btc_tx_test".to_string()));
        assert!(tree.archive_location.is_none());
        assert!(!tree.created_at.is_empty());
    }

    // --- ArchivedTree (just ensure it's constructible) ---

    #[test]
    fn test_archived_tree_construction() {
        let tree = ArchivedTree {
            tree_id: 42,
            root_hash: [1u8; 32],
            tree_size: 1000,
        };

        assert_eq!(tree.tree_id, 42);
        assert_eq!(tree.root_hash[0], 1);
        assert_eq!(tree.tree_size, 1000);
    }

    #[test]
    fn test_archived_tree_clone() {
        let tree = ArchivedTree {
            tree_id: 10,
            root_hash: [255u8; 32],
            tree_size: 500,
        };

        let cloned = tree.clone();
        assert_eq!(cloned.tree_id, tree.tree_id);
        assert_eq!(cloned.root_hash, tree.root_hash);
        assert_eq!(cloned.tree_size, tree.tree_size);
    }

    #[test]
    fn test_archived_tree_debug_format() {
        let tree = ArchivedTree {
            tree_id: 99,
            root_hash: [0xAB; 32],
            tree_size: 12345,
        };

        let debug_str = format!("{:?}", tree);
        assert!(debug_str.contains("ArchivedTree"));
        assert!(debug_str.contains("tree_id"));
    }
}
