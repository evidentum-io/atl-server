// File: src/storage/index/queries.rs

//! SQLite index operations for HTS
//!
//! Provides IndexStore for managing entry metadata, checkpoints, and anchors.
//! Tree nodes are NOT stored here - they live in Slab files.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::cell::RefCell;
use std::path::Path;

/// Entry with slab location
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct IndexEntry {
    pub id: uuid::Uuid,
    pub leaf_index: u64,
    pub slab_id: u32,
    pub slab_offset: u64,
    pub payload_hash: [u8; 32],
    pub metadata_hash: [u8; 32],
    pub metadata_cleartext: Option<serde_json::Value>,
    pub external_id: Option<String>,
    pub tree_id: Option<i64>,
    pub created_at: i64,
}

/// Batch insert data
#[derive(Debug, Clone)]
pub struct BatchInsert {
    pub id: uuid::Uuid,
    pub leaf_index: u64,
    pub slab_id: u32,
    pub slab_offset: u64,
    pub payload_hash: [u8; 32],
    pub metadata_hash: [u8; 32],
    pub metadata_cleartext: Option<String>,
    pub external_id: Option<String>,
}

/// SQLite index operations
pub struct IndexStore {
    conn: RefCell<Connection>,
}

impl IndexStore {
    /// Open or create index database
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;

        // Apply performance optimizations (safe because WAL handles durability)
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = OFF;
            PRAGMA cache_size = -16000;
            PRAGMA mmap_size = 268435456;
            PRAGMA foreign_keys = OFF;
            "#,
        )?;

        let store = Self {
            conn: RefCell::new(conn),
        };

        store.migrate()?;

        Ok(store)
    }

    /// Create IndexStore from an existing connection (for testing)
    #[cfg(test)]
    pub(crate) fn from_connection(conn: Connection) -> Self {
        Self {
            conn: RefCell::new(conn),
        }
    }

    /// Initialize schema (create tables if needed)
    pub fn initialize(&self) -> rusqlite::Result<()> {
        self.conn.borrow().execute_batch(super::schema::SCHEMA_V3)?;

        // Set schema version
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        self.conn.borrow().execute(
            "INSERT OR REPLACE INTO atl_config (key, value, updated_at) VALUES ('schema_version', ?1, ?2)",
            params![super::schema::SCHEMA_VERSION.to_string(), now],
        )?;

        Ok(())
    }

    /// Run migrations from v2 to latest
    pub fn migrate(&self) -> rusqlite::Result<()> {
        let table_exists = self
            .conn
            .borrow()
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='atl_config'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;

        if table_exists.is_none() {
            return Ok(());
        }

        let current: u32 = self
            .conn
            .borrow()
            .query_row(
                "SELECT COALESCE(value, '2') FROM atl_config WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0).map(|s| s.parse().unwrap_or(2)),
            )
            .unwrap_or(2);

        if current < 3 {
            self.conn
                .borrow()
                .execute_batch(super::schema::MIGRATE_V2_TO_V3)?;
        }

        if current < 4 {
            self.conn
                .borrow()
                .execute_batch(super::schema::MIGRATE_V3_TO_V4)?;
        }

        Ok(())
    }

    /// Insert batch of entries (within transaction)
    pub fn insert_batch(&mut self, entries: &[BatchInsert]) -> rusqlite::Result<()> {
        let tx = self.conn.get_mut().transaction()?;
        insert_batch_inner(&tx, entries)?;
        tx.commit()?;
        Ok(())
    }

    /// Get entry by ID
    pub fn get_entry(&self, id: &uuid::Uuid) -> rusqlite::Result<Option<IndexEntry>> {
        self.conn
            .borrow()
            .query_row(
                "SELECT id, leaf_index, slab_id, slab_offset, payload_hash, metadata_hash,
                        metadata_cleartext, external_id, tree_id, created_at
                 FROM entries WHERE id = ?1",
                params![id.to_string()],
                row_to_index_entry,
            )
            .optional()
    }

    /// Get entry by leaf index
    pub fn get_entry_by_index(&self, leaf_index: u64) -> rusqlite::Result<Option<IndexEntry>> {
        self.conn
            .borrow()
            .query_row(
                "SELECT id, leaf_index, slab_id, slab_offset, payload_hash, metadata_hash,
                        metadata_cleartext, external_id, tree_id, created_at
                 FROM entries WHERE leaf_index = ?1",
                params![leaf_index as i64],
                row_to_index_entry,
            )
            .optional()
    }

    /// Get entry by external ID
    pub fn get_entry_by_external_id(
        &self,
        external_id: &str,
    ) -> rusqlite::Result<Option<IndexEntry>> {
        self.conn
            .borrow()
            .query_row(
                "SELECT id, leaf_index, slab_id, slab_offset, payload_hash, metadata_hash,
                        metadata_cleartext, external_id, tree_id, created_at
                 FROM entries WHERE external_id = ?1",
                params![external_id],
                row_to_index_entry,
            )
            .optional()
    }

    /// Get current tree size
    pub fn get_tree_size(&self) -> rusqlite::Result<u64> {
        match self.conn.borrow().query_row(
            "SELECT value FROM atl_config WHERE key = 'tree_size'",
            [],
            |row| row.get::<_, String>(0),
        ) {
            Ok(s) => Ok(s.parse().unwrap_or(0)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// Update tree size
    pub fn set_tree_size(&self, size: u64) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        self.conn.borrow().execute(
            "INSERT OR REPLACE INTO atl_config (key, value, updated_at) VALUES ('tree_size', ?1, ?2)",
            params![size.to_string(), now],
        )?;
        Ok(())
    }

    /// Get a reference to the underlying database connection
    pub fn connection(&self) -> std::cell::Ref<'_, rusqlite::Connection> {
        self.conn.borrow()
    }

    /// Get a mutable reference to the underlying database connection
    pub fn connection_mut(&self) -> std::cell::RefMut<'_, rusqlite::Connection> {
        self.conn.borrow_mut()
    }

    /// Get Super-Tree genesis root (None if no trees closed yet)
    #[allow(dead_code)]
    pub fn get_super_genesis_root(&self) -> rusqlite::Result<Option<[u8; 32]>> {
        match self.conn.borrow().query_row(
            "SELECT value FROM atl_config WHERE key = 'super_genesis_root'",
            [],
            |row| row.get::<_, String>(0),
        ) {
            Ok(hex) => {
                let bytes = hex::decode(&hex).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;
                if bytes.len() != 32 {
                    return Ok(None);
                }
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                Ok(Some(arr))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Set Super-Tree genesis root (once only)
    #[allow(dead_code)]
    pub fn set_super_genesis_root(&self, root: &[u8; 32]) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        self.conn.borrow().execute(
            "INSERT OR IGNORE INTO atl_config (key, value, updated_at) VALUES ('super_genesis_root', ?1, ?2)",
            params![hex::encode(root), now],
        )?;
        Ok(())
    }

    /// Get Super-Tree size
    #[allow(dead_code)]
    pub fn get_super_tree_size(&self) -> rusqlite::Result<u64> {
        match self.conn.borrow().query_row(
            "SELECT value FROM atl_config WHERE key = 'super_tree_size'",
            [],
            |row| row.get::<_, String>(0),
        ) {
            Ok(s) => Ok(s.parse().unwrap_or(0)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// Set Super-Tree size
    #[allow(dead_code)]
    pub fn set_super_tree_size(&self, size: u64) -> rusqlite::Result<()> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        self.conn.borrow().execute(
            "INSERT OR REPLACE INTO atl_config (key, value, updated_at) VALUES ('super_tree_size', ?1, ?2)",
            params![size.to_string(), now],
        )?;
        Ok(())
    }
}

/// Insert batch using prepared statement
fn insert_batch_inner(tx: &Transaction, entries: &[BatchInsert]) -> rusqlite::Result<()> {
    let mut stmt = tx.prepare_cached(
        "INSERT INTO entries (id, leaf_index, slab_id, slab_offset, payload_hash, metadata_hash,
                              metadata_cleartext, external_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )?;

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    for entry in entries {
        stmt.execute(params![
            entry.id.to_string(),
            entry.leaf_index as i64,
            entry.slab_id as i64,
            entry.slab_offset as i64,
            entry.payload_hash.as_slice(),
            entry.metadata_hash.as_slice(),
            entry.metadata_cleartext.as_ref(),
            entry.external_id.as_ref(),
            now,
        ])?;
    }

    Ok(())
}

/// Convert SQLite row to IndexEntry
fn row_to_index_entry(row: &rusqlite::Row) -> rusqlite::Result<IndexEntry> {
    let id_str: String = row.get(0)?;
    let id = uuid::Uuid::parse_str(&id_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })?;

    let payload_hash: Vec<u8> = row.get(4)?;
    let payload_hash: [u8; 32] = payload_hash.try_into().map_err(|_| {
        rusqlite::Error::InvalidColumnType(
            4,
            "payload_hash".to_string(),
            rusqlite::types::Type::Blob,
        )
    })?;

    let metadata_hash: Vec<u8> = row.get(5)?;
    let metadata_hash: [u8; 32] = metadata_hash.try_into().map_err(|_| {
        rusqlite::Error::InvalidColumnType(
            5,
            "metadata_hash".to_string(),
            rusqlite::types::Type::Blob,
        )
    })?;

    let metadata_cleartext: Option<String> = row.get(6)?;
    let metadata_cleartext = metadata_cleartext
        .as_ref()
        .and_then(|s| serde_json::from_str(s).ok());

    Ok(IndexEntry {
        id,
        leaf_index: row.get::<_, i64>(1)? as u64,
        slab_id: row.get::<_, i64>(2)? as u32,
        slab_offset: row.get::<_, i64>(3)? as u64,
        payload_hash,
        metadata_hash,
        metadata_cleartext,
        external_id: row.get(7)?,
        tree_id: row.get(8)?,
        created_at: row.get(9)?,
    })
}

/// Convert IndexEntry to public Entry trait type
impl From<IndexEntry> for crate::traits::storage::Entry {
    fn from(idx: IndexEntry) -> Self {
        Self {
            id: idx.id,
            payload_hash: idx.payload_hash,
            metadata_hash: idx.metadata_hash,
            metadata_cleartext: idx.metadata_cleartext,
            leaf_index: Some(idx.leaf_index),
            created_at: chrono::DateTime::from_timestamp_nanos(idx.created_at),
            external_id: idx.external_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_schema_creation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let store = IndexStore::open(&path).unwrap();
        store.initialize().unwrap();

        // Verify tables exist
        let tables: Vec<String> = store
            .conn
            .borrow()
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert!(tables.contains(&"entries".to_string()));
        assert!(tables.contains(&"checkpoints".to_string()));
        assert!(!tables.contains(&"tree_nodes".to_string()));
    }

    #[test]
    fn test_batch_insert() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut store = IndexStore::open(&path).unwrap();
        store.initialize().unwrap();

        let entries: Vec<BatchInsert> = (0..100)
            .map(|i| BatchInsert {
                id: uuid::Uuid::new_v4(),
                leaf_index: i,
                slab_id: 1,
                slab_offset: i * 32,
                payload_hash: [i as u8; 32],
                metadata_hash: [0u8; 32],
                metadata_cleartext: None,
                external_id: None,
            })
            .collect();

        store.insert_batch(&entries).unwrap();

        // Verify count
        let count: i64 = store
            .conn
            .borrow()
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
            .unwrap();

        assert_eq!(count, 100);
    }

    #[test]
    fn test_entry_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut store = IndexStore::open(&path).unwrap();
        store.initialize().unwrap();

        let id = uuid::Uuid::new_v4();
        let entry = BatchInsert {
            id,
            leaf_index: 42,
            slab_id: 1,
            slab_offset: 1344,
            payload_hash: [42u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: Some(r#"{"test": true}"#.to_string()),
            external_id: Some("ext-123".to_string()),
        };

        store.insert_batch(&[entry]).unwrap();

        // Lookup by ID
        let found = store.get_entry(&id).unwrap().unwrap();
        assert_eq!(found.leaf_index, 42);
        assert_eq!(found.slab_id, 1);

        // Lookup by leaf_index
        let found = store.get_entry_by_index(42).unwrap().unwrap();
        assert_eq!(found.id, id);

        // Lookup by external_id
        let found = store.get_entry_by_external_id("ext-123").unwrap().unwrap();
        assert_eq!(found.id, id);
    }

    #[test]
    #[ignore = "benchmark test - machine dependent timing"]
    fn test_batch_insert_performance() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut store = IndexStore::open(&path).unwrap();
        store.initialize().unwrap();

        let entries: Vec<BatchInsert> = (0..10_000)
            .map(|i| BatchInsert {
                id: uuid::Uuid::new_v4(),
                leaf_index: i,
                slab_id: 1,
                slab_offset: i * 32,
                payload_hash: [(i % 256) as u8; 32],
                metadata_hash: [0u8; 32],
                metadata_cleartext: None,
                external_id: None,
            })
            .collect();

        let start = std::time::Instant::now();
        store.insert_batch(&entries).unwrap();
        let elapsed = start.elapsed();

        // Should complete in < 100ms (target: < 50ms)
        println!("10K inserts took: {:?}", elapsed);
        assert!(elapsed.as_millis() < 100, "Batch insert took {:?}", elapsed);
    }
}
