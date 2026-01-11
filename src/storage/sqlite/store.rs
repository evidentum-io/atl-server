// File: src/storage/sqlite/store.rs

use super::config::SqliteConfig;
use super::schema;
use crate::error::{ServerError, ServerResult, StorageError};
use rusqlite::Connection;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

/// SQLite implementation of the Storage trait
///
/// Stores ONLY hashes - NO payload data.
pub struct SqliteStore {
    /// Database connection (protected by mutex for thread safety)
    conn: Arc<Mutex<Connection>>,

    /// Configuration (unused for now, but kept for future features)
    #[allow(dead_code)]
    config: SqliteConfig,

    /// Cached origin ID (initialized once)
    origin: OnceLock<[u8; 32]>,
}

impl SqliteStore {
    /// Create a new SqliteStore with default configuration
    ///
    /// Creates the database file and schema if they don't exist.
    pub fn new<P: AsRef<Path>>(path: P) -> ServerResult<Self> {
        let config = SqliteConfig {
            path: path.as_ref().to_string_lossy().to_string(),
            ..Default::default()
        };
        Self::with_config(config)
    }

    /// Create with custom configuration
    pub fn with_config(config: SqliteConfig) -> ServerResult<Self> {
        let conn = Connection::open(&config.path).map_err(|e| {
            ServerError::Storage(StorageError::ConnectionFailed(format!(
                "failed to open db: {}",
                e
            )))
        })?;

        Self::configure_connection(&conn, &config)?;

        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            config,
            origin: OnceLock::new(), // Will be set by initialize()
        };

        Ok(store)
    }

    /// Create an in-memory database (for testing)
    pub fn in_memory() -> ServerResult<Self> {
        let config = SqliteConfig {
            path: ":memory:".to_string(),
            ..Default::default()
        };
        Self::with_config(config)
    }

    /// Open an existing database (fails if doesn't exist)
    pub fn open<P: AsRef<Path>>(path: P) -> ServerResult<Self> {
        if !path.as_ref().exists() {
            return Err(ServerError::Storage(StorageError::ConnectionFailed(
                "database does not exist".into(),
            )));
        }
        Self::new(path)
    }

    /// Configure SQLite connection pragmas
    fn configure_connection(conn: &Connection, config: &SqliteConfig) -> ServerResult<()> {
        if config.wal_mode {
            conn.pragma_update(None, "journal_mode", "WAL")?;
        }
        conn.pragma_update(None, "busy_timeout", config.busy_timeout_ms)?;
        if config.foreign_keys {
            conn.pragma_update(None, "foreign_keys", "ON")?;
        }
        // Performance optimizations
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "cache_size", -64000)?; // 64MB cache
        Ok(())
    }

    /// Initialize a new log instance
    ///
    /// Must be called once before appending entries.
    /// Generates a new origin UUID.
    pub fn initialize(&self) -> ServerResult<uuid::Uuid> {
        let uuid = uuid::Uuid::new_v4();
        self.initialize_with_uuid(uuid)?;
        Ok(uuid)
    }

    /// Initialize with a specific UUID (for testing/migration)
    pub fn initialize_with_uuid(&self, uuid: uuid::Uuid) -> ServerResult<()> {
        let conn = self.conn.lock().map_err(|_| {
            ServerError::Storage(StorageError::ConnectionFailed("lock poisoned".into()))
        })?;

        // Create schema
        schema::create_tables(&conn)?;

        // Store origin UUID
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        conn.execute(
            "INSERT OR REPLACE INTO atl_config (key, value, updated_at) VALUES ('origin_uuid', ?1, ?2)",
            rusqlite::params![uuid.to_string(), now],
        )?;

        // Initialize tree state
        conn.execute(
            "INSERT OR IGNORE INTO atl_config (key, value, updated_at) VALUES ('tree_size', '0', ?1)",
            rusqlite::params![now],
        )?;

        // Compute origin ID (hash of UUID)
        use sha2::Digest;
        let origin = sha2::Sha256::digest(uuid.as_bytes());
        drop(conn);

        // Set origin once (idempotent - will fail silently if already set)
        let _ = self.origin.set(origin.into());

        Ok(())
    }

    /// Get the origin UUID
    pub fn origin_uuid(&self) -> ServerResult<uuid::Uuid> {
        let conn = self.conn.lock().map_err(|_| {
            ServerError::Storage(StorageError::ConnectionFailed("lock poisoned".into()))
        })?;

        let uuid_str: String = conn
            .query_row(
                "SELECT value FROM atl_config WHERE key = 'origin_uuid'",
                [],
                |row| row.get(0),
            )
            .map_err(|_| ServerError::Storage(StorageError::NotInitialized))?;

        uuid_str.parse().map_err(|_| {
            ServerError::Storage(StorageError::Corruption("invalid origin UUID".into()))
        })
    }

    /// Check if the database is initialized
    pub fn is_initialized(&self) -> bool {
        self.origin_uuid().is_ok()
    }

    /// Run schema migrations
    pub fn migrate(&self) -> ServerResult<()> {
        let conn = self.conn.lock().map_err(|_| {
            ServerError::Storage(StorageError::ConnectionFailed("lock poisoned".into()))
        })?;
        schema::migrate(&conn)
    }

    /// Get database statistics
    pub fn stats(&self) -> ServerResult<super::config::StorageStats> {
        // Implementation delegated to helper function
        super::entries::get_stats(&*self.conn.lock().map_err(|_| {
            ServerError::Storage(StorageError::ConnectionFailed("lock poisoned".into()))
        })?)
    }

    /// Compact the database (VACUUM)
    pub fn compact(&self) -> ServerResult<()> {
        let conn = self.conn.lock().map_err(|_| {
            ServerError::Storage(StorageError::ConnectionFailed("lock poisoned".into()))
        })?;
        conn.execute("VACUUM", [])?;
        Ok(())
    }

    /// Get locked connection for internal operations
    pub(crate) fn get_conn(&self) -> ServerResult<std::sync::MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(|_| {
            ServerError::Storage(StorageError::ConnectionFailed("lock poisoned".into()))
        })
    }

    /// Get cached origin ID
    pub(crate) fn get_origin(&self) -> [u8; 32] {
        *self.origin.get().unwrap_or(&[0u8; 32])
    }
}

// Storage trait implementation
use crate::traits::{
    Anchor, AppendParams, AppendResult, ConsistencyProof, Entry, InclusionProof, Storage, TreeHead,
};

impl Storage for SqliteStore {
    fn initialize(&self) -> ServerResult<()> {
        // Storage initialization is handled separately via initialize() method
        // This method is for trait compatibility
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        self.origin_uuid().is_ok()
    }

    fn origin_id(&self) -> [u8; 32] {
        *self.origin.get().unwrap_or(&[0u8; 32])
    }

    fn append(&self, params: AppendParams) -> ServerResult<AppendResult> {
        self.append_impl(params)
    }

    fn append_batch(&self, params: Vec<AppendParams>) -> ServerResult<Vec<AppendResult>> {
        self.append_batch_impl(params)
    }

    fn get_entry(&self, id: &uuid::Uuid) -> ServerResult<Entry> {
        self.get_entry_impl(id)
    }

    fn get_entry_by_index(&self, index: u64) -> ServerResult<Entry> {
        self.get_entry_by_index_impl(index)
    }

    fn get_entry_by_external_id(&self, external_id: &str) -> ServerResult<Entry> {
        self.get_entry_by_external_id_impl(external_id)
    }

    fn get_tree_head(&self) -> ServerResult<TreeHead> {
        self.get_tree_head_impl()
    }

    fn get_inclusion_proof(
        &self,
        entry_id: &uuid::Uuid,
        tree_size: Option<u64>,
    ) -> ServerResult<InclusionProof> {
        self.get_inclusion_proof_impl(entry_id, tree_size)
    }

    fn get_consistency_proof(
        &self,
        from_size: u64,
        to_size: u64,
    ) -> ServerResult<ConsistencyProof> {
        self.get_consistency_proof_impl(from_size, to_size)
    }

    fn store_checkpoint(&self, checkpoint: &atl_core::Checkpoint) -> ServerResult<()> {
        self.store_checkpoint_impl(checkpoint)
    }

    fn get_latest_checkpoint(&self) -> ServerResult<Option<atl_core::Checkpoint>> {
        self.get_latest_checkpoint_impl()
    }

    fn get_checkpoint_by_size(&self, tree_size: u64) -> ServerResult<Option<atl_core::Checkpoint>> {
        self.get_checkpoint_by_size_impl(tree_size)
    }

    fn get_checkpoint_at_or_before(
        &self,
        tree_size: u64,
    ) -> ServerResult<Option<atl_core::Checkpoint>> {
        self.get_checkpoint_at_or_before_impl(tree_size)
    }

    fn store_anchor(&self, tree_size: u64, anchor: &Anchor) -> ServerResult<()> {
        self.store_anchor_impl(tree_size, anchor)
    }

    fn get_anchors(&self, tree_size: u64) -> ServerResult<Vec<Anchor>> {
        self.get_anchors_impl(tree_size)
    }

    fn get_latest_anchored_size(&self) -> ServerResult<Option<u64>> {
        self.get_latest_anchored_size_impl()
    }

    fn get_anchors_covering(
        &self,
        target_tree_size: u64,
        limit: usize,
    ) -> ServerResult<Vec<Anchor>> {
        self.get_anchors_covering_impl(target_tree_size, limit)
    }
}
