// File: src/storage/sqlite/config.rs

/// SQLite storage backend configuration
#[derive(Debug, Clone)]
pub struct SqliteConfig {
    /// Path to database file (or ":memory:" for in-memory)
    pub path: String,

    /// Enable WAL mode for better concurrency
    pub wal_mode: bool,

    /// Busy timeout in milliseconds
    pub busy_timeout_ms: u32,

    /// Enable foreign key enforcement
    pub foreign_keys: bool,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: "atl.db".to_string(),
            wal_mode: true, // Enable WAL for concurrent reads
            busy_timeout_ms: 5000,
            foreign_keys: true,
        }
    }
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    /// Total entries in log
    pub entry_count: u64,

    /// Current tree size
    pub tree_size: u64,

    /// Number of stored tree nodes
    pub node_count: u64,

    /// Number of checkpoints
    pub checkpoint_count: u64,

    /// Number of anchors
    pub anchor_count: u64,

    /// Database file size in bytes
    pub file_size: u64,
}
