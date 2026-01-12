// File: src/storage/config.rs

#![allow(dead_code)]

use std::path::PathBuf;

/// Storage engine configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Base directory for all storage files
    pub data_dir: PathBuf,

    /// WAL directory (default: data_dir/wal)
    pub wal_dir: Option<PathBuf>,

    /// Slab directory (default: data_dir/slabs)
    pub slab_dir: Option<PathBuf>,

    /// SQLite database path (default: data_dir/atl.db)
    pub db_path: Option<PathBuf>,

    /// Maximum entries per slab
    pub slab_capacity: u32,

    /// Maximum open slabs in memory
    pub max_open_slabs: usize,

    /// Keep N committed WAL files for debugging
    pub wal_keep_count: usize,

    /// Enable fsync (disable only for testing)
    pub fsync_enabled: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./atl-data"),
            wal_dir: None,
            slab_dir: None,
            db_path: None,
            slab_capacity: 1_000_000,
            max_open_slabs: 10,
            wal_keep_count: 2,
            fsync_enabled: true,
        }
    }
}

impl StorageConfig {
    /// Get effective WAL directory
    #[must_use]
    pub fn wal_dir(&self) -> PathBuf {
        self.wal_dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("wal"))
    }

    /// Get effective slab directory
    #[must_use]
    pub fn slab_dir(&self) -> PathBuf {
        self.slab_dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("slabs"))
    }

    /// Get effective database path
    #[must_use]
    pub fn db_path(&self) -> PathBuf {
        self.db_path
            .clone()
            .unwrap_or_else(|| self.data_dir.join("atl.db"))
    }
}
