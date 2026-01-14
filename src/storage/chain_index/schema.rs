// File: src/storage/chain_index/schema.rs

//! Chain Index database schema
//!
//! This module defines the SQLite schema for the Chain Index database.
//! Chain Index is a separate database from the main atl.db.

/// Chain Index schema version
pub const CHAIN_INDEX_SCHEMA_VERSION: u32 = 1;

/// Chain Index schema SQL
pub const CHAIN_INDEX_SCHEMA: &str = r#"
-- Schema version tracking
CREATE TABLE IF NOT EXISTS chain_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Tree metadata for chain verification
CREATE TABLE IF NOT EXISTS trees (
    tree_id INTEGER PRIMARY KEY,
    origin_id BLOB NOT NULL,          -- 32 bytes
    root_hash BLOB NOT NULL,          -- 32 bytes
    tree_size INTEGER NOT NULL,
    prev_tree_id INTEGER,             -- FK (nullable for first tree)
    data_tree_index INTEGER,          -- Position in Super-Tree (nullable for first)
    status TEXT NOT NULL,             -- 'active', 'closed', 'archived'
    bitcoin_txid TEXT,                -- nullable, hex string
    archive_location TEXT,            -- nullable, S3 URI or disk path
    created_at TEXT NOT NULL,         -- ISO 8601
    closed_at TEXT,                   -- ISO 8601
    archived_at TEXT,                 -- ISO 8601
    FOREIGN KEY (prev_tree_id) REFERENCES trees(tree_id)
);

CREATE INDEX IF NOT EXISTS idx_trees_status ON trees(status);
CREATE INDEX IF NOT EXISTS idx_trees_prev ON trees(prev_tree_id);
CREATE INDEX IF NOT EXISTS idx_trees_bitcoin ON trees(bitcoin_txid) WHERE bitcoin_txid IS NOT NULL;
"#;
