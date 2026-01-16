// File: src/storage/chain_index/schema.rs

//! Chain Index database schema
//!
//! This module defines the SQLite schema for the Chain Index database.
//! Chain Index is a separate database from the main atl.db.

/// Chain Index schema version (v2: remove prev_tree_id, make data_tree_index NOT NULL)
pub const CHAIN_INDEX_SCHEMA_VERSION: u32 = 2;

/// Chain Index schema SQL (v2)
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
    data_tree_index INTEGER NOT NULL, -- Position in Super-Tree
    status TEXT NOT NULL,             -- 'active', 'closed', 'archived'
    bitcoin_txid TEXT,                -- nullable, hex string
    archive_location TEXT,            -- nullable, S3 URI or disk path
    created_at TEXT NOT NULL,         -- ISO 8601
    closed_at TEXT,                   -- ISO 8601
    archived_at TEXT                  -- ISO 8601
);

CREATE INDEX IF NOT EXISTS idx_trees_status ON trees(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trees_data_tree_index ON trees(data_tree_index);
CREATE INDEX IF NOT EXISTS idx_trees_bitcoin ON trees(bitcoin_txid) WHERE bitcoin_txid IS NOT NULL;
"#;

/// Migration from v1 to v2: remove prev_tree_id, make data_tree_index NOT NULL
pub const MIGRATE_CHAIN_V1_TO_V2: &str = r#"
-- Step 1: Add data_tree_index if not exists (for v1 databases)
ALTER TABLE trees ADD COLUMN data_tree_index INTEGER;

-- Step 2: Populate data_tree_index based on tree ordering
UPDATE trees SET data_tree_index = (
    SELECT COUNT(*) - 1
    FROM trees t2
    WHERE t2.tree_id <= trees.tree_id
);

-- Step 3: Recreate table without prev_tree_id and with data_tree_index NOT NULL
CREATE TABLE trees_new (
    tree_id INTEGER PRIMARY KEY,
    origin_id BLOB NOT NULL,
    root_hash BLOB NOT NULL,
    tree_size INTEGER NOT NULL,
    data_tree_index INTEGER NOT NULL,
    status TEXT NOT NULL,
    bitcoin_txid TEXT,
    archive_location TEXT,
    created_at TEXT NOT NULL,
    closed_at TEXT,
    archived_at TEXT
);

INSERT INTO trees_new
SELECT tree_id, origin_id, root_hash, tree_size, data_tree_index,
       status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
FROM trees;

DROP TABLE trees;
ALTER TABLE trees_new RENAME TO trees;

-- Step 4: Recreate indexes
CREATE INDEX idx_trees_status ON trees(status);
CREATE UNIQUE INDEX idx_trees_data_tree_index ON trees(data_tree_index);
CREATE INDEX idx_trees_bitcoin ON trees(bitcoin_txid) WHERE bitcoin_txid IS NOT NULL;

-- Step 5: Update schema version
INSERT OR REPLACE INTO chain_config (key, value, updated_at)
VALUES ('schema_version', '2', strftime('%s', 'now'));
"#;
