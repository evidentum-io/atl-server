// File: src/storage/index/schema.rs

//! SQLite schema v3 for High-Throughput Storage
//!
//! This schema removes `tree_nodes` table (moved to Slab files) and adds
//! `slab_id`, `slab_offset` columns to entries.

/// Current schema version (v3: no tree_nodes, slab pointers added)
pub const SCHEMA_VERSION: u32 = 3;

/// Schema v3: SQLite as index/metadata store only
pub const SCHEMA_V3: &str = r#"
-- Schema version
CREATE TABLE IF NOT EXISTS atl_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Entries: pointers to Slab, NOT tree nodes
CREATE TABLE IF NOT EXISTS entries (
    id TEXT PRIMARY KEY,              -- UUID
    leaf_index INTEGER UNIQUE NOT NULL,
    slab_id INTEGER NOT NULL,         -- Which slab file
    slab_offset INTEGER NOT NULL,     -- Offset in slab (level 0 position)
    payload_hash BLOB NOT NULL,       -- 32 bytes (for verification)
    metadata_hash BLOB NOT NULL,      -- 32 bytes
    metadata_cleartext TEXT,          -- JSON (for receipt)
    external_id TEXT,                 -- Client correlation
    tree_id INTEGER,                  -- FK to trees
    created_at INTEGER NOT NULL       -- Unix nanos
);

-- Optimized indices
CREATE INDEX IF NOT EXISTS idx_entries_leaf_index ON entries(leaf_index);
CREATE INDEX IF NOT EXISTS idx_entries_external_id ON entries(external_id) WHERE external_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_entries_tree_id ON entries(tree_id) WHERE tree_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_entries_created ON entries(created_at);

-- Checkpoints (signed tree heads)
CREATE TABLE IF NOT EXISTS checkpoints (
    tree_size INTEGER PRIMARY KEY,
    origin BLOB NOT NULL,             -- 32 bytes
    timestamp INTEGER NOT NULL,       -- Unix nanos
    root_hash BLOB NOT NULL,          -- 32 bytes
    signature BLOB NOT NULL,          -- 64 bytes Ed25519
    key_id BLOB NOT NULL,             -- 32 bytes
    created_at INTEGER NOT NULL
);

-- Trees (batch anchoring lifecycle)
CREATE TABLE IF NOT EXISTS trees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    origin_id BLOB NOT NULL,          -- 32 bytes
    status TEXT NOT NULL DEFAULT 'active',
    start_size INTEGER NOT NULL,
    end_size INTEGER,
    root_hash BLOB,
    created_at INTEGER NOT NULL,
    first_entry_at INTEGER,
    closed_at INTEGER,
    tsa_anchor_id INTEGER,
    bitcoin_anchor_id INTEGER,
    FOREIGN KEY (tsa_anchor_id) REFERENCES anchors(id),
    FOREIGN KEY (bitcoin_anchor_id) REFERENCES anchors(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trees_active ON trees(status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_trees_status ON trees(status);

-- Anchors (TSA, Bitcoin)
CREATE TABLE IF NOT EXISTS anchors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tree_size INTEGER NOT NULL,
    anchor_type TEXT NOT NULL,        -- 'rfc3161', 'bitcoin_ots'
    anchored_hash BLOB NOT NULL,      -- 32 bytes
    timestamp INTEGER NOT NULL,       -- Unix nanos
    token BLOB NOT NULL,              -- Raw anchor data
    metadata TEXT,                    -- JSON
    status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anchors_tree_size ON anchors(tree_size);
CREATE INDEX IF NOT EXISTS idx_anchors_status ON anchors(status);
"#;

/// Migration from v2 to v3: remove tree_nodes, add slab columns
#[allow(dead_code)]
pub const MIGRATE_V2_TO_V3: &str = r#"
-- Add new columns to entries
ALTER TABLE entries ADD COLUMN slab_id INTEGER DEFAULT 0;
ALTER TABLE entries ADD COLUMN slab_offset INTEGER DEFAULT 0;

-- Drop tree_nodes table (data moved to slabs)
DROP TABLE IF EXISTS tree_nodes;

-- Update schema version
INSERT OR REPLACE INTO atl_config (key, value, updated_at)
VALUES ('schema_version', '3', strftime('%s', 'now') * 1000000000);
"#;
