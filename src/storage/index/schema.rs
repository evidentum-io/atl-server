// File: src/storage/index/schema.rs

//! SQLite schema v3 for High-Throughput Storage
//!
//! This schema removes `tree_nodes` table (moved to Slab files) and adds
//! `slab_id`, `slab_offset` columns to entries.

/// Current schema version (v6: add target and super_tree_size to anchors)
pub const SCHEMA_VERSION: u32 = 6;

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
    data_tree_index INTEGER,          -- Position in Super-Tree (NULL for active tree)
    created_at INTEGER NOT NULL,
    first_entry_at INTEGER,
    closed_at INTEGER,
    tsa_anchor_id INTEGER,
    bitcoin_anchor_id INTEGER,
    prev_tree_id INTEGER,             -- Chain link to previous tree (NULL for first tree)
    FOREIGN KEY (tsa_anchor_id) REFERENCES anchors(id),
    FOREIGN KEY (bitcoin_anchor_id) REFERENCES anchors(id),
    FOREIGN KEY (prev_tree_id) REFERENCES trees(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trees_active ON trees(status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_trees_status ON trees(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trees_data_tree_index ON trees(data_tree_index) WHERE data_tree_index IS NOT NULL;

-- Anchors (TSA, Bitcoin)
CREATE TABLE IF NOT EXISTS anchors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tree_size INTEGER,                -- Data Tree size (nullable for Super Root anchors)
    anchor_type TEXT NOT NULL,        -- 'rfc3161', 'bitcoin_ots'
    target TEXT NOT NULL DEFAULT 'data_tree_root',  -- 'data_tree_root' or 'super_root'
    anchored_hash BLOB NOT NULL,      -- 32 bytes (target_hash)
    super_tree_size INTEGER,          -- Super-Tree size (for OTS v2.0)
    timestamp INTEGER NOT NULL,       -- Unix nanos
    token BLOB NOT NULL,              -- Raw anchor data
    metadata TEXT,                    -- JSON
    status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anchors_tree_size ON anchors(tree_size);
CREATE INDEX IF NOT EXISTS idx_anchors_status ON anchors(status);
CREATE INDEX IF NOT EXISTS idx_anchors_super_tree_size ON anchors(super_tree_size);
"#;

/// Migration from v2 to v3: remove tree_nodes, add slab columns
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

/// Migration from v3 to v4: add tree chaining support
pub const MIGRATE_V3_TO_V4: &str = r#"
-- Add chain link column
ALTER TABLE trees ADD COLUMN prev_tree_id INTEGER REFERENCES trees(id);

-- Update schema version
INSERT OR REPLACE INTO atl_config (key, value, updated_at)
VALUES ('schema_version', '4', strftime('%s', 'now') * 1000000000);
"#;

/// Migration from v4 to v5: add data_tree_index for Super-Tree integration
pub const MIGRATE_V4_TO_V5: &str = r#"
-- Add data_tree_index column (NULL for active tree)
ALTER TABLE trees ADD COLUMN data_tree_index INTEGER;

-- Populate data_tree_index for closed trees only
-- Trees are ordered by id (auto-increment), so assign sequential indices
UPDATE trees SET data_tree_index = (
    SELECT COUNT(*) - 1
    FROM trees t2
    WHERE t2.status IN ('pending_bitcoin', 'closed') AND t2.id <= trees.id
)
WHERE status IN ('pending_bitcoin', 'closed');

-- Create partial unique index (only for closed trees with data_tree_index)
CREATE UNIQUE INDEX IF NOT EXISTS idx_trees_data_tree_index
    ON trees(data_tree_index) WHERE data_tree_index IS NOT NULL;

-- Update schema version
INSERT OR REPLACE INTO atl_config (key, value, updated_at)
VALUES ('schema_version', '5', strftime('%s', 'now') * 1000000000);
"#;

/// Migration from v5 to v6: add target and super_tree_size to anchors
pub const MIGRATE_V5_TO_V6: &str = r#"
-- Add new columns to anchors table
ALTER TABLE anchors ADD COLUMN target TEXT NOT NULL DEFAULT 'data_tree_root';
ALTER TABLE anchors ADD COLUMN super_tree_size INTEGER;

-- Populate target for existing anchors
-- All existing v1.x anchors targeted data_tree_root (both TSA and OTS)
UPDATE anchors SET target = 'data_tree_root' WHERE anchor_type = 'rfc3161';
UPDATE anchors SET target = 'data_tree_root' WHERE anchor_type = 'bitcoin_ots';

-- Make tree_size nullable (it will be NULL for new super_root OTS anchors)
-- Note: SQLite doesn't support DROP COLUMN or ALTER COLUMN NOT NULL,
-- so we keep tree_size as-is and allow NULL values in application code

-- Create index for super_tree_size
CREATE INDEX IF NOT EXISTS idx_anchors_super_tree_size ON anchors(super_tree_size);

-- Update schema version
INSERT OR REPLACE INTO atl_config (key, value, updated_at)
VALUES ('schema_version', '6', strftime('%s', 'now') * 1000000000);
"#;
