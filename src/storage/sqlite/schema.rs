// File: src/storage/sqlite/schema.rs

use crate::error::ServerResult;
use rusqlite::Connection;

/// Current schema version
pub const SCHEMA_VERSION: u32 = 2;

/// Create all tables (idempotent)
pub fn create_tables(conn: &Connection) -> ServerResult<()> {
    conn.execute_batch(SCHEMA_SQL)?;
    Ok(())
}

/// Run migrations from current version to latest
pub fn migrate(conn: &Connection) -> ServerResult<()> {
    let current: u32 = conn
        .query_row(
            "SELECT COALESCE(value, '1') FROM atl_config WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0).map(|s| s.parse().unwrap_or(1)),
        )
        .unwrap_or(1);

    if current < 2 {
        // Migration from v1 to v2: add trees table
        migrate_v1_to_v2(conn)?;
    }

    // Update schema version
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    conn.execute(
        "INSERT OR REPLACE INTO atl_config (key, value, updated_at) VALUES ('schema_version', ?1, ?2)",
        rusqlite::params![SCHEMA_VERSION.to_string(), now],
    )?;

    Ok(())
}

fn migrate_v1_to_v2(conn: &Connection) -> ServerResult<()> {
    // Add trees table and related indices
    conn.execute_batch(TREES_TABLE_SQL)?;
    Ok(())
}

const SCHEMA_SQL: &str = r#"
-- Core configuration
CREATE TABLE IF NOT EXISTS atl_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Entries (leaves) - stores ONLY hashes, NO payload data
CREATE TABLE IF NOT EXISTS entries (
    id TEXT PRIMARY KEY,                    -- UUID as text
    payload_hash BLOB NOT NULL,             -- 32 bytes SHA-256
    metadata_hash BLOB NOT NULL,            -- 32 bytes SHA-256
    metadata_cleartext TEXT,                -- JSON (nullable, for receipt)
    external_id TEXT,                       -- Client correlation ID (optional)
    leaf_index INTEGER UNIQUE,              -- Position in tree
    leaf_hash BLOB NOT NULL,                -- Computed leaf hash (32 bytes)
    tree_id INTEGER,                        -- FK to trees.id
    created_at INTEGER NOT NULL             -- Unix nanoseconds
);

-- Merkle tree nodes (sparse storage)
CREATE TABLE IF NOT EXISTS tree_nodes (
    level INTEGER NOT NULL,                 -- 0 = leaves
    idx INTEGER NOT NULL,                   -- Index within level
    hash BLOB NOT NULL,                     -- 32 bytes
    PRIMARY KEY (level, idx)
);

-- Checkpoints (signed tree heads)
CREATE TABLE IF NOT EXISTS checkpoints (
    tree_size INTEGER PRIMARY KEY,          -- Tree size at checkpoint
    origin BLOB NOT NULL,                   -- 32 bytes
    timestamp INTEGER NOT NULL,             -- Unix nanoseconds
    root_hash BLOB NOT NULL,                -- 32 bytes
    signature BLOB NOT NULL,                -- 64 bytes
    key_id BLOB NOT NULL,                   -- 32 bytes
    created_at INTEGER NOT NULL
);

-- Indices for common queries
CREATE INDEX IF NOT EXISTS idx_entries_leaf_index ON entries(leaf_index);
CREATE INDEX IF NOT EXISTS idx_entries_external_id ON entries(external_id);
CREATE INDEX IF NOT EXISTS idx_entries_created ON entries(created_at);
"#;

const TREES_TABLE_SQL: &str = r#"
-- Trees: tracks tree lifecycle for batch anchoring
CREATE TABLE IF NOT EXISTS trees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    origin_id BLOB NOT NULL,                -- 32 bytes
    status TEXT NOT NULL DEFAULT 'active',  -- 'active', 'pending_bitcoin', 'closed'
    start_size INTEGER NOT NULL,            -- Tree size when created
    end_size INTEGER,                       -- Tree size when closed (NULL while active)
    root_hash BLOB,                         -- 32 bytes (NULL while active)
    created_at INTEGER NOT NULL,
    first_entry_at INTEGER,                 -- When first entry was added (NULL = empty)
    closed_at INTEGER,                      -- When tree was closed
    tsa_anchor_id INTEGER,                  -- FK to anchors.id
    bitcoin_anchor_id INTEGER,              -- FK to anchors.id
    FOREIGN KEY (tsa_anchor_id) REFERENCES anchors(id),
    FOREIGN KEY (bitcoin_anchor_id) REFERENCES anchors(id)
);

-- Ensure only one tree can be active at any time
CREATE UNIQUE INDEX IF NOT EXISTS idx_trees_active ON trees(status) WHERE status = 'active';

-- Anchor attestations
CREATE TABLE IF NOT EXISTS anchors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tree_size INTEGER NOT NULL,             -- References checkpoint
    anchor_type TEXT NOT NULL,              -- 'rfc3161', 'bitcoin_ots'
    anchored_hash BLOB NOT NULL,            -- Should match root_hash
    timestamp INTEGER NOT NULL,             -- Unix nanoseconds
    token BLOB NOT NULL,                    -- Raw anchor data
    metadata TEXT,                          -- JSON metadata
    status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'confirmed'
    created_at INTEGER NOT NULL,
    FOREIGN KEY (tree_size) REFERENCES checkpoints(tree_size)
);

CREATE INDEX IF NOT EXISTS idx_entries_tree_id ON entries(tree_id);
CREATE INDEX IF NOT EXISTS idx_anchors_tree_size ON anchors(tree_size);
CREATE INDEX IF NOT EXISTS idx_anchors_status ON anchors(status);
CREATE INDEX IF NOT EXISTS idx_trees_status ON trees(status);
CREATE INDEX IF NOT EXISTS idx_trees_tsa_anchor ON trees(tsa_anchor_id);
CREATE INDEX IF NOT EXISTS idx_trees_bitcoin_anchor ON trees(bitcoin_anchor_id);
"#;
