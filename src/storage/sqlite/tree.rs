// File: src/storage/sqlite/tree.rs

use super::store::SqliteStore;
use crate::error::{ServerError, ServerResult, StorageError};
use crate::traits::{ConsistencyProof, InclusionProof, TreeHead};
use rusqlite::{Connection, Transaction, params};

impl SqliteStore {
    /// Get current tree head
    pub(crate) fn get_tree_head_impl(&self) -> ServerResult<TreeHead> {
        let conn = self.get_conn()?;
        get_tree_head_from_db(&conn, self.get_origin())
    }

    /// Get inclusion proof for an entry
    pub(crate) fn get_inclusion_proof_impl(
        &self,
        entry_id: &uuid::Uuid,
        tree_size: Option<u64>,
    ) -> ServerResult<InclusionProof> {
        let conn = self.get_conn()?;

        // Get entry's leaf index
        let leaf_index: i64 = conn
            .query_row(
                "SELECT leaf_index FROM entries WHERE id = ?1",
                params![entry_id.to_string()],
                |row| row.get(0),
            )
            .map_err(|_| ServerError::EntryNotFound(entry_id.to_string()))?;

        let leaf_index = leaf_index as u64;
        let size = tree_size.unwrap_or_else(|| get_current_tree_size(&conn).unwrap_or(0));

        if leaf_index >= size {
            return Err(ServerError::LeafIndexOutOfBounds {
                index: leaf_index,
                tree_size: size,
            });
        }

        let path = compute_inclusion_proof(&conn, leaf_index, size)?;

        Ok(InclusionProof {
            leaf_index,
            tree_size: size,
            path,
        })
    }

    /// Get consistency proof between two tree sizes
    pub(crate) fn get_consistency_proof_impl(
        &self,
        from_size: u64,
        to_size: u64,
    ) -> ServerResult<ConsistencyProof> {
        if from_size > to_size {
            return Err(ServerError::InvalidArgument(format!(
                "from_size {} > to_size {}",
                from_size, to_size
            )));
        }

        let conn = self.get_conn()?;

        let get_node = |level: u32, idx: u64| -> Option<[u8; 32]> {
            conn.query_row(
                "SELECT hash FROM tree_nodes WHERE level = ?1 AND idx = ?2",
                params![level as i64, idx as i64],
                |row| {
                    let bytes: Vec<u8> = row.get(0)?;
                    Ok(bytes.try_into().ok())
                },
            )
            .ok()
            .flatten()
        };

        let path = atl_core::generate_consistency_proof(from_size, to_size, get_node)
            .map_err(|e| ServerError::Internal(e.to_string()))?;

        Ok(ConsistencyProof {
            from_size,
            to_size,
            path: path.path,
        })
    }
}

/// Update tree nodes after appending a leaf
pub fn update_tree_nodes(
    tx: &Transaction,
    leaf_index: u64,
    leaf_hash: &[u8; 32],
) -> ServerResult<()> {
    // Store leaf at level 0
    tx.execute(
        "INSERT OR REPLACE INTO tree_nodes (level, idx, hash) VALUES (0, ?1, ?2)",
        params![leaf_index as i64, leaf_hash.as_slice()],
    )?;

    // Update parent nodes up the tree
    let mut level = 0u32;
    let mut idx = leaf_index;

    while idx > 0 || level == 0 {
        let parent_idx = idx / 2;
        let sibling_idx = if idx % 2 == 0 { idx + 1 } else { idx - 1 };

        // Get current node
        let current_hash: Vec<u8> = tx.query_row(
            "SELECT hash FROM tree_nodes WHERE level = ?1 AND idx = ?2",
            params![level as i64, idx as i64],
            |row| row.get(0),
        )?;

        // Try to get sibling
        let sibling_hash: Option<Vec<u8>> = tx
            .query_row(
                "SELECT hash FROM tree_nodes WHERE level = ?1 AND idx = ?2",
                params![level as i64, sibling_idx as i64],
                |row| row.get(0),
            )
            .ok();

        let parent_hash = if let Some(sibling) = sibling_hash {
            let (left, right) = if idx % 2 == 0 {
                (current_hash, sibling)
            } else {
                (sibling, current_hash)
            };

            let left_arr: [u8; 32] = left.try_into().map_err(|_| {
                ServerError::Storage(StorageError::Corruption("invalid hash length".into()))
            })?;
            let right_arr: [u8; 32] = right.try_into().map_err(|_| {
                ServerError::Storage(StorageError::Corruption("invalid hash length".into()))
            })?;

            atl_core::hash_children(&left_arr, &right_arr)
        } else {
            // Promote current node to parent (no sibling)
            current_hash.try_into().map_err(|_| {
                ServerError::Storage(StorageError::Corruption("invalid hash length".into()))
            })?
        };

        // Store parent
        tx.execute(
            "INSERT OR REPLACE INTO tree_nodes (level, idx, hash) VALUES (?1, ?2, ?3)",
            params![
                (level + 1) as i64,
                parent_idx as i64,
                parent_hash.as_slice()
            ],
        )?;

        level += 1;
        idx = parent_idx;

        // Stop at root
        if parent_idx == 0 && level > 0 {
            break;
        }
    }

    Ok(())
}

/// Batch update tree nodes with minimal hash computations ("jump" optimization)
pub fn batch_update_tree_nodes(
    tx: &Transaction,
    leaf_hashes: &[(u64, [u8; 32])],
    start_index: u64,
) -> ServerResult<()> {
    if leaf_hashes.is_empty() {
        return Ok(());
    }

    let end_index = start_index + leaf_hashes.len() as u64;
    let mut level = 0u32;
    let mut current_start = start_index;
    let mut current_end = end_index;

    while current_start < current_end || current_end > 1 {
        let parent_start = current_start / 2;
        let parent_end = current_end.div_ceil(2);

        for parent_idx in parent_start..parent_end {
            let left_idx = parent_idx * 2;
            let right_idx = left_idx + 1;

            let left_hash: Vec<u8> = tx
                .query_row(
                    "SELECT hash FROM tree_nodes WHERE level = ?1 AND idx = ?2",
                    params![level as i64, left_idx as i64],
                    |row| row.get(0),
                )
                .map_err(|e| {
                    ServerError::Storage(StorageError::QueryFailed(format!(
                        "missing node at level {} idx {}: {}",
                        level, left_idx, e
                    )))
                })?;

            let right_hash: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT hash FROM tree_nodes WHERE level = ?1 AND idx = ?2",
                    params![level as i64, right_idx as i64],
                    |row| row.get(0),
                )
                .ok();

            let parent_hash = match right_hash {
                Some(right) => {
                    let left_arr: [u8; 32] = left_hash.try_into().map_err(|_| {
                        ServerError::Storage(StorageError::Corruption("invalid hash".into()))
                    })?;
                    let right_arr: [u8; 32] = right.try_into().map_err(|_| {
                        ServerError::Storage(StorageError::Corruption("invalid hash".into()))
                    })?;
                    atl_core::hash_children(&left_arr, &right_arr)
                }
                None => left_hash.try_into().map_err(|_| {
                    ServerError::Storage(StorageError::Corruption("invalid hash".into()))
                })?,
            };

            tx.execute(
                "INSERT OR REPLACE INTO tree_nodes (level, idx, hash) VALUES (?1, ?2, ?3)",
                params![
                    (level + 1) as i64,
                    parent_idx as i64,
                    parent_hash.as_slice()
                ],
            )?;
        }

        level += 1;
        current_start = parent_start;
        current_end = parent_end;

        if parent_end <= 1 {
            break;
        }
    }

    Ok(())
}

/// Compute tree head for a given size
pub fn compute_tree_head(
    conn: &Connection,
    tree_size: u64,
    origin: [u8; 32],
) -> ServerResult<TreeHead> {
    if tree_size == 0 {
        return Ok(TreeHead {
            tree_size: 0,
            root_hash: [0u8; 32],
            origin,
        });
    }

    // Find root level (highest level with a node at index 0)
    let root_hash: Vec<u8> = conn
        .query_row(
            "SELECT hash FROM tree_nodes WHERE idx = 0 ORDER BY level DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            ServerError::Storage(StorageError::QueryFailed(format!("no root node: {}", e)))
        })?;

    let root_hash: [u8; 32] = root_hash
        .try_into()
        .map_err(|_| ServerError::Storage(StorageError::Corruption("invalid root hash".into())))?;

    Ok(TreeHead {
        tree_size,
        root_hash,
        origin,
    })
}

/// Compute inclusion proof for a leaf
pub fn compute_inclusion_proof(
    conn: &Connection,
    leaf_index: u64,
    tree_size: u64,
) -> ServerResult<Vec<[u8; 32]>> {
    let get_node = |level: u32, idx: u64| -> Option<[u8; 32]> {
        conn.query_row(
            "SELECT hash FROM tree_nodes WHERE level = ?1 AND idx = ?2",
            params![level as i64, idx as i64],
            |row| {
                let bytes: Vec<u8> = row.get(0)?;
                Ok(bytes.try_into().ok())
            },
        )
        .ok()
        .flatten()
    };

    let proof = atl_core::generate_inclusion_proof(leaf_index, tree_size, get_node)
        .map_err(|e| ServerError::Internal(e.to_string()))?;
    Ok(proof.path)
}

fn get_tree_head_from_db(conn: &Connection, origin: [u8; 32]) -> ServerResult<TreeHead> {
    let tree_size = get_current_tree_size(conn)?;
    compute_tree_head(conn, tree_size, origin)
}

fn get_current_tree_size(conn: &Connection) -> ServerResult<u64> {
    match conn.query_row(
        "SELECT value FROM atl_config WHERE key = 'tree_size'",
        [],
        |row| row.get::<_, String>(0),
    ) {
        Ok(s) => Ok(s.parse().unwrap_or(0)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
        Err(e) => Err(e.into()),
    }
}
