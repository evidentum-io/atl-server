//! Storage engine crash recovery orchestration
//!
//! Coordinates WAL, Slab, and Index recovery after crashes.

use crate::error::StorageError;
use crate::storage::index::{BatchInsert, IndexStore};
use crate::storage::slab::SlabManager;
use crate::storage::wal::{WalRecovery, WalWriter};
use uuid::Uuid;

/// Run crash recovery on storage components
///
/// This orchestrates the recovery process:
/// 1. Scan WAL directory for pending/committed batches
/// 2. Replay committed batches to Slab and Index
/// 3. Discard uncommitted batches
/// 4. Update WAL state with next batch ID
///
/// # Errors
///
/// Returns `StorageError` if recovery fails
pub async fn recover(
    wal: &mut WalWriter,
    slabs: &mut SlabManager,
    index: &IndexStore,
) -> Result<(), StorageError> {
    // Scan WAL directory for pending/committed batches
    let recovery = WalRecovery::new(wal.dir().to_path_buf());
    let result = recovery.scan()?;

    tracing::info!(
        "recovery: {} batches to replay, {} discarded",
        result.replay_needed.len(),
        result.discarded.len()
    );

    // Replay committed batches
    for batch in result.replay_needed {
        tracing::debug!("replaying batch {}", batch.batch_id);

        // Convert WAL entries to BatchInsert format
        let batch_inserts: Vec<BatchInsert> = batch
            .entries
            .iter()
            .enumerate()
            .map(|(i, entry)| {
                let leaf_index = batch.tree_size_before + i as u64;
                let id = Uuid::from_bytes(entry.id);
                let slab_capacity = 1_000_000u64; // Use default from config
                let slab_id = (leaf_index / slab_capacity) as u32 + 1;
                let slab_offset = (leaf_index % slab_capacity) * 32;

                let metadata_cleartext = if entry.metadata.is_empty() {
                    None
                } else {
                    String::from_utf8(entry.metadata.clone()).ok()
                };

                let external_id = if entry.external_id.is_empty() {
                    None
                } else {
                    String::from_utf8(entry.external_id.clone()).ok()
                };

                BatchInsert {
                    id,
                    leaf_index,
                    slab_id,
                    slab_offset,
                    payload_hash: entry.payload_hash,
                    metadata_hash: entry.metadata_hash,
                    metadata_cleartext,
                    external_id,
                }
            })
            .collect();

        // Compute leaf hashes and update slab
        let leaf_hashes: Vec<[u8; 32]> = batch
            .entries
            .iter()
            .map(|entry| atl_core::compute_leaf_hash(&entry.payload_hash, &entry.metadata_hash))
            .collect();

        slabs.append_leaves(&leaf_hashes)?;

        // Insert into SQLite index
        index.insert_batch(&batch_inserts)?;
        let new_tree_size = batch.tree_size_before + batch.entries.len() as u64;
        index.set_tree_size(new_tree_size)?;
    }

    // Update WAL state
    wal.set_next_batch_id(result.next_batch_id);

    Ok(())
}
