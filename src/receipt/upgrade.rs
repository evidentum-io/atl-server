//! Receipt upgrade with Bitcoin (Tier-2) anchors

use atl_core::{Receipt, ReceiptConsistencyProof};
use uuid::Uuid;

use crate::error::{ServerError, ServerResult};
use crate::receipt::convert::convert_anchor_to_receipt;
use crate::receipt::format::{current_timestamp_nanos, format_hash};
use crate::receipt::generator::{generate_receipt, CheckpointSigner};
use crate::receipt::options::ReceiptOptions;
use crate::traits::anchor::AnchorType;
use crate::traits::storage::ConsistencyProof;
use crate::traits::Storage;

/// Result of upgrade_receipt operation
#[allow(dead_code)]
#[derive(Debug)]
pub enum UpgradeResult {
    /// Receipt upgraded with Bitcoin proof
    Upgraded {
        /// Upgraded receipt with Bitcoin anchor and consistency proof
        receipt: Box<Receipt>,
        /// Consistency proof bridging receipt to Bitcoin anchor
        consistency_proof: ConsistencyProof,
    },
    /// No Bitcoin anchor covers this receipt yet
    Pending {
        /// Tree size from the receipt
        receipt_tree_size: u64,
        /// Tree size of last Bitcoin anchor (if any)
        last_anchor_tree_size: Option<u64>,
        /// Estimated completion timestamp (nanoseconds)
        estimated_completion: u64,
    },
}

/// Upgrade a receipt with Bitcoin (Tier-2) proof
///
/// Finds the first Bitcoin anchor where anchor.tree_size >= receipt.tree_size,
/// then builds a consistency proof bridging the receipt to the anchor.
///
/// # Arguments
/// * `entry_id` - UUID of the entry
/// * `storage` - Storage backend
/// * `signer` - Checkpoint signing key
///
/// # Returns
/// * `UpgradeResult::Upgraded` if Bitcoin anchor exists
/// * `UpgradeResult::Pending` if no anchor covers this receipt
///
/// # Errors
/// Returns error if entry not found or storage operation fails
#[allow(dead_code)]
pub fn upgrade_receipt<S: Storage + ?Sized>(
    entry_id: &Uuid,
    storage: &S,
    signer: &CheckpointSigner,
) -> ServerResult<UpgradeResult> {
    // 1. Get entry and its tree_size
    let entry = storage.get_entry(entry_id)?;
    let leaf_index = entry
        .leaf_index
        .ok_or_else(|| ServerError::EntryNotInTree(entry_id.to_string()))?;

    // Get the tree_size when receipt was created (leaf_index + 1 minimum)
    let receipt_tree_size = leaf_index + 1;

    // 2. Find Bitcoin anchor where anchor.tree_size >= receipt.tree_size
    let bitcoin_anchor = find_bitcoin_anchor_covering(storage, receipt_tree_size)?;

    match bitcoin_anchor {
        Some(anchor) => {
            // 3. Build consistency proof from receipt to anchor
            let consistency_proof =
                storage.get_consistency_proof(receipt_tree_size, anchor.tree_size)?;

            // 4. Generate full receipt at receipt_tree_size
            let mut receipt = generate_receipt(
                entry_id,
                storage,
                signer,
                ReceiptOptions {
                    at_tree_size: Some(receipt_tree_size),
                    include_anchors: true,
                    ..Default::default()
                },
            )?;

            // 5. Add Bitcoin anchor and consistency_proof
            receipt.anchors.push(convert_anchor_to_receipt(&anchor));

            // Add consistency proof bridging receipt to Bitcoin anchor
            receipt.proof.consistency_proof = Some(ReceiptConsistencyProof {
                from_tree_size: consistency_proof.from_size,
                path: consistency_proof.path.iter().map(format_hash).collect(),
            });

            Ok(UpgradeResult::Upgraded {
                receipt: Box::new(receipt),
                consistency_proof,
            })
        }
        None => {
            // No Bitcoin anchor covers this receipt yet
            let last_anchor = get_latest_bitcoin_anchor(storage)?;
            let estimated_completion = estimate_next_anchor_time();

            Ok(UpgradeResult::Pending {
                receipt_tree_size,
                last_anchor_tree_size: last_anchor.map(|a| a.tree_size),
                estimated_completion,
            })
        }
    }
}

/// Find Bitcoin anchor that covers a given tree size
///
/// Returns the first anchor where anchor.tree_size >= target_tree_size.
#[allow(dead_code)]
fn find_bitcoin_anchor_covering<S: Storage + ?Sized>(
    storage: &S,
    target_tree_size: u64,
) -> ServerResult<Option<crate::traits::anchor::Anchor>> {
    // Get current tree size to search from
    let tree_head = storage.tree_head();

    // Search from target_tree_size up to current
    for size in target_tree_size..=tree_head.tree_size {
        let anchors = storage.get_anchors(size)?;
        for anchor in anchors {
            if anchor.anchor_type == AnchorType::BitcoinOts && anchor.tree_size >= target_tree_size
            {
                return Ok(Some(anchor));
            }
        }
    }

    Ok(None)
}

/// Get the latest Bitcoin anchor from storage
#[allow(dead_code)]
fn get_latest_bitcoin_anchor<S: Storage + ?Sized>(
    storage: &S,
) -> ServerResult<Option<crate::traits::anchor::Anchor>> {
    let tree_head = storage.tree_head();

    // Search backwards from current tree size
    for size in (0..=tree_head.tree_size).rev() {
        let anchors = storage.get_anchors(size)?;
        for anchor in anchors {
            if anchor.anchor_type == AnchorType::BitcoinOts {
                return Ok(Some(anchor));
            }
        }
    }

    Ok(None)
}

/// Estimate when next Bitcoin anchor will be ready
///
/// Default: next anchor expected in ~1 hour
#[allow(dead_code)]
fn estimate_next_anchor_time() -> u64 {
    current_timestamp_nanos() + 3600 * 1_000_000_000
}
