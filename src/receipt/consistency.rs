//! Consistency proof determination for Split-View protection

use atl_core::ReceiptConsistencyProof;

use crate::error::ServerResult;
use crate::receipt::format::format_hash;
use crate::receipt::options::ReceiptOptions;
use crate::traits::Storage;

/// Determine consistency proof source based on options and anchors
///
/// This function implements Split-View attack protection by automatically
/// including a consistency proof from the last anchored checkpoint.
///
/// # Arguments
/// * `storage` - Storage backend for fetching proofs
/// * `current_tree_size` - Current tree size for the receipt
/// * `options` - Receipt generation options
///
/// # Returns
/// * `Ok(Some(proof))` if a consistency proof is available
/// * `Ok(None)` if no proof is needed or available
///
/// # Errors
/// Returns error if proof generation fails
#[allow(dead_code)]
pub fn determine_consistency_proof<S: Storage + ?Sized>(
    storage: &S,
    current_tree_size: u64,
    options: &ReceiptOptions,
) -> ServerResult<Option<ReceiptConsistencyProof>> {
    // Explicit override from options
    if let Some(from_size) = options.consistency_from {
        if from_size > 0 && from_size < current_tree_size {
            let proof = storage.get_consistency_proof(from_size, current_tree_size)?;
            return Ok(Some(ReceiptConsistencyProof {
                from_tree_size: proof.from_size,
                path: proof.path.iter().map(format_hash).collect(),
            }));
        }
        return Ok(None);
    }

    // Auto-detect from last anchor (Split-View protection)
    if options.auto_consistency_from_anchor {
        let last_anchored_size = storage.get_latest_anchored_size()?;

        if let Some(anchored_size) = last_anchored_size {
            if anchored_size < current_tree_size {
                let proof = storage.get_consistency_proof(anchored_size, current_tree_size)?;
                return Ok(Some(ReceiptConsistencyProof {
                    from_tree_size: proof.from_size,
                    path: proof.path.iter().map(format_hash).collect(),
                }));
            }
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    // Tests would require a mock Storage implementation
    // For now we verify the module compiles correctly
}
