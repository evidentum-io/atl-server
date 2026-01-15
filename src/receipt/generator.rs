//! Receipt generation implementation

use atl_core::{Checkpoint, CheckpointJson, Receipt, ReceiptEntry, ReceiptProof};
use ed25519_dalek::{Signer, SigningKey};
use uuid::Uuid;

use crate::error::{ServerError, ServerResult};
use crate::receipt::consistency::determine_consistency_proof;
use crate::receipt::convert::convert_anchor_to_receipt;
use crate::receipt::format::{current_timestamp_nanos, format_hash, format_signature};
use crate::receipt::options::ReceiptOptions;
use crate::storage::engine::StorageEngine;
use crate::traits::Storage;

/// Checkpoint signer wrapper
///
/// Wraps Ed25519 signing key and provides key ID computation.
#[derive(Clone)]
pub struct CheckpointSigner {
    signing_key: SigningKey,
    key_id: [u8; 32],
}

impl CheckpointSigner {
    /// Create a new checkpoint signer from Ed25519 signing key
    ///
    /// # Arguments
    /// * `signing_key` - Ed25519 signing key
    #[must_use]
    pub fn new(signing_key: SigningKey) -> Self {
        let public_key = signing_key.verifying_key().to_bytes();
        let key_id = atl_core::compute_key_id(&public_key);
        Self {
            signing_key,
            key_id,
        }
    }

    /// Create signer from raw 32-byte seed
    ///
    /// # Arguments
    /// * `seed` - 32-byte Ed25519 seed
    #[must_use]
    pub fn from_bytes(seed: &[u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(seed);
        Self::new(signing_key)
    }

    /// Create signer from file containing Ed25519 seed
    ///
    /// # Arguments
    /// * `path` - Path to file containing 32-byte seed
    ///
    /// # Errors
    /// Returns error if file cannot be read or seed is invalid
    pub fn from_file(path: &str) -> Result<Self, std::io::Error> {
        let seed_bytes = std::fs::read(path)?;
        if seed_bytes.len() != 32 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("signing key must be 32 bytes, got {}", seed_bytes.len()),
            ));
        }

        let mut seed = [0u8; 32];
        seed.copy_from_slice(&seed_bytes);
        Ok(Self::from_bytes(&seed))
    }

    /// Get the key ID (SHA256 of public key)
    #[must_use]
    #[allow(dead_code)]
    pub const fn key_id(&self) -> &[u8; 32] {
        &self.key_id
    }

    /// Get public key bytes
    #[must_use]
    #[allow(dead_code)]
    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    /// Get public key info for external verification
    ///
    /// Returns (key_id, public_key) tuple
    #[must_use]
    #[allow(dead_code)]
    pub fn public_key_info(&self) -> ([u8; 32], [u8; 32]) {
        (self.key_id, self.public_key_bytes())
    }

    /// Sign a checkpoint and return complete Checkpoint structure
    ///
    /// # Arguments
    /// * `origin` - Origin ID (hash of log's identity)
    /// * `tree_size` - Tree size at checkpoint
    /// * `root_hash` - Merkle tree root hash
    ///
    /// # Returns
    /// * Signed `atl_core::Checkpoint`
    #[must_use]
    pub fn sign_checkpoint_struct(
        &self,
        origin: [u8; 32],
        tree_size: u64,
        root_hash: &[u8; 32],
    ) -> atl_core::Checkpoint {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        // Create checkpoint struct with placeholder signature
        let mut checkpoint = atl_core::Checkpoint::new(
            origin,
            tree_size,
            timestamp,
            *root_hash,
            [0u8; 64], // placeholder signature
            self.key_id,
        );

        // Generate correct 98-byte wire format using atl-core's implementation
        let blob = checkpoint.to_bytes();

        // Sign the correctly formatted blob
        checkpoint.signature = self.sign_checkpoint(&blob);

        checkpoint
    }

    /// Sign a checkpoint
    ///
    /// # Arguments
    /// * `checkpoint_blob` - 98-byte checkpoint wire format
    ///
    /// # Returns
    /// * 64-byte Ed25519 signature
    fn sign_checkpoint(&self, checkpoint_blob: &[u8; 98]) -> [u8; 64] {
        self.signing_key.sign(checkpoint_blob).to_bytes()
    }
}

impl std::fmt::Debug for CheckpointSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointSigner")
            .field("key_id", &format_hash(&self.key_id))
            .finish_non_exhaustive()
    }
}

/// Generate a receipt for an entry (v2.0 with super_proof)
///
/// # Arguments
/// * `entry_id` - UUID of the entry
/// * `storage` - StorageEngine with access to super_slabs
/// * `signer` - Checkpoint signing key
/// * `options` - Generation options
///
/// # Returns
/// * `Receipt` on success with super_proof
///
/// # Errors
/// * `ServerError::EntryNotFound` if entry doesn't exist
/// * `ServerError::EntryNotInTree` if entry not yet indexed
/// * `ServerError::TreeNotClosed` if entry's tree not yet in Super-Tree
/// * `ServerError::SuperTreeNotInitialized` if no trees closed yet
/// * `ServerError::Storage` for storage errors
#[allow(dead_code)]
pub async fn generate_receipt(
    entry_id: &Uuid,
    storage: &StorageEngine,
    signer: &CheckpointSigner,
    options: ReceiptOptions,
) -> ServerResult<Receipt> {
    // 1. Fetch entry
    let entry = storage.get_entry(entry_id)?;

    // 2. Verify entry is in tree
    let leaf_index = entry
        .leaf_index
        .ok_or_else(|| ServerError::EntryNotInTree(entry_id.to_string()))?;

    // 3. Get tree head
    let tree_head = storage.tree_head();
    let tree_size = options.at_tree_size.unwrap_or(tree_head.tree_size);

    // Validate tree size
    if leaf_index >= tree_size {
        return Err(ServerError::LeafIndexOutOfBounds {
            index: leaf_index,
            tree_size,
        });
    }

    // 4. Generate inclusion proof
    let inclusion_proof = storage.get_inclusion_proof(entry_id, Some(tree_size))?;

    // 5. Get root hash for the specified tree size
    let root_hash = if tree_size == tree_head.tree_size {
        tree_head.root_hash
    } else {
        // Historical receipt - not yet implemented
        return Err(ServerError::NotSupported(
            "historical receipts not yet implemented".into(),
        ));
    };

    // 6. Create and sign checkpoint
    let timestamp = options.timestamp.unwrap_or_else(current_timestamp_nanos);
    let origin = storage.origin_id();

    let checkpoint = create_signed_checkpoint(origin, tree_size, root_hash, timestamp, signer);

    let checkpoint_json = CheckpointJson {
        origin: format_hash(&checkpoint.origin),
        tree_size: checkpoint.tree_size,
        root_hash: format_hash(&checkpoint.root_hash),
        timestamp: checkpoint.timestamp,
        signature: format_signature(&checkpoint.signature),
        key_id: format_hash(&checkpoint.key_id),
    };

    // 7. Generate super_proof (may be None for active tree entries)
    let super_proof = generate_super_proof(entry_id, storage).await?;

    // 8. Get all anchors
    let all_anchors = if options.include_anchors {
        storage.get_anchors(tree_size)?
    } else {
        Vec::new()
    };

    // 9. Filter anchors based on super_tree coverage (only if super_proof available)
    let filtered_anchors = if let Some(ref proof) = super_proof {
        select_anchors_for_receipt(&all_anchors, proof.data_tree_index)
    } else {
        // No super_proof yet, include TSA anchors only
        all_anchors
            .iter()
            .filter(|a| matches!(a.anchor_type, crate::traits::anchor::AnchorType::Rfc3161))
            .cloned()
            .collect()
    };

    // 10. Generate upgrade_url logic
    let has_super_proof = super_proof.is_some();
    let has_confirmed_ots = filtered_anchors.iter().any(|a| {
        matches!(a.anchor_type, crate::traits::anchor::AnchorType::BitcoinOts)
            && a.metadata.get("status").and_then(|v| v.as_str()) == Some("confirmed")
    });

    // upgrade_url: show when no super_proof OR no confirmed OTS
    let upgrade_url = if !has_super_proof || !has_confirmed_ots {
        options.upgrade_url_template.as_ref().map(|template| {
            // Support both {entry_id} and {} placeholders via sequential replace
            template
                .replace("{entry_id}", &entry_id.to_string())
                .replace("{}", &entry_id.to_string())
        })
    } else {
        None
    };

    // 11. Generate consistency proof (Split-View protection)
    let consistency_proof = determine_consistency_proof(storage, tree_size, &options)?;

    // 12. Assemble receipt
    Ok(Receipt {
        spec_version: "2.0.0".to_string(),
        upgrade_url,
        entry: ReceiptEntry {
            id: entry.id,
            payload_hash: format_hash(&entry.payload_hash),
            metadata: entry.metadata_cleartext.unwrap_or(serde_json::Value::Null),
        },
        proof: ReceiptProof {
            tree_size,
            root_hash: format_hash(&root_hash),
            inclusion_path: inclusion_proof.path.iter().map(format_hash).collect(),
            leaf_index,
            checkpoint: checkpoint_json,
            consistency_proof,
        },
        super_proof,
        anchors: filtered_anchors
            .iter()
            .map(convert_anchor_to_receipt)
            .collect(),
    })
}

/// Create and sign a checkpoint
///
/// Internal helper that builds a checkpoint and signs it.
#[allow(dead_code)]
fn create_signed_checkpoint(
    origin: [u8; 32],
    tree_size: u64,
    root_hash: [u8; 32],
    timestamp: u64,
    signer: &CheckpointSigner,
) -> Checkpoint {
    // Build checkpoint wire format
    let mut checkpoint = Checkpoint::new(
        origin,
        tree_size,
        timestamp,
        root_hash,
        [0u8; 64],
        *signer.key_id(),
    );

    // Sign the blob
    let blob = checkpoint.to_bytes();
    checkpoint.signature = signer.sign_checkpoint(&blob);

    checkpoint
}

/// Select anchors that are valid for v2.0 receipt with super_proof
///
/// - TSA anchors: include if target matches entry's tree root
/// - OTS anchors: include ONLY if:
///   1. target == "super_root"
///   2. super_tree_size >= data_tree_index + 1
///
/// # Arguments
/// * `all_anchors` - All available anchors from storage
/// * `data_tree_index` - Entry's data tree index in Super-Tree
fn select_anchors_for_receipt(
    all_anchors: &[crate::traits::anchor::Anchor],
    data_tree_index: u64,
) -> Vec<crate::traits::anchor::Anchor> {
    all_anchors
        .iter()
        .filter(|anchor| {
            match anchor.anchor_type {
                crate::traits::anchor::AnchorType::Rfc3161 => {
                    // TSA anchors always target data_tree_root, include them
                    anchor.target == "data_tree_root"
                }
                crate::traits::anchor::AnchorType::BitcoinOts => {
                    // OTS anchors must:
                    // 1. Target super_root (not legacy data_tree_root)
                    // 2. Cover entry's data_tree_index
                    anchor.target == "super_root"
                        && anchor.super_tree_size.unwrap_or(0) > data_tree_index
                }
                crate::traits::anchor::AnchorType::Other => false,
            }
        })
        .cloned()
        .collect()
}

/// Generate SuperProof for an entry (uses super_slabs directly)
///
/// # Arguments
/// * `entry_id` - UUID of the entry
/// * `storage` - StorageEngine with access to super_slabs and index_store
///
/// # Returns
/// * `Ok(Some(proof))` - Entry's tree is closed and in Super-Tree
/// * `Ok(None)` - Entry's tree is active (not yet in Super-Tree)
/// * `Err(...)` - Only for real errors (DB failure, entry not found, etc.)
///
/// # Errors
/// * `ServerError::EntryNotFound` if entry doesn't exist
/// * `ServerError::Storage` for storage errors
#[allow(dead_code)]
async fn generate_super_proof(
    entry_id: &Uuid,
    storage: &StorageEngine,
) -> ServerResult<Option<atl_core::SuperProof>> {
    // 1. Get entry's tree info
    let entry = {
        let index_store = storage.index_store();
        let index = index_store.lock().await;
        index
            .get_entry(entry_id)?
            .ok_or_else(|| ServerError::EntryNotFound(entry_id.to_string()))?
    };

    // 2. Check if entry has tree_id
    let tree_id = match entry.tree_id {
        Some(id) => id,
        None => {
            // Entry in active tree - no super_proof available
            tracing::debug!(
                entry_id = %entry_id,
                "Entry in active tree, super_proof not available"
            );
            return Ok(None);
        }
    };

    // 3. Check if tree has data_tree_index (is in Super-Tree)
    let data_tree_index = {
        let index_store = storage.index_store();
        let index = index_store.lock().await;
        match index.get_tree_data_tree_index(tree_id)? {
            Some(idx) => idx,
            None => {
                // Tree exists but not yet in Super-Tree (unusual but possible)
                tracing::warn!(tree_id = tree_id, "Tree exists but data_tree_index is NULL");
                return Ok(None);
            }
        }
    };

    // 4. Get Super-Tree state
    let (genesis_super_root, super_tree_size) = {
        let index_store = storage.index_store();
        let index = index_store.lock().await;
        let genesis = match index.get_super_genesis_root()? {
            Some(g) => g,
            None => {
                // Super-Tree not initialized (should not happen after first tree close)
                tracing::warn!("Super-Tree genesis root not found");
                return Ok(None);
            }
        };
        let size = index.get_super_tree_size()?;
        (genesis, size)
    };

    // 5. Validate data_tree_index is within bounds
    if data_tree_index >= super_tree_size {
        // Data corruption or race condition
        tracing::error!(
            data_tree_index = data_tree_index,
            super_tree_size = super_tree_size,
            "data_tree_index out of bounds"
        );
        return Ok(None);
    }

    // 6. Get inclusion proof (direct SlabManager call)
    let inclusion_path = {
        let mut super_slabs = storage.super_slabs().write().await;
        super_slabs
            .get_inclusion_path(data_tree_index, super_tree_size)
            .map_err(|e| ServerError::Storage(crate::error::StorageError::Io(e)))?
    };

    // 5. Get super_root
    let super_root = {
        let mut super_slabs = storage.super_slabs().write().await;
        super_slabs
            .get_root(super_tree_size)
            .map_err(|e| ServerError::Storage(crate::error::StorageError::Io(e)))?
    };

    // 6. Get consistency proof to origin (from size 1 to current)
    let consistency_path = {
        let mut super_slabs = storage.super_slabs().write().await;
        let slabs_cell = std::cell::RefCell::new(&mut *super_slabs);
        atl_core::generate_consistency_proof(1, super_tree_size, |level, index| {
            slabs_cell
                .borrow_mut()
                .get_node(level, index)
                .ok()
                .flatten()
        })?
        .path
    };

    // 7. Assemble atl_core::SuperProof (uses existing type)
    Ok(Some(atl_core::SuperProof {
        genesis_super_root: format_hash(&genesis_super_root),
        data_tree_index,
        super_tree_size,
        super_root: format_hash(&super_root),
        inclusion: inclusion_path.iter().map(format_hash).collect(),
        consistency_to_origin: consistency_path.iter().map(format_hash).collect(),
    }))
}

/// Build an immediate receipt from dispatch result
///
/// Used for POST `/v1/anchor` responses where entry is just appended
/// and not yet fully indexed. Does NOT query storage for entry data.
///
/// # Arguments
/// * `dispatch_result` - Result from sequencer dispatch
/// * `payload_hash` - Original payload hash from request
/// * `metadata` - Original metadata from request
/// * `storage` - Storage engine for tree head and origin
/// * `signer` - Checkpoint signer
/// * `base_url` - Server base URL for `upgrade_url`
///
/// # Returns
/// * `Receipt` with:
///   - `spec_version` = "2.0.0"
///   - `super_proof` = None (tree not closed, use upgrade_url to get full proof)
///   - `anchors` = [] (no anchors yet)
///   - `upgrade_url` = `Some(...)` (REQUIRED for immediate receipts)
///
/// # Notes
/// * Returns `super_proof = None` (entry in active tree)
/// * Clients MUST use `upgrade_url` to get full receipt with valid `super_proof`
/// * Does NOT include anchors (none exist yet)
/// * Does NOT query storage for entry (uses `dispatch_result`)
///
/// # Errors
/// * `ServerError::Storage` if inclusion proof generation fails
pub fn build_immediate_receipt(
    dispatch_result: &crate::traits::dispatcher::DispatchResult,
    payload_hash: [u8; 32],
    metadata: Option<serde_json::Value>,
    storage: &StorageEngine,
    signer: &CheckpointSigner,
    base_url: &str,
) -> ServerResult<Receipt> {
    let entry_id = dispatch_result.result.id;
    let leaf_index = dispatch_result.result.leaf_index;
    let tree_size = dispatch_result.result.tree_head.tree_size;
    let root_hash = dispatch_result.result.tree_head.root_hash;

    // Get origin from storage (static, always available)
    let origin = storage.origin_id();

    // Create and sign checkpoint
    let timestamp = current_timestamp_nanos();
    let checkpoint = create_signed_checkpoint(origin, tree_size, root_hash, timestamp, signer);

    let checkpoint_json = CheckpointJson {
        origin: format_hash(&checkpoint.origin),
        tree_size: checkpoint.tree_size,
        root_hash: format_hash(&checkpoint.root_hash),
        timestamp: checkpoint.timestamp,
        signature: format_signature(&checkpoint.signature),
        key_id: format_hash(&checkpoint.key_id),
    };

    // Generate inclusion proof from dispatch result
    // The sequencer already indexed the entry synchronously
    let inclusion_proof = storage.get_inclusion_proof(&entry_id, Some(tree_size))?;

    // Generate upgrade URL (REQUIRED for immediate receipts)
    let upgrade_url = Some(format!("{}/v1/anchor/{}", base_url, entry_id));

    Ok(Receipt {
        spec_version: "2.0.0".to_string(),
        upgrade_url,
        entry: ReceiptEntry {
            id: entry_id,
            payload_hash: format_hash(&payload_hash),
            metadata: metadata.unwrap_or(serde_json::Value::Null),
        },
        proof: ReceiptProof {
            tree_size,
            root_hash: format_hash(&root_hash),
            inclusion_path: inclusion_proof.path.iter().map(format_hash).collect(),
            leaf_index,
            checkpoint: checkpoint_json,
            consistency_proof: None, // Not needed for immediate receipt
        },
        super_proof: None, // Entry in active tree, not yet in Super-Tree
        anchors: vec![],   // No anchors yet
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_blob_format() {
        let signer = CheckpointSigner::from_bytes(&[42u8; 32]);
        let origin = [0xAAu8; 32];
        let root_hash = [0xBBu8; 32];

        let checkpoint = signer.sign_checkpoint_struct(origin, 12345, &root_hash);
        let blob = checkpoint.to_bytes();

        // Verify Magic bytes
        assert_eq!(&blob[0..18], b"ATL-Protocol-v1-CP");

        // Verify origin
        assert_eq!(&blob[18..50], &origin);

        // Verify tree_size (Little-Endian)
        assert_eq!(u64::from_le_bytes(blob[50..58].try_into().unwrap()), 12345);

        // Verify root_hash
        assert_eq!(&blob[66..98], &root_hash);
    }

    #[test]
    fn test_checkpoint_signature_verification() {
        let signer = CheckpointSigner::from_bytes(&[42u8; 32]);
        let (key_id, public_key) = signer.public_key_info();

        let checkpoint = signer.sign_checkpoint_struct([0u8; 32], 100, &[1u8; 32]);

        // Verify key_id matches
        assert_eq!(checkpoint.key_id, key_id);

        // Verify signature can be verified
        let verifier = atl_core::CheckpointVerifier::from_bytes(&public_key).unwrap();
        assert!(checkpoint.verify(&verifier).is_ok());
    }
}
