//! Receipt generation implementation

use atl_core::{
    canonicalize_and_hash, Checkpoint, CheckpointJson, Receipt, ReceiptEntry, ReceiptProof,
};
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

    // 3. Determine tree_size and root_hash:
    //    - For entries in closed trees: use the closed tree's end_size and root_hash
    //    - For entries in the active tree: use the current tree_head
    let (tree_size, root_hash) = {
        // Look up the entry's tree_id via index_store (Entry trait type does not carry tree_id)
        let index_entry = {
            let index_store = storage.index_store();
            let index = index_store.lock().await;
            index
                .get_entry(entry_id)?
                .ok_or_else(|| ServerError::EntryNotFound(entry_id.to_string()))?
        };

        if let Some(tree_id) = index_entry.tree_id {
            // Entry belongs to a known tree — check if it is closed
            let tree_record = {
                let index_store = storage.index_store();
                let index = index_store.lock().await;
                index
                    .get_tree(tree_id)?
                    .ok_or_else(|| ServerError::EntryNotFound(entry_id.to_string()))?
            };

            match (tree_record.end_size, tree_record.root_hash) {
                (Some(end_size), Some(closed_root)) => {
                    // Closed tree: receipt must use the frozen end_size and root_hash
                    (end_size, closed_root)
                }
                _ => {
                    // Active tree: fall back to the current tree_head
                    let tree_head = storage.tree_head();
                    (
                        options.at_tree_size.unwrap_or(tree_head.tree_size),
                        tree_head.root_hash,
                    )
                }
            }
        } else {
            // No tree_id assigned yet (entry in active tree before any rotation)
            let tree_head = storage.tree_head();
            (
                options.at_tree_size.unwrap_or(tree_head.tree_size),
                tree_head.root_hash,
            )
        }
    };

    // Validate tree size
    if leaf_index >= tree_size {
        return Err(ServerError::LeafIndexOutOfBounds {
            index: leaf_index,
            tree_size,
        });
    }

    // 4. Generate inclusion proof
    let inclusion_proof = storage.get_inclusion_proof(entry_id, Some(tree_size))?;

    // 5. Create and sign checkpoint
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

    // 6. Resolve data_tree_index (needed for OTS anchor selection before super_proof)
    let data_tree_index = resolve_data_tree_index(entry_id, storage).await?;

    // 7. Get covering anchors
    let (filtered_anchors, ots_super_tree_size) = if options.include_anchors {
        let mut result = Vec::new();

        // TSA anchor: covers if tree_size >= leaf_index + 1
        if let Some(tsa) = storage.get_tsa_anchor_covering(leaf_index + 1)? {
            result.push(tsa);
        }

        // OTS anchor: only if data_tree_index is available
        let ots_size = if let Some(idx) = data_tree_index {
            if let Some(ots) = storage.get_ots_anchor_covering(idx)? {
                let size = ots.super_tree_size;
                result.push(ots);
                size
            } else {
                None
            }
        } else {
            None
        };

        (result, ots_size)
    } else {
        (Vec::new(), None)
    };

    // 8. Generate super_proof at OTS anchor's super_tree_size (or current if no OTS anchor)
    let super_proof = generate_super_proof(data_tree_index, storage, ots_super_tree_size).await?;

    // 9. Generate upgrade_url logic
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

    // 10. Generate consistency proof (Split-View protection)
    let consistency_proof = determine_consistency_proof(storage, tree_size, &options)?;

    // 11. Assemble receipt
    Ok(Receipt {
        spec_version: "2.0.0".to_string(),
        upgrade_url,
        entry: ReceiptEntry {
            id: entry.id,
            payload_hash: format_hash(&entry.payload_hash),
            metadata_hash: format_hash(&canonicalize_and_hash(
                &entry
                    .metadata_cleartext
                    .clone()
                    .unwrap_or(serde_json::json!({})),
            )),
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

/// Resolve the data_tree_index for an entry (if it belongs to a closed tree in the Super-Tree).
///
/// # Arguments
/// * `entry_id` - UUID of the entry
/// * `storage` - StorageEngine with access to index_store
///
/// # Returns
/// * `Ok(Some(idx))` - Entry's tree is closed and has a position in the Super-Tree
/// * `Ok(None)` - Entry is in the active tree or its tree is not yet in the Super-Tree
///
/// # Errors
/// * `ServerError::EntryNotFound` if entry doesn't exist in index
/// * `ServerError::Storage` for storage errors
async fn resolve_data_tree_index(
    entry_id: &Uuid,
    storage: &StorageEngine,
) -> ServerResult<Option<u64>> {
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
        None => return Ok(None),
    };

    // 3. Check if tree has data_tree_index
    let index_store = storage.index_store();
    let index = index_store.lock().await;
    Ok(index.get_tree_data_tree_index(tree_id)?)
}

/// Generate SuperProof for an entry (uses super_slabs directly)
///
/// # Arguments
/// * `data_tree_index` - The index of the entry's data tree in the Super-Tree. `None` means
///   the entry's tree is still active (not yet closed into the Super-Tree), in which case
///   `Ok(None)` is returned immediately without any storage access.
/// * `storage` - StorageEngine with access to super_slabs and index_store
/// * `target_super_tree_size` - If `Some`, build the proof at this exact super_tree_size
///   (used to match the OTS anchor's size). If `None`, uses the current super_tree_size.
///
/// # Returns
/// * `Ok(Some(proof))` - Entry's tree is closed and in Super-Tree
/// * `Ok(None)` - Entry's tree is active (not yet in Super-Tree)
/// * `Err(...)` - Only for real errors (DB failure, etc.)
///
/// # Errors
/// * `ServerError::Storage` for storage errors
async fn generate_super_proof(
    data_tree_index: Option<u64>,
    storage: &StorageEngine,
    target_super_tree_size: Option<u64>,
) -> ServerResult<Option<atl_core::SuperProof>> {
    let data_tree_index = match data_tree_index {
        Some(idx) => idx,
        None => return Ok(None),
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
        let size = match target_super_tree_size {
            Some(target) => target,
            None => index.get_super_tree_size()?,
        };
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

    // 6. Get inclusion proof (direct PinnedSuperTreeSlab call)
    let inclusion_path = {
        let super_slab = storage.super_slab().read().await;
        super_slab
            .get_inclusion_path(data_tree_index, super_tree_size)
            .map_err(|e| ServerError::Storage(crate::error::StorageError::Io(e)))?
    };

    // 5. Get super_root
    let super_root = {
        let super_slab = storage.super_slab().read().await;
        super_slab
            .get_root(super_tree_size)
            .map_err(|e| ServerError::Storage(crate::error::StorageError::Io(e)))?
    };

    // 6. Get consistency proof to origin (from size 1 to current)
    let consistency_path = {
        let super_slab = storage.super_slab().read().await;
        atl_core::generate_consistency_proof(1, super_tree_size, |level, index| {
            super_slab.get_node(level, index)
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

    // Generate inclusion proof directly from leaf_index (no SQLite query needed)
    let inclusion_proof = storage.get_inclusion_proof_by_leaf_index(leaf_index, Some(tree_size))?;

    // Generate upgrade URL (REQUIRED for immediate receipts)
    let upgrade_url = Some(format!("{}/v1/anchor/{}", base_url, entry_id));

    Ok(Receipt {
        spec_version: "2.0.0".to_string(),
        upgrade_url,
        entry: ReceiptEntry {
            id: entry_id,
            payload_hash: format_hash(&payload_hash),
            metadata_hash: format_hash(&canonicalize_and_hash(
                &metadata.clone().unwrap_or(serde_json::json!({})),
            )),
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

    #[test]
    fn test_checkpoint_signer_clone() {
        let signer1 = CheckpointSigner::from_bytes(&[42u8; 32]);
        let signer2 = signer1.clone();

        assert_eq!(signer1.key_id, signer2.key_id);
        assert_eq!(signer1.public_key_bytes(), signer2.public_key_bytes());
    }

    #[test]
    fn test_checkpoint_signer_debug() {
        let signer = CheckpointSigner::from_bytes(&[42u8; 32]);
        let debug_str = format!("{:?}", signer);
        assert!(debug_str.contains("CheckpointSigner"));
        assert!(debug_str.contains("key_id"));
    }

    #[test]
    fn test_checkpoint_signer_from_file_invalid_size() {
        use std::io::Write;
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("invalid_key.bin");

        // Write file with wrong size
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&[0u8; 16])
            .unwrap();

        let result = CheckpointSigner::from_file(path.to_str().unwrap());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("signing key must be 32 bytes"));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_checkpoint_signer_from_file_valid() {
        use std::io::Write;
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("valid_key.bin");

        // Write valid 32-byte key
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&[42u8; 32])
            .unwrap();

        let result = CheckpointSigner::from_file(path.to_str().unwrap());
        assert!(result.is_ok());

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_checkpoint_signer_from_file_not_found() {
        let result = CheckpointSigner::from_file("/nonexistent/path/to/key.bin");
        assert!(result.is_err());
    }

    #[test]
    fn test_create_signed_checkpoint() {
        let signer = CheckpointSigner::from_bytes(&[42u8; 32]);
        let origin = [0xAAu8; 32];
        let root_hash = [0xBBu8; 32];
        let tree_size = 100;
        let timestamp = 1234567890;

        let checkpoint = create_signed_checkpoint(origin, tree_size, root_hash, timestamp, &signer);

        assert_eq!(checkpoint.origin, origin);
        assert_eq!(checkpoint.tree_size, tree_size);
        assert_eq!(checkpoint.root_hash, root_hash);
        assert_eq!(checkpoint.timestamp, timestamp);
        assert_eq!(checkpoint.key_id, *signer.key_id());
    }

    #[test]
    fn test_checkpoint_signer_new() {
        let signing_key = SigningKey::from_bytes(&[1u8; 32]);
        let signer = CheckpointSigner::new(signing_key);

        assert_eq!(signer.key_id.len(), 32);
        assert_eq!(signer.public_key_bytes().len(), 32);
    }

    #[test]
    fn test_checkpoint_signer_from_bytes_deterministic() {
        let seed = [42u8; 32];
        let signer1 = CheckpointSigner::from_bytes(&seed);
        let signer2 = CheckpointSigner::from_bytes(&seed);

        assert_eq!(signer1.key_id, signer2.key_id);
        assert_eq!(signer1.public_key_bytes(), signer2.public_key_bytes());
    }

    #[test]
    fn test_checkpoint_signer_key_id() {
        let signer = CheckpointSigner::from_bytes(&[99u8; 32]);
        let key_id = signer.key_id();

        assert_eq!(key_id.len(), 32);
        assert_eq!(key_id, &signer.key_id);
    }

    #[test]
    fn test_checkpoint_signer_public_key_info() {
        let signer = CheckpointSigner::from_bytes(&[7u8; 32]);
        let (key_id, public_key) = signer.public_key_info();

        assert_eq!(key_id, signer.key_id);
        assert_eq!(public_key, signer.public_key_bytes());
    }

    #[test]
    fn test_checkpoint_signer_sign_checkpoint_struct() {
        let signer = CheckpointSigner::from_bytes(&[13u8; 32]);
        let origin = [0xAAu8; 32];
        let root_hash = [0xBBu8; 32];
        let tree_size = 42;

        let checkpoint = signer.sign_checkpoint_struct(origin, tree_size, &root_hash);

        assert_eq!(checkpoint.origin, origin);
        assert_eq!(checkpoint.tree_size, tree_size);
        assert_eq!(checkpoint.root_hash, root_hash);
        assert_eq!(checkpoint.key_id, signer.key_id);
        assert_ne!(checkpoint.signature, [0u8; 64]); // Should be signed
    }

    #[test]
    fn test_checkpoint_signer_different_seeds_different_keys() {
        let signer1 = CheckpointSigner::from_bytes(&[1u8; 32]);
        let signer2 = CheckpointSigner::from_bytes(&[2u8; 32]);

        assert_ne!(signer1.key_id, signer2.key_id);
        assert_ne!(signer1.public_key_bytes(), signer2.public_key_bytes());
    }

    #[test]
    fn test_create_signed_checkpoint_fields() {
        let signer = CheckpointSigner::from_bytes(&[77u8; 32]);
        let origin = [0xEEu8; 32];
        let root_hash = [0xFFu8; 32];
        let tree_size = 999;
        let timestamp = 9876543210;

        let checkpoint = create_signed_checkpoint(origin, tree_size, root_hash, timestamp, &signer);

        assert_eq!(checkpoint.origin, origin);
        assert_eq!(checkpoint.tree_size, tree_size);
        assert_eq!(checkpoint.root_hash, root_hash);
        assert_eq!(checkpoint.timestamp, timestamp);
        assert_eq!(checkpoint.key_id, *signer.key_id());
        assert_ne!(checkpoint.signature, [0u8; 64]);
    }

    #[test]
    fn test_checkpoint_signer_debug_format() {
        let signer = CheckpointSigner::from_bytes(&[88u8; 32]);
        let debug_output = format!("{:?}", signer);

        assert!(debug_output.contains("CheckpointSigner"));
        assert!(debug_output.contains("key_id"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_generate_receipt_without_anchors() {
        use crate::receipt::options::ReceiptOptions;
        use crate::storage::config::StorageConfig;
        use crate::storage::engine::StorageEngine;
        use crate::traits::AppendParams;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let origin = [1u8; 32];
        let engine = StorageEngine::new(config, origin).await.unwrap();

        let params = vec![AppendParams {
            payload_hash: [42u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];
        let batch = engine.append_batch(params).await.unwrap();
        let entry_id = batch.entries[0].id;

        let signer = CheckpointSigner::from_bytes(&[1u8; 32]);
        let options = ReceiptOptions {
            include_anchors: false,
            consistency_from: Some(0),
            auto_consistency_from_anchor: false,
            timestamp: Some(1_000_000),
            at_tree_size: None,
            upgrade_url_template: None,
        };

        let receipt = generate_receipt(&entry_id, &engine, &signer, options).await;
        assert!(
            receipt.is_ok(),
            "generate_receipt should succeed: {:?}",
            receipt.err()
        );
        let receipt = receipt.unwrap();
        assert!(
            receipt.anchors.is_empty(),
            "anchors should be empty when include_anchors=false"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_generate_receipt_with_tsa_anchor() {
        use crate::receipt::options::ReceiptOptions;
        use crate::storage::config::StorageConfig;
        use crate::storage::engine::StorageEngine;
        use crate::traits::anchor::{Anchor, AnchorType};
        use crate::traits::AppendParams;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let origin = [2u8; 32];
        let engine = StorageEngine::new(config, origin).await.unwrap();

        let params = vec![AppendParams {
            payload_hash: [10u8; 32],
            metadata_hash: [0u8; 32],
            metadata_cleartext: None,
            external_id: None,
        }];
        let batch = engine.append_batch(params).await.unwrap();
        let entry_id = batch.entries[0].id;

        // Store a confirmed TSA anchor covering tree_size=1
        {
            let index_store = engine.index_store();
            let index = index_store.lock().await;
            let anchor = Anchor {
                anchor_type: AnchorType::Rfc3161,
                target: "data_tree_root".to_string(),
                anchored_hash: [0u8; 32],
                tree_size: 1,
                super_tree_size: None,
                timestamp: 1_000_000,
                token: vec![],
                metadata: serde_json::json!({"status": "confirmed"}),
            };
            let anchor_id = index
                .store_anchor_returning_id(1, &anchor, "confirmed")
                .unwrap();
            index
                .update_anchor_metadata(anchor_id, serde_json::json!({"status": "confirmed"}))
                .unwrap();
        }

        let signer = CheckpointSigner::from_bytes(&[2u8; 32]);
        let options = ReceiptOptions {
            include_anchors: true,
            consistency_from: Some(0),
            auto_consistency_from_anchor: false,
            timestamp: Some(2_000_000),
            at_tree_size: None,
            upgrade_url_template: None,
        };

        let receipt = generate_receipt(&entry_id, &engine, &signer, options).await;
        assert!(
            receipt.is_ok(),
            "generate_receipt with TSA anchor should succeed: {:?}",
            receipt.err()
        );
        let receipt = receipt.unwrap();
        assert!(
            !receipt.anchors.is_empty(),
            "receipt should contain TSA anchor"
        );
    }

    /// Helper: create a StorageEngine backed by a temp directory.
    /// Returns (engine, _dir) — _dir must be kept alive for the engine to function.
    async fn make_engine(origin: [u8; 32]) -> (StorageEngine, tempfile::TempDir) {
        use crate::storage::config::StorageConfig;
        let dir = tempfile::tempdir().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let engine = StorageEngine::new(config, origin).await.unwrap();
        (engine, dir)
    }

    /// Helper: append one entry and return its UUID.
    async fn append_one(engine: &StorageEngine, seed: u8) -> uuid::Uuid {
        use crate::traits::AppendParams;
        let batch = engine
            .append_batch(vec![AppendParams {
                payload_hash: [seed; 32],
                metadata_hash: [0u8; 32],
                metadata_cleartext: None,
                external_id: None,
            }])
            .await
            .unwrap();
        batch.entries[0].id
    }

    /// Helper: default ReceiptOptions with anchors disabled.
    fn opts_no_anchors() -> ReceiptOptions {
        use crate::receipt::options::ReceiptOptions;
        ReceiptOptions {
            include_anchors: false,
            consistency_from: Some(0),
            auto_consistency_from_anchor: false,
            timestamp: Some(1_000_000),
            at_tree_size: None,
            upgrade_url_template: None,
        }
    }

    /// Helper: default ReceiptOptions with anchors enabled.
    fn opts_with_anchors() -> ReceiptOptions {
        use crate::receipt::options::ReceiptOptions;
        ReceiptOptions {
            include_anchors: true,
            consistency_from: Some(0),
            auto_consistency_from_anchor: false,
            timestamp: Some(1_000_000),
            at_tree_size: None,
            upgrade_url_template: None,
        }
    }

    // -------------------------------------------------------------------------
    // resolve_data_tree_index: entry whose IndexEntry.tree_id IS NULL
    // This exercises the `None => return Ok(None)` branch at line 403.
    // -------------------------------------------------------------------------
    #[tokio::test(flavor = "multi_thread")]
    async fn test_resolve_data_tree_index_no_tree_id_returns_none() {
        let (engine, _dir) = make_engine([10u8; 32]).await;

        // Insert a single normal entry so the tree has at least one leaf.
        let entry_id = append_one(&engine, 0xAA).await;

        // Overwrite tree_id with NULL directly in SQLite to simulate an active-
        // tree entry that has no tree assignment yet.
        {
            let index_store = engine.index_store();
            let index = index_store.lock().await;
            index
                .connection()
                .execute(
                    "UPDATE entries SET tree_id = NULL WHERE id = ?1",
                    rusqlite::params![entry_id.to_string()],
                )
                .unwrap();
        }

        let signer = CheckpointSigner::from_bytes(&[10u8; 32]);
        let receipt = generate_receipt(&entry_id, &engine, &signer, opts_no_anchors()).await;

        // Receipt generation must succeed and super_proof must be None because
        // data_tree_index resolved to None (entry is in the active tree).
        assert!(
            receipt.is_ok(),
            "generate_receipt must succeed for active-tree entry: {:?}",
            receipt.err()
        );
        assert!(
            receipt.unwrap().super_proof.is_none(),
            "super_proof must be None when entry has no tree_id"
        );
    }

    // -------------------------------------------------------------------------
    // generate_super_proof(None, ...) — early return Ok(None)
    // Lines 434–437.  Entry is in a freshly-created engine (no tree rotation yet)
    // so resolve_data_tree_index naturally returns None.
    // -------------------------------------------------------------------------
    #[tokio::test(flavor = "multi_thread")]
    async fn test_generate_super_proof_none_when_no_rotation() {
        let (engine, _dir) = make_engine([11u8; 32]).await;
        let entry_id = append_one(&engine, 0xBB).await;

        let signer = CheckpointSigner::from_bytes(&[11u8; 32]);
        let receipt = generate_receipt(&entry_id, &engine, &signer, opts_no_anchors()).await;

        assert!(
            receipt.is_ok(),
            "generate_receipt must succeed: {:?}",
            receipt.err()
        );
        assert!(
            receipt.unwrap().super_proof.is_none(),
            "super_proof must be None when tree has never been rotated"
        );
    }

    // -------------------------------------------------------------------------
    // OTS anchor absent: get_ots_anchor_covering returns None
    // Lines 278–279: the else branch inside `if let Some(idx) = data_tree_index`.
    // Entry is in a closed tree (data_tree_index assigned) but no OTS anchor exists.
    // -------------------------------------------------------------------------
    #[tokio::test(flavor = "multi_thread")]
    async fn test_generate_receipt_no_ots_anchor_after_rotation() {
        let (engine, _dir) = make_engine([12u8; 32]).await;

        // Append an entry and then rotate the tree so the entry ends up in a
        // closed tree with a valid data_tree_index.
        let entry_id = append_one(&engine, 0xCC).await;
        let origin = engine.origin_id();
        let tree_head = engine.tree_head();
        engine
            .rotate_tree(&origin, tree_head.tree_size, &tree_head.root_hash)
            .await
            .unwrap();

        let signer = CheckpointSigner::from_bytes(&[12u8; 32]);
        // Request anchors so the OTS branch is executed.
        let receipt = generate_receipt(&entry_id, &engine, &signer, opts_with_anchors()).await;

        assert!(
            receipt.is_ok(),
            "generate_receipt must succeed after rotation with no OTS: {:?}",
            receipt.err()
        );
        let receipt = receipt.unwrap();

        // No OTS anchor was stored, so ots_super_tree_size stays None and the
        // super_proof is generated using the current super_tree_size.
        // The anchors list must not contain a BitcoinOts entry.
        let has_ots = receipt
            .anchors
            .iter()
            .any(|a| matches!(a, atl_core::ReceiptAnchor::BitcoinOts { .. }));
        assert!(!has_ots, "anchors must not contain OTS when none is stored");

        // super_proof should be Some because the entry is in a closed tree.
        assert!(
            receipt.super_proof.is_some(),
            "super_proof must be present after tree rotation"
        );
    }

    // -------------------------------------------------------------------------
    // OTS anchor present: lines 274–277 (Some branch) + lines 451–452
    // (target_super_tree_size = Some(target) path in generate_super_proof).
    // -------------------------------------------------------------------------
    #[tokio::test(flavor = "multi_thread")]
    async fn test_generate_receipt_with_ots_anchor_uses_target_super_tree_size() {
        use crate::traits::anchor::{Anchor, AnchorType};

        let (engine, _dir) = make_engine([13u8; 32]).await;

        // Append entry and rotate so it has a data_tree_index.
        let entry_id = append_one(&engine, 0xDD).await;
        let origin = engine.origin_id();
        let tree_head = engine.tree_head();
        let rotation = engine
            .rotate_tree(&origin, tree_head.tree_size, &tree_head.root_hash)
            .await
            .unwrap();

        // data_tree_index of the closed tree is 0 (first rotation).
        // OTS query: super_tree_size > data_tree_index, so use 1.
        let ots_super_tree_size = rotation.data_tree_index + 1; // == 1

        // Store a confirmed OTS anchor that covers data_tree_index 0.
        {
            let index_store = engine.index_store();
            let index = index_store.lock().await;
            let anchor = Anchor {
                anchor_type: AnchorType::BitcoinOts,
                target: "super_root".to_string(),
                anchored_hash: [0xAAu8; 32],
                tree_size: 0,
                super_tree_size: Some(ots_super_tree_size),
                timestamp: 1_000_000,
                token: vec![],
                metadata: serde_json::json!({"status": "confirmed"}),
            };
            let anchor_id = index
                .store_anchor_returning_id(0, &anchor, "confirmed")
                .unwrap();
            index
                .update_anchor_metadata(anchor_id, serde_json::json!({"status": "confirmed"}))
                .unwrap();
        }

        let signer = CheckpointSigner::from_bytes(&[13u8; 32]);
        let receipt = generate_receipt(&entry_id, &engine, &signer, opts_with_anchors()).await;

        assert!(
            receipt.is_ok(),
            "generate_receipt with OTS anchor must succeed: {:?}",
            receipt.err()
        );
        let receipt = receipt.unwrap();

        // OTS anchor must be present in receipt.
        let has_ots = receipt
            .anchors
            .iter()
            .any(|a| matches!(a, atl_core::ReceiptAnchor::BitcoinOts { .. }));
        assert!(has_ots, "receipt must contain the OTS anchor");

        // super_proof must exist and use the OTS super_tree_size.
        let sp = receipt.super_proof.expect("super_proof must be present");
        assert_eq!(
            sp.super_tree_size, ots_super_tree_size,
            "super_proof super_tree_size must match OTS anchor super_tree_size"
        );
    }
}
