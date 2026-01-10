//! Receipt generation implementation

use atl_core::{
    Checkpoint, CheckpointJson, RECEIPT_SPEC_VERSION, Receipt, ReceiptEntry, ReceiptProof,
};
use ed25519_dalek::{Signer, SigningKey};
use uuid::Uuid;

use crate::error::{ServerError, ServerResult};
use crate::receipt::consistency::determine_consistency_proof;
use crate::receipt::convert::convert_anchor_to_receipt;
use crate::receipt::format::{current_timestamp_nanos, format_hash, format_signature};
use crate::receipt::options::ReceiptOptions;
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn from_bytes(seed: &[u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(seed);
        Self::new(signing_key)
    }

    /// Get the key ID (SHA256 of public key)
    #[must_use]
    pub const fn key_id(&self) -> &[u8; 32] {
        &self.key_id
    }

    /// Get public key bytes
    #[must_use]
    #[allow(dead_code)]
    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
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

/// Builder for fine-grained receipt generation
#[allow(dead_code)]
pub struct ReceiptGenerator<'a, S: Storage> {
    storage: &'a S,
    signer: &'a CheckpointSigner,
    options: ReceiptOptions,
}

impl<'a, S: Storage> ReceiptGenerator<'a, S> {
    /// Create a new receipt generator
    #[must_use]
    #[allow(dead_code)]
    pub const fn new(storage: &'a S, signer: &'a CheckpointSigner) -> Self {
        Self {
            storage,
            signer,
            options: ReceiptOptions::with_anchors(),
        }
    }

    /// Include anchors in receipt
    #[must_use]
    #[allow(dead_code)]
    pub fn with_anchors(mut self) -> Self {
        self.options.include_anchors = true;
        self
    }

    /// Include consistency proof from given tree size
    #[must_use]
    #[allow(dead_code)]
    pub fn with_consistency_from(mut self, from_size: u64) -> Self {
        self.options.consistency_from = Some(from_size);
        self
    }

    /// Set custom timestamp
    #[must_use]
    #[allow(dead_code)]
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.options.timestamp = Some(timestamp);
        self
    }

    /// Generate receipt for entry
    ///
    /// # Errors
    /// Returns error if entry not found or proof generation fails
    #[allow(dead_code)]
    pub fn generate(&self, entry_id: &Uuid) -> ServerResult<Receipt> {
        generate_receipt(entry_id, self.storage, self.signer, self.options.clone())
    }
}

/// Generate a receipt for an entry
///
/// # Arguments
/// * `entry_id` - UUID of the entry
/// * `storage` - Storage backend
/// * `signer` - Checkpoint signing key
/// * `options` - Generation options
///
/// # Returns
/// * `Receipt` on success
///
/// # Errors
/// * `ServerError::EntryNotFound` if entry doesn't exist
/// * `ServerError::EntryNotInTree` if entry not yet indexed
/// * `ServerError::Storage` for storage errors
pub fn generate_receipt<S: Storage>(
    entry_id: &Uuid,
    storage: &S,
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
    let tree_head = storage.get_tree_head()?;
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

    // 7. Get anchors (default: included)
    let anchors = if options.include_anchors {
        storage
            .get_anchors(tree_size)?
            .iter()
            .map(convert_anchor_to_receipt)
            .collect()
    } else {
        Vec::new()
    };

    // 8. Generate consistency proof (Split-View protection)
    let consistency_proof = determine_consistency_proof(storage, tree_size, &options)?;

    // 9. Assemble receipt
    Ok(Receipt {
        spec_version: RECEIPT_SPEC_VERSION.to_string(),
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
        anchors,
    })
}

/// Generate receipt with default options
///
/// # Errors
/// Returns error if entry not found or proof generation fails
#[allow(dead_code)]
pub fn generate_receipt_simple<S: Storage>(
    entry_id: &Uuid,
    storage: &S,
    signer: &CheckpointSigner,
) -> ServerResult<Receipt> {
    generate_receipt(entry_id, storage, signer, ReceiptOptions::default())
}

/// Create and sign a checkpoint
///
/// Internal helper that builds a checkpoint and signs it.
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
