//! Receipt generation implementation

use atl_core::{Checkpoint, CheckpointJson, Receipt, ReceiptEntry, ReceiptProof};
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
#[allow(dead_code)]
pub fn generate_receipt<S: Storage + ?Sized>(
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
        spec_version: "2.0.0".to_string(),
        upgrade_url: None,
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
        super_proof: atl_core::SuperProof {
            genesis_super_root: format_hash(&[0u8; 32]),
            data_tree_index: 0,
            super_tree_size: 1,
            super_root: format_hash(&root_hash),
            inclusion: vec![],
            consistency_to_origin: vec![],
        },
        anchors,
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
