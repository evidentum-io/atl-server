//! Receipt generation implementation

use atl_core::{
    canonicalize_and_hash, Checkpoint, CheckpointJson, Receipt, ReceiptBuilder, ReceiptEntry,
    ReceiptProof, SourceTextCheck,
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

/// One tree state, resolved once and shared by every part of a receipt.
///
/// ATL Protocol Section 5.2 (steps 3-4), Section 5.3 (step 2), Section
/// 5.5.1 (step 2) and Section 5.5.2 (step 2) each compare a different part
/// of the receipt against `proof.root_hash`. A receipt is therefore only
/// verifiable when the inclusion path, the checkpoint and every anchor were
/// built against one and the same tree size `N` and its root `root(N)`.
#[derive(Debug, Clone, Copy)]
struct ReceiptState {
    /// `N` -- the tree size the whole receipt describes.
    tree_size: u64,

    /// `root(N)`, read back from the Merkle slab: the same structure the
    /// inclusion path is generated from, so the two cannot disagree.
    root_hash: [u8; 32],

    /// Position of this state's Data Tree in the Super-Tree. Present only
    /// when `N` is the end size of a closed tree that was appended to the
    /// Super-Tree; `None` for an intermediate state, which is why an
    /// intermediate state can never carry a `super_proof`.
    data_tree_index: Option<u64>,
}

/// Resolve the single tree state a receipt for `entry_id` must describe.
///
/// The chosen `N` is the end size of the closed Data Tree the entry belongs
/// to. Super-Tree leaves are the roots of *closed* Data Trees (ATL Protocol
/// Section 3.3), so that state is the only one that can carry a
/// `super_proof` and therefore the only one that can ever become a
/// Receipt-Full. While the entry's tree is still open there is no frozen
/// state yet, so `N` is the current head: an intermediate state that no
/// RFC 3161 anchor covers and that is served as a Receipt-Lite.
///
/// `options.at_tree_size` overrides the choice for callers that ask for a
/// specific state. No special-casing is needed for such a state: anchors
/// and the Super-Tree position are both derived from `root(N)` below, so an
/// arbitrary intermediate size simply yields a receipt with no anchor and
/// no `super_proof` instead of a mismatched one.
///
/// # Errors
/// * [`ServerError::EntryNotFound`] if the entry or its tree is missing
/// * [`ServerError::LeafIndexOutOfBounds`] if the entry is not in state `N`
/// * [`ServerError::ReceiptStateMismatch`] if the closed tree's recorded
///   root disagrees with the Merkle root at `N`
/// * [`ServerError::Storage`] for storage errors
async fn resolve_receipt_state(
    entry_id: &Uuid,
    leaf_index: u64,
    storage: &StorageEngine,
    options: &ReceiptOptions,
) -> ServerResult<ReceiptState> {
    // Closed-tree bookkeeping: (end_size, recorded root, Super-Tree position).
    let closed = {
        let index_store = storage.index_store();
        let index = index_store.lock().await;
        let entry = index
            .get_entry(entry_id)?
            .ok_or_else(|| ServerError::EntryNotFound(entry_id.to_string()))?;

        match entry.tree_id {
            None => None,
            Some(tree_id) => {
                let tree = index
                    .get_tree(tree_id)?
                    .ok_or_else(|| ServerError::EntryNotFound(entry_id.to_string()))?;
                match (tree.end_size, tree.root_hash) {
                    (Some(end_size), Some(recorded_root)) => Some((
                        end_size,
                        recorded_root,
                        index.get_tree_data_tree_index(tree_id)?,
                    )),
                    // Still open, or closed without a recorded root: there is
                    // no frozen state to describe.
                    _ => None,
                }
            }
        }
    };

    let tree_size = options
        .at_tree_size
        .or_else(|| closed.map(|(end_size, _, _)| end_size))
        .unwrap_or_else(|| storage.tree_head().tree_size);

    if leaf_index >= tree_size {
        return Err(ServerError::LeafIndexOutOfBounds {
            index: leaf_index,
            tree_size,
        });
    }

    // The Merkle slab is the source of truth for `root(N)`: it is what the
    // inclusion path is computed from, so binding the checkpoint and the
    // anchors to it is what makes Section 5.2 step 3 and Section 5.3 step 2
    // hold by construction.
    let root_hash = storage.get_root_at_size(tree_size)?;

    match closed {
        Some((end_size, recorded_root, data_tree_index)) if end_size == tree_size => {
            // The closed tree carries its own copy of the root. When that
            // copy disagrees with the Merkle data, two of the server's own
            // records contradict each other and there is no safe way to pick
            // a winner: refuse rather than issue evidence that cannot verify.
            if recorded_root != root_hash {
                return Err(ServerError::ReceiptStateMismatch {
                    tree_size,
                    source_name: "closed tree record",
                    found: hex::encode(recorded_root),
                    expected: hex::encode(root_hash),
                });
            }
            Ok(ReceiptState {
                tree_size,
                root_hash,
                data_tree_index,
            })
        }
        // Any other `N` is an intermediate state: it is not a root that was
        // appended to the Super-Tree, so it has no Super-Tree position.
        _ => Ok(ReceiptState {
            tree_size,
            root_hash,
            data_tree_index: None,
        }),
    }
}

/// Find the RFC 3161 anchor that commits to exactly this state's root.
///
/// ATL Protocol Section 5.5.1 step 2 requires `anchor.target_hash` to equal
/// `proof.root_hash`, so the anchor is selected *by that root hash*, not by
/// any tree-size relation. A smaller `tree_size` is not by itself evidence
/// of an earlier time -- only the verified time inside the token is -- and
/// an anchor taken from a different tree state does not make a weaker
/// receipt, it makes an invalid one.
///
/// The returned row is re-checked against `root(N)`. The lookup predicate
/// and the re-checked field read the same column today, but a receipt's
/// validity must not rest on one column having been written correctly: an
/// anchor that does not commit to `root(N)` is refused, never quietly
/// swapped for a different one.
///
/// Returning `Ok(None)` is not a failure: an entry whose state has not been
/// timestamped yet is served as a Receipt-Lite (Section 5.5.3), with
/// `upgrade_url` pointing at the state that will carry the anchor.
///
/// # Errors
/// * [`ServerError::ReceiptStateMismatch`] if a returned anchor does not
///   commit to `root(N)`
/// * [`ServerError::Storage`] for storage errors
async fn find_tsa_anchor_for_state(
    storage: &StorageEngine,
    state: &ReceiptState,
) -> ServerResult<Option<crate::traits::anchor::Anchor>> {
    let found = {
        let index_store = storage.index_store();
        let index = index_store.lock().await;
        index.get_tsa_anchor_with_token_for_hash(&state.root_hash)?
    };

    let Some(found) = found else {
        return Ok(None);
    };

    if found.anchor.anchored_hash != state.root_hash {
        return Err(ServerError::ReceiptStateMismatch {
            tree_size: state.tree_size,
            source_name: "rfc3161 anchor",
            found: hex::encode(found.anchor.anchored_hash),
            expected: hex::encode(state.root_hash),
        });
    }

    Ok(Some(found.anchor))
}

/// Generate a receipt for an entry (v2.0 with `super_proof`)
///
/// Every part of the returned receipt describes one tree state `N`
/// (see [`ReceiptState`]): `proof.tree_size` is `N`, `proof.root_hash` and
/// `checkpoint.root_hash` are `root(N)`, the inclusion path is computed at
/// `N`, and an anchor is attached only when it commits to `root(N)`
/// (RFC 3161, ATL Protocol Section 5.5.1 step 2) or to the Super-Tree root
/// the `super_proof` carries (Bitcoin OTS, Section 5.5.2 step 2).
///
/// # Arguments
/// * `entry_id` - UUID of the entry
/// * `storage` - `StorageEngine` with access to `super_slabs`
/// * `signer` - Checkpoint signing key
/// * `options` - Generation options
///
/// # Returns
/// * `Receipt` on success
///
/// # Errors
/// * `ServerError::EntryNotFound` if entry doesn't exist
/// * `ServerError::EntryNotInTree` if entry not yet indexed
/// * `ServerError::LeafIndexOutOfBounds` if the entry is not in state `N`
/// * `ServerError::ReceiptStateMismatch` if the server's own records
///   disagree about `root(N)`, in which case no receipt is issued
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

    // 3. Resolve the one state the whole receipt will describe
    let state = resolve_receipt_state(entry_id, leaf_index, storage, &options).await?;

    // 4. Generate inclusion proof at that state
    let inclusion_proof = storage.get_inclusion_proof(entry_id, Some(state.tree_size))?;

    // 5. Create and sign the checkpoint for that state
    //
    // The timestamp is the moment of issue (Section 4.1, field 4: "the time
    // the checkpoint was generated"), not a reconstructed time for `N`. The
    // temporal claim about `N` is made by the RFC 3161 token, not by this
    // signature, so a historical checkpoint is signed on the fly and never
    // stored.
    let timestamp = options.timestamp.unwrap_or_else(current_timestamp_nanos);
    let origin = storage.origin_id();

    let checkpoint =
        create_signed_checkpoint(origin, state.tree_size, state.root_hash, timestamp, signer);

    let checkpoint_json = CheckpointJson {
        origin: format_hash(&checkpoint.origin),
        tree_size: checkpoint.tree_size,
        root_hash: format_hash(&checkpoint.root_hash),
        timestamp: checkpoint.timestamp,
        signature: format_signature(&checkpoint.signature),
        key_id: format_hash(&checkpoint.key_id),
    };

    // 6. Super-Tree proof, then anchors.
    //
    // The order is load-bearing: a Bitcoin OTS anchor targets
    // `super_proof.super_root` (Section 5.5.2 step 2), so it can only be
    // attached to a receipt that actually carries a `super_proof`. The
    // candidate anchor is looked up first only to pin the Super-Tree size
    // the proof is built at, so that the proof matches the anchored root.
    //
    // `get_ots_anchor_covering` already filters `status = 'confirmed'` at
    // the SQL column level, so finding an anchor at all *is* "there is a
    // confirmed OTS anchor" -- this must not be re-derived from
    // `anchor.metadata.get("status")`, a redundant, historically-written
    // copy of the same fact that carries no guarantee of being present or
    // in sync with the source-of-truth `status` column.
    let ots_candidate = match (options.include_anchors, state.data_tree_index) {
        (true, Some(data_tree_index)) => storage.get_ots_anchor_covering(data_tree_index)?,
        _ => None,
    };

    let super_proof = generate_super_proof(
        &state,
        storage,
        ots_candidate
            .as_ref()
            .and_then(|anchor| anchor.super_tree_size),
    )
    .await?;

    let mut anchors = Vec::new();
    let mut has_confirmed_ots = false;

    if options.include_anchors {
        if let Some(tsa) = find_tsa_anchor_for_state(storage, &state).await? {
            anchors.push(tsa);
        }

        if let (Some(ots), Some((_, super_root))) = (ots_candidate, super_proof.as_ref()) {
            if ots.anchored_hash != *super_root {
                return Err(ServerError::ReceiptStateMismatch {
                    tree_size: state.tree_size,
                    source_name: "bitcoin_ots anchor",
                    found: hex::encode(ots.anchored_hash),
                    expected: hex::encode(super_root),
                });
            }
            anchors.push(ots);
            has_confirmed_ots = true;
        }
    }

    let super_proof = super_proof.map(|(proof, _)| proof);

    // 7. Generate upgrade_url logic
    // upgrade_url: show when no super_proof OR no confirmed OTS
    let upgrade_url = if super_proof.is_none() || !has_confirmed_ots {
        options.upgrade_url_template.as_ref().map(|template| {
            // Support both {entry_id} and {} placeholders via sequential replace
            template
                .replace("{entry_id}", &entry_id.to_string())
                .replace("{}", &entry_id.to_string())
        })
    } else {
        None
    };

    // 8. Generate consistency proof (Split-View protection)
    let consistency_proof = determine_consistency_proof(storage, state.tree_size, &options)?;

    // 9. Publish the entry's metadata document.
    //
    //    An absent metadata document is published as the empty object -- the
    //    same value the HTTP ingress hashes into the leaf (`hash_metadata`)
    //    and the value Section 4.2 shows in the schema. Publishing `null`
    //    instead made the verifier canonicalize a different document than the
    //    one the tree committed to, so Section 5.1 step 4 could never pass
    //    for an entry anchored without metadata.
    let published_metadata = entry
        .metadata_cleartext
        .clone()
        .unwrap_or_else(|| serde_json::json!({}));

    // 10. Assemble receipt
    let receipt_entry = ReceiptEntry {
        id: entry.id,
        payload_hash: format_hash(&entry.payload_hash),
        metadata_hash: format_hash(&hash_published_metadata(&published_metadata)?),
        metadata: published_metadata,
    };

    let receipt_proof = ReceiptProof {
        tree_size: state.tree_size,
        root_hash: format_hash(&state.root_hash),
        inclusion_path: inclusion_proof.path.iter().map(format_hash).collect(),
        leaf_index,
        checkpoint: checkpoint_json,
        consistency_proof,
    };

    Ok(
        ReceiptBuilder::new("2.0.0".to_string(), receipt_entry, receipt_proof)
            .super_proof_option(super_proof)
            .anchors(anchors.iter().map(convert_anchor_to_receipt).collect())
            .upgrade_url_option(upgrade_url)
            .build(issuance_provenance()),
    )
}

/// Hash the metadata document that the receipt publishes.
///
/// # Errors
/// [`ServerError::NotCanonicalizable`] if the document has no RFC 8785
/// canonical form. The refusal is reported as a client error rather than
/// through `ServerError::Core`, which is a 500: the document is the
/// depositor's, not the server's state, and no operator action would fix it.
/// It is never replaced by a default hash -- that would publish a
/// `metadata_hash` describing nothing.
fn hash_published_metadata(metadata: &serde_json::Value) -> ServerResult<[u8; 32]> {
    canonicalize_and_hash(metadata).map_err(|e| ServerError::NotCanonicalizable(e.to_string()))
}

/// Provenance for a receipt this server assembles at issuance.
///
/// [`SourceTextCheck`] states that the receipt's source bytes were checked for
/// the RFC 8785 Section 3.1 duplicate-property-name constraint. Here there are
/// no source bytes: the receipt is built field by field in memory and
/// serialized by this server, and two independent facts make the constraint
/// hold for the bytes it will become.
///
/// 1. **The receipt cannot be given a duplicate name.** Its own properties are
///    distinct struct fields, and `metadata` is a `serde_json::Value` whose
///    `Map` cannot hold one key twice. Whatever text `serde_json` produces from
///    that value repeats no name.
///
/// 2. **The metadata document itself was checked as text at ingress.** Every
///    route that can put a `metadata_cleartext` into the log now runs
///    `reject_duplicate_property_names` over the raw bytes before parsing them:
///    `api::handlers::anchor::anchor_json` over the HTTP request body, and both
///    gRPC anchor handlers over `metadata_json`. Storage does not reintroduce
///    the hazard either -- the column is written from `Value::to_string()` and
///    read back with `from_str`.
///
/// Fact 1 alone would justify the marker; fact 2 is what makes the
/// `metadata_hash` the receipt carries a commitment to a document the client
/// unambiguously sent. Entries written before those checks existed are covered
/// by fact 1 only: their stored text is duplicate-free because it was written
/// from a `Value`, but nothing verified the bytes the client originally sent.
const fn issuance_provenance() -> SourceTextCheck {
    SourceTextCheck::assume_duplicate_property_names_already_rejected()
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

/// Generate the Super-Tree proof for a receipt state.
///
/// A `super_proof` is only meaningful for a state whose root is a
/// Super-Tree leaf, i.e. the root of a *closed* Data Tree (ATL Protocol
/// Section 3.3). For any intermediate state `state.data_tree_index` is
/// `None` and this returns `Ok(None)`, which is what keeps an intermediate
/// receipt at the Receipt-TSA tier instead of emitting a `super_proof` that
/// Section 5.4.1 could not verify.
///
/// # Arguments
/// * `state` - The one state the receipt describes
/// * `storage` - `StorageEngine` with access to `super_slabs` and `index_store`
/// * `target_super_tree_size` - If `Some`, build the proof at this exact
///   `super_tree_size` (used to match the OTS anchor's size). If `None`,
///   uses the current `super_tree_size`.
///
/// # Returns
/// * `Ok(Some((proof, super_root)))` - state's tree is in the Super-Tree
/// * `Ok(None)` - state has no Super-Tree position
///
/// # Errors
/// * [`ServerError::ReceiptStateMismatch`] if the Super-Tree leaf at
///   `data_tree_index` is not `root(N)`
/// * `ServerError::Storage` for storage errors
async fn generate_super_proof(
    state: &ReceiptState,
    storage: &StorageEngine,
    target_super_tree_size: Option<u64>,
) -> ServerResult<Option<(atl_core::SuperProof, [u8; 32])>> {
    let Some(data_tree_index) = state.data_tree_index else {
        return Ok(None);
    };

    // The audit copy of the genesis root is read first and on its own, so no
    // task ever holds the Super-Tree lock while waiting for the index lock.
    let recorded_genesis = {
        let index_store = storage.index_store();
        let index = index_store.lock().await;
        index.get_super_genesis_root()?
    };

    let super_slab = storage.super_slab().read().await;

    // The Super-Tree's own leaf count is the source of truth for its size:
    // `rotate_tree` appends to the slab first and only then writes the SQLite
    // copy, explicitly as a best-effort audit record, so the slab is the one
    // that cannot lag.
    let super_tree_size = target_super_tree_size.unwrap_or_else(|| super_slab.leaf_count());

    if super_tree_size == 0 {
        // Super-Tree not initialized (no Data Tree closed yet)
        return Ok(None);
    }

    // Validate data_tree_index is within bounds
    if data_tree_index >= super_tree_size {
        // Data corruption or race condition
        tracing::error!(
            data_tree_index = data_tree_index,
            super_tree_size = super_tree_size,
            "data_tree_index out of bounds"
        );
        return Ok(None);
    }

    // `genesis_super_root` is "the immutable identifier for the log instance"
    // (Section 3.3.2), and Section 5.4.2 feeds it to the verifier as the root
    // at Super-Tree size 1 in a consistency check that Receipt-Full MUST
    // pass. It is therefore computed from the Super-Tree itself -- the same
    // data the verifier's recomputation runs over -- and never taken from the
    // SQLite copy, whose write path is best-effort by construction and which
    // would hand a client a Receipt-Full that fails at Section 5.4.2.
    let genesis_super_root = super_slab
        .get_root(1)
        .map_err(|e| ServerError::Storage(crate::error::StorageError::Io(e)))?;

    // The audit copy is read to be checked, not to be trusted. The genesis is
    // immutable by definition, so a stored value that differs is not
    // staleness -- it is a corrupted record of the log's identity, and no
    // receipt is issued while the server's two records of that identity
    // disagree. A missing copy is the known best-effort failure mode and is
    // only worth a warning.
    match recorded_genesis {
        Some(recorded) if recorded != genesis_super_root => {
            return Err(ServerError::ReceiptStateMismatch {
                tree_size: state.tree_size,
                source_name: "super-tree genesis root",
                found: hex::encode(recorded),
                expected: hex::encode(genesis_super_root),
            });
        }
        Some(_) => {}
        None => tracing::warn!(
            "super_genesis_root audit record missing; using the Super-Tree root at size 1"
        ),
    }

    // Section 5.4.1 step 1: the verifier recomputes the Super Root from
    // `proof.root_hash` placed at `data_tree_index`. If the leaf actually
    // stored there is a different root, that recomputation cannot succeed,
    // so the proof must not be issued at all.
    match super_slab.get_node(0, data_tree_index) {
        Some(leaf) if leaf == state.root_hash => {}
        Some(leaf) => {
            return Err(ServerError::ReceiptStateMismatch {
                tree_size: state.tree_size,
                source_name: "super-tree leaf",
                found: hex::encode(leaf),
                expected: hex::encode(state.root_hash),
            });
        }
        None => {
            tracing::error!(
                data_tree_index = data_tree_index,
                "Super-Tree leaf missing for data_tree_index"
            );
            return Ok(None);
        }
    }

    let inclusion_path = super_slab
        .get_inclusion_path(data_tree_index, super_tree_size)
        .map_err(|e| ServerError::Storage(crate::error::StorageError::Io(e)))?;

    let super_root = super_slab
        .get_root(super_tree_size)
        .map_err(|e| ServerError::Storage(crate::error::StorageError::Io(e)))?;

    // Consistency proof to origin (from size 1 to super_tree_size)
    let consistency_path =
        atl_core::generate_consistency_proof(1, super_tree_size, |level, index| {
            super_slab.get_node(level, index)
        })?
        .path;

    Ok(Some((
        atl_core::SuperProof {
            genesis_super_root: format_hash(&genesis_super_root),
            data_tree_index,
            super_tree_size,
            super_root: format_hash(&super_root),
            inclusion: inclusion_path.iter().map(format_hash).collect(),
            consistency_to_origin: consistency_path.iter().map(format_hash).collect(),
        },
        super_root,
    )))
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

    let published_metadata = metadata.unwrap_or_else(|| serde_json::json!({}));

    let receipt_entry = ReceiptEntry {
        // An absent metadata document is published as the empty object,
        // matching what `hash_metadata` hashed into the leaf and what
        // Section 4.2 shows in the schema, so that Section 5.1 step 4 can
        // pass for an entry anchored without metadata.
        id: entry_id,
        payload_hash: format_hash(&payload_hash),
        metadata_hash: format_hash(&hash_published_metadata(&published_metadata)?),
        metadata: published_metadata,
    };

    let receipt_proof = ReceiptProof {
        tree_size,
        root_hash: format_hash(&root_hash),
        inclusion_path: inclusion_proof.path.iter().map(format_hash).collect(),
        leaf_index,
        checkpoint: checkpoint_json,
        consistency_proof: None, // Not needed for immediate receipt
    };

    // No `super_proof` (entry is in the active tree, not yet in the
    // Super-Tree) and no anchors yet, so neither is set on the builder.
    Ok(
        ReceiptBuilder::new("2.0.0".to_string(), receipt_entry, receipt_proof)
            .upgrade_url_option(upgrade_url)
            .build(issuance_provenance()),
    )
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
            receipt.anchors().is_empty(),
            "anchors should be empty when include_anchors=false"
        );
        // The builder must be handed the issuance provenance, not
        // `SourceTextCheck::default()`: without it `ReceiptVerifier::verify`
        // reports `SourceTextNotChecked` and refuses to confirm a receipt this
        // server issued, whatever else checks out.
        assert!(
            receipt.source_text_was_checked(),
            "an issued receipt must carry the source-text provenance"
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

        // Store a confirmed TSA anchor committing to root(1) -- the root the
        // receipt will be built against.
        let anchored_root = engine.tree_head().root_hash;
        {
            let index_store = engine.index_store();
            let index = index_store.lock().await;
            let anchor = Anchor {
                anchor_type: AnchorType::Rfc3161,
                target: "data_tree_root".to_string(),
                anchored_hash: anchored_root,
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
            !receipt.anchors().is_empty(),
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
            receipt.unwrap().super_proof().is_none(),
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
            receipt.unwrap().super_proof().is_none(),
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
            .anchors()
            .iter()
            .any(|a| matches!(a, atl_core::ReceiptAnchor::BitcoinOts { .. }));
        assert!(!has_ots, "anchors must not contain OTS when none is stored");

        // super_proof should be Some because the entry is in a closed tree.
        assert!(
            receipt.super_proof().is_some(),
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
                anchored_hash: rotation.super_root,
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
            .anchors()
            .iter()
            .any(|a| matches!(a, atl_core::ReceiptAnchor::BitcoinOts { .. }));
        assert!(has_ots, "receipt must contain the OTS anchor");

        // super_proof must exist and use the OTS super_tree_size.
        let sp = receipt.super_proof().expect("super_proof must be present");
        assert_eq!(
            sp.super_tree_size, ots_super_tree_size,
            "super_proof super_tree_size must match OTS anchor super_tree_size"
        );
    }

    /// Regression: `upgrade_url` must reflect the anchor's actual
    /// `status` column (via `get_ots_anchor_covering`, which only ever
    /// returns `confirmed` rows), not the anchor's `metadata.status`
    /// field. This anchor is confirmed at the column level but its
    /// `metadata` deliberately omits `status` entirely (as a real
    /// pre-existing row might, e.g. one confirmed before that metadata
    /// field was introduced) -- if the check still depended on
    /// `metadata.status`, this would incorrectly show an `upgrade_url` for
    /// an already-confirmed anchor.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_generate_receipt_upgrade_url_ignores_metadata_status() {
        use crate::traits::anchor::{Anchor, AnchorType};

        let (engine, _dir) = make_engine([14u8; 32]).await;

        let entry_id = append_one(&engine, 0xEE).await;
        let origin = engine.origin_id();
        let tree_head = engine.tree_head();
        let rotation = engine
            .rotate_tree(&origin, tree_head.tree_size, &tree_head.root_hash)
            .await
            .unwrap();

        let ots_super_tree_size = rotation.data_tree_index + 1;

        // Confirmed at the column level (status = 'confirmed'), but
        // `metadata` has no `status` key at all.
        {
            let index_store = engine.index_store();
            let index = index_store.lock().await;
            let anchor = Anchor {
                anchor_type: AnchorType::BitcoinOts,
                target: "super_root".to_string(),
                anchored_hash: rotation.super_root,
                tree_size: 0,
                super_tree_size: Some(ots_super_tree_size),
                timestamp: 1_000_000,
                token: vec![],
                metadata: serde_json::json!({"calendar_url": "https://ots.example.com"}),
            };
            index
                .store_anchor_returning_id(0, &anchor, "confirmed")
                .unwrap();
        }

        let signer = CheckpointSigner::from_bytes(&[14u8; 32]);
        let mut options = opts_with_anchors();
        options.upgrade_url_template = Some("https://example.com/upgrade/{entry_id}".to_string());
        let receipt = generate_receipt(&entry_id, &engine, &signer, options).await;

        assert!(
            receipt.is_ok(),
            "generate_receipt must succeed: {:?}",
            receipt.err()
        );
        let receipt = receipt.unwrap();

        assert!(
            receipt.super_proof().is_some(),
            "super_proof must be present"
        );
        assert!(
            receipt.upgrade_url().is_none(),
            "a column-confirmed OTS anchor must suppress upgrade_url even when \
             metadata.status is absent/out of sync"
        );
    }

    // =====================================================================
    // Receipt state consistency (ATL Protocol Section 5.2 steps 3-4,
    // Section 5.3 step 2, Section 5.5.1 step 2, Section 5.5.2 step 2)
    // =====================================================================

    /// Assert the receipt describes exactly one tree state: the checkpoint,
    /// the inclusion proof and every anchor must all commit to the same root.
    fn assert_single_state(receipt: &atl_core::Receipt) {
        assert_eq!(
            receipt.proof().checkpoint.tree_size,
            receipt.proof().tree_size,
            "checkpoint.tree_size must equal proof.tree_size"
        );
        assert_eq!(
            receipt.proof().checkpoint.root_hash,
            receipt.proof().root_hash,
            "checkpoint.root_hash must equal proof.root_hash"
        );

        for anchor in receipt.anchors() {
            match anchor {
                atl_core::ReceiptAnchor::Rfc3161 {
                    target,
                    target_hash,
                    ..
                } => {
                    assert_eq!(target, "data_tree_root");
                    assert_eq!(
                        target_hash,
                        &receipt.proof().root_hash,
                        "rfc3161 anchor.target_hash must equal proof.root_hash"
                    );
                }
                atl_core::ReceiptAnchor::BitcoinOts {
                    target,
                    target_hash,
                    ..
                } => {
                    assert_eq!(target, "super_root");
                    let super_proof = receipt
                        .super_proof()
                        .expect("a bitcoin_ots anchor requires a super_proof to target");
                    assert_eq!(
                        target_hash, &super_proof.super_root,
                        "bitcoin_ots anchor.target_hash must equal super_proof.super_root"
                    );
                }
            }
        }
    }

    /// Helper: store a confirmed RFC 3161 anchor committing to `root`.
    async fn store_tsa_anchor(engine: &StorageEngine, root: &[u8; 32], tree_size: u64) {
        use crate::traits::anchor::{Anchor, AnchorType};
        let index_store = engine.index_store();
        let index = index_store.lock().await;
        index
            .store_anchor_returning_id(
                tree_size,
                &Anchor {
                    anchor_type: AnchorType::Rfc3161,
                    target: "data_tree_root".to_string(),
                    anchored_hash: *root,
                    tree_size,
                    super_tree_size: None,
                    timestamp: 1_000_000,
                    token: vec![],
                    metadata: serde_json::json!({"tsa_url": "https://tsa.example.com"}),
                },
                "confirmed",
            )
            .unwrap();
    }

    /// Helper: close the active tree at the current head, returning the
    /// closed state (end size and root).
    async fn close_tree(engine: &StorageEngine) -> (u64, [u8; 32]) {
        let origin = engine.origin_id();
        let head = engine.tree_head();
        engine
            .rotate_tree(&origin, head.tree_size, &head.root_hash)
            .await
            .unwrap();
        (head.tree_size, head.root_hash)
    }

    /// Regression for the production defect: an entry whose leaf index sits
    /// below an already anchored tree size must not be handed that anchor
    /// when its receipt describes a different state.
    ///
    /// The entry's own Data Tree was closed but never timestamped, while a
    /// later tree was. Selecting "the smallest confirmed anchor whose
    /// tree_size still covers this leaf" hands the later tree's anchor to
    /// this receipt, so `anchor.target_hash` and `proof.root_hash` name two
    /// different tree states and Section 5.5.1 step 2 rejects the receipt.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_unanchored_tree_does_not_borrow_a_later_anchor() {
        let (engine, _dir) = make_engine([20u8; 32]).await;

        let entry_id = append_one(&engine, 0x01).await;
        append_one(&engine, 0x02).await;
        let (size_a, root_a) = close_tree(&engine).await;
        // Deliberately no anchor for tree A.

        append_one(&engine, 0x03).await;
        append_one(&engine, 0x04).await;
        let (size_b, root_b) = close_tree(&engine).await;
        store_tsa_anchor(&engine, &root_b, size_b).await;

        assert_ne!(root_a, root_b);
        assert!(size_a < size_b);

        let signer = CheckpointSigner::from_bytes(&[20u8; 32]);
        let receipt = generate_receipt(&entry_id, &engine, &signer, opts_with_anchors())
            .await
            .expect("receipt for an unanchored closed tree is a Receipt-Lite, not an error");

        assert_eq!(
            receipt.proof().tree_size,
            size_a,
            "receipt must describe the entry's own closed tree"
        );
        assert_eq!(receipt.proof().root_hash, format_hash(&root_a));
        assert!(
            !receipt
                .anchors()
                .iter()
                .any(|a| matches!(a, atl_core::ReceiptAnchor::Rfc3161 { .. })),
            "tree A has no timestamp; tree B's anchor must not be attached"
        );
        assert_single_state(&receipt);
    }

    /// Regression for the production shape: the proof is built against the
    /// current head while an anchor for an earlier, smaller tree size still
    /// "covers" this leaf index.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_head_state_does_not_borrow_an_earlier_anchor() {
        let (engine, _dir) = make_engine([21u8; 32]).await;

        let entry_id = append_one(&engine, 0x01).await;
        append_one(&engine, 0x02).await;
        let (anchored_size, anchored_root) = close_tree(&engine).await;
        store_tsa_anchor(&engine, &anchored_root, anchored_size).await;

        append_one(&engine, 0x03).await;
        append_one(&engine, 0x04).await;
        append_one(&engine, 0x05).await;

        // Detach the entry from its closed tree so the receipt state falls
        // back to the current head, which is what the affected production
        // receipts show: leaf index below the anchored size, proof against a
        // later root.
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

        let head = engine.tree_head();
        assert!(head.tree_size > anchored_size);
        assert_ne!(head.root_hash, anchored_root);

        let signer = CheckpointSigner::from_bytes(&[21u8; 32]);
        let receipt = generate_receipt(&entry_id, &engine, &signer, opts_with_anchors())
            .await
            .expect("receipt generation must succeed");

        assert_eq!(receipt.proof().tree_size, head.tree_size);
        assert_eq!(receipt.proof().root_hash, format_hash(&head.root_hash));
        assert!(
            receipt.anchors().is_empty(),
            "no anchor commits to the head root, so none may be attached"
        );
        assert_single_state(&receipt);
    }

    /// Every leaf, wherever it sits relative to the anchored tree sizes,
    /// yields a receipt whose anchor, proof and checkpoint name one state.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_single_state_holds_for_every_leaf_position() {
        let (engine, _dir) = make_engine([22u8; 32]).await;

        let mut entries = Vec::new();
        for seed in 0..3u8 {
            entries.push(append_one(&engine, seed).await);
        }
        let (size_a, root_a) = close_tree(&engine).await;
        store_tsa_anchor(&engine, &root_a, size_a).await;

        for seed in 3..6u8 {
            entries.push(append_one(&engine, seed).await);
        }
        let (size_b, root_b) = close_tree(&engine).await;
        store_tsa_anchor(&engine, &root_b, size_b).await;

        // Tail entries still in the open tree.
        for seed in 6..8u8 {
            entries.push(append_one(&engine, seed).await);
        }

        let signer = CheckpointSigner::from_bytes(&[22u8; 32]);
        for (leaf_index, entry_id) in entries.iter().enumerate() {
            let receipt = generate_receipt(entry_id, &engine, &signer, opts_with_anchors())
                .await
                .unwrap_or_else(|e| panic!("leaf {leaf_index}: {e}"));

            assert_single_state(&receipt);
            assert_eq!(receipt.proof().leaf_index, leaf_index as u64);

            let expected_root = if (leaf_index as u64) < size_a {
                format_hash(&root_a)
            } else if (leaf_index as u64) < size_b {
                format_hash(&root_b)
            } else {
                format_hash(&engine.tree_head().root_hash)
            };
            assert_eq!(
                receipt.proof().root_hash,
                expected_root,
                "leaf {leaf_index} must be proved against its own tree state"
            );

            let has_tsa = receipt
                .anchors()
                .iter()
                .any(|a| matches!(a, atl_core::ReceiptAnchor::Rfc3161 { .. }));
            assert_eq!(
                has_tsa,
                (leaf_index as u64) < size_b,
                "leaf {leaf_index}: only closed, anchored states carry a TSA anchor"
            );
        }
    }

    /// A leaf that lands exactly on the boundary -- the last leaf of a tree
    /// whose end size is the anchored size -- carries that tree's anchor.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_leaf_on_anchored_boundary_carries_its_own_anchor() {
        let (engine, _dir) = make_engine([23u8; 32]).await;

        append_one(&engine, 0x01).await;
        let boundary_entry = append_one(&engine, 0x02).await; // leaf_index + 1 == end size
        let (size_a, root_a) = close_tree(&engine).await;
        store_tsa_anchor(&engine, &root_a, size_a).await;

        append_one(&engine, 0x03).await;

        let signer = CheckpointSigner::from_bytes(&[23u8; 32]);
        let receipt = generate_receipt(&boundary_entry, &engine, &signer, opts_with_anchors())
            .await
            .expect("receipt generation must succeed");

        assert_eq!(receipt.proof().leaf_index + 1, size_a);
        assert_eq!(receipt.proof().tree_size, size_a);
        assert!(
            receipt
                .anchors()
                .iter()
                .any(|a| matches!(a, atl_core::ReceiptAnchor::Rfc3161 { .. })),
            "the boundary leaf's own state is anchored, so the anchor belongs on the receipt"
        );
        assert_single_state(&receipt);
    }

    /// An intermediate state is not a Super-Tree leaf, so it must never
    /// carry a `super_proof` (Section 3.3, Section 5.4.1 step 1).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_intermediate_state_carries_no_super_proof() {
        let (engine, _dir) = make_engine([24u8; 32]).await;

        let entry_id = append_one(&engine, 0x01).await;
        append_one(&engine, 0x02).await;
        append_one(&engine, 0x03).await;
        let (size_a, root_a) = close_tree(&engine).await;
        store_tsa_anchor(&engine, &root_a, size_a).await;

        let signer = CheckpointSigner::from_bytes(&[24u8; 32]);

        // The closed state does carry a super_proof.
        let closed_receipt = generate_receipt(&entry_id, &engine, &signer, opts_with_anchors())
            .await
            .unwrap();
        assert!(closed_receipt.super_proof().is_some());
        assert_single_state(&closed_receipt);

        // The same entry asked for at an intermediate size does not.
        let mut options = opts_with_anchors();
        options.at_tree_size = Some(1);
        let intermediate = generate_receipt(&entry_id, &engine, &signer, options)
            .await
            .unwrap();

        assert_eq!(intermediate.proof().tree_size, 1);
        assert!(
            intermediate.super_proof().is_none(),
            "an intermediate root is not a Super-Tree leaf"
        );
        assert!(
            intermediate.anchors().is_empty(),
            "no anchor commits to an intermediate root"
        );
        assert_single_state(&intermediate);
    }

    /// A closed tree whose recorded root disagrees with the Merkle root at
    /// its end size must produce a refusal, not a receipt.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_refuses_when_closed_tree_root_disagrees_with_merkle_root() {
        let (engine, _dir) = make_engine([25u8; 32]).await;

        let entry_id = append_one(&engine, 0x01).await;
        append_one(&engine, 0x02).await;
        let (_size_a, root_a) = close_tree(&engine).await;
        store_tsa_anchor(&engine, &root_a, 2).await;

        // Corrupt the bookkeeping copy of the root.
        {
            let index_store = engine.index_store();
            let index = index_store.lock().await;
            index
                .connection()
                .execute(
                    "UPDATE trees SET root_hash = ?1 WHERE root_hash = ?2",
                    rusqlite::params![[0x99u8; 32].as_slice(), root_a.as_slice()],
                )
                .unwrap();
        }

        let signer = CheckpointSigner::from_bytes(&[25u8; 32]);
        let err = generate_receipt(&entry_id, &engine, &signer, opts_with_anchors())
            .await
            .expect_err("a receipt must not be issued from contradictory records");

        match err {
            ServerError::ReceiptStateMismatch { source_name, .. } => {
                assert_eq!(source_name, "closed tree record");
            }
            other => panic!("expected ReceiptStateMismatch, got {other:?}"),
        }
    }

    /// A confirmed OTS anchor that does not commit to the Super Root the
    /// receipt carries must produce a refusal (Section 5.5.2 step 2).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_refuses_when_ots_anchor_does_not_commit_to_super_root() {
        use crate::traits::anchor::{Anchor, AnchorType};

        let (engine, _dir) = make_engine([26u8; 32]).await;

        let entry_id = append_one(&engine, 0x01).await;
        let origin = engine.origin_id();
        let head = engine.tree_head();
        let rotation = engine
            .rotate_tree(&origin, head.tree_size, &head.root_hash)
            .await
            .unwrap();

        {
            let index_store = engine.index_store();
            let index = index_store.lock().await;
            index
                .store_anchor_returning_id(
                    0,
                    &Anchor {
                        anchor_type: AnchorType::BitcoinOts,
                        target: "super_root".to_string(),
                        anchored_hash: [0x77u8; 32],
                        tree_size: 0,
                        super_tree_size: Some(rotation.data_tree_index + 1),
                        timestamp: 1_000_000,
                        token: vec![],
                        metadata: serde_json::json!({}),
                    },
                    "confirmed",
                )
                .unwrap();
        }

        let signer = CheckpointSigner::from_bytes(&[26u8; 32]);
        let err = generate_receipt(&entry_id, &engine, &signer, opts_with_anchors())
            .await
            .expect_err("an OTS anchor for a foreign Super Root must be refused");

        match err {
            ServerError::ReceiptStateMismatch { source_name, .. } => {
                assert_eq!(source_name, "bitcoin_ots anchor");
            }
            other => panic!("expected ReceiptStateMismatch, got {other:?}"),
        }
    }

    /// A Data Tree whose recorded Super-Tree position holds a different root
    /// must produce a refusal: the verifier's recomputation in Section 5.4.1
    /// step 1 could not succeed.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_refuses_when_super_tree_leaf_is_not_the_state_root() {
        let (engine, _dir) = make_engine([27u8; 32]).await;

        let entry_id = append_one(&engine, 0x01).await;
        close_tree(&engine).await;
        append_one(&engine, 0x02).await;
        close_tree(&engine).await;

        // Point the first closed tree at the second tree's Super-Tree leaf.
        {
            let index_store = engine.index_store();
            let index = index_store.lock().await;
            let conn = index.connection();
            conn.execute(
                "UPDATE trees SET data_tree_index = 9 WHERE data_tree_index = 1",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE trees SET data_tree_index = 1 WHERE data_tree_index = 0",
                [],
            )
            .unwrap();
        }

        let signer = CheckpointSigner::from_bytes(&[27u8; 32]);
        let err = generate_receipt(&entry_id, &engine, &signer, opts_no_anchors())
            .await
            .expect_err("a super_proof that cannot verify must not be issued");

        match err {
            ServerError::ReceiptStateMismatch { source_name, .. } => {
                assert_eq!(source_name, "super-tree leaf");
            }
            other => panic!("expected ReceiptStateMismatch, got {other:?}"),
        }
    }

    /// `genesis_super_root` is the log instance's immutable identity
    /// (Section 3.3.2) and the verifier feeds it into the mandatory
    /// consistency check of Section 5.4.2. It is computed from the Super-Tree
    /// itself, so a foreign value in the SQLite audit copy can never reach a
    /// receipt -- and, because the genesis cannot legitimately change, that
    /// disagreement is refused rather than ignored.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_refuses_when_stored_genesis_super_root_is_foreign() {
        let (engine, _dir) = make_engine([28u8; 32]).await;

        let entry_id = append_one(&engine, 0x01).await;
        close_tree(&engine).await;

        // Well-formed but foreign: 32 bytes, right key, wrong log.
        // `set_super_genesis_root` is INSERT OR IGNORE (write-once), so the
        // row is overwritten directly, the way a corrupted or hand-edited
        // database would present.
        {
            let index_store = engine.index_store();
            let index = index_store.lock().await;
            assert!(
                index.get_super_genesis_root().unwrap().is_some(),
                "rotation must have written the audit copy for this test to mean anything"
            );
            index
                .connection()
                .execute(
                    "UPDATE atl_config SET value = ?1 WHERE key = 'super_genesis_root'",
                    rusqlite::params![hex::encode([0x5Au8; 32])],
                )
                .unwrap();
        }

        let signer = CheckpointSigner::from_bytes(&[28u8; 32]);
        let err = generate_receipt(&entry_id, &engine, &signer, opts_no_anchors())
            .await
            .expect_err("a foreign genesis root must not be served in a receipt");

        match err {
            ServerError::ReceiptStateMismatch {
                source_name,
                found,
                expected,
                ..
            } => {
                assert_eq!(source_name, "super-tree genesis root");
                assert_eq!(found, hex::encode([0x5Au8; 32]));
                assert_ne!(found, expected);
            }
            other => panic!("expected ReceiptStateMismatch, got {other:?}"),
        }
    }

    /// The Super-Tree's own root at size 1 is what the receipt publishes, not
    /// the SQLite copy: a receipt built while the audit copy is missing (the
    /// documented best-effort failure) still carries the correct identity.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_genesis_super_root_comes_from_the_super_tree() {
        let (engine, _dir) = make_engine([29u8; 32]).await;

        let entry_id = append_one(&engine, 0x01).await;
        close_tree(&engine).await;
        append_one(&engine, 0x02).await;
        close_tree(&engine).await;

        let expected_genesis = {
            let super_slab = engine.super_slab().read().await;
            super_slab.get_root(1).unwrap()
        };

        // Drop the audit copy entirely.
        {
            let index_store = engine.index_store();
            let index = index_store.lock().await;
            index
                .connection()
                .execute(
                    "DELETE FROM atl_config WHERE key = 'super_genesis_root'",
                    [],
                )
                .unwrap();
        }

        let signer = CheckpointSigner::from_bytes(&[29u8; 32]);
        let receipt = generate_receipt(&entry_id, &engine, &signer, opts_no_anchors())
            .await
            .expect("a missing audit copy must not block receipt generation");

        let super_proof = receipt.super_proof().expect("super_proof must be present");
        assert_eq!(
            super_proof.genesis_super_root,
            format_hash(&expected_genesis)
        );
    }
}
