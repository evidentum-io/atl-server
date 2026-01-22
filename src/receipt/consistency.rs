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
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    use crate::error::{ServerError, StorageError};
    use crate::traits::{
        anchor::Anchor, AppendParams, BatchResult, ConsistencyProof, Entry, InclusionProof,
        Storage, TreeHead,
    };

    // ========== Mock Storage Implementation ==========

    #[derive(Clone)]
    struct MockStorage {
        latest_anchored_size: Arc<Mutex<Option<u64>>>,
        consistency_proofs: Arc<Mutex<Vec<ConsistencyProof>>>,
        should_error: Arc<Mutex<bool>>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                latest_anchored_size: Arc::new(Mutex::new(None)),
                consistency_proofs: Arc::new(Mutex::new(Vec::new())),
                should_error: Arc::new(Mutex::new(false)),
            }
        }

        fn set_latest_anchored_size(&self, size: Option<u64>) {
            *self.latest_anchored_size.lock().unwrap() = size;
        }

        fn add_consistency_proof(&self, from: u64, to: u64, path: Vec<[u8; 32]>) {
            self.consistency_proofs
                .lock()
                .unwrap()
                .push(ConsistencyProof {
                    from_size: from,
                    to_size: to,
                    path,
                });
        }

        fn set_error(&self, should_error: bool) {
            *self.should_error.lock().unwrap() = should_error;
        }
    }

    #[async_trait]
    impl Storage for MockStorage {
        async fn append_batch(
            &self,
            _params: Vec<AppendParams>,
        ) -> Result<BatchResult, StorageError> {
            unimplemented!("not needed for tests")
        }

        async fn flush(&self) -> Result<(), StorageError> {
            unimplemented!("not needed for tests")
        }

        fn tree_head(&self) -> TreeHead {
            TreeHead {
                tree_size: 1000,
                root_hash: [0u8; 32],
                origin: [0u8; 32],
            }
        }

        fn origin_id(&self) -> [u8; 32] {
            [0u8; 32]
        }

        fn is_healthy(&self) -> bool {
            true
        }

        fn get_entry(&self, _id: &Uuid) -> crate::error::ServerResult<Entry> {
            unimplemented!("not needed for tests")
        }

        fn get_inclusion_proof(
            &self,
            _entry_id: &Uuid,
            _tree_size: Option<u64>,
        ) -> crate::error::ServerResult<InclusionProof> {
            unimplemented!("not needed for tests")
        }

        fn get_inclusion_proof_by_leaf_index(
            &self,
            _leaf_index: u64,
            _tree_size: Option<u64>,
        ) -> crate::error::ServerResult<InclusionProof> {
            unimplemented!("not needed for tests")
        }

        fn get_consistency_proof(
            &self,
            from_size: u64,
            to_size: u64,
        ) -> crate::error::ServerResult<ConsistencyProof> {
            if *self.should_error.lock().unwrap() {
                return Err(ServerError::Storage(StorageError::Database(
                    "test error".to_string(),
                )));
            }

            let proofs = self.consistency_proofs.lock().unwrap();
            for proof in proofs.iter() {
                if proof.from_size == from_size && proof.to_size == to_size {
                    return Ok(proof.clone());
                }
            }

            Ok(ConsistencyProof {
                from_size,
                to_size,
                path: vec![[1u8; 32], [2u8; 32]],
            })
        }

        fn get_anchors(
            &self,
            _tree_size: u64,
        ) -> crate::error::ServerResult<Vec<crate::traits::anchor::Anchor>> {
            unimplemented!("not needed for tests")
        }

        fn get_latest_anchored_size(&self) -> crate::error::ServerResult<Option<u64>> {
            if *self.should_error.lock().unwrap() {
                return Err(ServerError::Storage(StorageError::Database(
                    "test error".to_string(),
                )));
            }
            Ok(*self.latest_anchored_size.lock().unwrap())
        }

        fn get_anchors_covering(
            &self,
            _target_tree_size: u64,
            _limit: usize,
        ) -> crate::error::ServerResult<Vec<Anchor>> {
            unimplemented!("not needed for tests")
        }

        fn get_root_at_size(&self, _tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
            unimplemented!("not needed for tests")
        }

        fn get_super_root(&self, _super_tree_size: u64) -> crate::error::ServerResult<[u8; 32]> {
            unimplemented!("not needed for tests")
        }

        fn is_initialized(&self) -> bool {
            true
        }
    }

    // ========== Tests ==========

    #[test]
    fn test_explicit_consistency_from_with_valid_range() {
        let storage = MockStorage::new();
        storage.add_consistency_proof(100, 500, vec![[3u8; 32], [4u8; 32]]);

        let options = ReceiptOptions {
            consistency_from: Some(100),
            auto_consistency_from_anchor: false,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());

        let proof = result.unwrap();
        assert!(proof.is_some());

        let proof = proof.unwrap();
        assert_eq!(proof.from_tree_size, 100);
        assert_eq!(proof.path.len(), 2);
    }

    #[test]
    fn test_explicit_consistency_from_zero_returns_none() {
        let storage = MockStorage::new();

        let options = ReceiptOptions {
            consistency_from: Some(0),
            auto_consistency_from_anchor: false,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_explicit_consistency_from_equal_to_current_returns_none() {
        let storage = MockStorage::new();

        let options = ReceiptOptions {
            consistency_from: Some(500),
            auto_consistency_from_anchor: false,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_explicit_consistency_from_greater_than_current_returns_none() {
        let storage = MockStorage::new();

        let options = ReceiptOptions {
            consistency_from: Some(600),
            auto_consistency_from_anchor: false,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_auto_consistency_with_anchored_size() {
        let storage = MockStorage::new();
        storage.set_latest_anchored_size(Some(300));
        storage.add_consistency_proof(300, 500, vec![[5u8; 32]]);

        let options = ReceiptOptions {
            consistency_from: None,
            auto_consistency_from_anchor: true,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());

        let proof = result.unwrap();
        assert!(proof.is_some());

        let proof = proof.unwrap();
        assert_eq!(proof.from_tree_size, 300);
        assert_eq!(proof.path.len(), 1);
    }

    #[test]
    fn test_auto_consistency_no_anchored_size_returns_none() {
        let storage = MockStorage::new();
        storage.set_latest_anchored_size(None);

        let options = ReceiptOptions {
            consistency_from: None,
            auto_consistency_from_anchor: true,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_auto_consistency_anchored_size_equal_to_current_returns_none() {
        let storage = MockStorage::new();
        storage.set_latest_anchored_size(Some(500));

        let options = ReceiptOptions {
            consistency_from: None,
            auto_consistency_from_anchor: true,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_auto_consistency_anchored_size_greater_than_current_returns_none() {
        let storage = MockStorage::new();
        storage.set_latest_anchored_size(Some(600));

        let options = ReceiptOptions {
            consistency_from: None,
            auto_consistency_from_anchor: true,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_no_consistency_proof_when_auto_disabled() {
        let storage = MockStorage::new();
        storage.set_latest_anchored_size(Some(300));

        let options = ReceiptOptions {
            consistency_from: None,
            auto_consistency_from_anchor: false, // Explicitly disabled
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_explicit_consistency_takes_precedence_over_auto() {
        let storage = MockStorage::new();
        storage.set_latest_anchored_size(Some(200)); // Auto would use this
        storage.add_consistency_proof(150, 500, vec![[6u8; 32]]);

        let options = ReceiptOptions {
            consistency_from: Some(150), // Explicit takes precedence
            auto_consistency_from_anchor: true,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());

        let proof = result.unwrap();
        assert!(proof.is_some());

        let proof = proof.unwrap();
        assert_eq!(proof.from_tree_size, 150); // Uses explicit, not auto (200)
    }

    #[test]
    fn test_storage_error_on_consistency_proof_fetch() {
        let storage = MockStorage::new();
        storage.set_error(true);

        let options = ReceiptOptions {
            consistency_from: Some(100),
            auto_consistency_from_anchor: false,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_err());
    }

    #[test]
    fn test_storage_error_on_latest_anchored_size_fetch() {
        let storage = MockStorage::new();
        storage.set_error(true);

        let options = ReceiptOptions {
            consistency_from: None,
            auto_consistency_from_anchor: true,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_err());
    }

    #[test]
    fn test_format_hash_in_proof() {
        let storage = MockStorage::new();
        storage.add_consistency_proof(
            100,
            500,
            vec![
                [0xAA; 32], // Distinctive pattern
                [0xBB; 32],
            ],
        );

        let options = ReceiptOptions {
            consistency_from: Some(100),
            auto_consistency_from_anchor: false,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());

        let proof = result.unwrap().unwrap();
        assert_eq!(proof.path.len(), 2);
        // Verify hashes are formatted with "sha256:" prefix
        assert!(proof.path[0].starts_with("sha256:"));
        assert!(proof.path[1].starts_with("sha256:"));
        // Verify hex content
        assert!(proof.path[0].contains("aaaaaaaaaaaaaaaa"));
        assert!(proof.path[1].contains("bbbbbbbbbbbbbbbb"));
    }

    #[test]
    fn test_consistency_proof_with_empty_path() {
        let storage = MockStorage::new();
        storage.add_consistency_proof(100, 500, vec![]); // Empty path

        let options = ReceiptOptions {
            consistency_from: Some(100),
            auto_consistency_from_anchor: false,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());

        let proof = result.unwrap().unwrap();
        assert_eq!(proof.path.len(), 0);
    }

    #[test]
    fn test_consistency_proof_with_large_path() {
        let storage = MockStorage::new();
        let large_path: Vec<[u8; 32]> = (0..100).map(|i| [i as u8; 32]).collect();
        storage.add_consistency_proof(100, 500, large_path);

        let options = ReceiptOptions {
            consistency_from: Some(100),
            auto_consistency_from_anchor: false,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());

        let proof = result.unwrap().unwrap();
        assert_eq!(proof.path.len(), 100);
    }

    #[test]
    fn test_consistency_from_one() {
        let storage = MockStorage::new();
        storage.add_consistency_proof(1, 500, vec![[7u8; 32]]);

        let options = ReceiptOptions {
            consistency_from: Some(1),
            auto_consistency_from_anchor: false,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 500, &options);
        assert!(result.is_ok());

        let proof = result.unwrap();
        assert!(proof.is_some());
        assert_eq!(proof.unwrap().from_tree_size, 1);
    }

    #[test]
    fn test_current_tree_size_one() {
        let storage = MockStorage::new();

        let options = ReceiptOptions {
            consistency_from: Some(0),
            auto_consistency_from_anchor: false,
            ..Default::default()
        };

        let result = determine_consistency_proof(&storage, 1, &options);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
