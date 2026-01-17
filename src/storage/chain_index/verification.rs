// File: src/storage/chain_index/verification.rs

//! Chain verification functionality

use rusqlite::params;

use super::store::{ChainIndex, ChainTreeRecord};

/// Result of chain verification
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ChainVerificationResult {
    pub valid: bool,
    pub verified_trees: usize,
    pub first_invalid_tree: Option<i64>,
    pub error_message: Option<String>,
}

impl ChainIndex {
    /// Verify a single chain link between two trees
    ///
    /// Checks that:
    /// 1. tree_next.data_tree_index = tree_prev.data_tree_index + 1 (sequential Super-Tree indices)
    #[allow(dead_code)]
    pub fn verify_chain_link(
        &self,
        tree_prev: &ChainTreeRecord,
        tree_next: &ChainTreeRecord,
    ) -> Result<bool, String> {
        let expected_index = tree_prev.data_tree_index + 1;

        if tree_next.data_tree_index != expected_index {
            return Err(format!(
                "Data tree index mismatch for tree {}: expected {}, got {}",
                tree_next.tree_id, expected_index, tree_next.data_tree_index
            ));
        }

        Ok(true)
    }

    /// Verify the entire chain from first tree to latest
    ///
    /// Returns `ChainVerificationResult` with details.
    #[allow(dead_code)]
    pub fn verify_full_chain(&self) -> rusqlite::Result<ChainVerificationResult> {
        let trees = self.get_all_trees()?;

        if trees.is_empty() {
            return Ok(ChainVerificationResult {
                valid: true,
                verified_trees: 0,
                first_invalid_tree: None,
                error_message: None,
            });
        }

        if trees[0].data_tree_index != 0 {
            return Ok(ChainVerificationResult {
                valid: false,
                verified_trees: 0,
                first_invalid_tree: Some(trees[0].tree_id),
                error_message: Some("First tree should have data_tree_index = 0".to_string()),
            });
        }

        let mut verified = 1;
        for i in 1..trees.len() {
            match self.verify_chain_link(&trees[i - 1], &trees[i]) {
                Ok(true) => verified += 1,
                Ok(false) => {
                    return Ok(ChainVerificationResult {
                        valid: false,
                        verified_trees: verified,
                        first_invalid_tree: Some(trees[i].tree_id),
                        error_message: Some("Chain link verification returned false".to_string()),
                    });
                }
                Err(e) => {
                    return Ok(ChainVerificationResult {
                        valid: false,
                        verified_trees: verified,
                        first_invalid_tree: Some(trees[i].tree_id),
                        error_message: Some(e),
                    });
                }
            }
        }

        Ok(ChainVerificationResult {
            valid: true,
            verified_trees: verified,
            first_invalid_tree: None,
            error_message: None,
        })
    }

    /// Verify chain up to a specific tree
    #[allow(dead_code)]
    pub fn verify_chain_up_to(&self, tree_id: i64) -> rusqlite::Result<ChainVerificationResult> {
        let mut stmt = self.conn.prepare(
            "SELECT tree_id, origin_id, root_hash, tree_size, data_tree_index,
                    status, bitcoin_txid, archive_location, created_at, closed_at, archived_at
             FROM trees WHERE tree_id <= ?1 ORDER BY tree_id ASC",
        )?;
        let trees: Vec<ChainTreeRecord> = stmt
            .query_map(params![tree_id], super::store::row_to_chain_tree)?
            .collect::<Result<_, _>>()?;

        if trees.is_empty() {
            return Ok(ChainVerificationResult {
                valid: false,
                verified_trees: 0,
                first_invalid_tree: Some(tree_id),
                error_message: Some("Tree not found".to_string()),
            });
        }

        if trees[0].data_tree_index != 0 {
            return Ok(ChainVerificationResult {
                valid: false,
                verified_trees: 0,
                first_invalid_tree: Some(trees[0].tree_id),
                error_message: Some("First tree should have data_tree_index = 0".to_string()),
            });
        }

        let mut verified = 1;
        for i in 1..trees.len() {
            match self.verify_chain_link(&trees[i - 1], &trees[i]) {
                Ok(true) => verified += 1,
                Ok(false) | Err(_) => {
                    return Ok(ChainVerificationResult {
                        valid: false,
                        verified_trees: verified,
                        first_invalid_tree: Some(trees[i].tree_id),
                        error_message: Some(format!("Chain broken at tree {}", trees[i].tree_id)),
                    });
                }
            }
        }

        Ok(ChainVerificationResult {
            valid: true,
            verified_trees: verified,
            first_invalid_tree: None,
            error_message: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::store::ChainTreeStatus;
    use rusqlite::Connection;

    fn create_test_chain_index() -> ChainIndex {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE trees (
                tree_id INTEGER PRIMARY KEY,
                origin_id BLOB NOT NULL,
                root_hash BLOB,
                tree_size INTEGER,
                data_tree_index INTEGER,
                status TEXT,
                bitcoin_txid TEXT,
                archive_location TEXT,
                created_at TEXT,
                closed_at TEXT,
                archived_at TEXT
            )",
            [],
        )
        .unwrap();
        ChainIndex { conn }
    }

    fn insert_tree(
        index: &ChainIndex,
        tree_id: i64,
        data_tree_index: u64,
    ) -> rusqlite::Result<()> {
        index.conn.execute(
            "INSERT INTO trees (tree_id, origin_id, root_hash, tree_size, data_tree_index, status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                tree_id,
                &[0u8; 32],
                &[0u8; 32],
                100i64,
                data_tree_index as i64,
                "archived",
                "2024-01-01T00:00:00Z"
            ],
        )?;
        Ok(())
    }

    #[test]
    fn test_verify_chain_link_valid() {
        let _index = create_test_chain_index();
        let tree_prev = ChainTreeRecord {
            tree_id: 1,
            origin_id: [0u8; 32],
            root_hash: [0u8; 32],
            tree_size: 100,
            data_tree_index: 0,
            status: ChainTreeStatus::Archived,
            bitcoin_txid: None,
            archive_location: None,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            closed_at: Some("2024-01-01T00:00:00Z".to_string()),
            archived_at: Some("2024-01-01T00:00:00Z".to_string()),
        };

        let tree_next = ChainTreeRecord {
            tree_id: 2,
            origin_id: [0u8; 32],
            root_hash: [0u8; 32],
            tree_size: 200,
            data_tree_index: 1,
            status: ChainTreeStatus::Archived,
            bitcoin_txid: None,
            archive_location: None,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            closed_at: Some("2024-01-01T00:00:00Z".to_string()),
            archived_at: Some("2024-01-01T00:00:00Z".to_string()),
        };

        let result = ChainIndex::verify_chain_link(&_index, &tree_prev, &tree_next);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_verify_chain_link_invalid_sequence() {
        let _index = create_test_chain_index();
        let tree_prev = ChainTreeRecord {
            tree_id: 1,
            origin_id: [0u8; 32],
            root_hash: [0u8; 32],
            tree_size: 100,
            data_tree_index: 0,
            status: ChainTreeStatus::Archived,
            bitcoin_txid: None,
            archive_location: None,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            closed_at: Some("2024-01-01T00:00:00Z".to_string()),
            archived_at: Some("2024-01-01T00:00:00Z".to_string()),
        };

        let tree_next = ChainTreeRecord {
            tree_id: 2,
            origin_id: [0u8; 32],
            root_hash: [0u8; 32],
            tree_size: 200,
            data_tree_index: 5, // Gap in sequence
            status: ChainTreeStatus::Archived,
            bitcoin_txid: None,
            archive_location: None,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            closed_at: Some("2024-01-01T00:00:00Z".to_string()),
            archived_at: Some("2024-01-01T00:00:00Z".to_string()),
        };

        let result = ChainIndex::verify_chain_link(&_index, &tree_prev, &tree_next);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Data tree index mismatch"));
    }

    #[test]
    fn test_verify_full_chain_empty() {
        let index = create_test_chain_index();
        let result = index.verify_full_chain().unwrap();
        assert!(result.valid);
        assert_eq!(result.verified_trees, 0);
        assert!(result.first_invalid_tree.is_none());
    }

    #[test]
    fn test_verify_full_chain_valid() {
        let index = create_test_chain_index();
        insert_tree(&index, 1, 0).unwrap();
        insert_tree(&index, 2, 1).unwrap();
        insert_tree(&index, 3, 2).unwrap();

        let result = index.verify_full_chain().unwrap();
        assert!(result.valid);
        assert_eq!(result.verified_trees, 3);
        assert!(result.first_invalid_tree.is_none());
    }

    #[test]
    fn test_verify_full_chain_first_not_zero() {
        let index = create_test_chain_index();
        insert_tree(&index, 1, 5).unwrap(); // First tree not at index 0

        let result = index.verify_full_chain().unwrap();
        assert!(!result.valid);
        assert_eq!(result.verified_trees, 0);
        assert_eq!(result.first_invalid_tree, Some(1));
        assert!(result
            .error_message
            .unwrap()
            .contains("First tree should have data_tree_index = 0"));
    }

    #[test]
    fn test_verify_full_chain_broken_link() {
        let index = create_test_chain_index();
        insert_tree(&index, 1, 0).unwrap();
        insert_tree(&index, 2, 1).unwrap();
        insert_tree(&index, 3, 5).unwrap(); // Gap

        let result = index.verify_full_chain().unwrap();
        assert!(!result.valid);
        assert_eq!(result.verified_trees, 2);
        assert_eq!(result.first_invalid_tree, Some(3));
        assert!(result.error_message.is_some());
    }

    #[test]
    fn test_verify_chain_up_to_tree_not_found() {
        let index = create_test_chain_index();
        let result = index.verify_chain_up_to(999).unwrap();
        assert!(!result.valid);
        assert_eq!(result.verified_trees, 0);
        assert_eq!(result.first_invalid_tree, Some(999));
        assert!(result.error_message.unwrap().contains("Tree not found"));
    }

    #[test]
    fn test_verify_chain_up_to_valid() {
        let index = create_test_chain_index();
        insert_tree(&index, 1, 0).unwrap();
        insert_tree(&index, 2, 1).unwrap();
        insert_tree(&index, 3, 2).unwrap();

        let result = index.verify_chain_up_to(2).unwrap();
        assert!(result.valid);
        assert_eq!(result.verified_trees, 2);
        assert!(result.first_invalid_tree.is_none());
    }

    #[test]
    fn test_verify_chain_up_to_broken() {
        let index = create_test_chain_index();
        insert_tree(&index, 1, 0).unwrap();
        insert_tree(&index, 2, 5).unwrap(); // Gap

        let result = index.verify_chain_up_to(2).unwrap();
        assert!(!result.valid);
        assert_eq!(result.verified_trees, 1);
        assert_eq!(result.first_invalid_tree, Some(2));
    }

    #[test]
    fn test_verification_result_clone() {
        let result = ChainVerificationResult {
            valid: true,
            verified_trees: 5,
            first_invalid_tree: None,
            error_message: None,
        };
        let cloned = result.clone();
        assert_eq!(cloned.valid, result.valid);
        assert_eq!(cloned.verified_trees, result.verified_trees);
    }

    #[test]
    fn test_verification_result_debug() {
        let result = ChainVerificationResult {
            valid: false,
            verified_trees: 2,
            first_invalid_tree: Some(3),
            error_message: Some("test error".to_string()),
        };
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("ChainVerificationResult"));
        assert!(debug_str.contains("valid"));
        assert!(debug_str.contains("false"));
    }
}
