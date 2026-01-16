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
