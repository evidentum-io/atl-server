// File: src/background/mod.rs

//! Background job management for tree lifecycle and external anchoring
//!
//! This module implements proactive background jobs that:
//! - Close trees after their lifetime expires
//! - Anchor closed trees with TSA (RFC 3161) for Tier-1 evidence
//! - Poll OpenTimestamps for Bitcoin confirmation (Tier-2 evidence)
//!
//! All jobs run continuously and discover pending work by querying the database.
//! Jobs never block the HTTP request path - anchoring is fully asynchronous.

#![allow(dead_code)]

pub mod config;
pub mod ots_job;
pub mod tree_closer;
pub mod tsa_job;

use crate::error::ServerResult;
use crate::storage::chain_index::ChainIndex;
use crate::storage::engine::StorageEngine;
use crate::storage::index::IndexStore;
use crate::traits::{Storage, TreeRotator};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

pub use config::BackgroundConfig;
#[cfg(feature = "ots")]
pub use ots_job::OtsJob;
pub use tree_closer::TreeCloser;
pub use tsa_job::TsaAnchoringJob;

/// Background job runner
///
/// Manages all proactive anchoring jobs. Jobs run continuously until shutdown.
pub struct BackgroundJobRunner {
    /// Index store for tree lifecycle and anchoring
    index: Arc<Mutex<IndexStore>>,
    /// Storage engine (concrete type, implements both Storage and TreeRotator)
    storage: Arc<StorageEngine>,
    /// Chain Index for tree metadata
    chain_index: Arc<Mutex<ChainIndex>>,
    #[cfg(feature = "ots")]
    ots_client: Arc<dyn crate::anchoring::ots::OtsClient>,
    config: BackgroundConfig,
    shutdown_tx: broadcast::Sender<()>,
}

impl BackgroundJobRunner {
    pub fn new(
        storage: Arc<StorageEngine>,
        chain_index: Arc<Mutex<ChainIndex>>,
        config: BackgroundConfig,
    ) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);

        let index = storage.index_store();

        #[cfg(feature = "ots")]
        let ots_client: Arc<dyn crate::anchoring::ots::OtsClient> = {
            let ots_config = crate::anchoring::ots::OtsConfig::from_env();
            tracing::info!(
                calendar_urls = ?ots_config.calendar_urls,
                timeout_secs = ots_config.timeout_secs,
                min_confirmations = ots_config.min_confirmations,
                "OTS client configuration loaded"
            );
            Arc::new(
                crate::anchoring::ots::AsyncOtsClient::with_config(ots_config)
                    .expect("failed to create OTS client"),
            )
        };

        Self {
            index,
            storage,
            chain_index,
            #[cfg(feature = "ots")]
            ots_client,
            config,
            shutdown_tx,
        }
    }

    /// Start all background jobs
    ///
    /// All jobs run proactively and continuously until shutdown.
    /// Jobs discover pending work by querying the database on each tick.
    pub async fn start(&self) -> ServerResult<Vec<tokio::task::JoinHandle<()>>> {
        if self.config.disabled {
            tracing::info!("Background jobs disabled via ATL_BACKGROUND_DISABLED=true");
            return Ok(vec![]);
        }

        let mut handles = Vec::new();

        // 1. Tree Closer (rotates trees with genesis leaf insertion)
        {
            // StorageEngine implements both Storage and TreeRotator traits
            // We pass it as both Storage and TreeRotator to enable proper tree rotation
            let storage_arc = Arc::clone(&self.storage);
            let closer = TreeCloser::new(
                Arc::clone(&self.index),
                storage_arc.clone() as Arc<dyn Storage>,
                storage_arc.clone() as Arc<dyn TreeRotator>,
                Arc::clone(&self.chain_index),
                self.config.tree_closer.clone(),
            );
            let shutdown_rx = self.shutdown_tx.subscribe();
            handles.push(tokio::spawn(async move {
                closer.run(shutdown_rx).await;
            }));
            tracing::info!(
                interval_secs = self.config.tree_closer.interval_secs,
                lifetime_secs = self.config.tree_closer.tree_lifetime_secs,
                "Tree Closer job started"
            );
        }

        // 2. TSA Job (if TSA URLs configured)
        if !self.config.tsa_job.tsa_urls.is_empty() {
            let job = TsaAnchoringJob::new(
                Arc::clone(&self.index),
                Arc::clone(&self.storage) as Arc<dyn Storage>,
                self.config.tsa_job.clone(),
            );
            let shutdown_rx = self.shutdown_tx.subscribe();
            handles.push(tokio::spawn(async move {
                job.run(shutdown_rx).await;
            }));
            tracing::info!(
                interval_secs = self.config.tsa_job.interval_secs,
                servers = self.config.tsa_job.tsa_urls.len(),
                "TSA Job started"
            );
        } else {
            tracing::warn!("TSA Job disabled: no ATL_TSA_URLS configured");
        }

        // 3. OTS Job (submit + poll)
        #[cfg(feature = "ots")]
        {
            let ots_job = OtsJob::new(
                Arc::clone(&self.index),
                Arc::clone(&self.storage) as Arc<dyn Storage>,
                Arc::clone(&self.ots_client),
                self.config.ots_job.clone(),
            );
            let shutdown_rx = self.shutdown_tx.subscribe();
            handles.push(tokio::spawn(async move {
                ots_job.run(shutdown_rx).await;
            }));
            tracing::info!(
                interval_secs = self.config.ots_job.interval_secs,
                "OTS Job started (submit + poll)"
            );
        }

        tracing::info!(
            job_count = handles.len(),
            "Background jobs started (proactive anchoring enabled)"
        );

        Ok(handles)
    }

    /// Signal all jobs to shutdown gracefully
    pub fn shutdown(&self) {
        tracing::info!("Signaling background jobs to shutdown");
        let _ = self.shutdown_tx.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::background::ots_job::OtsJobConfig;
    use crate::background::tree_closer::TreeCloserConfig;
    use crate::background::tsa_job::TsaJobConfig;
    use crate::storage::config::StorageConfig;
    use tempfile::TempDir;

    /// Create a test storage engine with minimal config
    async fn create_test_storage(temp_dir: &TempDir) -> Arc<StorageEngine> {
        let data_dir = temp_dir.path().to_path_buf();
        let origin = [1u8; 32];

        let config = StorageConfig {
            data_dir,
            wal_dir: None,
            slab_dir: None,
            db_path: None,
            slab_capacity: 1024,
            max_open_slabs: 4,
            wal_keep_count: 2,
            fsync_enabled: false, // Disable fsync for tests
        };

        Arc::new(
            StorageEngine::new(config, origin)
                .await
                .expect("Failed to create test storage"),
        )
    }

    /// Create a test chain index
    async fn create_test_chain_index(temp_dir: &TempDir) -> Arc<Mutex<ChainIndex>> {
        let chain_db_path = temp_dir.path().join("chain_index.db");
        Arc::new(Mutex::new(
            ChainIndex::open(&chain_db_path).expect("Failed to create chain index"),
        ))
    }

    #[tokio::test]
    async fn test_new_constructs_runner_with_default_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig::default();

        let runner = BackgroundJobRunner::new(storage, chain_index, config);

        assert!(
            !runner.config.disabled,
            "Config should not be disabled by default"
        );
    }

    #[tokio::test]
    async fn test_new_stores_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig {
            disabled: true,
            ..Default::default()
        };

        let runner = BackgroundJobRunner::new(storage, chain_index, config);

        assert!(
            runner.config.disabled,
            "Config.disabled should be preserved"
        );
    }

    #[tokio::test]
    async fn test_new_creates_shutdown_channel() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig::default();

        let runner = BackgroundJobRunner::new(storage, chain_index, config);

        // Verify shutdown channel works by subscribing
        let mut rx = runner.shutdown_tx.subscribe();
        runner.shutdown_tx.send(()).unwrap();

        assert!(
            rx.try_recv().is_ok(),
            "Shutdown channel should receive signal"
        );
    }

    #[tokio::test]
    async fn test_new_extracts_index_store_from_storage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig::default();

        let _runner = BackgroundJobRunner::new(storage.clone(), chain_index, config);

        // Test passes if constructor succeeds (index is private, but construction validates it)
    }

    #[tokio::test]
    async fn test_start_returns_empty_when_disabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig {
            disabled: true,
            ..Default::default()
        };

        let runner = BackgroundJobRunner::new(storage, chain_index, config);
        let handles = runner.start().await.unwrap();

        assert_eq!(
            handles.len(),
            0,
            "Should return empty handles when disabled"
        );
    }

    #[tokio::test]
    async fn test_start_spawns_tree_closer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig {
            disabled: false,
            tsa_job: TsaJobConfig {
                tsa_urls: vec![], // Empty TSA URLs to skip TSA job
                ..Default::default()
            },
            ..Default::default()
        };

        let runner = BackgroundJobRunner::new(storage, chain_index, config);
        let handles = runner.start().await.unwrap();

        // With OTS feature: TreeCloser + OtsJob = 2
        // Without OTS feature: TreeCloser only = 1
        #[cfg(feature = "ots")]
        assert_eq!(
            handles.len(),
            2,
            "Should spawn TreeCloser + OtsJob when TSA disabled"
        );

        #[cfg(not(feature = "ots"))]
        assert_eq!(
            handles.len(),
            1,
            "Should spawn only TreeCloser when TSA disabled and OTS feature off"
        );

        // Cleanup: shutdown to terminate tasks
        runner.shutdown();
        for handle in handles {
            let _ = tokio::time::timeout(std::time::Duration::from_millis(100), handle).await;
        }
    }

    #[tokio::test]
    async fn test_start_spawns_tree_closer_and_tsa_job() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig {
            disabled: false,
            tsa_job: TsaJobConfig {
                tsa_urls: vec!["http://test-tsa.example.com".to_string()],
                ..Default::default()
            },
            ..Default::default()
        };

        let runner = BackgroundJobRunner::new(storage, chain_index, config);
        let handles = runner.start().await.unwrap();

        // With OTS: TreeCloser + TSA + OtsJob = 3
        // Without OTS: TreeCloser + TSA = 2
        #[cfg(feature = "ots")]
        assert_eq!(handles.len(), 3, "Should spawn TreeCloser + TSA + OtsJob");

        #[cfg(not(feature = "ots"))]
        assert_eq!(
            handles.len(),
            2,
            "Should spawn TreeCloser + TSA when OTS feature off"
        );

        // Cleanup
        runner.shutdown();
        for handle in handles {
            let _ = tokio::time::timeout(std::time::Duration::from_millis(100), handle).await;
        }
    }

    #[tokio::test]
    async fn test_start_skips_tsa_job_when_no_urls() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig {
            disabled: false,
            tsa_job: TsaJobConfig {
                tsa_urls: vec![], // Empty TSA URLs
                ..Default::default()
            },
            ..Default::default()
        };

        let runner = BackgroundJobRunner::new(storage, chain_index, config);
        let handles = runner.start().await.unwrap();

        // TSA job should be skipped
        #[cfg(feature = "ots")]
        assert_eq!(handles.len(), 2, "Should spawn TreeCloser + OtsJob only");

        #[cfg(not(feature = "ots"))]
        assert_eq!(handles.len(), 1, "Should spawn TreeCloser only");

        runner.shutdown();
        for handle in handles {
            let _ = tokio::time::timeout(std::time::Duration::from_millis(100), handle).await;
        }
    }

    #[tokio::test]
    #[cfg(feature = "ots")]
    async fn test_start_spawns_ots_job_when_feature_enabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig {
            disabled: false,
            tsa_job: TsaJobConfig {
                tsa_urls: vec![],
                ..Default::default()
            },
            ..Default::default()
        };

        let runner = BackgroundJobRunner::new(storage, chain_index, config);
        let handles = runner.start().await.unwrap();

        // Should include OtsJob
        assert!(
            handles.len() >= 2,
            "Should spawn at least TreeCloser + OtsJob"
        );

        runner.shutdown();
        for handle in handles {
            let _ = tokio::time::timeout(std::time::Duration::from_millis(100), handle).await;
        }
    }

    #[tokio::test]
    async fn test_start_can_be_called_multiple_times() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig {
            disabled: false,
            tsa_job: TsaJobConfig {
                tsa_urls: vec![],
                ..Default::default()
            },
            ..Default::default()
        };

        let runner = BackgroundJobRunner::new(storage, chain_index, config);

        // First start
        let handles1 = runner.start().await.unwrap();
        assert!(!handles1.is_empty(), "First start should spawn jobs");

        // Second start (should spawn new tasks)
        let handles2 = runner.start().await.unwrap();
        assert!(!handles2.is_empty(), "Second start should spawn jobs");

        // Cleanup
        runner.shutdown();
        for handle in handles1.into_iter().chain(handles2) {
            let _ = tokio::time::timeout(std::time::Duration::from_millis(100), handle).await;
        }
    }

    #[tokio::test]
    async fn test_shutdown_sends_signal() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig::default();

        let runner = BackgroundJobRunner::new(storage, chain_index, config);
        let mut rx = runner.shutdown_tx.subscribe();

        runner.shutdown();

        assert!(
            rx.try_recv().is_ok(),
            "Shutdown should send signal to channel"
        );
    }

    #[tokio::test]
    async fn test_shutdown_can_be_called_multiple_times() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig::default();

        let runner = BackgroundJobRunner::new(storage, chain_index, config);

        // Multiple shutdown calls should not panic
        runner.shutdown();
        runner.shutdown();
        runner.shutdown();

        // Test passes if no panic occurs
    }

    #[tokio::test]
    async fn test_shutdown_signal_received_by_multiple_subscribers() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig::default();

        let runner = BackgroundJobRunner::new(storage, chain_index, config);

        // Create multiple subscribers (simulating multiple jobs)
        let mut rx1 = runner.shutdown_tx.subscribe();
        let mut rx2 = runner.shutdown_tx.subscribe();
        let mut rx3 = runner.shutdown_tx.subscribe();

        runner.shutdown();

        assert!(rx1.try_recv().is_ok(), "Subscriber 1 should receive signal");
        assert!(rx2.try_recv().is_ok(), "Subscriber 2 should receive signal");
        assert!(rx3.try_recv().is_ok(), "Subscriber 3 should receive signal");
    }

    #[tokio::test]
    async fn test_background_config_is_cloneable() {
        let config = BackgroundConfig {
            disabled: true,
            tree_closer: TreeCloserConfig::default(),
            tsa_job: TsaJobConfig::default(),
            ots_job: OtsJobConfig::default(),
        };

        let cloned = config.clone();

        assert_eq!(config.disabled, cloned.disabled);
    }

    #[tokio::test]
    async fn test_runner_with_custom_tree_closer_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;

        let custom_config = BackgroundConfig {
            disabled: false,
            tree_closer: TreeCloserConfig {
                interval_secs: 99,
                tree_lifetime_secs: 999,
            },
            tsa_job: TsaJobConfig {
                tsa_urls: vec![],
                ..Default::default()
            },
            ots_job: OtsJobConfig::default(),
        };

        let runner = BackgroundJobRunner::new(storage, chain_index, custom_config);

        assert_eq!(
            runner.config.tree_closer.interval_secs, 99,
            "Custom tree_closer config should be preserved"
        );
        assert_eq!(
            runner.config.tree_closer.tree_lifetime_secs, 999,
            "Custom tree_closer lifetime should be preserved"
        );
    }

    #[tokio::test]
    async fn test_runner_with_custom_tsa_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;

        let custom_config = BackgroundConfig {
            disabled: false,
            tree_closer: TreeCloserConfig::default(),
            tsa_job: TsaJobConfig {
                tsa_urls: vec!["http://custom-tsa.test".to_string()],
                interval_secs: 88,
                timeout_ms: 8888,
                max_batch_size: 100,
                active_tree_interval_secs: 60,
            },
            ots_job: OtsJobConfig::default(),
        };

        let runner = BackgroundJobRunner::new(storage, chain_index, custom_config);

        assert_eq!(
            runner.config.tsa_job.tsa_urls.len(),
            1,
            "Custom TSA URLs should be preserved"
        );
        assert_eq!(
            runner.config.tsa_job.interval_secs, 88,
            "Custom TSA interval should be preserved"
        );
    }

    #[tokio::test]
    async fn test_runner_with_custom_ots_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;

        let custom_config = BackgroundConfig {
            disabled: false,
            tree_closer: TreeCloserConfig::default(),
            tsa_job: TsaJobConfig {
                tsa_urls: vec![],
                ..Default::default()
            },
            ots_job: OtsJobConfig {
                interval_secs: 77,
                max_batch_size: 100,
            },
        };

        let runner = BackgroundJobRunner::new(storage, chain_index, custom_config);

        assert_eq!(
            runner.config.ots_job.interval_secs, 77,
            "Custom OTS interval should be preserved"
        );
    }

    #[tokio::test]
    async fn test_start_returns_ok_result() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig {
            disabled: true,
            ..Default::default()
        };

        let runner = BackgroundJobRunner::new(storage, chain_index, config);
        let result = runner.start().await;

        assert!(result.is_ok(), "Start should return Ok result");
    }

    #[tokio::test]
    async fn test_spawned_tasks_are_running() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let chain_index = create_test_chain_index(&temp_dir).await;
        let config = BackgroundConfig {
            disabled: false,
            tsa_job: TsaJobConfig {
                tsa_urls: vec![],
                ..Default::default()
            },
            ..Default::default()
        };

        let runner = BackgroundJobRunner::new(storage, chain_index, config);
        let handles = runner.start().await.unwrap();

        // Check that tasks are still running (not finished immediately)
        for handle in &handles {
            assert!(!handle.is_finished(), "Task should still be running");
        }

        // Cleanup
        runner.shutdown();
        for handle in handles {
            let _ = tokio::time::timeout(std::time::Duration::from_millis(100), handle).await;
        }
    }

    #[tokio::test]
    async fn test_storage_engine_implements_both_traits() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = create_test_storage(&temp_dir).await;

        // Verify storage implements Storage trait (compilation check)
        let _storage_ref: &dyn Storage = &*storage;

        // Verify storage implements TreeRotator trait (compilation check)
        let _rotator_ref: &dyn TreeRotator = &*storage;

        // Test passes if code compiles (trait bounds satisfied)
    }

    #[tokio::test]
    async fn test_new_with_different_origins() {
        let temp_dir = tempfile::tempdir().unwrap();
        let origin1 = [1u8; 32];
        let origin2 = [2u8; 32];

        let config1 = StorageConfig {
            data_dir: temp_dir.path().join("storage1"),
            wal_dir: None,
            slab_dir: None,
            db_path: None,
            slab_capacity: 1024,
            max_open_slabs: 4,
            wal_keep_count: 2,
            fsync_enabled: false,
        };

        let config2 = StorageConfig {
            data_dir: temp_dir.path().join("storage2"),
            wal_dir: None,
            slab_dir: None,
            db_path: None,
            slab_capacity: 1024,
            max_open_slabs: 4,
            wal_keep_count: 2,
            fsync_enabled: false,
        };

        let storage1 = Arc::new(StorageEngine::new(config1, origin1).await.unwrap());
        let storage2 = Arc::new(StorageEngine::new(config2, origin2).await.unwrap());

        let chain_index = create_test_chain_index(&temp_dir).await;

        // Both should construct successfully (test passes if no panic)
        let _runner1 =
            BackgroundJobRunner::new(storage1, chain_index.clone(), BackgroundConfig::default());
        let _runner2 = BackgroundJobRunner::new(storage2, chain_index, BackgroundConfig::default());
    }
}
