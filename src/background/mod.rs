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
