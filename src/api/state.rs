//! Application state shared across HTTP handlers

use std::sync::Arc;

use crate::config::ServerMode;
use crate::receipt::CheckpointSigner;
use crate::storage::engine::StorageEngine;
use crate::traits::{SequencerClient, Storage};

/// Application state shared across handlers
///
/// Role-aware: In NODE mode, storage/signer are None; dispatcher handles everything.
#[derive(Clone)]
pub struct AppState {
    /// Server mode (STANDALONE, NODE, SEQUENCER)
    pub mode: ServerMode,

    /// Dispatcher for all entry operations (role-agnostic)
    pub dispatcher: Arc<dyn SequencerClient>,

    /// Direct storage access (None in NODE mode)
    #[allow(dead_code)]
    pub storage: Option<Arc<dyn Storage>>,

    /// StorageEngine for receipt generation (None in NODE mode)
    #[allow(dead_code)]
    pub storage_engine: Option<Arc<StorageEngine>>,

    /// Checkpoint signer for receipt generation (None in NODE mode)
    #[allow(dead_code)]
    pub signer: Option<Arc<CheckpointSigner>>,

    /// Bearer tokens for authentication (None = open mode)
    /// Source: ATL_ACCESS_TOKENS env var (comma-separated)
    #[allow(dead_code)]
    pub access_tokens: Option<Vec<String>>,

    /// Base URL for upgrade_url generation
    /// Source: ATL_BASE_URL env var (required)
    pub base_url: String,
}
