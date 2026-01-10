//! Server configuration

/// Server operating mode
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerMode {
    /// Standalone mode - local storage + local dispatcher
    Standalone,
    /// Node mode - no local storage, uses gRPC dispatcher
    Node,
    /// Sequencer mode - master storage + local dispatcher
    Sequencer,
}

impl ServerMode {
    /// Check if this mode should expose /health endpoint
    pub fn has_health_endpoint(&self) -> bool {
        matches!(self, ServerMode::Node | ServerMode::Sequencer)
    }

    /// Check if this mode has local storage
    #[allow(dead_code)]
    pub fn has_local_storage(&self) -> bool {
        matches!(self, ServerMode::Standalone | ServerMode::Sequencer)
    }
}

/// Server configuration
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub database_path: String,
    pub signing_key_path: Option<String>,
    pub log_level: String,
    /// Comma-separated access tokens (None = open mode)
    pub access_tokens: Option<Vec<String>>,
    /// TSA configuration (Tiered Evidence - Tier 1)
    pub tsa: TsaConfig,
    /// Server mode
    pub mode: ServerMode,
    /// Base URL for upgrade_url generation (required)
    pub base_url: String,
}

/// TSA (RFC 3161) configuration for Tier-1 evidence
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TsaConfig {
    /// List of TSA URLs to try in order (fallback on failure)
    pub urls: Vec<String>,
    /// Timeout per TSA request in milliseconds
    pub timeout_ms: u64,
    /// Strict mode: if true, fail request if all TSAs unavailable
    /// If false, return receipt without TSA (client can upgrade later)
    pub strict: bool,
}

impl Default for TsaConfig {
    fn default() -> Self {
        Self {
            urls: vec![],
            timeout_ms: 500,
            strict: true,
        }
    }
}

impl TsaConfig {
    /// Load from environment variables
    #[allow(dead_code)]
    pub fn from_env() -> Self {
        let urls = std::env::var("ATL_TSA_URLS")
            .ok()
            .map(|s| s.split(',').map(|u| u.trim().to_string()).collect())
            .unwrap_or_default();

        let timeout_ms = std::env::var("ATL_TSA_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(500);

        let strict = std::env::var("ATL_STRICT_TSA")
            .ok()
            .map(|s| s == "true" || s == "1")
            .unwrap_or(true);

        Self {
            urls,
            timeout_ms,
            strict,
        }
    }

    /// Check if TSA is configured
    #[allow(dead_code)]
    pub fn is_enabled(&self) -> bool {
        !self.urls.is_empty()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 3000,
            database_path: "./atl.db".to_string(),
            signing_key_path: None,
            log_level: "info".to_string(),
            access_tokens: None,
            tsa: TsaConfig::default(),
            mode: ServerMode::Standalone,
            base_url: "http://localhost:3000".to_string(),
        }
    }
}
