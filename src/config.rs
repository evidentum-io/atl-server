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

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    // ========== ServerMode Tests ==========

    #[test]
    fn test_server_mode_has_health_endpoint() {
        assert!(!ServerMode::Standalone.has_health_endpoint());
        assert!(ServerMode::Node.has_health_endpoint());
        assert!(ServerMode::Sequencer.has_health_endpoint());
    }

    #[test]
    fn test_server_mode_has_local_storage() {
        assert!(ServerMode::Standalone.has_local_storage());
        assert!(!ServerMode::Node.has_local_storage());
        assert!(ServerMode::Sequencer.has_local_storage());
    }

    #[test]
    fn test_server_mode_equality() {
        assert_eq!(ServerMode::Standalone, ServerMode::Standalone);
        assert_eq!(ServerMode::Node, ServerMode::Node);
        assert_eq!(ServerMode::Sequencer, ServerMode::Sequencer);

        assert_ne!(ServerMode::Standalone, ServerMode::Node);
        assert_ne!(ServerMode::Node, ServerMode::Sequencer);
    }

    #[test]
    fn test_server_mode_debug() {
        let debug_str = format!("{:?}", ServerMode::Standalone);
        assert!(debug_str.contains("Standalone"));
    }

    #[test]
    fn test_server_mode_clone() {
        let mode = ServerMode::Sequencer;
        let cloned = mode;
        assert_eq!(mode, cloned);
    }

    // ========== TsaConfig Tests ==========

    #[test]
    fn test_tsa_config_default() {
        let config = TsaConfig::default();
        assert!(config.urls.is_empty());
        assert_eq!(config.timeout_ms, 500);
        assert!(config.strict);
    }

    #[test]
    fn test_tsa_config_is_enabled_empty() {
        let config = TsaConfig {
            urls: vec![],
            timeout_ms: 500,
            strict: true,
        };
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_tsa_config_is_enabled_with_urls() {
        let config = TsaConfig {
            urls: vec!["https://example.com/tsa".to_string()],
            timeout_ms: 500,
            strict: true,
        };
        assert!(config.is_enabled());
    }

    #[test]
    #[serial]
    fn test_tsa_config_from_env_empty() {
        std::env::remove_var("ATL_TSA_URLS");
        std::env::remove_var("ATL_TSA_TIMEOUT_MS");
        std::env::remove_var("ATL_STRICT_TSA");

        let config = TsaConfig::from_env();
        assert!(config.urls.is_empty());
        assert_eq!(config.timeout_ms, 500);
        assert!(config.strict);
    }

    #[test]
    #[serial]
    fn test_tsa_config_from_env_single_url() {
        std::env::set_var("ATL_TSA_URLS", "https://tsa1.example.com");
        std::env::remove_var("ATL_TSA_TIMEOUT_MS");
        std::env::remove_var("ATL_STRICT_TSA");

        let config = TsaConfig::from_env();
        assert_eq!(config.urls.len(), 1);
        assert_eq!(config.urls[0], "https://tsa1.example.com");
        assert_eq!(config.timeout_ms, 500);
        assert!(config.strict);

        std::env::remove_var("ATL_TSA_URLS");
    }

    #[test]
    #[serial]
    fn test_tsa_config_from_env_multiple_urls() {
        std::env::set_var(
            "ATL_TSA_URLS",
            "https://tsa1.example.com, https://tsa2.example.com,https://tsa3.example.com",
        );

        let config = TsaConfig::from_env();
        assert_eq!(config.urls.len(), 3);
        assert_eq!(config.urls[0], "https://tsa1.example.com");
        assert_eq!(config.urls[1], "https://tsa2.example.com");
        assert_eq!(config.urls[2], "https://tsa3.example.com");

        std::env::remove_var("ATL_TSA_URLS");
    }

    #[test]
    #[serial]
    fn test_tsa_config_from_env_timeout() {
        std::env::set_var("ATL_TSA_TIMEOUT_MS", "1500");

        let config = TsaConfig::from_env();
        assert_eq!(config.timeout_ms, 1500);

        std::env::remove_var("ATL_TSA_TIMEOUT_MS");
    }

    #[test]
    #[serial]
    fn test_tsa_config_from_env_timeout_invalid() {
        std::env::set_var("ATL_TSA_TIMEOUT_MS", "invalid");

        let config = TsaConfig::from_env();
        assert_eq!(config.timeout_ms, 500); // Falls back to default

        std::env::remove_var("ATL_TSA_TIMEOUT_MS");
    }

    #[test]
    #[serial]
    fn test_tsa_config_from_env_strict_true() {
        std::env::set_var("ATL_STRICT_TSA", "true");

        let config = TsaConfig::from_env();
        assert!(config.strict);

        std::env::remove_var("ATL_STRICT_TSA");
    }

    #[test]
    #[serial]
    fn test_tsa_config_from_env_strict_1() {
        std::env::set_var("ATL_STRICT_TSA", "1");

        let config = TsaConfig::from_env();
        assert!(config.strict);

        std::env::remove_var("ATL_STRICT_TSA");
    }

    #[test]
    #[serial]
    fn test_tsa_config_from_env_strict_false() {
        std::env::set_var("ATL_STRICT_TSA", "false");

        let config = TsaConfig::from_env();
        assert!(!config.strict);

        std::env::remove_var("ATL_STRICT_TSA");
    }

    #[test]
    #[serial]
    fn test_tsa_config_from_env_strict_0() {
        std::env::set_var("ATL_STRICT_TSA", "0");

        let config = TsaConfig::from_env();
        assert!(!config.strict);

        std::env::remove_var("ATL_STRICT_TSA");
    }

    #[test]
    #[serial]
    fn test_tsa_config_from_env_all_values() {
        std::env::set_var("ATL_TSA_URLS", "https://tsa1.example.com,https://tsa2.example.com");
        std::env::set_var("ATL_TSA_TIMEOUT_MS", "2000");
        std::env::set_var("ATL_STRICT_TSA", "false");

        let config = TsaConfig::from_env();
        assert_eq!(config.urls.len(), 2);
        assert_eq!(config.timeout_ms, 2000);
        assert!(!config.strict);

        std::env::remove_var("ATL_TSA_URLS");
        std::env::remove_var("ATL_TSA_TIMEOUT_MS");
        std::env::remove_var("ATL_STRICT_TSA");
    }

    #[test]
    fn test_tsa_config_clone() {
        let config = TsaConfig {
            urls: vec!["https://tsa.example.com".to_string()],
            timeout_ms: 1000,
            strict: false,
        };
        let cloned = config.clone();

        assert_eq!(config.urls, cloned.urls);
        assert_eq!(config.timeout_ms, cloned.timeout_ms);
        assert_eq!(config.strict, cloned.strict);
    }

    #[test]
    fn test_tsa_config_debug() {
        let config = TsaConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("TsaConfig"));
    }

    // ========== Config Tests ==========

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 3000);
        assert_eq!(config.database_path, "./atl.db");
        assert!(config.signing_key_path.is_none());
        assert_eq!(config.log_level, "info");
        assert!(config.access_tokens.is_none());
        assert_eq!(config.mode, ServerMode::Standalone);
        assert_eq!(config.base_url, "http://localhost:3000");
    }

    #[test]
    fn test_config_clone() {
        let config = Config {
            host: "0.0.0.0".to_string(),
            port: 8080,
            database_path: "/data/atl.db".to_string(),
            signing_key_path: Some("/keys/key.pem".to_string()),
            log_level: "debug".to_string(),
            access_tokens: Some(vec!["token1".to_string(), "token2".to_string()]),
            tsa: TsaConfig {
                urls: vec!["https://tsa.example.com".to_string()],
                timeout_ms: 1000,
                strict: false,
            },
            mode: ServerMode::Sequencer,
            base_url: "https://api.example.com".to_string(),
        };

        let cloned = config.clone();
        assert_eq!(config.host, cloned.host);
        assert_eq!(config.port, cloned.port);
        assert_eq!(config.database_path, cloned.database_path);
        assert_eq!(config.signing_key_path, cloned.signing_key_path);
        assert_eq!(config.log_level, cloned.log_level);
        assert_eq!(config.access_tokens, cloned.access_tokens);
        assert_eq!(config.mode, cloned.mode);
        assert_eq!(config.base_url, cloned.base_url);
    }

    #[test]
    fn test_config_debug() {
        let config = Config::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("Config"));
        assert!(debug_str.contains("127.0.0.1"));
    }

    #[test]
    fn test_config_with_access_tokens() {
        let config = Config {
            access_tokens: Some(vec![
                "token1".to_string(),
                "token2".to_string(),
                "token3".to_string(),
            ]),
            ..Default::default()
        };

        assert!(config.access_tokens.is_some());
        assert_eq!(config.access_tokens.unwrap().len(), 3);
    }

    #[test]
    fn test_config_different_modes() {
        let standalone = Config {
            mode: ServerMode::Standalone,
            ..Default::default()
        };
        let node = Config {
            mode: ServerMode::Node,
            ..Default::default()
        };
        let sequencer = Config {
            mode: ServerMode::Sequencer,
            ..Default::default()
        };

        assert_eq!(standalone.mode, ServerMode::Standalone);
        assert_eq!(node.mode, ServerMode::Node);
        assert_eq!(sequencer.mode, ServerMode::Sequencer);
    }
}
