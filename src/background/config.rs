// File: src/background/config.rs

use super::ots_poll_job::OtsPollJobConfig;
use super::tree_closer::TreeCloserConfig;
use super::tsa_job::TsaJobConfig;

/// Global background jobs configuration
#[derive(Debug, Clone, Default)]
pub struct BackgroundConfig {
    /// Disable all background jobs (for testing)
    pub disabled: bool,

    pub tree_closer: TreeCloserConfig,
    pub tsa_job: TsaJobConfig,
    pub ots_poll_job: OtsPollJobConfig,
}

impl BackgroundConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        Self {
            disabled: std::env::var("ATL_BACKGROUND_DISABLED")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            tree_closer: TreeCloserConfig::from_env(),
            tsa_job: TsaJobConfig::from_env(),
            ots_poll_job: OtsPollJobConfig::from_env(),
        }
    }
}
