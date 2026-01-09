//! atl-server - Autonomous notarization primitive for ATL Protocol v1

use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod api;
mod config;
mod error;
mod traits;

#[cfg(feature = "sqlite")]
mod storage;

#[cfg(any(feature = "rfc3161", feature = "ots"))]
mod anchoring;

mod receipt;

#[derive(Parser, Debug)]
#[command(name = "atl-server")]
#[command(about = "Autonomous notarization primitive for ATL Protocol v1")]
struct Args {
    /// Host to bind to
    #[arg(long, env = "ATL_SERVER_HOST", default_value = "127.0.0.1")]
    host: String,

    /// Port to bind to
    #[arg(long, env = "ATL_SERVER_PORT", default_value = "3000")]
    port: u16,

    /// Path to SQLite database
    #[arg(long, env = "ATL_DATABASE_PATH", default_value = "./atl.db")]
    database: String,

    /// Path to Ed25519 signing key
    #[arg(long, env = "ATL_SIGNING_KEY_PATH")]
    signing_key: Option<String>,

    /// Comma-separated list of RFC 3161 TSA URLs (tried in order with fallback)
    #[arg(long, env = "ATL_TSA_URLS")]
    tsa_urls: Option<String>,

    /// Timeout per TSA request in milliseconds
    #[arg(long, env = "ATL_TSA_TIMEOUT_MS", default_value = "500")]
    tsa_timeout_ms: u64,

    /// Strict TSA mode: if true, return 503 if all TSAs fail
    /// If false, return receipt without TSA anchor (client can /upgrade later)
    #[arg(long, env = "ATL_STRICT_TSA", default_value = "true")]
    strict_tsa: bool,

    /// Log level
    #[arg(long, env = "ATL_LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// Comma-separated list of Bearer tokens for access control (optional)
    /// If not set, server operates in open mode
    #[arg(long, env = "ATL_ACCESS_TOKENS")]
    access_tokens: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse CLI args
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(&args.log_level))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Start active monitoring (if feature enabled and HEARTBEAT_URL is configured)
    #[cfg(feature = "monitoring")]
    betteruptime_heartbeat::spawn_from_env();

    tracing::info!("Starting atl-server v{}", env!("CARGO_PKG_VERSION"));
    tracing::info!("Privacy by Design: Server NEVER stores payload data");

    // TODO: Initialize storage, signer, router
    // TODO: Start HTTP server

    Ok(())
}
