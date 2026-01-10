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
mod sequencer;

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
    let _ = betteruptime_heartbeat::spawn_from_env();

    tracing::info!("Starting atl-server v{}", env!("CARGO_PKG_VERSION"));
    tracing::info!("Privacy by Design: Server NEVER stores payload data");

    // Initialize storage backend
    #[cfg(feature = "sqlite")]
    let storage = {
        use std::sync::Arc;
        use storage::SqliteStore;

        tracing::info!("Initializing SQLite storage at: {}", args.database);
        let store = SqliteStore::new(&args.database)?;

        // Initialize schema and origin key
        use traits::Storage;
        store.initialize()?;

        Arc::new(store) as Arc<dyn traits::Storage>
    };

    #[cfg(not(feature = "sqlite"))]
    let storage = {
        anyhow::bail!("No storage backend enabled. Compile with --features sqlite");
    };

    // Load Sequencer configuration
    let sequencer_config = sequencer::SequencerConfig::from_env();

    tracing::info!(
        batch_size = sequencer_config.batch_size,
        batch_timeout_ms = sequencer_config.batch_timeout_ms,
        buffer_size = sequencer_config.buffer_size,
        "Sequencer configuration loaded"
    );

    // Create Sequencer and get handle
    let (sequencer_instance, sequencer_handle) =
        sequencer::Sequencer::new(storage.clone(), sequencer_config);

    // Spawn sequencer as background task
    let sequencer_task = tokio::spawn(async move {
        sequencer_instance.run().await;
    });

    // Create LocalDispatcher with sequencer handle
    let dispatcher = std::sync::Arc::new(traits::LocalDispatcher::new(sequencer_handle))
        as std::sync::Arc<dyn traits::SequencerClient>;

    // Parse access tokens
    let access_tokens = args.access_tokens.as_ref().map(|tokens| {
        tokens
            .split(',')
            .map(|t| t.trim().to_string())
            .collect::<Vec<_>>()
    });

    // Get base URL from environment or build from host:port
    let base_url = std::env::var("ATL_BASE_URL")
        .unwrap_or_else(|_| format!("http://{}:{}", args.host, args.port));

    // Create application state
    let app_state = std::sync::Arc::new(api::AppState {
        mode: config::ServerMode::Standalone,
        dispatcher,
        storage: Some(storage),
        access_tokens,
        base_url,
    });

    // Create router
    let app = api::create_router(app_state);

    // Start HTTP server
    let bind_addr = format!("{}:{}", args.host, args.port);
    tracing::info!("Starting HTTP server on {}", bind_addr);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;

    // Run server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Wait for sequencer to finish
    tracing::info!("Waiting for sequencer to shut down...");
    sequencer_task.await?;

    tracing::info!("Server shutdown complete");
    Ok(())
}

/// Wait for SIGTERM or SIGINT
async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("Shutdown signal received, starting graceful shutdown...");
}
