//! atl-server - Autonomous notarization primitive for ATL Protocol v1

use clap::{Parser, ValueEnum};
use std::path::Path;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

mod api;
mod config;
mod error;
mod traits;

mod storage;

#[cfg(any(feature = "rfc3161", feature = "ots"))]
mod anchoring;

mod receipt;
mod sequencer;

mod background;

#[cfg(feature = "grpc")]
mod grpc;

/// Server role for distributed architecture
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ServerRole {
    /// Standalone mode: HTTP + SequencerCore + Storage + Signing
    Standalone,
    /// Node mode: HTTP only, forwards to SEQUENCER via gRPC
    Node,
    /// Sequencer mode: gRPC server + SequencerCore + Storage + Signing
    Sequencer,
}

#[derive(Parser, Debug)]
#[command(name = "atl-server")]
#[command(about = "Autonomous notarization primitive for ATL Protocol v1")]
struct Args {
    /// Server role
    #[arg(long, env = "ATL_ROLE", default_value = "standalone")]
    role: ServerRole,

    /// Host to bind to
    #[arg(long, env = "ATL_SERVER_HOST", default_value = "127.0.0.1")]
    host: String,

    /// Port to bind to
    #[arg(long, env = "ATL_SERVER_PORT", default_value = "3000")]
    port: u16,

    /// gRPC port (for SEQUENCER role)
    #[arg(long, env = "ATL_GRPC_PORT", default_value = "50051")]
    grpc_port: u16,

    /// Path to SQLite database
    #[arg(long, env = "ATL_DATABASE_PATH", default_value = "./atl.db")]
    database: String,

    /// Path to Ed25519 signing key
    #[arg(long, env = "ATL_SIGNING_KEY_PATH")]
    signing_key: Option<String>,

    /// Comma-separated list of RFC 3161 TSA URLs
    #[arg(long, env = "ATL_TSA_URLS")]
    tsa_urls: Option<String>,

    /// Timeout per TSA request in milliseconds
    #[arg(long, env = "ATL_TSA_TIMEOUT_MS", default_value = "500")]
    tsa_timeout_ms: u64,

    /// Strict TSA mode
    #[arg(long, env = "ATL_STRICT_TSA", default_value = "true")]
    strict_tsa: bool,

    /// Log level
    #[arg(long, env = "ATL_LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// Comma-separated list of Bearer tokens for access control
    #[arg(long, env = "ATL_ACCESS_TOKENS")]
    access_tokens: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(&args.log_level))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Start active monitoring
    #[cfg(feature = "monitoring")]
    let _ = betteruptime_heartbeat::spawn_from_env();

    tracing::info!("Starting atl-server v{}", env!("CARGO_PKG_VERSION"));
    tracing::info!("Privacy by Design: Server NEVER stores payload data");

    // Run based on role
    match args.role {
        ServerRole::Standalone => run_standalone(args).await,
        ServerRole::Node => run_node(args).await,
        ServerRole::Sequencer => run_sequencer(args).await,
    }
}

/// Get or create tree UUID from file or environment variable.
///
/// Priority:
/// 1. Read from `{data_dir}/tree_uuid` file if exists
/// 2. Use `ATL_TREE_UUID` environment variable if set
/// 3. Generate new UUID and save to file
fn get_or_create_tree_uuid(data_dir: &Path) -> anyhow::Result<Uuid> {
    let uuid_file = data_dir.join("tree_uuid");

    if uuid_file.exists() {
        let uuid_str = std::fs::read_to_string(&uuid_file)?;
        let uuid = Uuid::parse_str(uuid_str.trim())?;
        tracing::info!("Tree UUID (from file): {}", uuid);
        return Ok(uuid);
    }

    let uuid = if let Ok(env_uuid) = std::env::var("ATL_TREE_UUID") {
        Uuid::parse_str(&env_uuid)?
    } else {
        Uuid::new_v4()
    };

    std::fs::create_dir_all(data_dir)?;
    std::fs::write(&uuid_file, uuid.to_string())?;

    tracing::info!("Tree UUID: {}", uuid);
    Ok(uuid)
}

/// Run in STANDALONE mode
async fn run_standalone(args: Args) -> anyhow::Result<()> {
    use std::path::PathBuf;
    use std::sync::Arc;

    tracing::info!("Starting in STANDALONE mode");

    // Load signing key first (needed for origin)
    let signing_key_path = args
        .signing_key
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("ATL_SIGNING_KEY_PATH required for STANDALONE mode"))?;
    let signer = receipt::CheckpointSigner::from_file(signing_key_path)?;

    // Initialize StorageEngine
    tracing::info!("Initializing storage at: {}", args.database);
    let storage_config = crate::storage::StorageConfig {
        data_dir: PathBuf::from(&args.database),
        wal_dir: None,
        slab_dir: None,
        db_path: None,
        slab_capacity: 1_000_000,
        max_open_slabs: 10,
        wal_keep_count: 2,
        fsync_enabled: true,
    };

    let tree_uuid = get_or_create_tree_uuid(&PathBuf::from(&args.database))?;
    let origin_id = atl_core::compute_origin_id(&tree_uuid);

    let engine = crate::storage::StorageEngine::new(storage_config, origin_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize storage engine: {}", e))?;
    let storage_engine = Arc::new(engine);
    let storage: Arc<dyn traits::Storage> = storage_engine.clone();

    // Initialize Chain Index
    let chain_index_path = PathBuf::from(&args.database).join("chain_index.db");
    let chain_index = crate::storage::chain_index::ChainIndex::open(&chain_index_path)
        .map_err(|e| anyhow::anyhow!("Failed to initialize Chain Index: {}", e))?;
    let chain_index = Arc::new(tokio::sync::Mutex::new(chain_index));

    // Sync Chain Index with main DB
    {
        let index = storage_engine.index_store();
        let idx = index.lock().await;
        let ci = chain_index.lock().await;
        let synced = ci
            .sync_with_main_db(&idx)
            .map_err(|e| anyhow::anyhow!("Failed to sync Chain Index: {}", e))?;
        if synced > 0 {
            tracing::info!(synced_trees = synced, "Chain Index synced with main DB");
        }
    }

    // Create sequencer
    let sequencer_config = sequencer::SequencerConfig::from_env();
    let (sequencer_instance, sequencer_handle) =
        sequencer::Sequencer::new(storage.clone(), sequencer_config);

    let sequencer_task = tokio::spawn(async move {
        sequencer_instance.run().await;
    });

    let background_config = background::BackgroundConfig::from_env();
    let background_runner = background::BackgroundJobRunner::new(
        storage_engine.clone(),
        chain_index.clone(),
        background_config,
    );
    let background_handles = background_runner.start().await?;

    // Create dispatcher
    let dispatcher = Arc::new(traits::LocalDispatcher::new(
        sequencer_handle,
        signer,
        storage.clone(),
    )) as Arc<dyn traits::SequencerClient>;

    // Start HTTP server
    start_http_server(
        args,
        config::ServerMode::Standalone,
        dispatcher,
        Some(storage),
    )
    .await?;

    // Shutdown background jobs first
    tracing::info!("Shutting down background jobs...");
    background_runner.shutdown();
    for handle in background_handles {
        let _ = handle.await;
    }

    // Wait for sequencer
    tracing::info!("Waiting for sequencer to shut down...");
    sequencer_task.await?;

    Ok(())
}

/// Run in NODE mode
#[cfg(feature = "grpc")]
async fn run_node(args: Args) -> anyhow::Result<()> {
    use std::sync::Arc;

    tracing::info!("Starting in NODE mode");

    // Create gRPC dispatcher
    let grpc_config = grpc::GrpcClientConfig::from_env();
    tracing::info!("Connecting to Sequencer at: {}", grpc_config.sequencer_url);

    let dispatcher =
        Arc::new(grpc::GrpcDispatcher::new(grpc_config).await?) as Arc<dyn traits::SequencerClient>;

    // Start HTTP server (no storage)
    start_http_server(args, config::ServerMode::Node, dispatcher, None).await?;

    Ok(())
}

/// Run in SEQUENCER mode
#[cfg(feature = "grpc")]
async fn run_sequencer(args: Args) -> anyhow::Result<()> {
    use std::path::PathBuf;
    use std::sync::Arc;

    tracing::info!("Starting in SEQUENCER mode");

    // Load signing key first (needed for origin)
    let signing_key_path = args
        .signing_key
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("ATL_SIGNING_KEY_PATH required for SEQUENCER mode"))?;
    let signer = Arc::new(receipt::CheckpointSigner::from_file(signing_key_path)?);

    // Initialize StorageEngine
    tracing::info!("Initializing storage at: {}", args.database);
    let storage_config = crate::storage::StorageConfig {
        data_dir: PathBuf::from(&args.database),
        wal_dir: None,
        slab_dir: None,
        db_path: None,
        slab_capacity: 1_000_000,
        max_open_slabs: 10,
        wal_keep_count: 2,
        fsync_enabled: true,
    };

    let tree_uuid = get_or_create_tree_uuid(&PathBuf::from(&args.database))?;
    let origin_id = atl_core::compute_origin_id(&tree_uuid);

    let engine = crate::storage::StorageEngine::new(storage_config, origin_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize storage engine: {}", e))?;
    let storage_engine = Arc::new(engine);
    let storage: Arc<dyn traits::Storage> = storage_engine.clone();

    // Initialize Chain Index
    let chain_index_path = PathBuf::from(&args.database).join("chain_index.db");
    let chain_index = crate::storage::chain_index::ChainIndex::open(&chain_index_path)
        .map_err(|e| anyhow::anyhow!("Failed to initialize Chain Index: {}", e))?;
    let chain_index = Arc::new(tokio::sync::Mutex::new(chain_index));

    // Sync Chain Index with main DB
    {
        let index = storage_engine.index_store();
        let idx = index.lock().await;
        let ci = chain_index.lock().await;
        let synced = ci
            .sync_with_main_db(&idx)
            .map_err(|e| anyhow::anyhow!("Failed to sync Chain Index: {}", e))?;
        if synced > 0 {
            tracing::info!(synced_trees = synced, "Chain Index synced with main DB");
        }
    }

    // Create sequencer
    let sequencer_config = sequencer::SequencerConfig::from_env();
    let (sequencer_instance, sequencer_handle) =
        sequencer::Sequencer::new(storage.clone(), sequencer_config);

    let sequencer_task = tokio::spawn(async move {
        sequencer_instance.run().await;
    });

    let background_config = background::BackgroundConfig::from_env();
    let background_runner = background::BackgroundJobRunner::new(
        storage_engine.clone(),
        chain_index.clone(),
        background_config,
    );
    let background_handles = background_runner.start().await?;

    // Create gRPC server
    let token = std::env::var("ATL_SEQUENCER_TOKEN").ok();
    let grpc_server = grpc::SequencerGrpcServer::new(
        sequencer_handle.clone(),
        storage.clone(),
        signer.clone(),
        token,
    );

    let grpc_service = grpc_server.into_service();

    // Start gRPC server
    let grpc_addr = format!("0.0.0.0:{}", args.grpc_port);
    tracing::info!("Starting gRPC server on {}", grpc_addr);

    let grpc_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(grpc_service)
            .serve(grpc_addr.parse().unwrap())
            .await
    });

    // Create dispatcher for HTTP admin endpoints
    let dispatcher = Arc::new(traits::LocalDispatcher::new(
        sequencer_handle,
        signer.as_ref().clone(),
        storage.clone(),
    )) as Arc<dyn traits::SequencerClient>;

    // Start HTTP admin server
    let http_handle = tokio::spawn(async move {
        start_http_server(
            args,
            config::ServerMode::Sequencer,
            dispatcher,
            Some(storage),
        )
        .await
    });

    // Wait for both servers
    tokio::select! {
        r = grpc_handle => r??,
        r = http_handle => r??,
    }

    // Shutdown background jobs first
    tracing::info!("Shutting down background jobs...");
    background_runner.shutdown();
    for handle in background_handles {
        let _ = handle.await;
    }

    // Wait for sequencer
    tracing::info!("Waiting for sequencer to shut down...");
    sequencer_task.await?;

    Ok(())
}

/// Start HTTP server
async fn start_http_server(
    args: Args,
    mode: config::ServerMode,
    dispatcher: std::sync::Arc<dyn traits::SequencerClient>,
    storage: Option<std::sync::Arc<dyn traits::Storage>>,
) -> anyhow::Result<()> {
    use std::sync::Arc;

    // Parse access tokens
    let access_tokens = args.access_tokens.as_ref().map(|tokens| {
        tokens
            .split(',')
            .map(|t| t.trim().to_string())
            .collect::<Vec<_>>()
    });

    // Get base URL
    let base_url = std::env::var("ATL_BASE_URL")
        .unwrap_or_else(|_| format!("http://{}:{}", args.host, args.port));

    // Create app state
    let app_state = Arc::new(api::AppState {
        mode,
        dispatcher,
        storage,
        access_tokens,
        base_url,
    });

    // Create router
    let app = api::create_router(app_state);

    // Start server
    let bind_addr = format!("{}:{}", args.host, args.port);
    tracing::info!("Starting HTTP server on {}", bind_addr);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("HTTP server shutdown complete");
    Ok(())
}

// Fallback stubs for when features are not enabled
#[cfg(not(feature = "grpc"))]
async fn run_node(_args: Args) -> anyhow::Result<()> {
    anyhow::bail!("NODE mode requires --features grpc")
}

#[cfg(not(feature = "grpc"))]
async fn run_sequencer(_args: Args) -> anyhow::Result<()> {
    anyhow::bail!("SEQUENCER mode requires --features grpc")
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
