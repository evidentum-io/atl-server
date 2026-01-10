//! Health endpoint tests

use crate::common::*;
use atl_server::config::ServerMode;
use serde_json::Value;

/// Helper to create app in SEQUENCER mode
async fn test_app_sequencer() -> axum::Router {
    use atl_server::SqliteStore;
    use atl_server::api::{AppState, create_router};

    let mut storage = SqliteStore::in_memory().expect("Failed to create in-memory storage");
    storage.initialize().expect("Failed to initialize storage");
    let storage = Arc::new(storage);

    let dispatcher = Arc::new(crate::common::fixtures::LocalDispatcher::new(
        storage.clone() as Arc<dyn atl_server::traits::Storage>,
    ));

    let state = Arc::new(AppState {
        mode: ServerMode::Sequencer,
        dispatcher: dispatcher as Arc<dyn atl_server::traits::dispatcher::SequencerClient>,
        storage: Some(storage),
        access_tokens: None,
        base_url: "http://localhost:3000".to_string(),
    });

    create_router(state)
}

#[tokio::test]
async fn test_health_endpoint_in_sequencer_mode() {
    let app = test_app_sequencer().await;

    let response = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "healthy");
    assert_eq!(json["mode"], "SEQUENCER");
}

#[tokio::test]
async fn test_health_endpoint_not_available_in_standalone_mode() {
    let app = test_app().await;

    let response = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();

    // Should return 404 because router doesn't add /health in Standalone mode
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
