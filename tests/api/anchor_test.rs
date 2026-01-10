//! Tests for POST /v1/anchor and GET /v1/anchor/{id} endpoints

use crate::common::*;
use serde_json::Value;

#[tokio::test]
async fn test_anchor_json_payload() {
    let app = test_app().await;

    let body = serde_json::json!({
        "payload": {"document": "test", "version": 1},
        "metadata": {"source": "unit-test"}
    });

    let response = app
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body_bytes).unwrap();

    // Verify receipt structure
    assert_valid_receipt_structure(&json);
    assert_eq!(json["spec_version"], "1.0.0");
    assert!(json["entry"]["id"].is_string());

    // Verify hash formats
    let payload_hash = json["entry"]["payload_hash"].as_str().unwrap();
    let metadata_hash = json["entry"]["metadata_hash"].as_str().unwrap();
    assert_hash_format(payload_hash, "sha256");
    assert_hash_format(metadata_hash, "sha256");
}

#[tokio::test]
async fn test_anchor_json_without_metadata() {
    let app = test_app().await;

    let body = serde_json::json!({
        "payload": {"simple": "data"}
    });

    let response = app
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_anchor_invalid_json() {
    let app = test_app().await;

    let response = app
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .body(Body::from("not valid json"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_anchor_missing_payload() {
    let app = test_app().await;

    let body = serde_json::json!({
        "metadata": {"only": "metadata"}
    });

    let response = app
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_anchor_with_external_id() {
    let app = test_app().await;

    let body = serde_json::json!({
        "payload": {"doc": "test"},
        "external_id": "order-12345"
    });

    let response = app
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body_bytes).unwrap();

    // Verify external_id is present in entry
    assert_eq!(json["entry"]["external_id"], "order-12345");
}

#[tokio::test]
async fn test_get_receipt_after_anchor() {
    let app = test_app().await;

    // First anchor something
    let anchor_body = serde_json::json!({
        "payload": {"doc": "for-receipt-test"},
        "metadata": {"test": true}
    });

    let anchor_response = app
        .clone()
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_string(&anchor_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    let anchor_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(anchor_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();

    let entry_id = anchor_json["entry"]["id"].as_str().unwrap();

    // Now get the receipt
    let receipt_response = app
        .oneshot(
            Request::get(format!("/v1/anchor/{}", entry_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(receipt_response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(receipt_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_valid_receipt_structure(&json);
    assert_eq!(json["spec_version"], "1.0.0");
    assert_eq!(json["entry"]["id"], entry_id);
}

#[tokio::test]
async fn test_get_receipt_not_found() {
    let app = test_app().await;

    let response = app
        .oneshot(
            Request::get("/v1/anchor/550e8400-e29b-41d4-a716-446655440000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_receipt_invalid_uuid() {
    let app = test_app().await;

    let response = app
        .oneshot(
            Request::get("/v1/anchor/not-a-uuid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
