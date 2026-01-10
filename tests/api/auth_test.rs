//! Authentication middleware tests

use crate::common::*;

#[tokio::test]
async fn test_auth_required_when_configured() {
    let app = test_app_with_auth(vec!["secret-token".into()]).await;

    // Request without token
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"payload": {"test": "data"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Request with valid token
    let response = app
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer secret-token")
                .body(Body::from(r#"{"payload": {"test": "data"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_auth_invalid_token() {
    let app = test_app_with_auth(vec!["correct-token".into()]).await;

    let response = app
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer wrong-token")
                .body(Body::from(r#"{"payload": {"test": "data"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_auth_missing_bearer_prefix() {
    let app = test_app_with_auth(vec!["secret-token".into()]).await;

    let response = app
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .header("Authorization", "secret-token")
                .body(Body::from(r#"{"payload": {"test": "data"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_open_mode_no_auth() {
    let app = test_app().await;

    let response = app
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"payload": {"test": "data"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_auth_multiple_valid_tokens() {
    let app = test_app_with_auth(vec!["token1".into(), "token2".into()]).await;

    // Test first token
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer token1")
                .body(Body::from(r#"{"payload": {"test": "data"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    // Test second token
    let response = app
        .oneshot(
            Request::post("/v1/anchor")
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer token2")
                .body(Body::from(r#"{"payload": {"test": "data"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
}
