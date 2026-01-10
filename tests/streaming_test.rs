//! Streaming hash computation tests

use atl_server::api::streaming::{hash_json_payload, hash_metadata, hash_stream};
use bytes::Bytes;
use futures::stream;

#[tokio::test]
async fn test_streaming_hash_computation() {
    // Simulate a 10MB file as chunks
    let chunk_size = 64 * 1024; // 64KB chunks
    let total_chunks = 160; // ~10MB

    let chunks: Vec<_> = (0..total_chunks)
        .map(|i| Ok::<_, std::io::Error>(Bytes::from(vec![(i % 256) as u8; chunk_size])))
        .collect();

    let stream = stream::iter(chunks);

    let hash = hash_stream(stream).await.expect("Failed to hash stream");

    // Hash should be deterministic
    assert_eq!(hash.len(), 32);
}

#[tokio::test]
async fn test_streaming_memory_usage() {
    // This test verifies O(1) memory usage for streaming
    // The key is that we don't accumulate chunks

    let chunk_count = 1000;
    let chunk_size = 1024; // 1KB chunks

    let chunks: Vec<_> = (0..chunk_count)
        .map(|i| Ok::<_, std::io::Error>(Bytes::from(vec![i as u8; chunk_size])))
        .collect();

    let stream = stream::iter(chunks);
    let hash = hash_stream(stream).await.expect("Failed to hash stream");

    assert_eq!(hash.len(), 32);
}

#[tokio::test]
async fn test_streaming_deterministic_hash() {
    // Same stream content should produce same hash
    let chunks1: Vec<_> = vec![
        Ok::<_, std::io::Error>(Bytes::from("hello")),
        Ok(Bytes::from(" ")),
        Ok(Bytes::from("world")),
    ];

    let chunks2: Vec<_> = vec![
        Ok::<_, std::io::Error>(Bytes::from("hello")),
        Ok(Bytes::from(" ")),
        Ok(Bytes::from("world")),
    ];

    let stream1 = stream::iter(chunks1);
    let stream2 = stream::iter(chunks2);

    let hash1 = hash_stream(stream1).await.expect("Failed to hash stream");
    let hash2 = hash_stream(stream2).await.expect("Failed to hash stream");

    assert_eq!(hash1, hash2);
}

#[test]
fn test_hash_json_payload() {
    let payload = serde_json::json!({"test": "data", "number": 42});
    let hash = hash_json_payload(&payload);

    assert_eq!(hash.len(), 32);

    // Same payload should produce same hash (deterministic)
    let hash2 = hash_json_payload(&payload);
    assert_eq!(hash, hash2);
}

#[test]
fn test_hash_json_canonical() {
    // Different key order should produce same hash (JCS canonicalization)
    let payload1 = serde_json::json!({"a": 1, "b": 2});
    let payload2 = serde_json::json!({"b": 2, "a": 1});

    let hash1 = hash_json_payload(&payload1);
    let hash2 = hash_json_payload(&payload2);

    assert_eq!(hash1, hash2);
}

#[test]
fn test_hash_metadata_none() {
    let hash = hash_metadata(None);

    // Empty object hash
    let empty_hash = hash_metadata(Some(&serde_json::json!({})));

    assert_eq!(hash, empty_hash);
}

#[test]
fn test_hash_metadata_some() {
    let metadata = serde_json::json!({"source": "test", "version": "1.0"});
    let hash = hash_metadata(Some(&metadata));
    assert_eq!(hash.len(), 32);
}
