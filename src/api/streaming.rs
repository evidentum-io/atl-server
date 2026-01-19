//! Streaming hash computation for file uploads

use axum::body::Bytes;
use futures::Stream;
use sha2::{Digest, Sha256};

/// Compute SHA-256 hash from a byte stream without buffering entire content.
///
/// Memory usage is O(1) regardless of stream size.
///
/// # Errors
/// Returns the stream's error type if reading fails.
#[allow(dead_code)]
pub async fn hash_stream<S, E>(stream: S) -> Result<[u8; 32], E>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    use futures::StreamExt;

    let mut hasher = Sha256::new();
    let mut stream = stream;

    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        hasher.update(&bytes);
        // Chunk is dropped here - no accumulation
    }

    Ok(hasher.finalize().into())
}

/// Hash payload as raw bytes
///
/// - String values: hash the raw UTF-8 bytes (no JSON quotes)
/// - Other JSON values: hash JCS-canonicalized representation (for determinism)
///
/// According to ATL Protocol, `PayloadHash` is the hash of the original data,
/// not its JSON representation.
pub fn hash_payload(value: &serde_json::Value) -> [u8; 32] {
    match value {
        serde_json::Value::String(s) => {
            use sha2::{Digest, Sha256};
            Sha256::digest(s.as_bytes()).into()
        }
        _ => atl_core::canonicalize_and_hash(value),
    }
}

/// Hash JSON payload using JCS canonicalization
///
/// Uses RFC 8785 JSON Canonicalization Scheme for deterministic hashing.
/// This is kept for backward compatibility with metadata hashing.
pub fn hash_json_payload(value: &serde_json::Value) -> [u8; 32] {
    atl_core::canonicalize_and_hash(value)
}

/// Hash metadata (empty object if None)
///
/// Returns SHA-256 hash of canonicalized metadata JSON.
pub fn hash_metadata(metadata: Option<&serde_json::Value>) -> [u8; 32] {
    match metadata {
        Some(value) => hash_json_payload(value),
        None => {
            let empty = serde_json::json!({});
            hash_json_payload(&empty)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    #[test]
    fn test_hash_payload_string() {
        // String payload should be hashed as raw bytes, not JSON
        let payload = serde_json::json!("TEST");
        let hash = hash_payload(&payload);
        assert_eq!(hash.len(), 32);

        // Verify against expected hash: echo -n 'TEST' | sha256sum
        // Expected: 94ee059335e587e501cc4bf90613e0814f00a7b08bc7c648fd865a2af6a22cc2
        let expected: [u8; 32] = [
            0x94, 0xee, 0x05, 0x93, 0x35, 0xe5, 0x87, 0xe5, 0x01, 0xcc, 0x4b, 0xf9, 0x06, 0x13,
            0xe0, 0x81, 0x4f, 0x00, 0xa7, 0xb0, 0x8b, 0xc7, 0xc6, 0x48, 0xfd, 0x86, 0x5a, 0x2a,
            0xf6, 0xa2, 0x2c, 0xc2,
        ];
        assert_eq!(
            hash, expected,
            "Hash of string 'TEST' should match raw byte hash"
        );
    }

    #[test]
    fn test_hash_payload_object() {
        // Object payload should use JCS canonicalization
        let payload = serde_json::json!({"test": "data"});
        let hash = hash_payload(&payload);
        assert_eq!(hash.len(), 32);

        // Should be same as hash_json_payload for objects
        let expected = hash_json_payload(&payload);
        assert_eq!(hash, expected);
    }

    #[test]
    fn test_hash_json_payload() {
        let payload = serde_json::json!({"test": "data"});
        let hash = hash_json_payload(&payload);
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_hash_json_payload_deterministic() {
        let payload = serde_json::json!({"b": 2, "a": 1});
        let hash1 = hash_json_payload(&payload);
        let hash2 = hash_json_payload(&payload);
        assert_eq!(hash1, hash2, "Hash should be deterministic");

        // Same data, different key order - should produce same hash due to canonicalization
        let payload2 = serde_json::json!({"a": 1, "b": 2});
        let hash3 = hash_json_payload(&payload2);
        assert_eq!(
            hash1, hash3,
            "Canonicalization should produce same hash regardless of key order"
        );
    }

    #[test]
    fn test_hash_json_payload_empty_object() {
        let payload = serde_json::json!({});
        let hash = hash_json_payload(&payload);
        assert_eq!(hash.len(), 32);
        // Should be consistent
        let hash2 = hash_json_payload(&payload);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_json_payload_nested() {
        let payload = serde_json::json!({
            "level1": {
                "level2": {
                    "value": "deep"
                }
            }
        });
        let hash = hash_json_payload(&payload);
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_hash_json_payload_array() {
        let payload = serde_json::json!([1, 2, 3, 4, 5]);
        let hash = hash_json_payload(&payload);
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_hash_metadata_none() {
        let hash = hash_metadata(None);
        // Should hash empty object
        let empty_hash = hash_json_payload(&serde_json::json!({}));
        assert_eq!(hash, empty_hash);
    }

    #[test]
    fn test_hash_metadata_some() {
        let metadata = serde_json::json!({"source": "test"});
        let hash = hash_metadata(Some(&metadata));
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_hash_metadata_consistency() {
        let metadata = serde_json::json!({"key": "value"});
        let hash1 = hash_metadata(Some(&metadata));
        let hash2 = hash_metadata(Some(&metadata));
        assert_eq!(hash1, hash2, "Hash should be consistent");
    }

    #[tokio::test]
    async fn test_hash_stream_empty() {
        let empty_stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::new())]);
        let hash = hash_stream(empty_stream).await.unwrap();
        assert_eq!(hash.len(), 32);

        // Empty stream should produce SHA-256 hash of empty data
        let expected: [u8; 32] = Sha256::new().finalize().into();
        assert_eq!(hash, expected);
    }

    #[tokio::test]
    async fn test_hash_stream_single_chunk() {
        let data = b"hello world";
        let chunk_stream =
            stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(data))]);
        let hash = hash_stream(chunk_stream).await.unwrap();
        assert_eq!(hash.len(), 32);

        // Verify against direct hashing
        let expected: [u8; 32] = Sha256::digest(data).into();
        assert_eq!(hash, expected);
    }

    #[tokio::test]
    async fn test_hash_stream_multiple_chunks() {
        let chunk1 = b"hello ";
        let chunk2 = b"world";
        let chunk_stream = stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(chunk1)),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(chunk2)),
        ]);
        let hash = hash_stream(chunk_stream).await.unwrap();
        assert_eq!(hash.len(), 32);

        // Should be same as hashing concatenated data
        let expected: [u8; 32] = Sha256::digest(b"hello world").into();
        assert_eq!(hash, expected);
    }

    #[tokio::test]
    async fn test_hash_stream_large_chunks() {
        // Create a large data set split into chunks
        let chunk_size = 1024 * 64; // 64KB chunks
        let num_chunks = 10;
        let mut all_data = Vec::new();

        let chunks: Vec<Result<Bytes, std::io::Error>> = (0..num_chunks)
            .map(|i| {
                let chunk_data = vec![i as u8; chunk_size];
                all_data.extend_from_slice(&chunk_data);
                Ok(Bytes::from(chunk_data))
            })
            .collect();

        let chunk_stream = stream::iter(chunks);
        let hash = hash_stream(chunk_stream).await.unwrap();
        assert_eq!(hash.len(), 32);

        // Verify against direct hashing of all data
        let expected: [u8; 32] = Sha256::digest(&all_data).into();
        assert_eq!(hash, expected);
    }

    #[tokio::test]
    async fn test_hash_stream_error_propagation() {
        let error_stream = stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data")),
            Err(std::io::Error::other("stream error")),
        ]);

        let result = hash_stream(error_stream).await;
        assert!(result.is_err(), "Error should be propagated");
    }

    #[tokio::test]
    async fn test_hash_stream_consistency() {
        let data = b"test data for consistency";
        let stream1 = stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(data))]);
        let stream2 = stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(data))]);

        let hash1 = hash_stream(stream1).await.unwrap();
        let hash2 = hash_stream(stream2).await.unwrap();

        assert_eq!(hash1, hash2, "Same data should produce same hash");
    }

    #[tokio::test]
    async fn test_hash_stream_different_chunking_same_result() {
        let data = b"abcdefghijklmnopqrstuvwxyz";

        // Single chunk
        let stream1 = stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(data))]);

        // Multiple chunks
        let stream2 = stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(&data[0..10])),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(&data[10..20])),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(&data[20..])),
        ]);

        let hash1 = hash_stream(stream1).await.unwrap();
        let hash2 = hash_stream(stream2).await.unwrap();

        assert_eq!(hash1, hash2, "Different chunking should produce same hash");
    }
}
