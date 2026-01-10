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

/// Hash JSON payload using JCS canonicalization
///
/// Uses RFC 8785 JSON Canonicalization Scheme for deterministic hashing.
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

    #[test]
    fn test_hash_json_payload() {
        let payload = serde_json::json!({"test": "data"});
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
}
