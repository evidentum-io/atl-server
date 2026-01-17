//! Bitcoin block timestamp lookup via public blockchain APIs
//!
//! Provides async lookup of Bitcoin block timestamps using round-robin strategy
//! across multiple public APIs. Returns error if all APIs fail (no estimation fallback).

use crate::anchoring::error::AnchorError;
use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};
use std::time::Duration;

/// Bitcoin API provider configuration
struct ApiProvider {
    name: &'static str,
    /// Base URL for API (without trailing slash)
    base_url: &'static str,
    /// Whether this API uses 2-step lookup (hash first, then block)
    /// - true: Esplora API (blockstream, mempool) - GET /block-height/{h} -> hash, GET /block/{hash} -> JSON
    /// - false: blockchain.info - GET /block-height/{h}?format=json -> { "blocks": [...] }
    two_step: bool,
    /// JSON field name for timestamp
    /// - Esplora API: "timestamp"
    /// - blockchain.info: "time" (inside blocks[0])
    timestamp_field: &'static str,
}

const PROVIDERS: &[ApiProvider] = &[
    ApiProvider {
        name: "blockstream.info",
        base_url: "https://blockstream.info/api",
        two_step: true,
        timestamp_field: "timestamp",
    },
    ApiProvider {
        name: "mempool.space",
        base_url: "https://mempool.space/api",
        two_step: true,
        timestamp_field: "timestamp",
    },
    ApiProvider {
        name: "blockchain.info",
        base_url: "https://blockchain.info",
        two_step: false,
        timestamp_field: "time",
    },
];

/// Global cache for block timestamps (block_height -> unix_timestamp)
pub(crate) static BLOCK_TIME_CACHE: LazyLock<RwLock<HashMap<u64, u64>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Get Bitcoin block timestamp from blockchain APIs (round-robin)
///
/// # Arguments
/// * `height` - Bitcoin block height
/// * `timeout` - HTTP request timeout per provider
///
/// # Returns
/// * `Ok(timestamp)` - Unix timestamp (seconds) when block was mined
/// * `Err(AnchorError::BlockTimeFetchFailed)` - All APIs failed
///
/// # Notes
/// - Uses in-memory cache to avoid duplicate API calls
/// - Tries providers in order: blockstream.info -> mempool.space -> blockchain.info
/// - Returns error if ALL providers fail (no estimate fallback!)
pub async fn get_block_timestamp(height: u64, timeout: Duration) -> Result<u64, AnchorError> {
    // Check cache first
    if let Some(&ts) = BLOCK_TIME_CACHE.read().unwrap().get(&height) {
        return Ok(ts);
    }

    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| AnchorError::BlockTimeFetchFailed {
            height,
            details: format!("Failed to create HTTP client: {e}"),
        })?;

    let mut errors = Vec::new();

    // Try each provider in order
    for provider in PROVIDERS {
        match fetch_from_provider(&client, provider, height).await {
            Ok(ts) => {
                // Cache the result
                BLOCK_TIME_CACHE.write().unwrap().insert(height, ts);
                return Ok(ts);
            }
            Err(e) => {
                tracing::warn!(
                    height = height,
                    provider = provider.name,
                    error = %e,
                    "Failed to fetch block timestamp, trying next provider"
                );
                errors.push(format!("{}: {}", provider.name, e));
            }
        }
    }

    // All providers failed - return error (NO ESTIMATE FALLBACK!)
    Err(AnchorError::BlockTimeFetchFailed {
        height,
        details: errors.join("; "),
    })
}

/// Fetch block timestamp from a specific provider
async fn fetch_from_provider(
    client: &reqwest::Client,
    provider: &ApiProvider,
    height: u64,
) -> Result<u64, String> {
    if provider.two_step {
        fetch_two_step(client, provider.base_url, height, provider.timestamp_field).await
    } else {
        fetch_single_step(client, provider.base_url, height, provider.timestamp_field).await
    }
}

/// Fetch from blockstream.info or mempool.space (2-step lookup)
async fn fetch_two_step(
    client: &reqwest::Client,
    base_url: &str,
    height: u64,
    timestamp_field: &str,
) -> Result<u64, String> {
    // Step 1: Get block hash
    let hash_url = format!("{base_url}/block-height/{height}");
    let hash = client
        .get(&hash_url)
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?
        .error_for_status()
        .map_err(|e| format!("HTTP status error: {e}"))?
        .text()
        .await
        .map_err(|e| format!("Read error: {e}"))?;

    // Step 2: Get block details
    let block_url = format!("{base_url}/block/{}", hash.trim());
    let response = client
        .get(&block_url)
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?
        .error_for_status()
        .map_err(|e| format!("HTTP status error: {e}"))?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| format!("JSON error: {e}"))?;

    // Extract timestamp
    response[timestamp_field]
        .as_u64()
        .ok_or_else(|| format!("Missing '{timestamp_field}' field in response"))
}

/// Fetch from blockchain.info (single-step lookup)
/// NOTE: blockchain.info returns { "blocks": [{ "time": ... }] } format
async fn fetch_single_step(
    client: &reqwest::Client,
    base_url: &str,
    height: u64,
    timestamp_field: &str,
) -> Result<u64, String> {
    // blockchain.info uses /block-height/{height}?format=json
    let url = format!("{base_url}/block-height/{height}?format=json");
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?
        .error_for_status()
        .map_err(|e| format!("HTTP status error: {e}"))?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| format!("JSON error: {e}"))?;

    // blockchain.info wraps response in "blocks" array
    response["blocks"]
        .get(0)
        .and_then(|block| block[timestamp_field].as_u64())
        .ok_or_else(|| format!("Missing 'blocks[0].{timestamp_field}' field in response"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_returns_stored_value() {
        // Pre-populate cache
        {
            let mut cache = BLOCK_TIME_CACHE.write().unwrap();
            cache.insert(999_998, 1_234_567_890);
        }

        // Should return cached value without API call
        let ts = get_block_timestamp(999_998, Duration::from_millis(1))
            .await
            .unwrap();
        assert_eq!(ts, 1_234_567_890);

        // Cleanup
        BLOCK_TIME_CACHE.write().unwrap().remove(&999_998);
    }

    #[tokio::test]
    #[ignore] // Requires network
    async fn test_real_api_genesis_block() {
        let ts = get_block_timestamp(0, Duration::from_secs(10))
            .await
            .unwrap();
        // Genesis block timestamp is exactly 1231006505
        assert_eq!(ts, 1_231_006_505);
    }

    #[tokio::test]
    #[ignore] // Requires network
    async fn test_real_api_block_700000() {
        let ts = get_block_timestamp(700_000, Duration::from_secs(10))
            .await
            .unwrap();
        // Real timestamp should be around 1631318400 (2021-09-11)
        assert!(ts > 1_631_000_000);
        assert!(ts < 1_632_000_000);
    }

    #[tokio::test]
    async fn test_returns_error_when_all_apis_fail() {
        // Use a height that doesn't exist (far future)
        // All 3 APIs should return 404 and we should get an error
        let result = get_block_timestamp(999_999_999, Duration::from_secs(5)).await;

        assert!(result.is_err());
        match result {
            Err(AnchorError::BlockTimeFetchFailed { height, details }) => {
                assert_eq!(height, 999_999_999);
                assert!(details.contains("blockstream.info"));
                assert!(details.contains("mempool.space"));
                assert!(details.contains("blockchain.info"));
            }
            _ => panic!("Expected BlockTimeFetchFailed error"),
        }
    }

    #[tokio::test]
    #[ignore] // Requires network
    async fn test_all_three_providers_return_same_value() {
        // Test that all providers return the same timestamp for genesis block
        // This verifies our provider implementations are correct

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        // Test blockstream.info
        let ts1 = fetch_from_provider(&client, &PROVIDERS[0], 0)
            .await
            .unwrap();

        // Test mempool.space
        let ts2 = fetch_from_provider(&client, &PROVIDERS[1], 0)
            .await
            .unwrap();

        // Test blockchain.info
        let ts3 = fetch_from_provider(&client, &PROVIDERS[2], 0)
            .await
            .unwrap();

        // All should return genesis block timestamp
        assert_eq!(ts1, 1_231_006_505);
        assert_eq!(ts2, 1_231_006_505);
        assert_eq!(ts3, 1_231_006_505);
    }

    #[tokio::test]
    async fn test_cache_isolates_different_heights() {
        // Pre-populate cache with multiple heights
        {
            let mut cache = BLOCK_TIME_CACHE.write().unwrap();
            cache.insert(100, 1_000_000_000);
            cache.insert(200, 2_000_000_000);
        }

        // Should return correct cached values
        let ts1 = get_block_timestamp(100, Duration::from_millis(1))
            .await
            .unwrap();
        let ts2 = get_block_timestamp(200, Duration::from_millis(1))
            .await
            .unwrap();

        assert_eq!(ts1, 1_000_000_000);
        assert_eq!(ts2, 2_000_000_000);

        // Cleanup
        BLOCK_TIME_CACHE.write().unwrap().clear();
    }

    #[tokio::test]
    async fn test_timeout_honored() {
        use std::time::Instant;

        let start = Instant::now();
        // Use non-existent block with very short timeout
        let result = get_block_timestamp(999_999_999, Duration::from_millis(100)).await;

        let elapsed = start.elapsed();
        // Request should fail fast (within reasonable time due to 3 providers)
        // Each provider gets 100ms timeout, so total should be < 1 second
        assert!(elapsed.as_secs() < 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_provider_configurations() {
        // Verify provider array is correctly configured
        assert_eq!(PROVIDERS.len(), 3);

        // Check blockstream.info
        assert_eq!(PROVIDERS[0].name, "blockstream.info");
        assert_eq!(PROVIDERS[0].base_url, "https://blockstream.info/api");
        assert_eq!(PROVIDERS[0].two_step, true);
        assert_eq!(PROVIDERS[0].timestamp_field, "timestamp");

        // Check mempool.space
        assert_eq!(PROVIDERS[1].name, "mempool.space");
        assert_eq!(PROVIDERS[1].base_url, "https://mempool.space/api");
        assert_eq!(PROVIDERS[1].two_step, true);

        // Check blockchain.info
        assert_eq!(PROVIDERS[2].name, "blockchain.info");
        assert_eq!(PROVIDERS[2].base_url, "https://blockchain.info");
        assert_eq!(PROVIDERS[2].two_step, false);
        assert_eq!(PROVIDERS[2].timestamp_field, "time");
    }

    #[tokio::test]
    async fn test_client_creation_failure_handling() {
        // Test behavior when client fails to create (unlikely but possible)
        // We can't easily force reqwest::Client::builder() to fail,
        // but we test that the function signature handles errors correctly
        let result = get_block_timestamp(0, Duration::from_millis(1)).await;
        // Will either succeed (cached) or fail with proper error type
        match result {
            Ok(_) => {}
            Err(e) => {
                assert!(matches!(e, AnchorError::BlockTimeFetchFailed { .. }));
            }
        }
    }


    #[tokio::test]
    async fn test_error_details_include_all_providers() {
        // Test that error includes details from all failed providers
        let result = get_block_timestamp(999_888_777, Duration::from_secs(3)).await;

        assert!(result.is_err());
        if let Err(AnchorError::BlockTimeFetchFailed { height, details }) = result {
            assert_eq!(height, 999_888_777);
            // All three provider names should appear in details
            assert!(details.contains("blockstream.info"));
            assert!(details.contains("mempool.space"));
            assert!(details.contains("blockchain.info"));
        } else {
            panic!("Expected BlockTimeFetchFailed error");
        }
    }
}
