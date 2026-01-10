// File: src/background/ots_poll_job/upgrade.rs

use crate::error::ServerResult;
use crate::storage::AnchorWithId;

#[cfg(feature = "sqlite")]
use crate::storage::SqliteStore;

/// Check if anchor is confirmed and update if so
///
/// Returns true if anchor was upgraded to confirmed status.
#[cfg(feature = "sqlite")]
pub async fn check_and_upgrade(
    anchor: &AnchorWithId,
    _storage: &SqliteStore,
) -> ServerResult<bool> {
    // For now, return stub implementation
    // ANCHOR-1 spec will implement the actual OTS upgrade logic
    tracing::debug!(
        anchor_id = anchor.id,
        "OTS upgrade not implemented yet (ANCHOR-1 pending), skipping"
    );

    // In production, this would:
    // 1. Parse existing OTS proof
    // 2. Try to upgrade via calendar
    // 3. Check if Bitcoin attestation is present
    // 4. Update anchor token, status, and metadata
    // 5. Mark tree as closed

    Ok(false)
}

/// Stub: Parse OTS proof and check Bitcoin confirmation status
///
/// Returns (is_confirmed, block_height, block_time) if confirmed.
/// This is a placeholder until ANCHOR-1 implements actual OTS parsing.
#[allow(dead_code)]
fn check_ots_confirmation(_proof: &[u8]) -> Option<(u64, u64)> {
    // Stub: would parse OTS proof and check for Bitcoin attestation
    None
}

/// Stub: Upgrade OTS proof via calendar servers
///
/// Returns upgraded proof with Bitcoin attestation if available.
/// This is a placeholder until ANCHOR-1 implements actual OTS calendar protocol.
#[allow(dead_code)]
async fn upgrade_ots_proof(_proof: &[u8]) -> ServerResult<Option<Vec<u8>>> {
    // Stub: would contact OTS calendar servers to upgrade proof
    Ok(None)
}
