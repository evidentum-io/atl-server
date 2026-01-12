//! Write-Ahead Log (WAL) implementation for durability
//!
//! The WAL ensures that once a batch is acknowledged, it survives crashes.
//! Key properties:
//! - One file per batch
//! - Single fsync per batch (not per entry)
//! - CRC32 checksums for corruption detection
//! - Commit flag for atomicity

mod format;
mod recovery;
mod writer;

pub use format::{
    WalEntry, WalHeader, WalTrailer, COMMIT_DONE, COMMIT_PENDING, WAL_MAGIC, WAL_VERSION,
};
pub use recovery::{RecoveryBatch, RecoveryResult, WalRecovery};
pub use writer::WalWriter;
