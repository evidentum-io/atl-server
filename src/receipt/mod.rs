//! Receipt generation module
//!
//! This module provides receipt generation functionality for atl-server.
//! It bridges atl-core (receipt structures) with Storage (data access).

mod consistency;
mod convert;
mod format;
mod generator;
mod options;
mod upgrade;

// Re-export public API
#[allow(unused_imports)]
pub use convert::convert_anchor_to_receipt;
#[allow(unused_imports)]
pub use generator::{
    CheckpointSigner, ReceiptGenerator, generate_receipt, generate_receipt_simple,
};
#[allow(unused_imports)]
pub use options::ReceiptOptions;
#[allow(unused_imports)]
pub use upgrade::{UpgradeResult, upgrade_receipt};
