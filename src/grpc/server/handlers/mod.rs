//! Handler modules for gRPC service methods
//!
//! Each handler file implements a logical group of gRPC methods:
//! - anchor: AnchorEntry, AnchorBatch
//! - receipt: GetReceipt, UpgradeReceipt
//! - tree: GetTreeHead, GetPublicKeys
//! - health: HealthCheck, TriggerAnchoring

pub mod anchor;
pub mod health;
pub mod receipt;
pub mod tree;
