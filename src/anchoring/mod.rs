//! Anchoring implementations

#[cfg(feature = "rfc3161")]
mod rfc3161;

#[cfg(feature = "ots")]
mod ots;

#[cfg(feature = "rfc3161")]
pub use rfc3161::*;

#[cfg(feature = "ots")]
pub use ots::*;
