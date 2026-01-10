//! RFC 3161 Time-Stamp Protocol implementation
//!
//! Provides both synchronous and asynchronous TSA clients with fallback logic.

mod asn1;
mod async_client;
mod client;
mod service;
mod types;

pub use async_client::{AsyncRfc3161Client, TsaClient};
pub use client::Rfc3161Client;
pub use service::TsaService;
pub use types::{TsaAnchor, TsaConfig, TsaResponse};
