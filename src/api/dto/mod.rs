//! Data Transfer Objects (DTOs)

mod request;
mod response;

pub use request::AnchorJsonRequest;
#[allow(unused_imports)]
pub use response::{
    BitcoinAnchorDto, ConsistencyProofDto, ErrorResponse, HealthResponse, TsaAnchorDto,
};
