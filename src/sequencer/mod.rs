//! Sequencer module - micro-batching for high-throughput writes
//!
//! The Sequencer accumulates append requests in memory and flushes them
//! to storage in batches for massive performance gains.

mod batch;
mod buffer;
mod config;
mod core;
mod tsa;

pub use buffer::{AppendRequest, SequencerHandle};
pub use config::SequencerConfig;
pub use core::Sequencer;
