//! Sequencer module - micro-batching for high-throughput writes
//!
//! The Sequencer accumulates append requests in memory and flushes them
//! to storage in batches for massive performance gains.

mod batch;
mod buffer;
mod config;
mod core;

pub use buffer::SequencerHandle;

// AppendRequest is used only internally by the sequencer
#[allow(unused_imports)]
pub(crate) use buffer::AppendRequest;
pub use config::SequencerConfig;
pub use core::Sequencer;
