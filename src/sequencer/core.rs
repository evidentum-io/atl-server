//! Sequencer core logic - batch accumulation and flushing

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{Instant, interval};
use tracing::{debug, info};

use crate::traits::Storage;

use super::buffer::{AppendRequest, SequencerHandle};
use super::config::SequencerConfig;

/// The Sequencer accumulates append requests and flushes them in batches
pub struct Sequencer {
    /// Storage backend
    storage: Arc<dyn Storage>,

    /// Configuration
    config: SequencerConfig,

    /// Receiver for incoming requests
    rx: mpsc::Receiver<AppendRequest>,
}

impl Sequencer {
    /// Create a new Sequencer and return its handle
    pub fn new(storage: Arc<dyn Storage>, config: SequencerConfig) -> (Self, SequencerHandle) {
        let (tx, rx) = mpsc::channel(config.buffer_size);

        let sequencer = Self {
            storage,
            config,
            rx,
        };

        let handle = SequencerHandle::new(tx);

        (sequencer, handle)
    }

    /// Run the Sequencer loop (spawn as tokio task)
    pub async fn run(mut self) {
        info!(
            batch_size = self.config.batch_size,
            batch_timeout_ms = self.config.batch_timeout_ms,
            buffer_size = self.config.buffer_size,
            "Sequencer started"
        );

        let mut batch: Vec<AppendRequest> = Vec::with_capacity(self.config.batch_size);
        let mut flush_interval = interval(Duration::from_millis(self.config.batch_timeout_ms));
        let mut batch_start = Instant::now();

        loop {
            tokio::select! {
                // Receive new request
                request = self.rx.recv() => {
                    match request {
                        Some(req) => {
                            if batch.is_empty() {
                                batch_start = Instant::now();
                            }
                            batch.push(req);

                            // Flush if batch is full
                            if batch.len() >= self.config.batch_size {
                                debug!(
                                    batch_size = batch.len(),
                                    trigger = "size",
                                    "Flushing batch"
                                );
                                super::batch::flush_batch(
                                    &mut batch,
                                    &self.storage,
                                    &self.config,
                                )
                                .await;
                            }
                        }
                        None => {
                            // Channel closed, flush remaining and exit
                            if !batch.is_empty() {
                                info!(
                                    batch_size = batch.len(),
                                    "Flushing final batch before shutdown"
                                );
                                super::batch::flush_batch(
                                    &mut batch,
                                    &self.storage,
                                    &self.config,
                                )
                                .await;
                            }
                            info!("Sequencer shutting down");
                            return;
                        }
                    }
                }

                // Timeout tick
                _ = flush_interval.tick() => {
                    if !batch.is_empty() {
                        let elapsed = batch_start.elapsed().as_millis();
                        debug!(
                            batch_size = batch.len(),
                            elapsed_ms = elapsed,
                            trigger = "timeout",
                            "Flushing batch"
                        );
                        super::batch::flush_batch(
                            &mut batch,
                            &self.storage,
                            &self.config,
                        )
                        .await;
                    }
                }
            }
        }
    }
}
