//! Integration tests for WAL performance and correctness

use atl_server::storage::high_throughput::wal::{WalEntry, WalRecovery, WalWriter};
use std::time::Instant;
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn test_wal_write_performance() {
    let dir = tempdir().unwrap();
    let mut writer = WalWriter::new(dir.path().to_path_buf()).unwrap();

    // Create 10,000 entries (typical batch size)
    let entries: Vec<WalEntry> = (0..10_000)
        .map(|i| WalEntry {
            id: Uuid::new_v4().into_bytes(),
            payload_hash: [(i % 256) as u8; 32],
            metadata_hash: [0u8; 32],
            metadata: vec![],
            external_id: vec![],
        })
        .collect();

    let start = Instant::now();
    let batch_id = writer.write_batch(&entries, 0).unwrap();
    let elapsed = start.elapsed();

    // AC7: Should complete in < 10ms on NVMe (we allow 100ms for CI environments)
    assert!(
        elapsed.as_millis() < 100,
        "WAL write took {:?}, expected < 100ms",
        elapsed
    );

    println!(
        "WAL write performance: {} entries in {:?} ({:.2} entries/ms)",
        entries.len(),
        elapsed,
        entries.len() as f64 / elapsed.as_millis().max(1) as f64
    );

    // Mark committed
    writer.mark_committed(batch_id).unwrap();

    // Verify recovery can read it back
    let recovery = WalRecovery::new(dir.path().to_path_buf());
    let result = recovery.scan().unwrap();

    assert_eq!(result.replay_needed.len(), 1);
    assert_eq!(result.replay_needed[0].entries.len(), 10_000);
}

#[test]
fn test_wal_full_lifecycle() {
    let dir = tempdir().unwrap();
    let mut writer = WalWriter::new(dir.path().to_path_buf()).unwrap();

    // Batch 1: Write and commit
    let entries1 = vec![
        WalEntry {
            id: [1u8; 16],
            payload_hash: [2u8; 32],
            metadata_hash: [3u8; 32],
            metadata: b"batch 1".to_vec(),
            external_id: b"ext1".to_vec(),
        },
        WalEntry {
            id: [2u8; 16],
            payload_hash: [3u8; 32],
            metadata_hash: [4u8; 32],
            metadata: b"batch 1 entry 2".to_vec(),
            external_id: b"ext2".to_vec(),
        },
    ];
    let batch_id1 = writer.write_batch(&entries1, 0).unwrap();
    writer.mark_committed(batch_id1).unwrap();

    // Batch 2: Write but DON'T commit (simulates crash)
    let entries2 = vec![WalEntry {
        id: [3u8; 16],
        payload_hash: [4u8; 32],
        metadata_hash: [5u8; 32],
        metadata: b"batch 2 uncommitted".to_vec(),
        external_id: b"ext3".to_vec(),
    }];
    let batch_id2 = writer.write_batch(&entries2, 2).unwrap();

    // Batch 3: Write and commit
    let entries3 = vec![WalEntry {
        id: [4u8; 16],
        payload_hash: [5u8; 32],
        metadata_hash: [6u8; 32],
        metadata: b"batch 3".to_vec(),
        external_id: b"ext4".to_vec(),
    }];
    let batch_id3 = writer.write_batch(&entries3, 3).unwrap();
    writer.mark_committed(batch_id3).unwrap();

    // Recovery
    let recovery = WalRecovery::new(dir.path().to_path_buf());
    let result = recovery.scan().unwrap();

    // Should replay batches 1 and 3
    assert_eq!(result.replay_needed.len(), 2);
    assert_eq!(result.replay_needed[0].batch_id, batch_id1);
    assert_eq!(result.replay_needed[1].batch_id, batch_id3);

    // Should discard batch 2
    assert_eq!(result.discarded.len(), 1);
    assert_eq!(result.discarded[0], batch_id2);

    // Tree size should be computed correctly
    // Batch 1: 0 + 2 = 2
    // Batch 2: discarded
    // Batch 3: 3 + 1 = 4 (but we don't include batch 2's size)
    assert_eq!(result.tree_size, 4);

    // Next batch ID should be after highest ID
    assert_eq!(result.next_batch_id, 4);
}

#[test]
fn test_wal_cleanup() {
    let dir = tempdir().unwrap();
    let mut writer = WalWriter::new(dir.path().to_path_buf()).unwrap();

    // Create 10 batches
    for _ in 0..10 {
        let entries = vec![WalEntry {
            id: Uuid::new_v4().into_bytes(),
            payload_hash: [1u8; 32],
            metadata_hash: [2u8; 32],
            metadata: vec![],
            external_id: vec![],
        }];
        let batch_id = writer.write_batch(&entries, 0).unwrap();
        writer.mark_committed(batch_id).unwrap();
    }

    // Cleanup keeping last 3
    writer.cleanup(3).unwrap();

    // Count remaining .done files
    let done_count = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal.done"))
        .count();

    assert_eq!(done_count, 3, "should keep exactly 3 committed files");
}

#[test]
fn test_wal_large_metadata() {
    let dir = tempdir().unwrap();
    let mut writer = WalWriter::new(dir.path().to_path_buf()).unwrap();

    // Create entry with large metadata
    let large_metadata = vec![0xAB; 10_000]; // 10KB metadata
    let large_external_id = vec![0xCD; 1_000]; // 1KB external ID

    let entries = vec![WalEntry {
        id: Uuid::new_v4().into_bytes(),
        payload_hash: [1u8; 32],
        metadata_hash: [2u8; 32],
        metadata: large_metadata.clone(),
        external_id: large_external_id.clone(),
    }];

    let batch_id = writer.write_batch(&entries, 0).unwrap();
    writer.mark_committed(batch_id).unwrap();

    // Recovery should correctly read large metadata
    let recovery = WalRecovery::new(dir.path().to_path_buf());
    let result = recovery.scan().unwrap();

    assert_eq!(result.replay_needed.len(), 1);
    let recovered_entry = &result.replay_needed[0].entries[0];
    assert_eq!(recovered_entry.metadata, large_metadata);
    assert_eq!(recovered_entry.external_id, large_external_id);
}
