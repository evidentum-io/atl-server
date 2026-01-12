//! WAL recovery after crashes
//!
//! Implements the recovery decision tree:
//! - Validate CRC checksums
//! - Check commit flags
//! - Determine which batches need replay

use crate::storage::high_throughput::wal::format::{WalEntry, WalHeader, WalTrailer, COMMIT_DONE};
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// CRC32 algorithm for WAL entries
const CRC_ALGORITHM: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

/// WAL recovery result
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    /// Batches that need replay to SQLite
    pub replay_needed: Vec<RecoveryBatch>,

    /// Batches that were discarded (corrupted or uncommitted)
    pub discarded: Vec<u64>,

    /// Next batch ID to use
    pub next_batch_id: u64,

    /// Tree size after recovery (from last valid batch)
    pub tree_size: u64,
}

/// A batch that needs replay
#[derive(Debug, Clone)]
pub struct RecoveryBatch {
    /// Batch ID
    pub batch_id: u64,

    /// Entries in batch
    pub entries: Vec<WalEntry>,

    /// Tree size before this batch
    pub tree_size_before: u64,
}

/// Recovers WAL state after crash
pub struct WalRecovery {
    /// WAL directory path
    wal_dir: PathBuf,
}

impl WalRecovery {
    /// Create recovery handler
    #[must_use]
    pub fn new(wal_dir: PathBuf) -> Self {
        Self { wal_dir }
    }

    /// Scan WAL directory and determine recovery actions
    ///
    /// Implements the recovery decision tree from spec.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if directory listing fails
    pub fn scan(&self) -> io::Result<RecoveryResult> {
        let mut replay_needed = Vec::new();
        let mut discarded = Vec::new();
        let mut max_batch_id = 0u64;
        let mut tree_size = 0u64;

        // Collect all WAL files (both .wal and .wal.done)
        let mut wal_files = self.collect_wal_files()?;

        // Sort by batch_id
        wal_files.sort_by_key(|(id, _)| *id);

        for (batch_id, path) in wal_files {
            max_batch_id = max_batch_id.max(batch_id);

            match self.read_wal_file(&path) {
                Ok(Some(batch)) => {
                    // Valid batch, update tree size
                    tree_size = batch.tree_size_before + batch.entries.len() as u64;

                    // Check if committed
                    if self.is_committed(&path)? {
                        // Committed batch - needs replay (we don't check SQLite here)
                        replay_needed.push(batch);
                    } else {
                        // Uncommitted batch - discard
                        discarded.push(batch_id);
                        fs::remove_file(&path)?;
                    }
                }
                Ok(None) => {
                    // Invalid/corrupted batch
                    discarded.push(batch_id);
                    fs::remove_file(&path)?;
                }
                Err(e) => {
                    tracing::warn!("failed to read WAL file {:?}: {}", path, e);
                    discarded.push(batch_id);
                    // Try to delete corrupted file
                    let _ = fs::remove_file(&path);
                }
            }
        }

        Ok(RecoveryResult {
            replay_needed,
            discarded,
            next_batch_id: max_batch_id + 1,
            tree_size,
        })
    }

    /// Collect all WAL files from directory
    fn collect_wal_files(&self) -> io::Result<Vec<(u64, PathBuf)>> {
        let mut files = Vec::new();

        for entry in fs::read_dir(&self.wal_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            // Parse batch_{id}.wal or batch_{id}.wal.done
            if name_str.starts_with("batch_")
                && (name_str.ends_with(".wal") || name_str.ends_with(".wal.done"))
            {
                if let Some(id_str) = name_str.strip_prefix("batch_") {
                    let id_str = id_str.trim_end_matches(".wal").trim_end_matches(".done");
                    if let Ok(batch_id) = id_str.parse::<u64>() {
                        files.push((batch_id, entry.path()));
                    }
                }
            }
        }

        Ok(files)
    }

    /// Check if WAL file is committed
    fn is_committed(&self, path: &Path) -> io::Result<bool> {
        // .wal.done files are always committed
        if path.extension().and_then(|s| s.to_str()) == Some("done") {
            return Ok(true);
        }

        // Read trailer and check commit flag
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();

        if file_size < WalTrailer::SIZE as u64 {
            return Ok(false);
        }

        // Read last byte (commit flag)
        let mut trailer_buf = vec![0u8; WalTrailer::SIZE];
        let mut reader = BufReader::new(file);
        reader
            .get_mut()
            .seek(SeekFrom::Start(file_size - WalTrailer::SIZE as u64))?;
        reader.read_exact(&mut trailer_buf)?;

        let trailer = WalTrailer::read_from(&mut &trailer_buf[..])?;
        Ok(trailer.commit_flag == COMMIT_DONE)
    }

    /// Read and validate a single WAL file
    ///
    /// Returns `None` if file is corrupted.
    fn read_wal_file(&self, path: &Path) -> io::Result<Option<RecoveryBatch>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read header
        let header = match WalHeader::read_from(&mut reader) {
            Ok(h) => h,
            Err(_) => return Ok(None),
        };

        // Validate header CRC
        if !header.validate() {
            return Ok(None);
        }

        // Read entries
        let mut entries = Vec::with_capacity(header.entry_count as usize);
        let mut entries_crc = CRC_ALGORITHM.digest();

        for _ in 0..header.entry_count {
            let mut entry_buf = Vec::new();

            // Read entry while computing CRC
            let entry = match self.read_entry_with_crc(&mut reader, &mut entry_buf) {
                Ok(e) => e,
                Err(_) => return Ok(None),
            };

            entries_crc.update(&entry_buf);
            entries.push(entry);
        }

        let computed_entries_crc = entries_crc.finalize();

        // Read trailer
        let trailer = match WalTrailer::read_from(&mut reader) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };

        // Validate entries CRC
        if trailer.entries_crc != computed_entries_crc {
            return Ok(None);
        }

        Ok(Some(RecoveryBatch {
            batch_id: header.batch_id,
            entries,
            tree_size_before: header.tree_size_before,
        }))
    }

    /// Read entry while capturing raw bytes for CRC
    fn read_entry_with_crc<R: Read>(
        &self,
        reader: &mut R,
        buf: &mut Vec<u8>,
    ) -> io::Result<WalEntry> {
        // Read fixed fields
        let mut id = [0u8; 16];
        reader.read_exact(&mut id)?;
        buf.extend_from_slice(&id);

        let mut payload_hash = [0u8; 32];
        reader.read_exact(&mut payload_hash)?;
        buf.extend_from_slice(&payload_hash);

        let mut metadata_hash = [0u8; 32];
        reader.read_exact(&mut metadata_hash)?;
        buf.extend_from_slice(&metadata_hash);

        let mut metadata_len_bytes = [0u8; 2];
        reader.read_exact(&mut metadata_len_bytes)?;
        buf.extend_from_slice(&metadata_len_bytes);
        let metadata_len = u16::from_be_bytes(metadata_len_bytes) as usize;

        let mut metadata = vec![0u8; metadata_len];
        reader.read_exact(&mut metadata)?;
        buf.extend_from_slice(&metadata);

        let mut external_id_len_bytes = [0u8; 2];
        reader.read_exact(&mut external_id_len_bytes)?;
        buf.extend_from_slice(&external_id_len_bytes);
        let external_id_len = u16::from_be_bytes(external_id_len_bytes) as usize;

        let mut external_id = vec![0u8; external_id_len];
        reader.read_exact(&mut external_id)?;
        buf.extend_from_slice(&external_id);

        Ok(WalEntry {
            id,
            payload_hash,
            metadata_hash,
            metadata,
            external_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::high_throughput::wal::writer::WalWriter;
    use tempfile::tempdir;

    #[test]
    fn test_recovery_committed_batch() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(dir.path().to_path_buf()).unwrap();

        let entries = vec![
            WalEntry {
                id: [1u8; 16],
                payload_hash: [2u8; 32],
                metadata_hash: [3u8; 32],
                metadata: vec![],
                external_id: vec![],
            },
            WalEntry {
                id: [2u8; 16],
                payload_hash: [3u8; 32],
                metadata_hash: [4u8; 32],
                metadata: b"test".to_vec(),
                external_id: b"ext".to_vec(),
            },
        ];

        let batch_id = writer.write_batch(&entries, 100).unwrap();
        writer.mark_committed(batch_id).unwrap();

        // Simulate crash and recovery
        let recovery = WalRecovery::new(dir.path().to_path_buf());
        let result = recovery.scan().unwrap();

        // Committed batch should be in replay_needed
        assert_eq!(result.replay_needed.len(), 1);
        assert_eq!(result.discarded.len(), 0);
        assert_eq!(result.replay_needed[0].batch_id, batch_id);
        assert_eq!(result.replay_needed[0].entries.len(), 2);
        assert_eq!(result.tree_size, 102); // 100 + 2 entries
    }

    #[test]
    fn test_recovery_uncommitted_batch() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(dir.path().to_path_buf()).unwrap();

        let entries = vec![WalEntry {
            id: [1u8; 16],
            payload_hash: [2u8; 32],
            metadata_hash: [3u8; 32],
            metadata: vec![],
            external_id: vec![],
        }];

        let batch_id = writer.write_batch(&entries, 0).unwrap();
        // Note: NOT marking committed

        let recovery = WalRecovery::new(dir.path().to_path_buf());
        let result = recovery.scan().unwrap();

        // Uncommitted batch should be discarded
        assert_eq!(result.discarded.len(), 1);
        assert_eq!(result.discarded[0], batch_id);
        assert_eq!(result.replay_needed.len(), 0);

        // File should be deleted
        let path = dir.path().join(format!("batch_{batch_id:08}.wal"));
        assert!(!path.exists());
    }

    #[test]
    fn test_recovery_corrupted_header() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(dir.path().to_path_buf()).unwrap();

        let entries = vec![WalEntry {
            id: [1u8; 16],
            payload_hash: [2u8; 32],
            metadata_hash: [3u8; 32],
            metadata: vec![],
            external_id: vec![],
        }];

        let batch_id = writer.write_batch(&entries, 0).unwrap();

        // Corrupt the header
        let path = dir.path().join(format!("batch_{batch_id:08}.wal"));
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        use std::io::Write;
        file.write_all(&[0xFF; 10]).unwrap();

        let recovery = WalRecovery::new(dir.path().to_path_buf());
        let result = recovery.scan().unwrap();

        // Corrupted batch should be discarded
        assert_eq!(result.discarded.len(), 1);
        assert_eq!(result.replay_needed.len(), 0);
    }

    #[test]
    fn test_recovery_multiple_batches() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(dir.path().to_path_buf()).unwrap();

        // Batch 1: committed
        let entries1 = vec![WalEntry {
            id: [1u8; 16],
            payload_hash: [2u8; 32],
            metadata_hash: [3u8; 32],
            metadata: vec![],
            external_id: vec![],
        }];
        let batch_id1 = writer.write_batch(&entries1, 0).unwrap();
        writer.mark_committed(batch_id1).unwrap();

        // Batch 2: uncommitted
        let entries2 = vec![WalEntry {
            id: [2u8; 16],
            payload_hash: [3u8; 32],
            metadata_hash: [4u8; 32],
            metadata: vec![],
            external_id: vec![],
        }];
        let batch_id2 = writer.write_batch(&entries2, 1).unwrap();

        // Batch 3: committed
        let entries3 = vec![WalEntry {
            id: [3u8; 16],
            payload_hash: [4u8; 32],
            metadata_hash: [5u8; 32],
            metadata: vec![],
            external_id: vec![],
        }];
        let batch_id3 = writer.write_batch(&entries3, 2).unwrap();
        writer.mark_committed(batch_id3).unwrap();

        let recovery = WalRecovery::new(dir.path().to_path_buf());
        let result = recovery.scan().unwrap();

        // Should have 2 committed batches to replay
        assert_eq!(result.replay_needed.len(), 2);
        assert_eq!(result.replay_needed[0].batch_id, batch_id1);
        assert_eq!(result.replay_needed[1].batch_id, batch_id3);

        // Should discard 1 uncommitted batch
        assert_eq!(result.discarded.len(), 1);
        assert_eq!(result.discarded[0], batch_id2);
    }
}
