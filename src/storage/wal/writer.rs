//! WAL writer implementation
//!
//! Provides atomic batch writes with fsync guarantees.

use crate::storage::wal::format::{
    WalEntry, WalHeader, WalTrailer, COMMIT_DONE, COMMIT_PENDING,
};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// CRC32 algorithm for WAL entries
const CRC_ALGORITHM: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

/// Writes batch data to WAL files with durability guarantees
///
/// # File Naming
///
/// - Active: `wal/batch_{batch_id:08}.wal`
/// - Committed: `wal/batch_{batch_id:08}.wal.done`
pub struct WalWriter {
    /// WAL directory path
    wal_dir: PathBuf,

    /// Next batch ID to assign
    next_batch_id: u64,
}

impl WalWriter {
    /// Create new WAL writer
    ///
    /// Scans WAL directory to determine next batch ID.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if:
    /// - Directory creation fails
    /// - Directory listing fails
    pub fn new(wal_dir: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&wal_dir)?;

        // Scan directory to find highest batch ID
        let next_batch_id = Self::scan_next_batch_id(&wal_dir)?;

        Ok(Self {
            wal_dir,
            next_batch_id,
        })
    }

    /// Scan WAL directory to find next available batch ID
    fn scan_next_batch_id(wal_dir: &Path) -> io::Result<u64> {
        let mut max_batch_id = 0u64;

        for entry in fs::read_dir(wal_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            // Parse batch_{id}.wal or batch_{id}.wal.done
            if let Some(id_str) = name_str.strip_prefix("batch_") {
                let id_str = id_str.trim_end_matches(".wal").trim_end_matches(".done");
                if let Ok(batch_id) = id_str.parse::<u64>() {
                    max_batch_id = max_batch_id.max(batch_id);
                }
            }
        }

        Ok(max_batch_id + 1)
    }

    /// Write a batch to WAL and fsync
    ///
    /// Returns the batch_id for tracking. After this returns, data is durable.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if:
    /// - File creation fails
    /// - Write fails
    /// - fsync fails
    pub fn write_batch(&mut self, entries: &[WalEntry], tree_size_before: u64) -> io::Result<u64> {
        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;

        let path = self.batch_path(batch_id);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        let mut writer = BufWriter::new(file);

        // Get current timestamp
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as i64);

        // Write header
        let header = WalHeader::new(batch_id, entries.len() as u32, timestamp, tree_size_before);
        header.write_to(&mut writer)?;

        // Write entries and compute CRC
        let entries_crc = self.write_entries(&mut writer, entries)?;

        // Write trailer
        let trailer = WalTrailer::new(entries_crc);
        trailer.write_to(&mut writer)?;

        // Flush buffered data
        writer.flush()?;

        // CRITICAL: fsync for durability
        writer.into_inner()?.sync_all()?;

        Ok(batch_id)
    }

    /// Write entries and compute CRC32
    fn write_entries<W: Write>(&self, writer: &mut W, entries: &[WalEntry]) -> io::Result<u32> {
        let mut crc_writer = CrcWriter::new(writer);

        for entry in entries {
            entry.write_to(&mut crc_writer)?;
        }

        Ok(crc_writer.finish())
    }

    /// Mark batch as committed
    ///
    /// Sets commit flag to 0xFF. This is a single-byte atomic write.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if:
    /// - File open fails
    /// - Seek fails
    /// - Write fails
    /// - fsync fails
    pub fn mark_committed(&self, batch_id: u64) -> io::Result<()> {
        let path = self.batch_path(batch_id);
        let mut file = OpenOptions::new().write(true).open(&path)?;

        // Seek to commit flag position (last byte)
        file.seek(SeekFrom::End(-1))?;

        // Write commit flag
        file.write_all(&[COMMIT_DONE])?;

        // fsync to ensure durability
        file.sync_all()?;

        // Rename to .wal.done
        let done_path = self.batch_done_path(batch_id);
        fs::rename(&path, &done_path)?;

        Ok(())
    }

    /// Cleanup old committed WAL files
    ///
    /// Keeps last N committed files for debugging.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if directory listing or deletion fails
    pub fn cleanup(&self, keep_count: usize) -> io::Result<()> {
        let mut done_files = Vec::new();

        for entry in fs::read_dir(&self.wal_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str.ends_with(".wal.done") {
                if let Some(id_str) = name_str.strip_prefix("batch_") {
                    let id_str = id_str.trim_end_matches(".wal.done");
                    if let Ok(batch_id) = id_str.parse::<u64>() {
                        done_files.push((batch_id, entry.path()));
                    }
                }
            }
        }

        // Sort by batch_id (oldest first)
        done_files.sort_by_key(|(id, _)| *id);

        // Delete all but last N
        if done_files.len() > keep_count {
            let to_delete = &done_files[..done_files.len() - keep_count];
            for (_, path) in to_delete {
                fs::remove_file(path)?;
            }
        }

        Ok(())
    }

    /// Get next batch ID
    #[must_use]
    pub fn next_batch_id(&self) -> u64 {
        self.next_batch_id
    }

    /// Set next batch ID (used during recovery)
    pub fn set_next_batch_id(&mut self, batch_id: u64) {
        self.next_batch_id = batch_id;
    }

    /// Get WAL directory path
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.wal_dir
    }

    /// Get path for batch file
    fn batch_path(&self, batch_id: u64) -> PathBuf {
        self.wal_dir.join(format!("batch_{batch_id:08}.wal"))
    }

    /// Get path for committed batch file
    fn batch_done_path(&self, batch_id: u64) -> PathBuf {
        self.wal_dir.join(format!("batch_{batch_id:08}.wal.done"))
    }
}

/// Writer wrapper that computes CRC32 as data is written
struct CrcWriter<'a, W: Write> {
    writer: &'a mut W,
    crc: crc::Digest<'static, u32>,
}

impl<'a, W: Write> CrcWriter<'a, W> {
    fn new(writer: &'a mut W) -> Self {
        Self {
            writer,
            crc: CRC_ALGORITHM.digest(),
        }
    }

    fn finish(self) -> u32 {
        self.crc.finalize()
    }
}

impl<'a, W: Write> Write for CrcWriter<'a, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.writer.write(buf)?;
        self.crc.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_write_and_read_batch() {
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
                external_id: b"ext1".to_vec(),
            },
        ];

        let batch_id = writer.write_batch(&entries, 0).unwrap();
        assert_eq!(batch_id, 1, "first batch should have ID 1");

        // Verify file exists
        let path = dir.path().join("batch_00000001.wal");
        assert!(path.exists(), "WAL file should exist");

        // Verify next batch ID incremented
        assert_eq!(writer.next_batch_id(), 2);
    }

    #[test]
    fn test_mark_committed() {
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
        writer.mark_committed(batch_id).unwrap();

        // Verify renamed to .done
        let done_path = dir.path().join("batch_00000001.wal.done");
        assert!(done_path.exists(), "committed file should exist");

        // Read and verify commit flag
        let mut file = File::open(&done_path).unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        let mut flag = [0u8; 1];
        file.read_exact(&mut flag).unwrap();
        assert_eq!(flag[0], COMMIT_DONE);
    }

    #[test]
    fn test_cleanup_keeps_recent() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::new(dir.path().to_path_buf()).unwrap();

        // Create 5 batches and commit them
        for _ in 0..5 {
            let entries = vec![WalEntry {
                id: [1u8; 16],
                payload_hash: [2u8; 32],
                metadata_hash: [3u8; 32],
                metadata: vec![],
                external_id: vec![],
            }];
            let batch_id = writer.write_batch(&entries, 0).unwrap();
            writer.mark_committed(batch_id).unwrap();
        }

        // Cleanup keeping last 2
        writer.cleanup(2).unwrap();

        // Should have exactly 2 .done files
        let done_count = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().ends_with(".wal.done"))
            .count();

        assert_eq!(done_count, 2, "should keep exactly 2 committed files");
    }

    #[test]
    fn test_scan_next_batch_id() {
        let dir = tempdir().unwrap();

        // Create files with gaps
        File::create(dir.path().join("batch_00000003.wal")).unwrap();
        File::create(dir.path().join("batch_00000007.wal.done")).unwrap();
        File::create(dir.path().join("batch_00000005.wal")).unwrap();

        let writer = WalWriter::new(dir.path().to_path_buf()).unwrap();
        assert_eq!(writer.next_batch_id(), 8, "should start after highest ID");
    }
}
