//! WAL file format definitions and serialization
//!
//! This module defines the binary format for WAL files:
//! - Header (29 bytes): magic, version, batch metadata
//! - Entries (variable): serialized log entries
//! - Trailer (5 bytes): entries CRC and commit flag

use std::io::{self, Read, Write};

/// WAL file magic bytes "ATLW"
pub const WAL_MAGIC: [u8; 4] = *b"ATLW";

/// Current WAL format version
pub const WAL_VERSION: u8 = 1;

/// Commit flag: batch is still being processed
pub const COMMIT_PENDING: u8 = 0x00;

/// Commit flag: batch has been fully committed to SQLite
pub const COMMIT_DONE: u8 = 0xFF;

/// WAL file header (fixed size: 29 bytes)
///
/// Layout:
/// - magic (4 bytes)
/// - version (1 byte)
/// - batch_id (8 bytes, big-endian)
/// - entry_count (4 bytes, big-endian)
/// - timestamp (8 bytes, big-endian signed)
/// - tree_size_before (8 bytes, big-endian)
/// - header_crc (4 bytes, big-endian)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalHeader {
    /// Magic bytes: "ATLW"
    pub magic: [u8; 4],

    /// Format version
    pub version: u8,

    /// Batch ID (sequential)
    pub batch_id: u64,

    /// Number of entries in batch
    pub entry_count: u32,

    /// Timestamp when batch started (Unix nanos)
    pub timestamp: i64,

    /// Tree size BEFORE this batch
    pub tree_size_before: u64,

    /// CRC32 of header fields (excluding this field)
    pub header_crc: u32,
}

impl WalHeader {
    /// Header size in bytes
    pub const SIZE: usize = 29;

    /// Create new header with computed CRC
    #[must_use]
    pub fn new(batch_id: u64, entry_count: u32, timestamp: i64, tree_size_before: u64) -> Self {
        let mut header = Self {
            magic: WAL_MAGIC,
            version: WAL_VERSION,
            batch_id,
            entry_count,
            timestamp,
            tree_size_before,
            header_crc: 0,
        };

        // Compute CRC over all fields except header_crc
        header.header_crc = header.compute_crc();
        header
    }

    /// Compute CRC32 of header fields (excluding header_crc)
    #[must_use]
    pub fn compute_crc(&self) -> u32 {
        let mut buf = Vec::with_capacity(Self::SIZE - 4);
        buf.extend_from_slice(&self.magic);
        buf.push(self.version);
        buf.extend_from_slice(&self.batch_id.to_be_bytes());
        buf.extend_from_slice(&self.entry_count.to_be_bytes());
        buf.extend_from_slice(&self.timestamp.to_be_bytes());
        buf.extend_from_slice(&self.tree_size_before.to_be_bytes());

        crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(&buf)
    }

    /// Validate header CRC
    #[must_use]
    pub fn validate(&self) -> bool {
        self.header_crc == self.compute_crc()
    }

    /// Serialize header to writer
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if write fails
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.magic)?;
        writer.write_all(&[self.version])?;
        writer.write_all(&self.batch_id.to_be_bytes())?;
        writer.write_all(&self.entry_count.to_be_bytes())?;
        writer.write_all(&self.timestamp.to_be_bytes())?;
        writer.write_all(&self.tree_size_before.to_be_bytes())?;
        writer.write_all(&self.header_crc.to_be_bytes())?;
        Ok(())
    }

    /// Deserialize header from reader
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if read fails or format is invalid
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;

        let mut version = [0u8; 1];
        reader.read_exact(&mut version)?;

        let mut batch_id_bytes = [0u8; 8];
        reader.read_exact(&mut batch_id_bytes)?;
        let batch_id = u64::from_be_bytes(batch_id_bytes);

        let mut entry_count_bytes = [0u8; 4];
        reader.read_exact(&mut entry_count_bytes)?;
        let entry_count = u32::from_be_bytes(entry_count_bytes);

        let mut timestamp_bytes = [0u8; 8];
        reader.read_exact(&mut timestamp_bytes)?;
        let timestamp = i64::from_be_bytes(timestamp_bytes);

        let mut tree_size_bytes = [0u8; 8];
        reader.read_exact(&mut tree_size_bytes)?;
        let tree_size_before = u64::from_be_bytes(tree_size_bytes);

        let mut header_crc_bytes = [0u8; 4];
        reader.read_exact(&mut header_crc_bytes)?;
        let header_crc = u32::from_be_bytes(header_crc_bytes);

        Ok(Self {
            magic,
            version: version[0],
            batch_id,
            entry_count,
            timestamp,
            tree_size_before,
            header_crc,
        })
    }
}

/// WAL entry (variable size)
///
/// Layout:
/// - id (16 bytes)
/// - payload_hash (32 bytes)
/// - metadata_hash (32 bytes)
/// - metadata_len (2 bytes, big-endian)
/// - metadata (variable)
/// - external_id_len (2 bytes, big-endian)
/// - external_id (variable)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    /// Entry UUID (16 bytes)
    pub id: [u8; 16],

    /// Payload hash (32 bytes)
    pub payload_hash: [u8; 32],

    /// Metadata hash (32 bytes)
    pub metadata_hash: [u8; 32],

    /// Metadata cleartext (variable length)
    pub metadata: Vec<u8>,

    /// External ID (variable length)
    pub external_id: Vec<u8>,
}

impl WalEntry {
    /// Minimum entry size (without variable data)
    pub const MIN_SIZE: usize = 16 + 32 + 32 + 2 + 2;

    /// Calculate serialized size
    #[must_use]
    pub fn serialized_size(&self) -> usize {
        Self::MIN_SIZE + self.metadata.len() + self.external_id.len()
    }

    /// Serialize entry to writer
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if write fails or data is too large
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        if self.metadata.len() > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "metadata too large",
            ));
        }
        if self.external_id.len() > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "external_id too large",
            ));
        }

        writer.write_all(&self.id)?;
        writer.write_all(&self.payload_hash)?;
        writer.write_all(&self.metadata_hash)?;
        writer.write_all(&(self.metadata.len() as u16).to_be_bytes())?;
        writer.write_all(&self.metadata)?;
        writer.write_all(&(self.external_id.len() as u16).to_be_bytes())?;
        writer.write_all(&self.external_id)?;
        Ok(())
    }

    /// Deserialize entry from reader
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if read fails
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut id = [0u8; 16];
        reader.read_exact(&mut id)?;

        let mut payload_hash = [0u8; 32];
        reader.read_exact(&mut payload_hash)?;

        let mut metadata_hash = [0u8; 32];
        reader.read_exact(&mut metadata_hash)?;

        let mut metadata_len_bytes = [0u8; 2];
        reader.read_exact(&mut metadata_len_bytes)?;
        let metadata_len = u16::from_be_bytes(metadata_len_bytes) as usize;

        let mut metadata = vec![0u8; metadata_len];
        reader.read_exact(&mut metadata)?;

        let mut external_id_len_bytes = [0u8; 2];
        reader.read_exact(&mut external_id_len_bytes)?;
        let external_id_len = u16::from_be_bytes(external_id_len_bytes) as usize;

        let mut external_id = vec![0u8; external_id_len];
        reader.read_exact(&mut external_id)?;

        Ok(Self {
            id,
            payload_hash,
            metadata_hash,
            metadata,
            external_id,
        })
    }
}

/// WAL trailer (fixed size: 5 bytes)
///
/// Layout:
/// - entries_crc (4 bytes, big-endian)
/// - commit_flag (1 byte)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalTrailer {
    /// CRC32 of all entries
    pub entries_crc: u32,

    /// Commit flag: 0x00 = pending, 0xFF = committed
    pub commit_flag: u8,
}

impl WalTrailer {
    /// Trailer size in bytes
    pub const SIZE: usize = 5;

    /// Create new trailer with pending commit flag
    #[must_use]
    pub fn new(entries_crc: u32) -> Self {
        Self {
            entries_crc,
            commit_flag: COMMIT_PENDING,
        }
    }

    /// Serialize trailer to writer
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if write fails
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.entries_crc.to_be_bytes())?;
        writer.write_all(&[self.commit_flag])?;
        Ok(())
    }

    /// Deserialize trailer from reader
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if read fails
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut entries_crc_bytes = [0u8; 4];
        reader.read_exact(&mut entries_crc_bytes)?;
        let entries_crc = u32::from_be_bytes(entries_crc_bytes);

        let mut commit_flag = [0u8; 1];
        reader.read_exact(&mut commit_flag)?;

        Ok(Self {
            entries_crc,
            commit_flag: commit_flag[0],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_crc() {
        let header = WalHeader::new(1, 100, 1_234_567_890, 5000);
        assert!(header.validate(), "header CRC should be valid");

        let mut corrupted = header.clone();
        corrupted.batch_id = 2;
        assert!(
            !corrupted.validate(),
            "corrupted header should fail validation"
        );
    }

    #[test]
    fn test_header_serialization() {
        let header = WalHeader::new(42, 1000, 1_234_567_890, 9999);

        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let deserialized = WalHeader::read_from(&mut cursor).unwrap();

        assert_eq!(header, deserialized);
        assert!(deserialized.validate());
    }

    #[test]
    fn test_entry_serialization() {
        let entry = WalEntry {
            id: [1u8; 16],
            payload_hash: [2u8; 32],
            metadata_hash: [3u8; 32],
            metadata: b"test metadata".to_vec(),
            external_id: b"ext123".to_vec(),
        };

        let mut buf = Vec::new();
        entry.write_to(&mut buf).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let deserialized = WalEntry::read_from(&mut cursor).unwrap();

        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_trailer_serialization() {
        let trailer = WalTrailer::new(0x1234_5678);

        let mut buf = Vec::new();
        trailer.write_to(&mut buf).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let deserialized = WalTrailer::read_from(&mut cursor).unwrap();

        assert_eq!(trailer, deserialized);
        assert_eq!(deserialized.commit_flag, COMMIT_PENDING);
    }
}
