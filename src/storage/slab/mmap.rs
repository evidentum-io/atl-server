//! Memory-mapped slab file operations
//!
//! Provides safe abstractions over mmap'd slab files for storing Merkle tree nodes.

use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use super::format::{SlabHeader, NODE_SIZE, SLAB_MAGIC, SLAB_VERSION};

/// A single memory-mapped slab file
///
/// Each slab stores a complete binary tree with fixed capacity.
/// Nodes are stored level-by-level, from leaves (level 0) to root.
pub struct SlabFile {
    /// Memory-mapped region (read/write)
    mmap: MmapMut,

    /// Slab ID (sequential identifier)
    slab_id: u32,

    /// First leaf index in global tree
    start_index: u64,

    /// Maximum leaves this slab can hold
    max_leaves: u32,

    /// Current number of leaves written
    leaf_count: u32,
}

impl SlabFile {
    /// Create a new slab file
    ///
    /// Initializes the file with the correct size and writes the header.
    ///
    /// # Arguments
    /// * `path` - File path for the new slab
    /// * `slab_id` - Sequential slab identifier
    /// * `start_index` - First leaf index in global tree
    /// * `max_leaves` - Maximum capacity of this slab
    ///
    /// # Errors
    /// Returns IO error if file creation or mmap fails
    pub fn create(
        path: &Path,
        slab_id: u32,
        start_index: u64,
        max_leaves: u32,
    ) -> io::Result<Self> {
        let file_size = SlabHeader::file_size(max_leaves);

        // Create file with correct size
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(file_size as u64)?;

        // Memory-map the file
        let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        // Write header
        let header = SlabHeader::new(slab_id, start_index, max_leaves);
        let header_bytes = unsafe {
            std::slice::from_raw_parts(&header as *const SlabHeader as *const u8, SlabHeader::SIZE)
        };
        mmap[0..SlabHeader::SIZE].copy_from_slice(header_bytes);

        // Zero out all node regions
        mmap[SlabHeader::SIZE..].fill(0);

        // Flush to disk
        mmap.flush()?;

        Ok(Self {
            mmap,
            slab_id,
            start_index,
            max_leaves,
            leaf_count: 0,
        })
    }

    /// Open an existing slab file
    ///
    /// Reads and validates the header.
    ///
    /// # Arguments
    /// * `path` - Path to existing slab file
    ///
    /// # Errors
    /// Returns IO error if file doesn't exist, can't be mapped, or header is invalid
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        // Memory-map the file
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        // Read header
        let header = Self::read_header(&mmap)?;

        Ok(Self {
            mmap,
            slab_id: header.slab_id,
            start_index: header.start_index,
            max_leaves: header.max_leaves,
            leaf_count: header.leaf_count,
        })
    }

    /// Get node hash at (level, index)
    ///
    /// Returns None if the node hasn't been written yet or is out of bounds.
    ///
    /// # Arguments
    /// * `level` - Tree level (0 = leaves)
    /// * `index` - Node index within that level
    ///
    /// # Returns
    /// 32-byte hash if node exists, None otherwise
    pub fn get_node(&self, level: u32, index: u64) -> Option<[u8; 32]> {
        // Check bounds
        let nodes_at_level = SlabHeader::nodes_at_level(level, self.max_leaves);
        if index >= nodes_at_level as u64 {
            return None;
        }

        // Check if this node has been written
        // For level 0, check against leaf_count
        // For higher levels, we compute nodes bottom-up, so they exist if their children exist
        if level == 0 && index >= self.leaf_count as u64 {
            return None;
        }

        let offset = self.node_offset(level, index);
        if offset + NODE_SIZE > self.mmap.len() {
            return None;
        }

        let mut hash = [0u8; 32];
        hash.copy_from_slice(&self.mmap[offset..offset + NODE_SIZE]);

        // Check if it's a zero hash (not written yet)
        if hash == [0u8; 32] {
            return None;
        }

        Some(hash)
    }

    /// Set node hash at (level, index)
    ///
    /// # Arguments
    /// * `level` - Tree level (0 = leaves)
    /// * `index` - Node index within that level
    /// * `hash` - 32-byte hash to write
    ///
    /// # Panics
    /// Panics if index is out of bounds
    pub fn set_node(&mut self, level: u32, index: u64, hash: &[u8; 32]) {
        let offset = self.node_offset(level, index);
        assert!(
            offset + NODE_SIZE <= self.mmap.len(),
            "node offset out of bounds"
        );

        self.mmap[offset..offset + NODE_SIZE].copy_from_slice(hash);

        // Update leaf count if writing to level 0
        if level == 0 && index >= self.leaf_count as u64 {
            self.leaf_count = (index + 1) as u32;
            self.update_header();
        }
    }

    /// Get current leaf count
    #[must_use]
    pub fn leaf_count(&self) -> u32 {
        self.leaf_count
    }

    /// Check if slab is full
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.leaf_count >= self.max_leaves
    }

    /// Get slab ID
    #[must_use]
    pub fn slab_id(&self) -> u32 {
        self.slab_id
    }

    /// Get start index (first leaf index in global tree)
    #[must_use]
    pub fn start_index(&self) -> u64 {
        self.start_index
    }

    /// Get maximum capacity
    #[must_use]
    pub fn max_leaves(&self) -> u32 {
        self.max_leaves
    }

    /// Flush changes to disk
    ///
    /// Ensures all writes are durable.
    ///
    /// # Errors
    /// Returns IO error if flush fails
    pub fn flush(&self) -> io::Result<()> {
        self.mmap.flush()
    }

    /// Calculate byte offset for node at (level, index)
    ///
    /// # Arguments
    /// * `level` - Tree level (0 = leaves)
    /// * `index` - Node index within that level
    ///
    /// # Returns
    /// Byte offset from start of file
    fn node_offset(&self, level: u32, index: u64) -> usize {
        let level_start = SlabHeader::level_offset(level, self.max_leaves);
        level_start + (index as usize) * NODE_SIZE
    }

    /// Read and validate header from mmap
    fn read_header(mmap: &[u8]) -> io::Result<SlabHeader> {
        if mmap.len() < SlabHeader::SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file too small for header",
            ));
        }

        let header = unsafe { std::ptr::read_unaligned(mmap.as_ptr() as *const SlabHeader) };

        if header.magic != SLAB_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid magic: expected {:?}, got {:?}",
                    SLAB_MAGIC, header.magic
                ),
            ));
        }

        if header.version != SLAB_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported version: {}, expected {}",
                    header.version, SLAB_VERSION
                ),
            ));
        }

        if !header.validate() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header CRC validation failed",
            ));
        }

        Ok(header)
    }

    /// Update header in mmap with current leaf count
    fn update_header(&mut self) {
        let mut header = SlabHeader::new(self.slab_id, self.start_index, self.max_leaves);
        header.update_leaf_count(self.leaf_count);

        let header_bytes = unsafe {
            std::slice::from_raw_parts(&header as *const SlabHeader as *const u8, SlabHeader::SIZE)
        };
        self.mmap[0..SlabHeader::SIZE].copy_from_slice(header_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_slab_create_and_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("slab_0001.dat");

        // Create new slab
        let slab = SlabFile::create(&path, 1, 0, 1000).unwrap();
        assert_eq!(slab.slab_id(), 1);
        assert_eq!(slab.start_index(), 0);
        assert_eq!(slab.max_leaves(), 1000);
        assert_eq!(slab.leaf_count(), 0);
        drop(slab);

        // Reopen
        let slab = SlabFile::open(&path).unwrap();
        assert_eq!(slab.slab_id(), 1);
        assert_eq!(slab.leaf_count(), 0);
    }

    #[test]
    fn test_slab_write_and_read_node() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("slab_0001.dat");

        let mut slab = SlabFile::create(&path, 1, 0, 1000).unwrap();

        // Write a leaf
        let hash = [42u8; 32];
        slab.set_node(0, 0, &hash);

        // Read it back
        assert_eq!(slab.get_node(0, 0), Some(hash));

        // Non-existent node
        assert_eq!(slab.get_node(0, 999), None);
    }

    #[test]
    fn test_slab_multiple_levels() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("slab_0001.dat");

        let mut slab = SlabFile::create(&path, 1, 0, 8).unwrap();

        // Write leaves (level 0)
        for i in 0..8 {
            let mut hash = [0u8; 32];
            hash[0] = i;
            slab.set_node(0, i, &hash);
        }

        // Write internal nodes (level 1)
        for i in 0..4 {
            let mut hash = [0u8; 32];
            hash[0] = 100 + i as u8;
            slab.set_node(1, i, &hash);
        }

        // Verify
        assert_eq!(slab.get_node(0, 0).unwrap()[0], 0);
        assert_eq!(slab.get_node(0, 7).unwrap()[0], 7);
        assert_eq!(slab.get_node(1, 0).unwrap()[0], 100);
        assert_eq!(slab.get_node(1, 3).unwrap()[0], 103);
    }

    #[test]
    fn test_slab_is_full() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("slab_0001.dat");

        let mut slab = SlabFile::create(&path, 1, 0, 10).unwrap();
        assert!(!slab.is_full());

        // Fill up
        for i in 0..10 {
            let hash = [i as u8; 32];
            slab.set_node(0, i, &hash);
        }

        assert!(slab.is_full());
    }

    #[test]
    fn test_slab_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("slab_0001.dat");

        // Write and flush
        {
            let mut slab = SlabFile::create(&path, 1, 0, 100).unwrap();
            let hash = [123u8; 32];
            slab.set_node(0, 42, &hash);
            slab.flush().unwrap();
        }

        // Reopen and verify
        {
            let slab = SlabFile::open(&path).unwrap();
            assert_eq!(slab.get_node(0, 42).unwrap(), [123u8; 32]);
            assert_eq!(slab.leaf_count(), 43); // 0..=42
        }
    }

    #[test]
    fn test_slab_invalid_magic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupted.dat");

        // Create valid file
        let _slab = SlabFile::create(&path, 1, 0, 100).unwrap();
        drop(_slab);

        // Corrupt magic
        {
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.write_all(b"XXXX").unwrap();
        }

        // Try to open
        let result = SlabFile::open(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid magic"));
    }

    #[test]
    #[should_panic(expected = "node offset out of bounds")]
    fn test_slab_out_of_bounds_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("slab_0001.dat");

        let mut slab = SlabFile::create(&path, 1, 0, 10).unwrap();
        let hash = [0u8; 32];

        // Try to write beyond capacity
        slab.set_node(0, 1000, &hash);
    }
}
