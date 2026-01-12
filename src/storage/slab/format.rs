//! Slab file format definitions
//!
//! Defines the on-disk format for memory-mapped slab files.
//! Each slab stores a complete binary tree for up to 1M leaves.

/// Slab file magic bytes "ATLS" (Autonomous Transparency Log Slab)
pub const SLAB_MAGIC: [u8; 4] = *b"ATLS";

/// Current slab format version
pub const SLAB_VERSION: u8 = 1;

/// Default slab capacity (leaves per slab)
pub const DEFAULT_SLAB_CAPACITY: u32 = 1_000_000;

/// Node size (SHA-256 hash)
pub const NODE_SIZE: usize = 32;

/// Slab file header (fixed size: 32 bytes)
///
/// Stored at the beginning of each slab file.
/// The header is followed by the node data regions.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct SlabHeader {
    /// Magic bytes: "ATLS"
    pub magic: [u8; 4],

    /// Format version
    pub version: u8,

    /// Padding for alignment
    pub _padding: [u8; 3],

    /// Slab ID (sequential, starts at 1)
    pub slab_id: u32,

    /// First leaf index in this slab (global index)
    pub start_index: u64,

    /// Maximum leaves in this slab
    pub max_leaves: u32,

    /// Current leaf count (how many leaves are actually written)
    pub leaf_count: u32,

    /// CRC32 of header (for integrity checking)
    pub header_crc: u32,
}

impl SlabHeader {
    /// Header size in bytes
    pub const SIZE: usize = 32;

    /// Create a new header with validation
    ///
    /// # Arguments
    /// * `slab_id` - Sequential slab identifier
    /// * `start_index` - First leaf index in this slab
    /// * `max_leaves` - Maximum leaves capacity
    ///
    /// # Returns
    /// New header with CRC computed
    pub fn new(slab_id: u32, start_index: u64, max_leaves: u32) -> Self {
        let mut header = Self {
            magic: SLAB_MAGIC,
            version: SLAB_VERSION,
            _padding: [0; 3],
            slab_id,
            start_index,
            max_leaves,
            leaf_count: 0,
            header_crc: 0,
        };
        header.header_crc = header.compute_crc();
        header
    }

    /// Validate header magic and version
    ///
    /// # Errors
    /// Returns `false` if magic or version is invalid
    pub fn validate(&self) -> bool {
        self.magic == SLAB_MAGIC
            && self.version == SLAB_VERSION
            && self.header_crc == self.compute_crc_without_field()
    }

    /// Calculate total file size for given capacity
    ///
    /// # Arguments
    /// * `max_leaves` - Maximum leaves in slab
    ///
    /// # Returns
    /// Total file size in bytes (header + all nodes)
    pub fn file_size(max_leaves: u32) -> usize {
        Self::SIZE + Self::total_nodes(max_leaves) * NODE_SIZE
    }

    /// Total nodes in a complete binary tree with N leaves
    ///
    /// For a tree with N leaves, total nodes = 2N - 1
    /// (sum of geometric series: N + N/2 + N/4 + ... + 1)
    ///
    /// # Arguments
    /// * `max_leaves` - Number of leaves
    ///
    /// # Returns
    /// Total number of nodes in the tree
    pub fn total_nodes(max_leaves: u32) -> usize {
        (max_leaves as usize) * 2 - 1
    }

    /// Calculate number of nodes at a specific level
    ///
    /// Level 0 = leaves, level 1 = first internal level, etc.
    ///
    /// # Arguments
    /// * `level` - Tree level (0 = leaves)
    /// * `max_leaves` - Maximum leaves in tree
    ///
    /// # Returns
    /// Number of nodes at this level
    pub fn nodes_at_level(level: u32, max_leaves: u32) -> usize {
        (max_leaves as usize) >> level
    }

    /// Calculate file offset for start of a given level
    ///
    /// Levels are stored from bottom (leaves) to top (root).
    /// Level 0 starts right after header.
    ///
    /// # Arguments
    /// * `level` - Tree level (0 = leaves)
    /// * `max_leaves` - Maximum leaves in tree
    ///
    /// # Returns
    /// Byte offset from start of file
    pub fn level_offset(level: u32, max_leaves: u32) -> usize {
        let mut offset = Self::SIZE;
        let mut nodes_at_level = max_leaves as usize;

        for _ in 0..level {
            offset += nodes_at_level * NODE_SIZE;
            nodes_at_level /= 2;
        }

        offset
    }

    /// Calculate maximum tree height for given capacity
    ///
    /// Height = ceil(log2(max_leaves))
    ///
    /// # Arguments
    /// * `max_leaves` - Maximum leaves in tree
    ///
    /// # Returns
    /// Tree height (root level)
    pub fn tree_height(max_leaves: u32) -> u32 {
        if max_leaves <= 1 {
            return 0;
        }
        32 - (max_leaves - 1).leading_zeros()
    }

    /// Compute CRC32 of header (excluding the CRC field itself)
    fn compute_crc_without_field(&self) -> u32 {
        let mut bytes = Vec::with_capacity(Self::SIZE - 4);
        bytes.extend_from_slice(&self.magic);
        bytes.push(self.version);
        bytes.extend_from_slice(&self._padding);
        bytes.extend_from_slice(&self.slab_id.to_le_bytes());
        bytes.extend_from_slice(&self.start_index.to_le_bytes());
        bytes.extend_from_slice(&self.max_leaves.to_le_bytes());
        bytes.extend_from_slice(&self.leaf_count.to_le_bytes());
        crc32fast::hash(&bytes)
    }

    /// Compute CRC32 with current field values
    fn compute_crc(&self) -> u32 {
        self.compute_crc_without_field()
    }

    /// Update leaf count and recompute CRC
    ///
    /// # Arguments
    /// * `leaf_count` - New leaf count
    pub fn update_leaf_count(&mut self, leaf_count: u32) {
        self.leaf_count = leaf_count;
        self.header_crc = self.compute_crc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slab_header_size() {
        assert_eq!(std::mem::size_of::<SlabHeader>(), SlabHeader::SIZE);
    }

    #[test]
    fn test_slab_header_creation() {
        let header = SlabHeader::new(1, 0, 1000);
        // Copy fields to avoid packed struct issues
        let magic = header.magic;
        let version = header.version;
        let slab_id = header.slab_id;
        let start_index = header.start_index;
        let max_leaves = header.max_leaves;
        let leaf_count = header.leaf_count;

        assert_eq!(magic, SLAB_MAGIC);
        assert_eq!(version, SLAB_VERSION);
        assert_eq!(slab_id, 1);
        assert_eq!(start_index, 0);
        assert_eq!(max_leaves, 1000);
        assert_eq!(leaf_count, 0);
        assert!(header.validate());
    }

    #[test]
    fn test_total_nodes_calculation() {
        // Tree with 8 leaves:
        // Level 0 (leaves): 8 nodes
        // Level 1: 4 nodes
        // Level 2: 2 nodes
        // Level 3 (root): 1 node
        // Total: 15 nodes = 2*8 - 1
        assert_eq!(SlabHeader::total_nodes(8), 15);

        // 1M leaves
        assert_eq!(SlabHeader::total_nodes(1_000_000), 1_999_999);
    }

    #[test]
    fn test_level_offset_calculation() {
        let max_leaves = 1000;

        // Level 0 starts right after header
        assert_eq!(SlabHeader::level_offset(0, max_leaves), SlabHeader::SIZE);

        // Level 1 starts after all leaves
        assert_eq!(
            SlabHeader::level_offset(1, max_leaves),
            SlabHeader::SIZE + 1000 * NODE_SIZE
        );

        // Level 2 starts after level 0 and level 1
        assert_eq!(
            SlabHeader::level_offset(2, max_leaves),
            SlabHeader::SIZE + 1000 * NODE_SIZE + 500 * NODE_SIZE
        );
    }

    #[test]
    fn test_file_size_calculation() {
        // 8 leaves = 15 total nodes = 15 * 32 = 480 bytes data + 32 bytes header
        assert_eq!(SlabHeader::file_size(8), SlabHeader::SIZE + 15 * NODE_SIZE);

        // 1M leaves = ~2M nodes = ~64 MB
        let size = SlabHeader::file_size(1_000_000);
        assert!(size > 64_000_000 && size < 65_000_000);
    }

    #[test]
    fn test_tree_height() {
        assert_eq!(SlabHeader::tree_height(1), 0);
        assert_eq!(SlabHeader::tree_height(2), 1);
        assert_eq!(SlabHeader::tree_height(4), 2);
        assert_eq!(SlabHeader::tree_height(8), 3);
        assert_eq!(SlabHeader::tree_height(1_000_000), 20);
    }

    #[test]
    fn test_nodes_at_level() {
        let max_leaves = 1000;
        assert_eq!(SlabHeader::nodes_at_level(0, max_leaves), 1000);
        assert_eq!(SlabHeader::nodes_at_level(1, max_leaves), 500);
        assert_eq!(SlabHeader::nodes_at_level(2, max_leaves), 250);
    }

    #[test]
    fn test_header_crc_validation() {
        let mut header = SlabHeader::new(1, 0, 1000);
        assert!(header.validate());

        // Corrupt the magic
        header.magic = *b"XXXX";
        assert!(!header.validate());

        // Restore magic but corrupt CRC
        header.magic = SLAB_MAGIC;
        header.header_crc = 0;
        assert!(!header.validate());
    }

    #[test]
    fn test_update_leaf_count() {
        let mut header = SlabHeader::new(1, 0, 1000);
        let leaf_count = header.leaf_count;
        assert_eq!(leaf_count, 0);

        header.update_leaf_count(500);
        let leaf_count = header.leaf_count;
        assert_eq!(leaf_count, 500);
        assert!(header.validate());

        header.update_leaf_count(1000);
        let leaf_count = header.leaf_count;
        assert_eq!(leaf_count, 1000);
        assert!(header.validate());
    }
}
