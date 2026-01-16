//! Receipt generation options

/// Options for receipt generation
///
/// Controls which optional fields are included in the generated receipt.
#[derive(Debug, Clone)]
pub struct ReceiptOptions {
    /// Include consistency proof from this tree size (if available)
    /// If None (default), automatically uses the most recent anchored checkpoint
    /// for Split-View attack protection. Set to Some(0) to disable.
    pub consistency_from: Option<u64>,

    /// Include anchors in receipt (default: true)
    #[allow(dead_code)]
    pub include_anchors: bool,

    /// Custom timestamp for checkpoint (defaults to current time)
    #[allow(dead_code)]
    pub timestamp: Option<u64>,

    /// Tree size to generate receipt at (defaults to current)
    #[allow(dead_code)]
    pub at_tree_size: Option<u64>,

    /// Automatically find and include consistency proof from last anchor
    /// This is the default behavior for Split-View protection.
    /// Only set to false for special cases (e.g., testing).
    pub auto_consistency_from_anchor: bool,

    /// Template for upgrade URL generation
    /// Placeholder {entry_id} will be replaced with actual entry UUID
    /// Example: "https://api.example.com/v1/receipts/{entry_id}/upgrade"
    #[allow(dead_code)]
    pub upgrade_url_template: Option<String>,
}

impl Default for ReceiptOptions {
    fn default() -> Self {
        Self {
            consistency_from: None,
            include_anchors: true,
            timestamp: None,
            at_tree_size: None,
            auto_consistency_from_anchor: true, // Split-View protection by default
            upgrade_url_template: None,
        }
    }
}

impl ReceiptOptions {
    /// Create options with anchors included (explicit)
    #[must_use]
    #[allow(dead_code)]
    pub const fn with_anchors() -> Self {
        Self {
            consistency_from: None,
            include_anchors: true,
            timestamp: None,
            at_tree_size: None,
            auto_consistency_from_anchor: true,
            upgrade_url_template: None,
        }
    }

    /// Create options with explicit consistency proof from specific tree size
    /// This overrides auto_consistency_from_anchor.
    #[must_use]
    #[allow(dead_code)]
    pub const fn with_consistency_from(tree_size: u64) -> Self {
        Self {
            consistency_from: Some(tree_size),
            include_anchors: true,
            timestamp: None,
            at_tree_size: None,
            auto_consistency_from_anchor: false,
            upgrade_url_template: None,
        }
    }

    /// Disable automatic consistency proof (not recommended for production)
    #[must_use]
    #[allow(dead_code)]
    pub const fn without_auto_consistency() -> Self {
        Self {
            consistency_from: None,
            include_anchors: true,
            timestamp: None,
            at_tree_size: None,
            auto_consistency_from_anchor: false,
            upgrade_url_template: None,
        }
    }
}
