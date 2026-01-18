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

#[cfg(test)]
mod tests {
    use super::*;

    // ========== ReceiptOptions::default Tests ==========

    #[test]
    fn test_receipt_options_default() {
        let options = ReceiptOptions::default();
        assert!(options.consistency_from.is_none());
        assert!(options.include_anchors);
        assert!(options.timestamp.is_none());
        assert!(options.at_tree_size.is_none());
        assert!(options.auto_consistency_from_anchor);
        assert!(options.upgrade_url_template.is_none());
    }

    // ========== ReceiptOptions::with_anchors Tests ==========

    #[test]
    fn test_receipt_options_with_anchors() {
        let options = ReceiptOptions::with_anchors();
        assert!(options.consistency_from.is_none());
        assert!(options.include_anchors);
        assert!(options.timestamp.is_none());
        assert!(options.at_tree_size.is_none());
        assert!(options.auto_consistency_from_anchor);
        assert!(options.upgrade_url_template.is_none());
    }

    #[test]
    fn test_receipt_options_with_anchors_equals_default() {
        let default = ReceiptOptions::default();
        let with_anchors = ReceiptOptions::with_anchors();

        // Both should have anchors enabled
        assert_eq!(default.include_anchors, with_anchors.include_anchors);
        assert_eq!(
            default.auto_consistency_from_anchor,
            with_anchors.auto_consistency_from_anchor
        );
    }

    // ========== ReceiptOptions::with_consistency_from Tests ==========

    #[test]
    fn test_receipt_options_with_consistency_from_zero() {
        let options = ReceiptOptions::with_consistency_from(0);
        assert_eq!(options.consistency_from, Some(0));
        assert!(options.include_anchors);
        assert!(options.timestamp.is_none());
        assert!(options.at_tree_size.is_none());
        assert!(!options.auto_consistency_from_anchor); // Disabled when explicit
        assert!(options.upgrade_url_template.is_none());
    }

    #[test]
    fn test_receipt_options_with_consistency_from_specific_size() {
        let options = ReceiptOptions::with_consistency_from(1000);
        assert_eq!(options.consistency_from, Some(1000));
        assert!(!options.auto_consistency_from_anchor); // Explicit consistency disables auto
    }

    #[test]
    fn test_receipt_options_with_consistency_from_large_size() {
        let options = ReceiptOptions::with_consistency_from(u64::MAX);
        assert_eq!(options.consistency_from, Some(u64::MAX));
        assert!(!options.auto_consistency_from_anchor);
    }

    // ========== ReceiptOptions::without_auto_consistency Tests ==========

    #[test]
    fn test_receipt_options_without_auto_consistency() {
        let options = ReceiptOptions::without_auto_consistency();
        assert!(options.consistency_from.is_none());
        assert!(options.include_anchors);
        assert!(options.timestamp.is_none());
        assert!(options.at_tree_size.is_none());
        assert!(!options.auto_consistency_from_anchor); // Explicitly disabled
        assert!(options.upgrade_url_template.is_none());
    }

    #[test]
    fn test_receipt_options_without_auto_consistency_vs_default() {
        let default = ReceiptOptions::default();
        let without_auto = ReceiptOptions::without_auto_consistency();

        // Key difference: auto_consistency_from_anchor
        assert!(default.auto_consistency_from_anchor);
        assert!(!without_auto.auto_consistency_from_anchor);

        // Other fields should be the same
        assert_eq!(default.consistency_from, without_auto.consistency_from);
        assert_eq!(default.include_anchors, without_auto.include_anchors);
    }

    // ========== Clone and Debug Tests ==========

    #[test]
    fn test_receipt_options_clone() {
        let options = ReceiptOptions {
            consistency_from: Some(500),
            include_anchors: false,
            timestamp: Some(123_456_789),
            at_tree_size: Some(1000),
            auto_consistency_from_anchor: false,
            upgrade_url_template: Some("https://example.com/upgrade/{entry_id}".to_string()),
        };

        let cloned = options.clone();
        assert_eq!(options.consistency_from, cloned.consistency_from);
        assert_eq!(options.include_anchors, cloned.include_anchors);
        assert_eq!(options.timestamp, cloned.timestamp);
        assert_eq!(options.at_tree_size, cloned.at_tree_size);
        assert_eq!(
            options.auto_consistency_from_anchor,
            cloned.auto_consistency_from_anchor
        );
        assert_eq!(options.upgrade_url_template, cloned.upgrade_url_template);
    }

    #[test]
    fn test_receipt_options_debug() {
        let options = ReceiptOptions::default();
        let debug_str = format!("{:?}", options);
        assert!(debug_str.contains("ReceiptOptions"));
    }

    // ========== Custom Configuration Tests ==========

    #[test]
    fn test_receipt_options_custom_all_fields() {
        let options = ReceiptOptions {
            consistency_from: Some(100),
            include_anchors: false,
            timestamp: Some(1_234_567_890),
            at_tree_size: Some(5000),
            auto_consistency_from_anchor: false,
            upgrade_url_template: Some("https://api.example.com/receipts/{entry_id}".to_string()),
        };

        assert_eq!(options.consistency_from, Some(100));
        assert!(!options.include_anchors);
        assert_eq!(options.timestamp, Some(1_234_567_890));
        assert_eq!(options.at_tree_size, Some(5000));
        assert!(!options.auto_consistency_from_anchor);
        assert!(options.upgrade_url_template.is_some());
    }

    #[test]
    fn test_receipt_options_with_upgrade_url_template() {
        let template = "https://example.com/upgrade/{entry_id}";
        let options = ReceiptOptions {
            upgrade_url_template: Some(template.to_string()),
            ..Default::default()
        };

        assert!(options.upgrade_url_template.is_some());
        assert_eq!(options.upgrade_url_template.unwrap(), template);
    }

    #[test]
    fn test_receipt_options_without_anchors() {
        let options = ReceiptOptions {
            include_anchors: false,
            ..Default::default()
        };

        assert!(!options.include_anchors);
    }

    #[test]
    fn test_receipt_options_with_custom_timestamp() {
        let timestamp = 1_700_000_000_u64;
        let options = ReceiptOptions {
            timestamp: Some(timestamp),
            ..Default::default()
        };

        assert_eq!(options.timestamp, Some(timestamp));
    }

    #[test]
    fn test_receipt_options_with_at_tree_size() {
        let tree_size = 12345_u64;
        let options = ReceiptOptions {
            at_tree_size: Some(tree_size),
            ..Default::default()
        };

        assert_eq!(options.at_tree_size, Some(tree_size));
    }

    // ========== Edge Cases ==========

    #[test]
    fn test_receipt_options_consistency_from_one() {
        let options = ReceiptOptions::with_consistency_from(1);
        assert_eq!(options.consistency_from, Some(1));
    }

    #[test]
    fn test_receipt_options_mix_explicit_and_auto_consistency() {
        // Explicit consistency_from should disable auto
        let options = ReceiptOptions {
            consistency_from: Some(500),
            auto_consistency_from_anchor: true, // Will be ignored due to explicit consistency_from
            ..Default::default()
        };

        // When both are set, explicit should take precedence
        assert_eq!(options.consistency_from, Some(500));
        assert!(options.auto_consistency_from_anchor);
    }
}
