//! Custom test assertions for receipt validation

use serde_json::Value;

/// Assert that a receipt JSON has the expected structure
pub fn assert_valid_receipt_structure(receipt: &Value) {
    assert!(receipt.is_object(), "Receipt should be a JSON object");

    // Check spec version
    assert_eq!(
        receipt["spec_version"].as_str(),
        Some("1.0.0"),
        "Receipt should have spec_version 1.0.0"
    );

    // Check entry section
    let entry = &receipt["entry"];
    assert!(entry.is_object(), "Receipt should have 'entry' object");
    assert!(entry["id"].is_string(), "Entry should have 'id' string");
    assert!(
        entry["payload_hash"].is_string(),
        "Entry should have 'payload_hash' string"
    );
    assert!(
        entry["metadata_hash"].is_string(),
        "Entry should have 'metadata_hash' string"
    );

    // Check proof section
    let proof = &receipt["proof"];
    assert!(proof.is_object(), "Receipt should have 'proof' object");
    assert!(
        proof["leaf_index"].is_number(),
        "Proof should have 'leaf_index' number"
    );
    assert!(proof["path"].is_array(), "Proof should have 'path' array");

    // Check checkpoint section
    let checkpoint = &proof["checkpoint"];
    assert!(
        checkpoint.is_object(),
        "Proof should have 'checkpoint' object"
    );
    assert!(
        checkpoint["tree_size"].is_number(),
        "Checkpoint should have 'tree_size' number"
    );
    assert!(
        checkpoint["root_hash"].is_string(),
        "Checkpoint should have 'root_hash' string"
    );
    assert!(
        checkpoint["signature"].is_string(),
        "Checkpoint should have 'signature' string"
    );
}

/// Assert that a hash string has the expected format (algorithm:hexvalue)
pub fn assert_hash_format(hash: &str, algorithm: &str) {
    assert!(
        hash.starts_with(&format!("{}:", algorithm)),
        "Hash '{}' should start with '{}:'",
        hash,
        algorithm
    );

    let parts: Vec<&str> = hash.split(':').collect();
    assert_eq!(parts.len(), 2, "Hash should have format 'algorithm:value'");

    let hex_part = parts[1];
    assert!(!hex_part.is_empty(), "Hash hex part should not be empty");
    assert!(
        hex_part.chars().all(|c| c.is_ascii_hexdigit()),
        "Hash hex part should contain only hex digits"
    );
}

/// Assert that a base64 string has the expected format (base64:value)
#[allow(dead_code)]
pub fn assert_base64_format(value: &str) {
    assert!(
        value.starts_with("base64:"),
        "Base64 value '{}' should start with 'base64:'",
        value
    );
}
