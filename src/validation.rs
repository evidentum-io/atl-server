//! Ingress validation that is only decidable on raw request bytes.
//!
//! Everything here has to run *before* the request is handed to
//! `serde_json`. RFC 8785 §3.1 constrains "data to be canonicalized", and one
//! of its constraints -- "JSON objects MUST NOT exhibit duplicate property
//! names" -- is a property of a byte sequence, not of the value a parser
//! produces from it. `serde_json` keeps the last occurrence and discards the
//! rest, so `{"x":1,"x":2}` and `{"x":2}` are the same `Value` and no
//! inspection downstream can tell them apart.

use crate::error::{ServerError, ServerResult};

/// Reject JSON text whose objects repeat a property name.
///
/// `location` names the part of the request the text came from and appears in
/// the error message, so an operator reading a 400 can tell a duplicate in the
/// request envelope from one inside a metadata document.
///
/// # Why the check is on the whole text and not on the metadata subtree
///
/// Only `entry.metadata` is JCS-canonicalized, so §3.1 formally binds only
/// that subtree. Narrowing the scope would mean locating `metadata` in the raw
/// text before parsing it -- an ad-hoc JSON scanner whose idea of where the
/// subtree starts could itself disagree with `serde_json`'s. Rejecting at the
/// document level needs no such thing, and a request body that states
/// `"metadata"` twice is ambiguous in exactly the way §3.1 exists to prevent.
///
/// # Errors
///
/// * [`ServerError::DuplicatePropertyName`] if any object repeats a property
///   name. The message carries the RFC 6901 JSON Pointer of the offending
///   object.
/// * [`ServerError::InvalidArgument`] if `json` is not well-formed JSON at
///   all. The typed parse that follows would report the same thing; reporting
///   it here keeps the two paths from disagreeing about which error a
///   malformed body deserves.
pub fn reject_duplicate_property_names(json: &str, location: &'static str) -> ServerResult<()> {
    atl_core::check_unique_property_names(json).map_err(|e| match e {
        atl_core::AtlError::JcsInputConstraint { path, reason } => {
            ServerError::DuplicatePropertyName {
                location,
                reason: format!("at JSON Pointer {path:?}: {reason}"),
            }
        }
        other => ServerError::InvalidArgument(format!("invalid JSON in {location}: {other}")),
    })
}

/// Reject a request body that is not UTF-8.
///
/// [`reject_duplicate_property_names`] scans text, and JSON is defined over
/// Unicode (RFC 8259 §8.1 requires UTF-8 for interchange), so a body that is
/// not UTF-8 has no JSON reading at all.
///
/// # Errors
///
/// [`ServerError::InvalidArgument`] if `bytes` is not valid UTF-8.
pub fn body_as_utf8<'a>(bytes: &'a [u8], location: &'static str) -> ServerResult<&'a str> {
    std::str::from_utf8(bytes)
        .map_err(|e| ServerError::InvalidArgument(format!("{location} is not valid UTF-8: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_document_with_unique_names() {
        assert!(reject_duplicate_property_names(r#"{"a":1,"b":{"a":2}}"#, "body").is_ok());
    }

    #[test]
    fn rejects_a_duplicate_at_the_root() {
        let err = reject_duplicate_property_names(r#"{"a":1,"a":2}"#, "body")
            .expect_err("duplicate must be refused");
        assert!(matches!(err, ServerError::DuplicatePropertyName { .. }));
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(err.error_code(), "DUPLICATE_PROPERTY_NAME");
    }

    #[test]
    fn rejects_a_duplicate_nested_in_metadata() {
        let err = reject_duplicate_property_names(
            r#"{"payload":"x","metadata":{"case":{"id":1,"id":2}}}"#,
            "request body",
        )
        .expect_err("nested duplicate must be refused");
        // The pointer must locate the offending object, not just say "somewhere".
        assert!(
            err.to_string().contains("/metadata/case"),
            "message should carry the JSON Pointer, got: {err}"
        );
    }

    #[test]
    fn rejects_a_repeated_metadata_key_in_the_envelope() {
        let err = reject_duplicate_property_names(
            r#"{"payload":"x","metadata":{"a":1},"metadata":{"b":2}}"#,
            "request body",
        )
        .expect_err("ambiguous envelope must be refused");
        assert!(matches!(err, ServerError::DuplicatePropertyName { .. }));
    }

    #[test]
    fn malformed_json_is_a_plain_bad_request() {
        let err = reject_duplicate_property_names("{oops", "body").expect_err("must fail");
        assert!(matches!(err, ServerError::InvalidArgument(_)));
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn non_utf8_body_is_rejected() {
        let err = body_as_utf8(&[0xff, 0xfe], "request body").expect_err("must fail");
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }
}
