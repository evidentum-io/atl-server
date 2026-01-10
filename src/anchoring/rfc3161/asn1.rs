//! ASN.1 encoding/decoding for RFC 3161

#![allow(dead_code)]

use crate::anchoring::error::AnchorError;
use crate::anchoring::rfc3161::types::TsaResponse;
use rasn::types::{Any, Integer, ObjectIdentifier, OctetString};
use rasn::{AsnType, Decode, Encode};

/// SHA-256 OID: 2.16.840.1.101.3.4.2.1
#[allow(dead_code)]
const OID_SHA256: &[u64] = &[2, 16, 840, 1, 101, 3, 4, 2, 1];

/// CMS SignedData OID: 1.2.840.113549.1.7.2
const OID_SIGNED_DATA: &[u64] = &[1, 2, 840, 113549, 1, 7, 2];

/// id-ct-TSTInfo OID: 1.2.840.113549.1.9.16.1.4
const OID_TST_INFO: &[u64] = &[1, 2, 840, 113549, 1, 9, 16, 1, 4];

/// TimeStampReq (simplified structure for encoding)
#[allow(dead_code)]
#[derive(AsnType, Encode)]
struct TimeStampReq {
    version: Integer,
    message_imprint: MessageImprint,
    cert_req: bool,
}

/// MessageImprint structure
#[derive(AsnType, Encode)]
struct MessageImprint {
    hash_algorithm: AlgorithmIdentifier,
    hashed_message: OctetString,
}

/// AlgorithmIdentifier structure
#[derive(AsnType, Encode, Decode, Hash, PartialEq, Eq)]
struct AlgorithmIdentifier {
    algorithm: ObjectIdentifier,
}

/// CMS ContentInfo wrapper
#[derive(AsnType, Decode)]
struct ContentInfo {
    content_type: ObjectIdentifier,
    #[rasn(tag(explicit(0)))]
    content: Any,
}

/// CMS SignedData structure (simplified)
#[derive(AsnType, Decode)]
struct SignedData {
    version: Integer,
    digest_algorithms: rasn::types::SetOf<AlgorithmIdentifier>,
    encap_content_info: EncapsulatedContentInfo,
    // Optional fields skipped for simplicity
}

/// EncapsulatedContentInfo
#[derive(AsnType, Decode)]
struct EncapsulatedContentInfo {
    e_content_type: ObjectIdentifier,
    #[rasn(tag(explicit(0)))]
    e_content: Option<OctetString>,
}

/// TSTInfo structure
#[derive(AsnType, Decode)]
struct TstInfo {
    version: Integer,
    policy: ObjectIdentifier,
    message_imprint: MessageImprintDecode,
    serial_number: Integer,
    gen_time: rasn::types::GeneralizedTime,
    // Optional fields omitted
}

/// MessageImprint for decoding
#[derive(AsnType, Decode)]
struct MessageImprintDecode {
    hash_algorithm: AlgorithmIdentifier,
    hashed_message: OctetString,
}

/// TimeStampResp (simplified structure for decoding)
///
/// RFC 3161: `TimeStampResp ::= SEQUENCE { status PKIStatusInfo, timeStampToken TimeStampToken OPTIONAL }`
/// where `TimeStampToken ::= ContentInfo` (CMS SignedData structure).
/// We use `Any` to capture the raw DER bytes without parsing the full CMS structure.
#[derive(AsnType, Decode)]
struct TimeStampResp {
    status: PkiStatusInfo,
    time_stamp_token: Option<Any>,
}

/// PKIStatusInfo structure
#[derive(AsnType, Decode)]
struct PkiStatusInfo {
    status: Integer,
}

/// Extract the main DER structure from a byte slice that may contain trailing data.
///
/// DER encoding format: [tag] [length] [value]
/// - Tag: 1 byte (SEQUENCE = 0x30)
/// - Length: 1-127 bytes (short form) or 1 + n bytes (long form)
/// - Value: actual content
///
/// This function reads the tag and length to determine how many bytes the main structure uses,
/// and returns a slice containing only that structure (ignoring trailing data).
fn extract_main_structure(der: &[u8]) -> Result<&[u8], AnchorError> {
    if der.is_empty() {
        return Err(AnchorError::InvalidResponse("Empty DER data".into()));
    }

    // Read tag (should be SEQUENCE = 0x30)
    let _tag = der[0];

    if der.len() < 2 {
        return Err(AnchorError::InvalidResponse("DER data too short".into()));
    }

    // Parse length (second byte)
    let length_byte = der[1];

    if length_byte < 0x80 {
        // Short form: length is in the byte itself
        let content_length = length_byte as usize;
        let total_length = 2 + content_length; // tag + length byte + content

        if der.len() < total_length {
            return Err(AnchorError::InvalidResponse(
                "DER data shorter than indicated length".into(),
            ));
        }

        Ok(&der[..total_length])
    } else {
        // Long form: 0x80 + n bytes for length
        let num_length_bytes = (length_byte & 0x7F) as usize;

        if num_length_bytes == 0 || num_length_bytes > 4 {
            return Err(AnchorError::InvalidResponse(
                "Invalid DER length encoding".into(),
            ));
        }

        if der.len() < 2 + num_length_bytes {
            return Err(AnchorError::InvalidResponse(
                "DER data too short for length".into(),
            ));
        }

        // Parse multi-byte length (big-endian)
        let mut content_length: usize = 0;
        for i in 0..num_length_bytes {
            content_length = (content_length << 8) | (der[2 + i] as usize);
        }

        let total_length = 2 + num_length_bytes + content_length;

        if der.len() < total_length {
            return Err(AnchorError::InvalidResponse(
                "DER data shorter than indicated length".into(),
            ));
        }

        Ok(&der[..total_length])
    }
}

/// Build RFC 3161 TimeStampReq (DER encoded)
pub fn build_timestamp_request(hash: &[u8; 32]) -> Result<Vec<u8>, AnchorError> {
    let oid_vec: Vec<u32> = OID_SHA256.iter().map(|&x| x as u32).collect();
    let oid = ObjectIdentifier::new_unchecked(oid_vec.into());

    // Need to copy hash data to satisfy lifetime requirements
    let hash_vec = hash.to_vec();

    let req = TimeStampReq {
        version: Integer::from(1_i32),
        message_imprint: MessageImprint {
            hash_algorithm: AlgorithmIdentifier { algorithm: oid },
            hashed_message: OctetString::from(hash_vec),
        },
        cert_req: true,
    };

    rasn::der::encode(&req).map_err(Into::into)
}

/// Parse RFC 3161 TimeStampResp (DER encoded)
///
/// Handles TSA responses that include trailing certificate data (common in real TSA servers).
/// Since rasn 0.18 doesn't have `decode_with_remainder`, we extract and decode only the main
/// TimeStampResp structure, ignoring any trailing data (certificates, CRLs, etc.).
pub fn parse_timestamp_response(der: &[u8]) -> Result<TsaResponse, AnchorError> {
    // Real TSA responses often include certificates as trailing data.
    // Extract the main TimeStampResp structure by parsing the DER length.
    let resp_bytes = extract_main_structure(der)?;
    let resp: TimeStampResp = rasn::der::decode(resp_bytes)?;

    // Check status (0 = granted)
    // Note: Integer from rasn doesn't have to_i32, we need to check if it equals 0
    let status_is_zero = resp.status.status == Integer::from(0_i32);

    if !status_is_zero {
        return Err(AnchorError::ServiceError(
            "TSA returned non-zero status".into(),
        ));
    }

    let token = resp
        .time_stamp_token
        .ok_or_else(|| AnchorError::InvalidResponse("no token in response".into()))?;

    // Extract timestamp from token (simplified - in real impl parse TSTInfo)
    let timestamp = extract_timestamp_from_token(&token)?;

    // Store the raw DER bytes of the TimeStampToken (ContentInfo)
    let token_der = rasn::der::encode(&token)?;

    Ok(TsaResponse {
        token_der,
        timestamp,
    })
}

/// Extract timestamp from TimeStampToken
///
/// Parses CMS ContentInfo -> SignedData -> TSTInfo and extracts genTime.
fn extract_timestamp_from_token(token: &Any) -> Result<u64, AnchorError> {
    // Decode ContentInfo from token
    let token_bytes = rasn::der::encode(token)?;
    let content_info: ContentInfo = rasn::der::decode(&token_bytes)?;

    // Verify content_type is SignedData
    let signed_data_oid = ObjectIdentifier::new_unchecked(
        OID_SIGNED_DATA
            .iter()
            .map(|&x| x as u32)
            .collect::<Vec<_>>()
            .into(),
    );
    if content_info.content_type != signed_data_oid {
        return Err(AnchorError::InvalidResponse(
            "ContentInfo is not SignedData".into(),
        ));
    }

    // Decode SignedData from content
    let signed_data_bytes = rasn::der::encode(&content_info.content)?;
    let signed_data: SignedData = rasn::der::decode(&signed_data_bytes)?;

    // Verify encap_content_info.e_content_type is TSTInfo
    let tst_info_oid = ObjectIdentifier::new_unchecked(
        OID_TST_INFO
            .iter()
            .map(|&x| x as u32)
            .collect::<Vec<_>>()
            .into(),
    );
    if signed_data.encap_content_info.e_content_type != tst_info_oid {
        return Err(AnchorError::InvalidResponse(
            "EncapsulatedContentInfo is not TSTInfo".into(),
        ));
    }

    // Extract and decode TSTInfo from e_content
    let e_content = signed_data
        .encap_content_info
        .e_content
        .ok_or_else(|| AnchorError::InvalidResponse("Missing eContent in TSTInfo".into()))?;

    let tst_info: TstInfo = rasn::der::decode(e_content.as_ref())?;

    // Convert GeneralizedTime (chrono::DateTime) to Unix nanoseconds
    let unix_nanos = tst_info
        .gen_time
        .timestamp_nanos_opt()
        .ok_or_else(|| AnchorError::InvalidResponse("Timestamp out of range".into()))?;

    unix_nanos
        .try_into()
        .map_err(|_| AnchorError::InvalidResponse("Timestamp overflow".into()))
}

/// Verify message imprint in token matches expected hash
///
/// Extracts MessageImprint from TSTInfo and compares with expected hash.
pub fn verify_message_imprint(
    token_der: &[u8],
    expected_hash: &[u8; 32],
) -> Result<(), AnchorError> {
    // Decode ContentInfo from token
    let content_info: ContentInfo = rasn::der::decode(token_der)?;

    // Verify content_type is SignedData
    let signed_data_oid = ObjectIdentifier::new_unchecked(
        OID_SIGNED_DATA
            .iter()
            .map(|&x| x as u32)
            .collect::<Vec<_>>()
            .into(),
    );
    if content_info.content_type != signed_data_oid {
        return Err(AnchorError::InvalidResponse(
            "ContentInfo is not SignedData".into(),
        ));
    }

    // Decode SignedData from content
    let signed_data_bytes = rasn::der::encode(&content_info.content)?;
    let signed_data: SignedData = rasn::der::decode(&signed_data_bytes)?;

    // Verify encap_content_info.e_content_type is TSTInfo
    let tst_info_oid = ObjectIdentifier::new_unchecked(
        OID_TST_INFO
            .iter()
            .map(|&x| x as u32)
            .collect::<Vec<_>>()
            .into(),
    );
    if signed_data.encap_content_info.e_content_type != tst_info_oid {
        return Err(AnchorError::InvalidResponse(
            "EncapsulatedContentInfo is not TSTInfo".into(),
        ));
    }

    // Extract and decode TSTInfo from e_content
    let e_content = signed_data
        .encap_content_info
        .e_content
        .ok_or_else(|| AnchorError::InvalidResponse("Missing eContent in TSTInfo".into()))?;

    let tst_info: TstInfo = rasn::der::decode(e_content.as_ref())?;

    // Verify hash algorithm is SHA-256
    let sha256_oid = ObjectIdentifier::new_unchecked(
        OID_SHA256
            .iter()
            .map(|&x| x as u32)
            .collect::<Vec<_>>()
            .into(),
    );
    if tst_info.message_imprint.hash_algorithm.algorithm != sha256_oid {
        return Err(AnchorError::InvalidResponse(
            "MessageImprint does not use SHA-256".into(),
        ));
    }

    // Compare hashed_message with expected hash
    let hashed_message = tst_info.message_imprint.hashed_message.as_ref();
    if hashed_message != expected_hash {
        return Err(AnchorError::InvalidResponse(
            "MessageImprint hash mismatch".into(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_request() {
        let hash = [0u8; 32];
        let result = build_timestamp_request(&hash);
        assert!(result.is_ok());
        let der = result.unwrap();
        assert!(!der.is_empty());
    }

    #[test]
    fn test_extract_main_structure_short_form() {
        // Simple DER SEQUENCE with short-form length
        // Tag: 0x30 (SEQUENCE), Length: 0x05, Value: 5 bytes
        let der = vec![0x30, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05];
        let result = extract_main_structure(&der);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), &der[..]);
    }

    #[test]
    fn test_extract_main_structure_with_trailing_data() {
        // DER SEQUENCE with trailing garbage
        // Tag: 0x30, Length: 0x03, Value: 3 bytes, Trailing: 4 bytes
        let der = vec![0x30, 0x03, 0x01, 0x02, 0x03, 0xDE, 0xAD, 0xBE, 0xEF];
        let result = extract_main_structure(&der);
        assert!(result.is_ok());
        let extracted = result.unwrap();
        assert_eq!(extracted.len(), 5); // tag + length + 3 bytes of content
        assert_eq!(extracted, &der[..5]);
    }

    #[test]
    fn test_extract_main_structure_long_form() {
        // DER SEQUENCE with long-form length
        // Tag: 0x30, Length: 0x81 (1 byte follows) 0x05, Value: 5 bytes
        let der = vec![0x30, 0x81, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05];
        let result = extract_main_structure(&der);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), &der[..]);
    }

    #[test]
    fn test_extract_main_structure_empty() {
        let der: Vec<u8> = vec![];
        let result = extract_main_structure(&der);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_main_structure_too_short() {
        let der = vec![0x30]; // Only tag, no length
        let result = extract_main_structure(&der);
        assert!(result.is_err());
    }
}
