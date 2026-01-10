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
pub fn parse_timestamp_response(der: &[u8]) -> Result<TsaResponse, AnchorError> {
    let resp: TimeStampResp = rasn::der::decode(der)?;

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
}
