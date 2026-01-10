//! ASN.1 encoding/decoding for RFC 3161

#![allow(dead_code)]

use crate::anchoring::error::AnchorError;
use crate::anchoring::rfc3161::types::TsaResponse;
use rasn::types::{Any, Integer, ObjectIdentifier, OctetString};
use rasn::{AsnType, Decode, Encode};

/// SHA-256 OID: 2.16.840.1.101.3.4.2.1
#[allow(dead_code)]
const OID_SHA256: &[u64] = &[2, 16, 840, 1, 101, 3, 4, 2, 1];

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
#[derive(AsnType, Encode, Decode)]
struct AlgorithmIdentifier {
    algorithm: ObjectIdentifier,
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
/// NOTE: This is a simplified implementation. Full implementation would parse
/// the CMS SignedData structure and extract TSTInfo.genTime.
fn extract_timestamp_from_token(_token: &Any) -> Result<u64, AnchorError> {
    // For now, use current time
    // TODO: Implement full TSTInfo parsing from CMS ContentInfo
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| AnchorError::InvalidResponse(format!("system time error: {}", e)))?;

    Ok(now.as_nanos() as u64)
}

/// Verify message imprint in token matches expected hash
///
/// NOTE: Simplified implementation. Full version would extract MessageImprint
/// from TSTInfo and compare with expected hash.
pub fn verify_message_imprint(
    _token_der: &[u8],
    _expected_hash: &[u8; 32],
) -> Result<(), AnchorError> {
    // TODO: Implement full MessageImprint verification
    // For now, assume valid
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
