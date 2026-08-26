// File: src/background/tsa_job/audit.rs

//! One-off admin audit for stored RFC 3161 anchors.
//!
//! Scans every `rfc3161` anchor row and verifies its stored token against
//! its OWN `anchored_hash` column, exactly the way
//! `background::tsa_job::request::try_tsa_timestamp` verifies a freshly
//! received token before storing it. A row whose token fails
//! `messageImprint` verification predates that check (or was otherwise
//! corrupted) and is either reported (dry run, the default) or marked
//! `rejected` via [`IndexStore::reject_anchor_atomic`] (with `apply:
//! true`), which atomically releases any tree still pointing at it so it
//! is re-queued for anchoring.
//!
//! The hash used for comparison is always the anchor row's own
//! `anchored_hash`, never one derived from `tree_size` (e.g. by joining
//! against a tree's `root_hash`): an anchor row can be shared across
//! multiple trees via the reuse path in `try_tsa_timestamp`, so tree-size
//! derived roots are not an authoritative source of truth for what a
//! specific anchor row actually claims to attest. The row's own
//! `anchored_hash` is authoritative by construction (it is exactly what
//! [`TsaClient::verify`] checks the token against on receipt).
//!
//! This is deliberately **not** part of server startup or any background
//! job -- it exists purely as a manually run remediation tool (see
//! `src/bin/tsa_anchor_audit.rs`) for anchors stored before
//! verification-on-receipt existed. It is idempotent: rows already marked
//! `rejected` are skipped, so running it repeatedly (with `--apply`) only
//! ever acts on rows found bad for the first time.

use crate::storage::index::IndexStore;
#[cfg(feature = "rfc3161")]
use crate::storage::index::REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH;

/// One anchor row whose stored token fails `messageImprint` verification
/// against its own `anchored_hash`.
#[derive(Debug, Clone)]
pub struct BadAnchor {
    /// `anchors.id`
    pub id: i64,
    /// `anchors.tree_size` (NULL for `super_root`-targeted anchors, though
    /// in practice all `rfc3161` rows target `data_tree_root` and have one)
    pub tree_size: Option<u64>,
    /// `anchors.anchored_hash` -- the hash this row claims to attest
    pub anchored_hash: [u8; 32],
    /// Why verification failed (from `TsaClient::verify`'s error, or from
    /// audit-side plumbing such as a failed database read)
    pub reason: String,
}

/// Result of a single audit pass.
#[derive(Debug, Clone, Default)]
pub struct AuditReport {
    /// Number of `rfc3161` rows actually verified (excludes rows already
    /// `rejected`, which need no further action).
    pub scanned: usize,
    /// Number of `rfc3161` rows skipped because they were already
    /// `rejected` by a previous run.
    pub already_rejected: usize,
    /// Rows found to fail `messageImprint` verification this pass.
    pub bad: Vec<BadAnchor>,
    /// Whether `bad` rows were actually marked rejected (`apply: true`)
    /// or only reported (dry run).
    pub applied: bool,
}

/// Errors specific to running the audit, distinct from a single row
/// failing verification (which is a normal, expected `AuditReport::bad`
/// entry, not an error).
#[derive(Debug, thiserror::Error)]
pub enum AuditError {
    /// A database read or write failed.
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    /// Could not construct the verifier (e.g. TLS backend init failure).
    #[error("failed to create TSA client: {0}")]
    Client(String),

    /// This build was compiled without the `rfc3161` feature, so there is
    /// no verifier available and nothing to audit.
    #[error("this build was compiled without the `rfc3161` feature")]
    FeatureNotEnabled,
}

/// Run one audit pass over every stored `rfc3161` anchor.
///
/// With `apply: false` (dry run), only reports what would change -- no
/// database writes happen. With `apply: true`, every row found bad this
/// pass is marked `rejected` via
/// [`IndexStore::reject_anchor_atomic`](crate::storage::index::IndexStore::reject_anchor_atomic).
#[cfg(feature = "rfc3161")]
pub fn audit_tsa_anchors(index: &IndexStore, apply: bool) -> Result<AuditReport, AuditError> {
    use crate::anchoring::rfc3161::{AsyncRfc3161Client, TsaClient, TsaResponse};

    // Verification is purely local ASN.1 parsing + byte comparison (see
    // `TsaClient::verify`); constructing this client does not perform any
    // network I/O.
    let client = AsyncRfc3161Client::new().map_err(|e| AuditError::Client(e.to_string()))?;

    let rows = index.list_rfc3161_anchors_for_audit()?;

    let mut report = AuditReport {
        applied: apply,
        ..Default::default()
    };

    for row in rows {
        if row.status == "rejected" {
            report.already_rejected += 1;
            continue;
        }
        report.scanned += 1;

        let response = TsaResponse {
            token_der: row.token,
            timestamp: row.timestamp,
        };

        if let Err(e) = client.verify(&response, &row.anchored_hash) {
            report.bad.push(BadAnchor {
                id: row.id,
                tree_size: row.tree_size,
                anchored_hash: row.anchored_hash,
                reason: e.to_string(),
            });
        }
    }

    if apply {
        for bad in &report.bad {
            index.reject_anchor_atomic(bad.id, REJECTION_REASON_MESSAGE_IMPRINT_MISMATCH)?;
        }
    }

    Ok(report)
}

/// Stub for builds without the `rfc3161` feature: there is no verifier to
/// run, so this always reports [`AuditError::FeatureNotEnabled`] rather
/// than silently doing nothing.
#[cfg(not(feature = "rfc3161"))]
pub fn audit_tsa_anchors(_index: &IndexStore, _apply: bool) -> Result<AuditReport, AuditError> {
    Err(AuditError::FeatureNotEnabled)
}

#[cfg(test)]
#[cfg(feature = "rfc3161")]
mod tests {
    use super::*;
    use crate::traits::{Anchor, AnchorType};
    use rusqlite::Connection;

    fn create_test_store() -> IndexStore {
        let conn = Connection::open_in_memory().expect("failed to open in-memory DB");
        let store = IndexStore::from_connection(conn);
        store.initialize().expect("failed to initialize schema");
        store
    }

    /// Real, captured FreeTSA response whose TSTInfo.messageImprint equals
    /// `GOOD_HASH_HEX` below (same fixture as
    /// `background::tsa_job::request::tests`, `asn1::tests` and
    /// `round_robin::tests`).
    const GOOD_TOKEN_HEX: &str = "3082155d30030201003082155406092a864886f70d010702a082154530821541020103310f300d060960864801650304020305003082018f060b2a864886f70d0109100104a082017e0482017a3082017602010106042a0304013031300d060960864801650304020105000420954d5a49fd70d9b8bcdb35d252267829957f7ef7fa6c74f88419bdc5e82209f40204026eb423180f32303236303131313030303131325a0101ff02090081c082603883d30da0820111a482010d308201093111300f060355040a13084672656520545341310c300a060355040b130354534131763074060355040d136d54686973206365727469666963617465206469676974616c6c79207369676e7320646f63756d656e747320616e642074696d65207374616d70207265717565737473206d616465207573696e672074686520667265657473612e6f7267206f6e6c696e65207365727669636573311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310b3009060355040613024445310f300d0603550408130642617965726ea082100830820801308205e9a003020102020900c1e986160da8e982300d06092a864886f70d01010d05003081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b3009060355040613024445301e170d3136303331333031353733395a170d3236303331313031353733395a308201093111300f060355040a13084672656520545341310c300a060355040b130354534131763074060355040d136d54686973206365727469666963617465206469676974616c6c79207369676e7320646f63756d656e747320616e642074696d65207374616d70207265717565737473206d616465207573696e672074686520667265657473612e6f7267206f6e6c696e65207365727669636573311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310b3009060355040613024445310f300d0603550408130642617965726e30820222300d06092a864886f70d01010105000382020f003082020a0282020100b591048c4e486f34e9dc08627fc2375162236984b82cb130beff517cfc38f84bce5c65a874dab2621ae0bce7e33563e0ede934fd5f8823159f07848808227460c1ed88261706f4281334359dfbb81bd1353fc179610af1a8c8c865dc00ea23b3a89be6bd03ba85a9ec827d60565905e22d6a584ed1380ae150280cee397e98a012f380464007862443bc077cb95f421af31712d9683cdb6dffbaf3c8ba5ba566ae523d459d6177346d4d840e27886b7c01c5b890d78a2e27bba8dd2f9a2812e157d62f921c65962548069dcdb7d06de181de0e9570d66f87220ce28b628ab55906f3ee0c210f7051e8f4858af8b9a92d09e46af2d9cba5bfcfad168cdf604491a4b06603b114caf7031f065e7eeefa53c575f3490c059d2e32ddc76ac4d4c4c710683b97fd1be591bc61055186d88f9a0391b307b6f91ed954daa36f9acd6a1e14aa2e4adf17464b54db18dbb6ffe30080246547370436ce4e77bae5de6fe0f3f9d6e7ffbeb461e794e92fb0951f8aae61a412cce9b21074635c8be327ae1a0f6b4a646eb0f8463bc63bf845530435d19e802511ec9f66c3496952d8becb69b0aa4d4c41f60515fe7dcbb89319cdda59ba6aea4be3ceae718e6fcb6ccd7db9fc50bb15b12f3665b0aa307289c2e6dd4b111ce48ba2d9efdb5a6b9a506069334fb34f6fc7ae330f0b34208aac80df3266fdd90465876ba2cb898d9505315b6e7b0203010001a38201db308201d730090603551d1304023000301d0603551d0e041604146e760b7b4e4f9ce160ca6d2ce927a2a294b37737301f0603551d23041830168014fa550d8c346651434cf7e7b3a76c95af7ae6a497300b0603551d0f0404030206c030160603551d250101ff040c300a06082b06010505070308306306082b0601050507010104573055302a06082b06010505073002861e687474703a2f2f7777772e667265657473612e6f72672f7473612e637274302706082b06010505073001861b687474703a2f2f7777772e667265657473612e6f72673a3235363030370603551d1f0430302e302ca02aa0288626687474703a2f2f7777772e667265657473612e6f72672f63726c2f726f6f745f63612e63726c3081c60603551d200481be3081bb3081b80601003081b2303306082b060105050702011627687474703a2f2f7777772e667265657473612e6f72672f667265657473615f6370732e68746d6c303206082b060105050702011626687474703a2f2f7777772e667265657473612e6f72672f667265657473615f6370732e706466304706082b06010505070202303b1a394672656554534120747275737465642074696d657374616d70696e6720536f6674776172652061732061205365727669636520285361615329300d06092a864886f70d01010d05000382020100a5c944e2c6fac0a14d930a7fd0a0b172b41fc1483c3e957c68a2bcd9b9764f1a950161fd72472d41a5eed277786203b5422240fb3a26cde176087b6fb1011df4cc19e2571aa4a051109665e94c46f50bd2adee6ac4137e251b25a39dabda451515d8ff9e07209e8ec20b7874f7e1a0ede7c00937fe84a334f8b3265ced2d8ed9df61396583677feb382c1ee3b23e6ea5f05df30de7b9f89005d25266f612f39c8b4f6daba6d7bfbac19632b90637329f52a6f066a10e43eaa81f849a6c5fe3fe8b5ea23275f687f2052e502ea6c30762a668cce07871dd8e97e315bba929e25589977a0a312ce96c5106b1437c779f2b361b182888f3ee8a234374fa063e956192627f7c431073965d1260928eba009e803429ae324cf96f042354f37bca5afddc79f79346ab388bfc79f01dc9861254ea6cc129941076b83d20556f3be51326837f2876f7833b370e7c3d410523827d4f53400c72218d75229ff10c6f8893a9a3a1c0c42bb4c898c13df41c7f6573b4fc56515971a610a7b0d2857c8225a9fb204eaceca2e8971aa1af87886a2ae3c72fe0a0aae842980a77bef16b92115458090d982b5946603764e75a0ad3d11454b9986f678b9ab6afe8497033ae3abfd4eb43b7bc9dee68815949e6481582a82e785277f2282107efe390200e0508acb8ea82ea2505276f3c9da2a3d3b4ad38bbf8842bda36fc2448291f558dc02dd1e0308207ff308205e7a003020102020900c1e986160da8e980300d06092a864886f70d01010d05003081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b3009060355040613024445301e170d3136303331333031353231335a170d3431303330373031353231335a3081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b300906035504061302444530820222300d06092a864886f70d01010105000382020f003082020a0282020100b6028e0e3032f11110d964cda94b9d0278e1942ae913aaa59907cda69793995bd9ac7e33bad9fe3704da1c01a98d21afe3f591a59d7067705167998f5016722e0ab462b21f439171d2cfcc4593f3735af794a5ab311f6c010c7898de33d75c4510ee76f4bd1d1498cf17d303f06a5dd9f796cc6ca9b657a56fe3ea4fefbe7ce6b6a18d3e35a30cee5ff170d1cf39a333d3fda8964d22db685b29e561be890f0aa845873b2e84ab26ab839ffe8fade9d23bb31e61d273cc9b880649185fabecfa0534600aba901b614e2e854582dea2226fc19cd7df52bed50d8777cd9988c053a3fc7dc3287a068a4ff12b713cd9803666e955385456ff38f80298cf6b93856e9224774a66cf1cdd11c2f8efd85203d7458b25664b13ed639cded4ff8113d6cc5353d2729473c3c307157c722aa5b5dd0bfb2d6c38b1b93749c881ec60026d08951b3824bd71bacbce473aebd636f0b918b4a2c8ff4694f07457af2d6f1cf82554d1770fd79ff5d314dcd104cddcabc94138056dfcf017e7eb8572fd52f70144f188da05f5823f58dd06297e7387bed2d772c13da8266601045fe412dd70986c0c987ba7344b9037387516d258e7885b51f8968b7f2601213bc4cb4c85f8ff0b84af6a988337cdfb81868f7ecf31dca6716d7ec2dd802c1672629e5c0052cb357dd29aafc43f615b3b1ff9d4e1ce08c71c73e1febb7dc56a33621329e9ed6c230203010001a382024e3082024a300c0603551d13040530030101ff300e0603551d0f0101ff0404030201c6301d0603551d0e04160414fa550d8c346651434cf7e7b3a76c95af7ae6a4973081ca0603551d230481c23081bf8014fa550d8c346651434cf7e7b3a76c95af7ae6a497a1819ba481983081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b3009060355040613024445820900c1e986160da8e98030330603551d1f042c302a3028a026a0248622687474703a2f2f7777772e667265657473612e6f72672f726f6f745f63612e63726c3081cf0603551d200481c73081c43081c1060a2b0601040181f22401013081b2303306082b060105050702011627687474703a2f2f7777772e667265657473612e6f72672f667265657473615f6370732e68746d6c303206082b060105050702011626687474703a2f2f7777772e667265657473612e6f72672f667265657473615f6370732e706466304706082b06010505070202303b1a394672656554534120747275737465642074696d657374616d70696e6720536f6674776172652061732061205365727669636520285361615329303706082b06010505070101042b3029302706082b06010505073001861b687474703a2f2f7777772e667265657473612e6f72673a32353630300d06092a864886f70d01010d0500038202010068af7ebf938562ef4ceb3b580be2faf6cc35a26772962f3d95901fa5630c87d09198984ce8a06a33f8a9c282ed9f1cb11ac6c23e17108ee4efce6fb294de95c133262255725522ca61971d4a3b7f78250dfb8d4aeec0fb1959b164100520b9c10e64c62662e4ad4d0abae2298fc948fc4e99e8d9e6b8fdbe4404121ec7c1422eacb2c9d7328e07396e60b4f3bb803ad4a555c80fefb53f85e7764a0a9fb4afc399f4cd2f5fbf587105c6081cf3d05337b6bb7d1b010b749f4888c912f3696ba1b6902d77b7dfc046c04a0cc1ec4f8d185e2da55dfb7bc2a2036c6219246a4f99ddbb6f1f829398f3b803dc0ad90dcb59bef4c27c77404b99043b78271867991152c399f12cbfc4c625adc096355ae44e342100ec517a502e2f06f940b8d43599bbc1154f8ae761a0b0d555fb4a1391d4f3420af8dbf12f2d7ddb9d77dce1537804074af175e4f2d6d55b34b5d6f7dcbdd31730af56480d4c0cff143f9e83bc151866d0ba0f0bbdc47fe27864176bbd6c1ab85df325edf777889bc4471bf3fa73e56cc591e8b160cda7b0786a1ec04ac3b24fa2e28d5d19e5e48004d5e166a83c82ec6fd54fb385ebaf7133a85b52de46db5244e1c34ae8d36e712f9fce0d493d7d3edd586c6198e3ec3e6e96346f417ac9f221e0aff33a8f6a0b1ef4c023630b76adaa8d91433825ecc41c49a5b98b181c7da30e997ab954c73c2cd805afda993182038a308203860201013081a33081953111300f060355040a130846726565205453413110300e060355040b1307526f6f74204341311830160603550403130f7777772e667265657473612e6f72673122302006092a864886f70d0109011613627573696c657a617340676d61696c2e636f6d3112301006035504071309577565727a62757267310f300d0603550408130642617965726e310b3009060355040613024445020900c1e986160da8e982300d06096086480165030402030500a081b8301a06092a864886f70d010903310d060b2a864886f70d0109100104301c06092a864886f70d010905310f170d3236303131313030303131325a302b060b2a864886f70d010910020c311c301a301830160414916da3d860ecca82e34bc59d1793e7e968875f14304f06092a864886f70d01090431420440044f479a6d1240954966f51b86e85ef72ea23c12a0a3dc502901adca336334339ebf18f39b7708cd60780514841423adf531b44b3795699ca8ecf9a9325d3c53300d06092a864886f70d0101010500048202003f09255516f561f9f09786cb3abd61aca90f6217f01145b1e9107be8fcd17c0d3334f056c4e3c9871ef58b63c8e52c008aecf8bc345fc7242b22091c9b0020ed37626450c93a65d130a57a8f473709dbf458b0a9fea1e5564d4efd04bf0c935759be75543c6a3e59c9c42eb25cc89bb8491794b5aeaa598c03023118e07b71ae1e1b6236c2f9e8f252b73e993de3a6c9a99622d919b2abfedc77ffb0d40d85641ef40054f2fc1f15d8bf24b4d02801f16fc1a8c4d9b4cd0806b02b0270225d022fcc6453d17e55123a3fa1144cb6aa6e4652fecfb1290105b198a3bd98f9d2da733dbb5d4d31accd89a1feb065f9fba28458c883adba81dbb665299ffccdba1845e620854866939b84ad76f4dd76f1bcc45b5ae0316802c24d4cc3d0244fcf41bfe23d9edd8272d86c5ed1560cd7f7ed1314c6d7f78d9b31ea0df30f5a5784e59a876f1e0a389c9a16010e5f4ddb91874c9699517bf34016ae64e20fbddc8dd6db1ff68608d14ce073bc4725dfe70ea22306fa11dd0547b1b3a4ef78bea22682f63d907e3a508346ef4f2f0a9af84aa6b6ac833ca9d6e386a9a811b251a9ea3c774eaffae9a90aa75ae5af75cff823cbe65a999b99cfc3088445e9a011d164da17f1b6862dd2ce1cd95237b56fbe780e13e8833504899d8062fa2423f990e3361ee702d75c5e0a061fce4acf390a3423397365902d86ac4c77644b575415ebc5";
    /// SHA-256 hash embedded in `GOOD_TOKEN_HEX`'s messageImprint.
    const GOOD_HASH_HEX: &str = "954d5a49fd70d9b8bcdb35d252267829957f7ef7fa6c74f88419bdc5e82209f4";

    /// `GOOD_TOKEN_HEX` is the raw `TimeStampResp` wire format (`SEQUENCE {
    /// status, timeStampToken }`), but what a stored anchor's `token`
    /// column actually holds -- and what `TsaClient::verify` expects -- is
    /// only the inner `timeStampToken` (`ContentInfo`), re-encoded by
    /// `parse_timestamp_response`. Obtain that exact transformation the
    /// same way production code does, via a real (mocked) TSA round-trip,
    /// rather than duplicating the ASN.1 extraction here (see also
    /// `background::tsa_job::request::tests::freetsa_token_der`).
    async fn good_token() -> Vec<u8> {
        use crate::anchoring::rfc3161::{AsyncRfc3161Client, TsaClient};

        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(hex::decode(GOOD_TOKEN_HEX).expect("valid hex"))
            .create_async()
            .await;

        let client = AsyncRfc3161Client::new().expect("failed to create TSA client");
        let response = client
            .timestamp(&server.url(), &good_hash(), 5000)
            .await
            .expect("mock TSA round-trip must succeed");
        response.token_der
    }

    fn good_hash() -> [u8; 32] {
        let bytes = hex::decode(GOOD_HASH_HEX).expect("valid hex fixture");
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes);
        hash
    }

    fn store_rfc3161(index: &IndexStore, tree_size: u64, hash: [u8; 32], token: Vec<u8>) -> i64 {
        let anchor = Anchor {
            anchor_type: AnchorType::Rfc3161,
            target: "data_tree_root".to_string(),
            anchored_hash: hash,
            tree_size,
            super_tree_size: None,
            timestamp: 1_234_567_890,
            token,
            metadata: serde_json::json!({}),
        };
        index
            .store_anchor_returning_id(tree_size, &anchor, "confirmed")
            .expect("failed to store anchor")
    }

    #[tokio::test]
    async fn test_audit_dry_run_finds_but_does_not_change() {
        let index = create_test_store();
        let good_id = store_rfc3161(&index, 100, good_hash(), good_token().await);
        // Bound to a hash that does NOT match the token's own imprint.
        let bad_id = store_rfc3161(&index, 200, [0xAAu8; 32], good_token().await);

        let report = audit_tsa_anchors(&index, false).expect("audit failed");

        assert_eq!(report.scanned, 2);
        assert_eq!(report.already_rejected, 0);
        assert_eq!(report.bad.len(), 1);
        assert_eq!(report.bad[0].id, bad_id);
        assert!(!report.applied);

        // Dry run: neither row was touched.
        let rows = index
            .list_rfc3161_anchors_for_audit()
            .expect("failed to list");
        assert!(rows.iter().all(|r| r.status == "confirmed"));
        assert!(rows.iter().any(|r| r.id == good_id));
        assert!(rows.iter().any(|r| r.id == bad_id));
    }

    #[tokio::test]
    async fn test_audit_apply_marks_only_bad_rows() {
        let index = create_test_store();
        let good_id = store_rfc3161(&index, 100, good_hash(), good_token().await);
        let bad_id = store_rfc3161(&index, 200, [0xAAu8; 32], good_token().await);

        let report = audit_tsa_anchors(&index, true).expect("audit failed");
        assert_eq!(report.bad.len(), 1);
        assert_eq!(report.bad[0].id, bad_id);
        assert!(report.applied);

        let rows = index
            .list_rfc3161_anchors_for_audit()
            .expect("failed to list");
        let good_row = rows.iter().find(|r| r.id == good_id).unwrap();
        let bad_row = rows.iter().find(|r| r.id == bad_id).unwrap();
        assert_eq!(
            good_row.status, "confirmed",
            "good anchor must be untouched"
        );
        assert_eq!(bad_row.status, "rejected", "bad anchor must be rejected");

        // Rejected rows must no longer be servable.
        assert!(index.get_anchors(200).unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_audit_apply_is_idempotent() {
        let index = create_test_store();
        let bad_id = store_rfc3161(&index, 200, [0xAAu8; 32], good_token().await);

        let first = audit_tsa_anchors(&index, true).expect("first audit failed");
        assert_eq!(first.bad.len(), 1);
        assert_eq!(first.bad[0].id, bad_id);
        assert_eq!(first.already_rejected, 0);

        let second = audit_tsa_anchors(&index, true).expect("second audit failed");
        assert_eq!(
            second.bad.len(),
            0,
            "an already-rejected row must not be reported as newly bad again"
        );
        assert_eq!(second.scanned, 0);
        assert_eq!(second.already_rejected, 1);
    }

    #[test]
    fn test_audit_empty_database() {
        let index = create_test_store();
        let report = audit_tsa_anchors(&index, false).expect("audit failed");
        assert_eq!(report.scanned, 0);
        assert_eq!(report.already_rejected, 0);
        assert!(report.bad.is_empty());
    }
}
