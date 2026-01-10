// File: src/storage/sqlite/convert.rs

use super::lifecycle::{AnchorWithId, TreeRecord, TreeStatus};
use crate::traits::{Anchor, AnchorType, Entry};
use rusqlite::Row;

/// Convert a database row to Entry
pub fn row_to_entry(row: &Row) -> rusqlite::Result<Entry> {
    let id: String = row.get(0)?;
    let payload_hash: Vec<u8> = row.get(1)?;
    let metadata_hash: Vec<u8> = row.get(2)?;
    let metadata_cleartext: Option<String> = row.get(3)?;
    let external_id: Option<String> = row.get(4)?;
    let leaf_index: Option<i64> = row.get(5)?;
    let _leaf_hash: Vec<u8> = row.get(6)?;
    let created_at: i64 = row.get(7)?;

    Ok(Entry {
        id: id.parse().map_err(|_| {
            rusqlite::Error::InvalidColumnType(0, "id".into(), rusqlite::types::Type::Text)
        })?,
        payload_hash: payload_hash.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                1,
                "payload_hash".into(),
                rusqlite::types::Type::Blob,
            )
        })?,
        metadata_hash: metadata_hash.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                2,
                "metadata_hash".into(),
                rusqlite::types::Type::Blob,
            )
        })?,
        metadata_cleartext: metadata_cleartext.and_then(|s| serde_json::from_str(&s).ok()),
        external_id,
        leaf_index: leaf_index.map(|i| i as u64),
        created_at: chrono::DateTime::from_timestamp_nanos(created_at),
    })
}

/// Convert a database row to Checkpoint
pub fn row_to_checkpoint(row: &Row) -> rusqlite::Result<atl_core::Checkpoint> {
    let tree_size: i64 = row.get(0)?;
    let origin: Vec<u8> = row.get(1)?;
    let timestamp: i64 = row.get(2)?;
    let root_hash: Vec<u8> = row.get(3)?;
    let signature: Vec<u8> = row.get(4)?;
    let key_id: Vec<u8> = row.get(5)?;

    Ok(atl_core::Checkpoint {
        origin: origin.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(1, "origin".into(), rusqlite::types::Type::Blob)
        })?,
        tree_size: tree_size as u64,
        timestamp: timestamp as u64,
        root_hash: root_hash.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(3, "root_hash".into(), rusqlite::types::Type::Blob)
        })?,
        signature: signature.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(4, "signature".into(), rusqlite::types::Type::Blob)
        })?,
        key_id: key_id.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(5, "key_id".into(), rusqlite::types::Type::Blob)
        })?,
    })
}

/// Convert a database row to Anchor
pub fn row_to_anchor(row: &Row) -> rusqlite::Result<Anchor> {
    let _id: i64 = row.get(0)?;
    let _tree_size: i64 = row.get(1)?;
    let anchor_type: String = row.get(2)?;
    let anchored_hash: Vec<u8> = row.get(3)?;
    let timestamp: i64 = row.get(4)?;
    let token: Vec<u8> = row.get(5)?;
    let metadata: Option<String> = row.get(6)?;

    let anchor_type = match anchor_type.as_str() {
        "rfc3161" => AnchorType::Rfc3161,
        "bitcoin_ots" => AnchorType::BitcoinOts,
        _ => AnchorType::Other,
    };

    Ok(Anchor {
        anchor_type,
        anchored_hash: anchored_hash.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                3,
                "anchored_hash".into(),
                rusqlite::types::Type::Blob,
            )
        })?,
        timestamp: timestamp as u64,
        token,
        metadata: metadata
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default(),
    })
}

/// Convert a database row to AnchorWithId (used by BACKGROUND-1)
#[allow(dead_code)]
pub(super) fn row_to_anchor_with_id(row: &Row) -> rusqlite::Result<AnchorWithId> {
    let id: i64 = row.get(0)?;
    let _tree_size: i64 = row.get(1)?;
    let anchor_type: String = row.get(2)?;
    let anchored_hash: Vec<u8> = row.get(3)?;
    let timestamp: i64 = row.get(4)?;
    let token: Vec<u8> = row.get(5)?;
    let metadata: Option<String> = row.get(6)?;
    let _status: String = row.get(7)?;

    let anchor_type = match anchor_type.as_str() {
        "rfc3161" => AnchorType::Rfc3161,
        "bitcoin_ots" => AnchorType::BitcoinOts,
        _ => AnchorType::Other,
    };

    Ok(AnchorWithId {
        id,
        anchor: Anchor {
            anchor_type,
            anchored_hash: anchored_hash.try_into().map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    3,
                    "anchored_hash".into(),
                    rusqlite::types::Type::Blob,
                )
            })?,
            timestamp: timestamp as u64,
            token,
            metadata: metadata
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
        },
    })
}

/// Convert a database row to TreeRecord
pub fn row_to_tree(row: &Row) -> rusqlite::Result<TreeRecord> {
    let id: i64 = row.get(0)?;
    let origin_id: Vec<u8> = row.get(1)?;
    let status: String = row.get(2)?;
    let start_size: i64 = row.get(3)?;
    let end_size: Option<i64> = row.get(4)?;
    let root_hash: Option<Vec<u8>> = row.get(5)?;
    let created_at: i64 = row.get(6)?;
    let first_entry_at: Option<i64> = row.get(7)?;
    let closed_at: Option<i64> = row.get(8)?;
    let tsa_anchor_id: Option<i64> = row.get(9)?;
    let bitcoin_anchor_id: Option<i64> = row.get(10)?;

    Ok(TreeRecord {
        id,
        origin_id: origin_id.try_into().map_err(|_| {
            rusqlite::Error::InvalidColumnType(1, "origin_id".into(), rusqlite::types::Type::Blob)
        })?,
        status: TreeStatus::parse(&status).unwrap_or(TreeStatus::Active),
        start_size: start_size as u64,
        end_size: end_size.map(|s| s as u64),
        root_hash: root_hash.and_then(|h| h.try_into().ok()),
        created_at,
        first_entry_at,
        closed_at,
        tsa_anchor_id,
        bitcoin_anchor_id,
    })
}
