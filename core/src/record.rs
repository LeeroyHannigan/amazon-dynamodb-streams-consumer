//! Typed DynamoDB Streams change record — the payload carried in
//! [`crate::Record::data`] and delivered to language bindings over the wire.
//!
//! Follows KCL's `RecordAdapter` pattern: the ordering engine treats the record
//! payload as opaque bytes; this typed model is what the DDB source encodes and
//! what a binding decodes. [`AttrValue`] mirrors the DynamoDB attribute model
//! exactly (S/N/B/BOOL/NULL/M/L/SS/NS/BS) so item images round-trip losslessly.
//!
//! This lives in `core` (dependency-free apart from serde) so both the DDB
//! source and the binding/wire layer share ONE record type — no duplication.
//! The `aws-sdk-dynamodbstreams` → [`StreamRecord`] converter lives in
//! `source-ddbstreams` behind its `aws` feature (it needs the SDK types).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A DynamoDB attribute value (the full type set). `BTreeMap` keeps map key
/// order deterministic for stable encoding/tests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AttrValue {
    S(String),
    /// Numbers are carried as their canonical string form (as DynamoDB does).
    N(String),
    Bool(bool),
    Null,
    B(Vec<u8>),
    M(BTreeMap<String, AttrValue>),
    L(Vec<AttrValue>),
    Ss(Vec<String>),
    Ns(Vec<String>),
    Bs(Vec<Vec<u8>>),
}

pub type Item = BTreeMap<String, AttrValue>;

/// One item-level change from a DynamoDB stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct StreamRecord {
    /// INSERT / MODIFY / REMOVE.
    pub event_name: Option<String>,
    pub sequence_number: Option<String>,
    pub size_bytes: Option<i64>,
    /// KEYS_ONLY / NEW_IMAGE / OLD_IMAGE / NEW_AND_OLD_IMAGES.
    pub stream_view_type: Option<String>,
    pub keys: Item,
    pub new_image: Option<Item>,
    pub old_image: Option<Item>,
}

impl StreamRecord {
    /// Encode into the opaque bytes carried by [`crate::Record::data`].
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StreamRecord serialize")
    }

    /// Decode a [`crate::Record::data`] payload produced by [`StreamRecord::encode`].
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_nested_item_images() {
        let mut keys = Item::new();
        keys.insert("pk".into(), AttrValue::S("k1".into()));
        keys.insert("sk".into(), AttrValue::N("42".into()));

        let mut new_image = keys.clone();
        new_image.insert("active".into(), AttrValue::Bool(true));
        new_image.insert("tags".into(), AttrValue::Ss(vec!["a".into(), "b".into()]));
        new_image.insert(
            "nested".into(),
            AttrValue::M(BTreeMap::from([
                ("count".to_string(), AttrValue::N("3".into())),
                (
                    "list".to_string(),
                    AttrValue::L(vec![AttrValue::Null, AttrValue::S("x".into())]),
                ),
            ])),
        );

        let rec = StreamRecord {
            event_name: Some("MODIFY".into()),
            sequence_number: Some("100000000000000000001".into()),
            size_bytes: Some(128),
            stream_view_type: Some("NEW_AND_OLD_IMAGES".into()),
            keys,
            new_image: Some(new_image),
            old_image: None,
        };

        let decoded = StreamRecord::decode(&rec.encode()).unwrap();
        assert_eq!(decoded, rec);
    }

    #[test]
    fn binary_and_number_set_round_trip() {
        let mut keys = Item::new();
        keys.insert("b".into(), AttrValue::B(vec![0, 1, 2, 255]));
        keys.insert("ns".into(), AttrValue::Ns(vec!["1".into(), "2.5".into()]));
        let rec = StreamRecord {
            keys,
            ..Default::default()
        };
        assert_eq!(StreamRecord::decode(&rec.encode()).unwrap(), rec);
    }
}
