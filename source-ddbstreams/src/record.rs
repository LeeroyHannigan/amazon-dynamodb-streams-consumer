//! DynamoDB Streams payload for `core::Record.data`.
//!
//! The typed model ([`AttrValue`], [`Item`], [`StreamRecord`]) now lives in
//! `ddbstreams-kcl-core` so the DDB source and the binding/wire layer share one
//! type. This module re-exports it and adds the `aws-sdk-dynamodbstreams` →
//! [`StreamRecord`] converter behind the `aws` feature (it needs the SDK types).

pub use ddbstreams_kcl_core::record::{AttrValue, Item, StreamRecord};

#[cfg(feature = "aws")]
pub use from_sdk::from_sdk;

#[cfg(feature = "aws")]
mod from_sdk {
    use super::{AttrValue, Item, StreamRecord};
    use aws_sdk_dynamodbstreams::types::{AttributeValue as Sdk, Record as SdkRecord};

    fn attr(av: &Sdk) -> AttrValue {
        if let Ok(s) = av.as_s() {
            AttrValue::S(s.clone())
        } else if let Ok(n) = av.as_n() {
            AttrValue::N(n.clone())
        } else if let Ok(b) = av.as_bool() {
            AttrValue::Bool(*b)
        } else if av.as_null().is_ok() {
            AttrValue::Null
        } else if let Ok(b) = av.as_b() {
            AttrValue::B(b.as_ref().to_vec())
        } else if let Ok(m) = av.as_m() {
            AttrValue::M(map_btree(m))
        } else if let Ok(l) = av.as_l() {
            AttrValue::L(l.iter().map(attr).collect())
        } else if let Ok(ss) = av.as_ss() {
            AttrValue::Ss(ss.clone())
        } else if let Ok(ns) = av.as_ns() {
            AttrValue::Ns(ns.clone())
        } else if let Ok(bs) = av.as_bs() {
            AttrValue::Bs(bs.iter().map(|b| b.as_ref().to_vec()).collect())
        } else {
            AttrValue::Null
        }
    }

    fn map_btree(m: &std::collections::HashMap<String, Sdk>) -> Item {
        m.iter().map(|(k, v)| (k.clone(), attr(v))).collect()
    }

    /// Convert an `aws-sdk-dynamodbstreams` record into the typed core model.
    pub fn from_sdk(r: &SdkRecord) -> StreamRecord {
        let event_name = r.event_name().map(|e| e.as_str().to_string());
        let sr = r.dynamodb();
        StreamRecord {
            event_name,
            sequence_number: sr.and_then(|d| d.sequence_number()).map(|s| s.to_string()),
            size_bytes: sr.and_then(|d| d.size_bytes()),
            stream_view_type: sr.and_then(|d| d.stream_view_type()).map(|v| v.as_str().to_string()),
            keys: sr.and_then(|d| d.keys()).map(map_btree).unwrap_or_default(),
            new_image: sr.and_then(|d| d.new_image()).filter(|m| !m.is_empty()).map(map_btree),
            old_image: sr.and_then(|d| d.old_image()).filter(|m| !m.is_empty()).map(map_btree),
        }
    }
}
