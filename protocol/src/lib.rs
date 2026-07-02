//! Wire protocol between the `amazon-dynamodb-streams-consumer` **sidecar** (the Rust consumer
//! process) and a **language binding** (the thin client library embedded in the
//! customer's app). This is the JVM-free analog of KCL's MultiLangDaemon
//! protocol: the sidecar owns all coordination (shard discovery, leases,
//! ordering, checkpoints) and streams ordered record batches to the client; the
//! client processes them in the customer's language and acks with a checkpoint.
//!
//! ## Framing
//! Newline-delimited JSON ("JSON Lines"): one message per line, each a JSON
//! object tagged by `type`. Records never contain raw newlines (they are JSON),
//! so `\n` is an unambiguous message delimiter. Use [`ServerMessage::to_line`]
//! / [`ClientMessage::to_line`] to write and [`ServerMessage::parse`] /
//! [`ClientMessage::parse`] to read a single (already newline-stripped) line.
//!
//! ## Checkpoint semantics (at-least-once)
//! The sidecar sends [`ServerMessage::Records`] with the batch's `last_seq`. The
//! client processes the batch and, once durable, replies with
//! [`ClientMessage::Checkpoint`] carrying that `last_seq`; only then does the
//! sidecar persist the checkpoint under the optimistic lock. If the client dies
//! before acking, the lease is not advanced and another worker re-delivers from
//! the last committed checkpoint — at-least-once, exactly like KCL.

use amazon_dynamodb_streams_consumer_core::record::StreamRecord;
use serde::{Deserialize, Serialize};

/// Messages the **sidecar → client** (records to process, lifecycle signals).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// A batch of records for one shard, in sequence order. The client processes
    /// them and replies with [`ClientMessage::Checkpoint`] carrying `last_seq`
    /// once they are durably handled.
    Records {
        shard: String,
        /// The sequence number of the last record in `records` — the token to
        /// ack in the checkpoint reply.
        last_seq: String,
        records: Vec<StreamRecord>,
    },
    /// The shard reached SHARD_END; no more records will arrive for it. Its
    /// children (if any) will begin once their parents complete.
    ShardComplete { shard: String },
    /// The sidecar is shutting down (graceful stop or fatal error).
    Shutdown { reason: String },
}

/// Messages the **client → sidecar** (handshake, checkpoint acks, stop).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Handshake: the client is ready to receive records.
    Ready,
    /// Ack that records up to `seq` on `shard` are durably processed → the
    /// sidecar advances the checkpoint under the optimistic lock.
    Checkpoint { shard: String, seq: String },
    /// The client is shutting down; the sidecar should release its leases.
    Stop,
}

/// A malformed protocol line.
pub type ParseError = serde_json::Error;

impl ServerMessage {
    /// Serialize to a single JSON line **without** the trailing newline.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ServerMessage serialize")
    }
    /// Serialize to a framed line **with** the trailing newline.
    pub fn to_line(&self) -> String {
        let mut s = self.to_json();
        s.push('\n');
        s
    }
    /// Parse one newline-stripped line.
    pub fn parse(line: &str) -> Result<Self, ParseError> {
        serde_json::from_str(line.trim_end_matches(['\n', '\r']))
    }
}

impl ClientMessage {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ClientMessage serialize")
    }
    pub fn to_line(&self) -> String {
        let mut s = self.to_json();
        s.push('\n');
        s
    }
    pub fn parse(line: &str) -> Result<Self, ParseError> {
        serde_json::from_str(line.trim_end_matches(['\n', '\r']))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amazon_dynamodb_streams_consumer_core::record::{AttrValue, Item};

    fn sample_record(seq: &str) -> StreamRecord {
        let mut keys = Item::new();
        keys.insert("pk".into(), AttrValue::S("k1".into()));
        StreamRecord {
            event_name: Some("INSERT".into()),
            sequence_number: Some(seq.into()),
            stream_view_type: Some("NEW_AND_OLD_IMAGES".into()),
            keys,
            ..Default::default()
        }
    }

    #[test]
    fn records_message_round_trips() {
        let msg = ServerMessage::Records {
            shard: "shardId-0001".into(),
            last_seq: "100000000000000000009".into(),
            records: vec![sample_record("100000000000000000008"), sample_record("100000000000000000009")],
        };
        let line = msg.to_line();
        assert!(line.ends_with('\n'));
        assert!(!line.trim_end().contains('\n'), "exactly one line");
        assert_eq!(ServerMessage::parse(&line).unwrap(), msg);
    }

    #[test]
    fn lifecycle_messages_round_trip() {
        for msg in [
            ServerMessage::ShardComplete { shard: "s1".into() },
            ServerMessage::Shutdown { reason: "SIGTERM".into() },
        ] {
            assert_eq!(ServerMessage::parse(&msg.to_line()).unwrap(), msg);
        }
    }

    #[test]
    fn client_messages_round_trip() {
        for msg in [
            ClientMessage::Ready,
            ClientMessage::Checkpoint { shard: "s1".into(), seq: "42".into() },
            ClientMessage::Stop,
        ] {
            assert_eq!(ClientMessage::parse(&msg.to_line()).unwrap(), msg);
        }
    }

    #[test]
    fn type_tag_is_stable_snake_case() {
        // The wire tag is part of the contract with non-Rust clients; pin it.
        let j = ServerMessage::ShardComplete { shard: "s1".into() }.to_json();
        assert!(j.contains(r#""type":"shard_complete""#), "got {j}");
        let c = ClientMessage::Checkpoint { shard: "s1".into(), seq: "9".into() }.to_json();
        assert!(c.contains(r#""type":"checkpoint""#), "got {c}");
    }

    #[test]
    fn unknown_message_is_a_clean_error_not_a_panic() {
        assert!(ServerMessage::parse(r#"{"type":"bogus"}"#).is_err());
        assert!(ClientMessage::parse("not json").is_err());
    }
}
