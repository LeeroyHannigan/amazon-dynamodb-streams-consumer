//! Live async adapter over `aws-sdk-dynamodbstreams` (Apache-2.0 AWS SDK).
//!
//! This is the thin glue that turns real `DescribeStream` / `GetShardIterator` /
//! `GetRecords` calls into the values the pure engine consumes. All of the
//! correctness-critical shard-graph logic lives in the parent module and is
//! reused here: [`crate::build_shard_graph`], [`crate::close_open_parents`].
//!
//! Compiled only under the `aws` feature (needs the AWS SDK + a tokio runtime).
//! Grounded in `DynamoDBStreamsShardDetector` / `DynamoDBStreamsDataFetcher`
//! (awslabs/dynamodb-streams-kinesis-adapter, Apache-2.0). See core/REFERENCES.md.

use crate::{build_shard_graph, close_open_parents, DdbShard};
use amazon_dynamodb_streams_consumer_core::{Record, RecordBatch, ShardMeta};
use aws_sdk_dynamodbstreams::types::ShardIteratorType;
use aws_sdk_dynamodbstreams::Client;
use std::collections::HashMap;
use std::sync::Mutex;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A threaded shard iterator: the `next_shard_iterator` handed back by the last
/// `GetRecords`, plus the logical position (`after`) it continues from. Reusing
/// it avoids a `GetShardIterator` call per poll — this is KCL's
/// `DynamoDBStreamsDataFetcher` behavior (hold the iterator, re-derive only on
/// reposition/expiry).
#[derive(Clone)]
struct Cursor {
    /// The `after` checkpoint this iterator continues from (`None` = TRIM_HORIZON).
    after: Option<String>,
    iterator: String,
}

/// A live DynamoDB Streams source bound to one stream ARN.
pub struct DdbStreamsSource {
    client: Client,
    stream_arn: String,
    /// Per-shard threaded iterators (shard id -> next iterator + its position).
    cursors: Mutex<HashMap<String, Cursor>>,
}

impl DdbStreamsSource {
    pub fn new(client: Client, stream_arn: impl Into<String>) -> Self {
        Self {
            client,
            stream_arn: stream_arn.into(),
            cursors: Mutex::new(HashMap::new()),
        }
    }

    /// Build a source from the ambient AWS environment (creds, region).
    pub async fn from_env(stream_arn: impl Into<String>) -> Self {
        let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Self::new(Client::new(&cfg), stream_arn)
    }

    /// Full paginated `DescribeStream` → normalized shards → `close_open_parents`
    /// → shard-graph lineage. This is the live `describe_shards`.
    pub async fn describe_shards(&self) -> Result<Vec<ShardMeta>, BoxError> {
        let mut raw: Vec<DdbShard> = Vec::new();
        let mut start: Option<String> = None;
        loop {
            let resp = self
                .client
                .describe_stream()
                .stream_arn(&self.stream_arn)
                .set_exclusive_start_shard_id(start.clone())
                .send()
                .await?;
            let Some(desc) = resp.stream_description() else {
                break;
            };
            for s in desc.shards() {
                let shard_id = s.shard_id().unwrap_or_default().to_string();
                if shard_id.is_empty() {
                    continue;
                }
                let parent_shard_id = s.parent_shard_id().map(|p| p.to_string());
                let ending_sequence_number = s
                    .sequence_number_range()
                    .and_then(|r| r.ending_sequence_number())
                    .map(|e| e.to_string());
                raw.push(DdbShard {
                    shard_id,
                    parent_shard_id,
                    ending_sequence_number,
                });
            }
            match desc.last_evaluated_shard_id() {
                Some(id) => start = Some(id.to_string()),
                None => break,
            }
        }
        // Phase 2 (close open parents) then build the lineage graph.
        let normalized = close_open_parents(raw);
        Ok(build_shard_graph(vec![normalized]))
    }

    /// Derive a *fresh* iterator from the stream via `GetShardIterator`
    /// (`AFTER_SEQUENCE_NUMBER` when resuming from a checkpoint, else
    /// `TRIM_HORIZON`). Used on first read, reposition, or after an
    /// expired/trimmed iterator — not on the steady-state poll path.
    async fn derive_iterator(
        &self,
        shard: &str,
        after: Option<&str>,
    ) -> Result<Option<String>, BoxError> {
        let (iter_type, seq) = match after {
            Some(s) => (ShardIteratorType::AfterSequenceNumber, Some(s.to_string())),
            None => (ShardIteratorType::TrimHorizon, None),
        };
        let resp = self
            .client
            .get_shard_iterator()
            .stream_arn(&self.stream_arn)
            .shard_id(shard)
            .shard_iterator_type(iter_type)
            .set_sequence_number(seq)
            .send()
            .await?;
        Ok(resp.shard_iterator().map(|s| s.to_string()))
    }

    /// Return a reusable threaded iterator for `shard` iff a cached cursor
    /// continues from exactly the requested `after` position. A mismatch means
    /// the caller is repositioning (or this is a fresh/restarted process), so we
    /// must not reuse.
    fn cached_iterator(&self, shard: &str, after: Option<&str>) -> Option<String> {
        let cursors = self.cursors.lock().unwrap();
        cursors
            .get(shard)
            .filter(|c| cursor_continues(c.after.as_deref(), after))
            .map(|c| c.iterator.clone())
    }

    fn store_cursor(&self, shard: &str, after: Option<String>, iterator: Option<String>) {
        let mut cursors = self.cursors.lock().unwrap();
        match iterator {
            Some(it) => {
                cursors.insert(
                    shard.to_string(),
                    Cursor {
                        after,
                        iterator: it,
                    },
                );
            }
            None => {
                cursors.remove(shard); // SHARD_END → nothing more to thread.
            }
        }
    }

    fn drop_cursor(&self, shard: &str) {
        self.cursors.lock().unwrap().remove(shard);
    }

    /// One `GetRecords` round after the opaque checkpoint `after` (`None` =
    /// `TRIM_HORIZON`). Returns the batch and whether the shard is closed
    /// (`next_shard_iterator == None` → SHARD_END).
    ///
    /// Reuses the threaded `next_shard_iterator` from the previous poll when it
    /// continues from the same `after` (avoiding a `GetShardIterator` per call);
    /// otherwise derives a fresh iterator. Self-heals the two recoverable
    /// iterator failures the adapter is expected to handle:
    /// `TrimmedDataAccessException` and `ExpiredIteratorException` → drop the
    /// stale cursor and restart from `after`/`TRIM_HORIZON` (matches
    /// `DynamoDBStreamsDataFetcher`).
    pub async fn get_records(
        &self,
        shard: &str,
        after: Option<&str>,
    ) -> Result<RecordBatch, BoxError> {
        // 1) Obtain an iterator: reuse the threaded one, else derive fresh.
        let iterator = match self.cached_iterator(shard, after) {
            Some(it) => it,
            None => match self.derive_iterator(shard, after).await {
                Ok(Some(it)) => it,
                Ok(None) => {
                    self.drop_cursor(shard);
                    return Ok(RecordBatch {
                        records: vec![],
                        shard_end: true,
                    });
                }
                Err(e) if is_recoverable(&e) && after.is_some() => {
                    // Checkpoint too old → restart at TRIM_HORIZON.
                    match self.derive_iterator(shard, None).await? {
                        Some(it) => it,
                        None => {
                            return Ok(RecordBatch {
                                records: vec![],
                                shard_end: true,
                            })
                        }
                    }
                }
                Err(e) => return Err(e),
            },
        };

        // 2) GetRecords, self-healing an expired/trimmed threaded iterator once.
        let resp = match self
            .client
            .get_records()
            .shard_iterator(&iterator)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let be: BoxError = e.into();
                if is_recoverable(&be) {
                    // The (possibly cached) iterator expired → drop it and
                    // re-derive from the checkpoint, retrying once.
                    self.drop_cursor(shard);
                    let fresh = match self.derive_iterator(shard, after).await? {
                        Some(it) => it,
                        None => {
                            return Ok(RecordBatch {
                                records: vec![],
                                shard_end: true,
                            })
                        }
                    };
                    self.client
                        .get_records()
                        .shard_iterator(&fresh)
                        .send()
                        .await?
                } else {
                    return Err(be);
                }
            }
        };

        let mut records = Vec::new();
        for r in resp.records() {
            if let Some(sr) = r.dynamodb() {
                let seq = sr.sequence_number().unwrap_or_default().to_string();
                if seq.is_empty() {
                    continue;
                }
                // Carry the full typed change record (Keys/NewImage/OldImage/
                // eventName) as the opaque payload, per KCL's RecordAdapter model.
                let payload = crate::record::from_sdk(r).encode();
                records.push(Record {
                    shard_id: shard.to_string(),
                    seq,
                    data: payload,
                });
            }
        }
        // 3) Thread the next iterator. The cursor's logical position advances to
        // the last delivered seq (or stays at `after` if this poll was empty).
        let next = resp.next_shard_iterator().map(|s| s.to_string());
        let new_after = advanced_after(after, records.last().map(|r| r.seq.as_str()));
        self.store_cursor(shard, new_after, next.clone());
        // A closed shard yields no next iterator.
        let shard_end = next.is_none();
        Ok(RecordBatch { records, shard_end })
    }
}

/// A cached cursor continues the caller's read iff it is positioned at exactly
/// the requested `after`. A mismatch means a reposition (or fresh/restarted
/// process), so the threaded iterator must NOT be reused.
fn cursor_continues(cursor_after: Option<&str>, requested: Option<&str>) -> bool {
    cursor_after == requested
}

/// The cursor's new logical position after a poll: the last delivered seq, or
/// the unchanged `requested` position if the poll was empty.
fn advanced_after(requested: Option<&str>, last_seq: Option<&str>) -> Option<String> {
    last_seq.or(requested).map(|s| s.to_string())
}

/// Trimmed-data / expired-iterator / resource-not-found are recoverable by
/// restarting the shard at `TRIM_HORIZON`.
fn is_recoverable(e: &BoxError) -> bool {
    let msg = e.to_string();
    msg.contains("TrimmedDataAccessException")
        || msg.contains("ExpiredIteratorException")
        || msg.contains("ResourceNotFoundException")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_reused_only_when_it_continues_from_requested_position() {
        // Same position → reuse the threaded iterator.
        assert!(cursor_continues(Some("seq-5"), Some("seq-5")));
        assert!(cursor_continues(None, None)); // both at TRIM_HORIZON
                                               // Reposition / restart → do not reuse.
        assert!(!cursor_continues(Some("seq-5"), Some("seq-9")));
        assert!(!cursor_continues(Some("seq-5"), None));
        assert!(!cursor_continues(None, Some("seq-5")));
    }

    #[test]
    fn cursor_position_advances_to_last_seq_else_holds() {
        // Records delivered → advance to the last seq.
        assert_eq!(advanced_after(Some("5"), Some("8")).as_deref(), Some("8"));
        assert_eq!(advanced_after(None, Some("1")).as_deref(), Some("1"));
        // Empty poll → hold the requested position (open shard keeps polling).
        assert_eq!(advanced_after(Some("5"), None).as_deref(), Some("5"));
        assert_eq!(advanced_after(None, None), None);
    }

    #[test]
    fn recoverable_errors_are_classified() {
        let mk = |s: &str| -> BoxError { s.to_string().into() };
        assert!(is_recoverable(&mk(
            "ExpiredIteratorException: iterator expired"
        )));
        assert!(is_recoverable(&mk(
            "com.amazonaws...TrimmedDataAccessException"
        )));
        assert!(is_recoverable(&mk("ResourceNotFoundException")));
        assert!(!is_recoverable(&mk("ValidationException: bad input")));
        assert!(!is_recoverable(&mk("some other service error")));
    }
}
