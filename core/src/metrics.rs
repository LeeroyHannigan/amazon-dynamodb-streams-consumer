//! Pluggable metrics sink (dependency-free core abstraction).
//!
//! The engine emits observations through a [`MetricsSink`]; concrete exporters
//! (OpenTelemetry/OTLP, CloudWatch EMF, a language-binding callback) implement
//! it without pulling any exporter dependency into `core`. The default
//! [`NoopSink`] makes metrics strictly opt-in and zero-cost when unused.
//!
//! The headline metric is [`ShardMetrics::millis_behind_latest`] — the
//! DynamoDB-Streams analog of Kinesis `MillisBehindLatest` (consumer lag),
//! which KCL/KCA expose as the primary health signal. Unlike Kinesis, DDB
//! Streams `GetRecords` returns no lag field, so the source derives it from the
//! newest record's `ApproximateCreationDateTime`.

use std::sync::Arc;

/// Per-batch observation, emitted once per delivered (non-empty) batch.
#[derive(Clone, Debug)]
pub struct ShardMetrics<'a> {
    pub shard_id: &'a str,
    /// Records delivered in this batch.
    pub records: u64,
    /// Total payload bytes delivered in this batch.
    pub bytes: u64,
    /// Consumer lag in ms (`now - newest ApproximateCreationDateTime`), when known.
    pub millis_behind_latest: Option<i64>,
}

/// Sink for engine metrics. All methods are cheap and non-blocking; an exporter
/// should aggregate/export off the hot path. Every method has a no-op default so
/// implementors only override what they emit.
pub trait MetricsSink: Send + Sync {
    /// A batch of records was delivered on a shard.
    fn on_batch(&self, _m: &ShardMetrics<'_>) {}
    /// A shard reached SHARD_END (completed).
    fn on_shard_end(&self, _shard_id: &str) {}
    /// The leader issued a `DescribeStream` (full sync or a CHILD_SHARDS query).
    fn on_describe_stream(&self) {}
    /// This worker acquired (or created) a lease.
    fn on_lease_acquired(&self, _shard_id: &str) {}
}

/// Default sink that records nothing — metrics are opt-in and cost nothing when
/// left disabled.
pub struct NoopSink;
impl MetricsSink for NoopSink {}

/// Convenience alias for a shared sink handed to the fleet.
pub type SharedMetricsSink = Arc<dyn MetricsSink>;

/// A shared [`NoopSink`].
pub fn noop_sink() -> SharedMetricsSink {
    Arc::new(NoopSink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// (shard_id, records, bytes, millis_behind_latest) captured per batch.
    type CapturedBatch = (String, u64, u64, Option<i64>);

    #[derive(Default)]
    struct Capture {
        batches: Mutex<Vec<CapturedBatch>>,
        describes: Mutex<u64>,
    }
    impl MetricsSink for Capture {
        fn on_batch(&self, m: &ShardMetrics<'_>) {
            self.batches.lock().unwrap().push((
                m.shard_id.to_string(),
                m.records,
                m.bytes,
                m.millis_behind_latest,
            ));
        }
        fn on_describe_stream(&self) {
            *self.describes.lock().unwrap() += 1;
        }
    }

    #[test]
    fn sink_receives_batch_and_lag() {
        let c = Capture::default();
        c.on_batch(&ShardMetrics {
            shard_id: "s0",
            records: 3,
            bytes: 120,
            millis_behind_latest: Some(450),
        });
        c.on_describe_stream();
        let b = c.batches.lock().unwrap();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0], ("s0".to_string(), 3, 120, Some(450)));
        assert_eq!(*c.describes.lock().unwrap(), 1);
    }

    #[test]
    fn noop_sink_is_inert() {
        let s = NoopSink;
        s.on_batch(&ShardMetrics {
            shard_id: "s",
            records: 1,
            bytes: 1,
            millis_behind_latest: Some(0),
        });
        s.on_shard_end("s");
        s.on_describe_stream();
        // Nothing to assert — just proving the default impls are callable/inert.
    }
}
