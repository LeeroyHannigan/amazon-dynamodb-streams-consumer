//! OpenTelemetry (OTLP) metrics sink — "Model A": the sidecar exports metrics
//! directly over OTLP to whatever collector the customer runs, configured via
//! the standard `OTEL_EXPORTER_OTLP_*` / `OTEL_SERVICE_NAME` /
//! `OTEL_RESOURCE_ATTRIBUTES` env vars. Feature-gated (`otel`) so the default
//! build carries none of the OpenTelemetry dependency tree.
//!
//! Emits the KCL/KCA-parity signals (all dimensioned by `shard_id` where
//! applicable):
//!
//! - `ddbstreams.consumer.millis_behind_latest` (gauge, ms) — consumer lag
//! - `ddbstreams.consumer.records_processed` (counter)
//! - `ddbstreams.consumer.bytes_processed` (counter)
//! - `ddbstreams.consumer.describe_stream.count` (counter)
//! - `ddbstreams.consumer.shard_end.count` (counter)

use amazon_dynamodb_streams_consumer_core::metrics::{MetricsSink, ShardMetrics};
use opentelemetry::metrics::{Counter, Gauge, Meter, MeterProvider};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use std::time::Duration;

/// Holds the meter provider (kept alive for the process) + the instruments.
pub struct OtelMetricsSink {
    _provider: SdkMeterProvider,
    lag: Gauge<i64>,
    records: Counter<u64>,
    bytes: Counter<u64>,
    describes: Counter<u64>,
    shard_ends: Counter<u64>,
}

impl OtelMetricsSink {
    /// Build an OTLP-exporting sink from the ambient `OTEL_*` environment.
    /// Returns an error if the exporter cannot be constructed (e.g. bad endpoint).
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // The OTLP exporter reads OTEL_EXPORTER_OTLP_ENDPOINT / _HEADERS /
        // _PROTOCOL from the environment by default.
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .build()?;
        let interval_ms = std::env::var("OTEL_METRIC_EXPORT_INTERVAL")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10_000);
        let reader = PeriodicReader::builder(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_interval(Duration::from_millis(interval_ms))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter: Meter = provider.meter("amazon-dynamodb-streams-consumer");

        let lag = meter
            .i64_gauge("ddbstreams.consumer.millis_behind_latest")
            .with_description("Consumer lag: now - newest record ApproximateCreationDateTime")
            .with_unit("ms")
            .build();
        let records = meter
            .u64_counter("ddbstreams.consumer.records_processed")
            .with_description("Records delivered to the record processor")
            .build();
        let bytes = meter
            .u64_counter("ddbstreams.consumer.bytes_processed")
            .with_description("Payload bytes delivered")
            .with_unit("By")
            .build();
        let describes = meter
            .u64_counter("ddbstreams.consumer.describe_stream.count")
            .with_description("DescribeStream calls issued by the shard-sync leader")
            .build();
        let shard_ends = meter
            .u64_counter("ddbstreams.consumer.shard_end.count")
            .with_description("Shards that reached SHARD_END")
            .build();

        Ok(Self {
            _provider: provider,
            lag,
            records,
            bytes,
            describes,
            shard_ends,
        })
    }
}

impl MetricsSink for OtelMetricsSink {
    fn on_batch(&self, m: &ShardMetrics<'_>) {
        let attrs = [KeyValue::new("shard_id", m.shard_id.to_string())];
        self.records.add(m.records, &attrs);
        self.bytes.add(m.bytes, &attrs);
        if let Some(lag) = m.millis_behind_latest {
            self.lag.record(lag, &attrs);
        }
    }
    fn on_shard_end(&self, shard_id: &str) {
        self.shard_ends
            .add(1, &[KeyValue::new("shard_id", shard_id.to_string())]);
    }
    fn on_describe_stream(&self) {
        self.describes.add(1, &[]);
    }
}
