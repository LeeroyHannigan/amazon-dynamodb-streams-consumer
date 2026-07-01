#![cfg(feature = "aws")]
//! End-to-end live integration test: the `Worker` composes a real
//! `DdbStreamsSource` + `DynamoDbLeaseStore` + a recording processor and
//! consumes a real DynamoDB stream, acquiring a lease and checkpointing in
//! DynamoDB. Skipped unless `DDBSTREAMS_KCL_IT=1`. Creates + deletes its own
//! data table and lease table.
//!
//! Run:
//!   DDBSTREAMS_KCL_IT=1 cargo test -p ddbstreams-kcl-worker \
//!     --features aws --test live_worker -- --nocapture

use aws_sdk_dynamodb as ddb;
use aws_sdk_dynamodbstreams as streams;
use ddb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus,
};
use ddbstreams_kcl_core::{Record, RecordProcessor, ShardId};
use ddbstreams_kcl_lease_dynamodb::dynamodb::DynamoDbLeaseStore;
use ddbstreams_kcl_source_ddbstreams::aws::DdbStreamsSource;
use ddbstreams_kcl_worker::Worker;
use std::time::Duration;

#[derive(Default)]
struct Recording {
    seqs: Vec<String>,
    shard: Option<String>,
}
impl RecordProcessor for Recording {
    fn initialize(&mut self, _s: &ShardId) {}
    fn process_records(&mut self, rs: &[Record]) {
        for r in rs {
            self.shard = Some(r.shard_id.clone());
            self.seqs.push(r.seq.clone());
        }
    }
    fn shard_ended(&mut self, _s: &ShardId) {}
}

#[tokio::test]
async fn live_worker_consumes_and_checkpoints() {
    if std::env::var("DDBSTREAMS_KCL_IT").is_err() {
        eprintln!("skipping live worker integ test (set DDBSTREAMS_KCL_IT=1 to run)");
        return;
    }

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let db = ddb::Client::new(&cfg);
    let st = streams::Client::new(&cfg);

    let pid = std::process::id();
    let data_table = format!("ddbstreams-kcl-worker-it-{pid}");
    let lease_table = format!("ddbstreams-kcl-worker-leases-it-{pid}");

    // --- data table with streams ---
    db.create_table()
        .table_name(&data_table)
        .attribute_definitions(AttributeDefinition::builder().attribute_name("pk").attribute_type(ScalarAttributeType::S).build().unwrap())
        .key_schema(KeySchemaElement::builder().attribute_name("pk").key_type(KeyType::Hash).build().unwrap())
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(StreamSpecification::builder().stream_enabled(true).stream_view_type(StreamViewType::NewAndOldImages).build().unwrap())
        .send().await.expect("create data table");

    let stream_arn = loop {
        let d = db.describe_table().table_name(&data_table).send().await.unwrap();
        let t = d.table().unwrap();
        if t.table_status() == Some(&TableStatus::Active) {
            break t.latest_stream_arn().unwrap().to_string();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };

    for i in 0..5 {
        db.put_item().table_name(&data_table)
            .item("pk", AttributeValue::S(format!("k{i}")))
            .send().await.unwrap();
    }

    let outcome = run_worker(&cfg, &st, &stream_arn, &lease_table).await;

    // cleanup both tables
    let _ = db.delete_table().table_name(&data_table).send().await;
    let _ = db.delete_table().table_name(&lease_table).send().await;

    let (count, checkpoint) = outcome.expect("worker run");
    eprintln!("worker consumed {count} records; lease checkpoint = {checkpoint:?}");
    assert!(count >= 5, "expected >= 5 records, got {count}");
    assert!(checkpoint.is_some(), "expected a lease checkpoint to be persisted");
}

async fn run_worker(
    cfg: &aws_config::SdkConfig,
    st: &streams::Client,
    stream_arn: &str,
    lease_table: &str,
) -> Result<(usize, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
    let source = DdbStreamsSource::new(st.clone(), stream_arn);
    let leases = DynamoDbLeaseStore::from_env(lease_table).await;
    leases.ensure_table().await?;
    // Second handle (same table) for post-run assertions.
    let leases_check = DynamoDbLeaseStore::new(ddb::Client::new(cfg), lease_table);

    let worker = Worker::new(source, leases, "worker-1");
    let mut proc = Recording::default();

    // Step the worker; the shard is open (no SHARD_END), and stream records lag
    // writes, so poll a bounded number of cycles until we've seen all 5.
    for _ in 0..25 {
        let _ = worker.run_once(&mut proc).await?;
        if proc.seqs.len() >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Records within a shard must be delivered in order.
    let mut sorted = proc.seqs.clone();
    // DDB Streams sequence numbers are stringified big integers; compare by
    // (length, lexical) to reflect numeric magnitude for the ordering assertion.
    sorted.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
    assert_eq!(proc.seqs, sorted, "records not delivered in sequence order");

    let checkpoint = match &proc.shard {
        Some(shard) => leases_check.get(shard).await?.and_then(|l| l.checkpoint),
        None => None,
    };
    Ok((proc.seqs.len(), checkpoint))
}
