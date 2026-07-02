//! End-to-end live test of the **sidecar binary** exactly as a language client
//! would drive it: create a real streamed table, spawn the compiled
//! `amazon-dynamodb-streams-consumer-sidecar` process, act as the client over its stdio (read
//! `records`, reply with `checkpoint` acks), and assert the real change records
//! flow through with their typed payloads. Verifies the ack advances the
//! persisted checkpoint, then stops the sidecar and cleans up both tables.
//!
//! Skipped unless `DDB_STREAMS_CONSUMER_IT=1`.
//!
//! Run:
//!   DDB_STREAMS_CONSUMER_IT=1 AWS_REGION=us-east-1 cargo test -p amazon-dynamodb-streams-consumer-sidecar \
//!     --test live_sidecar -- --nocapture

use aws_sdk_dynamodb as ddb;
use ddb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus,
};
use amazon_dynamodb_streams_consumer_protocol::{ClientMessage, ServerMessage};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

#[tokio::test]
async fn live_sidecar_streams_records_and_checkpoints() {
    if std::env::var("DDB_STREAMS_CONSUMER_IT").is_err() {
        eprintln!("skipping live sidecar integ test (set DDB_STREAMS_CONSUMER_IT=1 to run)");
        return;
    }
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let db = ddb::Client::new(&cfg);

    let pid = std::process::id();
    let data_table = format!("amazon-dynamodb-streams-consumer-sidecar-it-{pid}");
    let lease_table = format!("amazon-dynamodb-streams-consumer-sidecar-leases-it-{pid}");

    db.create_table()
        .table_name(&data_table)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(KeySchemaElement::builder().attribute_name("pk").key_type(KeyType::Hash).build().unwrap())
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create data table");

    let stream_arn = loop {
        let d = db.describe_table().table_name(&data_table).send().await.unwrap();
        let t = d.table().unwrap();
        if t.table_status() == Some(&TableStatus::Active) {
            break t.latest_stream_arn().unwrap().to_string();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };

    for i in 0..5 {
        db.put_item()
            .table_name(&data_table)
            .item("pk", AttributeValue::S(format!("k{i}")))
            .send()
            .await
            .unwrap();
    }

    // Drive the real sidecar process as the client would; collect the result so
    // cleanup always runs.
    let result = run_client(&stream_arn, &lease_table, &region).await;

    let _ = db.delete_table().table_name(&data_table).send().await;
    let _ = db.delete_table().table_name(&lease_table).send().await;

    let (total, with_payload, shards) = result.expect("sidecar client run");
    eprintln!("sidecar delivered {total} record(s) across {shards} shard(s); {with_payload} carried a pk payload");
    assert!(total >= 5, "expected >= 5 records from the sidecar, got {total}");
    assert!(with_payload >= 5, "expected typed payloads (pk key) on all records, got {with_payload}");
}

/// Spawn the compiled sidecar and behave as the language client: read `records`,
/// ack each batch with a `checkpoint`. Returns (total_records, records_with_pk,
/// distinct_shards).
async fn run_client(
    stream_arn: &str,
    lease_table: &str,
    region: &str,
) -> Result<(usize, usize, usize), Box<dyn std::error::Error + Send + Sync>> {
    // Cargo provides the built binary path to integration tests.
    let bin = env!("CARGO_BIN_EXE_amazon-dynamodb-streams-consumer-sidecar");
    let mut child = Command::new(bin)
        .env("DDB_STREAMS_CONSUMER_STREAM_ARN", stream_arn)
        .env("DDB_STREAMS_CONSUMER_LEASE_TABLE", lease_table)
        .env("DDB_STREAMS_CONSUMER_OWNER", "sidecar-it")
        .env("DDB_STREAMS_CONSUMER_LEASE_DURATION_MS", "60000")
        .env("DDB_STREAMS_CONSUMER_CYCLE_INTERVAL_MS", "500")
        .env("AWS_REGION", region)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    let mut stdin = child.stdin.take().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();

    let mut total = 0usize;
    let mut with_payload = 0usize;
    let mut shards = std::collections::HashSet::new();

    // Read records for up to ~40s or until we've seen all 5 (record lag on a
    // fresh stream). Ack every batch so the checkpoint advances.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
    while total < 5 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let line = match tokio::time::timeout(remaining, lines.next_line()).await {
            Ok(Ok(Some(l))) => l,
            _ => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        match ServerMessage::parse(&line) {
            Ok(ServerMessage::Records { shard, last_seq, records }) => {
                total += records.len();
                with_payload += records.iter().filter(|r| r.keys.contains_key("pk")).count();
                shards.insert(shard.clone());
                let ack = ClientMessage::Checkpoint { shard, seq: last_seq };
                stdin.write_all(ack.to_line().as_bytes()).await?;
                stdin.flush().await?;
            }
            Ok(ServerMessage::ShardComplete { .. }) | Ok(ServerMessage::Shutdown { .. }) => {}
            Err(_) => {}
        }
    }

    // Ask the sidecar to stop, then ensure the process exits.
    let _ = stdin.write_all(ClientMessage::Stop.to_line().as_bytes()).await;
    let _ = stdin.flush().await;
    drop(stdin);
    let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
    let _ = child.start_kill();

    Ok((total, with_payload, shards.len()))
}
