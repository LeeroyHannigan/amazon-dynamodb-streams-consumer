#![cfg(feature = "aws")]
//! Live integration test for the DynamoDB lease store. Skipped unless
//! `DDB_STREAMS_CONSUMER_IT=1`. Proves the optimistic-lock cycle against real DynamoDB:
//! acquire → renew → checkpoint, and that a stale-counter renew is rejected
//! (`LeaseError::Lost`). Creates and deletes its own lease table.
//!
//! Run:
//!   DDB_STREAMS_CONSUMER_IT=1 cargo test -p amazon-dynamodb-streams-consumer-lease-dynamodb \
//!     --features aws --test live_lease -- --nocapture

use aws_sdk_dynamodb as ddb;
use amazon_dynamodb_streams_consumer_lease_dynamodb::dynamodb::{DynamoDbLeaseStore, LeaseError};

#[tokio::test]
async fn live_optimistic_lock_lease_cycle() {
    if std::env::var("DDB_STREAMS_CONSUMER_IT").is_err() {
        eprintln!("skipping live lease integ test (set DDB_STREAMS_CONSUMER_IT=1 to run)");
        return;
    }

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = ddb::Client::new(&cfg);
    let table = format!("amazon-dynamodb-streams-consumer-leases-it-{}", std::process::id());

    let store = DynamoDbLeaseStore::new(client.clone(), &table);
    store.ensure_table().await.expect("ensure_table");

    let outcome = run_cycle(&store).await;

    // Best-effort cleanup.
    let _ = client.delete_table().table_name(&table).send().await;

    outcome.expect("lease cycle");
}

async fn run_cycle(store: &DynamoDbLeaseStore) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Acquire (fresh → counter 1).
    let lease = store.acquire("shard-1", "w1").await.map_err(to_box)?;
    assert_eq!(lease.lease_counter, 1);
    assert_eq!(lease.lease_owner.as_deref(), Some("w1"));

    // Renew (counter 1 → 2).
    let c2 = store.renew("shard-1", "w1", 1).await.map_err(to_box)?;
    assert_eq!(c2, 2);

    // Checkpoint (counter 2 → 3, stores opaque seq).
    let c3 = store.checkpoint("shard-1", "w1", 2, "seq-abc").await.map_err(to_box)?;
    assert_eq!(c3, 3);

    // Optimistic lock: renewing at a STALE counter (2, but it's now 3) must lose.
    match store.renew("shard-1", "w1", 2).await {
        Err(LeaseError::Lost) => {}
        other => return Err(format!("expected LeaseError::Lost on stale renew, got {other:?}").into()),
    }

    // Checkpoint persisted and readable.
    let cur = store.get("shard-1").await?.expect("lease row");
    assert_eq!(cur.checkpoint.as_deref(), Some("seq-abc"));
    assert_eq!(cur.lease_counter, 3);
    eprintln!("lease cycle OK: acquire→renew→checkpoint, stale renew rejected");
    Ok(())
}

fn to_box(e: LeaseError) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(e)
}
