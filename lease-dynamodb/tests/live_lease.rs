#![cfg(feature = "aws")]
//! Live integration test for the DynamoDB lease store. Skipped unless
//! `DDB_STREAMS_CONSUMER_IT=1`. Proves the optimistic-lock cycle against real DynamoDB:
//! acquire → renew → checkpoint, and that a stale-counter renew is rejected
//! (`LeaseError::Lost`). Creates and deletes its own lease table.
//!
//! Run:
//!   DDB_STREAMS_CONSUMER_IT=1 cargo test -p amazon-dynamodb-streams-consumer-lease \
//!     --features aws --test live_lease -- --nocapture

use amazon_dynamodb_streams_consumer_lease::dynamodb::{DynamoDbLeaseStore, LeaseError};
use aws_sdk_dynamodb as ddb;

#[tokio::test]
async fn live_optimistic_lock_lease_cycle() {
    if std::env::var("DDB_STREAMS_CONSUMER_IT").is_err() {
        eprintln!("skipping live lease integ test (set DDB_STREAMS_CONSUMER_IT=1 to run)");
        return;
    }

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = ddb::Client::new(&cfg);
    let table = format!(
        "amazon-dynamodb-streams-consumer-leases-it-{}",
        std::process::id()
    );

    let store = DynamoDbLeaseStore::new(client.clone(), &table);
    store.ensure_table().await.expect("ensure_table");

    let outcome = run_cycle(&store).await;

    // Best-effort cleanup.
    let _ = client.delete_table().table_name(&table).send().await;

    outcome.expect("lease cycle");
}

async fn run_cycle(
    store: &DynamoDbLeaseStore,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Acquire (fresh → counter 1).
    let lease = store.acquire("shard-1", "w1").await.map_err(to_box)?;
    assert_eq!(lease.lease_counter, 1);
    assert_eq!(lease.lease_owner.as_deref(), Some("w1"));

    // Renew (counter 1 → 2).
    let c2 = store.renew("shard-1", "w1", 1).await.map_err(to_box)?;
    assert_eq!(c2, 2);

    // Checkpoint (counter 2 → 3, stores opaque seq).
    let c3 = store
        .checkpoint("shard-1", "w1", 2, "seq-abc")
        .await
        .map_err(to_box)?;
    assert_eq!(c3, 3);

    // Optimistic lock: renewing at a STALE counter (2, but it's now 3) must lose.
    match store.renew("shard-1", "w1", 2).await {
        Err(LeaseError::Lost) => {}
        other => {
            return Err(format!("expected LeaseError::Lost on stale renew, got {other:?}").into())
        }
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

/// `delete_lease` must ONLY remove a lease that is marked completed — its
/// conditional delete (`attribute_exists AND completed = true`) is the guard
/// that stops lease-GC from ever deleting a live worker's lease (which would
/// cause double-processing / data loss). Verifies both arms against real DDB.
#[tokio::test]
async fn live_delete_lease_only_removes_completed() {
    if std::env::var("DDB_STREAMS_CONSUMER_IT").is_err() {
        eprintln!("skipping live delete-lease guard test (set DDB_STREAMS_CONSUMER_IT=1 to run)");
        return;
    }

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = ddb::Client::new(&cfg);
    let table = format!(
        "amazon-dynamodb-streams-consumer-del-it-{}",
        std::process::id()
    );
    let store = DynamoDbLeaseStore::new(client.clone(), &table);
    store.ensure_table().await.expect("ensure_table");

    let outcome = run_delete_guard(&store).await;
    let _ = client.delete_table().table_name(&table).send().await;
    outcome.expect("delete-lease guard");
}

async fn run_delete_guard(
    store: &DynamoDbLeaseStore,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // A live (non-completed) lease must survive a delete attempt.
    let lease = store.acquire("shard-del", "w1").await.map_err(to_box)?;
    match store.delete_lease("shard-del").await {
        Err(LeaseError::Lost) => {} // conditional-check failed → guarded, as intended
        Ok(()) => return Err("delete_lease removed a NON-completed lease (data-loss bug)".into()),
        Err(e) => return Err(format!("unexpected delete_lease error on live lease: {e:?}").into()),
    }
    if store.get("shard-del").await?.is_none() {
        return Err("live lease was deleted despite the completed-guard".into());
    }

    // Once completed, the same delete must succeed and remove the row.
    store
        .mark_complete("shard-del", "w1", lease.lease_counter)
        .await
        .map_err(to_box)?;
    store.delete_lease("shard-del").await.map_err(to_box)?;
    if store.get("shard-del").await?.is_some() {
        return Err("completed lease was not deleted".into());
    }
    eprintln!("delete-lease guard OK: live lease retained, completed lease removed");
    Ok(())
}
