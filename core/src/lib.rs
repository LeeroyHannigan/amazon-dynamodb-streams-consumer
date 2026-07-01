//! ddbstreams-kcl-core — shared Rust engine for a multi-language, JVM-free
//! DynamoDB Streams KCL.
//!
//! This crate owns the correctness-critical logic that is IDENTICAL regardless
//! of how languages attach on top (daemon+IPC "Architecture A" or FFI "B"):
//!   * shard graph construction (from DescribeStream)
//!   * ORDERING: single-owner-per-shard (in-sequence) + parent-before-child
//!   * checkpointing
//!
//! AWS is abstracted behind `StreamSource` + `LeaseStore` so the engine is unit
//! testable with zero network. The real DDB Streams / DynamoDB adapters are
//! added later as implementors of these traits.

pub type ShardId = String;
/// Spike uses a numeric sequence; real DDB Streams sequence numbers are opaque
/// monotonic strings — swapped in when the SDK adapter lands.
pub type SequenceNumber = u64;

#[derive(Clone, Debug)]
pub struct Record {
    pub shard_id: ShardId,
    pub seq: SequenceNumber,
    pub data: Vec<u8>,
}

/// Shard lineage as reported by DescribeStream.
#[derive(Clone, Debug)]
pub struct ShardMeta {
    pub id: ShardId,
    pub parent: Option<ShardId>,
}

pub struct RecordBatch {
    pub records: Vec<Record>,
    /// True when this shard is closed (SHARD_END) — no more records will arrive.
    pub shard_end: bool,
}

/// The stream side (DynamoDB Streams in prod). Behind a trait so the engine is
/// testable in-memory and so a Kinesis source could be slotted in later.
pub trait StreamSource {
    fn describe_shards(&self) -> Vec<ShardMeta>;
    /// Return records after `after` (exclusive); None = from TRIM_HORIZON.
    fn get_records(&self, shard: &ShardId, after: Option<SequenceNumber>) -> RecordBatch;
}

/// Lease + checkpoint state (DynamoDB lease table in prod).
pub trait LeaseStore {
    fn checkpoint(&mut self, shard: &ShardId, seq: SequenceNumber);
    fn last_checkpoint(&self, shard: &ShardId) -> Option<SequenceNumber>;
    fn mark_complete(&mut self, shard: &ShardId);
    fn is_complete(&self, shard: &ShardId) -> bool;
}

/// Customer business logic. In the real system a language binding bridges these
/// callbacks to the customer's Go/Python/etc. record processor.
pub trait RecordProcessor {
    fn initialize(&mut self, shard: &ShardId);
    fn process_records(&mut self, records: &[Record]);
    fn shard_ended(&mut self, shard: &ShardId);
}

/// Single-worker scheduler enforcing the ordering guarantees. Multi-host lease
/// stealing / balancing is a later phase; this proves the ordering core.
pub struct Scheduler<S: StreamSource, L: LeaseStore> {
    source: S,
    leases: L,
}

impl<S: StreamSource, L: LeaseStore> Scheduler<S, L> {
    pub fn new(source: S, leases: L) -> Self {
        Self { source, leases }
    }

    /// A shard is eligible only if it has no parent, or its parent has been
    /// fully processed (SHARD_END + checkpoint). This is the parent-before-child
    /// guarantee that preserves item-history order across resharding.
    fn eligible(&self, meta: &ShardMeta) -> bool {
        if self.leases.is_complete(&meta.id) {
            return false;
        }
        match &meta.parent {
            None => true,
            Some(p) => self.leases.is_complete(p),
        }
    }

    /// Drain all shards in dependency order. Returns when every shard is complete.
    pub fn run<P: RecordProcessor>(&mut self, processor: &mut P) {
        loop {
            let shards = self.source.describe_shards();
            if shards.iter().all(|m| self.leases.is_complete(&m.id)) {
                break;
            }

            let mut progressed = false;
            for meta in &shards {
                if !self.eligible(meta) {
                    continue;
                }
                progressed = true;
                processor.initialize(&meta.id);

                // Deliver strictly in sequence order, checkpointing as we go.
                loop {
                    let after = self.leases.last_checkpoint(&meta.id);
                    let batch = self.source.get_records(&meta.id, after);
                    if !batch.records.is_empty() {
                        // Invariant: records within a shard arrive in seq order.
                        processor.process_records(&batch.records);
                        let max = batch.records.iter().map(|r| r.seq).max().unwrap();
                        self.leases.checkpoint(&meta.id, max);
                    }
                    if batch.shard_end {
                        processor.shard_ended(&meta.id);
                        self.leases.mark_complete(&meta.id);
                        break;
                    }
                    if batch.records.is_empty() {
                        break; // no data yet; a real impl would back off and poll
                    }
                }
            }
            if !progressed {
                // No eligible shard advanced — would only happen on an
                // inconsistent shard graph; a real impl re-syncs here.
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// In-memory fakes for testing the ordering core without AWS.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct InMemSource {
        metas: Vec<ShardMeta>,
        // shard -> records (assumed pre-sorted by seq)
        data: HashMap<ShardId, Vec<Record>>,
    }

    impl StreamSource for InMemSource {
        fn describe_shards(&self) -> Vec<ShardMeta> {
            self.metas.clone()
        }
        fn get_records(&self, shard: &ShardId, after: Option<SequenceNumber>) -> RecordBatch {
            let all = self.data.get(shard).cloned().unwrap_or_default();
            let records: Vec<Record> = all
                .into_iter()
                .filter(|r| after.map_or(true, |a| r.seq > a))
                .collect();
            // In this fake, once we've handed everything over, the shard is closed.
            RecordBatch { records, shard_end: true }
        }
    }

    #[derive(Default)]
    struct InMemLeases {
        checkpoints: HashMap<ShardId, SequenceNumber>,
        complete: HashMap<ShardId, bool>,
    }
    impl LeaseStore for InMemLeases {
        fn checkpoint(&mut self, shard: &ShardId, seq: SequenceNumber) {
            self.checkpoints.insert(shard.clone(), seq);
        }
        fn last_checkpoint(&self, shard: &ShardId) -> Option<SequenceNumber> {
            self.checkpoints.get(shard).copied()
        }
        fn mark_complete(&mut self, shard: &ShardId) {
            self.complete.insert(shard.clone(), true);
        }
        fn is_complete(&self, shard: &ShardId) -> bool {
            *self.complete.get(shard).unwrap_or(&false)
        }
    }

    /// Records the exact order of engine callbacks so we can assert ordering.
    #[derive(Default)]
    struct RecordingProcessor {
        events: Vec<String>,
    }
    impl RecordProcessor for RecordingProcessor {
        fn initialize(&mut self, shard: &ShardId) {
            self.events.push(format!("init:{shard}"));
        }
        fn process_records(&mut self, records: &[Record]) {
            for r in records {
                self.events.push(format!("rec:{}:{}", r.shard_id, r.seq));
            }
        }
        fn shard_ended(&mut self, shard: &ShardId) {
            self.events.push(format!("end:{shard}"));
        }
    }

    fn rec(shard: &str, seq: SequenceNumber) -> Record {
        Record { shard_id: shard.to_string(), seq, data: vec![] }
    }

    /// SPIKE SUCCESS CRITERION: a parent shard splits into a child; the engine
    /// MUST deliver all parent records (in order) and finish the parent before
    /// delivering any child record.
    #[test]
    fn parent_before_child_ordering() {
        let mut data = HashMap::new();
        data.insert("shard-parent".to_string(), vec![rec("shard-parent", 1), rec("shard-parent", 2)]);
        data.insert("shard-child".to_string(), vec![rec("shard-child", 3), rec("shard-child", 4)]);

        let source = InMemSource {
            metas: vec![
                // Deliberately list child first to prove ordering isn't just list order.
                ShardMeta { id: "shard-child".into(), parent: Some("shard-parent".into()) },
                ShardMeta { id: "shard-parent".into(), parent: None },
            ],
            data,
        };

        let mut proc = RecordingProcessor::default();
        let mut sched = Scheduler::new(source, InMemLeases::default());
        sched.run(&mut proc);

        assert_eq!(
            proc.events,
            vec![
                "init:shard-parent",
                "rec:shard-parent:1",
                "rec:shard-parent:2",
                "end:shard-parent",
                "init:shard-child",
                "rec:shard-child:3",
                "rec:shard-child:4",
                "end:shard-child",
            ],
            "child must not be touched until parent reaches SHARD_END + checkpoint"
        );
    }

    /// Per-shard records are delivered in strictly increasing sequence order.
    #[test]
    fn per_shard_sequence_order() {
        let mut data = HashMap::new();
        data.insert("s".to_string(), vec![rec("s", 10), rec("s", 11), rec("s", 12)]);
        let source = InMemSource {
            metas: vec![ShardMeta { id: "s".into(), parent: None }],
            data,
        };
        let mut proc = RecordingProcessor::default();
        let mut sched = Scheduler::new(source, InMemLeases::default());
        sched.run(&mut proc);
        assert_eq!(
            proc.events,
            vec!["init:s", "rec:s:10", "rec:s:11", "rec:s:12", "end:s"]
        );
    }
}
