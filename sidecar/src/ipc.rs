//! Stdio IPC between the sidecar and the language client.
//!
//! The sidecar writes newline-delimited [`ServerMessage`]s (record batches,
//! lifecycle) to one stream and reads [`ClientMessage`]s (checkpoint acks, stop)
//! from another. A single [`Ipc`] instance owns the writer and a reader task
//! that routes each `checkpoint` ack to the shard task awaiting it.
//!
//! Per-shard delivery is ack-gated: a shard task registers a one-shot, sends its
//! `records` message, and awaits the client's `checkpoint`; the awaited sequence
//! becomes the value the fleet persists under the optimistic lock. Because a
//! shard is processed by exactly one task at a time, there is at most one
//! outstanding ack per shard.

use amazon_dynamodb_streams_consumer_core::record::StreamRecord;
use amazon_dynamodb_streams_consumer_core::{Record, ShardId};
use amazon_dynamodb_streams_consumer_protocol::{ClientMessage, ServerMessage};
use amazon_dynamodb_streams_consumer_worker::{
    AsyncShardConsumer, ShardConsumerFactory, WorkerError,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{oneshot, Mutex, Notify};

/// Owns the client connection: a serialized writer plus a reader task that
/// fulfills per-shard ack waiters.
pub struct Ipc {
    writer: Mutex<Box<dyn AsyncWrite + Unpin + Send>>,
    /// shard id -> waiter for the client's next checkpoint ack on that shard.
    pending: Mutex<HashMap<ShardId, oneshot::Sender<String>>>,
    /// Set when the client sends `stop` (or its stream closes).
    stop: Arc<Notify>,
    stopped: std::sync::atomic::AtomicBool,
}

impl Ipc {
    pub fn new<W: AsyncWrite + Unpin + Send + 'static>(writer: W) -> Arc<Self> {
        Arc::new(Self {
            writer: Mutex::new(Box::new(writer)),
            pending: Mutex::new(HashMap::new()),
            stop: Arc::new(Notify::new()),
            stopped: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Spawn the reader task: parse each client line and route acks. Returns
    /// immediately; the task lives for the process.
    pub fn spawn_reader<R: AsyncRead + Unpin + Send + 'static>(self: &Arc<Self>, reader: R) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) if line.trim().is_empty() => continue,
                    Ok(Some(line)) => match ClientMessage::parse(&line) {
                        Ok(ClientMessage::Checkpoint { shard, seq }) => {
                            if let Some(tx) = this.pending.lock().await.remove(&shard) {
                                let _ = tx.send(seq);
                            }
                        }
                        Ok(ClientMessage::Ready) => { /* handshake, no-op */ }
                        Ok(ClientMessage::Stop) => {
                            this.signal_stop();
                            break;
                        }
                        Err(_) => { /* ignore malformed line, keep reading */ }
                    },
                    // EOF or read error → client is gone; stop the fleet.
                    Ok(None) | Err(_) => {
                        this.signal_stop();
                        break;
                    }
                }
            }
        });
    }

    fn signal_stop(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.stop.notify_waiters();
    }

    /// Request a graceful stop (e.g. from a SIGTERM/Ctrl-C handler).
    pub fn request_stop(&self) {
        self.signal_stop();
    }

    /// Best-effort notify the client that the sidecar is shutting down.
    pub async fn shutdown(&self, reason: &str) {
        let _ = self
            .send(&ServerMessage::Shutdown {
                reason: reason.to_string(),
            })
            .await;
    }

    /// Best-effort notify the client that a shard's lease is about to be handed
    /// off, so its processor can flush before another worker takes over.
    pub async fn shutdown_requested(&self, shard: &str) {
        let _ = self
            .send(&ServerMessage::ShutdownRequested {
                shard: shard.to_string(),
            })
            .await;
    }

    /// True once the client has asked to stop or disconnected.
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Wait until stop is signalled (used by the main loop to break promptly).
    pub async fn stopped(&self) {
        if self.is_stopped() {
            return;
        }
        self.stop.notified().await;
    }

    async fn send(&self, msg: &ServerMessage) -> std::io::Result<()> {
        let mut w = self.writer.lock().await;
        w.write_all(msg.to_line().as_bytes()).await?;
        w.flush().await
    }

    /// Send a batch for `shard` and await the client's checkpoint ack for it.
    /// Returns the acked sequence, or `None` if the client went away first.
    async fn deliver_batch(
        &self,
        shard: &ShardId,
        last_seq: String,
        records: Vec<StreamRecord>,
    ) -> Option<String> {
        let (tx, rx) = oneshot::channel();
        // Register the waiter BEFORE sending so an immediate ack can't race past us.
        self.pending.lock().await.insert(shard.clone(), tx);
        let msg = ServerMessage::Records {
            shard: shard.clone(),
            last_seq,
            records,
        };
        if self.send(&msg).await.is_err() {
            self.pending.lock().await.remove(shard);
            return None;
        }
        rx.await.ok()
    }
}

/// Creates one [`IpcConsumer`] per shard, all sharing the single [`Ipc`].
pub struct IpcConsumerFactory {
    ipc: Arc<Ipc>,
}

impl IpcConsumerFactory {
    pub fn new(ipc: Arc<Ipc>) -> Self {
        Self { ipc }
    }
}

impl ShardConsumerFactory for IpcConsumerFactory {
    fn create(&self, shard: &ShardId) -> Box<dyn AsyncShardConsumer + Send> {
        Box::new(IpcConsumer {
            ipc: self.ipc.clone(),
            shard: shard.clone(),
        })
    }
}

struct IpcConsumer {
    ipc: Arc<Ipc>,
    shard: ShardId,
}

#[async_trait::async_trait]
impl AsyncShardConsumer for IpcConsumer {
    async fn deliver(&mut self, records: &[Record]) -> Result<Option<String>, WorkerError> {
        let last_seq = match records.last() {
            Some(r) => r.seq.clone(),
            None => return Ok(None),
        };
        // Decode each opaque payload back into the typed change record for the
        // client. A malformed payload is skipped rather than aborting the batch.
        let decoded: Vec<StreamRecord> = records
            .iter()
            .filter_map(|r| StreamRecord::decode(&r.data).ok())
            .collect();
        // `None` (client gone / no ack) → the fleet holds the lease without
        // advancing the durable checkpoint; on restart we resume from the last ack.
        Ok(self.ipc.deliver_batch(&self.shard, last_seq, decoded).await)
    }

    async fn shard_ended(&mut self) -> Result<(), WorkerError> {
        let _ = self
            .ipc
            .send(&ServerMessage::ShardComplete {
                shard: self.shard.clone(),
            })
            .await;
        Ok(())
    }

    async fn lease_lost(&mut self) -> Result<(), WorkerError> {
        let _ = self
            .ipc
            .send(&ServerMessage::LeaseLost {
                shard: self.shard.clone(),
            })
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amazon_dynamodb_streams_consumer_core::record::{AttrValue, Item};

    fn rec(shard: &str, seq: &str) -> Record {
        let mut keys = Item::new();
        keys.insert("pk".into(), AttrValue::S("k".into()));
        let sr = StreamRecord {
            event_name: Some("INSERT".into()),
            sequence_number: Some(seq.into()),
            keys,
            ..Default::default()
        };
        Record {
            shard_id: shard.into(),
            seq: seq.into(),
            data: sr.encode(),
        }
    }

    // A client that reads Records, decodes them, and acks last_seq — exactly
    // what a real language binding does. Runs over an in-memory duplex.
    #[tokio::test]
    async fn deliver_is_gated_on_the_client_checkpoint_ack() {
        // server writes -> client reads (c2s reader side); client writes -> server reads.
        let (server_w, client_r) = tokio::io::duplex(64 * 1024);
        let (client_w, server_r) = tokio::io::duplex(64 * 1024);

        let ipc = Ipc::new(server_w);
        ipc.spawn_reader(server_r);

        // Fake client: read one Records message, verify payload decoded, ack it.
        let client = tokio::spawn(async move {
            let mut lines = BufReader::new(client_r).lines();
            let mut cw = client_w;
            let line = lines.next_line().await.unwrap().unwrap();
            let msg = ServerMessage::parse(&line).unwrap();
            let (shard, last_seq, n, has_pk) = match msg {
                ServerMessage::Records {
                    shard,
                    last_seq,
                    records,
                } => (
                    shard,
                    last_seq,
                    records.len(),
                    records
                        .first()
                        .map(|r| r.keys.contains_key("pk"))
                        .unwrap_or(false),
                ),
                other => panic!("expected records, got {other:?}"),
            };
            // Ack the batch.
            cw.write_all(
                ClientMessage::Checkpoint {
                    shard,
                    seq: last_seq.clone(),
                }
                .to_line()
                .as_bytes(),
            )
            .await
            .unwrap();
            cw.flush().await.unwrap();
            (last_seq, n, has_pk)
        });

        let mut consumer = IpcConsumer {
            ipc: ipc.clone(),
            shard: "s0".into(),
        };
        let batch = vec![
            rec("s0", "100000000000000000001"),
            rec("s0", "100000000000000000002"),
        ];
        let ack = consumer.deliver(&batch).await.unwrap();

        assert_eq!(
            ack.as_deref(),
            Some("100000000000000000002"),
            "fleet checkpoints the acked seq"
        );
        let (client_last, n, has_pk) = client.await.unwrap();
        assert_eq!(client_last, "100000000000000000002");
        assert_eq!(n, 2, "client received both records");
        assert!(has_pk, "typed payload decoded with the pk key");
    }

    #[tokio::test]
    async fn client_disconnect_signals_stop_and_unblocks_delivery() {
        let (server_w, _client_r) = tokio::io::duplex(1024);
        let (client_w, server_r) = tokio::io::duplex(1024);
        let ipc = Ipc::new(server_w);
        ipc.spawn_reader(server_r);

        // Drop the client's writer → server reader hits EOF → stop is signalled.
        drop(client_w);
        ipc.stopped().await;
        assert!(ipc.is_stopped());

        // A delivery with no client to ack resolves to None (no checkpoint), not a hang.
        let mut consumer = IpcConsumer {
            ipc: ipc.clone(),
            shard: "s0".into(),
        };
        let batch = [rec("s0", "1")];
        let ack = tokio::select! {
            a = consumer.deliver(&batch) => a.unwrap(),
            _ = ipc.stopped() => None,
        };
        assert_eq!(ack, None);
    }
}
