use crate::cache::store::CacheStore;
use crate::persistence::snapshot;
use crate::persistence::wal::WalWriter;
use crate::shutdown::ShutdownSignal;
use bytes::Bytes;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};

pub struct ReplicationServer {
    listen_addr: String,
    store: Arc<CacheStore>,
    wal: Option<Arc<Mutex<WalWriter>>>,
    shutdown: ShutdownSignal,
    wal_broadcast: broadcast::Sender<Bytes>,
}

impl ReplicationServer {
    pub fn new(
        listen_addr: String,
        store: Arc<CacheStore>,
        wal: Option<Arc<Mutex<WalWriter>>>,
        shutdown: ShutdownSignal,
        wal_broadcast: broadcast::Sender<Bytes>,
    ) -> Self {
        Self {
            listen_addr,
            store,
            wal,
            shutdown,
            wal_broadcast,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.listen_addr).await?;
        tracing::info!("Replication server listening on {}", self.listen_addr);

        let mut shutdown_rx = self.shutdown.subscribe();

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, addr)) => {
                            tracing::info!("Replica connected from {}", addr);
                            let store = Arc::clone(&self.store);
                            let wal = self.wal.clone();
                            let broadcast_rx = self.wal_broadcast.subscribe();

                            tokio::spawn(async move {
                                if let Err(e) = handle_replica(socket, store, wal, broadcast_rx).await {
                                    tracing::error!("Replica {} error: {}", addr, e);
                                }
                            });
                        }
                        Err(e) => tracing::error!("Replication accept error: {}", e),
                    }
                }
                _ = shutdown_rx.recv() => {
                    tracing::info!("Replication server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

async fn handle_replica(
    mut socket: tokio::net::TcpStream,
    store: Arc<CacheStore>,
    wal: Option<Arc<Mutex<WalWriter>>>,
    mut broadcast_rx: broadcast::Receiver<Bytes>,
) -> anyhow::Result<()> {
    // 1. Wait for PSYNC
    let mut buf = [0u8; 128];
    let n = socket.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    if !req.starts_with("PSYNC") {
        return Err(anyhow::anyhow!("Expected PSYNC"));
    }

    tracing::info!("Replica requested PSYNC. Generating full snapshot.");

    // 2. Generate an in-memory snapshot
    let data = store.snapshot_data().await;
    let wal_seq = if let Some(ref w) = wal {
        w.lock().await.sequence()
    } else {
        0
    };

    // Serialize snapshot directly to memory for the replica
    let mut snap_buf = Vec::new();
    snapshot::write_snapshot_to_writer(&mut snap_buf, &data, wal_seq)?;

    // 3. Send FULLRESYNC
    let header = format!("+FULLRESYNC {}\n", snap_buf.len());
    socket.write_all(header.as_bytes()).await?;
    socket.write_all(&snap_buf).await?;

    tracing::info!("Sent snapshot of size {} to replica.", snap_buf.len());

    // 4. Stream continuous WAL records
    loop {
        match broadcast_rx.recv().await {
            Ok(wal_record_bytes) => {
                if let Err(e) = socket.write_all(&wal_record_bytes).await {
                    tracing::error!("Failed to write to replica: {}", e);
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                // If the replica is too slow, we drop the connection.
                // The replica will reconnect and trigger a new FULLRESYNC.
                tracing::warn!("Replica lagged by {} messages. Disconnecting.", skipped);
                break;
            }
            Err(broadcast::error::RecvError::Closed) => {
                break;
            }
        }
    }

    Ok(())
}
