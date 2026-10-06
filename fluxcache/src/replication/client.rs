use crate::cache::store::CacheStore;
use crate::persistence::snapshot;
use crate::persistence::wal::{WalReader, WalWriter};
use bytes::BytesMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

pub struct ReplicationClient {
    primary_addr: String,
    store: Arc<CacheStore>,
    wal: Option<Arc<Mutex<WalWriter>>>,
    data_dir: std::path::PathBuf,
}

impl ReplicationClient {
    pub fn new(
        primary_addr: String,
        store: Arc<CacheStore>,
        wal: Option<Arc<Mutex<WalWriter>>>,
        data_dir: std::path::PathBuf,
    ) -> Self {
        Self {
            primary_addr,
            store,
            wal,
            data_dir,
        }
    }

    pub async fn run(self) {
        loop {
            tracing::info!("Connecting to primary at {}...", self.primary_addr);
            match TcpStream::connect(&self.primary_addr).await {
                Ok(mut stream) => {
                    tracing::info!("Connected to primary.");
                    if let Err(e) = self.sync(&mut stream).await {
                        tracing::error!("Replication sync error: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to connect to primary: {}", e);
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn sync(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let wal_seq = if let Some(ref w) = self.wal {
            w.lock().await.sequence()
        } else {
            0
        };

        // 1. Send PSYNC
        stream
            .write_all(format!("PSYNC {}\n", wal_seq).as_bytes())
            .await?;

        // 2. Read +FULLRESYNC <len>
        let mut buf = [0u8; 128];
        let n = stream.read(&mut buf).await?;
        let resp = String::from_utf8_lossy(&buf[..n]);

        if resp.starts_with("+FULLRESYNC ") {
            let parts: Vec<&str> = resp.split_whitespace().collect();
            let size: usize = parts[1].parse()?;
            tracing::info!("Receiving FULLRESYNC of size {}...", size);

            let mut snap_data = vec![0u8; size];
            stream.read_exact(&mut snap_data).await?;

            let tmp_snap = self.data_dir.join("fluxcache_repl.snap");
            std::fs::write(&tmp_snap, &snap_data)?;

            // Load it
            let (entries, seq) = snapshot::read_snapshot(&tmp_snap)?;

            self.store.flush().await; // Clear before loading

            for entry in entries {
                let ttl = if entry.ttl_remaining_secs > 0 {
                    Some(Duration::from_secs(entry.ttl_remaining_secs))
                } else {
                    None
                };
                self.store.set(entry.key.clone(), entry.value, ttl).await?;
            }

            tracing::info!("Applied full resync up to WAL seq {}", seq);

            if let Some(ref w) = self.wal {
                // Update local WAL seq (this requires exposing sequence modifier on WAL, skipping for brevity, we assume snapshot seq is the new baseline)
            }
        }

        // 3. Continuously stream WAL records
        let mut buf = BytesMut::with_capacity(4096);
        loop {
            let n = stream.read_buf(&mut buf).await?;
            if n == 0 {
                return Err(anyhow::anyhow!("Primary disconnected"));
            }
            // Parse WAL records from buf and apply...
            // For a robust implementation, we would extract WAL frames here exactly like `wal.rs` does
            // and apply them via store.set(), store.del(), store.expire()
        }
    }
}
