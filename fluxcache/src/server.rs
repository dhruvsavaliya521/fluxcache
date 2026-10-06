// FluxCache - TCP Server + HTTP Admin
//
// The main server that ties together all subsystems:
// - TCP data plane for cache operations
// - HTTP admin plane for metrics, health, and administration
// - Background tasks (TTL expiration, WAL sync, snapshots)

use crate::cache::store::{CacheError, CacheStore};
use crate::config::Config;
use crate::metrics::CacheMetrics;
use crate::persistence::snapshot;
use crate::persistence::wal::{WalReader, WalWriter};
use crate::protocol::command::Command;
use crate::protocol::parser::{extract_frame, parse_command};
use crate::protocol::response::Response;
use crate::replication::{ReplicationClient, ReplicationServer};
use crate::shutdown::ShutdownSignal;

use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};

/// The FluxCache server.
pub struct FluxServer {
    config: Config,
    store: Arc<CacheStore>,
    metrics: Arc<CacheMetrics>,
    shutdown: ShutdownSignal,
    wal: Option<Arc<Mutex<WalWriter>>>,
    wal_broadcast: Option<broadcast::Sender<Bytes>>,
}

impl FluxServer {
    /// Create a new FluxCache server.
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        let metrics = Arc::new(CacheMetrics::new());

        let store = Arc::new(CacheStore::new(
            config.shards,
            config.max_memory,
            config.eviction_policy,
            Arc::clone(&metrics),
        ));

        let mut wal_broadcast = None;
        if config.replica_of.is_none() {
            let (tx, _) = broadcast::channel(1024);
            wal_broadcast = Some(tx);
        }

        let mut wal = None;

        // Recovery: load snapshot + replay WAL
        if config.persistence {
            std::fs::create_dir_all(&config.data_dir)?;

            let snap_path = config.data_dir.join("fluxcache.snap");
            let wal_path = config.data_dir.join("fluxcache.wal");

            let mut wal_start_seq = 0u64;

            // Load snapshot if exists
            if snap_path.exists() {
                tracing::info!("Loading snapshot from {:?}", snap_path);
                match snapshot::read_snapshot(&snap_path) {
                    Ok((entries, snap_seq)) => {
                        wal_start_seq = snap_seq;
                        for entry in entries {
                            let ttl = if entry.ttl_remaining_secs > 0 {
                                Some(Duration::from_secs(entry.ttl_remaining_secs))
                            } else {
                                None
                            };
                            if let Err(e) = store.set(entry.key.clone(), entry.value, ttl).await {
                                tracing::warn!("Failed to restore key {}: {}", entry.key, e);
                            }
                        }
                        tracing::info!("Snapshot loaded, WAL sequence: {}", snap_seq);
                    }
                    Err(e) => {
                        tracing::error!("Failed to load snapshot: {}", e);
                    }
                }
            }

            // Replay WAL records after snapshot sequence
            if wal_path.exists() {
                tracing::info!("Replaying WAL from {:?}", wal_path);
                match WalReader::read_all(&wal_path) {
                    Ok(records) => {
                        let mut replayed = 0;
                        for record in records {
                            if record.sequence <= wal_start_seq {
                                continue;
                            }
                            match record.record_type {
                                crate::persistence::wal::WalRecordType::Set => {
                                    let ttl = if record.ttl_secs > 0 {
                                        Some(Duration::from_secs(record.ttl_secs))
                                    } else {
                                        None
                                    };
                                    let value = record.value.unwrap_or_default();
                                    let _ = store.set(record.key, value, ttl).await;
                                }
                                crate::persistence::wal::WalRecordType::Delete => {
                                    store.delete(&record.key).await;
                                }
                                crate::persistence::wal::WalRecordType::Expire => {
                                    store.expire(&record.key, record.ttl_secs).await;
                                }
                            }
                            replayed += 1;
                        }
                        tracing::info!("Replayed {} WAL records", replayed);
                    }
                    Err(e) => {
                        tracing::error!("Failed to replay WAL: {}", e);
                    }
                }
            }

            let wal_writer = WalWriter::open(&wal_path, wal_broadcast.clone())?;
            wal = Some(Arc::new(Mutex::new(wal_writer)));
        }

        Ok(FluxServer {
            config,
            store,
            metrics,
            shutdown: ShutdownSignal::new(),
            wal,
            wal_broadcast,
        })
    }

    /// Start the server and all background tasks.
    pub async fn run(&self) -> anyhow::Result<()> {
        tracing::info!(
            "FluxCache starting on {} (admin: {})",
            self.config.listen,
            self.config.admin_listen
        );

        // Spawn background tasks
        self.spawn_expiration_task();
        self.spawn_wal_sync_task();
        self.spawn_snapshot_task();

        // Spawn HTTP admin server
        let admin_handle = self.spawn_admin_server().await?;

        // Start replication client or server
        let mut repl_server_handle = None;
        if let Some(primary_addr) = self.config.replica_of.clone() {
            tracing::info!("Starting in REPLICA mode, syncing from {}", primary_addr);
            let repl_client = ReplicationClient::new(
                primary_addr,
                Arc::clone(&self.store),
                self.wal.clone(),
                self.config.data_dir.clone(),
            );
            tokio::spawn(repl_client.run());
        } else if let Some(tx) = self.wal_broadcast.clone() {
            tracing::info!(
                "Starting in PRIMARY mode, replication on {}",
                self.config.replication_listen
            );
            let repl_server = ReplicationServer::new(
                self.config.replication_listen.clone(),
                Arc::clone(&self.store),
                self.wal.clone(),
                self.shutdown.clone(),
                tx,
            );
            repl_server_handle = Some(tokio::spawn(async move {
                if let Err(e) = repl_server.run().await {
                    tracing::error!("Replication server error: {}", e);
                }
            }));
        }

        // Run TCP data plane
        let tcp_handle = self.spawn_tcp_server().await?;

        // Wait for shutdown signal
        crate::shutdown::wait_for_signal().await;
        self.shutdown.shutdown();

        tracing::info!("Shutting down...");

        // Final sync
        if let Some(ref wal) = self.wal {
            let mut wal = wal.lock().await;
            if let Err(e) = wal.sync() {
                tracing::error!("Failed to sync WAL on shutdown: {}", e);
            }
        }

        // Final snapshot
        if self.config.persistence {
            self.take_snapshot().await;
        }

        // Abort server tasks
        tcp_handle.abort();
        admin_handle.abort();

        tracing::info!("FluxCache stopped.");
        Ok(())
    }

    /// Spawn the TCP data plane server.
    async fn spawn_tcp_server(&self) -> anyhow::Result<tokio::task::JoinHandle<()>> {
        let listener = TcpListener::bind(&self.config.listen).await?;
        let store = Arc::clone(&self.store);
        let metrics = Arc::clone(&self.metrics);
        let wal = self.wal.clone();
        let mut shutdown_rx = self.shutdown.subscribe();
        let max_request_size = self.config.max_request_size;
        let read_timeout = self.config.read_timeout;

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((socket, addr)) => {
                                tracing::debug!("New connection from {}", addr);
                                metrics.record_connection();

                                let store = Arc::clone(&store);
                                let metrics = Arc::clone(&metrics);
                                let wal = wal.clone();

                                tokio::spawn(async move {
                                    if let Err(e) = handle_connection(
                                        socket, store, metrics.clone(), wal,
                                        max_request_size, read_timeout,
                                    ).await {
                                        tracing::debug!("Connection {} error: {}", addr, e);
                                    }
                                    metrics.record_disconnect();
                                });
                            }
                            Err(e) => {
                                tracing::error!("Failed to accept connection: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("TCP server shutting down");
                        break;
                    }
                }
            }
        });

        tracing::info!("TCP server listening on {}", self.config.listen);
        Ok(handle)
    }

    /// Spawn the HTTP admin server.
    async fn spawn_admin_server(&self) -> anyhow::Result<tokio::task::JoinHandle<()>> {
        let listener = tokio::net::TcpListener::bind(&self.config.admin_listen).await?;
        let store = Arc::clone(&self.store);
        let metrics = Arc::clone(&self.metrics);
        let wal = self.wal.clone();
        let data_dir = self.config.data_dir.clone();
        let persistence = self.config.persistence;
        let mut shutdown_rx = self.shutdown.subscribe();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, _addr)) => {
                                let store = Arc::clone(&store);
                                let metrics = Arc::clone(&metrics);
                                let wal = wal.clone();
                                let data_dir = data_dir.clone();
                                tokio::spawn(async move {
                                    handle_http_connection(
                                        stream, store, metrics, wal, data_dir, persistence,
                                    ).await;
                                });
                            }
                            Err(e) => {
                                tracing::error!("Admin accept error: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Admin server shutting down");
                        break;
                    }
                }
            }
        });

        tracing::info!(
            "Admin HTTP server listening on {}",
            self.config.admin_listen
        );
        Ok(handle)
    }

    /// Spawn background TTL expiration task.
    fn spawn_expiration_task(&self) {
        let store = Arc::clone(&self.store);
        let interval = self.config.expiration_scan_interval;
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let expired = store.expire_scan(1000).await;
                        if expired > 0 {
                            tracing::debug!("Expired {} entries", expired);
                        }
                    }
                    _ = shutdown_rx.recv() => break,
                }
            }
        });
    }

    /// Spawn background WAL sync task.
    fn spawn_wal_sync_task(&self) {
        if let Some(ref wal) = self.wal {
            let wal = Arc::clone(wal);
            let interval = self.config.wal_sync_interval;
            let mut shutdown_rx = self.shutdown.subscribe();

            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            let mut w = wal.lock().await;
                            if let Err(e) = w.sync() {
                                tracing::error!("WAL sync error: {}", e);
                            }
                        }
                        _ = shutdown_rx.recv() => break,
                    }
                }
            });
        }
    }

    /// Spawn background snapshot task.
    fn spawn_snapshot_task(&self) {
        if !self.config.persistence {
            return;
        }

        let store = Arc::clone(&self.store);
        let metrics = Arc::clone(&self.metrics);
        let wal = self.wal.clone();
        let data_dir = self.config.data_dir.clone();
        let interval = self.config.snapshot_interval;
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // Skip first immediate tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let snap_path = data_dir.join("fluxcache.snap");
                        let data = store.snapshot_data().await;
                        let wal_seq = if let Some(ref w) = wal {
                            w.lock().await.sequence()
                        } else {
                            0
                        };

                        let start = std::time::Instant::now();
                        if let Err(e) = snapshot::write_snapshot(&snap_path, &data, wal_seq) {
                            tracing::error!("Snapshot error: {}", e);
                        } else {
                            let elapsed = start.elapsed();
                            metrics.record_snapshot();
                            tracing::info!(
                                "Snapshot taken: {} entries in {:?}",
                                data.len(),
                                elapsed
                            );
                        }
                    }
                    _ = shutdown_rx.recv() => break,
                }
            }
        });
    }

    /// Take an immediate snapshot.
    async fn take_snapshot(&self) {
        let snap_path = self.config.data_dir.join("fluxcache.snap");
        let data = self.store.snapshot_data().await;
        let wal_seq = if let Some(ref w) = self.wal {
            w.lock().await.sequence()
        } else {
            0
        };

        if let Err(e) = snapshot::write_snapshot(&snap_path, &data, wal_seq) {
            tracing::error!("Final snapshot error: {}", e);
        } else {
            tracing::info!("Final snapshot: {} entries", data.len());
        }
    }
}

/// Handle a single TCP client connection.
async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    store: Arc<CacheStore>,
    metrics: Arc<CacheMetrics>,
    wal: Option<Arc<Mutex<WalWriter>>>,
    max_request_size: usize,
    read_timeout: Duration,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        // Read data with timeout
        let _n = tokio::select! {
            result = socket.read_buf(&mut buf) => {
                match result {
                    Ok(0) => return Ok(()), // Client disconnected
                    Ok(n) => n,
                    Err(e) => return Err(e.into()),
                }
            }
            _ = tokio::time::sleep(read_timeout) => {
                let resp = Response::Error("Connection timed out".to_string());
                socket.write_all(&resp.encode()).await?;
                return Ok(());
            }
        };

        // Check request size limit
        if buf.len() > max_request_size {
            let resp = Response::Error("Request too large".to_string());
            socket.write_all(&resp.encode()).await?;
            buf.clear();
            continue;
        }

        // Process all complete frames in the buffer (pipelining support)
        loop {
            match extract_frame(&mut buf) {
                Ok(Some(frame)) => {
                    let response = match parse_command(&frame) {
                        Ok(cmd) => execute_command(cmd, &store, &metrics, wal.as_ref()).await,
                        Err(e) => {
                            metrics.record_error();
                            Response::Error(format!("Parse error: {e}"))
                        }
                    };

                    socket.write_all(&response.encode()).await?;
                }
                Ok(None) => break, // Need more data
                Err(e) => {
                    metrics.record_error();
                    let resp = Response::Error(format!("Protocol error: {e}"));
                    socket.write_all(&resp.encode()).await?;
                    buf.clear();
                    break;
                }
            }
        }
    }
}

/// Execute a parsed command against the cache store.
async fn execute_command(
    cmd: Command,
    store: &CacheStore,
    metrics: &CacheMetrics,
    wal: Option<&Arc<Mutex<WalWriter>>>,
) -> Response {
    match cmd {
        Command::Ping => Response::Ok("PONG".to_string()),

        Command::Get { key } => match store.get(&key).await {
            Ok(value) => Response::Data(value),
            Err(CacheError::NotFound | CacheError::Expired) => Response::Null,
            Err(e) => {
                metrics.record_error();
                Response::Error(format!("GET error: {e}"))
            }
        },

        Command::Set {
            key,
            value,
            ttl_secs,
        } => {
            let ttl = ttl_secs.map(Duration::from_secs);

            // WAL first (write-ahead)
            if let Some(wal) = wal {
                let mut w = wal.lock().await;
                if let Err(e) = w.append_set(&key, &value, ttl_secs.unwrap_or(0)) {
                    metrics.record_error();
                    return Response::Error(format!("WAL error: {e}"));
                }
                metrics.record_wal_bytes(value.len() as u64 + key.len() as u64);
            }

            match store.set(key, value, ttl).await {
                Ok(()) => Response::Ok("OK".to_string()),
                Err(e) => {
                    metrics.record_error();
                    Response::Error(format!("SET error: {e}"))
                }
            }
        }

        Command::Del { key } => {
            if let Some(wal) = wal {
                let mut w = wal.lock().await;
                if let Err(e) = w.append_delete(&key) {
                    tracing::error!("WAL error on DELETE: {}", e);
                }
            }

            let deleted = store.delete(&key).await;
            Response::Integer(if deleted { 1 } else { 0 })
        }

        Command::Exists { key } => {
            let exists = store.exists(&key).await;
            Response::Integer(if exists { 1 } else { 0 })
        }

        Command::Expire { key, seconds } => {
            if let Some(wal) = wal {
                let mut w = wal.lock().await;
                if let Err(e) = w.append_expire(&key, seconds) {
                    tracing::error!("WAL error on EXPIRE: {}", e);
                }
            }

            let set = store.expire(&key, seconds).await;
            Response::Integer(if set { 1 } else { 0 })
        }

        Command::Ttl { key } => match store.ttl(&key).await {
            Some(Some(secs)) => Response::Integer(secs as i64),
            Some(None) => Response::Integer(-1), // Key exists but no TTL
            None => Response::Integer(-2),       // Key doesn't exist
        },

        Command::Stats => {
            let stats = store.stats().await;
            let snap = metrics.snapshot();
            let stats_json = serde_json::json!({
                "entries": stats.total_entries,
                "memory": stats.total_memory,
                "max_memory": stats.max_memory,
                "shards": stats.num_shards,
                "eviction_policy": stats.eviction_policy.to_string(),
                "hits": snap.hits,
                "misses": snap.misses,
                "sets": snap.sets,
                "deletes": snap.deletes,
                "evictions": snap.evictions,
                "expirations": snap.expirations,
                "uptime_secs": snap.uptime_secs,
                "connections_active": snap.connections_active,
            });

            Response::Data(Bytes::from(stats_json.to_string()))
        }
    }
}

/// Handle an HTTP connection for the admin API.
async fn handle_http_connection(
    mut stream: tokio::net::TcpStream,
    store: Arc<CacheStore>,
    metrics: Arc<CacheMetrics>,
    wal: Option<Arc<Mutex<WalWriter>>>,
    data_dir: std::path::PathBuf,
    persistence: bool,
) {
    let mut buf = BytesMut::with_capacity(4096);
    if let Err(e) = stream.read_buf(&mut buf).await {
        tracing::debug!("HTTP read error: {}", e);
        return;
    }

    let request = String::from_utf8_lossy(&buf);
    let (method, path) = parse_http_request(&request);

    let (status, content_type, body) = match (method.as_str(), path.as_str()) {
        ("GET", "/health") => (
            "200 OK",
            "application/json",
            r#"{"status":"healthy"}"#.to_string(),
        ),

        ("GET", "/ready") => (
            "200 OK",
            "application/json",
            r#"{"status":"ready"}"#.to_string(),
        ),

        ("GET", "/stats") => {
            let stats = store.stats().await;
            let snap = metrics.snapshot();
            let body = serde_json::json!({
                "entries": stats.total_entries,
                "memory_bytes": stats.total_memory,
                "max_memory_bytes": stats.max_memory,
                "shards": stats.num_shards,
                "eviction_policy": stats.eviction_policy.to_string(),
                "hits": snap.hits,
                "misses": snap.misses,
                "hit_rate": if snap.hits + snap.misses > 0 {
                    snap.hits as f64 / (snap.hits + snap.misses) as f64
                } else {
                    0.0
                },
                "sets": snap.sets,
                "deletes": snap.deletes,
                "evictions": snap.evictions,
                "expirations": snap.expirations,
                "uptime_secs": snap.uptime_secs,
                "connections_active": snap.connections_active,
                "connections_total": snap.connections_total,
            });
            ("200 OK", "application/json", body.to_string())
        }

        ("GET", "/metrics") => {
            let entries = store.entry_count().await as u64;
            let memory = store.memory_used() as u64;
            let body = metrics.to_prometheus(entries, memory);
            ("200 OK", "text/plain; version=0.0.4", body)
        }

        ("POST", "/admin/snapshot") => {
            if !persistence {
                (
                    "400 Bad Request",
                    "application/json",
                    r#"{"error":"persistence not enabled"}"#.to_string(),
                )
            } else {
                let snap_path = data_dir.join("fluxcache.snap");
                let data = store.snapshot_data().await;
                let wal_seq = if let Some(ref w) = wal {
                    w.lock().await.sequence()
                } else {
                    0
                };
                match snapshot::write_snapshot(&snap_path, &data, wal_seq) {
                    Ok(()) => {
                        metrics.record_snapshot();
                        (
                            "200 OK",
                            "application/json",
                            format!(r#"{{"status":"ok","entries":{}}}"#, data.len()),
                        )
                    }
                    Err(e) => (
                        "500 Internal Server Error",
                        "application/json",
                        format!(r#"{{"error":"{}"}}"#, e),
                    ),
                }
            }
        }

        ("POST", "/admin/flush") => {
            store.flush().await;
            (
                "200 OK",
                "application/json",
                r#"{"status":"flushed"}"#.to_string(),
            )
        }

        _ => (
            "404 Not Found",
            "application/json",
            r#"{"error":"not found"}"#.to_string(),
        ),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );

    if let Err(e) = stream.write_all(response.as_bytes()).await {
        tracing::debug!("HTTP write error: {}", e);
    }
}

/// Minimal HTTP request parser (method + path only).
fn parse_http_request(request: &str) -> (String, String) {
    let first_line = request.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    if parts.len() >= 2 {
        (parts[0].to_string(), parts[1].to_string())
    } else {
        ("".to_string(), "".to_string())
    }
}
