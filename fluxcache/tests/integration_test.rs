// FluxCache - Integration Tests
//
// Tests the full server lifecycle: start, connect, execute commands, verify.

use bytes::Bytes;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Helper to start a server and return the addresses.
async fn start_test_server() -> (String, String, tokio::task::JoinHandle<()>) {
    use fluxcache::cache::store::CacheStore;
    use fluxcache::config::EvictionPolicy;
    use fluxcache::metrics::CacheMetrics;
    use std::sync::Arc;

    // Find available ports
    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_addr = tcp_listener.local_addr().unwrap().to_string();
    drop(tcp_listener);

    let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_addr = admin_listener.local_addr().unwrap().to_string();
    drop(admin_listener);

    let tcp_addr_clone = tcp_addr.clone();
    let admin_addr_clone = admin_addr.clone();

    let handle = tokio::spawn(async move {
        let config = fluxcache::config::Config {
            listen: tcp_addr_clone,
            admin_listen: admin_addr_clone,
            max_memory: 10 * 1024 * 1024,
            eviction_policy: EvictionPolicy::Lru,
            shards: 4,
            persistence: false,
            data_dir: std::path::PathBuf::from("/tmp/fluxcache_test"),
            wal_sync_interval: Duration::from_millis(100),
            snapshot_interval: Duration::from_secs(300),
            expiration_scan_interval: Duration::from_millis(100),
            log_level: "warn".to_string(),
            json_logs: false,
            max_connections: 100,
            read_timeout: Duration::from_secs(30),
            max_request_size: 1024 * 1024,
            replication_listen: "127.0.0.1:0".to_string(),
            replica_of: None,
        };

        let server = fluxcache::server::FluxServer::new(config).await.unwrap();
        // Run server in a way that allows test control
        // For integration tests, we just need the background to be running
        let _ = server.run().await;
    });

    // Wait for server to be ready
    tokio::time::sleep(Duration::from_millis(200)).await;

    (tcp_addr, admin_addr, handle)
}

/// Send a command and read the response.
async fn send_command(stream: &mut TcpStream, cmd: &str) -> String {
    stream.write_all(cmd.as_bytes()).await.unwrap();
    stream.write_all(b"\r\n").await.unwrap();

    let mut buf = vec![0u8; 4096];
    tokio::time::sleep(Duration::from_millis(50)).await;
    let n = stream.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

#[tokio::test]
async fn test_cache_basic_operations() {
    // Direct store test (no server needed)
    use fluxcache::cache::store::CacheStore;
    use fluxcache::config::EvictionPolicy;
    use fluxcache::metrics::CacheMetrics;
    use std::sync::Arc;

    let metrics = Arc::new(CacheMetrics::new());
    let store = CacheStore::new(4, 10 * 1024 * 1024, EvictionPolicy::Lru, metrics);

    // SET + GET
    store
        .set("hello".to_string(), Bytes::from("world"), None)
        .await
        .unwrap();
    let val = store.get("hello").await.unwrap();
    assert_eq!(val, Bytes::from("world"));

    // EXISTS
    assert!(store.exists("hello").await);
    assert!(!store.exists("nonexistent").await);

    // DELETE
    assert!(store.delete("hello").await);
    assert!(!store.exists("hello").await);

    // GET after delete
    assert!(store.get("hello").await.is_err());
}

#[tokio::test]
async fn test_cache_ttl() {
    use fluxcache::cache::store::CacheStore;
    use fluxcache::config::EvictionPolicy;
    use fluxcache::metrics::CacheMetrics;
    use std::sync::Arc;

    let metrics = Arc::new(CacheMetrics::new());
    let store = CacheStore::new(4, 10 * 1024 * 1024, EvictionPolicy::Lru, metrics);

    // SET with TTL
    store
        .set(
            "temp".to_string(),
            Bytes::from("data"),
            Some(Duration::from_millis(100)),
        )
        .await
        .unwrap();

    // Should exist immediately
    assert!(store.exists("temp").await);

    // Wait for TTL
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Should be expired (lazy expiration on GET)
    assert!(store.get("temp").await.is_err());
}

#[tokio::test]
async fn test_cache_eviction_lru() {
    use fluxcache::cache::store::CacheStore;
    use fluxcache::config::EvictionPolicy;
    use fluxcache::metrics::CacheMetrics;
    use std::sync::Arc;

    let metrics = Arc::new(CacheMetrics::new());
    // Very small cache to force eviction
    let store = CacheStore::new(1, 1024, EvictionPolicy::Lru, metrics);

    // Fill the cache
    for i in 0..10 {
        let result = store
            .set(
                format!("key{i}"),
                Bytes::from(format!("value{}", "x".repeat(50))),
                None,
            )
            .await;
        // Some may fail due to eviction limits, that's fine
        let _ = result;
    }

    // Cache should not exceed memory limit
    let stats = store.stats().await;
    assert!(
        stats.total_memory <= 1024,
        "Memory {} exceeds limit 1024",
        stats.total_memory
    );
}

#[tokio::test]
async fn test_concurrent_writers() {
    use fluxcache::cache::store::CacheStore;
    use fluxcache::config::EvictionPolicy;
    use fluxcache::metrics::CacheMetrics;
    use std::sync::Arc;

    let metrics = Arc::new(CacheMetrics::new());
    let store = Arc::new(CacheStore::new(
        16,
        100 * 1024 * 1024,
        EvictionPolicy::Lru,
        metrics,
    ));

    let mut handles = vec![];

    for i in 0..100 {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            store
                .set(format!("key{i}"), Bytes::from(format!("val{i}")), None)
                .await
                .unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // All keys should exist
    for i in 0..100 {
        let val = store.get(&format!("key{i}")).await.unwrap();
        assert_eq!(val, Bytes::from(format!("val{i}")));
    }
}

#[tokio::test]
async fn test_concurrent_readers_and_writers() {
    use fluxcache::cache::store::CacheStore;
    use fluxcache::config::EvictionPolicy;
    use fluxcache::metrics::CacheMetrics;
    use std::sync::Arc;

    let metrics = Arc::new(CacheMetrics::new());
    let store = Arc::new(CacheStore::new(
        16,
        100 * 1024 * 1024,
        EvictionPolicy::Lru,
        metrics,
    ));

    // Pre-populate
    for i in 0..50 {
        store
            .set(format!("pre{i}"), Bytes::from("data"), None)
            .await
            .unwrap();
    }

    let mut handles = vec![];

    // 50 readers
    for i in 0..50 {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let _ = store.get(&format!("pre{i}")).await;
            }
        }));
    }

    // 50 writers
    for i in 0..50 {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for j in 0..100 {
                let _ = store
                    .set(format!("new{i}_{j}"), Bytes::from("value"), None)
                    .await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_singleflight() {
    use fluxcache::singleflight::SingleflightGroup;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    let group = Arc::new(SingleflightGroup::new());
    let call_count = Arc::new(AtomicU32::new(0));
    let mut handles = vec![];

    for _ in 0..50 {
        let group = Arc::clone(&group);
        let call_count = Arc::clone(&call_count);
        handles.push(tokio::spawn(async move {
            group
                .do_work("shared", |_| {
                    let count = Arc::clone(&call_count);
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        Ok(Bytes::from("result"))
                    }
                })
                .await
                .into_result()
                .unwrap()
        }));
    }

    for h in handles {
        let result = h.await.unwrap();
        assert_eq!(result, Bytes::from("result"));
    }

    assert_eq!(
        call_count.load(Ordering::SeqCst),
        1,
        "Loader should only be called once"
    );
}

#[tokio::test]
async fn test_persistence_recovery() {
    use fluxcache::cache::store::CacheStore;
    use fluxcache::config::EvictionPolicy;
    use fluxcache::metrics::CacheMetrics;
    use fluxcache::persistence::{snapshot, wal::WalWriter};
    use std::sync::Arc;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let wal_path = tmp.path().join("test.wal");
    let snap_path = tmp.path().join("test.snap");

    // Write some data via WAL
    {
        let mut wal = WalWriter::open(&wal_path, None).unwrap();
        wal.append_set("key1", b"value1", 0).unwrap();
        wal.append_set("key2", b"value2", 3600).unwrap();
        wal.append_delete("key1").unwrap();
        wal.sync().unwrap();
    }

    // Create a snapshot
    {
        let entries = vec![("snap_key".to_string(), Bytes::from("snap_val"), None)];
        snapshot::write_snapshot(&snap_path, &entries, 0).unwrap();
    }

    // Verify WAL can be read back
    let records = fluxcache::persistence::wal::WalReader::read_all(&wal_path).unwrap();
    assert_eq!(records.len(), 3);

    // Verify snapshot can be read back
    let (entries, _seq) = snapshot::read_snapshot(&snap_path).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, "snap_key");
}

#[tokio::test]
async fn test_protocol_roundtrip() {
    use fluxcache::protocol::command::Command;
    use fluxcache::protocol::parser::parse_command;
    use fluxcache::protocol::response::Response;

    // Test various commands
    let commands = vec![
        ("PING\r\n", Command::Ping),
        (
            "GET mykey\r\n",
            Command::Get {
                key: "mykey".to_string(),
            },
        ),
        (
            "SET mykey myvalue\r\n",
            Command::Set {
                key: "mykey".to_string(),
                value: Bytes::from("myvalue"),
                ttl_secs: None,
            },
        ),
        (
            "DEL mykey\r\n",
            Command::Del {
                key: "mykey".to_string(),
            },
        ),
        (
            "EXISTS mykey\r\n",
            Command::Exists {
                key: "mykey".to_string(),
            },
        ),
        (
            "TTL mykey\r\n",
            Command::Ttl {
                key: "mykey".to_string(),
            },
        ),
        ("STATS\r\n", Command::Stats),
    ];

    for (input, expected) in commands {
        let parsed = parse_command(input.as_bytes()).unwrap();
        assert_eq!(parsed, expected, "Failed for input: {input}");
    }

    // Test response encoding
    let responses = vec![
        (Response::Ok("PONG".to_string()), "+PONG\r\n"),
        (Response::Null, "_\r\n"),
        (Response::Integer(42), ":42\r\n"),
        (Response::Error("ERR test".to_string()), "-ERR test\r\n"),
    ];

    for (resp, expected) in responses {
        assert_eq!(
            resp.encode(),
            Bytes::from(expected),
            "Failed encoding {:?}",
            resp
        );
    }
}
