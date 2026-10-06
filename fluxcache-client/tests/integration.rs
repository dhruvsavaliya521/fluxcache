use fluxcache_client::ClusterClient;
use std::time::Duration;

#[tokio::test]
async fn test_client_cluster() {
    // 1. Start a local server instances
    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp_listener.local_addr().unwrap().to_string();
    drop(tcp_listener);

    let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_addr = admin_listener.local_addr().unwrap().to_string();
    drop(admin_listener);

    let addr_clone = addr.clone();
    
    // Start fluxcache server
    tokio::spawn(async move {
        let config = fluxcache::config::Config {
            listen: addr_clone,
            admin_listen: admin_addr,
            max_memory: 10 * 1024 * 1024,
            eviction_policy: fluxcache::config::EvictionPolicy::Lru,
            shards: 4,
            persistence: false,
            data_dir: std::path::PathBuf::from("/tmp/fluxcache_client_test"),
            wal_sync_interval: Duration::from_millis(100),
            snapshot_interval: Duration::from_secs(300),
            expiration_scan_interval: Duration::from_millis(100),
            log_level: "error".to_string(),
            json_logs: false,
            max_connections: 100,
            read_timeout: Duration::from_secs(30),
            max_request_size: 1024 * 1024,
        };

        let server = fluxcache::server::FluxServer::new(config).await.unwrap();
        let _ = server.run().await;
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 2. Connect client
    let nodes = vec![addr.as_str()];
    let client = ClusterClient::new(&nodes, 150, 5);

    // 3. Test basic operations
    let key = "client_key";
    let value = b"client_value";

    // Set
    client.set(key, value, None).await.expect("Set failed");

    // Exists
    let exists = client.exists(key).await.expect("Exists failed");
    assert!(exists);

    // Get
    let data = client.get(key).await.expect("Get failed").expect("Value not found");
    assert_eq!(data.as_ref(), value);

    // Delete
    let deleted = client.del(key).await.expect("Del failed");
    assert!(deleted);

    // Get after delete
    let data = client.get(key).await.expect("Get failed");
    assert!(data.is_none());
}
