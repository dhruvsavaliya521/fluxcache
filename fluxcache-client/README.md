# FluxCache Client

An asynchronous, pooled Rust client for the FluxCache server featuring connection pooling, automatic protocol parsing, and consistent hashing for distributed caching over multiple nodes.

## Features
* **Async Native**: Built purely on `tokio`.
* **Connection Pooling**: Uses `deadpool` for robust connection reuse and lifecycle management.
* **Consistent Hashing**: Includes a `ClusterClient` that automatically distributes keys across a ring of physical nodes and virtual nodes using fast `xxh3` hashing.
* **Type-Safe API**: Clean Rust bindings (`get`, `set`, `exists`, `del`) abstracting the raw TCP wire format.

## Example Usage

```rust
use fluxcache_client::ClusterClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to a cluster of FluxCache nodes
    let nodes = vec!["127.0.0.1:6380", "127.0.0.1:6381", "127.0.0.1:6382"];
    
    // 150 virtual nodes per physical node, pool size of 10 connections per node
    let client = ClusterClient::new(&nodes, 150, 10);

    // Set a value with a 60-second TTL
    client.set("my_key", b"hello world", Some(60)).await?;

    // Check if it exists
    if client.exists("my_key").await? {
        println!("Key exists!");
    }

    // Get the value
    if let Some(data) = client.get("my_key").await? {
        println!("Value: {}", String::from_utf8_lossy(&data));
    }

    // Delete it
    client.del("my_key").await?;

    Ok(())
}
```

## Internal Design
The `ClusterClient` maps keys to `deadpool::Pool` instances associated with physical nodes. When a command is issued, it performs an O(log N) binary search on the `BTreeMap` consistent hash ring to select the responsible node pool, leases a `Connection`, and performs the text or length-prefixed binary request asynchronously.
