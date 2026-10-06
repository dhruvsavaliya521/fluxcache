# FluxCache Architecture

## Overview
FluxCache is a highly concurrent, asynchronous key-value cache designed to run on the Tokio runtime. It employs a modular architecture with clearly separated subsystems for caching logic, protocol handling, persistence, and network coordination.

## Core Components

### 1. TCP Server & HTTP Admin Plane (`server.rs`)
The entry point. It manages the `TcpListener`, spawning lightweight Tokio tasks per incoming connection. It also binds an HTTP server for observability (`/metrics`, `/stats`) and management (triggering snapshots).

### 2. Protocol Parser & Responder (`protocol/`)
Implements a custom text and length-prefixed binary framed protocol.
- `parser.rs` handles converting raw socket byte streams (`BytesMut`) into structured `Command` enums.
- `response.rs` converts internal results back into the serialized wire format.

### 3. Sharded Cache Store (`cache/store.rs`)
To prevent lock contention on a single global `HashMap`, the store is partitioned into N shards (default 16).
- Each shard contains its own `HashMap`, `LruPolicy`/`LfuPolicy`, `TtlTracker`, and memory tracker.
- Keys are mapped to shards using fast `xxh3_64` hashing.
- Synchronization is handled via fine-grained per-shard `tokio::sync::RwLock`.

### 4. Eviction Policies (`cache/lru.rs`, `cache/lfu.rs`)
- **LRU (Least Recently Used)**: Built using an arena-allocated doubly-linked list (`Vec<Node>`) to maintain O(1) performance without unsafe pointers.
- **LFU (Least Frequently Used)**: Implements frequency bucketing with periodic exponential decay to prevent old "hot" keys from permanently poisoning the cache.

### 5. TTL Manager (`cache/ttl.rs`)
Handles expiration in two ways:
- **Lazy Expiration**: Checked automatically during `GET`. If expired, it's deleted instantly.
- **Active Scanning**: A background Tokio task periodically polls the `TtlTracker` (which uses a sorted `BTreeMap` of timestamps) to sweep out dead keys.

### 6. Persistence Engine (`persistence/`)
- **WAL (Write-Ahead Log)**: Appends mutations (SET/DEL/EXPIRE) to disk sequentially. Includes CRC32 checksums to protect against disk corruption. Syncs to disk periodically via background task.
- **Snapshots**: Takes a complete memory dump to disk using an atomic `rename` strategy. On startup, FluxCache loads the snapshot, then replays the WAL from the snapshot's saved sequence number.

### 7. Singleflight Coalescing (`singleflight/`)
When multiple concurrent requests ask for the same missing key (e.g., triggering an expensive database load), `SingleflightGroup` deduplicates them. One task fetches the data; the rest await the result via Toko `broadcast` channels, mitigating cache stampedes.

### 8. Consistent Hashing (`shard/consistent_hash.rs`)
Implements a virtual-node-based hash ring for distributed key placement. Ensures minimal key migration when physical nodes are added or removed from the cache cluster.
