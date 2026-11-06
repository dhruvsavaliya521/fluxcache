# 03 — High-Level Architecture

This document presents the reconstructed high-level architecture of **FluxCache** based entirely on source code analysis. It details subsystem boundaries, communication patterns, data flow, concurrency models, error propagation paths, and provides 5 comprehensive Mermaid diagrams.

---

## 1. System Components & Subsystem Boundaries

```mermaid
graph TB
    subgraph Client Layer
        App[Application Code]
        Cluster[ClusterClient]
        Pool[Deadpool Connection Pool]
        Ring[ConsistentHashRing BTreeMap]
        App --> Cluster
        Cluster --> Ring
        Cluster --> Pool
    end

    subgraph Network Transport Layer
        TCP_Data[TCP Data Plane :6380]
        HTTP_Admin[HTTP Admin Plane :9090]
        TCP_Repl[Replication Plane :6381]
    end

    Pool -->|Raw TCP Framed| TCP_Data
    Admin_Client[Prometheus / Operator] -->|HTTP/1.1| HTTP_Admin

    subgraph FluxCache Server Node (fluxcache)
        subgraph Server Core (server.rs)
            Listener[TcpListener Task]
            ConnTask[Connection Handler Tasks]
            AdminTask[HTTP Admin Handler]
            Shutdown[ShutdownSignal]
        end

        subgraph Protocol Engine (protocol/)
            Parser[Protocol Parser]
            Cmd[Command Enum]
            Resp[Response Encoder]
        end

        subgraph Storage Engine (cache/)
            Store[CacheStore Router]
            Shard0[CacheShard 0<br/>RwLock]
            ShardN[CacheShard N-1<br/>RwLock]
            
            subgraph Shard Internals
                Map[HashMap String, CacheEntry]
                LRU[LruPolicy Arena List]
                LFU[LfuPolicy Frequency Buckets]
                TTL[TtlTracker BTreeMap]
            end
        end

        subgraph Persistence Engine (persistence/)
            WAL[WalWriter<br/>Mutex]
            Snap[Snapshot Engine]
            WAL_File[(fluxcache.wal)]
            Snap_File[(fluxcache.snap)]
        end

        subgraph Replication Engine (replication/)
            ReplServer[ReplicationServer]
            ReplClient[ReplicationClient]
            Broadcast[Broadcast Channel]
        end

        subgraph Observability (metrics.rs)
            Metrics[CacheMetrics Atomic Counters]
        end
    end

    TCP_Data --> Listener
    Listener --> ConnTask
    ConnTask --> Parser
    Parser --> Cmd
    Cmd --> Store
    Store --> Resp
    Resp --> ConnTask

    TCP_Repl --> ReplServer
    ReplServer --> Broadcast
    WAL --> Broadcast

    Store --> Shard0
    Store --> ShardN
    Shard0 --- Map
    Shard0 --- LRU
    Shard0 --- LFU
    Shard0 --- TTL

    Cmd -->|If SET/DEL/EXPIRE| WAL
    WAL --> WAL_File
    Snap --> Snap_File
    Snap -.->|Reads state| Store

    ConnTask -.->|Record hits/misses| Metrics
    HTTP_Admin --> AdminTask
    AdminTask -.->|Scrape| Metrics
    AdminTask -.->|Snapshot command| Snap
```

---

## 2. Component Responsibilities & Communication

### 2.1 The Layers

1. **Client Layer (`fluxcache-client`)**:
   - `ClusterClient`: Computes key hash (`xxh3_64`) against a virtual node ring (`BTreeMap<u64, String>`) to select the target node.
   - `Pool`: Manages reusable TCP streams via `deadpool` to prevent socket creation overhead.
   - `Connection`: Formats ASCII/binary command frames, manages `BytesMut` read/write buffers, and parses RESP responses.

2. **Server Network Layer (`server.rs`)**:
   - `TcpListener`: Binds to `config.listen` (default `127.0.0.1:6380`) and spawns an independent, lightweight Tokio task per connection.
   - `read_timeout` & `max_request_size`: Guard against denial-of-service, slowloris attacks, and memory exhaustion.
   - Frame Extractor (`parser.rs`): Handles framed binary messages or newline-delimited ASCII strings with support for pipelining.

3. **Storage Engine (`cache/store.rs`)**:
   - `CacheStore`: Top-level facade owning `Vec<RwLock<CacheShard>>`.
   - Modulo Sharding: Maps keys to shards via `xxh3_64(key) % num_shards`.
   - Memory Enforcement: Computes shard memory pressure and triggers synchronous eviction before inserting new items.
   - Eviction Delegation: Dispatches eviction to either `LruPolicy` or `LfuPolicy`.

4. **Persistence Engine (`persistence/`)**:
   - `WalWriter`: Thread-safe append-only file writer wrapped in `Arc<Mutex<WalWriter>>`. Emits 30-byte fixed headers + payload + CRC32 checksums.
   - Periodic Sync: Background task flushes and invokes `sync_data()` based on `wal_sync_interval` (default 100ms).
   - Atomic Snapshot: Writes whole cache state into a temporary file (`.snap.tmp`), forces disk sync (`sync_all()`), and renames atomically to `fluxcache.snap`.

5. **Replication Engine (`replication/`)**:
   - Handshake: Replica issues `PSYNC <seq>`. Primary replies with `+FULLRESYNC <len>` followed by raw binary snapshot bytes.
   - Streaming: Primary broadcasts live mutations through an in-memory `tokio::sync::broadcast` channel (capacity 1024) to active replica handlers.

---

## 3. Concurrency & Threading Model

FluxCache avoids the single-threaded event loop architecture of Redis in favor of a multi-threaded, work-stealing task architecture:

```mermaid
graph TD
    subgraph Multi-Threaded Tokio Runtime
        Worker1[OS Worker Thread 1]
        Worker2[OS Worker Thread 2]
        WorkerN[OS Worker Thread N]
    end

    subgraph Connection Tasks
        Conn1[Conn Task 1]
        Conn2[Conn Task 2]
        ConnM[Conn Task M]
    end

    Worker1 -.->|Executes| Conn1
    Worker2 -.->|Executes| Conn2
    WorkerN -.->|Executes| ConnM

    subgraph Lock Partitioning
        Shard0[Shard 0 Lock]
        Shard1[Shard 1 Lock]
        ShardK[Shard 15 Lock]
    end

    Conn1 -->|Hashes to Shard 0| Shard0
    Conn2 -->|Hashes to Shard 1| Shard1
    ConnM -->|Hashes to Shard 0| Shard0
```

### Concurrency Highlights
- **Fine-Grained Sharding**: Instead of one global lock, the keyspace is partitioned into $N$ shards (default 16). Concurrent operations accessing different shards execute completely in parallel without contention.
- **Write Lock Limitation on GET**: Notice that in `fluxcache/src/cache/store.rs:130`, `store.get()` acquires a **write lock** (`self.shards[shard_idx].write().await`) to support metadata touching and lazy expiration. Thus, concurrent reads to keys in the **same shard** serialize.
- **Lock-Free Metrics**: All operational metrics (`hits`, `misses`, `sets`, etc.) use `std::sync::atomic::AtomicU64` with `Ordering::Relaxed` (`metrics.rs:46-94`), ensuring zero lock contention on counter updates.

---

## 4. End-to-End Operation Flows

### 4.1 Diagram: Complete SET Request Flow

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant ConnTask as Connection Handler (server.rs)
    participant Parser as Protocol Parser (parser.rs)
    participant WAL as WalWriter (wal.rs)
    participant Store as CacheStore (store.rs)
    participant Shard as CacheShard (store.rs)
    participant LRU as LruPolicy (lru.rs)

    Client->>ConnTask: [TCP Stream] "SET session:99 12\r\nactive_state\r\n"
    ConnTask->>Parser: extract_frame(&mut buf)
    Parser-->>ConnTask: Frame bytes
    ConnTask->>Parser: parse_command(&frame)
    Parser-->>ConnTask: Command::Set { key: "session:99", value: "active_state", ttl_secs: None }

    alt Persistence Enabled (wal != None)
        ConnTask->>WAL: lock().await
        ConnTask->>WAL: append_set("session:99", "active_state", 0)
        WAL->>WAL: write_record() (Header + Key + Value + CRC32)
        opt Replication Active
            WAL->>WAL: broadcast_tx.send(wal_bytes)
        end
    end

    ConnTask->>Store: set("session:99", Bytes, None)
    Store->>Store: shard_idx = xxh3_64("session:99") % num_shards
    Store->>Shard: shards[shard_idx].write().await

    opt Overwrite Existing Key
        Shard->>Shard: data.remove("session:99")
        Shard->>LRU: remove_key("session:99")
        Shard->>Shard: memory_used -= old_size
    end

    loop While memory_used + new_size > max_memory_per_shard
        Store->>LRU: evict()
        LRU-->>Store: victim_key
        Store->>Shard: data.remove(victim_key)
        Store->>Shard: memory_used -= victim_size
    end

    Store->>LRU: touch("session:99")
    Store->>Shard: data.insert("session:99", CacheEntry)
    Store->>Shard: memory_used += new_size
    Store->>Store: total_memory.fetch_add(new_size)
    Store-->>ConnTask: Ok(())

    ConnTask->>Client: [TCP Stream] "+OK\r\n"
```

---

### 4.2 Diagram: Complete GET Request Flow

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant ConnTask as Connection Handler (server.rs)
    participant Parser as Protocol Parser (parser.rs)
    participant Store as CacheStore (store.rs)
    participant Shard as CacheShard (store.rs)
    participant LRU as LruPolicy (lru.rs)

    Client->>ConnTask: [TCP Stream] "GET session:99\r\n"
    ConnTask->>Parser: extract_frame(&mut buf)
    Parser-->>ConnTask: Frame bytes
    ConnTask->>Parser: parse_command(&frame)
    Parser-->>ConnTask: Command::Get { key: "session:99" }

    ConnTask->>Store: get("session:99")
    Store->>Store: shard_idx = xxh3_64("session:99") % num_shards
    Store->>Shard: shards[shard_idx].write().await

    alt Key Missing in Shard
        Store->>Store: metrics.record_miss()
        Store-->>ConnTask: Err(CacheError::NotFound)
        ConnTask->>Client: "_\r\n" (Null)
    else Key Present but Expired (Instant::now() >= expires_at)
        Store->>Shard: data.remove("session:99")
        Store->>LRU: remove_key("session:99")
        Store->>Shard: memory_used -= size
        Store->>Store: metrics.record_expiration()
        Store->>Store: metrics.record_miss()
        Store-->>ConnTask: Err(CacheError::Expired)
        ConnTask->>Client: "_\r\n" (Null)
    else Key Present and Valid (Hit)
        Shard->>Shard: entry.touch() (last_accessed = now, freq += 1)
        Store->>LRU: shard.lru.touch("session:99")
        Store->>Store: metrics.record_hit()
        Store-->>ConnTask: Ok(Bytes("active_state"))
        ConnTask->>Client: "$12\r\nactive_state\r\n"
    end
```

---

### 4.3 Diagram: Persistence & Recovery Flow

```mermaid
sequenceDiagram
    autonumber
    participant Main as Server Boot (server.rs)
    participant SnapEngine as Snapshot Engine (snapshot.rs)
    participant WalEngine as WAL Reader (wal.rs)
    participant Store as CacheStore (store.rs)
    participant Disk as Local Disk (data_dir)

    Note over Main, Disk: Phase 1: Point-in-time Snapshot Loading
    Main->>Disk: Check if fluxcache.snap exists
    alt Snapshot Exists
        Main->>Disk: Read full file into memory
        Main->>SnapEngine: read_snapshot(path)
        SnapEngine->>SnapEngine: Validate MAGIC ("FLXS") & Version
        SnapEngine->>SnapEngine: Compute & Verify CRC32 Checksum
        SnapEngine-->>Main: (entries: Vec<SnapshotEntry>, wal_sequence: u64)
        loop For each entry in snapshot
            Main->>Store: store.set(key, value, remaining_ttl)
        end
        Note over Main: Base state reconstructed up to wal_sequence
    else Snapshot Missing
        Note over Main: wal_sequence initialized to 0
    end

    Note over Main, Disk: Phase 2: WAL Delta Replay
    Main->>Disk: Check if fluxcache.wal exists
    alt WAL Exists
        Main->>WalEngine: read_all(path)
        loop Read record by record
            WalEngine->>WalEngine: Validate MAGIC ("FLUX") & Version
            WalEngine->>WalEngine: Compute & Verify CRC32
            alt Corruption or Truncation Encountered
                WalEngine-->>Main: Stop reading & return valid records up to corruption
            else Record Valid
                WalEngine-->>Main: WalRecord
            end
        end
        loop For each record where sequence > wal_sequence
            alt Record is SET
                Main->>Store: store.set(key, value, ttl)
            else Record is DELETE
                Main->>Store: store.delete(key)
            else Record is EXPIRE
                Main->>Store: store.expire(key, seconds)
            end
        end
        Note over Main: State brought forward to last flushed WAL mutation
    end

    Note over Main: Phase 3: Resume Normal Server Operations
```

---

### 4.4 Diagram: Replication Flow & Incomplete Implementation Boundary

```mermaid
sequenceDiagram
    autonumber
    participant Primary as Primary Server (:6380, repl :6381)
    participant ReplicaClient as Replica Daemon (client.rs)
    participant ReplicaStore as Replica CacheStore

    ReplicaClient->>Primary: TCP Connect to :6381
    ReplicaClient->>Primary: "PSYNC 0\n"
    Note over Primary: Primary ignores requested sequence<br/>and triggers full state dump
    Primary->>Primary: store.snapshot_data()
    Primary->>Primary: Serialize binary snapshot to buffer
    Primary->>ReplicaClient: "+FULLRESYNC <byte_length>\n"
    Primary->>ReplicaClient: <Binary Snapshot Data>

    Note over ReplicaClient: Replica consumes snapshot
    ReplicaClient->>ReplicaClient: Write to fluxcache_repl.snap
    ReplicaClient->>ReplicaClient: read_snapshot() & CRC verify
    ReplicaClient->>ReplicaStore: store.flush() (Wipe local state)
    loop For each entry in snapshot
        ReplicaClient->>ReplicaStore: store.set(key, value, ttl)
    end
    Note over ReplicaClient: State successfully initialized from snapshot!

    Note over Primary, ReplicaClient: Continuous Live WAL Streaming
    Primary->>Primary: Mutating operation appends to WAL
    Primary->>Primary: Broadcasts raw WAL record bytes
    Primary->>ReplicaClient: [TCP Stream] Raw WAL Record Bytes

    critical INCOMPLETE IMPLEMENTATION GAP
        ReplicaClient->>ReplicaClient: stream.read_buf(&mut buf)
        Note over ReplicaClient: replication/client.rs:103-110<br/>Bytes read into buffer are DROPPED.<br/>NO parser exists. NO store mutation occurs.<br/>Replica falls permanently out of sync!
    end
```

---

## 5. Error Propagation & Observability Architecture

### 5.1 Error Propagation Strategy
- **Client Facing**: Domain errors (`CacheError::Expired`, `CacheError::NotFound`) map cleanly to RESP responses (`Response::Null`). Severe issues (`ValueTooLarge`, `EvictionFailed`) map to RESP error strings (`Response::Error("-ERR ...\r\n")`).
- **Disk Failures**: WAL append failures immediately return an error response to the client (`server.rs:516`), preventing silent unpersisted writes. Snapshot failures are logged via `tracing::error!` but do not crash the daemon.
- **Protocol Parse Errors**: Malformed ASCII or invalid binary frames increment `metrics.request_errors`, return a `-ERR` string, clear the input buffer, and close the stream if non-recoverable.

### 5.2 Observability Plane
- **Metrics Scraping**: Prometheus metrics are served on a separate port (`admin_listen`, default `:9090`) at `/metrics` so that telemetry scrapers cannot block or consume capacity on the primary TCP cache data plane.
- **Health Probes**: `/health` and `/ready` endpoints return HTTP 200 JSON payloads for Kubernetes liveness and readiness probes.
