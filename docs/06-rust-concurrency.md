# 06 — Rust Concurrency & Asynchronous Design

This document audits the asynchronous architecture and concurrency primitives across **FluxCache**. It examines task lifecycles, synchronization strategies, lock contention hotspots, atomic memory orderings, potential race conditions, and blocking operations executed inside async runtimes.

---

## 1. Concurrency Model & Runtime Topology

FluxCache is built on the **Tokio multi-threaded asynchronous runtime** (`#[tokio::main]`). It operates a work-stealing thread pool where incoming I/O events wake lightweight async tasks scheduled across all available physical CPU cores.

```mermaid
graph TD
    subgraph Tokio Multi-Threaded Runtime
        subgraph OS Thread Pool (Worker Threads)
            T1[Worker Thread 1]
            T2[Worker Thread 2]
            TN[Worker Thread N]
        end

        subgraph Task Scheduling Queue
            TaskA[Client Conn Task 1]
            TaskB[Client Conn Task 2]
            TaskC[HTTP Admin Task]
            TaskTTL[Active TTL Scan Task]
            TaskWAL[WAL fsync Task]
            TaskSnap[Periodic Snapshot Task]
        end

        T1 <--> TaskScheduling[Work-Stealing Scheduler]
        T2 <--> TaskScheduling
        TN <--> TaskScheduling
        TaskScheduling --> TaskA
        TaskScheduling --> TaskB
        TaskScheduling --> TaskC
        TaskScheduling --> TaskTTL
        TaskScheduling --> TaskWAL
        TaskScheduling --> TaskSnap
    end

    subgraph Shared Synchronized State
        StoreShards["shards: Vec<tokio::sync::RwLock<CacheShard>>"]
        WalLock["wal: Arc<tokio::sync::Mutex<WalWriter>>"]
        MetricsAtomics["metrics: CacheMetrics (AtomicU64)"]
    end

    TaskA -->|Shard Index mod 16| StoreShards
    TaskB -->|Shard Index mod 16| StoreShards
    TaskA -->|Mutating command| WalLock
    TaskB -->|Mutating command| WalLock
    TaskA -.->|Relaxed increment| MetricsAtomics
```

---

## 2. Synchronization Primitives: Usage & Rationale

| Primitive | Used In | Purpose | Verified Design Rationale vs Alternative |
|---|---|---|---|
| **`tokio::sync::RwLock`** | `fluxcache/src/cache/store.rs:80` | Protects each `CacheShard` independently | Chosen over `std::sync::RwLock` because the lock is held across `.await` yield points (e.g. during async eviction or I/O checks). |
| **`tokio::sync::Mutex`** | `server.rs:32`, `persistence/wal.rs`, `singleflight/mod.rs:61` | Protects `WalWriter` and `SingleflightGroup` in-flight map | Chosen to allow serialized access to the single sequential WAL file without blocking the Tokio OS worker thread during lock contention. |
| **`std::sync::Arc`** | `server.rs`, `cache/store.rs`, `metrics.rs` | Shared ownership of data structures across connection tasks | Thread-safe atomic reference counting. Allows multiple connection tasks to reference the same `CacheStore`, `CacheMetrics`, and `WalWriter`. |
| **`std::sync::atomic::AtomicU64` / `AtomicUsize`** | `metrics.rs:12-23`, `cache/store.rs:90` | High-frequency telemetry counters and global memory usage | Eliminates lock overhead on every telemetry increment. Updates execute via single CPU atomic instructions. |
| **`tokio::sync::broadcast`** | `shutdown.rs:6`, `singleflight/mod.rs:14`, `server.rs:50` | One-to-many event notification | Fan-out communication pattern used for graceful shutdown signals, WAL replication streaming, and singleflight result sharing. |

---

## 3. Deep Dive: Lock Contention & Synchronization Trade-Offs

### 3.1 The "RwLock that Always Writes" Phenomenon
In `fluxcache/src/cache/store.rs:130`:
```rust
pub async fn get(&self, key: &str) -> CacheResult<Bytes> {
    let shard_idx = self.shard_index(key);
    let mut shard = self.shards[shard_idx].write().await;
    ...
}
```

#### Why did this happen?
An `RwLock` is designed to allow multiple concurrent readers (`.read().await`) OR a single exclusive writer (`.write().await`).
However, in FluxCache, **a cache read is not an immutable operation**:
1. It updates the entry's `last_accessed` timestamp.
2. It increments the entry's `frequency` counter.
3. It updates the doubly-linked list pointers in `LruPolicy` or the buckets in `LfuPolicy`.
4. It may execute lazy eviction if the key has expired.

Because all these structures are stored inside `CacheShard` without interior mutability, `get()` must acquire an exclusive `write()` lock.

#### Engineering Consequence
- The `RwLock` provides **zero concurrent read sharing** for data queries.
- Only administrative/inspection operations (`exists`, `ttl`, `stats`, `snapshot_data`, and `entry_count`) actually utilize `.read().await`.
- **Production Solution**: To achieve true read parallelism, separate the key-value dictionary from eviction tracking, or use atomic clock touches and lock-free read-biased cache structures (e.g., Moka's TinyLFU architecture with ring-buffered read events).

---

### 3.2 The Global WAL Lock Contention Bottleneck
In `fluxcache/src/server.rs:512-519`:
```rust
if let Some(wal) = wal {
    let mut w = wal.lock().await;
    if let Err(e) = w.append_set(&key, &value, ttl_secs.unwrap_or(0)) {
        ...
    }
}
```

#### Analysis
- `shards` partitions the cache into 16 independent locks, allowing 16 threads to write in parallel.
- However, when `--persistence true` is enabled, **every `SET`, `DEL`, and `EXPIRE` command across all shards must acquire the single global `wal.lock().await`**.
- This completely serializes write throughput at the WAL lock, negating the concurrency benefit of the 16 cache shards for write operations.
- **Production Solution**: Implement a group-commit pipeline where connection tasks push WAL records into an unbounded MPSC channel, and a single dedicated background task writes and syncs batches to disk.

---

## 4. Atomic Types & Memory Ordering Analysis

FluxCache makes deliberate use of atomic operations in `metrics.rs` and `cache/store.rs`.

### 4.1 Usage of `Ordering::Relaxed`
All telemetry counters in `fluxcache/src/metrics.rs` and the global memory tracking counter `total_memory` in `cache/store.rs` use `Ordering::Relaxed`:

```rust
// metrics.rs:47
self.hits.fetch_add(1, Ordering::Relaxed);

// store.rs:220
self.total_memory.fetch_add(entry_size, Ordering::Relaxed);
```

#### Why `Relaxed` is Correct Here
- `Relaxed` guarantees atomicity of the modification (no lost increments or torn reads).
- It does **not** enforce synchronization or memory ordering between different memory locations (no `happens-before` relationship).
- Because `hits`, `misses`, and `total_memory` are independent statistical counters, they do not guard or synchronize any surrounding non-atomic memory. `Relaxed` ordering is the most efficient choice, avoiding expensive memory fence CPU instructions.

### 4.2 Usage of `Ordering::SeqCst`
In `fluxcache/tests/integration_test.rs:275`, the integration test for `SingleflightGroup` uses `Ordering::SeqCst`:
```rust
count.fetch_add(1, Ordering::SeqCst);
```
- `SeqCst` (Sequentially Consistent) enforces a globally consistent execution order across all threads.
- It is used in test harnesses to verify that the loader executed strictly once across multiple concurrent tasks.

---

## 5. Critical Concurrency Findings: Bugs, Risks, and Antipatterns

### Finding 1: Blocking Synchronous File I/O Inside Async Contexts
- **Classification**: **Confirmed Antipattern / High Performance Risk**
- **Location**: `fluxcache/src/persistence/wal.rs:227` (`self.writer.write_all(&buf)?`), `wal.rs:175` (`self.writer.get_ref().sync_data()?`), and `snapshot.rs:108-115` (`std::fs::File::create`, `sync_all()`, `std::fs::rename`).
- **Evidence**:
  ```rust
  // fluxcache/src/persistence/wal.rs:175
  pub fn sync(&mut self) -> Result<(), WalError> {
      self.writer.flush()?;
      self.writer.get_ref().sync_data()?; // Blocking OS syscall!
      Ok(())
  }
  ```
- **Technical Problem**: `std::fs::File` and `sync_data()` are synchronous blocking operating system calls. When executed inside a Tokio worker thread without `tokio::task::spawn_blocking`, the worker thread is **completely stalled** waiting for physical disk hardware. If the disk stutters or fsync takes 10ms, that Tokio worker thread cannot process any other connection tasks scheduled on it.
- **Production Fix**: Either use `tokio::fs::File` or wrap synchronous persistence operations in `tokio::task::spawn_blocking(move || { ... })`.

---

### Finding 2: `is_shutdown()` Semantic Inversion in `ShutdownSignal`
- **Classification**: **Confirmed Implementation Bug**
- **Location**: `fluxcache/src/shutdown.rs:35-37`
- **Evidence**:
  ```rust
  // fluxcache/src/shutdown.rs:35-37
  pub fn is_shutdown(&self) -> bool {
      self.sender.receiver_count() == 0
  }
  ```
- **Technical Problem**:
  `self.sender.receiver_count()` returns the number of active `broadcast::Receiver` instances.
  1. When the server boots and all tasks subscribe to the shutdown signal, `receiver_count() > 0`. Therefore, `is_shutdown()` returns `false` (correct).
  2. If `shutdown.shutdown()` is called, the signal is broadcast. But tasks have not dropped their receivers yet! `receiver_count()` is still $>0$. `is_shutdown()` still returns `false` (bug)!
  3. Conversely, if no tasks have subscribed yet on initialization, `receiver_count() == 0`. `is_shutdown()` returns `true` before any shutdown ever occurred (bug)!
- **Production Fix**: Maintain an internal `AtomicBool` indicating whether `shutdown()` has been invoked:
  ```rust
  pub struct ShutdownSignal {
      sender: broadcast::Sender<()>,
      triggered: Arc<AtomicBool>,
  }
  ```

---

### Finding 3: Lack of Active Connection Limiting (`max_connections`)
- **Classification**: **Resource Limit / Denial of Service Risk**
- **Location**: `fluxcache/src/server.rs:229-253`
- **Evidence**:
  `config.max_connections` (default 10,000) is parsed into `Config`, but in `spawn_tcp_server`, the `listener.accept()` loop unconditionally accepts new connections and spawns tasks:
  ```rust
  // server.rs:230-248
  match result {
      Ok((socket, addr)) => {
          metrics.record_connection();
          ...
          tokio::spawn(async move { ... });
      }
  }
  ```
- **Technical Problem**: If a client opens 50,000 connections, the server will accept all of them, spawning 50,000 Tokio tasks and consuming file descriptors until the OS crashes the process with `EMFILE` (Too many open files).
- **Production Fix**: Wrap the listener in a `tokio::sync::Semaphore` with `config.max_connections` permits, acquiring a permit before `accept()` and releasing it on connection drop.

---

### Finding 4: Bounded Broadcast Channel Lag in Replication
- **Classification**: **Design Trade-Off / Documented Behavior**
- **Location**: `fluxcache/src/replication/server.rs:119-124`
- **Evidence**:
  ```rust
  Err(broadcast::error::RecvError::Lagged(skipped)) => {
      tracing::warn!("Replica lagged by {} messages. Disconnecting.", skipped);
      break;
  }
  ```
- **Analysis**:
  The replication broadcast channel is bounded (`channel(1024)` in `server.rs:50`). If a replica is slow to consume or experiences network latency, it lags. Tokio's broadcast channel drops older messages and yields `RecvError::Lagged`.
  FluxCache handles this by explicitly **disconnecting the slow replica**, forcing it to reconnect and perform a fresh `+FULLRESYNC`. This prevents slow replicas from causing unbounded memory buffering on the primary.

---

### Finding 5: `SingleflightGroup` Mutex Hold Time
- **Classification**: **Good Concurrency Practice**
- **Location**: `fluxcache/src/singleflight/mod.rs:88-96`
- **Evidence**:
  ```rust
  {
      let map = self.in_flight.lock().await;
      if let Some(in_flight) = map.get(key) {
          let mut rx = in_flight.tx.subscribe();
          ...
          drop(map); // Lock is dropped immediately!
          ...
          match rx.recv().await { ... } // Await is executed outside the lock
      }
  }
  ```
- **Analysis**:
  `SingleflightGroup` correctly releases the `in_flight` mutex **before** calling `rx.recv().await`. Holding the mutex while waiting for the broadcast result would cause all subsequent requests for *different keys* to dead-end on the mutex.
