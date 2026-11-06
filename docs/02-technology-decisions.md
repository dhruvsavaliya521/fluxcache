# 02 — Technology Stack & Engineering Decisions

This document evaluates the complete technology stack used across the **FluxCache** workspace. For each component, we document its verified role in the source code, why it was chosen, viable production alternatives, engineering trade-offs, and what architectural changes would result from switching technologies.

---

## 1. Verified Design Intent vs. Engineering Interpretation

To maintain strict technical rigor, this audit explicitly separates:
- **Verified Design Intent**: Design choices explicitly justified in codebase comments, docstrings, or architecture decision records (such as `fluxcache/docs/decisions/001-core-architecture.md`).
- **Engineering Interpretation**: Architectural deductions based on industry standard practices, Rust idioms, and structural analysis of the codebase.

---

## 2. Technology Decision Table

| Technology | Category | Primary Locations in Code | Verified Intent vs Interpretation | Why Chosen | Viable Alternatives | Trade-Offs (Pros vs Cons) | Impact if Alternative Selected |
|---|---|---|---|---|---|---|---|
| **Rust (2021 Edition)** | Systems Language | Entire workspace | **Verified**: Built to explore lock-free abstractions, concurrent hash maps, and safety without GC (`README.md:9`). | Memory safety without garbage collection pauses; zero-cost abstractions; ownership model guarantees thread safety at compile time. | C++, Go, Java | **Pros**: Predictable sub-millisecond latencies; no GC stop-the-world pauses; memory safety.<br>**Cons**: Steep learning curve; longer compile times; borrow checker constraints on intrusive data structures. | Switching to Go/Java would introduce GC pauses impacting p99 latencies; switching to C++ would increase risk of memory corruption (use-after-free, buffer overflows). |
| **Tokio (`1.x`, full)** | Async Runtime | `fluxcache/src/main.rs`, `server.rs`, `client`, `bench` | **Verified**: "We will use `tokio` for the async networking layer to handle thousands of concurrent TCP connections efficiently" (`docs/decisions/001-core-architecture.md:9`). | Industry standard work-stealing multi-threaded async runtime. Provides non-blocking TCP, timers (`Interval`), and synchronization primitives. | `async-std`, `smol`, Thread-per-core (e.g., `glommio` / `monoio`) | **Pros**: Battle-tested, vast ecosystem, high task throughput.<br>**Cons**: Work-stealing across cores adds cache bouncing and thread synchronization overhead compared to thread-per-core architectures. | Thread-per-core architecture (`monoio` / `io_uring`) would eliminate cross-thread locking entirely (share-nothing), significantly increasing throughput for cache workloads. |
| **Raw TCP (Framed)** | Transport Protocol | `fluxcache/src/protocol/parser.rs`, `response.rs`, `server.rs` | **Verified**: "Raw ASCII string parser with length-prefixed binary strings instead of FlatBuffers/Protobuf to minimize byte overhead on small payloads" (`README.md:138`). | Lightweight wire protocol with low framing overhead for key-value transactions; human-readable debugging via telnet/netcat. | gRPC / HTTP/2, FlatBuffers, Redis RESP3 | **Pros**: Zero serialization library overhead; easy telnet debugging; supports pipelining.<br>**Cons**: Requires custom manual parsing code; edge cases in line-mode vs length-prefixed mode can lead to parser bugs. | Switching to gRPC/Protobuf would add formal schema validation and auto-generated multi-language SDKs, but would increase CPU overhead and latency for small 64-byte payloads. |
| **`bytes` (`1.x`)** | Zero-Copy Memory Management | `cache/entry.rs:12`, `protocol/parser.rs`, `response.rs` | **Interpretation**: Chosen for efficient slicing and reference-counted sharing without copying raw byte arrays. | `Bytes` provides thread-safe $O(1)$ shallow clones via internal `Arc` pointers; `BytesMut` provides contiguous buffer accumulation. | `Vec<u8>`, `Box<[u8]>` | **Pros**: Avoids deep data copies when slicing frames or returning values from cache entries.<br>**Cons**: Atomic reference count increments/decrements on clones; potential buffer fragmentation if slices retain large buffers. | Using `Vec<u8>` would require deep heap allocations on every `GET` cache hit to return values to the network writer. |
| **`xxhash-rust` (`xxh3`)** | Hashing Algorithm | `cache/store.rs:26`, `shard/consistent_hash.rs:10`, `client/src/cluster.rs:6` | **Verified**: "Uses xxh3 for fast, high-quality hashing" (`shard/consistent_hash.rs:6`). | High-throughput non-cryptographic hash algorithm (~10–15 GB/s), excellent distribution uniformity, resilient to hash collisions. | SipHash (`DefaultHasher`), MurmurHash3, CityHash, FNV-1a | **Pros**: Significantly faster than SipHash (standard library default); passes all SMHasher tests.<br>**Cons**: Not cryptographically secure (susceptible to HashDoS if attacker crafts keys deliberately). | Using standard `SipHash` would protect against HashDoS attacks, but would incur a 3–5x CPU overhead on every key hash calculation. |
| **`crc32fast` (`1.x`)** | Integrity Verification | `persistence/wal.rs:17`, `persistence/snapshot.rs:21` | **Verified**: "CRC32 to guarantee corruption detection" (`README.md:134`). | SIMD-accelerated CRC32 calculation for detecting bitrot and truncated disk writes in WAL records and snapshots. | Adler-32, Blake3, SHA-256 | **Pros**: Extremely fast (SIMD instructions: SSE4.2 / ARM CRC); 4-byte compact checksum overhead.<br>**Cons**: Detects only accidental corruption, not malicious tampering (not a MAC). | Cryptographic hashes like Blake3/SHA-256 would provide tamper resistance, but at higher CPU cost per appended WAL transaction. |
| **`deadpool` (`0.10`)** | Client Connection Pooling | `fluxcache-client/src/pool.rs:4` | **Interpretation**: Chosen to manage persistent TCP connections without writing a custom pool from scratch. | Async-first pool manager with lifecycle callbacks (`create`, `recycle` via `PING`). | `bb8`, `r2d2`, `mobc` | **Pros**: Well-maintained, minimal async overhead, built-in health checking.<br>**Cons**: Extra dependency; recycling connection on check-in incurs a network ping latency round-trip. | A custom lock-free ring-buffer pool would save dependency weight, but would require extensive testing to avoid socket leaks under connection churn. |
| **`hdrhistogram` (`7.5`)** | Benchmark Metrics | `fluxcache-bench/src/main.rs:3` | **Interpretation**: Chosen for accurate latency distribution measurements without percentile distortion. | High Dynamic Range Histogram captures latency across multiple orders of magnitude (microseconds to seconds) with bounded memory. | Standard percentiles (`Vec` sorting), rolling averages | **Pros**: Accurate percentiles (p50, p95, p99, p99.9) with configurable precision (3 sig figs).<br>**Cons**: Memory pre-allocation for bins. | Storing raw latency samples in a `Vec<u64>` and sorting would consume massive RAM for 100M operations and introduce GC/allocation jitter. |
| **`thiserror` & `anyhow`** | Error Handling | Across all crates | **Interpretation**: Standard Rust idiom for layered error handling. | `thiserror` derives `std::error::Error` for domain types (`CacheError`, `WalError`, `ParseError`); `anyhow` provides context for application entry points. | Custom error boilerplate, `Box<dyn Error>` | **Pros**: Type-safe domain error matching; clean propagation with `?`.<br>**Cons**: Two error crates in the dependency tree. | Manual error enum implementation would reduce dependencies, but adds boilerplate. |
| **`serde` & `serde_json`** | Configuration & Serialization | `config.rs`, `server.rs:568`, `server.rs:623` | **Interpretation**: Standard serialization framework for TOML configs and HTTP JSON admin endpoints. | Automatic derive macros for TOML/JSON structures; high performance. | `simd-json`, `miniserde`, manual formatting | **Pros**: Declarative, reliable, highly optimized.<br>**Cons**: Compile-time macro expansion overhead. | Manual string concatenation is already used in `server.rs:703` for HTTP responses; replacing `serde_json` with manual formatting would save compile time. |
| **`clap` (`4.x`, derive)** | CLI Parsing | `fluxcache/src/config.rs:65`, `fluxcache-bench/src/main.rs:10` | **Interpretation**: Standard CLI parser with declarative struct validation and auto-generated help menus. | Robust flag validation, environment variable integration, type conversion (`parse_memory_size`). | `argh`, `lexopt`, `pico-args` | **Pros**: Feature-rich, highly ergonomic derive API.<br>**Cons**: Substantial compile-time dependency overhead. | Minimal alternatives like `pico-args` would reduce compilation time by several seconds with minimal loss of functionality. |
| **`tracing` & `tracing-subscriber`** | Observability | `main.rs:16`, `server.rs`, `wal.rs` | **Interpretation**: Modern structured diagnostic framework replacing legacy `log` crate. | Structured key-value logging; asynchronous span tracking; JSON log formatting for cloud collectors. | `log` + `env_logger`, `slog` | **Pros**: Non-blocking async event tracing; JSON formatting support.<br>**Cons**: Non-trivial binary size and compilation overhead. | Standard `log` crate would be lighter, but lacks native structured JSON output and span contexts. |

---

## 3. Deep Dive: Unused Dependencies Identified in `Cargo.toml`

An audit of the dependencies declared in `fluxcache/Cargo.toml` revealed several declared dependencies that are **never imported or used anywhere in the codebase**:

```toml
# In fluxcache/Cargo.toml:
prometheus = "0.13"
hyper = { version = "1", features = ["server", "http1"] }
hyper-util = { version = "0.1", features = ["tokio", "server-auto", "http1"] }
http-body-util = "0.1"
chrono = { version = "0.4", features = ["serde"] }
uuid = { version = "1", features = ["v4"] }
```

### 3.1 Verification & Evidence

1. **`prometheus = "0.13"`**:
   - Grep verification across `fluxcache/src/` returns zero imports (`use prometheus`).
   - In `fluxcache/src/metrics.rs:120-231`, Prometheus exposition format is produced by manual string formatting:
     ```rust
     fn write_metric(out: &mut String, name: &str, metric_type: &str, help: &str, value: u64) {
         out.push_str(&format!("# HELP {name} {help}\n"));
         out.push_str(&format!("# TYPE {name} {metric_type}\n"));
         out.push_str(&format!("{name} {value}\n"));
     }
     ```
2. **`hyper`, `hyper-util`, `http-body-util`**:
   - Zero imports across all source files.
   - The HTTP admin plane (`/health`, `/metrics`, `/stats`) in `fluxcache/src/server.rs:590-711` is implemented manually over raw `tokio::net::TcpListener`, reading raw socket streams, parsing the first line with whitespace splits, and formatting raw HTTP/1.1 response strings.
3. **`chrono = "0.4"`**:
   - Zero imports. Time calculations rely entirely on `std::time::Instant` and `std::time::Duration`.
4. **`uuid = "1"`**:
   - Zero imports. No request tracing identifiers are generated or consumed.

### 3.2 Engineering Evaluation & Interview Defense
- **Why this happens in real-world projects**: Authors frequently declare dependencies anticipating future requirements (e.g., plans to build a full Hyper-based REST API or official Prometheus metrics registry), but end up implementing lightweight bespoke solutions to reduce runtime complexity.
- **Production Recommendation**: Remove these unused dependencies from `Cargo.toml`. Removing `hyper`, `prometheus`, `chrono`, and `uuid` eliminates over 40 transitive dependencies from the dependency tree, cutting compilation time and attack surface significantly.

---

## 4. Key Architectural Alternatives Considered

### 4.1 Storage Engine: Sharded `HashMap` vs. Lock-Free Map (`crossbeam-skiplist` / `dashmap`)
- **Current Choice**: Partitioned vector of `RwLock<CacheShard>`, where each shard contains a `HashMap<String, CacheEntry>`.
- **Alternative**: Concurrent lock-free hash map (`dashmap` or `crossbeam-skiplist::SkipMap`).
- **Why Current Design Was Chosen**:
  - Eviction algorithms (LRU doubly-linked list and LFU frequency buckets) require **atomic coordination between the key-value dictionary and the eviction order tracking structure**.
  - In a lock-free dictionary, inserting a key and updating an LRU list cannot be done atomically without a mutex.
  - Locking at the shard level allows the shard's `HashMap`, `LruPolicy`, `TtlTracker`, and `memory_used` counters to mutate under a single cohesive lock.

### 4.2 Persistence Architecture: WAL + Snapshots vs. Embedded LSM-Tree (RocksDB/Sled)
- **Current Choice**: Custom append-only WAL (`fluxcache.wal`) + periodic full atomic memory snapshot (`fluxcache.snap`).
- **Alternative**: Embedded Key-Value engine like `RocksDB` or `sled`.
- **Why Current Design Was Chosen**:
  - FluxCache is primarily an **in-memory** cache, not a durable database. All reads must be served directly from RAM without disk I/O.
  - LSM-trees introduce read amplification, SSTable compaction overhead, and cache pollution.
  - A custom append-only WAL provides sequential disk writes with minimal latency penalty on writes, while snapshots truncate WAL growth cleanly.

### 4.3 Distributed Placement: Client-Side Consistent Hashing vs. Cluster Gossip (Redis Cluster)
- **Current Choice**: Client-side consistent hashing using `ConsistentHashRing` (`xxh3_64` over virtual nodes).
- **Alternative**: Server-side proxy routing or cluster gossip protocol (like Redis Cluster hash slots).
- **Why Current Design Was Chosen**:
  - Extreme simplicity on the server side: server nodes remain completely stateless regarding peer nodes.
  - Eliminates inter-node network hops: clients route directly to the authoritative cache node.
  - Trade-off: Client reconfiguration is required when cluster topology changes; no automated slot migration or rebalancing.
