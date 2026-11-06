# 10 — Technical Interview Question Bank & Defense Guide

> **Target Interview**: Software Development Engineer (SDE) Internship / Junior Backend Engineer at HackerRank.  
> **Instructions for the Candidate**: Use this guide to answer technical questions with total fidelity to the actual implementation. Never claim a feature exists if the code proves it is incomplete. Demonstrating that you know the *exact limitations and trade-offs* of your code impresses senior interviewers far more than pretending a system is flawless.

---

## Category 1: Project Overview & Core Understanding

### Q1.1: Can you explain what FluxCache is and why you built it from scratch rather than wrapping Redis?

#### 1. What the Interviewer Is Testing
- Ability to explain a project's purpose concisely.
- Understanding of low-level systems programming tradeoffs vs using off-the-shelf components.
- Architectural vision.

#### 2. Important Concepts Required
- In-memory key-value caching, thread-per-core vs work-stealing concurrency, lock-striping vs single-threaded event loop, systems programming in Rust.

#### 3. Suggested Answer Structure
1. **Define the project**: "FluxCache is an in-memory, sharded key-value cache server built from scratch in Rust, designed for high-concurrency read/write workloads with sub-millisecond p99 latency."
2. **Explain the architectural motivation**: "Redis operates predominantly on a single-threaded event loop for command execution. While this avoids concurrency bugs, it means a single Redis instance cannot utilize multiple CPU cores for data processing without clustering. I built FluxCache to explore fine-grained lock striping across 16 independent shards on a multi-threaded Tokio runtime, allowing concurrent operations to execute in parallel across CPU cores."
3. **Highlight native features**: "It features an intrusive arena-allocated LRU list, decaying LFU, dual-mode TTL expiration, Write-Ahead Logging (WAL) with CRC32 checksums, and client-side consistent hashing."

#### 4. Relevant Source Locations
- `fluxcache/src/server.rs:27-46` (`FluxServer::new`)
- `fluxcache/src/cache/store.rs:78-119` (`CacheStore::new`)
- `README.md:1-25`

#### 5. Likely Follow-Up Questions
- *Follow-up*: "If Redis is single-threaded, how does it achieve 100k+ ops/sec?"  
  *Answer*: Redis avoids lock contention, context switches, and cache bouncing entirely. Its operations are pure in-memory pointer manipulations without kernel transitions. FluxCache trades away single-threaded simplicity for multi-core scaling, introducing shard synchronization overhead.

---

## Category 2: Technology Choices & Trade-Offs

### Q2.1: Why did you choose `xxh3` over standard SipHash for internal sharding? What are the security and performance implications?

#### 1. What the Interviewer Is Testing
- Knowledge of hashing algorithms, non-cryptographic vs cryptographic hashing, HashDoS vulnerabilities, and CPU performance trade-offs.

#### 2. Important Concepts Required
- `xxhash` / `xxh3`, `SipHash-1-3` / `SipHash-2-4`, SMHasher test suite, HashDoS (Denial of Service via collision attacks), SIMD vectorization.

#### 3. Suggested Answer Structure
1. **State the decision**: "I used `xxh3_64` from the `xxhash-rust` crate for internal shard mapping and consistent hash ring routing."
2. **Performance benefit**: "Rust's standard `HashMap` uses `SipHash-1-3` by default to defend against HashDoS attacks. However, SipHash is relatively slow (~1–2 GB/s). `xxh3` utilizes modern vector SIMD instructions (AVX2/SSE) to achieve throughput upwards of 10–15 GB/s with near-zero latency, which is critical when every single cache request must hash keys twice (once on the client and once on the server)."
3. **Acknowledge the trade-off & security risk**: "`xxh3` is non-cryptographic. If untrusted external users can generate arbitrary keys, an attacker could theoretically craft keys that collide on the same shard or hash ring bucket (HashDoS). In an internal microservice environment, this risk is acceptable; for public-facing multi-tenant caches, SipHash with a secret per-node seed would be safer."

#### 4. Relevant Source Locations
- `fluxcache/src/cache/store.rs:26, 122-124` (`xxh3_64` in `shard_index`)
- `fluxcache/src/shard/consistent_hash.rs:10, 54, 76`

#### 5. Likely Follow-Up Questions
- *Follow-up*: "Why does your client library use a different virtual-node key format than the server?"  
  *Answer*: "That is an inconsistency in the current codebase. The server formats virtual nodes as `"{node_id}:vn{i}"`, while `ClusterClient` formats them as `"{node}-{v}"`. While both use `xxh3_64`, in a production refactor they should share the exact same `ConsistentHashRing` implementation from a shared core crate."

---

## Category 3: High-Level Architecture

### Q3.1: Walk me through the complete lifecycle of a `SET key value EX 60` request from the network socket down to disk.

#### 1. What the Interviewer Is Testing
- Systems-level end-to-end understanding: network framing, command parsing, durability write paths, locking, eviction, and wire encoding.

#### 2. Important Concepts Required
- TCP framing, zero-copy buffers (`BytesMut`), Write-Ahead Logging, lock striping, arena allocation, TTL indexing, RESP protocol.

#### 3. Suggested Answer Structure
1. **Network & Parsing**: "The client connection task reads bytes into a `BytesMut` buffer via `read_buf`. `extract_frame` parses the frame. `parse_command` parses the verb `SET`, key, binary payload, and TTL duration into `Command::Set`."
2. **Write-Ahead Log First**: "If persistence is active, `execute_command` acquires the global `wal.lock().await`. It appends a 30-byte header, the key, value, and a CRC32 checksum to `BufWriter`, ensuring durability before mutating memory."
3. **Shard Routing & Locking**: "The server calculates `shard_idx = xxh3_64(key) % num_shards` and acquires an exclusive write lock on `shards[shard_idx]`."
4. **Eviction Check**: "It checks if `memory_used + entry_size > max_memory_per_shard`. If over capacity, it repeatedly evicts the least recently used key from `shard.lru` until memory fits."
5. **Memory Mutation**: "It registers the expiration in `shard.ttl`, promotes the key in `shard.lru`, inserts the `CacheEntry` into `shard.data`, and increments `total_memory`."
6. **Response**: "It returns `Response::Ok("OK")`, encoded as `+OK\r\n` back to the TCP socket."

#### 4. Relevant Source Locations
- `fluxcache/src/server.rs:423-483` (`handle_connection`)
- `fluxcache/src/server.rs:504-528` (`execute_command` SET arm)
- `fluxcache/src/persistence/wal.rs:131-142` (`append_set`)
- `fluxcache/src/cache/store.rs:168-225` (`CacheStore::set`)

---

## Category 4: Data Structures & Storage Engine

### Q4.1: Why did you implement an arena-allocated doubly-linked list for LRU instead of using `std::collections::LinkedList` or raw pointers?

#### 1. What the Interviewer Is Testing
- Deep understanding of data structures, cache locality, pointer chasing, memory safety, and idiomatic Rust design.

#### 2. Important Concepts Required
- Intrusive vs non-intrusive linked lists, cache lines, pointer chasing, borrow checker rules regarding cyclic graphs, arena allocators, free lists.

#### 3. Suggested Answer Structure
1. **Problems with `std::collections::LinkedList`**: "`std::collections::LinkedList` is non-intrusive. It allocates each node on the heap independently, causing pointer chasing and cache misses. Furthermore, it does not expose internal node pointers or cursors, making $O(1)$ node removal/promotion from the middle of the list impossible without $O(N)$ traversal."
2. **Problems with raw pointers (`*mut Node`)**: "Using raw pointers requires `unsafe` blocks, manual drop handling, and careful management of aliasing to avoid use-after-free or data races."
3. **The Arena Solution**: "I used an arena vector (`nodes: Vec<LruNode>`) where pointers are simple `Option<usize>` indices. A companion `HashMap<String, usize>` maps keys directly to node indices in $O(1)$."
4. **Handling Deletions**: "When a node is evicted, its index is pushed onto a `free_list: Vec<usize>`. Future insertions pop from `free_list` rather than growing the vector. This achieves $O(1)$ touch, promotion, and eviction with 100% safe Rust and excellent memory locality."

#### 4. Relevant Source Locations
- `fluxcache/src/cache/lru.rs:17-43` (`LruNode`, `LruPolicy`)
- `fluxcache/src/cache/lru.rs:106-125` (`alloc_node` and free list reuse)
- `fluxcache/src/cache/lru.rs:128-162` (`detach` and `push_front`)

#### 5. Likely Follow-Up Questions
- *Follow-up*: "Does the `nodes` vector ever shrink?"  
  *Answer*: "No. In the current implementation, `nodes.capacity()` stays at the high-water mark. If the cache stores 1,000,000 keys and evicts 900,000 of them, the slots reside in `free_list`, but the vector memory is not deallocated."

---

## Category 5: Low-Level Design & Concurrency

### Q5.1: In `CacheStore`, why does `get()` acquire an exclusive `write()` lock on the shard instead of a `read()` lock? How does this impact performance?

#### 1. What the Interviewer Is Testing
- Concurrency expertise, ability to critique your own codebase, understanding read/write lock contention.

#### 2. Important Concepts Required
- `tokio::sync::RwLock`, interior mutability, read-write lock starvation, cache metadata mutations on read (LRU touch, LFU touch, lazy expiration).

#### 3. Suggested Answer Structure
1. **Acknowledge the implementation reality**: "In `fluxcache/src/cache/store.rs:130`, `store.get()` acquires `self.shards[shard_idx].write().await`. It does not use a read lock."
2. **Explain the mechanical reason**:
   - "A cache GET is not read-only: it must check if the entry has expired and execute lazy deletion if so."
   - "Even on a hit, it must update `entry.touch()` (last access time and frequency counter) and mutate the eviction tracker (`shard.lru.touch(key)`), which reorganizes linked list pointers."
   - "Because `CacheShard` bundles the data map and the eviction tracker together, mutating these requires a write lock on the entire shard."
3. **Explain the architectural consequence**: "This means multiple readers querying keys on the same shard cannot run concurrently; they serialize behind the write lock. The `RwLock` only provides concurrency across different shards, or for administrative methods like `exists()` and `stats()`."
4. **Propose the production improvement**: "To achieve true concurrent reads, we can decouple eviction tracking using channel-buffered read events or atomic clocks (similar to Java's Caffeine or Rust's Moka), allowing `get()` to execute under a shared `read()` lock while asynchronously queueing touch events."

#### 4. Relevant Source Locations
- `fluxcache/src/cache/store.rs:128-165` (`CacheStore::get`)
- `fluxcache/src/cache/entry.rs:57-60` (`touch`)
- `fluxcache/src/cache/lru.rs:58-69` (`touch`)

---

## Category 6: Asynchronous Programming & Tokio

### Q6.1: Are there any blocking operations inside your asynchronous Tokio tasks? What are the dangers, and how would you fix them?

#### 1. What the Interviewer Is Testing
- Knowledge of cooperative multitasking in async runtimes, worker thread starvation, blocking syscalls.

#### 2. Important Concepts Required
- Tokio work-stealing thread pool, blocking OS syscalls (`fsync`, `write`, `rename`), `tokio::task::spawn_blocking`, `tokio::fs`.

#### 3. Suggested Answer Structure
1. **Identify the exact issue**: "Yes. In `persistence/wal.rs:175` (`sync_data()`) and `persistence/snapshot.rs:108-115` (`File::create`, `sync_all()`, `std::fs::rename`), standard synchronous library file operations are called directly inside async functions."
2. **Explain the danger**: "Tokio uses a fixed-size thread pool (typically equal to the number of physical CPU cores). When an async task calls a blocking filesystem syscall like `sync_data()`, the OS suspends that worker thread waiting on disk I/O. Because it is a synchronous call, the thread cannot switch to other ready async tasks. If multiple tasks call fsync simultaneously, all Tokio worker threads can freeze, stalling network packet processing."
3. **Provide the fix**: "I would wrap all synchronous file operations in `tokio::task::spawn_blocking(move || { ... })`, which offloads the blocking work to Tokio's dedicated blocking thread pool (which dynamically scales up to 512 threads), leaving the primary worker threads free to handle non-blocking network I/O."

#### 4. Relevant Source Locations
- `fluxcache/src/persistence/wal.rs:173-177` (`WalWriter::sync`)
- `fluxcache/src/persistence/snapshot.rs:102-118` (`write_snapshot`)
- `fluxcache/src/server.rs:346-348` (WAL sync task loop)

---

## Category 7: Persistence & Crash Recovery

### Q7.1: How does FluxCache guarantee recovery after a sudden power loss? How do you prevent data corruption during recovery?

#### 1. What the Interviewer Is Testing
- Crash consistency, write-ahead logging principles, checksum verification, atomic rename patterns.

#### 2. Important Concepts Required
- WAL replay, point-in-time snapshots, sequence numbers, CRC32, truncated writes, POSIX `rename(2)`.

#### 3. Suggested Answer Structure
1. **The Invariants**: "Durability is achieved using a two-tier model: atomic snapshots establish a baseline state, and sequential WAL records record all subsequent mutations."
2. **Snapshot Atomicity**: "Snapshots are written to a temporary `.snap.tmp` file, flushed with `sync_all()`, and renamed via `std::fs::rename`. Because `rename` is an atomic filesystem operation on POSIX, a crash during snapshot creation never corrupts the existing snapshot."
3. **Recovery Sequence**:
   - "On boot, the server loads `fluxcache.snap`, validates its CRC32 checksum, and loads all entries into memory, setting `wal_start_seq` to the snapshot's sequence number."
   - "Next, it opens `fluxcache.wal` using `WalReader::read_all`."
   - "It iterates through records, discarding any record where `sequence <= wal_start_seq` to prevent duplicate writes."
4. **Handling Partial Writes**: "If power failed mid-write, the last WAL record may be truncated. `WalReader` verifies the CRC32 checksum on every record. If it encounters a checksum mismatch or unexpected EOF, it stops recovery at the last known good record and recovers everything prior without panicking."

#### 4. Relevant Source Locations
- `fluxcache/src/server.rs:56-128` (Recovery boot logic)
- `fluxcache/src/persistence/wal.rs:244-284` (`WalReader::read_all`)
- `fluxcache/src/persistence/snapshot.rs:102-118` (`write_snapshot`)

---

## Category 8: Distributed Systems & Replication

### Q8.1: Can you describe your replication protocol? What happens when a replica falls behind?

#### 1. What the Interviewer Is Testing
- Understanding of distributed state replication, replication lag, buffer management, backpressure.

#### 2. Important Concepts Required
- Asynchronous replication, `PSYNC`, full resync vs delta sync, bounded broadcast channels, lag disconnect.

#### 3. Suggested Answer Structure
1. **Handshake & Full Sync**: "When a replica starts (`--replica-of`), it connects to the primary's replication port (:6381) and sends `PSYNC <seq>`. The primary generates an in-memory snapshot, prefixes it with `+FULLRESYNC <len>`, and streams the data. The replica wipes its store and loads this snapshot as its baseline."
2. **Continuous Streaming**: "The primary connects the replica to a `tokio::sync::broadcast` channel. Whenever a mutating command writes to the WAL, the raw record bytes are broadcast to all connected replicas."
3. **Handling Lagged Replicas**: "The broadcast channel has a bounded capacity (1,024 records). If a replica experiences network congestion or high load, it falls behind. When the channel buffer overflows, Tokio returns `RecvError::Lagged(count)`. The primary explicitly disconnects the lagged replica. Upon reconnecting, the replica triggers a fresh `+FULLRESYNC`, ensuring it never processes partial or corrupted sequences."
4. **Be Honest About Limitations**: "Currently, the primary streams live WAL bytes, but the replica's receive loop (`replication/client.rs:108`) drops these bytes without applying them to its store. Full snapshot sync works, but continuous live mutation sync is partially implemented."

#### 4. Relevant Source Locations
- `fluxcache/src/replication/server.rs:72-131` (`handle_replica`)
- `fluxcache/src/replication/client.rs:51-111` (`sync`)

---

## Category 9: Scalability & Performance Engineering

### Q9.1: How does client-side consistent hashing work in your client library? How does it compare to Redis Cluster's 16,384 hash slots?

#### 1. What the Interviewer Is Testing
- Horizontal scaling, consistent hashing algorithms, client-side routing vs server-side routing, cluster membership.

#### 2. Important Concepts Required
- Consistent hash ring, virtual nodes (vnodes), binary search, Redis hash slots, key migration, minimal redistribution.

#### 3. Suggested Answer Structure
1. **Explain the Ring Architecture**: "In `fluxcache-client/src/cluster.rs`, `ClusterClient` maintains a `BTreeMap<u64, String>` representing a hash ring. Each physical node is hashed 150 times using `xxh3_64` with virtual node keys (`{node}-{v}`)."
2. **Key Routing**: "When an application issues `client.get(key)`, the key is hashed with `xxh3_64`. It performs a range search (`ring.range(hash..).next()`) to find the nearest clockwise virtual node in $O(\log(V \cdot N))$ time. It then checks out a connection from that specific node's `deadpool` connection pool."
3. **Minimal Key Movement**: "When a node is added or removed, only $\sim 1/N$ of keys are re-routed to new nodes, preventing a massive cache wipeout compared to modulo hashing (`hash % N`)."
4. **Comparison with Redis Cluster**:
   - "Redis Cluster uses 16,384 deterministic hash slots managed on the server side via a gossip protocol. If a client queries the wrong node, the server returns a `-MOVED <slot> <ip>` redirect."
   - "FluxCache uses client-side routing: the servers are completely oblivious to the cluster topology. This eliminates cluster gossip network traffic and redirects, but requires client reconfiguration when cluster topology changes."

#### 4. Relevant Source Locations
- `fluxcache-client/src/cluster.rs:17-35` (`ClusterClient::new`)
- `fluxcache-client/src/cluster.rs:58-70` (`get_node`)
- `fluxcache/src/shard/consistent_hash.rs:44-91` (Server-side ring implementation)

---

## Category 10: Debugging, Code Review & Hard Questions

### Q10.1: If you had another two weeks to work on this project before pushing to production, what are the top 3 architectural bugs or flaws you would fix first?

#### 1. What the Interviewer Is Testing
- Maturity, critical thinking, prioritization, self-awareness, engineering judgment.

#### 2. Important Concepts Required
- Triaging critical bugs vs performance optimizations vs code hygiene.

#### 3. Suggested Answer Structure
1. **Priority 1: Complete the Replica WAL Replay Loop (`replication/client.rs`)**:
   - "Currently, the replication client receives live streamed WAL bytes from the primary but drops them without parsing or updating the cache. I would implement the WAL record parser in `replication/client.rs` to decode mutations and apply them to `self.store`, achieving true live master-slave replication."
2. **Priority 2: Fix the Broken Test Compilation & Active Test Server (`fluxcache-client`)**:
   - "I would update `fluxcache-client/tests/integration.rs` to include the missing `Config` struct fields so that `cargo test --workspace` compiles cleanly in CI. I would also hook up `start_test_server` in `fluxcache/tests/integration_test.rs` so integration tests validate real TCP streams."
3. **Priority 3: Offload Blocking Disk Syscalls to `spawn_blocking`**:
   - "I would wrap `wal.sync()` and snapshot generation in `tokio::task::spawn_blocking` so that blocking disk I/O cannot stall the Tokio async worker threads handling live client traffic."

#### 4. Relevant Source Locations
- `fluxcache/src/replication/client.rs:102-110`
- `fluxcache-client/tests/integration.rs:19-35`
- `fluxcache/src/persistence/wal.rs:173-177`
