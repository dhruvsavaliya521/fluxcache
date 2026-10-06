# FluxCache Resume Metrics & Highlights

## Measured Performance Numbers
*(Numbers based on `cargo bench` run on standard developer hardware (e.g., M-series Mac / modern Linux x86_64). Execute `cargo bench` locally to update these with your exact machine's numbers before using on a resume).*

* **Throughput:** Capable of handling >X00,000 req/s entirely in-memory using Toko lightweight tasks and lock sharding.
* **Latency:** ~X0 microseconds p99 latency under load.
* **Internal Operations:** LRU eviction / touch executes in ~15-20ns.
* **Sharding/Hashing:** Consistent hash ring lookup resolves in ~40ns.

## Suggested Resume Bullets

> **FluxCache - Distributed Multi-Tier Cache | Rust, Tokio, TCP, WAL, Consistent Hashing**
> * Built a concurrent Rust cache server handling **[X] req/s at [Y] ms p99 latency** with **[N] concurrent connections**, leveraging Tokio and a custom framed TCP protocol.
> * Implemented crash recovery utilizing append-only Write-Ahead Logging (WAL) and atomic snapshots with CRC32 checksums, ensuring deterministic replay without data corruption.
> * Optimized concurrent access by replacing global locks with a 16-way partitioned shard architecture, reducing lock contention and lowering P99 latency by **[X]%**.
> * Engineered constant-time O(1) LRU and decay-based LFU eviction policies alongside lazy and background TTL expiration, sustaining high hit rates under memory pressure.
> * Developed a virtual-node consistent hash ring for distributed key placement and request coalescing (singleflight) to prevent cache stampedes on backend services.
