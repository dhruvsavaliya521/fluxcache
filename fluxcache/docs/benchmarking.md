# Benchmarking Methodology

All performance claims must be backed by reproducible measurements.

## Tools
FluxCache uses `criterion` for macro and micro-benchmarking.

## Running Benchmarks
```bash
cargo bench
```

## Workloads Tested
The benchmark suite covers:
* **LRU/LFU Tracking**: Measuring the isolated overhead of eviction structures.
* **Consistent Hashing**: Measuring the binary search cost over large rings.
* **Protocol Parsing**: Simulating incoming byte streams.
* **Async Store Operations**: Full `SET`/`GET` operations over the sharded `CacheStore` under a simulated Tokio runtime.

## Load Testing (External)
For end-to-end load testing, standard tools like `redis-benchmark` (if protocol matches) or custom TCP clients can be used.

### Setup Requirements for E2E
1. **Hardware**: Specify CPU (e.g., M1 Max, 10 cores), RAM (e.g., 32GB), OS (e.g., macOS 14 / Linux 6.x).
2. **Warmup**: Run 100,000 requests to fill the cache and stabilize memory.
3. **Concurrency**: Measure at 10, 100, and 1,000 concurrent clients.
4. **Metrics**: P50, P95, P99 latency, and Operations/sec.

*Always record the exact commit hash and environment when publishing numbers.*
