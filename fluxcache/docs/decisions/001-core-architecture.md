# Architecture Decision Record: 001 - Core Architecture

## Status
Accepted

## Context
We need to build a high-performance in-memory cache server in Rust. It must support high concurrency, TTL expiration, and strict memory bounds with eviction policies (LRU/LFU).

## Decision
1. **Runtime:** We will use `tokio` for the async networking layer to handle thousands of concurrent TCP connections efficiently.
2. **Locking & Sharding:** A single `HashMap` protected by an `RwLock` will bottleneck under heavy write loads. We will partition the data store into `N` shards (default 16). Each shard will have its own `HashMap`, `LRU/LFU` tracker, and `RwLock`.
3. **Eviction Structures:** 
   - LRU: An arena-allocated doubly-linked list. Avoiding `std::collections::LinkedList` and `unsafe` code while maintaining cache locality.
   - LFU: A frequency bucketing system (`BTreeMap<u64, HashSet>`) combined with periodic halving (decay) to prevent immortal hot keys.
4. **Memory Tracking:** We will track memory heuristically by summing string lengths and struct sizes rather than intercepting the global allocator, balancing accuracy with performance.

## Consequences
- **Positive:** High concurrent throughput; no `unsafe` code; graceful memory management.
- **Negative:** Memory tracking is an approximation; shard sizing might be imbalanced if keys hash poorly (though `xxhash` mitigates this).
