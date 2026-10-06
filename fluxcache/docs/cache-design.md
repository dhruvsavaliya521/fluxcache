# Cache Design

FluxCache uses a multi-layered approach to provide O(1) expected time complexity for operations while staying within strict memory limits.

## Memory Tracking
Instead of hooking into the global allocator, the cache tracks memory heuristically. Each `CacheEntry` knows its approximate size (`key.len() + value.len() + std::mem::size_of::<CacheEntry>()`). The shards maintain a running sum of these sizes. This is highly performant but cannot account for allocator fragmentation.

## Eviction
Eviction triggers synchronously during a `SET` operation if the new entry would exceed the shard's memory limit.

* **LRU (Least Recently Used)**: We use an arena-backed doubly linked list. Nodes are stored in a `Vec`, and references are indices. This avoids pointer chasing, improves cache locality, and completely sidesteps Rust's `unsafe` requirements for cyclic data structures.
* **LFU (Least Frequently Used)**: To prevent cache pollution by historically hot but currently cold items, we implemented LFU with decay. Frequencies are bucketed. Every `N` accesses across the cache, all tracked frequencies are halved (decay).

## Expiration (TTL)
* **Lazy Expiration**: Checked on every `GET`. If the current time > expiration time, it's purged.
* **Active Sweep**: A background tokio task wakes up periodically (e.g., every 1s) and asks the `TtlTracker` (which uses a `BTreeMap` indexed by expiration timestamp) for all keys that have expired up to `Instant::now()`. These are then removed from the main store.
