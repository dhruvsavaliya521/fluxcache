# Sharding & Consistent Hashing

FluxCache employs two types of sharding: **Internal Sharding** and **Distributed Sharding**.

## Internal Sharding (Lock Striping)
To prevent thread contention on a single global `RwLock`, the core `CacheStore` is partitioned into N independent shards (default 16). 
When a key arrives, its `xxhash3_64` modulo N determines which shard it belongs to. Only that specific shard's lock is acquired. This scales almost linearly with the number of CPU cores.

## Distributed Sharding (Consistent Hashing)
For scaling horizontally across multiple physical machines, FluxCache includes a `ConsistentHashRing`.

* **Virtual Nodes**: To ensure uniform distribution of keys across nodes, each physical node is represented by 150 "virtual nodes" on the ring.
* **Placement**: Keys are hashed and placed on the ring. The system finds the next node on the ring clockwise using binary search.
* **Stability**: Adding or removing a node only requires migrating `1/N` of the keys (where N is the number of nodes), minimizing cache stampedes compared to modulo hashing (`hash % nodes`).
