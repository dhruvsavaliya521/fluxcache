// FluxCache - Cache Store (Sharded)
//
// The core key-value store with:
// - Thread-safe concurrent access via internal sharding
// - Configurable LRU/LFU eviction
// - TTL expiration (lazy + active)
// - Memory tracking
//
// Design: The store is partitioned into N shards, each protected by its own
// RwLock. This reduces contention compared to a single global lock.
// Keys are assigned to shards via a hash of the key.

use crate::cache::entry::CacheEntry;
use crate::cache::lfu::LfuPolicy;
use crate::cache::lru::LruPolicy;
use crate::cache::ttl::TtlTracker;
use crate::config::EvictionPolicy;
use crate::metrics::CacheMetrics;

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use xxhash_rust::xxh3::xxh3_64;

/// Result type for cache operations.
pub type CacheResult<T> = Result<T, CacheError>;

/// Errors that can occur during cache operations.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("Key not found")]
    NotFound,

    #[error("Key has expired")]
    Expired,

    #[error("Value too large: {size} bytes exceeds maximum {max} bytes")]
    ValueTooLarge { size: usize, max: usize },

    #[error("Cache is full and eviction failed")]
    EvictionFailed,

    #[error("Internal error: {0}")]
    Internal(String),
}

/// A single shard of the cache.
#[derive(Debug)]
struct CacheShard {
    /// The actual key-value data.
    data: HashMap<String, CacheEntry>,
    /// LRU eviction tracker (used when policy is LRU).
    lru: LruPolicy,
    /// LFU eviction tracker (used when policy is LFU).
    lfu: LfuPolicy,
    /// TTL tracker for this shard.
    ttl: TtlTracker,
    /// Current memory usage of this shard in bytes.
    memory_used: usize,
}

impl CacheShard {
    fn new() -> Self {
        CacheShard {
            data: HashMap::new(),
            lru: LruPolicy::new(),
            lfu: LfuPolicy::new(10_000),
            ttl: TtlTracker::new(),
            memory_used: 0,
        }
    }
}

/// The sharded cache store.
pub struct CacheStore {
    /// The shards, each independently locked.
    shards: Vec<RwLock<CacheShard>>,
    /// Number of shards.
    num_shards: usize,
    /// Maximum memory per shard in bytes.
    max_memory_per_shard: usize,
    /// Total maximum memory in bytes.
    max_memory: usize,
    /// Eviction policy.
    eviction_policy: EvictionPolicy,
    /// Global memory counter (approximate).
    total_memory: AtomicUsize,
    /// Metrics collector.
    metrics: Arc<CacheMetrics>,
}

impl CacheStore {
    /// Create a new cache store with the given configuration.
    pub fn new(
        num_shards: usize,
        max_memory: usize,
        eviction_policy: EvictionPolicy,
        metrics: Arc<CacheMetrics>,
    ) -> Self {
        let num_shards = num_shards.max(1);
        let max_memory_per_shard = max_memory / num_shards;

        let shards = (0..num_shards)
            .map(|_| RwLock::new(CacheShard::new()))
            .collect();

        CacheStore {
            shards,
            num_shards,
            max_memory_per_shard,
            max_memory,
            eviction_policy,
            total_memory: AtomicUsize::new(0),
            metrics,
        }
    }

    /// Determine which shard a key belongs to.
    fn shard_index(&self, key: &str) -> usize {
        (xxh3_64(key.as_bytes()) as usize) % self.num_shards
    }

    /// Get a value from the cache.
    /// Implements lazy expiration: if the entry is expired, it is removed and NotFound is returned.
    pub async fn get(&self, key: &str) -> CacheResult<Bytes> {
        let shard_idx = self.shard_index(key);
        let mut shard = self.shards[shard_idx].write().await;

        let value = {
            if let Some(entry) = shard.data.get_mut(key) {
                if entry.is_expired() {
                    // Lazy expiration
                    let size = entry.size;
                    shard.data.remove(key);
                    shard.lru.remove_key(key);
                    shard.lfu.remove_key(key);
                    shard.ttl.remove_expiry(key);
                    shard.memory_used = shard.memory_used.saturating_sub(size);
                    self.total_memory.fetch_sub(size, Ordering::Relaxed);
                    self.metrics.record_expiration();
                    self.metrics.record_miss();
                    return Err(CacheError::Expired);
                }

                entry.touch();
                entry.value.clone()
            } else {
                self.metrics.record_miss();
                return Err(CacheError::NotFound);
            }
        };

        // Update eviction tracker
        match self.eviction_policy {
            EvictionPolicy::Lru => shard.lru.touch(key),
            EvictionPolicy::Lfu => shard.lfu.touch(key),
            EvictionPolicy::None => {}
        }

        self.metrics.record_hit();
        Ok(value)
    }

    /// Set a key-value pair in the cache with an optional TTL.
    pub async fn set(&self, key: String, value: Bytes, ttl: Option<Duration>) -> CacheResult<()> {
        let entry = CacheEntry::new(value, ttl);
        let entry_size = entry.size + key.len();

        // Reject if single entry exceeds shard limit
        if entry_size > self.max_memory_per_shard {
            return Err(CacheError::ValueTooLarge {
                size: entry_size,
                max: self.max_memory_per_shard,
            });
        }

        let shard_idx = self.shard_index(&key);
        let mut shard = self.shards[shard_idx].write().await;

        // Remove old entry if exists
        if let Some(old_entry) = shard.data.remove(&key) {
            shard.memory_used = shard.memory_used.saturating_sub(old_entry.size + key.len());
            self.total_memory
                .fetch_sub(old_entry.size + key.len(), Ordering::Relaxed);
            shard.lru.remove_key(&key);
            shard.lfu.remove_key(&key);
            shard.ttl.remove_expiry(&key);
        }

        // Evict entries if needed
        while shard.memory_used + entry_size > self.max_memory_per_shard {
            let evicted = self.evict_one(&mut shard);
            if !evicted {
                // If we can't evict anything and we're over limit, fail
                if shard.memory_used + entry_size > self.max_memory_per_shard
                    && !shard.data.is_empty()
                {
                    return Err(CacheError::EvictionFailed);
                }
                break;
            }
        }

        // Register TTL if set
        if let Some(exp) = entry.expires_at {
            shard.ttl.set_expiry(key.clone(), exp);
        }

        // Update eviction tracker
        match self.eviction_policy {
            EvictionPolicy::Lru => shard.lru.touch(&key),
            EvictionPolicy::Lfu => shard.lfu.touch(&key),
            EvictionPolicy::None => {}
        }

        shard.memory_used += entry_size;
        self.total_memory.fetch_add(entry_size, Ordering::Relaxed);
        shard.data.insert(key, entry);

        self.metrics.record_set();
        Ok(())
    }

    /// Delete a key from the cache. Returns true if the key existed.
    pub async fn delete(&self, key: &str) -> bool {
        let shard_idx = self.shard_index(key);
        let mut shard = self.shards[shard_idx].write().await;

        if let Some(entry) = shard.data.remove(key) {
            let size = entry.size + key.len();
            shard.memory_used = shard.memory_used.saturating_sub(size);
            self.total_memory.fetch_sub(size, Ordering::Relaxed);
            shard.lru.remove_key(key);
            shard.lfu.remove_key(key);
            shard.ttl.remove_expiry(key);
            self.metrics.record_delete();
            true
        } else {
            false
        }
    }

    /// Check if a key exists (and is not expired).
    pub async fn exists(&self, key: &str) -> bool {
        let shard_idx = self.shard_index(key);
        let shard = self.shards[shard_idx].read().await;

        if let Some(entry) = shard.data.get(key) {
            !entry.is_expired()
        } else {
            false
        }
    }

    /// Get the remaining TTL for a key in seconds.
    /// Returns None if the key doesn't exist, Some(None) if no TTL is set,
    /// Some(Some(seconds)) if a TTL is set.
    pub async fn ttl(&self, key: &str) -> Option<Option<u64>> {
        let shard_idx = self.shard_index(key);
        let shard = self.shards[shard_idx].read().await;

        shard.data.get(key).map(|entry| {
            if entry.is_expired() {
                None
            } else {
                entry.remaining_ttl()
            }
        })
    }

    /// Set the TTL on an existing key. Returns true if the key exists.
    pub async fn expire(&self, key: &str, seconds: u64) -> bool {
        let shard_idx = self.shard_index(key);
        let mut shard = self.shards[shard_idx].write().await;

        if let Some(entry) = shard.data.get_mut(key) {
            if entry.is_expired() {
                return false;
            }
            let ttl = Duration::from_secs(seconds);
            entry.set_ttl(ttl);
            shard.ttl.set_expiry(key.to_string(), Instant::now() + ttl);
            true
        } else {
            false
        }
    }

    /// Run background expiration scan across all shards.
    /// Returns the total number of expired entries removed.
    pub async fn expire_scan(&self, max_per_shard: usize) -> usize {
        let now = Instant::now();
        let mut total_expired = 0;

        for shard_lock in &self.shards {
            let mut shard = shard_lock.write().await;
            let expired_keys = shard.ttl.collect_expired(now, max_per_shard);

            for key in &expired_keys {
                if let Some(entry) = shard.data.remove(key) {
                    let size = entry.size + key.len();
                    shard.memory_used = shard.memory_used.saturating_sub(size);
                    self.total_memory.fetch_sub(size, Ordering::Relaxed);
                    shard.lru.remove_key(key);
                    shard.lfu.remove_key(key);
                    self.metrics.record_expiration();
                }
            }

            total_expired += expired_keys.len();
        }

        total_expired
    }

    /// Flush all entries from the cache.
    pub async fn flush(&self) {
        for shard_lock in &self.shards {
            let mut shard = shard_lock.write().await;
            shard.data.clear();
            shard.lru = LruPolicy::new();
            shard.lfu = LfuPolicy::new(10_000);
            shard.ttl = TtlTracker::new();
            shard.memory_used = 0;
        }
        self.total_memory.store(0, Ordering::Relaxed);
    }

    /// Get cache statistics.
    pub async fn stats(&self) -> CacheStats {
        let mut total_entries = 0;
        let mut total_memory = 0;

        for shard_lock in &self.shards {
            let shard = shard_lock.read().await;
            total_entries += shard.data.len();
            total_memory += shard.memory_used;
        }

        CacheStats {
            total_entries,
            total_memory,
            max_memory: self.max_memory,
            num_shards: self.num_shards,
            eviction_policy: self.eviction_policy,
        }
    }

    /// Get all key-value pairs for snapshotting.
    /// Returns Vec of (key, value, optional_ttl_remaining_secs).
    pub async fn snapshot_data(&self) -> Vec<(String, Bytes, Option<u64>)> {
        let mut data = Vec::new();
        for shard_lock in &self.shards {
            let shard = shard_lock.read().await;
            for (key, entry) in &shard.data {
                if !entry.is_expired() {
                    data.push((key.clone(), entry.value.clone(), entry.remaining_ttl()));
                }
            }
        }
        data
    }

    /// Get total memory usage.
    pub fn memory_used(&self) -> usize {
        self.total_memory.load(Ordering::Relaxed)
    }

    /// Get total number of entries across all shards.
    pub async fn entry_count(&self) -> usize {
        let mut count = 0;
        for shard_lock in &self.shards {
            let shard = shard_lock.read().await;
            count += shard.data.len();
        }
        count
    }

    /// Evict one entry from a shard. Returns true if an entry was evicted.
    fn evict_one(&self, shard: &mut CacheShard) -> bool {
        let victim_key = match self.eviction_policy {
            EvictionPolicy::Lru => shard.lru.evict(),
            EvictionPolicy::Lfu => shard.lfu.evict(),
            EvictionPolicy::None => return false,
        };

        if let Some(key) = victim_key {
            if let Some(entry) = shard.data.remove(&key) {
                let size = entry.size + key.len();
                shard.memory_used = shard.memory_used.saturating_sub(size);
                self.total_memory.fetch_sub(size, Ordering::Relaxed);
                shard.ttl.remove_expiry(&key);
                // Clean up the other tracker
                match self.eviction_policy {
                    EvictionPolicy::Lru => shard.lfu.remove_key(&key),
                    EvictionPolicy::Lfu => shard.lru.remove_key(&key),
                    EvictionPolicy::None => {}
                }
                self.metrics.record_eviction();
                return true;
            }
        }

        false
    }
}

/// Cache statistics snapshot.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CacheStats {
    pub total_entries: usize,
    pub total_memory: usize,
    pub max_memory: usize,
    pub num_shards: usize,
    pub eviction_policy: EvictionPolicy,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_metrics() -> Arc<CacheMetrics> {
        Arc::new(CacheMetrics::new())
    }

    #[tokio::test]
    async fn test_basic_set_get() {
        let store = CacheStore::new(4, 1024 * 1024, EvictionPolicy::Lru, test_metrics());
        store
            .set("key1".to_string(), Bytes::from("value1"), None)
            .await
            .unwrap();

        let val = store.get("key1").await.unwrap();
        assert_eq!(val, Bytes::from("value1"));
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let store = CacheStore::new(4, 1024 * 1024, EvictionPolicy::Lru, test_metrics());
        let result = store.get("nonexistent").await;
        assert!(matches!(result, Err(CacheError::NotFound)));
    }

    #[tokio::test]
    async fn test_delete() {
        let store = CacheStore::new(4, 1024 * 1024, EvictionPolicy::Lru, test_metrics());
        store
            .set("key1".to_string(), Bytes::from("value1"), None)
            .await
            .unwrap();

        assert!(store.delete("key1").await);
        assert!(!store.delete("key1").await);
        assert!(matches!(store.get("key1").await, Err(CacheError::NotFound)));
    }

    #[tokio::test]
    async fn test_exists() {
        let store = CacheStore::new(4, 1024 * 1024, EvictionPolicy::Lru, test_metrics());
        assert!(!store.exists("key1").await);

        store
            .set("key1".to_string(), Bytes::from("value1"), None)
            .await
            .unwrap();
        assert!(store.exists("key1").await);
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let store = CacheStore::new(4, 1024 * 1024, EvictionPolicy::Lru, test_metrics());
        store
            .set(
                "key1".to_string(),
                Bytes::from("value1"),
                Some(Duration::from_millis(50)),
            )
            .await
            .unwrap();

        // Should exist immediately
        assert!(store.exists("key1").await);

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should be gone (lazy expiration on GET)
        assert!(matches!(store.get("key1").await, Err(CacheError::Expired)));
    }

    #[tokio::test]
    async fn test_expire_command() {
        let store = CacheStore::new(4, 1024 * 1024, EvictionPolicy::Lru, test_metrics());
        store
            .set("key1".to_string(), Bytes::from("value1"), None)
            .await
            .unwrap();

        assert!(store.expire("key1", 60).await);
        let ttl = store.ttl("key1").await;
        assert!(ttl.is_some());
    }

    #[tokio::test]
    async fn test_lru_eviction() {
        // Very small cache to force eviction
        let store = CacheStore::new(1, 512, EvictionPolicy::Lru, test_metrics());

        // Fill the cache
        store
            .set("key1".to_string(), Bytes::from("a".repeat(100)), None)
            .await
            .unwrap();
        store
            .set("key2".to_string(), Bytes::from("b".repeat(100)), None)
            .await
            .unwrap();

        // Access key1 to make it recently used
        let _ = store.get("key1").await;

        // Add key3 which should evict key2 (LRU)
        store
            .set("key3".to_string(), Bytes::from("c".repeat(100)), None)
            .await
            .unwrap();

        // key1 should still exist (recently used)
        assert!(store.exists("key1").await);
    }

    #[tokio::test]
    async fn test_overwrite_key() {
        let store = CacheStore::new(4, 1024 * 1024, EvictionPolicy::Lru, test_metrics());
        store
            .set("key1".to_string(), Bytes::from("value1"), None)
            .await
            .unwrap();
        store
            .set("key1".to_string(), Bytes::from("value2"), None)
            .await
            .unwrap();

        let val = store.get("key1").await.unwrap();
        assert_eq!(val, Bytes::from("value2"));
    }

    #[tokio::test]
    async fn test_flush() {
        let store = CacheStore::new(4, 1024 * 1024, EvictionPolicy::Lru, test_metrics());
        for i in 0..100 {
            store
                .set(format!("key{i}"), Bytes::from("value"), None)
                .await
                .unwrap();
        }

        store.flush().await;
        assert_eq!(store.entry_count().await, 0);
        assert_eq!(store.memory_used(), 0);
    }

    #[tokio::test]
    async fn test_background_expiration() {
        let store = CacheStore::new(4, 1024 * 1024, EvictionPolicy::Lru, test_metrics());

        for i in 0..10 {
            store
                .set(
                    format!("key{i}"),
                    Bytes::from("value"),
                    Some(Duration::from_millis(50)),
                )
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        let expired = store.expire_scan(100).await;
        assert_eq!(expired, 10);
        assert_eq!(store.entry_count().await, 0);
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let store = Arc::new(CacheStore::new(
            16,
            10 * 1024 * 1024,
            EvictionPolicy::Lru,
            test_metrics(),
        ));

        let mut handles = vec![];

        // Spawn 100 concurrent writers
        for i in 0..100 {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                store
                    .set(format!("key{i}"), Bytes::from(format!("value{i}")), None)
                    .await
                    .unwrap();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // All keys should exist
        for i in 0..100 {
            assert!(store.exists(&format!("key{i}")).await);
        }
    }
}
