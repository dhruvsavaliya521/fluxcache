// FluxCache - TTL Expiration Manager
//
// Handles background expiration of cache entries.
// Implements both lazy expiration (on read) and active background expiration.
//
// Design: Uses a sorted data structure to efficiently find expired entries
// during periodic scans. The scan interval is configurable.

use std::collections::BTreeMap;
use std::time::Instant;

/// Tracks expiration times for cache entries.
///
/// Uses a BTreeMap keyed by expiration time for efficient scanning.
/// Multiple keys can share the same expiration time.
#[derive(Debug)]
pub struct TtlTracker {
    /// Map from expiration time to set of keys expiring at that time.
    /// BTreeMap provides ordered traversal for efficient expired-key scanning.
    expirations: BTreeMap<Instant, Vec<String>>,
    /// Reverse map: key -> expiration time (for removal/update).
    key_expiry: std::collections::HashMap<String, Instant>,
}

impl TtlTracker {
    /// Create a new TTL tracker.
    pub fn new() -> Self {
        TtlTracker {
            expirations: BTreeMap::new(),
            key_expiry: std::collections::HashMap::new(),
        }
    }

    /// Register an expiration for a key.
    pub fn set_expiry(&mut self, key: String, expires_at: Instant) {
        // Remove any existing expiry for this key
        self.remove_expiry(&key);

        // Add new expiry
        self.expirations
            .entry(expires_at)
            .or_default()
            .push(key.clone());
        self.key_expiry.insert(key, expires_at);
    }

    /// Remove expiration tracking for a key.
    pub fn remove_expiry(&mut self, key: &str) {
        if let Some(old_exp) = self.key_expiry.remove(key) {
            if let Some(keys) = self.expirations.get_mut(&old_exp) {
                keys.retain(|k| k != key);
                if keys.is_empty() {
                    self.expirations.remove(&old_exp);
                }
            }
        }
    }

    /// Collect all keys that have expired as of `now`.
    /// Returns up to `max_batch` expired keys to bound scan time.
    pub fn collect_expired(&mut self, now: Instant, max_batch: usize) -> Vec<String> {
        let mut expired = Vec::new();
        let mut empty_times = Vec::new();

        for (&exp_time, keys) in &self.expirations {
            if exp_time > now {
                break; // BTreeMap is sorted, no more expired entries
            }

            for key in keys {
                if expired.len() >= max_batch {
                    break;
                }
                expired.push(key.clone());
            }

            if expired.len() >= max_batch {
                break;
            }
            empty_times.push(exp_time);
        }

        // Clean up collected entries
        for key in &expired {
            self.key_expiry.remove(key);
        }
        for time in &empty_times {
            self.expirations.remove(time);
        }

        // If we stopped mid-bucket due to max_batch, clean up those keys too
        for key in &expired {
            // Check remaining buckets for this key
            for keys in self.expirations.values_mut() {
                keys.retain(|k| k != key);
            }
        }

        expired
    }

    /// Check if a specific key has expired.
    pub fn is_expired(&self, key: &str, now: Instant) -> bool {
        if let Some(&exp) = self.key_expiry.get(key) {
            exp <= now
        } else {
            false
        }
    }

    /// Get the expiration time for a key, if set.
    pub fn get_expiry(&self, key: &str) -> Option<Instant> {
        self.key_expiry.get(key).copied()
    }

    /// Number of keys being tracked.
    pub fn len(&self) -> usize {
        self.key_expiry.len()
    }

    /// Whether the tracker is empty.
    pub fn is_empty(&self) -> bool {
        self.key_expiry.is_empty()
    }
}

impl Default for TtlTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_set_and_check_expiry() {
        let mut tracker = TtlTracker::new();
        let future = Instant::now() + Duration::from_secs(60);
        tracker.set_expiry("key1".to_string(), future);

        assert!(!tracker.is_expired("key1", Instant::now()));
        assert_eq!(tracker.len(), 1);
    }

    #[test]
    fn test_collect_expired() {
        let mut tracker = TtlTracker::new();
        let now = Instant::now();
        let past = now - Duration::from_secs(1);
        let future = now + Duration::from_secs(60);

        tracker.set_expiry("expired1".to_string(), past);
        tracker.set_expiry("expired2".to_string(), past);
        tracker.set_expiry("alive".to_string(), future);

        let expired = tracker.collect_expired(now, 100);
        assert_eq!(expired.len(), 2);
        assert!(expired.contains(&"expired1".to_string()));
        assert!(expired.contains(&"expired2".to_string()));

        // 'alive' should still be tracked
        assert_eq!(tracker.len(), 1);
    }

    #[test]
    fn test_remove_expiry() {
        let mut tracker = TtlTracker::new();
        let future = Instant::now() + Duration::from_secs(60);
        tracker.set_expiry("key1".to_string(), future);
        assert_eq!(tracker.len(), 1);

        tracker.remove_expiry("key1");
        assert_eq!(tracker.len(), 0);
    }

    #[test]
    fn test_update_expiry() {
        let mut tracker = TtlTracker::new();
        let time1 = Instant::now() + Duration::from_secs(30);
        let time2 = Instant::now() + Duration::from_secs(60);

        tracker.set_expiry("key1".to_string(), time1);
        tracker.set_expiry("key1".to_string(), time2);

        // Should only have one entry
        assert_eq!(tracker.len(), 1);
        assert_eq!(tracker.get_expiry("key1"), Some(time2));
    }

    #[test]
    fn test_batch_limit() {
        let mut tracker = TtlTracker::new();
        let past = Instant::now() - Duration::from_secs(1);

        for i in 0..100 {
            tracker.set_expiry(format!("key{i}"), past);
        }

        let expired = tracker.collect_expired(Instant::now(), 10);
        assert_eq!(expired.len(), 10);
    }
}
