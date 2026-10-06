// FluxCache - Cache Entry
//
// Represents a single cached value with metadata for TTL and eviction tracking.

use bytes::Bytes;
use std::time::Instant;

/// A single cache entry containing the value and associated metadata.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// The cached value as raw bytes.
    pub value: Bytes,

    /// When this entry was created.
    pub created_at: Instant,

    /// When this entry was last accessed (for LRU).
    pub last_accessed: Instant,

    /// Access frequency counter (for LFU).
    pub frequency: u64,

    /// Optional absolute expiration time.
    pub expires_at: Option<Instant>,

    /// Approximate size in bytes (key + value + overhead).
    pub size: usize,
}

impl CacheEntry {
    /// Create a new cache entry with the given value and optional TTL.
    pub fn new(value: Bytes, ttl: Option<std::time::Duration>) -> Self {
        let now = Instant::now();
        let expires_at = ttl.map(|d| now + d);
        let size = value.len() + std::mem::size_of::<Self>();

        CacheEntry {
            value,
            created_at: now,
            last_accessed: now,
            frequency: 1,
            expires_at,
            size,
        }
    }

    /// Check if this entry has expired.
    pub fn is_expired(&self) -> bool {
        if let Some(exp) = self.expires_at {
            Instant::now() >= exp
        } else {
            false
        }
    }

    /// Record an access (updates last_accessed and increments frequency).
    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
        self.frequency = self.frequency.saturating_add(1);
    }

    /// Set a new TTL on this entry.
    pub fn set_ttl(&mut self, ttl: std::time::Duration) {
        self.expires_at = Some(Instant::now() + ttl);
    }

    /// Remove expiration from this entry.
    pub fn clear_ttl(&mut self) {
        self.expires_at = None;
    }

    /// Get the remaining TTL in seconds, or None if no TTL is set.
    pub fn remaining_ttl(&self) -> Option<u64> {
        self.expires_at.map(|exp| {
            let now = Instant::now();
            if exp > now {
                (exp - now).as_secs()
            } else {
                0
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_new_entry_without_ttl() {
        let entry = CacheEntry::new(Bytes::from("hello"), None);
        assert_eq!(entry.value, Bytes::from("hello"));
        assert_eq!(entry.frequency, 1);
        assert!(entry.expires_at.is_none());
        assert!(!entry.is_expired());
    }

    #[test]
    fn test_new_entry_with_ttl() {
        let entry = CacheEntry::new(Bytes::from("hello"), Some(Duration::from_secs(60)));
        assert!(entry.expires_at.is_some());
        assert!(!entry.is_expired());
    }

    #[test]
    fn test_expired_entry() {
        let mut entry = CacheEntry::new(Bytes::from("hello"), None);
        // Set expiration in the past
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        assert!(entry.is_expired());
    }

    #[test]
    fn test_touch_increments_frequency() {
        let mut entry = CacheEntry::new(Bytes::from("hello"), None);
        assert_eq!(entry.frequency, 1);
        entry.touch();
        assert_eq!(entry.frequency, 2);
        entry.touch();
        assert_eq!(entry.frequency, 3);
    }

    #[test]
    fn test_set_and_clear_ttl() {
        let mut entry = CacheEntry::new(Bytes::from("hello"), None);
        assert!(entry.expires_at.is_none());

        entry.set_ttl(Duration::from_secs(30));
        assert!(entry.expires_at.is_some());
        assert!(!entry.is_expired());

        entry.clear_ttl();
        assert!(entry.expires_at.is_none());
    }

    #[test]
    fn test_remaining_ttl() {
        let entry = CacheEntry::new(Bytes::from("hello"), Some(Duration::from_secs(60)));
        let remaining = entry.remaining_ttl().unwrap();
        assert!(remaining <= 60);
        assert!(remaining >= 59);
    }
}
