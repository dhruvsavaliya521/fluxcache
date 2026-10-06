// FluxCache - LFU Eviction Policy
//
// Implements a frequency-based eviction tracker with aging/decay.
//
// Design decision: We use a bucketed approach where keys are grouped by frequency.
// The minimum-frequency bucket is tracked for O(1) eviction. To prevent old hot keys
// from remaining immortal, we implement periodic frequency decay: all frequencies
// are halved, which allows recently-hot keys to remain while aging out stale ones.

use std::collections::{BTreeMap, HashMap, HashSet};

/// LFU eviction tracker with frequency decay.
#[derive(Debug)]
pub struct LfuPolicy {
    /// Map from key to its current frequency.
    key_freq: HashMap<String, u64>,
    /// Map from frequency to set of keys with that frequency.
    freq_keys: BTreeMap<u64, HashSet<String>>,
    /// Total number of accesses since last decay.
    access_count: u64,
    /// Number of accesses between decay passes.
    decay_interval: u64,
}

impl LfuPolicy {
    /// Create a new LFU tracker.
    ///
    /// `decay_interval` controls how often frequencies are halved.
    /// A value of 1000 means every 1000 accesses, all frequencies are decayed.
    pub fn new(decay_interval: u64) -> Self {
        LfuPolicy {
            key_freq: HashMap::new(),
            freq_keys: BTreeMap::new(),
            access_count: 0,
            decay_interval,
        }
    }

    /// Record an access to `key`, incrementing its frequency.
    pub fn touch(&mut self, key: &str) {
        self.access_count += 1;

        if let Some(&old_freq) = self.key_freq.get(key) {
            // Remove from old frequency bucket
            if let Some(keys) = self.freq_keys.get_mut(&old_freq) {
                keys.remove(key);
                if keys.is_empty() {
                    self.freq_keys.remove(&old_freq);
                }
            }

            let new_freq = old_freq + 1;
            self.key_freq.insert(key.to_string(), new_freq);
            self.freq_keys
                .entry(new_freq)
                .or_default()
                .insert(key.to_string());
        } else {
            // New key starts at frequency 1
            self.key_freq.insert(key.to_string(), 1);
            self.freq_keys.entry(1).or_default().insert(key.to_string());
        }

        // Check if we should decay
        if self.access_count >= self.decay_interval {
            self.decay();
            self.access_count = 0;
        }
    }

    /// Evict the least frequently used key.
    /// If multiple keys share the minimum frequency, one is chosen arbitrarily.
    pub fn evict(&mut self) -> Option<String> {
        // Get the lowest frequency bucket
        let min_freq = *self.freq_keys.keys().next()?;
        let keys = self.freq_keys.get_mut(&min_freq)?;

        // Take an arbitrary key from this bucket
        let key = keys.iter().next()?.clone();
        keys.remove(&key);

        if keys.is_empty() {
            self.freq_keys.remove(&min_freq);
        }

        self.key_freq.remove(&key);
        Some(key)
    }

    /// Remove a specific key from the LFU tracker.
    pub fn remove_key(&mut self, key: &str) {
        if let Some(freq) = self.key_freq.remove(key) {
            if let Some(keys) = self.freq_keys.get_mut(&freq) {
                keys.remove(key);
                if keys.is_empty() {
                    self.freq_keys.remove(&freq);
                }
            }
        }
    }

    /// Get the frequency of a key.
    pub fn frequency(&self, key: &str) -> Option<u64> {
        self.key_freq.get(key).copied()
    }

    /// Number of tracked keys.
    pub fn len(&self) -> usize {
        self.key_freq.len()
    }

    /// Whether the tracker is empty.
    pub fn is_empty(&self) -> bool {
        self.key_freq.is_empty()
    }

    /// Decay all frequencies by halving them.
    /// This prevents old hot keys from being immortal.
    fn decay(&mut self) {
        let mut new_freq_keys: BTreeMap<u64, HashSet<String>> = BTreeMap::new();

        for (key, freq) in &mut self.key_freq {
            let new_freq = (*freq / 2).max(1);
            *freq = new_freq;
            new_freq_keys
                .entry(new_freq)
                .or_default()
                .insert(key.clone());
        }

        self.freq_keys = new_freq_keys;
    }
}

impl Default for LfuPolicy {
    fn default() -> Self {
        Self::new(10_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_lfu_eviction() {
        let mut lfu = LfuPolicy::new(10_000);
        lfu.touch("a"); // freq 1
        lfu.touch("b"); // freq 1
        lfu.touch("b"); // freq 2
        lfu.touch("c"); // freq 1
        lfu.touch("c"); // freq 2
        lfu.touch("c"); // freq 3

        // 'a' has lowest frequency (1), should be evicted first
        assert_eq!(lfu.evict(), Some("a".to_string()));
    }

    #[test]
    fn test_frequency_tracking() {
        let mut lfu = LfuPolicy::new(10_000);
        lfu.touch("x");
        assert_eq!(lfu.frequency("x"), Some(1));
        lfu.touch("x");
        assert_eq!(lfu.frequency("x"), Some(2));
        lfu.touch("x");
        assert_eq!(lfu.frequency("x"), Some(3));
    }

    #[test]
    fn test_remove_key() {
        let mut lfu = LfuPolicy::new(10_000);
        lfu.touch("a");
        lfu.touch("b");
        lfu.remove_key("a");

        assert_eq!(lfu.len(), 1);
        assert_eq!(lfu.frequency("a"), None);
        assert_eq!(lfu.evict(), Some("b".to_string()));
    }

    #[test]
    fn test_empty_evict() {
        let mut lfu = LfuPolicy::new(10_000);
        assert_eq!(lfu.evict(), None);
    }

    #[test]
    fn test_decay() {
        let mut lfu = LfuPolicy::new(3); // Decay every 3 accesses

        lfu.touch("a"); // freq 1, access_count=1
        lfu.touch("a"); // freq 2, access_count=2
        lfu.touch("a"); // freq 3, access_count=3 -> decay: freq becomes 1

        // After decay, freq should be halved: 3/2 = 1 (clamped to 1)
        assert_eq!(lfu.frequency("a"), Some(1));
    }

    #[test]
    fn test_decay_preserves_relative_order() {
        let mut lfu = LfuPolicy::new(100);

        // Build up different frequencies
        for _ in 0..10 {
            lfu.touch("hot");
        }
        for _ in 0..5 {
            lfu.touch("warm");
        }
        lfu.touch("cold");

        // Force decay
        lfu.decay();

        // hot should still have higher freq than warm, which is higher than cold
        let hot_freq = lfu.frequency("hot").unwrap();
        let warm_freq = lfu.frequency("warm").unwrap();
        let cold_freq = lfu.frequency("cold").unwrap();
        assert!(hot_freq >= warm_freq);
        assert!(warm_freq >= cold_freq);
    }
}
