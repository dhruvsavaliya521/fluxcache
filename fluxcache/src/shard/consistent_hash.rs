// FluxCache - Consistent Hashing
//
// Implements a consistent hash ring with virtual nodes for
// even key distribution across cache nodes/shards.
//
// Design: Uses xxh3 for fast, high-quality hashing.
// Virtual nodes (replicas) improve distribution uniformity.
// The ring is a sorted Vec of (hash, node_id) pairs with binary search.

use xxhash_rust::xxh3::xxh3_64;

/// A point on the hash ring.
#[derive(Debug, Clone)]
struct RingPoint {
    hash: u64,
    node_id: String,
}

/// Consistent hash ring with virtual nodes.
#[derive(Debug, Clone)]
pub struct ConsistentHashRing {
    /// Sorted ring points.
    ring: Vec<RingPoint>,
    /// Number of virtual nodes per physical node.
    virtual_nodes: usize,
    /// List of physical nodes.
    nodes: Vec<String>,
}

impl ConsistentHashRing {
    /// Create a new consistent hash ring.
    ///
    /// `virtual_nodes` controls distribution quality — higher values
    /// give more uniform distribution at the cost of more memory.
    /// Recommended: 100-300 virtual nodes per physical node.
    pub fn new(virtual_nodes: usize) -> Self {
        ConsistentHashRing {
            ring: Vec::new(),
            virtual_nodes: virtual_nodes.max(1),
            nodes: Vec::new(),
        }
    }

    /// Add a node to the ring.
    pub fn add_node(&mut self, node_id: &str) {
        if self.nodes.contains(&node_id.to_string()) {
            return;
        }

        self.nodes.push(node_id.to_string());

        for i in 0..self.virtual_nodes {
            let virtual_key = format!("{node_id}:vn{i}");
            let hash = xxh3_64(virtual_key.as_bytes());
            self.ring.push(RingPoint {
                hash,
                node_id: node_id.to_string(),
            });
        }

        self.ring.sort_by_key(|p| p.hash);
    }

    /// Remove a node from the ring.
    pub fn remove_node(&mut self, node_id: &str) {
        self.nodes.retain(|n| n != node_id);
        self.ring.retain(|p| p.node_id != node_id);
    }

    /// Get the node responsible for the given key.
    pub fn get_node(&self, key: &str) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = xxh3_64(key.as_bytes());

        // Binary search for the first ring point >= hash
        let idx = match self.ring.binary_search_by_key(&hash, |p| p.hash) {
            Ok(i) => i,
            Err(i) => {
                if i >= self.ring.len() {
                    0 // Wrap around
                } else {
                    i
                }
            }
        };

        Some(&self.ring[idx].node_id)
    }

    /// Get N unique nodes responsible for the given key (for replication).
    pub fn get_nodes(&self, key: &str, count: usize) -> Vec<&str> {
        if self.ring.is_empty() {
            return Vec::new();
        }

        let hash = xxh3_64(key.as_bytes());
        let start_idx = match self.ring.binary_search_by_key(&hash, |p| p.hash) {
            Ok(i) => i,
            Err(i) => {
                if i >= self.ring.len() {
                    0
                } else {
                    i
                }
            }
        };

        let mut result = Vec::new();
        let ring_len = self.ring.len();

        for offset in 0..ring_len {
            let idx = (start_idx + offset) % ring_len;
            let node_id = &self.ring[idx].node_id;

            if !result.contains(&node_id.as_str()) {
                result.push(node_id.as_str());
                if result.len() >= count {
                    break;
                }
            }
        }

        result
    }

    /// Number of physical nodes in the ring.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get all node IDs.
    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    /// Total number of ring points (physical * virtual).
    pub fn ring_size(&self) -> usize {
        self.ring.len()
    }
}

impl Default for ConsistentHashRing {
    fn default() -> Self {
        Self::new(150)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_empty_ring() {
        let ring = ConsistentHashRing::new(150);
        assert_eq!(ring.get_node("any_key"), None);
    }

    #[test]
    fn test_single_node() {
        let mut ring = ConsistentHashRing::new(150);
        ring.add_node("node1");

        // All keys should map to node1
        for i in 0..100 {
            assert_eq!(ring.get_node(&format!("key{i}")), Some("node1"));
        }
    }

    #[test]
    fn test_deterministic() {
        let mut ring = ConsistentHashRing::new(150);
        ring.add_node("node1");
        ring.add_node("node2");
        ring.add_node("node3");

        // Same key should always map to the same node
        let node1 = ring.get_node("test_key").unwrap();
        let node2 = ring.get_node("test_key").unwrap();
        assert_eq!(node1, node2);
    }

    #[test]
    fn test_distribution() {
        let mut ring = ConsistentHashRing::new(150);
        ring.add_node("node1");
        ring.add_node("node2");
        ring.add_node("node3");

        let mut counts: HashMap<String, usize> = HashMap::new();
        let num_keys = 10_000;

        for i in 0..num_keys {
            let node = ring.get_node(&format!("key{i}")).unwrap();
            *counts.entry(node.to_string()).or_insert(0) += 1;
        }

        // Each node should get roughly 1/3 of keys (within 15% tolerance)
        let expected = num_keys / 3;
        for (node, count) in &counts {
            let deviation = (*count as f64 - expected as f64).abs() / expected as f64;
            assert!(
                deviation < 0.15,
                "Node {node} has {count} keys, expected ~{expected} (deviation: {:.1}%)",
                deviation * 100.0
            );
        }
    }

    #[test]
    fn test_add_node_minimal_redistribution() {
        let mut ring = ConsistentHashRing::new(150);
        ring.add_node("node1");
        ring.add_node("node2");

        let num_keys = 10_000;
        let mut before: HashMap<String, String> = HashMap::new();
        for i in 0..num_keys {
            let key = format!("key{i}");
            let node = ring.get_node(&key).unwrap().to_string();
            before.insert(key, node);
        }

        // Add a third node
        ring.add_node("node3");

        let mut moved = 0;
        for i in 0..num_keys {
            let key = format!("key{i}");
            let node = ring.get_node(&key).unwrap();
            if node != before[&key] {
                moved += 1;
            }
        }

        // Adding a node should move approximately 1/N of keys
        // With 3 nodes, expect ~33% to move. Allow up to 50%.
        let move_pct = moved as f64 / num_keys as f64;
        assert!(
            move_pct < 0.50,
            "Too many keys moved: {moved}/{num_keys} ({:.1}%)",
            move_pct * 100.0
        );
    }

    #[test]
    fn test_remove_node() {
        let mut ring = ConsistentHashRing::new(150);
        ring.add_node("node1");
        ring.add_node("node2");
        ring.add_node("node3");

        ring.remove_node("node2");
        assert_eq!(ring.node_count(), 2);

        // All keys should still map to a valid node
        for i in 0..100 {
            let node = ring.get_node(&format!("key{i}")).unwrap();
            assert!(node == "node1" || node == "node3");
        }
    }

    #[test]
    fn test_get_nodes_replication() {
        let mut ring = ConsistentHashRing::new(150);
        ring.add_node("node1");
        ring.add_node("node2");
        ring.add_node("node3");

        let nodes = ring.get_nodes("test_key", 2);
        assert_eq!(nodes.len(), 2);
        assert_ne!(nodes[0], nodes[1]);
    }

    #[test]
    fn test_duplicate_add() {
        let mut ring = ConsistentHashRing::new(150);
        ring.add_node("node1");
        ring.add_node("node1"); // Duplicate
        assert_eq!(ring.node_count(), 1);
    }
}
