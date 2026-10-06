// FluxCache - LRU Eviction Policy
//
// Implements an O(1) LRU cache eviction tracker using an intrusive doubly-linked list
// backed by a HashMap. This provides O(1) lookup, promotion (on access), and eviction
// (remove least-recently-used).
//
// Design decision: We use a Vec-based linked list (arena allocation) to avoid
// pointer chasing and unsafe code. Each node stores an index into the arena.
// This gives us cache-friendly traversal while maintaining O(1) operations.

use std::collections::HashMap;

/// Index into the arena-allocated linked list.
type NodeIndex = usize;

/// A node in the doubly-linked list.
#[derive(Debug)]
struct LruNode {
    key: String,
    prev: Option<NodeIndex>,
    next: Option<NodeIndex>,
    /// Whether this node is still live (not removed).
    active: bool,
}

/// O(1) LRU eviction tracker.
///
/// Maintains a doubly-linked list of keys ordered by recency.
/// The head is the most recently used; the tail is the least recently used.
#[derive(Debug)]
pub struct LruPolicy {
    /// Arena of linked list nodes.
    nodes: Vec<LruNode>,
    /// Map from key to node index.
    map: HashMap<String, NodeIndex>,
    /// Index of the most recently used node.
    head: Option<NodeIndex>,
    /// Index of the least recently used node.
    tail: Option<NodeIndex>,
    /// Free list for reusing removed node slots.
    free_list: Vec<NodeIndex>,
}

impl LruPolicy {
    /// Create a new empty LRU tracker.
    pub fn new() -> Self {
        LruPolicy {
            nodes: Vec::new(),
            map: HashMap::new(),
            head: None,
            tail: None,
            free_list: Vec::new(),
        }
    }

    /// Record an access to `key`, moving it to the front (most recently used).
    /// If the key doesn't exist, it is inserted.
    pub fn touch(&mut self, key: &str) {
        if let Some(&idx) = self.map.get(key) {
            // Already exists — promote to head
            self.detach(idx);
            self.push_front(idx);
        } else {
            // New key — allocate a node and push to front
            let idx = self.alloc_node(key.to_string());
            self.map.insert(key.to_string(), idx);
            self.push_front(idx);
        }
    }

    /// Remove the least recently used key and return it.
    pub fn evict(&mut self) -> Option<String> {
        let tail_idx = self.tail?;
        let key = self.nodes[tail_idx].key.clone();
        self.remove_key(&key);
        Some(key)
    }

    /// Remove a specific key from the LRU tracker.
    pub fn remove_key(&mut self, key: &str) {
        if let Some(idx) = self.map.remove(key) {
            self.detach(idx);
            self.nodes[idx].active = false;
            self.free_list.push(idx);
        }
    }

    /// Peek at the least recently used key without removing it.
    pub fn peek_lru(&self) -> Option<&str> {
        self.tail.map(|idx| self.nodes[idx].key.as_str())
    }

    /// Number of tracked keys.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the tracker is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    // --- Internal linked-list operations ---

    /// Allocate a new node (or reuse from free list).
    fn alloc_node(&mut self, key: String) -> NodeIndex {
        if let Some(idx) = self.free_list.pop() {
            self.nodes[idx] = LruNode {
                key,
                prev: None,
                next: None,
                active: true,
            };
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(LruNode {
                key,
                prev: None,
                next: None,
                active: true,
            });
            idx
        }
    }

    /// Detach a node from the linked list (does not free it).
    fn detach(&mut self, idx: NodeIndex) {
        let prev = self.nodes[idx].prev;
        let next = self.nodes[idx].next;

        if let Some(p) = prev {
            self.nodes[p].next = next;
        } else {
            self.head = next;
        }

        if let Some(n) = next {
            self.nodes[n].prev = prev;
        } else {
            self.tail = prev;
        }

        self.nodes[idx].prev = None;
        self.nodes[idx].next = None;
    }

    /// Push a node to the front of the list (most recently used).
    fn push_front(&mut self, idx: NodeIndex) {
        self.nodes[idx].prev = None;
        self.nodes[idx].next = self.head;

        if let Some(old_head) = self.head {
            self.nodes[old_head].prev = Some(idx);
        }

        self.head = Some(idx);

        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }
}

impl Default for LruPolicy {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_lru_eviction_order() {
        let mut lru = LruPolicy::new();
        lru.touch("a");
        lru.touch("b");
        lru.touch("c");

        // Order: c (MRU) -> b -> a (LRU)
        assert_eq!(lru.peek_lru(), Some("a"));
        assert_eq!(lru.evict(), Some("a".to_string()));
        assert_eq!(lru.evict(), Some("b".to_string()));
        assert_eq!(lru.evict(), Some("c".to_string()));
        assert_eq!(lru.evict(), None);
    }

    #[test]
    fn test_promotion_on_access() {
        let mut lru = LruPolicy::new();
        lru.touch("a");
        lru.touch("b");
        lru.touch("c");

        // Access 'a' again — it should move to the front
        lru.touch("a");

        // Order: a (MRU) -> c -> b (LRU)
        assert_eq!(lru.peek_lru(), Some("b"));
        assert_eq!(lru.evict(), Some("b".to_string()));
        assert_eq!(lru.evict(), Some("c".to_string()));
        assert_eq!(lru.evict(), Some("a".to_string()));
    }

    #[test]
    fn test_remove_key() {
        let mut lru = LruPolicy::new();
        lru.touch("a");
        lru.touch("b");
        lru.touch("c");

        lru.remove_key("b");
        assert_eq!(lru.len(), 2);

        assert_eq!(lru.evict(), Some("a".to_string()));
        assert_eq!(lru.evict(), Some("c".to_string()));
    }

    #[test]
    fn test_empty_evict() {
        let mut lru = LruPolicy::new();
        assert_eq!(lru.evict(), None);
    }

    #[test]
    fn test_single_element() {
        let mut lru = LruPolicy::new();
        lru.touch("only");
        assert_eq!(lru.peek_lru(), Some("only"));
        assert_eq!(lru.evict(), Some("only".to_string()));
        assert!(lru.is_empty());
    }

    #[test]
    fn test_repeated_touch_same_key() {
        let mut lru = LruPolicy::new();
        lru.touch("a");
        lru.touch("a");
        lru.touch("a");
        assert_eq!(lru.len(), 1);
        assert_eq!(lru.evict(), Some("a".to_string()));
    }

    #[test]
    fn test_free_list_reuse() {
        let mut lru = LruPolicy::new();
        lru.touch("a");
        lru.touch("b");
        lru.remove_key("a");

        // "c" should reuse the freed slot
        lru.touch("c");
        assert_eq!(lru.len(), 2);
        assert_eq!(lru.evict(), Some("b".to_string()));
        assert_eq!(lru.evict(), Some("c".to_string()));
    }
}
