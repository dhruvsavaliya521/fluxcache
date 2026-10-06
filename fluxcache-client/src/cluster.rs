// FluxCache Client - Cluster & Consistent Hashing

use crate::pool::{create_pool, Pool};
use bytes::Bytes;
use std::collections::BTreeMap;
use xxhash_rust::xxh3::xxh3_64;

/// A FluxCache cluster client managing multiple nodes via consistent hashing
pub struct ClusterClient {
    ring: BTreeMap<u64, String>,
    pools: std::collections::HashMap<String, Pool>,
    virtual_nodes: usize,
}

impl ClusterClient {
    /// Create a new cluster client
    pub fn new(nodes: &[&str], virtual_nodes: usize, pool_size: usize) -> Self {
        let mut ring = BTreeMap::new();
        let mut pools = std::collections::HashMap::new();

        for node in nodes {
            pools.insert(node.to_string(), create_pool(*node, pool_size));
            for v in 0..virtual_nodes {
                let v_key = format!("{}-{}", node, v);
                let hash = xxh3_64(v_key.as_bytes());
                ring.insert(hash, node.to_string());
            }
        }

        Self {
            ring,
            pools,
            virtual_nodes,
        }
    }

    /// Add a node to the cluster
    pub fn add_node(&mut self, node: &str, pool_size: usize) {
        if !self.pools.contains_key(node) {
            self.pools
                .insert(node.to_string(), create_pool(node, pool_size));
            for v in 0..self.virtual_nodes {
                let v_key = format!("{}-{}", node, v);
                let hash = xxh3_64(v_key.as_bytes());
                self.ring.insert(hash, node.to_string());
            }
        }
    }

    /// Remove a node from the cluster
    pub fn remove_node(&mut self, node: &str) {
        self.pools.remove(node);
        // Note: Removing from ring requires scanning or recreating it. Recreating is simple here.
        self.ring.retain(|_, v| v != node);
    }

    /// Get the node responsible for a given key
    pub fn get_node(&self, key: &str) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = xxh3_64(key.as_bytes());
        if let Some((_, node)) = self.ring.range(hash..).next() {
            Some(node)
        } else {
            // Wrap around to first node
            self.ring.values().next().map(|s| s.as_str())
        }
    }

    /// Get a connection from the pool for a given key
    pub async fn get_connection(&self, key: &str) -> crate::connection::Error {
        // We actually want to return the Object, but this is a complex type.
        // We'll expose the methods directly on the cluster instead.
        unimplemented!()
    }

    // Core methods routing to the appropriate node pool

    /// GET a value
    pub async fn get(&self, key: &str) -> Result<Option<Bytes>, crate::connection::Error> {
        if let Some(node) = self.get_node(key) {
            if let Some(pool) = self.pools.get(node) {
                let mut conn = pool
                    .get()
                    .await
                    .map_err(|e| crate::connection::Error::Server(e.to_string()))?;
                return conn.get(key).await;
            }
        }
        Err(crate::connection::Error::Server(
            "No nodes available".into(),
        ))
    }

    /// SET a value
    pub async fn set(
        &self,
        key: &str,
        value: &[u8],
        ttl_secs: Option<u64>,
    ) -> Result<(), crate::connection::Error> {
        if let Some(node) = self.get_node(key) {
            if let Some(pool) = self.pools.get(node) {
                let mut conn = pool
                    .get()
                    .await
                    .map_err(|e| crate::connection::Error::Server(e.to_string()))?;
                return conn.set(key, value, ttl_secs).await;
            }
        }
        Err(crate::connection::Error::Server(
            "No nodes available".into(),
        ))
    }

    /// DEL a value
    pub async fn del(&self, key: &str) -> Result<bool, crate::connection::Error> {
        if let Some(node) = self.get_node(key) {
            if let Some(pool) = self.pools.get(node) {
                let mut conn = pool
                    .get()
                    .await
                    .map_err(|e| crate::connection::Error::Server(e.to_string()))?;
                return conn.del(key).await;
            }
        }
        Err(crate::connection::Error::Server(
            "No nodes available".into(),
        ))
    }

    /// EXISTS check
    pub async fn exists(&self, key: &str) -> Result<bool, crate::connection::Error> {
        if let Some(node) = self.get_node(key) {
            if let Some(pool) = self.pools.get(node) {
                let mut conn = pool
                    .get()
                    .await
                    .map_err(|e| crate::connection::Error::Server(e.to_string()))?;
                return conn.exists(key).await;
            }
        }
        Err(crate::connection::Error::Server(
            "No nodes available".into(),
        ))
    }
}
