// FluxCache Client - Connection Pooling

use crate::connection::{Connection, Error};
use deadpool::managed;

/// A manager for FluxCache connections
pub struct ConnectionManager {
    addr: String,
}

impl ConnectionManager {
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }
}

#[async_trait::async_trait]
impl managed::Manager for ConnectionManager {
    type Type = Connection;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        Connection::connect(&self.addr).await
    }

    async fn recycle(
        &self,
        obj: &mut Self::Type,
        _: &managed::Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        // Ensure connection is still alive via PING
        match obj.ping().await {
            Ok(true) => Ok(()),
            Ok(false) => Err(managed::RecycleError::Message(
                "Invalid ping response".into(),
            )),
            Err(e) => Err(managed::RecycleError::Backend(e)),
        }
    }
}

pub type Pool = managed::Pool<ConnectionManager>;
pub type Object = managed::Object<ConnectionManager>;

/// Build a connection pool for a specific node
pub fn create_pool(addr: impl Into<String>, max_size: usize) -> Pool {
    let manager = ConnectionManager::new(addr);
    Pool::builder(manager)
        .max_size(max_size)
        .build()
        .expect("Failed to build connection pool")
}
