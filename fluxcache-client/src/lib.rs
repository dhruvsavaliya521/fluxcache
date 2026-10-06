// FluxCache Client
//
// An asynchronous, pooled client for FluxCache clusters utilizing consistent hashing.

pub mod cluster;
pub mod connection;
pub mod pool;
pub mod protocol;

pub use cluster::ClusterClient;
pub use connection::{Connection, Error};
pub use pool::{create_pool, Pool};
pub use protocol::{ProtocolError, Response};
