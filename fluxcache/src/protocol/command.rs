// FluxCache - Protocol Command Definitions
//
// Defines all commands supported by the FluxCache protocol.

use bytes::Bytes;

/// A parsed FluxCache command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// PING - health check
    Ping,

    /// GET <key> - retrieve a value
    Get { key: String },

    /// SET <key> <value> [EX <seconds>] - store a value with optional TTL
    Set {
        key: String,
        value: Bytes,
        ttl_secs: Option<u64>,
    },

    /// DEL <key> - delete a key
    Del { key: String },

    /// EXISTS <key> - check if key exists
    Exists { key: String },

    /// EXPIRE <key> <seconds> - set TTL on existing key
    Expire { key: String, seconds: u64 },

    /// TTL <key> - get remaining TTL
    Ttl { key: String },

    /// STATS - get cache statistics
    Stats,
}
