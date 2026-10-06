// FluxCache - High Performance Cache Server
//
// Configuration module: handles CLI arguments, config file parsing,
// and runtime configuration.

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Eviction policy for cache entries when memory limit is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EvictionPolicy {
    Lru,
    Lfu,
    None,
}

impl std::str::FromStr for EvictionPolicy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "lru" => Ok(EvictionPolicy::Lru),
            "lfu" => Ok(EvictionPolicy::Lfu),
            "none" => Ok(EvictionPolicy::None),
            _ => Err(format!("Unknown eviction policy: {s}")),
        }
    }
}

impl std::fmt::Display for EvictionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionPolicy::Lru => write!(f, "lru"),
            EvictionPolicy::Lfu => write!(f, "lfu"),
            EvictionPolicy::None => write!(f, "none"),
        }
    }
}

/// Parse a human-readable memory size string (e.g., "512mb", "1gb") into bytes.
fn parse_memory_size(s: &str) -> Result<usize, String> {
    let s = s.trim().to_lowercase();
    let (num_str, multiplier) = if s.ends_with("gb") {
        (&s[..s.len() - 2], 1024 * 1024 * 1024)
    } else if s.ends_with("mb") {
        (&s[..s.len() - 2], 1024 * 1024)
    } else if s.ends_with("kb") {
        (&s[..s.len() - 2], 1024)
    } else if s.ends_with('b') {
        (&s[..s.len() - 1], 1)
    } else {
        (s.as_str(), 1)
    };

    num_str
        .trim()
        .parse::<usize>()
        .map(|n| n * multiplier)
        .map_err(|e| format!("Invalid memory size '{s}': {e}"))
}

/// CLI arguments for FluxCache server.
#[derive(Parser, Debug, Clone)]
#[command(name = "fluxcache")]
#[command(about = "FluxCache - A high-performance cache server")]
#[command(version)]
pub struct CliArgs {
    /// Path to configuration file
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// TCP listen address for data plane
    #[arg(long, default_value = "127.0.0.1:6380")]
    pub listen: String,

    /// HTTP admin listen address
    #[arg(long, default_value = "127.0.0.1:9090")]
    pub admin_listen: String,

    /// Maximum memory (e.g., "512mb", "1gb")
    #[arg(long, default_value = "256mb", value_parser = parse_memory_size)]
    pub max_memory: usize,

    /// Eviction policy
    #[arg(long, default_value = "lru")]
    pub eviction_policy: EvictionPolicy,

    /// Number of shards for internal partitioning
    #[arg(long, default_value = "16")]
    pub shards: usize,

    /// Enable persistence
    #[arg(long, default_value = "false")]
    pub persistence: bool,

    /// Directory for persistence files (WAL + snapshots)
    #[arg(long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// WAL sync interval in milliseconds
    #[arg(long, default_value = "100")]
    pub wal_sync_interval_ms: u64,

    /// Snapshot interval in seconds
    #[arg(long, default_value = "300")]
    pub snapshot_interval_secs: u64,

    /// TTL expiration scan interval in milliseconds
    #[arg(long, default_value = "1000")]
    pub expiration_scan_interval_ms: u64,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Enable JSON structured logging
    #[arg(long, default_value = "false")]
    pub json_logs: bool,

    /// Maximum number of concurrent connections
    #[arg(long, default_value = "10000")]
    pub max_connections: usize,

    /// Connection read timeout in seconds
    #[arg(long, default_value = "300")]
    pub read_timeout_secs: u64,

    /// Maximum request size in bytes
    #[arg(long, default_value = "1048576")]
    pub max_request_size: usize,

    /// TCP listen address for replication data plane
    #[arg(long, default_value = "127.0.0.1:6381")]
    pub replication_listen: String,

    /// If set, run as a replica of the given primary address
    #[arg(long)]
    pub replica_of: Option<String>,
}

/// File-based configuration (mirrors CLI but loaded from TOML).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileConfig {
    pub listen: Option<String>,
    pub admin_listen: Option<String>,
    pub max_memory: Option<String>,
    pub eviction_policy: Option<EvictionPolicy>,
    pub shards: Option<usize>,
    pub persistence: Option<bool>,
    pub data_dir: Option<PathBuf>,
    pub wal_sync_interval_ms: Option<u64>,
    pub snapshot_interval_secs: Option<u64>,
    pub expiration_scan_interval_ms: Option<u64>,
    pub log_level: Option<String>,
    pub json_logs: Option<bool>,
    pub max_connections: Option<usize>,
    pub read_timeout_secs: Option<u64>,
    pub max_request_size: Option<usize>,
    pub replication_listen: Option<String>,
    pub replica_of: Option<String>,
}

/// Resolved runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub listen: String,
    pub admin_listen: String,
    pub max_memory: usize,
    pub eviction_policy: EvictionPolicy,
    pub shards: usize,
    pub persistence: bool,
    pub data_dir: PathBuf,
    pub wal_sync_interval: Duration,
    pub snapshot_interval: Duration,
    pub expiration_scan_interval: Duration,
    pub log_level: String,
    pub json_logs: bool,
    pub max_connections: usize,
    pub read_timeout: Duration,
    pub max_request_size: usize,
    pub replication_listen: String,
    pub replica_of: Option<String>,
}

impl Config {
    /// Build a Config from CLI args, optionally merging with a config file.
    pub fn from_args(args: CliArgs) -> anyhow::Result<Self> {
        // If a config file is specified, load and merge
        let file_config = if let Some(ref path) = args.config {
            let contents = std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("Failed to read config file: {e}"))?;
            Some(
                toml::from_str::<FileConfig>(&contents)
                    .map_err(|e| anyhow::anyhow!("Failed to parse config file: {e}"))?,
            )
        } else {
            None
        };

        let max_memory = if let Some(ref fc) = file_config {
            if let Some(ref mem_str) = fc.max_memory {
                parse_memory_size(mem_str).map_err(|e| anyhow::anyhow!(e))?
            } else {
                args.max_memory
            }
        } else {
            args.max_memory
        };

        Ok(Config {
            listen: file_config
                .as_ref()
                .and_then(|f| f.listen.clone())
                .unwrap_or(args.listen),
            admin_listen: file_config
                .as_ref()
                .and_then(|f| f.admin_listen.clone())
                .unwrap_or(args.admin_listen),
            max_memory,
            eviction_policy: file_config
                .as_ref()
                .and_then(|f| f.eviction_policy)
                .unwrap_or(args.eviction_policy),
            shards: file_config
                .as_ref()
                .and_then(|f| f.shards)
                .unwrap_or(args.shards),
            persistence: file_config
                .as_ref()
                .and_then(|f| f.persistence)
                .unwrap_or(args.persistence),
            data_dir: file_config
                .as_ref()
                .and_then(|f| f.data_dir.clone())
                .unwrap_or(args.data_dir),
            wal_sync_interval: Duration::from_millis(
                file_config
                    .as_ref()
                    .and_then(|f| f.wal_sync_interval_ms)
                    .unwrap_or(args.wal_sync_interval_ms),
            ),
            snapshot_interval: Duration::from_secs(
                file_config
                    .as_ref()
                    .and_then(|f| f.snapshot_interval_secs)
                    .unwrap_or(args.snapshot_interval_secs),
            ),
            expiration_scan_interval: Duration::from_millis(
                file_config
                    .as_ref()
                    .and_then(|f| f.expiration_scan_interval_ms)
                    .unwrap_or(args.expiration_scan_interval_ms),
            ),
            log_level: file_config
                .as_ref()
                .and_then(|f| f.log_level.clone())
                .unwrap_or(args.log_level),
            json_logs: file_config
                .as_ref()
                .and_then(|f| f.json_logs)
                .unwrap_or(args.json_logs),
            max_connections: file_config
                .as_ref()
                .and_then(|f| f.max_connections)
                .unwrap_or(args.max_connections),
            read_timeout: Duration::from_secs(
                file_config
                    .as_ref()
                    .and_then(|f| f.read_timeout_secs)
                    .unwrap_or(args.read_timeout_secs),
            ),
            max_request_size: file_config
                .as_ref()
                .and_then(|f| f.max_request_size)
                .unwrap_or(args.max_request_size),
            replication_listen: file_config
                .as_ref()
                .and_then(|f| f.replication_listen.clone())
                .unwrap_or(args.replication_listen),
            replica_of: file_config
                .as_ref()
                .and_then(|f| f.replica_of.clone())
                .or(args.replica_of),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_size() {
        assert_eq!(parse_memory_size("512mb").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("1gb").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("1024kb").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_size("100b").unwrap(), 100);
        assert_eq!(parse_memory_size("1024").unwrap(), 1024);
        assert!(parse_memory_size("abc").is_err());
    }

    #[test]
    fn test_eviction_policy_parse() {
        assert_eq!(
            "lru".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::Lru
        );
        assert_eq!(
            "lfu".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::Lfu
        );
        assert_eq!(
            "none".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::None
        );
        assert!("invalid".parse::<EvictionPolicy>().is_err());
    }
}
