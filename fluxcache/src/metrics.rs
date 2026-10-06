// FluxCache - Metrics
//
// Collects and exposes cache metrics for observability.
// Provides both internal counters and Prometheus-compatible output.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Thread-safe cache metrics collector.
#[derive(Debug)]
pub struct CacheMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub sets: AtomicU64,
    pub deletes: AtomicU64,
    pub evictions: AtomicU64,
    pub expirations: AtomicU64,
    pub request_errors: AtomicU64,
    pub singleflight_dedup: AtomicU64,
    pub wal_bytes_written: AtomicU64,
    pub snapshot_count: AtomicU64,
    pub connections_total: AtomicU64,
    pub connections_active: AtomicU64,
    start_time: Instant,
}

impl CacheMetrics {
    pub fn new() -> Self {
        CacheMetrics {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            sets: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            expirations: AtomicU64::new(0),
            request_errors: AtomicU64::new(0),
            singleflight_dedup: AtomicU64::new(0),
            wal_bytes_written: AtomicU64::new(0),
            snapshot_count: AtomicU64::new(0),
            connections_total: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_set(&self) {
        self.sets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_expiration(&self) {
        self.expirations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.request_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_singleflight_dedup(&self) {
        self.singleflight_dedup.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_wal_bytes(&self, bytes: u64) {
        self.wal_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_snapshot(&self) {
        self.snapshot_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connection(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
        self.connections_active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_disconnect(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get uptime in seconds.
    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Generate a snapshot of all metrics as a JSON-compatible structure.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            sets: self.sets.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            expirations: self.expirations.load(Ordering::Relaxed),
            request_errors: self.request_errors.load(Ordering::Relaxed),
            singleflight_dedup: self.singleflight_dedup.load(Ordering::Relaxed),
            wal_bytes_written: self.wal_bytes_written.load(Ordering::Relaxed),
            snapshot_count: self.snapshot_count.load(Ordering::Relaxed),
            connections_total: self.connections_total.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed),
            uptime_secs: self.uptime_secs(),
        }
    }

    /// Render metrics in Prometheus exposition format.
    pub fn to_prometheus(&self, entries: u64, memory_bytes: u64) -> String {
        let snap = self.snapshot();
        let mut out = String::with_capacity(2048);

        write_metric(
            &mut out,
            "fluxcache_cache_hits_total",
            "counter",
            "Total cache hits",
            snap.hits,
        );
        write_metric(
            &mut out,
            "fluxcache_cache_misses_total",
            "counter",
            "Total cache misses",
            snap.misses,
        );
        write_metric(
            &mut out,
            "fluxcache_cache_sets_total",
            "counter",
            "Total cache sets",
            snap.sets,
        );
        write_metric(
            &mut out,
            "fluxcache_cache_deletes_total",
            "counter",
            "Total cache deletes",
            snap.deletes,
        );
        write_metric(
            &mut out,
            "fluxcache_evictions_total",
            "counter",
            "Total evictions",
            snap.evictions,
        );
        write_metric(
            &mut out,
            "fluxcache_expired_entries_total",
            "counter",
            "Total expired entries",
            snap.expirations,
        );
        write_metric(
            &mut out,
            "fluxcache_request_errors_total",
            "counter",
            "Total request errors",
            snap.request_errors,
        );
        write_metric(
            &mut out,
            "fluxcache_singleflight_dedup_total",
            "counter",
            "Total deduplicated requests",
            snap.singleflight_dedup,
        );
        write_metric(
            &mut out,
            "fluxcache_current_entries",
            "gauge",
            "Current number of entries",
            entries,
        );
        write_metric(
            &mut out,
            "fluxcache_current_memory_bytes",
            "gauge",
            "Current memory usage in bytes",
            memory_bytes,
        );
        write_metric(
            &mut out,
            "fluxcache_wal_bytes_written_total",
            "counter",
            "Total WAL bytes written",
            snap.wal_bytes_written,
        );
        write_metric(
            &mut out,
            "fluxcache_snapshot_count_total",
            "counter",
            "Total snapshots taken",
            snap.snapshot_count,
        );
        write_metric(
            &mut out,
            "fluxcache_connections_total",
            "counter",
            "Total connections",
            snap.connections_total,
        );
        write_metric(
            &mut out,
            "fluxcache_connections_active",
            "gauge",
            "Active connections",
            snap.connections_active,
        );
        write_metric(
            &mut out,
            "fluxcache_uptime_seconds",
            "gauge",
            "Server uptime in seconds",
            snap.uptime_secs,
        );

        out
    }
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self::new()
    }
}

fn write_metric(out: &mut String, name: &str, metric_type: &str, help: &str, value: u64) {
    out.push_str(&format!("# HELP {name} {help}\n"));
    out.push_str(&format!("# TYPE {name} {metric_type}\n"));
    out.push_str(&format!("{name} {value}\n"));
}

/// A point-in-time snapshot of metrics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub sets: u64,
    pub deletes: u64,
    pub evictions: u64,
    pub expirations: u64,
    pub request_errors: u64,
    pub singleflight_dedup: u64,
    pub wal_bytes_written: u64,
    pub snapshot_count: u64,
    pub connections_total: u64,
    pub connections_active: u64,
    pub uptime_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_counting() {
        let m = CacheMetrics::new();
        m.record_hit();
        m.record_hit();
        m.record_miss();
        m.record_set();

        let snap = m.snapshot();
        assert_eq!(snap.hits, 2);
        assert_eq!(snap.misses, 1);
        assert_eq!(snap.sets, 1);
    }

    #[test]
    fn test_prometheus_format() {
        let m = CacheMetrics::new();
        m.record_hit();
        let output = m.to_prometheus(100, 50000);
        assert!(output.contains("fluxcache_cache_hits_total 1"));
        assert!(output.contains("fluxcache_current_entries 100"));
        assert!(output.contains("fluxcache_current_memory_bytes 50000"));
    }

    #[test]
    fn test_connection_tracking() {
        let m = CacheMetrics::new();
        m.record_connection();
        m.record_connection();
        m.record_disconnect();

        let snap = m.snapshot();
        assert_eq!(snap.connections_total, 2);
        assert_eq!(snap.connections_active, 1);
    }
}
