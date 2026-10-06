use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluxcache::cache::lfu::LfuPolicy;
use fluxcache::cache::lru::LruPolicy;
use fluxcache::cache::store::CacheStore;
use fluxcache::config::EvictionPolicy;
use fluxcache::metrics::CacheMetrics;
use fluxcache::protocol::parser::parse_command;
use fluxcache::protocol::response::Response;
use fluxcache::shard::consistent_hash::ConsistentHashRing;
use std::sync::Arc;

fn bench_lru_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru");

    group.bench_function("touch_new", |b| {
        b.iter_with_setup(
            || LruPolicy::new(),
            |mut lru| {
                for i in 0..1000 {
                    lru.touch(black_box(&format!("key{i}")));
                }
            },
        );
    });

    group.bench_function("touch_existing", |b| {
        b.iter_with_setup(
            || {
                let mut lru = LruPolicy::new();
                for i in 0..1000 {
                    lru.touch(&format!("key{i}"));
                }
                lru
            },
            |mut lru| {
                for i in 0..1000 {
                    lru.touch(black_box(&format!("key{i}")));
                }
            },
        );
    });

    group.bench_function("evict", |b| {
        b.iter_with_setup(
            || {
                let mut lru = LruPolicy::new();
                for i in 0..1000 {
                    lru.touch(&format!("key{i}"));
                }
                lru
            },
            |mut lru| {
                for _ in 0..1000 {
                    black_box(lru.evict());
                }
            },
        );
    });

    group.finish();
}

fn bench_lfu_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("lfu");

    group.bench_function("touch_new", |b| {
        b.iter_with_setup(
            || LfuPolicy::new(100_000),
            |mut lfu| {
                for i in 0..1000 {
                    lfu.touch(black_box(&format!("key{i}")));
                }
            },
        );
    });

    group.bench_function("evict", |b| {
        b.iter_with_setup(
            || {
                let mut lfu = LfuPolicy::new(100_000);
                for i in 0..1000 {
                    lfu.touch(&format!("key{i}"));
                }
                lfu
            },
            |mut lfu| {
                for _ in 0..1000 {
                    black_box(lfu.evict());
                }
            },
        );
    });

    group.finish();
}

fn bench_consistent_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("consistent_hash");

    for &num_nodes in &[3, 10, 50] {
        group.bench_with_input(
            BenchmarkId::new("lookup", num_nodes),
            &num_nodes,
            |b, &n| {
                let mut ring = ConsistentHashRing::new(150);
                for i in 0..n {
                    ring.add_node(&format!("node{i}"));
                }
                b.iter(|| {
                    for i in 0..1000 {
                        black_box(ring.get_node(&format!("key{i}")));
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_protocol_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol");

    group.bench_function("parse_get", |b| {
        let frame = b"GET mykey\r\n";
        b.iter(|| {
            black_box(parse_command(frame).unwrap());
        });
    });

    group.bench_function("parse_set", |b| {
        let frame = b"SET mykey myvalue EX 60\r\n";
        b.iter(|| {
            black_box(parse_command(frame).unwrap());
        });
    });

    group.bench_function("encode_response", |b| {
        let resp = Response::Data(Bytes::from("hello world value data"));
        b.iter(|| {
            black_box(resp.encode());
        });
    });

    group.finish();
}

fn bench_cache_store(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cache_store");

    group.bench_function("set", |b| {
        let metrics = Arc::new(CacheMetrics::new());
        let store = Arc::new(CacheStore::new(
            16,
            100 * 1024 * 1024,
            EvictionPolicy::Lru,
            metrics,
        ));
        b.to_async(&rt).iter(|| {
            let store = Arc::clone(&store);
            async move {
                store
                    .set("bench_key".to_string(), Bytes::from("bench_value"), None)
                    .await
                    .unwrap();
            }
        });
    });

    group.bench_function("get_hit", |b| {
        let metrics = Arc::new(CacheMetrics::new());
        let store = Arc::new(CacheStore::new(
            16,
            100 * 1024 * 1024,
            EvictionPolicy::Lru,
            metrics,
        ));
        rt.block_on(async {
            store
                .set("bench_key".to_string(), Bytes::from("bench_value"), None)
                .await
                .unwrap();
        });
        b.to_async(&rt).iter(|| {
            let store = Arc::clone(&store);
            async move {
                black_box(store.get("bench_key").await.unwrap());
            }
        });
    });

    group.bench_function("get_miss", |b| {
        let metrics = Arc::new(CacheMetrics::new());
        let store = Arc::new(CacheStore::new(
            16,
            100 * 1024 * 1024,
            EvictionPolicy::Lru,
            metrics,
        ));
        b.to_async(&rt).iter(|| {
            let store = Arc::clone(&store);
            async move {
                black_box(store.get("nonexistent").await.ok());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_lru_operations,
    bench_lfu_operations,
    bench_consistent_hash,
    bench_protocol_parsing,
    bench_cache_store,
);
criterion_main!(benches);
