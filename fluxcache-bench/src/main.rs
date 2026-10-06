use clap::Parser;
use fluxcache_client::ClusterClient;
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(Parser, Debug, Clone)]
#[command(name = "fluxcache-bench")]
#[command(about = "Load testing tool for FluxCache")]
pub struct CliArgs {
    /// Server address
    #[arg(short, long, default_value = "127.0.0.1:6380")]
    pub server: String,

    /// Number of concurrent clients
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Total number of requests
    #[arg(short, long, default_value = "100000")]
    pub requests: usize,

    /// Data payload size in bytes
    #[arg(short, long, default_value = "256")]
    pub payload_size: usize,

    /// Ratio of SET vs GET operations (0.0 to 1.0, where 1.0 is 100% SETs)
    #[arg(long, default_value = "0.2")]
    pub write_ratio: f64,

    /// Keyspace size (to control cache hit rate)
    #[arg(short, long, default_value = "10000")]
    pub keyspace: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();

    println!("========================================================");
    println!("FluxCache Benchmark");
    println!("Server: {}", args.server);
    println!("Concurrency: {}", args.concurrency);
    println!("Total Requests: {}", args.requests);
    println!("Payload Size: {} bytes", args.payload_size);
    println!(
        "Workload: {:.0}% SET, {:.0}% GET",
        args.write_ratio * 100.0,
        (1.0 - args.write_ratio) * 100.0
    );
    println!("Keyspace: {} unique keys", args.keyspace);
    println!("========================================================\n");

    let nodes = vec![args.server.as_str()];
    // Pool size equals concurrency / nodes
    let pool_size = std::cmp::max(1, args.concurrency);
    let client = Arc::new(ClusterClient::new(&nodes, 150, pool_size));

    // Populate data for GETs
    println!("Pre-populating keyspace...");
    let pb = ProgressBar::new(args.keyspace);
    pb.set_style(ProgressStyle::default_bar().template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} keys pre-populated")?.progress_chars("##-"));

    let payload = vec![0xAB; args.payload_size];
    let chunk_size = std::cmp::max(1, args.keyspace / 10);
    
    for start in (0..args.keyspace).step_by(chunk_size as usize) {
        let mut js = JoinSet::new();
        let end = std::cmp::min(args.keyspace, start + chunk_size);
        for i in start..end {
            let c = Arc::clone(&client);
            let p = payload.clone();
            js.spawn(async move {
                let key = format!("bench:key:{}", i);
                let _ = c.set(&key, &p, None).await;
            });
        }
        while let Some(_) = js.join_next().await {}
        pb.inc(end - start);
    }
    pb.finish_with_message("Pre-population complete.");

    // Benchmark Run
    println!("\nStarting benchmark...");
    let reqs_per_worker = args.requests / args.concurrency;
    let mut join_set = JoinSet::new();

    let start_time = Instant::now();

    // Shared histogram
    let global_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));

    let pb = ProgressBar::new(args.requests as u64);
    pb.set_style(ProgressStyle::default_bar().template("[{elapsed_precise}] {bar:40.green/blue} {pos}/{len} requests ({per_sec})")?.progress_chars("=>-"));

    let pb_clone = pb.clone();

    for _ in 0..args.concurrency {
        let c = Arc::clone(&client);
        let p = payload.clone();
        let write_ratio = args.write_ratio;
        let keyspace = args.keyspace;
        let hist_mutex = Arc::clone(&global_hist);
        let pb_inner = pb_clone.clone();

        join_set.spawn(async move {
            let mut local_hist = Histogram::<u64>::new(3).unwrap();
            for _ in 0..reqs_per_worker {
                let (key_id, is_write) = {
                    let mut rng = rand::thread_rng();
                    (rng.gen_range(0..keyspace), rng.gen_bool(write_ratio))
                };
                let key = format!("bench:key:{}", key_id);

                let req_start = Instant::now();

                if is_write {
                    let _ = c.set(&key, &p, None).await;
                } else {
                    let _ = c.get(&key).await;
                }

                let elapsed_micros = req_start.elapsed().as_micros() as u64;
                local_hist.record(elapsed_micros).unwrap();
            }

            let mut global = hist_mutex.lock().unwrap();
            global.add(local_hist).unwrap();
            pb_inner.inc(reqs_per_worker as u64);
        });
    }

    while let Some(res) = join_set.join_next().await {
        res?;
    }

    pb.finish();
    let total_elapsed = start_time.elapsed();
    let total_secs = total_elapsed.as_secs_f64();
    let tps = args.requests as f64 / total_secs;

    let hist = global_hist.lock().unwrap();

    println!("\n========================================================");
    println!("RESULTS");
    println!("========================================================");
    println!("Time taken:       {:.2} seconds", total_secs);
    println!("Total Requests:   {}", args.requests);
    println!("Throughput:       {:.2} req/sec", tps);
    println!("\nLatency (microseconds):");
    println!("  Min:  {}", hist.min());
    println!("  p50:  {}", hist.value_at_quantile(0.50));
    println!("  p95:  {}", hist.value_at_quantile(0.95));
    println!("  p99:  {}", hist.value_at_quantile(0.99));
    println!("  Max:  {}", hist.max());
    println!("========================================================");

    Ok(())
}
