use xemm_rust::connector::pacifica::{OrderbookClient, OrderbookConfig};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

/// Example demonstrating low-latency mode for maximum speed
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    info!("═══════════════════════════════════════════════════");
    info!("  LOW-LATENCY MODE EXAMPLE - Maximum Speed");
    info!("═══════════════════════════════════════════════════");
    info!("This example demonstrates ultra-fast data reception");
    info!("with minimal processing overhead.");
    info!("═══════════════════════════════════════════════════");
    info!("");

    // Create orderbook client configuration
    let config = OrderbookConfig {
        symbol: "SOL".to_string(),
        agg_level: 1,
        reconnect_attempts: 5,
        ping_interval_secs: 30,
    };

    let mut client = OrderbookClient::new(config)?;

    // Performance tracking
    let update_count = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();
    let count_clone = update_count.clone();

    // Spawn a task to print statistics every 5 seconds
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let count = count_clone.load(Ordering::Relaxed);
            let elapsed = start_time.elapsed().as_secs_f64();
            let rate = count as f64 / elapsed;

            info!("═══════════════════════════════════════════════════");
            info!("  PERFORMANCE STATS");
            info!("  Total Updates: {}", count);
            info!("  Elapsed Time: {:.2}s", elapsed);
            info!("  Update Rate: {:.2} updates/sec", rate);
            info!("═══════════════════════════════════════════════════");
        }
    });

    info!("Starting LOW-LATENCY orderbook stream for SOL...");
    info!("Updates are processed as fast as possible with minimal overhead.");
    info!("");

    // Start client with minimal processing callback
    client.start(move |_best_bid, _best_ask, _symbol, _timestamp| {
        // Increment counter with atomic operation (lock-free, very fast)
        update_count.fetch_add(1, Ordering::Relaxed);

        // NO string parsing
        // NO calculations
        // NO logging (except in stats task)
        // Just raw data reception!

        // In production, you would:
        // 1. Send to a lock-free queue (crossbeam-channel)
        // 2. Process in a separate thread/task
        // 3. Store in shared memory for other processes
    }).await?;

    Ok(())
}
