use anyhow::Result;

/// mimalloc: measurably lower allocation latency and far less fragmentation
/// than the system allocator for a long-running, allocation-heavy async
/// process. Works on both the Windows dev box and the Debian deploy image.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// XEMM Bot - Cross-Exchange Market Making Bot
///
/// Single-cycle arbitrage bot that:
/// 1. Evaluates opportunities between Pacifica and Hyperliquid
/// 2. Places limit order on Pacifica
/// 3. Monitors profitability (cancels if profit drops >3 bps)
/// 4. Auto-refreshes orders based on configured interval (default 60 seconds)
/// 5. Hedges on Hyperliquid when filled (WebSocket + REST API detection)
/// 6. Hedges partial fills above $10 notional
/// 7. Exits after successful hedge
///
/// Tasks:
/// 1. Pacifica Orderbook (WebSocket - real-time push)
/// 2. Hyperliquid Orderbook (WebSocket - 99ms request/response)
/// 3. Fill Detection (WebSocket - primary, real-time)
/// 4. Pacifica REST Polling (orderbook fallback, every 2s)
/// 4.5. Hyperliquid REST Polling (orderbook fallback, every 2s)
/// 5. REST API Fill Detection (backup, 500ms polling with rate limit handling)
/// 5.5. Position Monitor (4th layer, ground truth, 500ms polling)
/// 6. Order Monitoring (profit/age checks, every 25ms)
/// 7. Hedge Execution
/// 8. Main Opportunity Loop (every 100ms)
#[tokio::main]
async fn main() -> Result<()> {
    // Non-blocking logging: the default fmt writer takes a global lock and
    // writes to stdout synchronously, so ANY log line from ANY task can stall
    // the async runtime if the stdout pipe backs up (docker log driver, ssh
    // session). The background writer thread absorbs that; under sustained
    // backpressure lines are dropped (lossy) rather than blocking the hot
    // path - the durable record is the JSONL journals, not stdout.
    let (writer, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(writer)
        .with_ansi(true)
        .init();

    // Create and initialize bot (all wiring happens in XemmBot::new())
    let bot = xemm_rust::app::XemmBot::new().await?;

    // Run the bot (spawns all services and executes main loop).
    // `_guard` lives until here so buffered log lines flush on exit.
    bot.run().await
}
