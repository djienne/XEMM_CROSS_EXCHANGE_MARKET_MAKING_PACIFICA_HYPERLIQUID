use anyhow::Result;

/// mimalloc: measurably lower allocation latency and far less fragmentation
/// than the system allocator for a long-running, allocation-heavy async
/// process. Works on both the Windows dev box and the Debian deploy image.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// XEMM Bot - Cross-Exchange Market Making Bot
///
/// Quotes post-only maker orders on Pacifica priced off the Hyperliquid book,
/// hedges fills on Hyperliquid, audits each cycle, and repeats until Ctrl+C /
/// SIGTERM. Evaluation is event-driven on book updates; see README
/// "Concurrent Tasks" for the task list.
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
        .init();

    // Create and initialize bot (all wiring happens in XemmBot::new())
    let bot = xemm_rust::app::XemmBot::new().await?;

    // Run the bot (spawns all services and executes main loop).
    // `_guard` lives until here so buffered log lines flush on exit.
    bot.run().await
}
