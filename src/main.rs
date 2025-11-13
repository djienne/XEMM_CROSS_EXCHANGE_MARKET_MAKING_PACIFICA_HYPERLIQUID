use anyhow::Result;

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
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Create and initialize bot (all wiring happens in XemmBot::new())
    let bot = xemm_rust::app::XemmBot::new().await?;

    // Run the bot (spawns all services and executes main loop)
    bot.run().await
}
