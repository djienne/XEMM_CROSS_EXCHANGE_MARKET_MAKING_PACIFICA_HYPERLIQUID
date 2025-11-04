use anyhow::Result;
use xemm_rust::connector::hyperliquid::{
    OrderbookClient, OrderbookConfig, HyperliquidTrading, HyperliquidCredentials,
};
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tracing::info;

/// Test XPL market orders with $20 notional
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("════════════════════════════════════════════════");
    info!("  XPL Market Order Test ($20 notional)");
    info!("════════════════════════════════════════════════");
    info!("");

    // Load credentials
    dotenv::dotenv().ok();
    let credentials = HyperliquidCredentials::from_env()?;

    use ethers::signers::Signer;
    let wallet: ethers::signers::LocalWallet = credentials.private_key.parse()?;
    let wallet_address = format!("{:?}", wallet.address());
    info!("✓ Loaded credentials for wallet: {}", wallet_address);

    // Shared state for current prices
    let prices = Arc::new(Mutex::new((0.0, 0.0))); // (bid, ask)
    let prices_clone = prices.clone();

    // Start orderbook client
    info!("Starting orderbook client for XPL...");
    let orderbook_config = OrderbookConfig {
        coin: "XPL".to_string(),
        reconnect_attempts: 5,
        ping_interval_secs: 30,
        request_interval_ms: 100,
    };

    let mut orderbook_client = OrderbookClient::new(orderbook_config)?;

    // Spawn orderbook client in background
    tokio::spawn(async move {
        orderbook_client.start(move |bid, ask, _coin, _timestamp| {
            let bid_price: f64 = bid.parse().unwrap_or(0.0);
            let ask_price: f64 = ask.parse().unwrap_or(0.0);
            *prices_clone.lock().unwrap() = (bid_price, ask_price);
        }).await.ok();
    });

    // Wait for initial price data
    info!("Waiting for price data...");
    sleep(Duration::from_secs(3)).await;

    // Get current prices
    let (bid, ask) = *prices.lock().unwrap();
    if bid == 0.0 || ask == 0.0 {
        anyhow::bail!("Failed to get valid prices from orderbook");
    }

    let mid = (bid + ask) / 2.0;
    info!("Current XPL prices - Bid: ${:.6}, Ask: ${:.6}, Mid: ${:.6}", bid, ask, mid);

    // Calculate size for $20 notional
    let target_notional = 20.0;
    let size = target_notional / mid;
    info!("Target notional: ${:.2}, Size: {:.2} XPL", target_notional, size);

    // Create trading client
    let trading_client = HyperliquidTrading::new(credentials, false)?;

    info!("");
    info!("════════════════════════════════════════════════");
    info!("  Test 1: BUY {:.2} XPL (~$20)", size);
    info!("════════════════════════════════════════════════");
    info!("");

    // Buy XPL
    let (bid, ask) = *prices.lock().unwrap();
    let buy_result = trading_client
        .place_market_order(
            "XPL",
            true,          // is_buy = true
            size,          // size
            0.05,          // 5% slippage
            false,         // reduce_only = false (NEVER true on Hyperliquid)
            Some(bid),
            Some(ask),
        )
        .await?;

    info!("✓ BUY order result: {:?}", buy_result);
    info!("");

    // Wait 5 seconds
    info!("Waiting 5 seconds...");
    sleep(Duration::from_secs(5)).await;

    info!("");
    info!("════════════════════════════════════════════════");
    info!("  Test 2: SELL {:.2} XPL (~$20)", size);
    info!("════════════════════════════════════════════════");
    info!("");

    // Sell XPL (close position)
    let (bid, ask) = *prices.lock().unwrap();
    let sell_result = trading_client
        .place_market_order(
            "XPL",
            false,         // is_buy = false
            size,          // size
            0.05,          // 5% slippage
            false,         // reduce_only = false (NEVER true on Hyperliquid)
            Some(bid),
            Some(ask),
        )
        .await?;

    info!("✓ SELL order result: {:?}", sell_result);
    info!("");

    info!("════════════════════════════════════════════════");
    info!("  All XPL tests completed!");
    info!("════════════════════════════════════════════════");
    info!("Exiting in 3 seconds...");

    sleep(Duration::from_secs(3)).await;

    Ok(())
}
