use xemm_rust::connector::hyperliquid::{
    OrderbookClient, OrderbookConfig, HyperliquidTrading, HyperliquidCredentials,
};
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tracing::info;

/// Test script for Hyperliquid market orders
///
/// IMPORTANT: reduce_only should NEVER be used on Hyperliquid
///
/// This example:
/// 1. Connects to Hyperliquid orderbook to get current ENA prices
/// 2. Opens a long position (50 ENA, ~$19.50 notional)
/// 3. Waits 10 seconds
/// 4. Closes the long position (opposite order, NOT reduce-only)
/// 5. Opens a short position (50 ENA, ~$19.50 notional)
/// 6. Waits 10 seconds
/// 7. Closes the short position (opposite order, NOT reduce-only)
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    info!("════════════════════════════════════════════════");
    info!("  Hyperliquid Market Order Test");
    info!("════════════════════════════════════════════════");
    info!("");

    // Load credentials from .env
    dotenv::dotenv().ok();
    let credentials = HyperliquidCredentials::from_env()?;

    // Derive wallet address from private key
    use ethers::signers::Signer;
    let wallet: ethers::signers::LocalWallet = credentials.private_key.parse()?;
    let wallet_address = format!("{:?}", wallet.address());
    info!("✓ Loaded credentials for wallet: {}", wallet_address);

    // Shared state for current prices
    let prices = Arc::new(Mutex::new((0.0, 0.0))); // (bid, ask)
    let prices_clone = prices.clone();

    // Start orderbook client to get real-time prices
    info!("Starting orderbook client for ENA...");
    let orderbook_config = OrderbookConfig {
        coin: "ENA".to_string(),
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
    info!("Current prices - Bid: ${:.2}, Ask: ${:.2}, Mid: ${:.2}", bid, ask, mid);

    // Create trading client
    let mut trading_client = HyperliquidTrading::new(credentials, false)?;

    info!("");
    info!("════════════════════════════════════════════════");
    info!("  Test 1: Long Position");
    info!("════════════════════════════════════════════════");
    info!("Opening long position: BUY 50 ENA");
    info!("Expected notional: ${:.2}", 50.0 * mid);
    info!("════════════════════════════════════════════════");
    info!("");

    // Open long position (buy)
    let (bid, ask) = *prices.lock().unwrap();
    let long_open_result = trading_client
        .place_market_order(
            "ENA",
            true,          // is_buy = true
            50.0,          // size
            0.05,          // 5% slippage
            false,         // reduce_only = false
            Some(bid),
            Some(ask),
        )
        .await?;

    info!("✓ Long position opened: {:?}", long_open_result);
    info!("");

    // Wait 10 seconds
    info!("Waiting 10 seconds...");
    sleep(Duration::from_secs(10)).await;

    info!("");
    info!("════════════════════════════════════════════════");
    info!("  Closing Long Position");
    info!("════════════════════════════════════════════════");
    info!("Closing long position: SELL 50 ENA");
    info!("════════════════════════════════════════════════");
    info!("");

    // Close long position (sell - NOT reduce-only)
    let (bid, ask) = *prices.lock().unwrap();
    let long_close_result = trading_client
        .place_market_order(
            "ENA",
            false,         // is_buy = false
            50.0,          // size
            0.05,          // 5% slippage
            false,         // reduce_only = false (NEVER true on Hyperliquid)
            Some(bid),
            Some(ask),
        )
        .await?;

    info!("✓ Long position closed: {:?}", long_close_result);
    info!("");

    // Small delay before opening short
    sleep(Duration::from_secs(2)).await;

    info!("");
    info!("════════════════════════════════════════════════");
    info!("  Test 2: Short Position");
    info!("════════════════════════════════════════════════");
    let (bid, ask) = *prices.lock().unwrap();
    let mid = (bid + ask) / 2.0;
    info!("Opening short position: SELL 50 ENA");
    info!("Expected notional: ${:.2}", 50.0 * mid);
    info!("════════════════════════════════════════════════");
    info!("");

    // Open short position (sell)
    let short_open_result = trading_client
        .place_market_order(
            "ENA",
            false,         // is_buy = false
            50.0,          // size
            0.05,          // 5% slippage
            false,         // reduce_only = false
            Some(bid),
            Some(ask),
        )
        .await?;

    info!("✓ Short position opened: {:?}", short_open_result);
    info!("");

    // Wait 10 seconds
    info!("Waiting 10 seconds...");
    sleep(Duration::from_secs(10)).await;

    info!("");
    info!("════════════════════════════════════════════════");
    info!("  Closing Short Position");
    info!("════════════════════════════════════════════════");
    info!("Closing short position: BUY 50 ENA");
    info!("════════════════════════════════════════════════");
    info!("");

    // Close short position (buy - NOT reduce-only)
    let (bid, ask) = *prices.lock().unwrap();
    let short_close_result = trading_client
        .place_market_order(
            "ENA",
            true,          // is_buy = true
            50.0,          // size
            0.05,          // 5% slippage
            false,         // reduce_only = false (NEVER true on Hyperliquid)
            Some(bid),
            Some(ask),
        )
        .await?;

    info!("✓ Short position closed: {:?}", short_close_result);
    info!("");

    info!("════════════════════════════════════════════════");
    info!("  All tests completed successfully!");
    info!("════════════════════════════════════════════════");
    info!("Exiting in 3 seconds...");

    sleep(Duration::from_secs(3)).await;

    Ok(())
}
