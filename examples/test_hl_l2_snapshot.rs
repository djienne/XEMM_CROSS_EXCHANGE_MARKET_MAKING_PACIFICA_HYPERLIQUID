/// Test Hyperliquid L2 snapshot REST API
///
/// This example tests fetching orderbook data via Hyperliquid's info endpoint.
/// Verifies that the REST API method works correctly for getting bid/ask prices.
///
/// Run with: cargo run --example test_hl_l2_snapshot --release

use anyhow::{Context, Result};
use xemm_rust::connector::hyperliquid::{HyperliquidCredentials, HyperliquidTrading};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("═══════════════════════════════════════════════════");
    println!("  Hyperliquid L2 Snapshot REST API Test");
    println!("═══════════════════════════════════════════════════");
    println!();

    // Load credentials
    dotenv::dotenv().ok();
    let hyperliquid_credentials = HyperliquidCredentials::from_env()
        .context("Failed to load Hyperliquid credentials from environment")?;

    // Initialize trading client
    let hl_trading = HyperliquidTrading::new(hyperliquid_credentials, false)
        .context("Failed to create Hyperliquid trading client")?;

    println!("✓ Trading client initialized");
    println!();

    // Test symbols to fetch
    let test_symbols = vec!["BTC", "ETH", "SOL", "PUMP", "ENA"];

    for symbol in test_symbols {
        println!("Fetching L2 snapshot for {}...", symbol);

        match hl_trading.get_l2_snapshot(symbol).await {
            Ok(Some((bid, ask))) => {
                let mid = (bid + ask) / 2.0;
                let spread = ask - bid;
                let spread_bps = (spread / mid) * 10000.0;

                println!("  ✓ Success:");
                println!("    Bid:    ${:.6}", bid);
                println!("    Ask:    ${:.6}", ask);
                println!("    Mid:    ${:.6}", mid);
                println!("    Spread: ${:.6} ({:.2} bps)", spread, spread_bps);
            }
            Ok(None) => {
                println!("  ⚠ No orderbook data available");
            }
            Err(e) => {
                println!("  ✗ Error: {}", e);
            }
        }

        println!();

        // Small delay between requests to avoid rate limits
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    println!("═══════════════════════════════════════════════════");
    println!("  Test Complete!");
    println!("═══════════════════════════════════════════════════");

    Ok(())
}
