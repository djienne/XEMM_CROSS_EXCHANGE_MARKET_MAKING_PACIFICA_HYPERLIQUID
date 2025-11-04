use xemm_rust::connector::pacifica::trading::PacificaTrading;
use anyhow::Result;

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
    println!("  Pacifica REST API Orderbook Test");
    println!("═══════════════════════════════════════════════════");
    println!();

    // Load credentials from environment
    // Note: Credentials are only needed for authenticated endpoints
    // For public orderbook data, credentials don't need to be valid
    let credentials = match xemm_rust::connector::pacifica::trading::PacificaCredentials::from_env() {
        Ok(creds) => creds,
        Err(_) => {
            println!("⚠️  Warning: No credentials found in .env file");
            println!("   Using dummy credentials (OK for public endpoints)");
            xemm_rust::connector::pacifica::trading::PacificaCredentials {
                account: "dummy".to_string(),
                agent_wallet: "dummy".to_string(),
                private_key: "11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111".to_string(),
            }
        }
    };

    let trading = PacificaTrading::new(credentials);

    // Test symbols to fetch
    let test_symbols = vec!["SOL", "BTC", "ETH"];
    let agg_level = 1; // Top of book

    println!("[TEST] Fetching orderbook data for {} symbols", test_symbols.len());
    println!();

    for symbol in &test_symbols {
        println!("──────────────────────────────────────────────────");
        println!("Symbol: {}", symbol);
        println!("──────────────────────────────────────────────────");

        // Test 1: Get full orderbook snapshot
        match trading.get_orderbook_rest(symbol, agg_level).await {
            Ok(snapshot) => {
                println!("✓ Orderbook snapshot retrieved");
                println!("  Bids: {} levels", snapshot.bids.len());
                println!("  Asks: {} levels", snapshot.asks.len());

                // Show top 5 bids
                if !snapshot.bids.is_empty() {
                    println!("\n  Top 5 Bids:");
                    for (i, level) in snapshot.bids.iter().take(5).enumerate() {
                        println!("    {}: ${} @ {}", i + 1, level.price, level.size);
                    }
                }

                // Show top 5 asks
                if !snapshot.asks.is_empty() {
                    println!("\n  Top 5 Asks:");
                    for (i, level) in snapshot.asks.iter().take(5).enumerate() {
                        println!("    {}: ${} @ {}", i + 1, level.price, level.size);
                    }
                }
            }
            Err(e) => {
                println!("✗ Failed to get orderbook: {}", e);
            }
        }

        println!();

        // Test 2: Get best bid/ask
        match trading.get_best_bid_ask_rest(symbol, agg_level).await {
            Ok(Some((bid, ask))) => {
                let mid = (bid + ask) / 2.0;
                let spread = ask - bid;
                let spread_bps = (spread / mid) * 10000.0;

                println!("✓ Best bid/ask retrieved");
                println!("  Best Bid: ${:.4}", bid);
                println!("  Best Ask: ${:.4}", ask);
                println!("  Mid Price: ${:.4}", mid);
                println!("  Spread: ${:.4} ({:.2} bps)", spread, spread_bps);
            }
            Ok(None) => {
                println!("✗ No bid/ask available (empty orderbook)");
            }
            Err(e) => {
                println!("✗ Failed to get bid/ask: {}", e);
            }
        }

        println!();
    }

    println!("═══════════════════════════════════════════════════");
    println!("  Test Complete!");
    println!("═══════════════════════════════════════════════════");

    Ok(())
}
