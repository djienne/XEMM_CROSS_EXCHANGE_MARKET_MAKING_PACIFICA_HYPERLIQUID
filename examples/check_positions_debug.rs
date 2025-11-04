/// Debug Position Checker - Shows raw API response
///
/// This utility fetches and displays the raw position data from Hyperliquid
/// to debug what values we're actually getting.

use anyhow::{Context, Result};
use std::env;

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

    println!("=== Position Debug Checker ===\n");

    // Load credentials
    dotenv::dotenv().ok();
    let hl_credentials = HyperliquidCredentials::from_env()
        .context("Failed to load Hyperliquid credentials")?;
    let hl_wallet = env::var("HL_WALLET")
        .context("HL_WALLET environment variable not set")?;

    println!("Wallet: {}\n", hl_wallet);

    // Initialize trading client
    let hl_trading = HyperliquidTrading::new(hl_credentials, false)
        .context("Failed to create Hyperliquid trading client")?;

    // Fetch user state
    let user_state = hl_trading.get_user_state(&hl_wallet).await
        .context("Failed to fetch user state")?;

    println!("Found {} position(s)\n", user_state.asset_positions.len());

    for asset_pos in &user_state.asset_positions {
        let pos = &asset_pos.position;

        println!("─────────────────────────────────────────");
        println!("Symbol: {}", pos.coin);
        println!("Raw szi: {}", pos.szi);
        println!("Raw positionValue: {}", pos.position_value);
        println!("Raw unrealizedPnl: {}", pos.unrealized_pnl);
        println!("Raw marginUsed: {}", pos.margin_used);
        println!("Entry price: {:?}", pos.entry_px);
        println!("Leverage type: {}", pos.leverage.type_);
        println!("Leverage value: {}", pos.leverage.value);

        // Parse values
        let szi: f64 = pos.szi.parse().unwrap_or(0.0);
        let position_value: f64 = pos.position_value.parse().unwrap_or(0.0);
        let entry_px: f64 = pos.entry_px.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0.0);

        println!("\nParsed values:");
        println!("  szi (signed size): {}", szi);
        println!("  position_value: {}", position_value);
        println!("  entry_px: {}", entry_px);

        // Calculate notional properly
        let abs_size = szi.abs();
        let calculated_notional = abs_size * entry_px;

        println!("\nCalculations:");
        println!("  abs(szi) * entry_px = {} * {} = ${:.2}", abs_size, entry_px, calculated_notional);
        println!("  position_value (from API) = ${:.2}", position_value.abs());
        println!("  Difference: ${:.2}", (position_value.abs() - calculated_notional).abs());
        println!();
    }

    println!("\n=== Cross Margin Summary ===");
    println!("Account Value: {}", user_state.cross_margin_summary.account_value);
    println!("Total Margin Used: {}", user_state.cross_margin_summary.total_margin_used);
    println!("Total Ntl Pos: {}", user_state.cross_margin_summary.total_ntl_pos);
    println!("Total Raw USD: {}", user_state.cross_margin_summary.total_raw_usd);

    Ok(())
}
