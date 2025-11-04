use anyhow::Result;
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

    println!("=== Hyperliquid Trade History Test ===\n");

    // Load credentials
    dotenv::dotenv().ok();
    let credentials = HyperliquidCredentials::from_env()?;
    let wallet_address = std::env::var("HL_WALLET")?;

    println!("Wallet: {}", wallet_address);
    println!();

    // Create trading client (mainnet)
    let trading = HyperliquidTrading::new(credentials, false)?;

    // Get user fills (with aggregation enabled)
    println!("Fetching user fills (aggregated by time)...");
    let fills = trading.get_user_fills(&wallet_address, true).await?;

    println!("Retrieved {} fill(s)\n", fills.len());

    // Display fills (show up to 10 most recent)
    for (i, fill) in fills.iter().take(10).enumerate() {
        println!("--- Fill {} ---", i + 1);
        println!("  Coin: {}", fill.coin);
        println!("  Direction: {}", fill.dir);
        println!("  Side: {}", fill.side);
        println!("  Size: {} {}", fill.sz, fill.coin);
        println!("  Price: ${}", fill.px);

        // Calculate notional value
        let size: f64 = fill.sz.parse().unwrap_or(0.0);
        let price: f64 = fill.px.parse().unwrap_or(0.0);
        let notional = size * price;
        println!("  Notional: ${:.2}", notional);

        // Display fee with percentage
        let fee: f64 = fill.fee.parse().unwrap_or(0.0);
        let fee_bps = if notional > 0.0 { (fee / notional) * 10000.0 } else { 0.0 };
        println!("  Fee: {} {} ({:.2} bps)", fill.fee, fill.fee_token, fee_bps);

        if let Some(builder_fee) = &fill.builder_fee {
            let builder_fee_val: f64 = builder_fee.parse().unwrap_or(0.0);
            let builder_fee_bps = if notional > 0.0 { (builder_fee_val / notional) * 10000.0 } else { 0.0 };
            println!("  Builder Fee: {} {} ({:.2} bps)", builder_fee, fill.fee_token, builder_fee_bps);
        }

        // Convert timestamp to readable format
        let datetime = chrono::DateTime::from_timestamp_millis(fill.time as i64)
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S UTC");
        println!("  Time: {}", datetime);

        println!("  Order ID: {}", fill.oid);
        println!("  Trade ID: {}", fill.tid);
        println!("  Crossed: {}", fill.crossed);
        println!("  Closed PnL: ${}", fill.closed_pnl);
        println!("  Start Position: {}", fill.start_position);

        println!("  Hash: {}", fill.hash);
        println!();
    }

    // Filter for ENA fills
    let ena_fills: Vec<_> = fills.iter()
        .filter(|f| f.coin == "ENA")
        .collect();

    if !ena_fills.is_empty() {
        println!("\n=== ENA Fills ({}) ===\n", ena_fills.len());
        for fill in ena_fills.iter().take(5) {
            let datetime = chrono::DateTime::from_timestamp_millis(fill.time as i64)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S");

            // Calculate fee in bps
            let size: f64 = fill.sz.parse().unwrap_or(0.0);
            let price: f64 = fill.px.parse().unwrap_or(0.0);
            let notional = size * price;
            let fee: f64 = fill.fee.parse().unwrap_or(0.0);
            let fee_bps = if notional > 0.0 { (fee / notional) * 10000.0 } else { 0.0 };

            println!("  {} - {} {} ENA @ ${} | Fee: {} {} ({:.2} bps)",
                datetime,
                fill.dir,
                fill.sz,
                fill.px,
                fill.fee,
                fill.fee_token,
                fee_bps
            );
        }
    }

    Ok(())
}
