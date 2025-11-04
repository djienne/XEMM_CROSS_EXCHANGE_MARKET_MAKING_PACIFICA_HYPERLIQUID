use anyhow::Result;
use xemm_rust::connector::pacifica::{PacificaCredentials, PacificaTrading};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("=== Pacifica Trade History Test ===\n");

    // Load credentials
    dotenv::dotenv().ok();
    let credentials = PacificaCredentials::from_env()?;

    println!("Account: {}", credentials.account);
    println!();

    // Create trading client
    let trading = PacificaTrading::new(credentials);

    // Get recent trade history for ENA
    println!("Fetching recent ENA trade history...");
    let trades = trading.get_trade_history(
        Some("ENA"),  // symbol
        Some(10),     // limit to 10 most recent trades
        None,         // no start time filter
        None,         // no end time filter
    ).await?;

    println!("Retrieved {} trade(s)\n", trades.len());

    // Display trades
    for (i, trade) in trades.iter().enumerate() {
        println!("--- Trade {} ---", i + 1);
        println!("  History ID: {}", trade.history_id);
        println!("  Order ID: {}", trade.order_id);
        println!("  Symbol: {}", trade.symbol);
        println!("  Side: {}", trade.side);
        println!("  Event Type: {}", trade.event_type);
        println!("  Amount: {} {}", trade.amount, trade.symbol);
        println!("  Fill Price: ${}", trade.entry_price);
        println!("  Market Price: ${}", trade.price);

        // Calculate notional value
        let amount: f64 = trade.amount.parse().unwrap_or(0.0);
        let price: f64 = trade.entry_price.parse().unwrap_or(0.0);
        let notional = amount * price;
        println!("  Notional: ${:.2}", notional);

        // Display fee with percentage
        let fee: f64 = trade.fee.parse().unwrap_or(0.0);
        let fee_bps = if notional > 0.0 { (fee / notional) * 10000.0 } else { 0.0 };
        println!("  Fee: ${} ({:.2} bps)", trade.fee, fee_bps);

        println!("  PnL: ${}", trade.pnl);

        // Convert timestamp to readable format
        let datetime = chrono::DateTime::from_timestamp_millis(trade.created_at as i64)
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S UTC");
        println!("  Time: {}", datetime);
        println!("  Cause: {}", trade.cause);
        println!();
    }

    // Get all recent trades (no symbol filter)
    println!("\nFetching all recent trades (limit 5)...");
    let all_trades = trading.get_trade_history(
        None,     // all symbols
        Some(5),  // limit to 5
        None,
        None,
    ).await?;

    println!("Retrieved {} trade(s) across all symbols\n", all_trades.len());

    for trade in all_trades.iter() {
        let amount: f64 = trade.amount.parse().unwrap_or(0.0);
        let price: f64 = trade.entry_price.parse().unwrap_or(0.0);
        let notional = amount * price;
        let fee: f64 = trade.fee.parse().unwrap_or(0.0);
        let fee_bps = if notional > 0.0 { (fee / notional) * 10000.0 } else { 0.0 };

        println!("  {} - {} {} @ ${} ({}) | Fee: ${} ({:.2} bps)",
            trade.symbol,
            trade.side,
            trade.amount,
            trade.entry_price,
            trade.event_type,
            trade.fee,
            fee_bps
        );
    }

    Ok(())
}
