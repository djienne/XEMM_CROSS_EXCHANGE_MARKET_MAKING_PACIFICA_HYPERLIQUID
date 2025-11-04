/// Test script to fetch recent PUMP trades from both exchanges
///
/// This demonstrates the trade history fetching logic used in the bot's
/// profit calculation after a hedge execution.

use anyhow::{Context, Result};
use colored::Colorize;

use xemm_rust::connector::pacifica::{PacificaCredentials, PacificaTrading};
use xemm_rust::connector::hyperliquid::{HyperliquidCredentials, HyperliquidTrading};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    println!("{}", "  Fetch PUMP Trades Test".bright_cyan().bold());
    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    println!();

    let symbol = "PUMP";

    // Load credentials
    let pacifica_creds = PacificaCredentials::from_env()
        .context("Failed to load Pacifica credentials")?;
    let hyperliquid_creds = HyperliquidCredentials::from_env()
        .context("Failed to load Hyperliquid credentials")?;

    println!("{} {} Credentials loaded", "✓".green().bold(), "Pacifica");
    println!("{} {} Credentials loaded", "✓".green().bold(), "Hyperliquid");
    println!();

    // Initialize trading clients
    let pacifica_trading = PacificaTrading::new(pacifica_creds.clone());
    let hyperliquid_trading = HyperliquidTrading::new(hyperliquid_creds.clone(), false)?;

    // ═══════════════════════════════════════════════════
    // Fetch Pacifica Trade History
    // ═══════════════════════════════════════════════════

    println!("{}", "─────────────────────────────────────────────────".bright_white());
    println!("{} {}", "[PACIFICA]".magenta().bold(), "Fetching recent trades...");
    println!();

    match pacifica_trading.get_trade_history(Some(symbol), Some(10), None, None).await {
        Ok(trades) => {
            if trades.is_empty() {
                println!("{} No Pacifica trades found for {}", "⚠".yellow().bold(), symbol.bright_white().bold());
            } else {
                println!("{} Found {} Pacifica trade(s)", "✓".green().bold(), trades.len());
                println!();

                for (idx, trade) in trades.iter().enumerate() {
                    let side_colored = if trade.side.to_lowercase() == "buy" || trade.side.to_lowercase() == "bid" {
                        trade.side.green()
                    } else {
                        trade.side.red()
                    };

                    println!("  Trade #{}", idx + 1);
                    println!("    Symbol:           {}", trade.symbol.bright_white().bold());
                    println!("    Side:             {}", side_colored);
                    println!("    Price:            {}", format!("${}", trade.entry_price).cyan());
                    println!("    Amount:           {}", trade.amount.bright_white());
                    println!("    Fee:              {}", format!("${}", trade.fee).yellow());
                    println!("    Client Order ID:  {}...{}",
                        &trade.client_order_id[..8],
                        &trade.client_order_id[trade.client_order_id.len()-4..]
                    );

                    // Convert timestamp to readable format
                    let dt = chrono::DateTime::from_timestamp_millis(trade.created_at as i64);
                    let time_str = dt.map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| trade.created_at.to_string());
                    println!("    Time:             {}", time_str);
                    println!();
                }

                // Show most recent trade details
                if let Some(latest) = trades.first() {
                    println!("{}", "Most Recent Pacifica Trade:".bright_yellow().bold());

                    let price: f64 = latest.entry_price.parse().unwrap_or(0.0);
                    let amount: f64 = latest.amount.parse().unwrap_or(0.0);
                    let fee: f64 = latest.fee.parse().unwrap_or(0.0);
                    let notional = price * amount;
                    let fee_bps = if notional > 0.0 { (fee / notional) * 10000.0 } else { 0.0 };

                    println!("  Price:    {} (${:.6})", format!("${}", latest.entry_price).cyan().bold(), price);
                    println!("  Amount:   {} {}", latest.amount.bright_white().bold(), symbol);
                    println!("  Notional: {}", format!("${:.4}", notional).bright_white());
                    println!("  Fee:      {} ({:.2} bps)", format!("${:.6}", fee).yellow().bold(), fee_bps);
                }
            }
        }
        Err(e) => {
            println!("{} Failed to fetch Pacifica trades: {}", "✗".red().bold(), e);
        }
    }

    println!();

    // ═══════════════════════════════════════════════════
    // Fetch Hyperliquid User Fills
    // ═══════════════════════════════════════════════════

    println!("{}", "─────────────────────────────────────────────────".bright_white());
    println!("{} {}", "[HYPERLIQUID]".magenta().bold(), "Fetching recent fills...");
    println!();

    let hl_wallet = std::env::var("HL_WALLET").unwrap_or_default();

    match hyperliquid_trading.get_user_fills(&hl_wallet, true).await {
        Ok(fills) => {
            // Filter to PUMP symbol
            let pump_fills: Vec<_> = fills.iter()
                .filter(|f| f.coin == symbol)
                .take(10)
                .collect();

            if pump_fills.is_empty() {
                println!("{} No Hyperliquid fills found for {}", "⚠".yellow().bold(), symbol.bright_white().bold());
            } else {
                println!("{} Found {} Hyperliquid fill(s) for {}",
                    "✓".green().bold(),
                    pump_fills.len(),
                    symbol.bright_white().bold()
                );
                println!();

                for (idx, fill) in pump_fills.iter().enumerate() {
                    let side_colored = if fill.side.to_lowercase() == "b" {
                        "BUY".green()
                    } else {
                        "SELL".red()
                    };

                    // Convert timestamp to readable format
                    let timestamp_ms = fill.time;
                    let dt = chrono::DateTime::from_timestamp_millis(timestamp_ms as i64);
                    let time_str = dt.map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| timestamp_ms.to_string());

                    println!("  Fill #{}", idx + 1);
                    println!("    Coin:     {}", fill.coin.bright_white().bold());
                    println!("    Side:     {}", side_colored);
                    println!("    Price:    {}", format!("${}", fill.px).cyan());
                    println!("    Size:     {}", fill.sz.bright_white());
                    println!("    Fee:      {}", format!("${}", fill.fee).yellow());
                    println!("    Time:     {}", time_str);
                    println!();
                }

                // Show most recent fill details
                if let Some(latest) = pump_fills.first() {
                    println!("{}", "Most Recent Hyperliquid Fill:".bright_yellow().bold());

                    let price: f64 = latest.px.parse().unwrap_or(0.0);
                    let size: f64 = latest.sz.parse().unwrap_or(0.0);
                    let fee: f64 = latest.fee.parse().unwrap_or(0.0);
                    let notional = price * size;
                    let fee_bps = if notional > 0.0 { (fee / notional) * 10000.0 } else { 0.0 };

                    println!("  Price:    {} (${:.6})", format!("${}", latest.px).cyan().bold(), price);
                    println!("  Size:     {} {}", latest.sz.bright_white().bold(), symbol);
                    println!("  Notional: {}", format!("${:.4}", notional).bright_white());
                    println!("  Fee:      {} ({:.2} bps)", format!("${:.6}", fee).yellow().bold(), fee_bps);

                    // Calculate time since fill
                    let now = chrono::Utc::now().timestamp_millis() as u64;
                    let age_ms = now.saturating_sub(latest.time);
                    let age_secs = age_ms as f64 / 1000.0;
                    println!("  Age:      {:.1} seconds ago", age_secs);
                }
            }
        }
        Err(e) => {
            println!("{} Failed to fetch Hyperliquid fills: {}", "✗".red().bold(), e);
        }
    }

    println!();
    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());

    Ok(())
}
