/// Test script to fetch recent trades from both exchanges
///
/// This uses the EXACT SAME trade fetching logic as the bot's profit calculation.
/// Run with: cargo run --example fetch_recent_trades --release [SYMBOL]
///
/// Examples:
///   cargo run --example fetch_recent_trades --release PUMP
///   cargo run --example fetch_recent_trades --release ENA
///   cargo run --example fetch_recent_trades --release BTC

use anyhow::{Context, Result};
use colored::Colorize;
use std::sync::Arc;
use tokio::sync::Mutex;

use xemm_rust::connector::pacifica::{PacificaCredentials, PacificaTrading};
use xemm_rust::connector::hyperliquid::{HyperliquidCredentials, HyperliquidTrading};
use xemm_rust::trade_fetcher;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let args: Vec<String> = std::env::args().collect();
    let symbol = if args.len() > 1 {
        &args[1]
    } else {
        println!("{}", "Usage: cargo run --example fetch_recent_trades --release [SYMBOL]".yellow());
        println!("Defaulting to PUMP symbol...\n");
        "PUMP"
    };

    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    println!("{}", format!("  Fetch {} Trades Test", symbol).bright_cyan().bold());
    println!("{}", "  Using BOT's Profit Calculation Logic".bright_cyan().bold());
    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    println!();

    // Load credentials
    let pacifica_creds = PacificaCredentials::from_env()
        .context("Failed to load Pacifica credentials")?;
    let hyperliquid_creds = HyperliquidCredentials::from_env()
        .context("Failed to load Hyperliquid credentials")?;

    println!("{} {} Credentials loaded", "✓".green().bold(), "Pacifica");
    println!("{} {} Credentials loaded", "✓".green().bold(), "Hyperliquid");
    println!();

    // Initialize trading clients
    let pacifica_trading = Arc::new(Mutex::new(PacificaTrading::new(pacifica_creds.clone())));
    let hyperliquid_trading = HyperliquidTrading::new(hyperliquid_creds.clone(), false)?;

    // ═══════════════════════════════════════════════════
    // First, get the most recent client_order_id for this symbol
    // ═══════════════════════════════════════════════════

    println!("{}", "─────────────────────────────────────────────────".bright_white());
    println!("{} {}", "[PACIFICA]".magenta().bold(), "Finding most recent trade...");
    println!();

    let (client_order_id, pacifica_timestamp) = match pacifica_trading.lock().await.get_trade_history(Some(symbol), Some(5), None, None).await {
        Ok(trades) => {
            if trades.is_empty() {
                println!("{} No Pacifica trades found for {}", "⚠".yellow().bold(), symbol.bright_white().bold());
                println!("Cannot proceed without a recent trade. Try running the bot first.");
                return Ok(());
            }

            let most_recent = &trades[0];
            println!("{} Found most recent trade:", "✓".green().bold());
            println!("  Client Order ID: {}...{}",
                &most_recent.client_order_id[..8],
                &most_recent.client_order_id[most_recent.client_order_id.len()-4..]
            );
            println!("  Price:           {}", format!("${}", most_recent.entry_price).cyan());
            println!("  Amount:          {} {}", most_recent.amount.bright_white(), symbol);
            println!("  Fee:             {}", format!("${}", most_recent.fee).yellow());

            let dt = chrono::DateTime::from_timestamp_millis(most_recent.created_at as i64);
            let time_str = dt.map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| most_recent.created_at.to_string());
            println!("  Time:            {}", time_str);
            println!();

            (most_recent.client_order_id.clone(), most_recent.created_at)
        }
        Err(e) => {
            println!("{} Failed to fetch Pacifica trades: {}", "✗".red().bold(), e);
            return Ok(());
        }
    };

    // ═══════════════════════════════════════════════════
    // Test Pacifica Trade Fetcher (BOT LOGIC)
    // ═══════════════════════════════════════════════════

    println!("{}", "─────────────────────────────────────────────────".bright_white());
    println!("{} {}", "[TEST PACIFICA FETCHER]".magenta().bold(), "Using bot's fetch logic...");
    println!();

    let pac_result = trade_fetcher::fetch_pacifica_trade(
        pacifica_trading.clone(),
        symbol,
        &client_order_id,
        3, // max_attempts
        |msg| println!("  {}", msg),
    ).await;

    // Show details of matched Pacifica fills (MAKER ONLY)
    if let Ok(trades) = pacifica_trading.lock().await.get_trade_history(Some(symbol), Some(20), None, None).await {
        // First, show ALL fills with this client_order_id for debugging
        let all_matching: Vec<_> = trades.iter()
            .filter(|t| &t.client_order_id == &client_order_id)
            .collect();

        if !all_matching.is_empty() {
            println!();
            println!("  {} ALL fills with this client_order_id (before maker filter):", "DEBUG:".bright_black());
            for (i, trade) in all_matching.iter().enumerate() {
                println!("  {}   Fill {}: {} {} @ ${} - {} - {}",
                    "DEBUG:".bright_black(),
                    i + 1,
                    trade.amount,
                    symbol,
                    trade.entry_price,
                    trade.event_type.to_uppercase(),
                    trade.side
                );
            }
        }

        // Now filter to only MAKER fills
        let matching_trades: Vec<_> = trades.iter()
            .filter(|t| {
                &t.client_order_id == &client_order_id &&
                t.event_type == "fulfill_maker"
            })
            .collect();

        if !matching_trades.is_empty() {
            println!();
            println!("  {} Pacifica fills breakdown (MAKER ONLY):", "DETAIL:".bright_blue());

            let mut total_notional = 0.0;
            let mut total_size = 0.0;

            for (i, trade) in matching_trades.iter().enumerate() {
                let dt = chrono::DateTime::from_timestamp_millis(trade.created_at as i64);
                let time_str = dt.map(|d| d.format("%H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| trade.created_at.to_string());

                // Parse values from API
                let price_f64 = trade.entry_price.parse::<f64>().unwrap_or(0.0);
                let size_f64 = trade.amount.parse::<f64>().unwrap_or(0.0);
                let notional = price_f64 * size_f64;

                total_notional += notional;
                total_size += size_f64;

                println!("  {}   Fill {}: amount=\"{}\" ({})",
                    "✓".green(),
                    i + 1,
                    trade.amount,         // RAW amount string from API
                    size_f64
                );
                println!("  {}           entry_price=\"{}\" (${:.15})",
                    "✓".green(),
                    trade.entry_price,    // RAW entry_price string from API
                    price_f64
                );
                println!("  {}           notional = {} × {:.15} = ${:.15}",
                    "✓".green(),
                    size_f64,
                    price_f64,
                    notional
                );
                println!("  {}           side={}, event_type={}, fee={} ({})",
                    "✓".green(),
                    trade.side,
                    trade.event_type,
                    trade.fee,
                    time_str
                );
            }

            let manual_weighted_avg = if total_size > 0.0 {
                total_notional / total_size
            } else {
                0.0
            };

            println!();
            println!("  {} Weighted Average Calculation:", "VERIFY:".bright_magenta());
            println!("  {} Total Notional: ${:.8}", "VERIFY:".bright_magenta(), total_notional);
            println!("  {} Total Size: {}", "VERIFY:".bright_magenta(), total_size);
            println!("  {} Weighted Avg: ${:.8}", "VERIFY:".bright_magenta(), manual_weighted_avg);
        }
    }

    println!();
    println!("{}", "Pacifica Fetch Result:".bright_yellow().bold());
    match (pac_result.fill_price, pac_result.actual_fee, pac_result.total_size) {
        (Some(price), Some(fee), Some(size)) => {
            println!("  Fill Price: {} (${:.6})", format!("${}", price).cyan().bold(), price);
            println!("  Actual Fee: {} (${:.6})", format!("${}", fee).yellow().bold(), fee);
            println!("  Total Size: {} {}", size.to_string().bright_white(), symbol);

            let notional = price * size;
            let fee_bps = (fee / notional) * 10000.0;
            println!("  Fee Rate:   {:.2} bps", fee_bps);
        }
        (Some(price), Some(fee), None) => {
            println!("  Fill Price: {} (${:.6})", format!("${}", price).cyan().bold(), price);
            println!("  Actual Fee: {} (${:.6})", format!("${}", fee).yellow().bold(), fee);
            println!("  Total Size: {} (not available)", "N/A".yellow());
        }
        (Some(price), None, _) => {
            println!("  Fill Price: {} (${:.6})", format!("${}", price).cyan().bold(), price);
            println!("  Actual Fee: {} (not found)", "N/A".yellow());
        }
        (None, _, _) => {
            println!("  {} No trade data retrieved", "✗".red().bold());
        }
    }

    println!();

    // ═══════════════════════════════════════════════════
    // Test Hyperliquid Fills Fetcher (BOT LOGIC)
    // ═══════════════════════════════════════════════════

    println!("{}", "─────────────────────────────────────────────────".bright_white());
    println!("{} {}", "[TEST HYPERLIQUID FETCHER]".magenta().bold(), "Using bot's fetch logic...");
    println!();

    let hl_wallet = std::env::var("HL_WALLET").unwrap_or_default();

    // Fetch Hyperliquid fills and filter by timestamp proximity to Pacifica trade
    let pac_dt = chrono::DateTime::from_timestamp_millis(pacifica_timestamp as i64);
    let pac_time_str = pac_dt.map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| pacifica_timestamp.to_string());

    println!("  {} Retrieving Hyperliquid fills...", "INFO:".bright_blue());
    println!("  {} Pacifica trade timestamp: {} ({})",
        "INFO:".bright_blue(),
        pac_time_str,
        pacifica_timestamp
    );
    println!("  {} Matching fills within ±10 seconds of this time", "INFO:".bright_blue());
    println!();

    let hl_result = match hyperliquid_trading.get_user_fills(&hl_wallet, true).await {
        Ok(all_fills) => {
            println!("  {} Retrieved {} total fill(s)", "DEBUG:".bright_black(), all_fills.len());

            // Filter fills by symbol AND timestamp proximity (±10 seconds)
            let time_window_ms = 10000u64; // ±10 seconds
            let matching_fills: Vec<_> = all_fills.iter()
                .filter(|f| {
                    f.coin == symbol &&
                    (f.time as i64 - pacifica_timestamp as i64).abs() < time_window_ms as i64
                })
                .collect();

            if !matching_fills.is_empty() {
                println!("  {} Found {} matching Hyperliquid fill(s) within ±10s", "✓".green().bold(), matching_fills.len());

                // Show matched fills with details
                for (i, fill) in matching_fills.iter().enumerate() {
                    let dt = chrono::DateTime::from_timestamp_millis(fill.time as i64);
                    let time_str = dt.map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| fill.time.to_string());
                    let time_diff = (fill.time as i64 - pacifica_timestamp as i64) as f64 / 1000.0;
                    println!("  {}   Fill {}: {} {} @ ${} - Fee: ${} ({}, {:+.1}s)",
                        "✓".green(),
                        i + 1,
                        fill.sz,
                        symbol,
                        fill.px,
                        fill.fee,
                        time_str,
                        time_diff
                    );
                }

                // Calculate result from matching fills
                trade_fetcher::calculate_hyperliquid_fill_result(&matching_fills)
            } else {
                println!("  {} No Hyperliquid fills found within ±10s of Pacifica trade", "⚠".yellow().bold());

                // Show nearby fills for debugging
                let symbol_fills: Vec<_> = all_fills.iter()
                    .filter(|f| f.coin == symbol)
                    .take(5)
                    .collect();

                if !symbol_fills.is_empty() {
                    println!("  {} Available {} fills (may be from different cycles):", "DEBUG:".bright_black(), symbol);
                    for (i, fill) in symbol_fills.iter().enumerate() {
                        let dt = chrono::DateTime::from_timestamp_millis(fill.time as i64);
                        let time_str = dt.map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                            .unwrap_or_else(|| fill.time.to_string());
                        let time_diff = (fill.time as i64 - pacifica_timestamp as i64) as f64 / 1000.0;
                        println!("  {}   Fill {}: {} @ ${} ({}, {:+.1}s from Pacifica)",
                            "DEBUG:".bright_black(),
                            i + 1,
                            fill.sz,
                            fill.px,
                            time_str,
                            time_diff
                        );
                    }
                }

                trade_fetcher::TradeFetchResult {
                    fill_price: None,
                    actual_fee: None,
                    total_size: None,
                    total_notional: None,
                }
            }
        }
        Err(e) => {
            println!("  {} Failed to retrieve Hyperliquid fills: {}", "✗".red().bold(), e);
            trade_fetcher::TradeFetchResult {
                fill_price: None,
                actual_fee: None,
                total_size: None,
                total_notional: None,
            }
        }
    };

    println!();
    println!("{}", "Hyperliquid Fetch Result:".bright_yellow().bold());
    match (hl_result.fill_price, hl_result.actual_fee, hl_result.total_size) {
        (Some(price), Some(fee), Some(size)) => {
            println!("  Fill Price: {} (${:.6})", format!("${}", price).cyan().bold(), price);
            println!("  Actual Fee: {} (${:.6})", format!("${}", fee).yellow().bold(), fee);
            println!("  Total Size: {} {}", size.to_string().bright_white(), symbol);

            let notional = price * size;
            let fee_bps = (fee / notional) * 10000.0;
            println!("  Fee Rate:   {:.2} bps", fee_bps);
        }
        (Some(price), Some(fee), None) => {
            println!("  Fill Price: {} (${:.6})", format!("${}", price).cyan().bold(), price);
            println!("  Actual Fee: {} (${:.6})", format!("${}", fee).yellow().bold(), fee);
            println!("  Total Size: {} (not available)", "N/A".yellow());
        }
        (Some(price), None, _) => {
            println!("  Fill Price: {} (${:.6})", format!("${}", price).cyan().bold(), price);
            println!("  Actual Fee: {} (not found)", "N/A".yellow());
        }
        (None, _, _) => {
            println!("  {} No fill data retrieved", "✗".red().bold());
        }
    }

    println!();

    // ═══════════════════════════════════════════════════
    // Calculate Combined Profit (like the bot does)
    // ═══════════════════════════════════════════════════

    if let (Some(pac_notional), Some(pac_fee), Some(pac_size), Some(hl_notional), Some(hl_fee), Some(hl_size)) =
        (pac_result.total_notional, pac_result.actual_fee, pac_result.total_size,
         hl_result.total_notional, hl_result.actual_fee, hl_result.total_size)
    {
        println!("{}", "─────────────────────────────────────────────────".bright_white());
        println!("{} {}", "[PROFIT CALCULATION]".bright_green().bold(), "Combined hedge profit...");
        println!();

        // Get the side from most recent Pacifica trade to determine direction
        let pac_side = pacifica_trading.lock().await
            .get_trade_history(Some(symbol), Some(1), None, None).await
            .ok()
            .and_then(|trades| trades.first().map(|t| t.side.clone()));

        // Verify sizes match (they should for a proper hedge)
        let size_diff_pct = ((pac_size - hl_size).abs() / pac_size) * 100.0;
        if size_diff_pct > 0.1 {
            println!("  {} Size mismatch detected!", "⚠".yellow().bold());
            println!("  {} Pacifica: {} {}, Hyperliquid: {} {}",
                "⚠".yellow().bold(),
                pac_size, symbol,
                hl_size, symbol
            );
            println!("  {} This may indicate partial fills or matching issues", "⚠".yellow().bold());
            println!();
        }

        let is_pacifica_buy = pac_side.as_ref().map(|s| s == "buy").unwrap_or(false);

        // Use the shared profit calculation function (same as main bot!)
        let profit = trade_fetcher::calculate_hedge_profit(
            pac_notional,
            hl_notional,
            pac_fee,
            hl_fee,
            is_pacifica_buy,
        );

        println!("  Trade Direction: {}",
            if is_pacifica_buy {
                "BUY on Pacifica, SELL on Hyperliquid".green().bold()
            } else {
                "SELL on Pacifica, BUY on Hyperliquid".red().bold()
            });
        println!("  Size:            {} {}", pac_size.to_string().bright_white(), symbol);
        println!();
        println!("  Pacifica Notional:  {}", format!("${:.6}", pac_notional).cyan());
        println!("  Hyperliquid Notional: {}", format!("${:.6}", hl_notional).cyan());
        println!("  Notional Diff:   {}",
            if profit.gross_pnl >= 0.0 {
                format!("${:.6}", profit.gross_pnl).green()
            } else {
                format!("${:.6}", profit.gross_pnl).red()
            });
        println!();
        println!("  Gross P&L:       {}",
            if profit.gross_pnl >= 0.0 {
                format!("${:.6}", profit.gross_pnl).green()
            } else {
                format!("${:.6}", profit.gross_pnl).red()
            });
        println!("  Pacifica Fee:    {}", format!("-${:.6}", profit.pac_fee).yellow());
        println!("  Hyperliquid Fee: {}", format!("-${:.6}", profit.hl_fee).yellow());
        println!("  {}", "─".repeat(47).bright_black());
        println!("  Net Profit:      {}",
            if profit.net_profit >= 0.0 {
                format!("${:.6}", profit.net_profit).bright_green().bold()
            } else {
                format!("${:.6}", profit.net_profit).bright_red().bold()
            });
        println!("  Profit (bps):    {}",
            if profit.profit_bps >= 0.0 {
                format!("{:.2} bps", profit.profit_bps).bright_green().bold()
            } else {
                format!("{:.2} bps", profit.profit_bps).bright_red().bold()
            });

        println!();
    } else {
        println!("{}", "─────────────────────────────────────────────────".bright_white());
        println!("{} Cannot calculate combined profit - missing data from one or both exchanges", "⚠".yellow().bold());
        println!();
    }

    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    println!();
    println!("{}", "✓ This test uses the EXACT SAME code as the bot's profit calculation".green().bold());

    Ok(())
}
