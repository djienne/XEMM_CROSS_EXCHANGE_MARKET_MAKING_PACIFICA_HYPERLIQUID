/// Position Rebalancer Utility
///
/// This utility checks for position imbalances on Hyperliquid and automatically
/// rebalances them using market orders if the notional value exceeds the threshold.
///
/// Usage:
///   cargo run --example rebalancer --release
///
/// Environment variables required:
///   HL_WALLET - Hyperliquid wallet address
///   HL_PRIVATE_KEY - Hyperliquid private key
///
/// Optional command-line arguments:
///   --symbol <SYMBOL> - Symbol to check (default: all positions)
///   --threshold <USD> - Minimum notional value to rebalance (default: 13.0)
///   --dry-run - Check positions without executing rebalance

use anyhow::{Context, Result};
use colored::Colorize;
use std::env;

use xemm_rust::connector::hyperliquid::{HyperliquidCredentials, HyperliquidTrading};

const DEFAULT_THRESHOLD_USD: f64 = 13.0;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    println!("{}", "  Position Rebalancer".bright_cyan().bold());
    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    println!();

    // Parse command-line arguments
    let args: Vec<String> = env::args().collect();
    let mut target_symbol: Option<String> = None;
    let mut threshold_usd = DEFAULT_THRESHOLD_USD;
    let mut dry_run = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--symbol" => {
                if i + 1 < args.len() {
                    target_symbol = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: --symbol requires a value");
                    std::process::exit(1);
                }
            }
            "--threshold" => {
                if i + 1 < args.len() {
                    threshold_usd = args[i + 1].parse()
                        .context("Invalid threshold value")?;
                    i += 2;
                } else {
                    eprintln!("Error: --threshold requires a value");
                    std::process::exit(1);
                }
            }
            "--dry-run" => {
                dry_run = true;
                i += 1;
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                eprintln!("Usage: cargo run --example rebalancer [--symbol SYMBOL] [--threshold USD] [--dry-run]");
                std::process::exit(1);
            }
        }
    }

    println!("{} Threshold: {}", "[CONFIG]".blue().bold(), format!("${:.2}", threshold_usd).bright_white());
    if let Some(ref symbol) = target_symbol {
        println!("{} Target Symbol: {}", "[CONFIG]".blue().bold(), symbol.bright_white().bold());
    } else {
        println!("{} Target: {}", "[CONFIG]".blue().bold(), "All positions".bright_white());
    }
    if dry_run {
        println!("{} Mode: {}", "[CONFIG]".blue().bold(), "DRY RUN (no trades)".yellow().bold());
    }
    println!();

    // Load credentials
    dotenv::dotenv().ok();
    let hl_credentials = HyperliquidCredentials::from_env()
        .context("Failed to load Hyperliquid credentials")?;
    let hl_wallet = env::var("HL_WALLET")
        .context("HL_WALLET environment variable not set")?;

    println!("{} Hyperliquid wallet: {}", "[INIT]".cyan().bold(), hl_wallet.bright_white());

    // Initialize trading client
    let hl_trading = HyperliquidTrading::new(hl_credentials, false)
        .context("Failed to create Hyperliquid trading client")?;

    println!("{} {} Trading client initialized", "[INIT]".cyan().bold(), "✓".green().bold());
    println!();

    // Fetch user state (positions)
    println!("{} Fetching positions...", "[CHECK]".magenta().bold());
    let user_state = hl_trading.get_user_state(&hl_wallet).await
        .context("Failed to fetch user state")?;

    println!("{} {} Found {} position(s)",
        "[CHECK]".magenta().bold(),
        "✓".green().bold(),
        user_state.asset_positions.len()
    );
    println!();

    // Filter positions by symbol if specified
    let positions_to_check: Vec<_> = user_state.asset_positions.iter()
        .filter(|pos| {
            if let Some(ref symbol) = target_symbol {
                &pos.position.coin == symbol
            } else {
                true // Check all positions
            }
        })
        .collect();

    if positions_to_check.is_empty() {
        println!("{} No positions found", "[RESULT]".bright_green().bold());
        if let Some(ref symbol) = target_symbol {
            println!("  Symbol {} has no open position", symbol.bright_white().bold());
        }
        return Ok(());
    }

    // Check each position
    let mut rebalanced_count = 0;
    let mut total_rebalanced_notional = 0.0;

    for asset_pos in positions_to_check {
        let pos = &asset_pos.position;
        let symbol = &pos.coin;
        let position_size: f64 = pos.szi.parse().unwrap_or(0.0);
        let position_value: f64 = pos.position_value.parse().unwrap_or(0.0);
        let notional = position_value.abs();

        println!("{}", "─────────────────────────────────────────────────".bright_black());
        println!("{} {}", "Symbol:".bright_white(), symbol.bright_white().bold());
        println!("{} {} ({})",
            "Position:".bright_white(),
            format!("{:.4}", position_size).bright_white(),
            if position_size > 0.0 { "LONG".green() } else { "SHORT".red() }
        );
        println!("{} {}",
            "Notional:".bright_white(),
            format!("${:.2}", notional).cyan().bold()
        );

        if let Some(entry_px) = &pos.entry_px {
            println!("{} {}", "Entry:".bright_white(), format!("${}", entry_px).cyan());
        }

        println!("{} {}",
            "Unrealized PnL:".bright_white(),
            if pos.unrealized_pnl.starts_with('-') {
                pos.unrealized_pnl.red()
            } else {
                pos.unrealized_pnl.green()
            }
        );

        // Check if position needs rebalancing
        if position_size == 0.0 {
            println!("{} {} No position to rebalance",
                "[STATUS]".bright_blue().bold(),
                "○".bright_black()
            );
            continue;
        }

        if notional < threshold_usd {
            println!("{} {} Position below threshold (${:.2} < ${:.2})",
                "[STATUS]".bright_blue().bold(),
                "○".bright_black(),
                notional,
                threshold_usd
            );
            continue;
        }

        // Position needs rebalancing
        println!("{} {} Position EXCEEDS threshold (${:.2} > ${:.2})",
            "[REBALANCE]".bright_yellow().bold(),
            "⚠".yellow().bold(),
            notional,
            threshold_usd
        );

        if dry_run {
            println!("{} {} DRY RUN - Would close {} position of {:.4} {}",
                "[REBALANCE]".bright_yellow().bold(),
                "◉".yellow(),
                if position_size > 0.0 { "LONG" } else { "SHORT" },
                position_size.abs(),
                symbol.bright_white().bold()
            );
            rebalanced_count += 1;
            total_rebalanced_notional += notional;
            continue;
        }

        // Execute rebalance: close position with market order
        println!("{} Executing rebalance...", "[REBALANCE]".bright_yellow().bold());

        // To close a position:
        // - If LONG (positive position), SELL to close
        // - If SHORT (negative position), BUY to close
        let is_buy = position_size < 0.0;
        let close_size = position_size.abs();

        // Get current market price for display
        let (current_bid, current_ask) = match hl_trading.get_l2_snapshot(symbol).await {
            Ok(Some((bid, ask))) => (Some(bid), Some(ask)),
            _ => (None, None),
        };

        if let (Some(bid), Some(ask)) = (current_bid, current_ask) {
            println!("  Current market: bid=${:.4}, ask=${:.4}", bid, ask);
        }

        match hl_trading.place_market_order(
            symbol,
            is_buy,
            close_size,
            0.05, // 5% slippage tolerance
            false, // not reduce_only (we want to fully close)
            current_bid,
            current_ask,
        ).await {
            Ok(response) => {
                println!("{} {} Successfully closed {} position",
                    "[REBALANCE]".bright_yellow().bold(),
                    "✓".green().bold(),
                    if is_buy { "SHORT" } else { "LONG" }
                );

                // Extract fill price from response
                if let Some(status) = response.response.data.statuses.first() {
                    match status {
                        xemm_rust::connector::hyperliquid::OrderStatus::Filled { filled } => {
                            let avg_px: f64 = filled.avgPx.parse().unwrap_or(0.0);
                            let total_sz: f64 = filled.totalSz.parse().unwrap_or(0.0);
                            println!("  Fill: {} {} @ ${:.4}",
                                if is_buy { "BOUGHT".green() } else { "SOLD".red() },
                                format!("{:.4}", total_sz).bright_white(),
                                avg_px
                            );
                        }
                        xemm_rust::connector::hyperliquid::OrderStatus::Error { error } => {
                            println!("{} {} Order failed: {}",
                                "[REBALANCE]".bright_yellow().bold(),
                                "✗".red().bold(),
                                error.red()
                            );
                        }
                        _ => {}
                    }
                }

                rebalanced_count += 1;
                total_rebalanced_notional += notional;
            }
            Err(e) => {
                println!("{} {} Failed to close position: {}",
                    "[REBALANCE]".bright_yellow().bold(),
                    "✗".red().bold(),
                    e.to_string().red()
                );
            }
        }
    }

    // Summary
    println!();
    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    println!("{}", "  Rebalancing Summary".bright_cyan().bold());
    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    println!();

    if rebalanced_count == 0 {
        println!("{} {} No positions required rebalancing",
            "[SUMMARY]".bright_green().bold(),
            "✓".green().bold()
        );
    } else {
        println!("{} {} Rebalanced {} position(s)",
            "[SUMMARY]".bright_green().bold(),
            "✓".green().bold(),
            rebalanced_count
        );
        println!("  Total notional: {}",
            format!("${:.2}", total_rebalanced_notional).cyan().bold()
        );

        if dry_run {
            println!();
            println!("{} This was a DRY RUN - no actual trades were executed",
                "⚠".yellow().bold()
            );
            println!("  Run without --dry-run to execute rebalancing");
        }
    }

    Ok(())
}
