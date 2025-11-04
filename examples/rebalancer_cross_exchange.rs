/// Cross-Exchange Position Rebalancer Utility
///
/// This utility checks for NET position imbalances between Pacifica and Hyperliquid
/// and automatically rebalances them using market orders on Hyperliquid if the NET
/// notional value exceeds the threshold.
///
/// NET position = Hyperliquid position - Pacifica position
///
/// Usage:
///   cargo run --example rebalancer_cross_exchange --release
///
/// Environment variables required:
///   PACIFICA_API_KEY, PACIFICA_SECRET_KEY, PACIFICA_ACCOUNT
///   HL_WALLET, HL_PRIVATE_KEY
///
/// Optional command-line arguments:
///   --symbol <SYMBOL> - Symbol to check (default: all positions)
///   --threshold <USD> - Minimum notional value to rebalance (default: 13.0)
///   --dry-run - Check positions without executing rebalance

use anyhow::{Context, Result};
use colored::Colorize;
use std::collections::HashMap;
use std::env;

use xemm_rust::connector::hyperliquid::{HyperliquidCredentials, HyperliquidTrading};
use xemm_rust::connector::pacifica::{PacificaCredentials, PacificaTrading};

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
    println!("{}", "  Cross-Exchange Position Rebalancer".bright_cyan().bold());
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
                eprintln!("Usage: cargo run --example rebalancer_cross_exchange [--symbol SYMBOL] [--threshold USD] [--dry-run]");
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

    let pac_credentials = PacificaCredentials::from_env()
        .context("Failed to load Pacifica credentials")?;
    let hl_credentials = HyperliquidCredentials::from_env()
        .context("Failed to load Hyperliquid credentials")?;
    let hl_wallet = env::var("HL_WALLET")
        .context("HL_WALLET environment variable not set")?;

    println!("{} Pacifica account: {}", "[INIT]".cyan().bold(), pac_credentials.account.bright_white());
    println!("{} Hyperliquid wallet: {}", "[INIT]".cyan().bold(), hl_wallet.bright_white());

    // Initialize trading clients
    let pac_trading = PacificaTrading::new(pac_credentials);
    let hl_trading = HyperliquidTrading::new(hl_credentials, false)
        .context("Failed to create Hyperliquid trading client")?;

    println!("{} {} Trading clients initialized", "[INIT]".cyan().bold(), "✓".green().bold());
    println!();

    // Fetch positions from both exchanges
    println!("{} Fetching positions from Pacifica...", "[CHECK]".magenta().bold());
    let pac_positions = pac_trading.get_positions(target_symbol.as_deref()).await
        .context("Failed to fetch Pacifica positions")?;
    println!("{} {} Found {} Pacifica position(s)",
        "[CHECK]".magenta().bold(),
        "✓".green().bold(),
        pac_positions.len()
    );

    println!("{} Fetching positions from Hyperliquid...", "[CHECK]".magenta().bold());
    let hl_user_state = hl_trading.get_user_state(&hl_wallet).await
        .context("Failed to fetch Hyperliquid positions")?;
    println!("{} {} Found {} Hyperliquid position(s)",
        "[CHECK]".magenta().bold(),
        "✓".green().bold(),
        hl_user_state.asset_positions.len()
    );
    println!();

    // Build position maps
    let mut pac_positions_map: HashMap<String, f64> = HashMap::new();
    for pos in &pac_positions {
        let amount: f64 = pos.amount.parse().unwrap_or(0.0);
        pac_positions_map.insert(pos.symbol.clone(), amount);
    }

    let mut hl_positions_map: HashMap<String, (f64, String)> = HashMap::new(); // (szi, entry_px)
    for asset_pos in &hl_user_state.asset_positions {
        let pos = &asset_pos.position;
        let szi: f64 = pos.szi.parse().unwrap_or(0.0);
        hl_positions_map.insert(pos.coin.clone(), (szi, pos.entry_px.clone().unwrap_or_default()));
    }

    // Combine all symbols from both exchanges
    let mut all_symbols: Vec<String> = Vec::new();
    for symbol in pac_positions_map.keys() {
        if !all_symbols.contains(symbol) {
            all_symbols.push(symbol.clone());
        }
    }
    for symbol in hl_positions_map.keys() {
        if !all_symbols.contains(symbol) {
            all_symbols.push(symbol.clone());
        }
    }

    // Filter by target symbol if specified
    if let Some(ref target) = target_symbol {
        all_symbols.retain(|s| s == target);
    }

    if all_symbols.is_empty() {
        println!("{} No positions found", "[RESULT]".bright_green().bold());
        return Ok(());
    }

    // Calculate net positions and rebalance if needed
    let mut rebalanced_count = 0;
    let mut total_rebalanced_notional = 0.0;

    for symbol in &all_symbols {
        let pac_pos = *pac_positions_map.get(symbol).unwrap_or(&0.0);
        let (hl_pos, hl_entry_px) = hl_positions_map.get(symbol).cloned().unwrap_or((0.0, String::new()));

        // NET position = HL - Pacifica
        let net_pos = hl_pos - pac_pos;

        println!("{}", "─────────────────────────────────────────────────".bright_black());
        println!("{} {}", "Symbol:".bright_white(), symbol.bright_white().bold());
        println!("{} {}", "Pacifica position:".bright_white(), format!("{:.4}", pac_pos).cyan());
        println!("{} {}", "Hyperliquid position:".bright_white(), format!("{:.4}", hl_pos).cyan());
        println!("{} {} ({})",
            "NET position:".bright_white(),
            format!("{:.4}", net_pos).bright_yellow().bold(),
            if net_pos > 0.0 { "LONG".green() } else if net_pos < 0.0 { "SHORT".red() } else { "FLAT".bright_black() }
        );

        if net_pos == 0.0 {
            println!("{} {} Position is balanced", "[STATUS]".bright_blue().bold(), "✓".green().bold());
            continue;
        }

        // Get current price to calculate notional
        let (current_bid, current_ask) = match hl_trading.get_l2_snapshot(symbol).await {
            Ok(Some((bid, ask))) => (bid, ask),
            _ => {
                println!("{} {} Cannot fetch current price, skipping",
                    "[STATUS]".bright_blue().bold(),
                    "⚠".yellow().bold()
                );
                continue;
            }
        };

        let mid_price = (current_bid + current_ask) / 2.0;
        let net_notional = net_pos.abs() * mid_price;

        println!("{} {}",
            "Current mid price:".bright_white(),
            format!("${:.4}", mid_price).cyan()
        );
        println!("{} {}",
            "NET notional:".bright_white(),
            format!("${:.2}", net_notional).bright_yellow().bold()
        );

        if net_notional < threshold_usd {
            println!("{} {} NET position below threshold (${:.2} < ${:.2})",
                "[STATUS]".bright_blue().bold(),
                "○".bright_black(),
                net_notional,
                threshold_usd
            );
            continue;
        }

        // Need to rebalance
        println!("{} {} NET position EXCEEDS threshold (${:.2} > ${:.2})",
            "[REBALANCE]".bright_yellow().bold(),
            "⚠".yellow().bold(),
            net_notional,
            threshold_usd
        );

        // To rebalance NET position, trade on Hyperliquid:
        // - If NET is LONG (+), SELL on HL to reduce
        // - If NET is SHORT (-), BUY on HL to reduce
        let is_buy = net_pos < 0.0;
        let close_size = net_pos.abs();

        println!("{} Need to {} {} {} on Hyperliquid to balance",
            "[REBALANCE]".bright_yellow().bold(),
            if is_buy { "BUY".green() } else { "SELL".red() },
            format!("{:.4}", close_size).bright_white(),
            symbol.bright_white().bold()
        );

        if dry_run {
            println!("{} {} DRY RUN - Would {} {:.4} {}",
                "[REBALANCE]".bright_yellow().bold(),
                "◉".yellow(),
                if is_buy { "BUY" } else { "SELL" },
                close_size,
                symbol.bright_white().bold()
            );
            rebalanced_count += 1;
            total_rebalanced_notional += net_notional;
            continue;
        }

        // Execute rebalance
        println!("{} Executing rebalance...", "[REBALANCE]".bright_yellow().bold());

        match hl_trading.place_market_order(
            symbol,
            is_buy,
            close_size,
            0.05, // 5% slippage tolerance
            false,
            Some(current_bid),
            Some(current_ask),
        ).await {
            Ok(response) => {
                println!("{} {} Successfully rebalanced NET position",
                    "[REBALANCE]".bright_yellow().bold(),
                    "✓".green().bold()
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
                total_rebalanced_notional += net_notional;
            }
            Err(e) => {
                println!("{} {} Failed to rebalance: {}",
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
        println!("{} {} No NET positions required rebalancing",
            "[SUMMARY]".bright_green().bold(),
            "✓".green().bold()
        );
    } else {
        println!("{} {} Rebalanced {} NET position(s)",
            "[SUMMARY]".bright_green().bold(),
            "✓".green().bold(),
            rebalanced_count
        );
        println!("  Total NET notional: {}",
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
