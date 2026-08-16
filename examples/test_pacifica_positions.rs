/// Test Pacifica Positions API
///
/// This example tests fetching current positions from Pacifica.
///
/// Usage:
/// ```bash
/// cargo run --example test_pacifica_positions --release
/// ```
///
/// Environment variables required:
/// - PACIFICA_API_KEY
/// - PACIFICA_SECRET_KEY
/// - PACIFICA_ACCOUNT

use anyhow::{Context, Result};
use colored::Colorize;
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

    println!("{}", "═".repeat(60).bright_cyan());
    println!("{}", "  Test Pacifica Positions API".bright_white().bold());
    println!("{}", "═".repeat(60).bright_cyan());
    println!();

    // Load credentials from environment
    let credentials = PacificaCredentials::from_env()
        .context("Failed to load Pacifica credentials from .env")?;

    println!("{} {}: {}",
        "[INFO]".cyan().bold(),
        "Account".bright_white(),
        credentials.account.yellow()
    );
    println!();

    // Create trading client
    let trading_client = PacificaTrading::new(credentials).unwrap();

    // Fetch current positions
    println!("{} Fetching positions...", "→".cyan().bold());
    let positions = trading_client.get_positions().await
        .context("Failed to fetch positions")?;

    if positions.is_empty() {
        println!("{} {}",
            "✓".green().bold(),
            "No open positions".bright_white()
        );
    } else {
        println!("{} Found {} position(s):\n",
            "✓".green().bold(),
            positions.len().to_string().bright_white().bold()
        );

        for pos in &positions {
            let side_display = if pos.side == "bid" {
                "LONG".green().bold()
            } else {
                "SHORT".red().bold()
            };

            let amount: f64 = pos.amount.parse().unwrap_or(0.0);
            let entry: f64 = pos.entry_price.parse().unwrap_or(0.0);
            let funding: f64 = pos.funding.parse().unwrap_or(0.0);
            let notional = amount * entry;

            println!("  {} {}",
                pos.symbol.bright_white().bold(),
                side_display
            );
            println!("    Size:     {}", amount.to_string().bright_white());
            println!("    Entry:    ${:.4}", entry.to_string().cyan());
            println!("    Notional: ${:.2}", notional.to_string().bright_white());
            println!("    Funding:  ${:.4}", funding.to_string().yellow());
            println!("    Isolated: {}", pos.isolated);
            println!();
        }

        // Summary
        let total_notional: f64 = positions.iter()
            .map(|p| {
                let amount: f64 = p.amount.parse().unwrap_or(0.0);
                let entry: f64 = p.entry_price.parse().unwrap_or(0.0);
                amount * entry
            })
            .sum();

        println!("{}", "─".repeat(40).bright_black());
        println!("  Total Notional: ${:.2}", total_notional.to_string().bright_white().bold());
    }

    println!();
    println!("{} Test completed successfully", "✓".green().bold());

    Ok(())
}
