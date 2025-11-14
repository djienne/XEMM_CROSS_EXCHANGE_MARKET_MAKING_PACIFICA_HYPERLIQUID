/// CSV trade history logging module
///
/// This module provides functionality to log completed trades to a CSV file
/// for historical tracking and analysis.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use csv::Writer;
use serde::Serialize;
use std::fs::OpenOptions;
use std::path::Path;

use crate::strategy::OrderSide;

/// Trade record for CSV logging
#[derive(Debug, Serialize)]
pub struct TradeRecord {
    /// Timestamp of the trade (ISO 8601 format)
    pub timestamp: String,
    /// End-to-end latency from fill detection to hedge execution (milliseconds)
    pub latency_ms: f64,
    /// Trading symbol (e.g., "ENA", "BTC")
    pub symbol: String,
    /// Direction on Pacifica (Buy or Sell)
    pub pacifica_side: String,
    /// Direction on Hyperliquid (opposite of Pacifica)
    pub hyperliquid_side: String,
    /// Pacifica fill price
    pub pacifica_price: f64,
    /// Pacifica fill size
    pub pacifica_size: f64,
    /// Pacifica notional value (USD)
    pub pacifica_notional: f64,
    /// Pacifica fee paid (USD)
    pub pacifica_fee: f64,
    /// Hyperliquid fill price
    pub hyperliquid_price: f64,
    /// Hyperliquid fill size
    pub hyperliquid_size: f64,
    /// Hyperliquid notional value (USD)
    pub hyperliquid_notional: f64,
    /// Hyperliquid fee paid (USD)
    pub hyperliquid_fee: f64,
    /// Total fees paid (USD)
    pub total_fees: f64,
    /// Expected profit in basis points
    pub expected_profit_bps: f64,
    /// Actual profit in basis points
    pub actual_profit_bps: f64,
    /// Actual profit in USD
    pub actual_profit_usd: f64,
    /// Gross PnL before fees (USD)
    pub gross_pnl: f64,
}

impl TradeRecord {
    /// Create a new trade record
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        timestamp: DateTime<Utc>,
        latency_ms: f64,
        symbol: String,
        pacifica_side: OrderSide,
        pacifica_price: f64,
        pacifica_size: f64,
        pacifica_notional: f64,
        pacifica_fee: f64,
        hyperliquid_price: f64,
        hyperliquid_size: f64,
        hyperliquid_notional: f64,
        hyperliquid_fee: f64,
        expected_profit_bps: f64,
        actual_profit_bps: f64,
        actual_profit_usd: f64,
    ) -> Self {
        let hyperliquid_side = match pacifica_side {
            OrderSide::Buy => "SELL",
            OrderSide::Sell => "BUY",
        };

        let gross_pnl = match pacifica_side {
            OrderSide::Buy => hyperliquid_notional - pacifica_notional,
            OrderSide::Sell => pacifica_notional - hyperliquid_notional,
        };

        let total_fees = pacifica_fee + hyperliquid_fee;

        Self {
            timestamp: timestamp.to_rfc3339(),
            latency_ms,
            symbol,
            pacifica_side: pacifica_side.as_str().to_uppercase(),
            hyperliquid_side: hyperliquid_side.to_string(),
            pacifica_price,
            pacifica_size,
            pacifica_notional,
            pacifica_fee,
            hyperliquid_price,
            hyperliquid_size,
            hyperliquid_notional,
            hyperliquid_fee,
            total_fees,
            expected_profit_bps,
            actual_profit_bps,
            actual_profit_usd,
            gross_pnl,
        }
    }
}

/// Append a trade record to the CSV file
///
/// Creates the file with headers if it doesn't exist.
/// Appends to the file if it already exists.
///
/// # Arguments
/// * `file_path` - Path to the CSV file (e.g., "trades_history.csv")
/// * `record` - Trade record to append
///
/// # Returns
/// Result indicating success or failure
pub fn log_trade(file_path: &str, record: &TradeRecord) -> Result<()> {
    let path = Path::new(file_path);
    let file_exists = path.exists();

    // Open file in append mode (create if doesn't exist)
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open CSV file: {}", file_path))?;

    let mut writer = Writer::from_writer(file);

    // Write headers only if file is new
    if !file_exists {
        writer.serialize(record)
            .context("Failed to write CSV headers")?;
    } else {
        // For existing files, don't write headers again
        writer.serialize(record)
            .context("Failed to write CSV record")?;
    }

    writer.flush()
        .context("Failed to flush CSV writer")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_trade_record_creation() {
        let record = TradeRecord::new(
            Utc::now(),
            125.5,  // latency_ms
            "ENA".to_string(),
            OrderSide::Buy,
            0.3900,
            50.0,
            19.50,
            0.0029,
            0.3920,
            50.0,
            19.60,
            0.0078,
            10.0,
            8.5,
            0.0021,
        );

        assert_eq!(record.symbol, "ENA");
        assert_eq!(record.pacifica_side, "BUY");
        assert_eq!(record.hyperliquid_side, "SELL");
        assert_eq!(record.pacifica_price, 0.3900);
        assert_eq!(record.total_fees, 0.0107);
        assert_eq!(record.latency_ms, 125.5);
    }

    #[test]
    fn test_csv_logging() {
        let test_file = "test_trades.csv";

        // Clean up any existing test file
        let _ = fs::remove_file(test_file);

        let record = TradeRecord::new(
            Utc::now(),
            89.3,  // latency_ms
            "BTC".to_string(),
            OrderSide::Sell,
            50000.0,
            0.001,
            50.0,
            0.0075,
            49950.0,
            0.001,
            49.95,
            0.02,
            10.0,
            -43.5,
            -0.0775,
        );

        // Log the trade
        let result = log_trade(test_file, &record);
        assert!(result.is_ok());

        // Verify file exists
        assert!(Path::new(test_file).exists());

        // Clean up
        let _ = fs::remove_file(test_file);
    }
}
