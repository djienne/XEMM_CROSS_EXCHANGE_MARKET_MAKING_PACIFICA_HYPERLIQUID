/// CSV trade history logging module
///
/// This module provides functionality to log completed trades to a CSV file
/// for historical tracking and analysis.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use csv::WriterBuilder;
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

    // Use WriterBuilder to control header writing
    // Only write headers for new files
    let mut writer = WriterBuilder::new()
        .has_headers(!file_exists)
        .from_writer(file);

    writer.serialize(record)
        .context("Failed to write CSV record")?;

    writer.flush()
        .context("Failed to flush CSV writer")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{BufRead, BufReader};
    use tempfile::NamedTempFile;

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
        let temp_file = NamedTempFile::new().unwrap();
        let test_file = temp_file.path().to_str().unwrap();

        // Remove the file so log_trade creates it fresh
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
    }

    #[test]
    fn test_trade_record_buy_gross_pnl_calculation() {
        // BUY on Pacifica: gross_pnl = HL_notional - PAC_notional
        // Buy 100 SOL at $100 on Pacifica ($10,000), sell on HL at $100.50 ($10,050)
        // Gross PnL = $10,050 - $10,000 = $50
        let record = TradeRecord::new(
            Utc::now(),
            50.0,
            "SOL".to_string(),
            OrderSide::Buy,
            100.0,    // pacifica_price
            100.0,    // pacifica_size
            10000.0,  // pacifica_notional
            1.5,      // pacifica_fee
            100.50,   // hyperliquid_price
            100.0,    // hyperliquid_size
            10050.0,  // hyperliquid_notional
            4.0,      // hyperliquid_fee
            5.0,      // expected_profit_bps
            4.5,      // actual_profit_bps
            44.5,     // actual_profit_usd
        );

        assert_eq!(record.gross_pnl, 50.0);
        assert_eq!(record.total_fees, 5.5);
    }

    #[test]
    fn test_trade_record_sell_gross_pnl_calculation() {
        // SELL on Pacifica: gross_pnl = PAC_notional - HL_notional
        // Sell 100 SOL at $100.50 on Pacifica ($10,050), buy on HL at $100 ($10,000)
        // Gross PnL = $10,050 - $10,000 = $50
        let record = TradeRecord::new(
            Utc::now(),
            50.0,
            "SOL".to_string(),
            OrderSide::Sell,
            100.50,   // pacifica_price
            100.0,    // pacifica_size
            10050.0,  // pacifica_notional
            1.5,      // pacifica_fee
            100.0,    // hyperliquid_price
            100.0,    // hyperliquid_size
            10000.0,  // hyperliquid_notional
            4.0,      // hyperliquid_fee
            5.0,      // expected_profit_bps
            4.5,      // actual_profit_bps
            44.5,     // actual_profit_usd
        );

        assert_eq!(record.gross_pnl, 50.0);
        assert_eq!(record.total_fees, 5.5);
        assert_eq!(record.pacifica_side, "SELL");
        assert_eq!(record.hyperliquid_side, "BUY");
    }

    #[test]
    fn test_trade_record_negative_gross_pnl() {
        // Scenario: Slippage causes negative gross PnL
        // BUY on Pacifica at $101, sell on HL at $100 (unfavorable)
        let record = TradeRecord::new(
            Utc::now(),
            50.0,
            "SOL".to_string(),
            OrderSide::Buy,
            101.0,    // pacifica_price
            10.0,     // pacifica_size
            1010.0,   // pacifica_notional
            0.15,     // pacifica_fee
            100.0,    // hyperliquid_price
            10.0,     // hyperliquid_size
            1000.0,   // hyperliquid_notional
            0.4,      // hyperliquid_fee
            5.0,      // expected_profit_bps
            -100.0,   // actual_profit_bps (lost money)
            -10.55,   // actual_profit_usd
        );

        // gross_pnl = 1000 - 1010 = -10
        assert_eq!(record.gross_pnl, -10.0);
    }

    #[test]
    fn test_csv_header_format() {
        let temp_file = NamedTempFile::new().unwrap();
        let test_file = temp_file.path().to_str().unwrap();

        // Remove the file so log_trade creates it fresh with headers
        let _ = fs::remove_file(test_file);

        let record = TradeRecord::new(
            Utc::now(),
            50.0,
            "SOL".to_string(),
            OrderSide::Buy,
            100.0,
            10.0,
            1000.0,
            1.5,
            100.5,
            10.0,
            1005.0,
            4.0,
            5.0,
            4.5,
            0.5,
        );

        log_trade(test_file, &record).unwrap();

        // Read the file and check the first line contains headers
        let file = fs::File::open(test_file).unwrap();
        let reader = BufReader::new(file);
        let lines: Vec<String> = reader.lines().map(|l| l.unwrap()).collect();

        assert!(lines.len() >= 1);
        let header = &lines[0];

        // Check that header contains expected column names
        assert!(header.contains("timestamp"));
        assert!(header.contains("latency_ms"));
        assert!(header.contains("symbol"));
        assert!(header.contains("pacifica_side"));
        assert!(header.contains("hyperliquid_side"));
        assert!(header.contains("pacifica_price"));
        assert!(header.contains("pacifica_size"));
        assert!(header.contains("pacifica_notional"));
        assert!(header.contains("pacifica_fee"));
        assert!(header.contains("hyperliquid_price"));
        assert!(header.contains("hyperliquid_size"));
        assert!(header.contains("hyperliquid_notional"));
        assert!(header.contains("hyperliquid_fee"));
        assert!(header.contains("total_fees"));
        assert!(header.contains("expected_profit_bps"));
        assert!(header.contains("actual_profit_bps"));
        assert!(header.contains("actual_profit_usd"));
        assert!(header.contains("gross_pnl"));
    }

    #[test]
    fn test_csv_append_multiple_records() {
        let temp_file = NamedTempFile::new().unwrap();
        let test_file = temp_file.path().to_str().unwrap();

        // Remove the file so log_trade creates it fresh
        let _ = fs::remove_file(test_file);

        // Create and log multiple records
        for i in 0..5 {
            let record = TradeRecord::new(
                Utc::now(),
                50.0 + i as f64,
                format!("TEST{}", i),
                if i % 2 == 0 { OrderSide::Buy } else { OrderSide::Sell },
                100.0 + i as f64,
                10.0,
                1000.0 + i as f64 * 10.0,
                1.5,
                100.5 + i as f64,
                10.0,
                1005.0 + i as f64 * 10.0,
                4.0,
                5.0,
                4.5,
                0.5,
            );
            log_trade(test_file, &record).unwrap();
        }

        // Read the file and count lines
        let file = fs::File::open(test_file).unwrap();
        let reader = BufReader::new(file);
        let lines: Vec<String> = reader.lines().map(|l| l.unwrap()).collect();

        // Should have 1 header + 5 data rows = 6 lines
        assert_eq!(lines.len(), 6, "Expected 6 lines (1 header + 5 records)");

        // First line should be header
        assert!(lines[0].contains("timestamp"));

        // Data rows should contain the symbols
        assert!(lines[1].contains("TEST0"));
        assert!(lines[2].contains("TEST1"));
        assert!(lines[3].contains("TEST2"));
        assert!(lines[4].contains("TEST3"));
        assert!(lines[5].contains("TEST4"));
    }

    #[test]
    fn test_csv_data_format() {
        let temp_file = NamedTempFile::new().unwrap();
        let test_file = temp_file.path().to_str().unwrap();

        // Remove the file so log_trade creates it fresh
        let _ = fs::remove_file(test_file);

        let record = TradeRecord::new(
            Utc::now(),
            123.456,
            "ETH".to_string(),
            OrderSide::Sell,
            3500.50,
            1.5,
            5250.75,
            0.79,
            3499.00,
            1.5,
            5248.50,
            2.10,
            5.0,
            4.2,
            -3.14,
        );

        log_trade(test_file, &record).unwrap();

        let file = fs::File::open(test_file).unwrap();
        let reader = BufReader::new(file);
        let lines: Vec<String> = reader.lines().map(|l| l.unwrap()).collect();

        assert_eq!(lines.len(), 2);

        let data_line = &lines[1];
        // Verify data values are present
        assert!(data_line.contains("ETH"));
        assert!(data_line.contains("SELL"));
        assert!(data_line.contains("BUY"));
        assert!(data_line.contains("123.456"));
        assert!(data_line.contains("3500.5"));
        assert!(data_line.contains("3499"));
    }

    #[test]
    fn test_trade_record_timestamp_format() {
        let timestamp = Utc::now();
        let record = TradeRecord::new(
            timestamp,
            50.0,
            "SOL".to_string(),
            OrderSide::Buy,
            100.0,
            10.0,
            1000.0,
            1.5,
            100.5,
            10.0,
            1005.0,
            4.0,
            5.0,
            4.5,
            0.5,
        );

        // Verify timestamp is in RFC3339 format
        assert!(record.timestamp.contains("T"));
        assert!(record.timestamp.contains("+") || record.timestamp.contains("Z"));
    }

    #[test]
    fn test_trade_record_side_conversion() {
        // Test BUY -> SELL hedge
        let buy_record = TradeRecord::new(
            Utc::now(),
            50.0,
            "SOL".to_string(),
            OrderSide::Buy,
            100.0, 10.0, 1000.0, 1.5,
            100.5, 10.0, 1005.0, 4.0,
            5.0, 4.5, 0.5,
        );
        assert_eq!(buy_record.pacifica_side, "BUY");
        assert_eq!(buy_record.hyperliquid_side, "SELL");

        // Test SELL -> BUY hedge
        let sell_record = TradeRecord::new(
            Utc::now(),
            50.0,
            "SOL".to_string(),
            OrderSide::Sell,
            100.0, 10.0, 1000.0, 1.5,
            100.5, 10.0, 1005.0, 4.0,
            5.0, 4.5, 0.5,
        );
        assert_eq!(sell_record.pacifica_side, "SELL");
        assert_eq!(sell_record.hyperliquid_side, "BUY");
    }

    #[test]
    fn test_trade_record_zero_fees() {
        let record = TradeRecord::new(
            Utc::now(),
            50.0,
            "SOL".to_string(),
            OrderSide::Buy,
            100.0, 10.0, 1000.0, 0.0,  // zero pacifica fee
            100.5, 10.0, 1005.0, 0.0,  // zero hyperliquid fee
            5.0, 5.0, 5.0,
        );

        assert_eq!(record.pacifica_fee, 0.0);
        assert_eq!(record.hyperliquid_fee, 0.0);
        assert_eq!(record.total_fees, 0.0);
    }

    #[test]
    fn test_trade_record_extreme_values() {
        // Test with very large values
        let record = TradeRecord::new(
            Utc::now(),
            0.001,  // sub-millisecond latency
            "BTC".to_string(),
            OrderSide::Buy,
            100000.0,     // $100k BTC price
            100.0,        // 100 BTC
            10000000.0,   // $10M notional
            15000.0,      // $15k fee
            100100.0,     // $100.1k HL price
            100.0,
            10010000.0,   // $10.01M notional
            40000.0,      // $40k fee
            5.0,
            4.5,
            -45000.0,     // Large loss
        );

        assert_eq!(record.total_fees, 55000.0);
        assert_eq!(record.gross_pnl, 10000.0);  // 10.01M - 10M
    }

    #[test]
    fn test_log_trade_creates_directory_error() {
        // Trying to write to a non-existent directory should fail
        let result = log_trade("/nonexistent/path/trades.csv", &TradeRecord::new(
            Utc::now(), 50.0, "SOL".to_string(), OrderSide::Buy,
            100.0, 10.0, 1000.0, 1.5,
            100.5, 10.0, 1005.0, 4.0,
            5.0, 4.5, 0.5,
        ));

        assert!(result.is_err());
    }
}
