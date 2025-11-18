use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// Application configuration loaded from config.json
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Trading symbol (e.g., "SOL", "BTC", "ETH")
    pub symbol: String,

    /// Orderbook aggregation level (1, 2, 5, 10, 100, 1000)
    #[serde(default = "default_agg_level")]
    pub agg_level: u32,

    /// Maximum number of reconnection attempts
    #[serde(default = "default_reconnect_attempts")]
    pub reconnect_attempts: u32,

    /// Ping interval in seconds (keep below 60 to avoid timeout)
    #[serde(default = "default_ping_interval")]
    pub ping_interval_secs: u64,

    /// Low-latency mode: minimal logging and processing
    #[serde(default = "default_low_latency")]
    pub low_latency_mode: bool,

    /// Pacifica maker fee in basis points (e.g., 1.0 = 0.01%)
    #[serde(default = "default_pacifica_maker_fee")]
    pub pacifica_maker_fee_bps: f64,

    /// Hyperliquid taker fee in basis points (e.g., 2.5 = 0.025%)
    #[serde(default = "default_hyperliquid_taker_fee")]
    pub hyperliquid_taker_fee_bps: f64,

    /// Target profit rate in basis points (e.g., 10.0 = 0.1%)
    #[serde(default = "default_profit_rate")]
    pub profit_rate_bps: f64,

    /// Order notional size in USD (e.g., 20.0 = $20)
    #[serde(default = "default_order_notional")]
    pub order_notional_usd: f64,

    /// Profit cancel threshold in basis points (cancel if profit drops by this much)
    #[serde(default = "default_profit_cancel_threshold")]
    pub profit_cancel_threshold_bps: f64,

    /// Order refresh interval in seconds (cancel and replace if order is this old)
    #[serde(default = "default_order_refresh_interval")]
    pub order_refresh_interval_secs: u64,

    /// Hyperliquid slippage tolerance for market orders (e.g., 0.05 = 5%)
    #[serde(default = "default_hyperliquid_slippage")]
    pub hyperliquid_slippage: f64,

    /// Use Hyperliquid WebSocket for hedge market orders (default: true).
    /// When disabled, REST API is used for hedge execution.
    #[serde(default = "default_hyperliquid_use_ws_for_hedge")]
    pub hyperliquid_use_ws_for_hedge: bool,

    /// Pacifica REST API polling interval in seconds (complement to WebSocket)
    #[serde(default = "default_pacifica_rest_poll_interval")]
    pub pacifica_rest_poll_interval_secs: u64,
}

// Default values
fn default_agg_level() -> u32 {
    1
}

fn default_reconnect_attempts() -> u32 {
    5
}

fn default_ping_interval() -> u64 {
    15 // 15 seconds
}

fn default_low_latency() -> bool {
    false
}

fn default_pacifica_maker_fee() -> f64 {
    1.5 // 1.5 bps = 0.015%
}

fn default_hyperliquid_taker_fee() -> f64 {
    4.0 // 4 bps = 0.04%
}

fn default_profit_rate() -> f64 {
    15.0 // 15 bps = 0.15%
}

fn default_order_notional() -> f64 {
    20.0 // $20 USD
}

fn default_profit_cancel_threshold() -> f64 {
    3.0 // 3 bps = 0.03%
}

fn default_order_refresh_interval() -> u64 {
    60 // 60 seconds
}

fn default_hyperliquid_slippage() -> f64 {
    0.05 // 5%
}

fn default_hyperliquid_use_ws_for_hedge() -> bool {
    true
}

fn default_pacifica_rest_poll_interval() -> u64 {
    2 // 2 seconds
}

impl Default for Config {
    fn default() -> Self {
        Self {
            symbol: "SOL".to_string(),
            agg_level: default_agg_level(),
            reconnect_attempts: default_reconnect_attempts(),
            ping_interval_secs: default_ping_interval(),
            low_latency_mode: default_low_latency(),
            pacifica_maker_fee_bps: default_pacifica_maker_fee(),
            hyperliquid_taker_fee_bps: default_hyperliquid_taker_fee(),
            profit_rate_bps: default_profit_rate(),
            order_notional_usd: default_order_notional(),
            profit_cancel_threshold_bps: default_profit_cancel_threshold(),
            order_refresh_interval_secs: default_order_refresh_interval(),
            hyperliquid_slippage: default_hyperliquid_slippage(),
            hyperliquid_use_ws_for_hedge: default_hyperliquid_use_ws_for_hedge(),
            pacifica_rest_poll_interval_secs: default_pacifica_rest_poll_interval(),
        }
    }
}

impl Config {
    /// Load configuration from a JSON file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: Config = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        Ok(config)
    }

    /// Load configuration from default location (config.json)
    pub fn load_default() -> Result<Self> {
        Self::from_file("config.json")
    }

    /// Save configuration to a JSON file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        let content = serde_json::to_string_pretty(self)
            .context("Failed to serialize config")?;

        fs::write(path, content)
            .with_context(|| format!("Failed to write config file: {}", path.display()))?;

        Ok(())
    }

    /// Validate configuration values
    pub fn validate(&self) -> Result<()> {
        // Check symbol is not empty
        anyhow::ensure!(!self.symbol.is_empty(), "Symbol cannot be empty");

        // Check aggregation level is valid
        let valid_agg_levels = [1, 2, 5, 10, 100, 1000];
        anyhow::ensure!(
            valid_agg_levels.contains(&self.agg_level),
            "Invalid aggregation level: {}. Must be one of: 1, 2, 5, 10, 100, 1000",
            self.agg_level
        );

        // Check ping interval is reasonable
        anyhow::ensure!(
            self.ping_interval_secs > 0 && self.ping_interval_secs <= 30,
            "Ping interval must be between 1 and 30 seconds"
        );

        // Check reconnect attempts is reasonable
        anyhow::ensure!(
            self.reconnect_attempts > 0,
            "Reconnect attempts must be greater than 0"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.symbol, "SOL");
        assert_eq!(config.agg_level, 1);
        assert_eq!(config.reconnect_attempts, 5);
        assert_eq!(config.ping_interval_secs, 15);
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        // Test invalid aggregation level
        config.agg_level = 3;
        assert!(config.validate().is_err());

        // Test invalid ping interval
        config.agg_level = 1;
        config.ping_interval_secs = 0;
        assert!(config.validate().is_err());

        config.ping_interval_secs = 60;
        assert!(config.validate().is_err());
    }
}
