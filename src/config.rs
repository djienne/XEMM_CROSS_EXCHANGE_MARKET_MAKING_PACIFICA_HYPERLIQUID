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

    /// Pacifica REST poll interval for active orders (ms).
    /// Used by REST fill detection to poll more frequently when an order is active.
    #[serde(default = "default_pacifica_active_order_rest_poll_interval")]
    pub pacifica_active_order_rest_poll_interval_ms: u64,

    /// Maximum quote age for maker opportunity evaluation.
    #[serde(default = "default_max_quote_age_ms")]
    pub max_quote_age_ms: u64,

    /// Maximum Hyperliquid quote age accepted by the hedge executor.
    #[serde(default = "default_hedge_quote_max_age_ms")]
    pub hedge_quote_max_age_ms: u64,

    /// Maximum distance from Pacifica top-of-book for maker quotes.
    #[serde(default = "default_max_quote_distance_bps")]
    pub max_quote_distance_bps: f64,

    /// Net base-unit dust tolerance for final neutral checks.
    #[serde(default = "default_neutral_dust_base")]
    pub neutral_dust_base: f64,

    /// Hyperliquid WebSocket hedge response timeout.
    #[serde(default = "default_hedge_ws_response_timeout_ms")]
    pub hedge_ws_response_timeout_ms: u64,

    /// Maximum unhedged base exposure before reconciler escalates.
    #[serde(default = "default_max_unhedged_base")]
    pub max_unhedged_base: f64,

    /// Maximum unhedged USD exposure before reconciler escalates.
    #[serde(default = "default_max_unhedged_usd")]
    pub max_unhedged_usd: f64,

    /// Maximum unhedged age before reconciler escalates.
    #[serde(default = "default_max_unhedged_ms")]
    pub max_unhedged_ms: u64,

    /// Maximum consecutive hedge failures before halting.
    #[serde(default = "default_max_consecutive_hedge_failures")]
    pub max_consecutive_hedge_failures: u32,

    /// Minimum partial-fill hedge notional.
    #[serde(default = "default_partial_hedge_min_notional_usd")]
    pub partial_hedge_min_notional_usd: f64,

    /// Minimum fraction of cumulative filled size that triggers a partial hedge.
    #[serde(default = "default_partial_hedge_min_fraction")]
    pub partial_hedge_min_fraction: f64,

    /// Idle timeout before a partial fill is hedged.
    #[serde(default = "default_partial_hedge_idle_timeout_ms")]
    pub partial_hedge_idle_timeout_ms: u64,

    /// Minimum base quantity for partial hedge emission.
    #[serde(default = "default_partial_hedge_min_base")]
    pub partial_hedge_min_base: f64,

    /// Maximum number of fill accumulators kept in memory.
    #[serde(default = "default_fill_aggregator_max_entries")]
    pub fill_aggregator_max_entries: usize,

    /// Initial hedge slippage tolerance in basis points.
    #[serde(default = "default_hedge_slippage_bps_initial")]
    pub hedge_slippage_bps_initial: f64,

    /// Retry hedge slippage tolerance in basis points.
    #[serde(default = "default_hedge_slippage_bps_retry")]
    pub hedge_slippage_bps_retry: f64,

    /// Emergency hedge slippage tolerance in basis points.
    #[serde(default = "default_hedge_slippage_bps_emergency")]
    pub hedge_slippage_bps_emergency: f64,

    /// Base retry delay for hedge attempts.
    #[serde(default = "default_hedge_retry_base_delay_ms")]
    pub hedge_retry_base_delay_ms: u64,

    /// Max retry delay for hedge attempts.
    #[serde(default = "default_hedge_retry_max_delay_ms")]
    pub hedge_retry_max_delay_ms: u64,

    /// Permit startup to continue if stale Pacifica order cancellation fails.
    #[serde(default = "default_allow_startup_with_cancel_failure")]
    pub allow_startup_with_cancel_failure: bool,
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

fn default_pacifica_active_order_rest_poll_interval() -> u64 {
    500 // 500 ms (safer than 100ms to avoid rate limits)
}

fn default_max_quote_age_ms() -> u64 {
    500
}

fn default_hedge_quote_max_age_ms() -> u64 {
    250
}

fn default_max_quote_distance_bps() -> f64 {
    50.0
}

fn default_neutral_dust_base() -> f64 {
    0.01
}

fn default_hedge_ws_response_timeout_ms() -> u64 {
    800
}

fn default_max_unhedged_base() -> f64 {
    0.05
}

fn default_max_unhedged_usd() -> f64 {
    10.0
}

fn default_max_unhedged_ms() -> u64 {
    5_000
}

fn default_max_consecutive_hedge_failures() -> u32 {
    3
}

fn default_partial_hedge_min_notional_usd() -> f64 {
    5.0
}

fn default_partial_hedge_min_fraction() -> f64 {
    0.35
}

fn default_partial_hedge_idle_timeout_ms() -> u64 {
    250
}

fn default_partial_hedge_min_base() -> f64 {
    0.0
}

fn default_fill_aggregator_max_entries() -> usize {
    10_000
}

fn default_hedge_slippage_bps_initial() -> f64 {
    10.0
}

fn default_hedge_slippage_bps_retry() -> f64 {
    25.0
}

fn default_hedge_slippage_bps_emergency() -> f64 {
    50.0
}

fn default_hedge_retry_base_delay_ms() -> u64 {
    250
}

fn default_hedge_retry_max_delay_ms() -> u64 {
    1_500
}

fn default_allow_startup_with_cancel_failure() -> bool {
    false
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
            pacifica_active_order_rest_poll_interval_ms:
                default_pacifica_active_order_rest_poll_interval(),
            max_quote_age_ms: default_max_quote_age_ms(),
            hedge_quote_max_age_ms: default_hedge_quote_max_age_ms(),
            max_quote_distance_bps: default_max_quote_distance_bps(),
            neutral_dust_base: default_neutral_dust_base(),
            hedge_ws_response_timeout_ms: default_hedge_ws_response_timeout_ms(),
            max_unhedged_base: default_max_unhedged_base(),
            max_unhedged_usd: default_max_unhedged_usd(),
            max_unhedged_ms: default_max_unhedged_ms(),
            max_consecutive_hedge_failures: default_max_consecutive_hedge_failures(),
            partial_hedge_min_notional_usd: default_partial_hedge_min_notional_usd(),
            partial_hedge_min_fraction: default_partial_hedge_min_fraction(),
            partial_hedge_idle_timeout_ms: default_partial_hedge_idle_timeout_ms(),
            partial_hedge_min_base: default_partial_hedge_min_base(),
            fill_aggregator_max_entries: default_fill_aggregator_max_entries(),
            hedge_slippage_bps_initial: default_hedge_slippage_bps_initial(),
            hedge_slippage_bps_retry: default_hedge_slippage_bps_retry(),
            hedge_slippage_bps_emergency: default_hedge_slippage_bps_emergency(),
            hedge_retry_base_delay_ms: default_hedge_retry_base_delay_ms(),
            hedge_retry_max_delay_ms: default_hedge_retry_max_delay_ms(),
            allow_startup_with_cancel_failure: default_allow_startup_with_cancel_failure(),
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
        let content = serde_json::to_string_pretty(self).context("Failed to serialize config")?;

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

        anyhow::ensure!(
            self.order_notional_usd > 0.0,
            "Order notional must be positive"
        );
        anyhow::ensure!(
            self.pacifica_maker_fee_bps.is_finite() && self.hyperliquid_taker_fee_bps.is_finite(),
            "Fees must be finite"
        );
        anyhow::ensure!(
            self.profit_rate_bps.is_finite(),
            "Profit rate must be finite"
        );
        anyhow::ensure!(
            self.profit_cancel_threshold_bps >= 0.0,
            "Profit cancel threshold cannot be negative"
        );
        anyhow::ensure!(
            self.order_refresh_interval_secs > 0,
            "Order refresh interval must be positive"
        );
        anyhow::ensure!(
            self.hyperliquid_slippage > 0.0 && self.hyperliquid_slippage < 1.0,
            "Hyperliquid slippage must be between 0 and 1"
        );
        anyhow::ensure!(
            self.pacifica_rest_poll_interval_secs > 0
                && self.pacifica_active_order_rest_poll_interval_ms > 0,
            "Poll intervals must be positive"
        );
        anyhow::ensure!(self.max_quote_age_ms > 0, "Max quote age must be positive");
        anyhow::ensure!(
            self.hedge_quote_max_age_ms > 0,
            "Hedge quote max age must be positive"
        );
        anyhow::ensure!(
            self.max_quote_distance_bps > 0.0,
            "Max quote distance must be positive"
        );
        anyhow::ensure!(
            self.neutral_dust_base >= 0.0,
            "Neutral dust cannot be negative"
        );
        anyhow::ensure!(
            self.hedge_ws_response_timeout_ms > 0,
            "Hedge WS response timeout must be positive"
        );
        anyhow::ensure!(
            self.max_unhedged_base >= 0.0 && self.max_unhedged_usd >= 0.0,
            "Max unhedged exposure limits cannot be negative"
        );
        anyhow::ensure!(self.max_unhedged_ms > 0, "Max unhedged ms must be positive");
        anyhow::ensure!(
            self.max_consecutive_hedge_failures > 0,
            "Max consecutive hedge failures must be positive"
        );
        anyhow::ensure!(
            self.partial_hedge_min_notional_usd >= 0.0
                && self.partial_hedge_min_fraction >= 0.0
                && self.partial_hedge_min_base >= 0.0,
            "Partial hedge thresholds cannot be negative"
        );
        anyhow::ensure!(
            self.partial_hedge_idle_timeout_ms > 0 && self.fill_aggregator_max_entries > 0,
            "Partial hedge idle timeout and aggregator max entries must be positive"
        );
        anyhow::ensure!(
            self.hedge_slippage_bps_initial > 0.0
                && self.hedge_slippage_bps_retry > 0.0
                && self.hedge_slippage_bps_emergency > 0.0,
            "Hedge slippage bps values must be positive"
        );
        anyhow::ensure!(
            self.hedge_retry_base_delay_ms > 0 && self.hedge_retry_max_delay_ms > 0,
            "Hedge retry delays must be positive"
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
