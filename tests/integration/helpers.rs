//! Shared test utilities for integration tests

use anyhow::{Context, Result};
use std::time::{Duration, Instant};
use xemm_rust::config::Config;
use xemm_rust::connector::pacifica::trading::{PacificaCredentials, PacificaTrading};
use xemm_rust::connector::hyperliquid::trading::{HyperliquidCredentials, HyperliquidTrading};

/// Minimum test order notional in USD
pub const MIN_TEST_NOTIONAL_USD: f64 = 10.0;

/// Default aggregation level for orderbook queries
pub const DEFAULT_AGG_LEVEL: u32 = 1;

/// Load test configuration from test_config.json
pub fn test_config() -> Result<Config> {
    Config::from_file("test_config.json")
        .context("test_config.json required for integration tests. See test_config.json.example")
}

/// Check if credentials are available in environment
pub fn credentials_available() -> bool {
    dotenv::dotenv().ok();

    // Check Pacifica credentials
    let pac_ok = std::env::var("SOL_WALLET").is_ok()
        && std::env::var("API_PUBLIC").is_ok()
        && std::env::var("API_PRIVATE").is_ok();

    // Check Hyperliquid credentials
    let hl_ok = std::env::var("HL_PRIVATE_KEY").is_ok();

    pac_ok && hl_ok
}

/// Skip test if credentials not available
pub fn require_credentials() {
    if !credentials_available() {
        panic!("Integration tests require API credentials in .env file");
    }
}

/// Create Pacifica trading client for tests (synchronous - not async)
pub fn create_pacifica_client() -> Result<PacificaTrading> {
    let credentials = PacificaCredentials::from_env()?;
    PacificaTrading::new(credentials)
}

/// Create Hyperliquid trading client for tests
pub fn create_hyperliquid_client() -> Result<HyperliquidTrading> {
    let credentials = HyperliquidCredentials::from_env()?;
    HyperliquidTrading::new(credentials, false) // mainnet
}

/// Wait for a condition to become true with timeout
pub async fn wait_for<F>(mut condition: F, timeout: Duration, poll_interval: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        tokio::time::sleep(poll_interval).await;
    }
    false
}

/// Wait for an async condition to become true with timeout
pub async fn wait_for_async<F, Fut>(mut condition: F, timeout: Duration, poll_interval: Duration) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(poll_interval).await;
    }
    false
}

/// Calculate safe limit price far from market (won't fill)
/// For buy orders: significantly below best bid
/// For sell orders: significantly above best ask
pub fn safe_limit_price(best_bid: f64, best_ask: f64, is_buy: bool, tick_size: f64) -> f64 {
    if is_buy {
        // Place buy order 10% below best bid (won't fill)
        let price = best_bid * 0.90;
        // Round down to tick size
        (price / tick_size).floor() * tick_size
    } else {
        // Place sell order 10% above best ask (won't fill)
        let price = best_ask * 1.10;
        // Round up to tick size
        (price / tick_size).ceil() * tick_size
    }
}

/// Calculate aggressive limit price near market (likely to fill)
/// For buy orders: just above best bid
/// For sell orders: just below best ask
pub fn aggressive_limit_price(best_bid: f64, best_ask: f64, is_buy: bool, tick_size: f64) -> f64 {
    if is_buy {
        // Place buy order 1-2 ticks above best bid
        best_bid + tick_size * 2.0
    } else {
        // Place sell order 1-2 ticks below best ask
        best_ask - tick_size * 2.0
    }
}

/// Format a duration for logging
pub fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();
    if secs > 0 {
        format!("{}s {}ms", secs, millis)
    } else {
        format!("{}ms", millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_limit_price_buy() {
        let best_bid = 100.0;
        let best_ask = 101.0;
        let tick_size = 0.1;

        let price = safe_limit_price(best_bid, best_ask, true, tick_size);

        // Should be ~90 (10% below bid), rounded to tick
        assert!(price < best_bid);
        assert!(price >= 89.0 && price <= 91.0);
        // Should be on tick
        assert!((price / tick_size).fract().abs() < 0.0001);
    }

    #[test]
    fn test_safe_limit_price_sell() {
        let best_bid = 100.0;
        let best_ask = 101.0;
        let tick_size = 0.1;

        let price = safe_limit_price(best_bid, best_ask, false, tick_size);

        // Should be ~111 (10% above ask), rounded to tick
        assert!(price > best_ask);
        assert!(price >= 110.0 && price <= 112.0);
        // Should be on tick
        assert!((price / tick_size).fract().abs() < 0.0001);
    }

    #[test]
    fn test_aggressive_limit_price_buy() {
        let best_bid = 100.0;
        let best_ask = 101.0;
        let tick_size = 0.1;

        let price = aggressive_limit_price(best_bid, best_ask, true, tick_size);

        // Should be slightly above best bid
        assert!(price > best_bid);
        assert!(price < best_ask);
    }

    #[test]
    fn test_aggressive_limit_price_sell() {
        let best_bid = 100.0;
        let best_ask = 101.0;
        let tick_size = 0.1;

        let price = aggressive_limit_price(best_bid, best_ask, false, tick_size);

        // Should be slightly below best ask
        assert!(price < best_ask);
        assert!(price > best_bid);
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_millis(500)), "500ms");
        assert_eq!(format_duration(Duration::from_millis(1500)), "1s 500ms");
        assert_eq!(format_duration(Duration::from_secs(5)), "5s 0ms");
    }
}
