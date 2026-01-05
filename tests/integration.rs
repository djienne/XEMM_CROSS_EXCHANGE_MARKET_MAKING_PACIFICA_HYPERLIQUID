//! Integration tests requiring API credentials
//!
//! Run with: cargo test --features integration-tests --test integration -- --test-threads=1
//!
//! These tests connect to live exchange APIs and may place/cancel real orders.
//! Requires:
//! - .env file with API credentials
//! - test_config.json with test parameters
//!
//! IMPORTANT: Use --test-threads=1 to avoid rate limiting and concurrent order issues.

#![cfg(feature = "integration-tests")]

#[path = "integration/helpers.rs"]
mod helpers;

#[path = "integration/pacifica_trading.rs"]
mod pacifica_trading;

#[path = "integration/hyperliquid_trading.rs"]
mod hyperliquid_trading;

#[path = "integration/fill_detection.rs"]
mod fill_detection;

#[path = "integration/hedge_execution.rs"]
mod hedge_execution;
