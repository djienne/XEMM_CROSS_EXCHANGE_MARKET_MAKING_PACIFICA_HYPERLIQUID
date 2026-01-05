//! Integration tests for hedge execution on Hyperliquid
//!
//! These tests verify hedge execution works correctly.
//! Run with: cargo test --features integration-tests hedge_execution -- --test-threads=1

use super::helpers::*;
use anyhow::Result;
use std::time::{Duration, Instant};
use tracing::info;

/// Test hedge latency measurement via REST
#[tokio::test]
async fn test_hedge_latency_measurement_preparation() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let hl = create_hyperliquid_client()?;

    // Measure time to fetch L2 snapshot (required before hedge)
    let start = Instant::now();
    let snapshot = hl.get_l2_snapshot(&config.symbol).await?;
    let l2_latency = start.elapsed();

    let (best_bid, best_ask) = snapshot.expect("No L2 snapshot");
    info!(
        "[TEST] L2 snapshot latency: {:?} (bid={}, ask={})",
        l2_latency, best_bid, best_ask
    );

    // Measure time to get asset info (needed for order building)
    let start = Instant::now();
    let _asset_info = hl.get_asset_info(&config.symbol).await?;
    let asset_latency = start.elapsed();

    info!("[TEST] Asset info latency: {:?}", asset_latency);

    // Total preparation latency
    let total_prep = l2_latency + asset_latency;
    info!(
        "[TEST] Total hedge preparation latency: {:?}",
        total_prep
    );

    // Preparation should be reasonably fast
    assert!(
        total_prep < Duration::from_secs(5),
        "Hedge preparation too slow"
    );

    Ok(())
}

/// Test that we can get user state for hedge verification
#[tokio::test]
async fn test_hedge_position_verification() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let hl = create_hyperliquid_client()?;
    let wallet = std::env::var("HL_WALLET")?;

    // Get user state
    let state = hl.get_user_state(&wallet).await?;

    info!(
        "[TEST] Account value: {}, withdrawable: {}",
        state.margin_summary.account_value, state.withdrawable
    );

    // Check for existing position in configured symbol
    for pos in &state.asset_positions {
        let position = &pos.position;
        if position.coin == config.symbol {
            info!(
                "[TEST] Existing {} position: size={}",
                position.coin, position.szi
            );
        }
    }

    Ok(())
}

/// Test hedge order building with different sizes
#[tokio::test]
async fn test_hedge_size_calculations() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let hl = create_hyperliquid_client()?;

    // Get current prices
    let (best_bid, _best_ask) = hl
        .get_l2_snapshot(&config.symbol)
        .await?
        .expect("No L2 snapshot");

    // Get asset info for size decimals
    let asset_info = hl.get_asset_info(&config.symbol).await?;

    // Test various notional sizes
    let test_sizes_usd = [10.0, 50.0, 100.0];

    for notional in test_sizes_usd {
        let raw_size = notional / best_bid;
        let decimals = asset_info.sz_decimals as u32;
        let factor = 10_f64.powi(decimals as i32);
        let rounded_size = (raw_size * factor).floor() / factor;

        info!(
            "[TEST] Notional ${}: raw_size={}, rounded_size={} (sz_decimals={})",
            notional, raw_size, rounded_size, decimals
        );

        assert!(rounded_size > 0.0, "Rounded size should be positive");
    }

    Ok(())
}

/// Test hedge via REST API
///
/// WARNING: This test executes real trades!
#[tokio::test]
#[ignore] // Run with: cargo test --features integration-tests -- --ignored
async fn test_hedge_market_order_via_rest() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let hl = create_hyperliquid_client()?;
    let wallet = std::env::var("HL_WALLET")?;

    // Get current position
    let state = hl.get_user_state(&wallet).await?;
    let mut initial_position = 0.0;
    for pos in &state.asset_positions {
        let position = &pos.position;
        if position.coin == config.symbol {
            initial_position = position.szi.parse().unwrap_or(0.0);
        }
    }

    info!(
        "[TEST] Initial {} position: {}",
        config.symbol, initial_position
    );

    // Get market prices
    let (best_bid, best_ask) = hl
        .get_l2_snapshot(&config.symbol)
        .await?
        .expect("No L2 snapshot");

    let size = MIN_TEST_NOTIONAL_USD / best_bid;

    info!(
        "[TEST] Executing BUY market order: {} {} (notional ~${})",
        size, config.symbol, MIN_TEST_NOTIONAL_USD
    );

    // Execute hedge BUY
    let start = Instant::now();
    let result = hl
        .place_market_order(
            &config.symbol,
            true, // BUY
            size,
            config.hyperliquid_slippage,
            false, // reduce_only
            Some(best_bid),
            Some(best_ask),
        )
        .await?;

    let hedge_latency = start.elapsed();
    info!(
        "[TEST] BUY order completed in {:?}: {:?}",
        hedge_latency, result
    );

    // Wait for settlement
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify position changed
    let state = hl.get_user_state(&wallet).await?;
    let mut new_position = 0.0;
    for pos in &state.asset_positions {
        let position = &pos.position;
        if position.coin == config.symbol {
            new_position = position.szi.parse().unwrap_or(0.0);
        }
    }

    let position_delta = new_position - initial_position;
    info!(
        "[TEST] Position delta: {} (expected ~{})",
        position_delta, size
    );

    // Close position with SELL
    let (best_bid, best_ask) = hl
        .get_l2_snapshot(&config.symbol)
        .await?
        .expect("No L2 snapshot");

    info!("[TEST] Executing SELL market order to close position");
    let start = Instant::now();
    let result = hl
        .place_market_order(
            &config.symbol,
            false, // SELL
            size,
            config.hyperliquid_slippage,
            false, // reduce_only
            Some(best_bid),
            Some(best_ask),
        )
        .await?;

    let close_latency = start.elapsed();
    info!(
        "[TEST] SELL order completed in {:?}: {:?}",
        close_latency, result
    );

    info!(
        "[TEST] Hedge latencies - Open: {:?}, Close: {:?}",
        hedge_latency, close_latency
    );

    Ok(())
}
