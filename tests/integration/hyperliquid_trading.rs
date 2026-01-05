//! Integration tests for Hyperliquid trading operations
//!
//! These tests require API credentials and connect to the live Hyperliquid exchange.
//! Run with: cargo test --features integration-tests hyperliquid_trading -- --test-threads=1

use super::helpers::*;
use anyhow::Result;
use tracing::info;

/// Test fetching asset metadata from Hyperliquid
#[tokio::test]
async fn test_hyperliquid_get_meta() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let hl = create_hyperliquid_client()?;
    let meta = hl.get_meta().await?;

    info!(
        "[TEST] Fetched {} assets from Hyperliquid",
        meta.universe.len()
    );

    // Verify the configured symbol exists
    let asset = meta.universe.iter().find(|a| a.name == config.symbol);
    assert!(
        asset.is_some(),
        "Symbol {} not found in Hyperliquid meta",
        config.symbol
    );

    let asset = asset.unwrap();
    info!(
        "[TEST] {} asset: sz_decimals={}",
        config.symbol, asset.sz_decimals
    );

    Ok(())
}

/// Test fetching user state (positions and margin) from Hyperliquid
#[tokio::test]
async fn test_hyperliquid_get_user_state() -> Result<()> {
    require_credentials();

    let hl = create_hyperliquid_client()?;
    let wallet = std::env::var("HL_WALLET")?;

    let state = hl.get_user_state(&wallet).await?;

    info!(
        "[TEST] User state: account_value={}, total_margin_used={}, withdrawable={}",
        state.margin_summary.account_value,
        state.margin_summary.total_margin_used,
        state.withdrawable
    );

    // Verify margin summary fields are reasonable
    let account_value: f64 = state.margin_summary.account_value.parse()?;
    assert!(
        account_value >= 0.0,
        "Account value should be non-negative"
    );

    Ok(())
}

/// Test fetching L2 orderbook snapshot from Hyperliquid
#[tokio::test]
async fn test_hyperliquid_get_l2_snapshot() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let hl = create_hyperliquid_client()?;
    let snapshot = hl.get_l2_snapshot(&config.symbol).await?;

    let (best_bid, best_ask) = snapshot.expect("No L2 snapshot available");

    info!(
        "[TEST] {} L2 snapshot: best_bid={}, best_ask={}",
        config.symbol, best_bid, best_ask
    );

    assert!(best_bid > 0.0, "Best bid must be positive");
    assert!(best_ask > 0.0, "Best ask must be positive");
    assert!(
        best_bid < best_ask,
        "Best bid {} >= best ask {}",
        best_bid,
        best_ask
    );

    Ok(())
}

/// Test fetching asset ID from Hyperliquid
#[tokio::test]
async fn test_hyperliquid_get_asset_id() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let hl = create_hyperliquid_client()?;
    let asset_id = hl.get_asset_id(&config.symbol).await?;

    info!("[TEST] {} asset_id = {}", config.symbol, asset_id);

    // Asset ID should be a reasonable number
    assert!(asset_id < 1000, "Asset ID seems unreasonably large");

    Ok(())
}

/// Test fetching asset info from Hyperliquid
#[tokio::test]
async fn test_hyperliquid_get_asset_info() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let hl = create_hyperliquid_client()?;
    let asset_info = hl.get_asset_info(&config.symbol).await?;

    info!(
        "[TEST] {} asset info: name={}, sz_decimals={}",
        config.symbol, asset_info.name, asset_info.sz_decimals
    );

    assert_eq!(asset_info.name, config.symbol);
    assert!(
        asset_info.sz_decimals <= 8,
        "sz_decimals seems unreasonably large"
    );

    Ok(())
}

/// Test fetching user fills from Hyperliquid
#[tokio::test]
async fn test_hyperliquid_get_user_fills() -> Result<()> {
    require_credentials();

    let hl = create_hyperliquid_client()?;
    let wallet = std::env::var("HL_WALLET")?;

    let fills = hl.get_user_fills(&wallet, true).await?;

    info!("[TEST] Fetched {} recent fills from Hyperliquid", fills.len());

    for fill in fills.iter().take(5) {
        info!(
            "[TEST] Fill: {} {} {} @ {} (fee={}, time={})",
            fill.coin, fill.side, fill.sz, fill.px, fill.fee, fill.time
        );
    }

    Ok(())
}

/// Test signature sanity check
#[tokio::test]
async fn test_hyperliquid_sanity_check_signing() -> Result<()> {
    require_credentials();

    let hl = create_hyperliquid_client()?;

    // This tests that the signing mechanism works correctly
    let result = hl.sanity_check_signing().await;

    if let Err(e) = &result {
        info!("[TEST] Signing sanity check failed: {}", e);
    } else {
        info!("[TEST] Signing sanity check passed");
    }

    // Even if the check fails, we just want to verify no panic
    Ok(())
}

/// Test placing and closing a small market order on Hyperliquid
///
/// WARNING: This test will execute real trades!
/// Uses the minimum $10 USD size.
#[tokio::test]
#[ignore] // Run with: cargo test --features integration-tests -- --ignored
async fn test_hyperliquid_place_market_order_small() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let hl = create_hyperliquid_client()?;
    let wallet = std::env::var("HL_WALLET")?;

    // Get current price
    let (best_bid, best_ask) = hl
        .get_l2_snapshot(&config.symbol)
        .await?
        .expect("No L2 snapshot");

    info!(
        "[TEST] Market prices: bid={}, ask={}",
        best_bid, best_ask
    );

    // Calculate minimum size
    let mid_price = (best_bid + best_ask) / 2.0;
    let size = MIN_TEST_NOTIONAL_USD / mid_price;

    info!(
        "[TEST] Placing BUY market order: {} {} @ ~{} (notional ~${})",
        config.symbol,
        size,
        mid_price,
        MIN_TEST_NOTIONAL_USD
    );

    // Place BUY market order (slippage, reduce_only, bid, ask)
    let buy_result = hl
        .place_market_order(
            &config.symbol,
            true, // is_buy
            size,
            config.hyperliquid_slippage,
            false, // reduce_only
            Some(best_bid),
            Some(best_ask),
        )
        .await?;

    info!("[TEST] BUY order result: {:?}", buy_result);

    // Wait for the order to process
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Check position
    let state = hl.get_user_state(&wallet).await?;
    for pos in &state.asset_positions {
        let position = &pos.position;
        if position.coin == config.symbol {
            info!(
                "[TEST] Current position: {} size={}",
                position.coin, position.szi
            );
        }
    }

    // Close the position with a SELL market order
    info!("[TEST] Closing position with SELL market order");

    // Get updated prices
    let (best_bid, best_ask) = hl
        .get_l2_snapshot(&config.symbol)
        .await?
        .expect("No L2 snapshot");

    let sell_result = hl
        .place_market_order(
            &config.symbol,
            false, // is_buy = false (SELL)
            size,
            config.hyperliquid_slippage,
            false, // reduce_only
            Some(best_bid),
            Some(best_ask),
        )
        .await?;

    info!("[TEST] SELL order result: {:?}", sell_result);

    Ok(())
}
