//! Integration tests for fill detection mechanisms
//!
//! These tests verify the various fill detection layers work correctly.
//! Run with: cargo test --features integration-tests fill_detection -- --test-threads=1

use super::helpers::*;
use anyhow::Result;
use std::time::Duration;
use tracing::info;
use xemm_rust::connector::pacifica::PacificaTradingClient;
use xemm_rust::strategy::OrderSide;

/// Test REST-based fill detection by polling open orders
#[tokio::test]
async fn test_rest_fill_detection_polling() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let pac = create_pacifica_client()?;

    // Get market info
    let markets = pac.get_market_info().await?;
    let market = &markets[&config.symbol];
    let tick_size: f64 = market.tick_size.parse()?;
    let lot_size: f64 = market.lot_size.parse()?;

    // Get current prices
    let (best_bid, best_ask) = pac
        .get_best_bid_ask_rest(&config.symbol, DEFAULT_AGG_LEVEL)
        .await?
        .expect("No best bid/ask");

    // Place an order far from market (won't fill)
    let limit_price = safe_limit_price(best_bid, best_ask, true, tick_size);
    let size = (MIN_TEST_NOTIONAL_USD / limit_price / lot_size).ceil() * lot_size;

    info!("[TEST] Placing test order for REST polling verification");
    let order = pac
        .place_limit_order(
            &config.symbol,
            OrderSide::Buy,
            size,
            Some(limit_price),
            tick_size,
            None,
            None,
        )
        .await?;

    let cloid = order.client_order_id.clone()
        .expect("Order should have client_order_id");

    // Poll open orders and verify we can detect the order
    let mut found_count = 0;
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let open_orders = pac.get_open_orders().await?;
        if open_orders.iter().any(|o| o.client_order_id == cloid) {
            found_count += 1;
        }
    }

    info!(
        "[TEST] REST polling found order {} times out of 5 polls",
        found_count
    );
    assert!(
        found_count >= 3,
        "REST polling should consistently find the order"
    );

    // Cleanup
    pac.cancel_order(&config.symbol, &cloid).await?;

    Ok(())
}

/// Test position monitoring infrastructure
#[tokio::test]
async fn test_position_monitor_polling() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let pac = create_pacifica_client()?;

    // Get current positions
    let positions = pac.get_positions().await?;

    // Find position for the configured symbol
    let position = positions.iter().find(|p| p.symbol == config.symbol);

    if let Some(pos) = position {
        info!(
            "[TEST] Current {} position: size={}, entry_price={}",
            config.symbol, pos.amount, pos.entry_price
        );
    } else {
        info!("[TEST] No current position for {}", config.symbol);
    }

    // Verify we can successfully poll positions multiple times
    let mut successful_polls = 0;

    for _ in 0..5 {
        let result = pac.get_positions().await;
        if result.is_ok() {
            successful_polls += 1;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    info!(
        "[TEST] Position polling: {}/5 successful",
        successful_polls
    );

    assert!(
        successful_polls >= 4,
        "Position polling should be reliable"
    );

    Ok(())
}

/// Test trade history polling for fill verification
#[tokio::test]
async fn test_trade_history_polling() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let pac = create_pacifica_client()?;

    // Get recent trade history (symbol, limit, start_time, end_time)
    let history = pac.get_trade_history(Some(&config.symbol), Some(10), None, None).await?;

    info!(
        "[TEST] Fetched {} recent trades for {}",
        history.len(),
        config.symbol
    );

    // Log the most recent trades
    for trade in history.iter().take(3) {
        info!(
            "[TEST] Recent trade: {} {} @ {} (created_at={})",
            trade.side, trade.amount, trade.entry_price, trade.created_at
        );
    }

    Ok(())
}

/// Test order status transition detection
#[tokio::test]
async fn test_order_status_transition_detection() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let pac = create_pacifica_client()?;

    // Get market info
    let markets = pac.get_market_info().await?;
    let market = &markets[&config.symbol];
    let tick_size: f64 = market.tick_size.parse()?;
    let lot_size: f64 = market.lot_size.parse()?;

    // Get current prices
    let (best_bid, best_ask) = pac
        .get_best_bid_ask_rest(&config.symbol, DEFAULT_AGG_LEVEL)
        .await?
        .expect("No best bid/ask");

    // Place an order
    let limit_price = safe_limit_price(best_bid, best_ask, true, tick_size);
    let size = (MIN_TEST_NOTIONAL_USD / limit_price / lot_size).ceil() * lot_size;

    let order = pac
        .place_limit_order(
            &config.symbol,
            OrderSide::Buy,
            size,
            Some(limit_price),
            tick_size,
            None,
            None,
        )
        .await?;

    let cloid = order.client_order_id.clone()
        .expect("Order should have client_order_id");

    // Verify order is open
    tokio::time::sleep(Duration::from_millis(300)).await;
    let open_orders = pac.get_open_orders().await?;
    let is_open = open_orders.iter().any(|o| o.client_order_id == cloid);
    assert!(is_open, "Order should be open");
    info!("[TEST] Order status: OPEN");

    // Cancel the order
    pac.cancel_order(&config.symbol, &cloid).await?;

    // Verify order is no longer open
    tokio::time::sleep(Duration::from_millis(300)).await;
    let open_orders = pac.get_open_orders().await?;
    let still_open = open_orders.iter().any(|o| o.client_order_id == cloid);
    assert!(!still_open, "Order should no longer be open");
    info!("[TEST] Order status: CANCELLED");

    info!("[TEST] Successfully detected order status transition: OPEN -> CANCELLED");

    Ok(())
}
