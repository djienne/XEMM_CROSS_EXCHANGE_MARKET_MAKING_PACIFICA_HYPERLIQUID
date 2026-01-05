//! Integration tests for Pacifica trading operations
//!
//! These tests require API credentials and connect to the live Pacifica exchange.
//! Run with: cargo test --features integration-tests pacifica_trading -- --test-threads=1

use super::helpers::*;
use anyhow::Result;
use tracing::info;
use xemm_rust::connector::pacifica::PacificaTradingClient;
use xemm_rust::strategy::OrderSide;

/// Test fetching market info from Pacifica
#[tokio::test]
async fn test_pacifica_get_market_info() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let pac = create_pacifica_client()?;
    let markets = pac.get_market_info().await?;

    info!("[TEST] Fetched {} markets from Pacifica", markets.len());

    // Verify the configured symbol exists
    assert!(
        markets.contains_key(&config.symbol),
        "Symbol {} not found in markets",
        config.symbol
    );

    let market = &markets[&config.symbol];
    info!(
        "[TEST] {} market: tick_size={}, lot_size={}",
        config.symbol, market.tick_size, market.lot_size
    );

    // Verify tick_size and lot_size are valid positive numbers
    let tick_size: f64 = market.tick_size.parse()?;
    let lot_size: f64 = market.lot_size.parse()?;

    assert!(tick_size > 0.0, "tick_size must be positive");
    assert!(lot_size > 0.0, "lot_size must be positive");

    Ok(())
}

/// Test fetching positions from Pacifica
#[tokio::test]
async fn test_pacifica_get_positions() -> Result<()> {
    require_credentials();

    let pac = create_pacifica_client()?;
    let positions = pac.get_positions().await?;

    info!("[TEST] Fetched {} positions from Pacifica", positions.len());

    // Just verify the response structure is valid
    for pos in &positions {
        info!(
            "[TEST] Position: {} {} size={} entry_price={}",
            pos.symbol, pos.side, pos.amount, pos.entry_price
        );
    }

    Ok(())
}

/// Test fetching open orders from Pacifica
#[tokio::test]
async fn test_pacifica_get_open_orders() -> Result<()> {
    require_credentials();

    let pac = create_pacifica_client()?;
    let orders = pac.get_open_orders().await?;

    info!("[TEST] Fetched {} open orders from Pacifica", orders.len());

    for order in &orders {
        info!(
            "[TEST] Open order: {} {} filled={} @ {} (cloid={})",
            order.symbol, order.side, order.filled_amount, order.price, order.client_order_id
        );
    }

    Ok(())
}

/// Test fetching orderbook via REST
#[tokio::test]
async fn test_pacifica_get_orderbook_rest() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let pac = create_pacifica_client()?;
    let book = pac.get_orderbook_rest(&config.symbol, DEFAULT_AGG_LEVEL).await?;

    info!(
        "[TEST] Orderbook for {}: {} bids, {} asks",
        config.symbol,
        book.bids.len(),
        book.asks.len()
    );

    // Should have at least some levels
    assert!(!book.bids.is_empty(), "No bids in orderbook");
    assert!(!book.asks.is_empty(), "No asks in orderbook");

    // Verify best bid < best ask
    let best_bid: f64 = book.bids[0].price.parse()?;
    let best_ask: f64 = book.asks[0].price.parse()?;

    info!("[TEST] Best bid={}, best ask={}", best_bid, best_ask);
    assert!(best_bid < best_ask, "Best bid {} >= best ask {}", best_bid, best_ask);

    Ok(())
}

/// Test fetching best bid/ask via REST
#[tokio::test]
async fn test_pacifica_get_best_bid_ask_rest() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let pac = create_pacifica_client()?;
    let result = pac.get_best_bid_ask_rest(&config.symbol, DEFAULT_AGG_LEVEL).await?;

    let (best_bid, best_ask) = result.expect("No best bid/ask available");

    info!("[TEST] {} best bid={}, best ask={}", config.symbol, best_bid, best_ask);

    assert!(best_bid > 0.0, "Best bid must be positive");
    assert!(best_ask > 0.0, "Best ask must be positive");
    assert!(best_bid < best_ask, "Best bid must be less than best ask");

    Ok(())
}

/// Test placing and cancelling a limit order on Pacifica
///
/// This test places a limit order far from market (won't fill), then cancels it.
#[tokio::test]
async fn test_pacifica_place_and_cancel_limit_order() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let pac = create_pacifica_client()?;

    // Get market info for tick/lot sizes
    let markets = pac.get_market_info().await?;
    let market = &markets[&config.symbol];
    let tick_size: f64 = market.tick_size.parse()?;
    let lot_size: f64 = market.lot_size.parse()?;

    // Get current best bid/ask
    let (best_bid, best_ask) = pac
        .get_best_bid_ask_rest(&config.symbol, DEFAULT_AGG_LEVEL)
        .await?
        .expect("No best bid/ask");
    info!("[TEST] Current market: bid={}, ask={}", best_bid, best_ask);

    // Calculate safe price and size that won't fill
    let limit_price = safe_limit_price(best_bid, best_ask, true, tick_size);
    let size = (MIN_TEST_NOTIONAL_USD / limit_price / lot_size).ceil() * lot_size;

    info!(
        "[TEST] Placing BUY limit order: {} @ {} (size={}, tick={}, lot={})",
        config.symbol, limit_price, size, tick_size, lot_size
    );

    // Place the order
    let order_response = pac
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

    let client_order_id = order_response.client_order_id.clone()
        .expect("Order response should contain client_order_id");
    info!(
        "[TEST] Order placed: cloid={}, order_id={:?}",
        client_order_id, order_response.order_id
    );

    // Verify order appears in open orders
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let open_orders = pac.get_open_orders().await?;
    let found = open_orders
        .iter()
        .any(|o| o.client_order_id == client_order_id);
    info!("[TEST] Order found in open orders: {}", found);
    assert!(found, "Order not found in open orders after placement");

    // Cancel the order
    info!("[TEST] Cancelling order: cloid={}", client_order_id);
    pac.cancel_order(&config.symbol, &client_order_id).await?;

    // Verify order is no longer in open orders
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let open_orders = pac.get_open_orders().await?;
    let still_found = open_orders
        .iter()
        .any(|o| o.client_order_id == client_order_id);

    info!("[TEST] Order still in open orders after cancel: {}", still_found);
    assert!(!still_found, "Order still in open orders after cancellation");

    Ok(())
}

/// Test cancel_all_orders on Pacifica
///
/// This test places two orders (buy and sell), then cancels all.
#[tokio::test]
async fn test_pacifica_cancel_all_orders() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let pac = create_pacifica_client()?;

    // Get market info
    let markets = pac.get_market_info().await?;
    let market = &markets[&config.symbol];
    let tick_size: f64 = market.tick_size.parse()?;
    let lot_size: f64 = market.lot_size.parse()?;

    // Get current best bid/ask
    let (best_bid, best_ask) = pac
        .get_best_bid_ask_rest(&config.symbol, DEFAULT_AGG_LEVEL)
        .await?
        .expect("No best bid/ask");

    // Place a BUY order far below market
    let buy_price = safe_limit_price(best_bid, best_ask, true, tick_size);
    let buy_size = (MIN_TEST_NOTIONAL_USD / buy_price / lot_size).ceil() * lot_size;

    info!("[TEST] Placing BUY order at {}", buy_price);
    let buy_order = pac
        .place_limit_order(
            &config.symbol,
            OrderSide::Buy,
            buy_size,
            Some(buy_price),
            tick_size,
            None,
            None,
        )
        .await?;
    let buy_cloid = buy_order.client_order_id.clone()
        .expect("Buy order should have client_order_id");

    // Place a SELL order far above market
    let sell_price = safe_limit_price(best_bid, best_ask, false, tick_size);
    let sell_size = (MIN_TEST_NOTIONAL_USD / sell_price / lot_size).ceil() * lot_size;

    info!("[TEST] Placing SELL order at {}", sell_price);
    let sell_order = pac
        .place_limit_order(
            &config.symbol,
            OrderSide::Sell,
            sell_size,
            Some(sell_price),
            tick_size,
            None,
            None,
        )
        .await?;
    let sell_cloid = sell_order.client_order_id.clone()
        .expect("Sell order should have client_order_id");

    // Verify both orders are open
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let open_orders = pac.get_open_orders().await?;
    let symbol_orders: Vec<_> = open_orders
        .iter()
        .filter(|o| o.symbol == config.symbol)
        .collect();

    info!("[TEST] Found {} open orders for {}", symbol_orders.len(), config.symbol);
    assert!(symbol_orders.len() >= 2, "Expected at least 2 orders");

    // Cancel all orders for the symbol (all_symbols=false, symbol=Some, exclude_reduce_only=false)
    info!("[TEST] Cancelling all orders for {}", config.symbol);
    pac.cancel_all_orders(false, Some(&config.symbol), false).await?;

    // Verify all orders are cancelled
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let open_orders = pac.get_open_orders().await?;
    let remaining: Vec<_> = open_orders
        .iter()
        .filter(|o| {
            o.symbol == config.symbol
                && (o.client_order_id == buy_cloid
                    || o.client_order_id == sell_cloid)
        })
        .collect();

    info!("[TEST] Remaining test orders: {}", remaining.len());
    assert!(remaining.is_empty(), "Some test orders still open after cancel_all");

    Ok(())
}

/// Test fetching trade history from Pacifica
#[tokio::test]
async fn test_pacifica_get_trade_history() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let pac = create_pacifica_client()?;
    // get_trade_history(symbol, limit, start_time, end_time)
    let history = pac.get_trade_history(Some(&config.symbol), Some(10), None, None).await?;

    info!(
        "[TEST] Fetched {} trade history items for {}",
        history.len(),
        config.symbol
    );

    for trade in history.iter().take(3) {
        info!(
            "[TEST] Trade: {} {} @ {} (fee={}, pnl={})",
            trade.side, trade.amount, trade.entry_price, trade.fee, trade.pnl
        );
    }

    Ok(())
}

/// Test order refresh flow (cancel and replace)
#[tokio::test]
async fn test_pacifica_order_refresh_flow() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let pac = create_pacifica_client()?;

    // Get market info
    let markets = pac.get_market_info().await?;
    let market = &markets[&config.symbol];
    let tick_size: f64 = market.tick_size.parse()?;
    let lot_size: f64 = market.lot_size.parse()?;

    // Get current best bid/ask
    let (best_bid, best_ask) = pac
        .get_best_bid_ask_rest(&config.symbol, DEFAULT_AGG_LEVEL)
        .await?
        .expect("No best bid/ask");

    // Place initial order
    let initial_price = safe_limit_price(best_bid, best_ask, true, tick_size);
    let size = (MIN_TEST_NOTIONAL_USD / initial_price / lot_size).ceil() * lot_size;

    info!("[TEST] Placing initial order at {}", initial_price);
    let initial_order = pac
        .place_limit_order(
            &config.symbol,
            OrderSide::Buy,
            size,
            Some(initial_price),
            tick_size,
            None,
            None,
        )
        .await?;
    let initial_cloid = initial_order.client_order_id.clone()
        .expect("Initial order should have client_order_id");

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Cancel the initial order
    info!("[TEST] Cancelling initial order");
    pac.cancel_order(&config.symbol, &initial_cloid).await?;

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Place replacement order at slightly different price
    let new_price = initial_price - tick_size * 5.0;
    info!("[TEST] Placing replacement order at {}", new_price);
    let replacement_order = pac
        .place_limit_order(
            &config.symbol,
            OrderSide::Buy,
            size,
            Some(new_price),
            tick_size,
            None,
            None,
        )
        .await?;
    let replacement_cloid = replacement_order.client_order_id.clone()
        .expect("Replacement order should have client_order_id");

    // Verify replacement order is active
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let open_orders = pac.get_open_orders().await?;
    let found = open_orders
        .iter()
        .any(|o| o.client_order_id == replacement_cloid);

    assert!(found, "Replacement order not found in open orders");

    // Cleanup
    pac.cancel_order(&config.symbol, &replacement_cloid).await?;

    info!("[TEST] Order refresh flow completed successfully");
    Ok(())
}

/// Test placing a market order
#[tokio::test]
#[ignore] // Market orders execute immediately - use with caution
async fn test_pacifica_place_market_order() -> Result<()> {
    require_credentials();
    let config = test_config()?;

    let pac = create_pacifica_client()?;

    // Get market info
    let markets = pac.get_market_info().await?;
    let market = &markets[&config.symbol];
    let lot_size: f64 = market.lot_size.parse()?;

    // Get current best bid/ask
    let (best_bid, _best_ask) = pac
        .get_best_bid_ask_rest(&config.symbol, DEFAULT_AGG_LEVEL)
        .await?
        .expect("No best bid/ask");

    // Calculate minimum size
    let size = (MIN_TEST_NOTIONAL_USD / best_bid / lot_size).ceil() * lot_size;

    info!("[TEST] Placing market BUY order: {} {}", size, config.symbol);

    // place_market_order(symbol, side, size, slippage_percent, reduce_only)
    let order_response = pac
        .place_market_order(&config.symbol, OrderSide::Buy, size, 0.05, false)
        .await?;

    info!("[TEST] Market order placed: {:?}", order_response);

    Ok(())
}
