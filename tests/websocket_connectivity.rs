//! WebSocket connectivity tests for orderbook feeds
//!
//! These tests verify WebSocket connections to public orderbook feeds work correctly.
//! No API credentials required, but network connectivity is needed.
//!
//! Run with: cargo test --features websocket-tests websocket_connectivity -- --test-threads=1

#![cfg(feature = "websocket-tests")]

use anyhow::Result;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use xemm_rust::connector::pacifica::{
    OrderbookClient as PacificaOrderbookClient,
    OrderbookConfig as PacificaOrderbookConfig,
};
use xemm_rust::connector::hyperliquid::{
    OrderbookClient as HyperliquidOrderbookClient,
    OrderbookConfig as HyperliquidOrderbookConfig,
};

/// Initialize tracing for test output
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();
}

/// Test symbol for connectivity tests
const TEST_SYMBOL: &str = "SOL";

// ============================================================================
// Pacifica WebSocket Tests
// ============================================================================

/// Test connecting to Pacifica orderbook WebSocket and receiving data
#[tokio::test]
async fn test_pacifica_ws_connect_and_receive() -> Result<()> {
    init_tracing();

    let config = PacificaOrderbookConfig {
        symbol: TEST_SYMBOL.to_string(),
        agg_level: 1,
        reconnect_attempts: 1,
        ping_interval_secs: 30,
    };

    let mut client = PacificaOrderbookClient::new(config)?;

    let received = Arc::new(AtomicBool::new(false));
    let received_clone = received.clone();
    let update_count = Arc::new(AtomicU32::new(0));
    let update_count_clone = update_count.clone();

    // Track best bid/ask for validation
    let last_bid = Arc::new(std::sync::Mutex::new(0.0_f64));
    let last_ask = Arc::new(std::sync::Mutex::new(0.0_f64));
    let last_bid_clone = last_bid.clone();
    let last_ask_clone = last_ask.clone();

    // Run client with timeout
    let mut client_handle = tokio::spawn(async move {
        client.start(move |best_bid, best_ask, symbol, _timestamp| {
            let count = update_count_clone.fetch_add(1, Ordering::SeqCst);
            if count < 5 {
                info!(
                    "[TEST] Pacifica {} update #{}: bid={}, ask={}",
                    symbol, count + 1, best_bid, best_ask
                );
            }
            *last_bid_clone.lock().unwrap() = best_bid;
            *last_ask_clone.lock().unwrap() = best_ask;
            received_clone.store(true, Ordering::SeqCst);
        }).await
    });

    // Wait for data or timeout
    let timeout = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => {
                info!("[TEST] Timeout reached");
                break;
            }
            result = &mut client_handle => {
                if let Err(e) = result {
                    info!("[TEST] Client task error: {}", e);
                }
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if update_count.load(Ordering::SeqCst) >= 5 {
                    info!("[TEST] Received 5 updates, stopping");
                    break;
                }
            }
        }
    }

    // Validate results
    let count = update_count.load(Ordering::SeqCst);
    assert!(received.load(Ordering::SeqCst), "Should have received at least one update");
    assert!(count >= 1, "Should have received at least 1 update, got {}", count);

    let bid = *last_bid.lock().unwrap();
    let ask = *last_ask.lock().unwrap();

    info!("[TEST] Final Pacifica prices: bid={}, ask={}", bid, ask);

    assert!(bid > 0.0, "Best bid should be positive, got {}", bid);
    assert!(ask > 0.0, "Best ask should be positive, got {}", ask);
    assert!(bid < ask, "Best bid ({}) should be less than best ask ({})", bid, ask);

    info!("[TEST] Pacifica WebSocket connectivity test PASSED ({} updates)", count);
    Ok(())
}

/// Test Pacifica orderbook data validity over multiple updates
#[tokio::test]
async fn test_pacifica_ws_data_validity() -> Result<()> {
    init_tracing();

    let config = PacificaOrderbookConfig {
        symbol: TEST_SYMBOL.to_string(),
        agg_level: 1,
        reconnect_attempts: 1,
        ping_interval_secs: 30,
    };

    let mut client = PacificaOrderbookClient::new(config)?;

    let update_count = Arc::new(AtomicU32::new(0));
    let update_count_clone = update_count.clone();
    let invalid_count = Arc::new(AtomicU32::new(0));
    let invalid_count_clone = invalid_count.clone();

    let mut client_handle = tokio::spawn(async move {
        client.start(move |best_bid, best_ask, _symbol, _timestamp| {
            update_count_clone.fetch_add(1, Ordering::SeqCst);

            // Validate each update
            if best_bid <= 0.0 || best_ask <= 0.0 || best_bid >= best_ask {
                invalid_count_clone.fetch_add(1, Ordering::SeqCst);
            }
        }).await
    });

    // Collect updates for 10 seconds
    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => break,
            result = &mut client_handle => {
                if let Err(e) = result {
                    info!("[TEST] Client error: {}", e);
                }
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if update_count.load(Ordering::SeqCst) >= 20 {
                    break;
                }
            }
        }
    }

    let total = update_count.load(Ordering::SeqCst);
    let invalid = invalid_count.load(Ordering::SeqCst);

    info!("[TEST] Pacifica data validity: {}/{} valid updates", total - invalid, total);

    assert!(total >= 5, "Should have received at least 5 updates, got {}", total);
    assert_eq!(invalid, 0, "All updates should be valid, got {} invalid out of {}", invalid, total);

    info!("[TEST] Pacifica data validity test PASSED");
    Ok(())
}

// ============================================================================
// Hyperliquid WebSocket Tests
// ============================================================================

/// Test connecting to Hyperliquid orderbook WebSocket and receiving data
#[tokio::test]
async fn test_hyperliquid_ws_connect_and_receive() -> Result<()> {
    init_tracing();

    let config = HyperliquidOrderbookConfig {
        coin: TEST_SYMBOL.to_string(),
        reconnect_attempts: 1,
        ping_interval_secs: 30,
    };

    let mut client = HyperliquidOrderbookClient::new(config)?;

    let received = Arc::new(AtomicBool::new(false));
    let received_clone = received.clone();
    let update_count = Arc::new(AtomicU32::new(0));
    let update_count_clone = update_count.clone();

    let last_bid = Arc::new(std::sync::Mutex::new(0.0_f64));
    let last_ask = Arc::new(std::sync::Mutex::new(0.0_f64));
    let last_bid_clone = last_bid.clone();
    let last_ask_clone = last_ask.clone();

    let mut client_handle = tokio::spawn(async move {
        client.start(move |best_bid, best_ask, coin, _timestamp| {
            let count = update_count_clone.fetch_add(1, Ordering::SeqCst);
            if count < 5 {
                info!(
                    "[TEST] Hyperliquid {} update #{}: bid={}, ask={}",
                    coin, count + 1, best_bid, best_ask
                );
            }
            *last_bid_clone.lock().unwrap() = best_bid;
            *last_ask_clone.lock().unwrap() = best_ask;
            received_clone.store(true, Ordering::SeqCst);
        }).await
    });

    // Wait for data or timeout
    let timeout = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => {
                info!("[TEST] Timeout reached");
                break;
            }
            result = &mut client_handle => {
                if let Err(e) = result {
                    info!("[TEST] Client task error: {}", e);
                }
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if update_count.load(Ordering::SeqCst) >= 5 {
                    info!("[TEST] Received 5 updates, stopping");
                    break;
                }
            }
        }
    }

    // Validate results
    let count = update_count.load(Ordering::SeqCst);
    assert!(received.load(Ordering::SeqCst), "Should have received at least one update");
    assert!(count >= 1, "Should have received at least 1 update, got {}", count);

    let bid = *last_bid.lock().unwrap();
    let ask = *last_ask.lock().unwrap();

    info!("[TEST] Final Hyperliquid prices: bid={}, ask={}", bid, ask);

    assert!(bid > 0.0, "Best bid should be positive, got {}", bid);
    assert!(ask > 0.0, "Best ask should be positive, got {}", ask);
    assert!(bid < ask, "Best bid ({}) should be less than best ask ({})", bid, ask);

    info!("[TEST] Hyperliquid WebSocket connectivity test PASSED ({} updates)", count);
    Ok(())
}

/// Test Hyperliquid orderbook data validity over multiple updates
#[tokio::test]
async fn test_hyperliquid_ws_data_validity() -> Result<()> {
    init_tracing();

    let config = HyperliquidOrderbookConfig {
        coin: TEST_SYMBOL.to_string(),
        reconnect_attempts: 1,
        ping_interval_secs: 30,
    };

    let mut client = HyperliquidOrderbookClient::new(config)?;

    let update_count = Arc::new(AtomicU32::new(0));
    let update_count_clone = update_count.clone();
    let invalid_count = Arc::new(AtomicU32::new(0));
    let invalid_count_clone = invalid_count.clone();

    let mut client_handle = tokio::spawn(async move {
        client.start(move |best_bid, best_ask, _coin, _timestamp| {
            update_count_clone.fetch_add(1, Ordering::SeqCst);

            if best_bid <= 0.0 || best_ask <= 0.0 || best_bid >= best_ask {
                invalid_count_clone.fetch_add(1, Ordering::SeqCst);
            }
        }).await
    });

    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => break,
            result = &mut client_handle => {
                if let Err(e) = result {
                    info!("[TEST] Client error: {}", e);
                }
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if update_count.load(Ordering::SeqCst) >= 20 {
                    break;
                }
            }
        }
    }

    let total = update_count.load(Ordering::SeqCst);
    let invalid = invalid_count.load(Ordering::SeqCst);

    info!("[TEST] Hyperliquid data validity: {}/{} valid updates", total - invalid, total);

    assert!(total >= 5, "Should have received at least 5 updates, got {}", total);
    assert_eq!(invalid, 0, "All updates should be valid, got {} invalid out of {}", invalid, total);

    info!("[TEST] Hyperliquid data validity test PASSED");
    Ok(())
}

// ============================================================================
// Cross-Exchange Tests
// ============================================================================

/// Test both exchanges receive data simultaneously
#[tokio::test]
async fn test_both_exchanges_concurrent() -> Result<()> {
    init_tracing();

    let pac_config = PacificaOrderbookConfig {
        symbol: TEST_SYMBOL.to_string(),
        agg_level: 1,
        reconnect_attempts: 1,
        ping_interval_secs: 30,
    };

    let hl_config = HyperliquidOrderbookConfig {
        coin: TEST_SYMBOL.to_string(),
        reconnect_attempts: 1,
        ping_interval_secs: 30,
    };

    let mut pac_client = PacificaOrderbookClient::new(pac_config)?;
    let mut hl_client = HyperliquidOrderbookClient::new(hl_config)?;

    let pac_count = Arc::new(AtomicU32::new(0));
    let pac_count_clone = pac_count.clone();
    let hl_count = Arc::new(AtomicU32::new(0));
    let hl_count_clone = hl_count.clone();

    let pac_bid = Arc::new(std::sync::Mutex::new(0.0_f64));
    let pac_ask = Arc::new(std::sync::Mutex::new(0.0_f64));
    let pac_bid_clone = pac_bid.clone();
    let pac_ask_clone = pac_ask.clone();

    let hl_bid = Arc::new(std::sync::Mutex::new(0.0_f64));
    let hl_ask = Arc::new(std::sync::Mutex::new(0.0_f64));
    let hl_bid_clone = hl_bid.clone();
    let hl_ask_clone = hl_ask.clone();

    // Start both clients concurrently
    let pac_handle = tokio::spawn(async move {
        pac_client.start(move |best_bid, best_ask, _symbol, _ts| {
            pac_count_clone.fetch_add(1, Ordering::SeqCst);
            *pac_bid_clone.lock().unwrap() = best_bid;
            *pac_ask_clone.lock().unwrap() = best_ask;
        }).await
    });

    let hl_handle = tokio::spawn(async move {
        hl_client.start(move |best_bid, best_ask, _coin, _ts| {
            hl_count_clone.fetch_add(1, Ordering::SeqCst);
            *hl_bid_clone.lock().unwrap() = best_bid;
            *hl_ask_clone.lock().unwrap() = best_ask;
        }).await
    });

    // Wait for both to receive data
    let timeout = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => break,
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                let pac = pac_count.load(Ordering::SeqCst);
                let hl = hl_count.load(Ordering::SeqCst);
                if pac >= 3 && hl >= 3 {
                    info!("[TEST] Both exchanges received sufficient updates");
                    break;
                }
            }
        }
    }

    // Abort client tasks
    pac_handle.abort();
    hl_handle.abort();

    let pac_updates = pac_count.load(Ordering::SeqCst);
    let hl_updates = hl_count.load(Ordering::SeqCst);

    info!(
        "[TEST] Concurrent test: Pacifica={} updates, Hyperliquid={} updates",
        pac_updates, hl_updates
    );

    assert!(pac_updates >= 1, "Pacifica should have received updates");
    assert!(hl_updates >= 1, "Hyperliquid should have received updates");

    // Compare prices - they should be reasonably close for the same asset
    let pac_mid = (*pac_bid.lock().unwrap() + *pac_ask.lock().unwrap()) / 2.0;
    let hl_mid = (*hl_bid.lock().unwrap() + *hl_ask.lock().unwrap()) / 2.0;

    if pac_mid > 0.0 && hl_mid > 0.0 {
        let price_diff_pct = ((pac_mid - hl_mid) / hl_mid * 100.0).abs();
        info!(
            "[TEST] Mid prices: Pacifica={:.2}, Hyperliquid={:.2}, diff={:.2}%",
            pac_mid, hl_mid, price_diff_pct
        );

        // Prices should be within 5% of each other for the same asset
        assert!(
            price_diff_pct < 5.0,
            "Price difference too large: {:.2}%",
            price_diff_pct
        );
    }

    info!("[TEST] Concurrent exchange test PASSED");
    Ok(())
}

/// Test spread comparison between exchanges
#[tokio::test]
async fn test_spread_comparison() -> Result<()> {
    init_tracing();

    let pac_config = PacificaOrderbookConfig {
        symbol: TEST_SYMBOL.to_string(),
        agg_level: 1,
        reconnect_attempts: 1,
        ping_interval_secs: 30,
    };

    let hl_config = HyperliquidOrderbookConfig {
        coin: TEST_SYMBOL.to_string(),
        reconnect_attempts: 1,
        ping_interval_secs: 30,
    };

    let mut pac_client = PacificaOrderbookClient::new(pac_config)?;
    let mut hl_client = HyperliquidOrderbookClient::new(hl_config)?;

    let pac_spreads = Arc::new(std::sync::Mutex::new(Vec::new()));
    let pac_spreads_clone = pac_spreads.clone();
    let hl_spreads = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hl_spreads_clone = hl_spreads.clone();

    let pac_handle = tokio::spawn(async move {
        pac_client.start(move |best_bid, best_ask, _symbol, _ts| {
            if best_bid > 0.0 {
                let spread_bps = (best_ask - best_bid) / best_bid * 10000.0;
                let mut spreads = pac_spreads_clone.lock().unwrap();
                if spreads.len() < 20 {
                    spreads.push(spread_bps);
                }
            }
        }).await
    });

    let hl_handle = tokio::spawn(async move {
        hl_client.start(move |best_bid, best_ask, _coin, _ts| {
            if best_bid > 0.0 {
                let spread_bps = (best_ask - best_bid) / best_bid * 10000.0;
                let mut spreads = hl_spreads_clone.lock().unwrap();
                if spreads.len() < 20 {
                    spreads.push(spread_bps);
                }
            }
        }).await
    });

    // Collect spread data
    let timeout = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => break,
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                let pac_len = pac_spreads.lock().unwrap().len();
                let hl_len = hl_spreads.lock().unwrap().len();
                if pac_len >= 10 && hl_len >= 10 {
                    break;
                }
            }
        }
    }

    pac_handle.abort();
    hl_handle.abort();

    let pac_data = pac_spreads.lock().unwrap();
    let hl_data = hl_spreads.lock().unwrap();

    if !pac_data.is_empty() && !hl_data.is_empty() {
        let pac_avg: f64 = pac_data.iter().sum::<f64>() / pac_data.len() as f64;
        let hl_avg: f64 = hl_data.iter().sum::<f64>() / hl_data.len() as f64;

        info!(
            "[TEST] Average spreads: Pacifica={:.2} bps ({} samples), Hyperliquid={:.2} bps ({} samples)",
            pac_avg, pac_data.len(), hl_avg, hl_data.len()
        );

        // Hyperliquid typically has tighter spreads - log this for reference
        if hl_avg < pac_avg {
            info!("[TEST] Hyperliquid spread is {:.2} bps tighter than Pacifica", pac_avg - hl_avg);
        }
    }

    assert!(!pac_data.is_empty(), "Should have Pacifica spread data");
    assert!(!hl_data.is_empty(), "Should have Hyperliquid spread data");

    info!("[TEST] Spread comparison test PASSED");
    Ok(())
}
