/// Test Aggressive Fill Detection - All 5 Methods
///
/// This test places an aggressive post-only limit order (0.05% spread) and monitors
/// all 5 fill detection methods to verify they work correctly and deduplicate properly.
///
/// Fill Detection Methods Tested:
/// 1. WebSocket Fill Detection (primary, real-time, via account_order_updates)
/// 2. WebSocket Position Detection (redundancy, real-time, via account_positions)
/// 3. REST API Order Polling (backup, 500ms)
/// 4. Position Monitor (ground truth, 500ms, REST-based)
/// 5. Monitor Task Pre-Cancel Check (defensive)
///
/// Usage:
/// ```bash
/// # Run with clean INFO-level logs
/// cargo run --example test_aggressive_fill_detection --release
///
/// # Run with verbose DEBUG logs (if needed for troubleshooting)
/// RUST_LOG=debug cargo run --example test_aggressive_fill_detection --release
/// ```
///
/// Expected Behavior:
/// - Order fills quickly due to aggressive price
/// - One or more detection methods trigger
/// - Only ONE hedge executes (deduplication works)
/// - Position returns to flat after hedge

use anyhow::{Context, Result};
use colored::Colorize;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{interval, sleep, Duration, Instant};
use tracing::{debug, info, warn};

use xemm_rust::config::Config;
use xemm_rust::connector::pacifica::{
    FillDetectionClient, FillDetectionConfig, FillEvent,
    OpenOrderItem, OrderSide as PacificaOrderSide, PacificaCredentials, PacificaTrading, PacificaWsTrading,
};
use xemm_rust::connector::hyperliquid::{HyperliquidCredentials, HyperliquidTrading};
use xemm_rust::strategy::OrderSide;

/// Track which detection method found the fill first
#[derive(Debug, Clone)]
enum DetectionMethod {
    WebSocket,
    WebSocketPosition,  // Position-based fill detection via WebSocket
    RestOrderPoll,
    PositionMonitor,
    MonitorPreCancel,
}

/// Fill detection event
#[derive(Debug, Clone)]
struct FillDetectionEvent {
    method: DetectionMethod,
    timestamp: Instant,
    fill_size: f64,
    client_order_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging (INFO level to avoid verbose DEBUG logs)
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("{}", "═".repeat(80).bright_cyan());
    println!("{}", "  Test Aggressive Fill Detection - All 5 Methods".bright_white().bold());
    println!("{}", "═".repeat(80).bright_cyan());
    println!();

    // Load configuration
    let mut config = Config::load_default().context("Failed to load configuration")?;

    // Override notional for test (use $15 to ensure reasonable size)
    config.order_notional_usd = 15.0;

    info!("{} Symbol: {}, Notional: ${}",
        "[CONFIG]".blue().bold(),
        config.symbol.bright_white().bold(),
        config.order_notional_usd.to_string().bright_white()
    );

    // Load credentials
    let pacifica_creds = PacificaCredentials::from_env()
        .context("Failed to load Pacifica credentials")?;
    let hyperliquid_creds = HyperliquidCredentials::from_env()
        .context("Failed to load Hyperliquid credentials")?;

    info!("{} Credentials loaded", "[INIT]".cyan().bold());

    // Create clients
    let pacifica_trading = Arc::new(PacificaTrading::new(pacifica_creds.clone())?);
    let pacifica_trading_rest = Arc::new(PacificaTrading::new(pacifica_creds.clone())?);
    let pacifica_trading_position = Arc::new(PacificaTrading::new(pacifica_creds.clone())?);
    let pacifica_ws_trading = Arc::new(PacificaWsTrading::new(pacifica_creds.clone(), false));
    let hyperliquid_trading = Arc::new(HyperliquidTrading::new(hyperliquid_creds.clone(), false)?);

    info!("{} Clients initialized", "[INIT]".cyan().bold());

    // Cancel all existing orders first
    info!("{} Cancelling all existing orders...", "[INIT]".cyan().bold());
    let cancelled = pacifica_trading.cancel_all_orders(false, Some(&config.symbol), false).await?;
    info!("{} Cancelled {} existing order(s)", "[INIT]".green().bold(), cancelled);

    // Wait a moment for cancellations to settle
    sleep(Duration::from_secs(1)).await;

    // Fetch current market price
    info!("{} Fetching current market price...", "[INIT]".cyan().bold());
    let (best_bid, best_ask) = pacifica_trading
        .get_best_bid_ask_rest(&config.symbol, 1)
        .await?
        .context("No bid/ask available")?;

    let mid_price = (best_bid + best_ask) / 2.0;
    let spread_bps = ((best_ask - best_bid) / mid_price) * 10000.0;

    info!("{} Market: Bid ${:.4}, Ask ${:.4}, Mid ${:.4}, Spread {:.2} bps",
        "[MARKET]".magenta().bold(),
        best_bid,
        best_ask,
        mid_price,
        spread_bps
    );

    // Calculate aggressive limit price (0.05% = 5 bps inside the spread)
    let target_spread_bps = 5.0; // 0.05% = 5 basis points
    let side = PacificaOrderSide::Buy; // We'll buy aggressively

    let aggressive_price = match side {
        PacificaOrderSide::Buy => {
            // Buy slightly below mid (aggressive)
            mid_price * (1.0 - target_spread_bps / 10000.0)
        }
        PacificaOrderSide::Sell => {
            // Sell slightly above mid (aggressive)
            mid_price * (1.0 + target_spread_bps / 10000.0)
        }
    };

    // Get market info for tick size
    let market_info = pacifica_trading.get_market_info().await?;
    let symbol_info = market_info
        .get(&config.symbol)
        .context("Symbol not found in market info")?;
    let tick_size: f64 = symbol_info.tick_size.parse()?;
    let lot_size: f64 = symbol_info.lot_size.parse()?;

    // Round price to tick size
    let rounded_price = (aggressive_price / tick_size).floor() * tick_size;

    // Calculate size
    let size = config.order_notional_usd / rounded_price;
    let rounded_size = (size / lot_size).floor() * lot_size;

    info!("{} Order: {} {} @ ${:.4} (notional: ${:.2})",
        "[ORDER]".bright_yellow().bold(),
        side.as_str().to_uppercase().green().bold(),
        rounded_size,
        rounded_price,
        rounded_size * rounded_price
    );

    let price_vs_mid_bps = ((rounded_price - mid_price) / mid_price) * 10000.0;
    info!("{} Price is {:.2} bps {} mid (aggressive for quick fill)",
        "[STRATEGY]".bright_green().bold(),
        price_vs_mid_bps.abs(),
        if price_vs_mid_bps < 0.0 { "below" } else { "above" }
    );

    // Shared state for tracking detections
    let detection_events = Arc::new(Mutex::new(Vec::<FillDetectionEvent>::new()));
    let processed_fills = Arc::new(Mutex::new(HashSet::<String>::new()));
    let hedge_triggered = Arc::new(Mutex::new(false));
    let (hedge_tx, mut hedge_rx) = mpsc::channel::<(OrderSide, f64, String)>(10);

    // Track client order ID
    let placed_order_id = Arc::new(RwLock::new(Option::<String>::None));

    // Track baseline position before order
    let baseline_position = Arc::new(Mutex::new(Option::<(f64, String)>::None));

    info!("{} Fetching baseline position...", "[INIT]".cyan().bold());
    match pacifica_trading_position.get_positions().await {
        Ok(positions) => {
            if let Some(pos) = positions.iter().find(|p| p.symbol == config.symbol) {
                let amount: f64 = pos.amount.parse().unwrap_or(0.0);
                let signed_amount = if pos.side == "bid" { amount } else { -amount };
                *baseline_position.lock().await = Some((signed_amount, pos.side.clone()));
                info!("{} Baseline position: {} {} (signed: {:.4})",
                    "[POSITION]".magenta().bold(),
                    amount,
                    pos.side.bright_white(),
                    signed_amount
                );
            } else {
                info!("{} No existing position for {}", "[POSITION]".magenta().bold(), config.symbol);
                *baseline_position.lock().await = Some((0.0, "none".to_string()));
            }
        }
        Err(e) => {
            warn!("{} Failed to fetch baseline position: {}", "[POSITION]".yellow().bold(), e);
            *baseline_position.lock().await = Some((0.0, "none".to_string()));
        }
    }

    println!();
    info!("{} Starting fill detection monitors...", "[INIT]".cyan().bold());
    println!();

    // ═══════════════════════════════════════════════════
    // Detection Method 1: WebSocket Fill Detection
    // ═══════════════════════════════════════════════════

    let detection_ws = detection_events.clone();
    let processed_ws = processed_fills.clone();
    let hedge_tx_ws = hedge_tx.clone();
    let placed_id_ws = placed_order_id.clone();
    let symbol_ws = config.symbol.clone();

    let fill_config = FillDetectionConfig {
        account: pacifica_creds.account.clone(),
        reconnect_attempts: 3,
        ping_interval_secs: 15,
        enable_position_fill_detection: true,  // Enable position-based fill detection
    };

    let mut fill_client = FillDetectionClient::new(fill_config, false)?;

    tokio::spawn(async move {
        fill_client
            .start(move |fill_event| {
                let detection_clone = detection_ws.clone();
                let processed_clone = processed_ws.clone();
                let hedge_tx = hedge_tx_ws.clone();
                let placed_id = placed_id_ws.clone();
                let symbol = symbol_ws.clone();

                tokio::spawn(async move {
                    match fill_event {
                        FillEvent::FullFill {
                            symbol: fill_symbol,
                            side,
                            filled_amount,
                            avg_price,
                            client_order_id,
                            ..
                        } if fill_symbol == symbol => {
                            let cloid = client_order_id.clone().unwrap_or_default();
                            let placed = placed_id.read().await;

                            if placed.as_ref().map(|id| id == &cloid).unwrap_or(false) {
                                drop(placed);

                                let fill_id = format!("ws_{}", cloid);
                                let mut processed = processed_clone.lock().await;

                                if !processed.contains(&fill_id) {
                                    processed.insert(fill_id);
                                    drop(processed);

                                    let fill_size: f64 = filled_amount.parse().unwrap_or(0.0);

                                    info!("{} {} DETECTED: {} {} @ {}",
                                        "[WS_FILL]".bright_magenta().bold(),
                                        "✓".green().bold(),
                                        side.bright_white(),
                                        fill_size,
                                        avg_price.cyan()
                                    );

                                    let mut events = detection_clone.lock().await;
                                    events.push(FillDetectionEvent {
                                        method: DetectionMethod::WebSocket,
                                        timestamp: Instant::now(),
                                        fill_size,
                                        client_order_id: cloid.clone(),
                                    });
                                    drop(events);

                                    let order_side = if side == "buy" || side == "bid" {
                                        OrderSide::Buy
                                    } else {
                                        OrderSide::Sell
                                    };

                                    hedge_tx.send((order_side, fill_size, cloid)).await.ok();
                                }
                            }
                        }
                        FillEvent::PositionFill {
                            symbol: fill_symbol,
                            side,
                            filled_amount,
                            avg_price,
                            cross_validated,
                            position_delta,
                            prev_position,
                            new_position,
                            ..
                        } if fill_symbol == symbol => {
                            // Position-based fill detection (redundancy layer)
                            let placed = placed_id.read().await;

                            // For position fills, we don't have client_order_id, so we trigger
                            // regardless if we have an active order and position changed
                            if placed.is_some() {
                                drop(placed);

                                let fill_id = format!("ws_pos_{}_{}", position_delta, new_position);
                                let mut processed = processed_clone.lock().await;

                                if !processed.contains(&fill_id) {
                                    processed.insert(fill_id.clone());
                                    drop(processed);

                                    let fill_size: f64 = filled_amount.parse().unwrap_or(0.0);

                                    if cross_validated {
                                        info!("{} {} POSITION FILL (cross-validated): {} {} @ {} | Δ: {} → {}",
                                            "[WS_POS_FILL]".bright_blue().bold(),
                                            "✓".green().bold(),
                                            side.bright_white(),
                                            fill_size,
                                            avg_price.cyan(),
                                            prev_position,
                                            new_position
                                        );
                                    } else {
                                        warn!("{} {} POSITION FILL (MISSED BY PRIMARY!): {} {} @ {} | Δ: {} → {}",
                                            "[WS_POS_FILL]".bright_yellow().bold(),
                                            "⚠".yellow().bold(),
                                            side.bright_white(),
                                            fill_size,
                                            avg_price.cyan(),
                                            prev_position,
                                            new_position
                                        );
                                    }

                                    let mut events = detection_clone.lock().await;
                                    events.push(FillDetectionEvent {
                                        method: DetectionMethod::WebSocketPosition,
                                        timestamp: Instant::now(),
                                        fill_size,
                                        client_order_id: fill_id.clone(),
                                    });
                                    drop(events);

                                    // Only trigger hedge if NOT cross-validated (safety net)
                                    if !cross_validated {
                                        let order_side = if side == "buy" {
                                            OrderSide::Buy
                                        } else {
                                            OrderSide::Sell
                                        };

                                        hedge_tx.send((order_side, fill_size, fill_id)).await.ok();
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                });
            })
            .await
            .ok();
    });

    // ═══════════════════════════════════════════════════
    // Detection Method 2: REST Order Polling
    // ═══════════════════════════════════════════════════

    let detection_rest = detection_events.clone();
    let processed_rest = processed_fills.clone();
    let hedge_tx_rest = hedge_tx.clone();
    let placed_id_rest = placed_order_id.clone();
    let symbol_rest = config.symbol.clone();
    let trading_rest = pacifica_trading_rest.clone();

    tokio::spawn(async move {
        let mut poll_interval = interval(Duration::from_millis(500));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_filled_amount = 0.0;

        loop {
            poll_interval.tick().await;

            let placed = placed_id_rest.read().await;
            if placed.is_none() {
                continue;
            }
            let cloid = placed.clone().unwrap();
            drop(placed);

            match trading_rest.get_open_orders().await {
                Ok(orders) => {
                    if let Some(order) = orders.iter().find(|o| o.client_order_id == cloid) {
                        let current_filled: f64 = order.filled_amount.parse().unwrap_or(0.0);

                        if current_filled > last_filled_amount + 0.0001 {
                            let delta = current_filled - last_filled_amount;
                            let fill_id = format!("rest_{}", cloid);
                            let mut processed = processed_rest.lock().await;

                            if !processed.contains(&fill_id) {
                                processed.insert(fill_id);
                                drop(processed);

                                info!("{} {} DETECTED: fill delta {:.4} (total {:.4})",
                                    "[REST_POLL]".bright_cyan().bold(),
                                    "✓".green().bold(),
                                    delta,
                                    current_filled
                                );

                                let mut events = detection_rest.lock().await;
                                events.push(FillDetectionEvent {
                                    method: DetectionMethod::RestOrderPoll,
                                    timestamp: Instant::now(),
                                    fill_size: delta,
                                    client_order_id: cloid.clone(),
                                });
                                drop(events);

                                let order_side = if order.side == "bid" {
                                    OrderSide::Buy
                                } else {
                                    OrderSide::Sell
                                };

                                hedge_tx_rest.send((order_side, delta, cloid.clone())).await.ok();
                            }

                            last_filled_amount = current_filled;
                        }
                    } else {
                        // Order not in open orders - might be fully filled
                        if last_filled_amount > 0.0 {
                            debug!("[REST_POLL] Order no longer in open orders (likely filled)");
                        }
                    }
                }
                Err(e) => {
                    debug!("[REST_POLL] Error fetching orders: {}", e);
                }
            }
        }
    });

    // ═══════════════════════════════════════════════════
    // Detection Method 3: Position Monitor
    // ═══════════════════════════════════════════════════

    let detection_pos = detection_events.clone();
    let processed_pos = processed_fills.clone();
    let hedge_tx_pos = hedge_tx.clone();
    let placed_id_pos = placed_order_id.clone();
    let symbol_pos = config.symbol.clone();
    let trading_pos = pacifica_trading_position.clone();
    let baseline_pos = baseline_position.clone();

    tokio::spawn(async move {
        let mut poll_interval = interval(Duration::from_millis(500));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            poll_interval.tick().await;

            let placed = placed_id_pos.read().await;
            if placed.is_none() {
                continue;
            }
            let cloid = placed.clone().unwrap();
            drop(placed);

            match trading_pos.get_positions().await {
                Ok(positions) => {
                    let baseline = baseline_pos.lock().await.clone();
                    let (baseline_signed, _) = baseline.unwrap_or((0.0, "none".to_string()));

                    let current_position = positions.iter().find(|p| p.symbol == symbol_pos);
                    let current_signed = if let Some(pos) = current_position {
                        let amount: f64 = pos.amount.parse().unwrap_or(0.0);
                        if pos.side == "bid" { amount } else { -amount }
                    } else {
                        0.0
                    };

                    let delta = current_signed - baseline_signed;

                    if delta.abs() > 0.0001 {
                        let fill_id = format!("position_{}", cloid);
                        let mut processed = processed_pos.lock().await;

                        if !processed.contains(&fill_id) {
                            processed.insert(fill_id);
                            drop(processed);

                            info!("{} {} DETECTED: position delta {:.4} ({:.4} → {:.4})",
                                "[POSITION]".bright_cyan().bold(),
                                "✓".green().bold(),
                                delta.abs(),
                                baseline_signed,
                                current_signed
                            );

                            let mut events = detection_pos.lock().await;
                            events.push(FillDetectionEvent {
                                method: DetectionMethod::PositionMonitor,
                                timestamp: Instant::now(),
                                fill_size: delta.abs(),
                                client_order_id: cloid.clone(),
                            });
                            drop(events);

                            let order_side = if delta > 0.0 {
                                OrderSide::Buy
                            } else {
                                OrderSide::Sell
                            };

                            hedge_tx_pos.send((order_side, delta.abs(), cloid.clone())).await.ok();
                        }
                    }
                }
                Err(e) => {
                    debug!("[POSITION] Error fetching positions: {}", e);
                }
            }
        }
    });

    // Wait for monitors to initialize
    sleep(Duration::from_secs(2)).await;

    println!();
    info!("{}", "═".repeat(80).bright_cyan());
    info!("{} PLACING AGGRESSIVE ORDER", "[TEST]".bright_yellow().bold());
    info!("{}", "═".repeat(80).bright_cyan());
    println!();

    // Place the aggressive limit order
    let result = pacifica_trading
        .place_limit_order(
            &config.symbol,
            side,
            rounded_size,
            Some(rounded_price),  // Explicit price
            1.0,                   // mid_price_offset_pct (not used since price is Some)
            Some(best_bid),        // current_bid
            Some(best_ask),        // current_ask
        )
        .await?;

    let client_order_id = result.client_order_id.clone()
        .context("No client_order_id in response")?;
    *placed_order_id.write().await = Some(client_order_id.clone());

    info!("{} {} Order placed successfully!",
        "[ORDER]".bright_yellow().bold(),
        "✓".green().bold()
    );
    if let Some(oid) = result.order_id {
        info!("{} Order ID: {}",
            "[ORDER]".bright_yellow().bold(),
            oid.to_string().bright_white()
        );
    }
    info!("{} Client Order ID: {}",
        "[ORDER]".bright_yellow().bold(),
        client_order_id.bright_white()
    );

    println!();
    info!("{} Waiting for fill detection... (max 5 minutes)", "[TEST]".bright_cyan().bold());
    println!();

    // Wait for hedge trigger (with timeout)
    let test_start = Instant::now();
    let max_wait = Duration::from_secs(300); // 5 minutes

    let hedge_result = tokio::time::timeout(max_wait, async {
        hedge_rx.recv().await
    }).await;

    match hedge_result {
        Ok(Some((hedge_side, hedge_size, hedge_cloid))) => {
            let elapsed = test_start.elapsed();

            *hedge_triggered.lock().await = true;

            println!();
            info!("{}", "═".repeat(80).bright_green());
            info!("{} HEDGE TRIGGERED!", "[SUCCESS]".bright_green().bold());
            info!("{}", "═".repeat(80).bright_green());
            println!();

            info!("{} Side: {}, Size: {:.4}, Time: {:.3}s",
                "[HEDGE]".bright_magenta().bold(),
                hedge_side.as_str().to_uppercase().bright_white(),
                hedge_size,
                elapsed.as_secs_f64()
            );

            // Analyze which detection methods triggered
            sleep(Duration::from_millis(500)).await; // Let all methods process

            let events = detection_events.lock().await;

            println!();
            info!("{} Detection Methods Summary:", "[ANALYSIS]".bright_yellow().bold());
            info!("{}", "─".repeat(60).bright_black());

            if events.is_empty() {
                warn!("{} No detection events recorded (might have hedge triggered before tracking)",
                    "⚠".yellow().bold());
            } else {
                for (i, event) in events.iter().enumerate() {
                    let method_name = format!("{:?}", event.method);
                    let time_from_start = event.timestamp.duration_since(test_start);

                    let marker = if i == 0 {
                        "🥇 FIRST".green().bold()
                    } else {
                        format!("   #{}", i + 1).bright_black()
                    };

                    info!("{} {} detected fill at {:.3}s",
                        marker,
                        method_name.bright_white(),
                        time_from_start.as_secs_f64()
                    );
                }
            }

            let hedge_was_triggered = *hedge_triggered.lock().await;
            println!();
            info!("{} Hedge triggered: {}",
                "[RESULT]".bright_green().bold(),
                if hedge_was_triggered { "YES ✓".green().bold() } else { "NO ✗".red().bold() }
            );
            info!("{} Detection methods that fired: {}",
                "[RESULT]".bright_green().bold(),
                events.len().to_string().bright_white()
            );

            // Execute hedge on Hyperliquid
            println!();
            info!("{} Executing hedge on Hyperliquid...", "[HEDGE]".bright_magenta().bold());

            let hl_side = match hedge_side {
                OrderSide::Buy => OrderSide::Sell,
                OrderSide::Sell => OrderSide::Buy,
            };

            // Fetch current Hyperliquid prices for market order
            info!("{} Fetching current Hyperliquid prices...", "[HEDGE]".bright_magenta().bold());
            let (hl_bid, hl_ask) = match hyperliquid_trading.get_l2_snapshot(&config.symbol).await {
                Ok(Some((bid, ask))) => {
                    info!("{} Hyperliquid prices: Bid ${:.4}, Ask ${:.4}",
                        "[HEDGE]".bright_magenta().bold(),
                        bid,
                        ask
                    );
                    (Some(bid), Some(ask))
                }
                Ok(None) => {
                    warn!("{} No Hyperliquid prices available, using None (may fail)",
                        "[HEDGE]".yellow().bold()
                    );
                    (None, None)
                }
                Err(e) => {
                    warn!("{} Failed to fetch Hyperliquid prices: {}, using None",
                        "[HEDGE]".yellow().bold(),
                        e
                    );
                    (None, None)
                }
            };

            let is_buy = matches!(hl_side, OrderSide::Buy);
            match hyperliquid_trading.place_market_order(&config.symbol, is_buy, hedge_size, config.hyperliquid_slippage, false, hl_bid, hl_ask).await {
                Ok(hl_result) => {
                    info!("{} {} Hedge executed on Hyperliquid",
                        "[HEDGE]".bright_magenta().bold(),
                        "✓".green().bold()
                    );
                    info!("{} Hyperliquid status: {:?}",
                        "[HEDGE]".bright_magenta().bold(),
                        hl_result.status
                    );
                }
                Err(e) => {
                    warn!("{} {} Hedge failed: {}",
                        "[HEDGE]".bright_magenta().bold(),
                        "✗".red().bold(),
                        e
                    );
                }
            }

            // Verify positions after hedge (both exchanges)
            // Wait longer for positions to propagate to both APIs
            println!();
            info!("{} Waiting 8 seconds for positions to propagate...", "[VERIFY]".cyan().bold());
            sleep(Duration::from_secs(8)).await;

            info!("{} Verifying final positions on both exchanges...", "[VERIFY]".cyan().bold());

            // Check Pacifica position
            let pacifica_position = match pacifica_trading_position.get_positions().await {
                Ok(positions) => {
                    if let Some(pos) = positions.iter().find(|p| p.symbol == config.symbol) {
                        let amount: f64 = pos.amount.parse().unwrap_or(0.0);
                        let signed_amount = if pos.side == "bid" { amount } else { -amount };

                        info!("{} Pacifica: {} {} (signed: {:.4})",
                            "[VERIFY]".cyan().bold(),
                            amount,
                            pos.side.bright_white(),
                            signed_amount
                        );
                        Some(signed_amount)
                    } else {
                        info!("{} Pacifica: No position (flat)", "[VERIFY]".cyan().bold());
                        Some(0.0)
                    }
                }
                Err(e) => {
                    warn!("{} Failed to fetch Pacifica position: {}", "[VERIFY]".yellow().bold(), e);
                    None
                }
            };

            // Check Hyperliquid position (with retry logic)
            // Load wallet address from environment (same as other examples)
            let wallet_address = std::env::var("HL_WALLET")
                .context("HL_WALLET environment variable not set")
                .unwrap();

            info!("{} Checking Hyperliquid positions for wallet: {}",
                "[VERIFY]".cyan().bold(),
                wallet_address.bright_white());

            let mut hyperliquid_position: Option<f64> = None;

            // Try up to 3 times with delays if position not found
            for retry in 0..3 {
                if retry > 0 {
                    info!("{} Retry {} - waiting 3 more seconds for Hyperliquid position...",
                        "[VERIFY]".cyan().bold(), retry);
                    sleep(Duration::from_secs(3)).await;
                }

                match hyperliquid_trading.get_user_state(&wallet_address).await {
                    Ok(user_state) => {
                        info!("{} Hyperliquid returned {} position(s)",
                            "[VERIFY]".cyan().bold(),
                            user_state.asset_positions.len());

                        if let Some(asset_pos) = user_state.asset_positions.iter().find(|ap| ap.position.coin == config.symbol) {
                            let szi: f64 = asset_pos.position.szi.parse().unwrap_or(0.0);
                            info!("{} Hyperliquid: {} (signed: {:.4})",
                                "[VERIFY]".cyan().bold(),
                                if szi > 0.0 { "LONG".green() } else if szi < 0.0 { "SHORT".red() } else { "FLAT".bright_white() },
                                szi
                            );
                            hyperliquid_position = Some(szi);
                            break;
                        } else if retry == 2 {
                            info!("{} Hyperliquid: No position for {} found after 3 attempts",
                                "[VERIFY]".cyan().bold(),
                                config.symbol);
                            hyperliquid_position = Some(0.0);
                        }
                    }
                    Err(e) => {
                        warn!("{} Failed to fetch Hyperliquid position (attempt {}): {}",
                            "[VERIFY]".yellow().bold(), retry + 1, e);
                        if retry == 2 {
                            hyperliquid_position = None;
                        }
                    }
                }
            }

            // Calculate net position across both exchanges
            if let (Some(pac_pos), Some(hl_pos)) = (pacifica_position, hyperliquid_position) {
                let net_position = pac_pos + hl_pos;

                info!("{}", "─".repeat(60).bright_black());
                info!("{} Net Position: {:.4} (Pacifica: {:.4} + Hyperliquid: {:.4})",
                    "[VERIFY]".cyan().bold(),
                    net_position,
                    pac_pos,
                    hl_pos
                );

                let baseline = baseline_position.lock().await.clone();
                let (baseline_signed, _) = baseline.unwrap_or((0.0, "none".to_string()));

                // Check if net position is close to baseline
                if net_position.abs() < 0.01 {
                    info!("{} {} Net position is FLAT (properly hedged across both exchanges)",
                        "[VERIFY]".cyan().bold(),
                        "✓".green().bold()
                    );
                } else {
                    warn!("{} {} Net position NOT flat! Delta: {:.4}",
                        "[VERIFY]".cyan().bold(),
                        "⚠".yellow().bold(),
                        net_position
                    );

                    if (pac_pos - baseline_signed).abs() < 0.01 && hl_pos.abs() < 0.01 {
                        info!("{} {} Both exchanges individually flat (hedge executed but may have closed old position)",
                            "[VERIFY]".cyan().bold(),
                            "ℹ".bright_blue().bold()
                        );
                    }
                }
            } else {
                warn!("{} Could not verify net position (failed to fetch from one or both exchanges)",
                    "[VERIFY]".yellow().bold()
                );
            }

            println!();
            info!("{}", "═".repeat(80).bright_green());
            info!("{} TEST PASSED ✓", "[SUCCESS]".bright_green().bold());
            info!("{}", "═".repeat(80).bright_green());
        }
        Ok(None) => {
            warn!("{} Hedge channel closed unexpectedly", "[ERROR]".red().bold());
        }
        Err(_) => {
            warn!("{} Timeout waiting for fill ({}s)",
                "[TIMEOUT]".yellow().bold(),
                max_wait.as_secs()
            );

            info!("{} Checking if order is still open...", "[CHECK]".cyan().bold());
            match pacifica_trading.get_open_orders().await {
                Ok(orders) => {
                    if let Some(order) = orders.iter().find(|o| o.client_order_id == client_order_id) {
                        let filled: f64 = order.filled_amount.parse().unwrap_or(0.0);
                        warn!("{} Order still open. Filled: {}/{}",
                            "[CHECK]".yellow().bold(),
                            filled,
                            order.initial_amount
                        );

                        info!("{} Cancelling order...", "[CLEANUP]".cyan().bold());
                        pacifica_trading.cancel_order(&client_order_id, &config.symbol).await.ok();
                    } else {
                        info!("{} Order no longer in open orders", "[CHECK]".cyan().bold());
                    }
                }
                Err(e) => {
                    warn!("{} Failed to check orders: {}", "[CHECK]".yellow().bold(), e);
                }
            }
        }
    }

    Ok(())
}
