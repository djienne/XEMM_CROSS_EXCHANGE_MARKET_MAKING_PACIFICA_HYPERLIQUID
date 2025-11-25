use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use colored::Colorize;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};
use fast_float::parse;
use parking_lot::Mutex;
use tracing::debug;

use crate::bot::BotState;
use crate::config::Config;
use crate::connector::hyperliquid::HyperliquidTrading;
use crate::connector::hyperliquid::types::{WsPostRequest, WsPostRequestInner, WsPostResponse};
use crate::connector::pacifica::PacificaTrading;
use crate::services::HedgeEvent;
use crate::strategy::OrderSide;
use crate::trade_fetcher;
use crate::csv_logger;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWrite = futures_util::stream::SplitSink<WsStream, Message>;
type WsRead = futures_util::stream::SplitStream<WsStream>;

// Macro for timestamped colored output
macro_rules! tprintln {
    ($($arg:tt)*) => {{
        println!("{} {}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string().bright_black(),
            format!($($arg)*)
        );
    }};
}

/// Hedge execution service
///
/// Receives hedge triggers via mpsc channel and executes the hedge flow:
/// 1. Pre-hedge cancellation of all Pacifica orders
/// 2. Execute market order on Hyperliquid (opposite direction)
/// 3. Wait for trade propagation (20s)
/// 4. Fetch trade history from both exchanges
/// 5. Calculate actual profit using real fill data and fees
/// 6. Display comprehensive trade summary
/// 7. Post-hedge cancellation (safety)
/// 8. Position verification on both exchanges
/// 9. Mark cycle complete and signal shutdown
pub struct HedgeService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_rx: mpsc::UnboundedReceiver<HedgeEvent>,
    pub hyperliquid_prices: Arc<Mutex<(f64, f64)>>,
    pub config: Config,
    pub hyperliquid_trading: Arc<HyperliquidTrading>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub shutdown_tx: mpsc::Sender<()>,
}

impl HedgeService {
    pub async fn run(mut self) {
        let use_ws_for_hedge = self.config.hyperliquid_use_ws_for_hedge;
        let mut ws_write: Option<WsWrite> = None;
        let mut ws_read: Option<WsRead> = None;
        let mut ws_request_id: u64 = 0;

        // Optionally establish trading WebSocket up front so it is hot
        if use_ws_for_hedge {
            match self.connect_hyperliquid_ws().await {
                Ok((write, read)) => {
                    tprintln!(
                        "{} {} Hyperliquid trading WebSocket connected (hedge execution via WS)",
                        format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                        "✓".green().bold(),
                    );
                    ws_write = Some(write);
                    ws_read = Some(read);
                }
                Err(e) => {
                    tprintln!(
                        "{} {} Failed to pre-connect Hyperliquid trading WebSocket (using REST until reconnect succeeds): {}",
                        format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                        "⚠".yellow().bold(),
                        e
                    );
                }
            }
        }

        // Keep-alive interval for WebSocket pings (5s to keep connection warm)
        let mut keepalive_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        keepalive_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Send periodic pings to keep WebSocket connection warm
                _ = keepalive_interval.tick() => {
                    if let Some(write) = ws_write.as_mut() {
                        if let Err(e) = write.send(Message::Ping(vec![])).await {
                            tprintln!("{} {} Failed to send keepalive ping: {}",
                                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                "⚠".yellow().bold(),
                                e
                            );
                            // Connection likely dead, clear write handle
                            ws_write = None;
                        }
                    }
                }

                // Main hedge event processing
                Some((side, size, avg_price, fill_timestamp)) = self.hedge_rx.recv() => {
            let reception_latency = fill_timestamp.elapsed();
            tprintln!("{} ⚡ HEDGE RECEIVED: {} {} @ {} | Reception latency: {:.1}ms",
                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                side.as_str().bright_yellow(),
                size,
                format!("${:.4}", avg_price).cyan(),
                reception_latency.as_secs_f64() * 1000.0
            );

            // *** HYBRID PRE-HEDGE CANCEL: Fire-and-Forget + Fallback ***
            // Phase 1: Launch parallel async cancels (0ms wait for speed)
            // Phase 2: Proceed immediately with hedge (state machine blocks new orders)
            // Phase 3: Monitor async results and retry if needed (safety net)
            tprintln!("{} {} Pre-hedge: Async dual cancel initiated (0ms wait)...",
                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                "⚡".yellow().bold()
            );

            let pac_clone = self.pacifica_trading.clone();
            let symbol_clone = self.config.symbol.clone();

            let cancel_handle = tokio::spawn(async move {
                pac_clone.cancel_all_orders(false, Some(&symbol_clone), false).await
            });

            // Small yield to start sending cancel requests
            tokio::task::yield_now().await;

            // Proceed immediately to hedge execution (state machine already prevents new orders)
            // Async monitoring and fallback retry happens in background

            // Update status
            {
                let mut state = self.bot_state.write().await;
                state.mark_hedging();
            }

            // Execute opposite direction on Hyperliquid
            let is_buy = match side {
                OrderSide::Buy => false, // Filled buy on Pacifica → sell on Hyperliquid
                OrderSide::Sell => true, // Filled sell on Pacifica → buy on Hyperliquid
            };

            let (mut hl_bid, mut hl_ask) = *self.hyperliquid_prices.lock();

            if hl_bid <= 0.0 || hl_ask <= 0.0 {
                tprintln!("{} {} Hyperliquid price cache empty - fetching fresh snapshot before hedging",
                    format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                    "⚠".yellow().bold()
                );

                const MAX_ATTEMPTS: usize = 3;  // Reduced from 5 for faster retry
                const RETRY_DELAY_MS: u64 = 100;  // Reduced from 500ms for faster retry
                for attempt in 1..=MAX_ATTEMPTS {
                    match self.hyperliquid_trading.get_l2_snapshot(&self.config.symbol).await {
                        Ok(Some((bid, ask))) if bid > 0.0 && ask > 0.0 => {
                            hl_bid = bid;
                            hl_ask = ask;
                            let mut cache = self.hyperliquid_prices.lock();
                            *cache = (bid, ask);
                            tprintln!("{} {} Refreshed Hyperliquid prices: bid ${:.4}, ask ${:.4}",
                                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                "✓".green().bold(),
                                hl_bid,
                                hl_ask
                            );
                            break;
                        }
                        Ok(_) => {
                            tprintln!("{} {} Snapshot missing bid/ask data (attempt {}/{})",
                                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                "⚠".yellow().bold(),
                                attempt,
                                MAX_ATTEMPTS
                            );
                        }
                        Err(err) => {
                            tprintln!("{} {} Failed to fetch Hyperliquid snapshot (attempt {}/{}): {}",
                                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                "⚠".yellow().bold(),
                                attempt,
                                MAX_ATTEMPTS,
                                err
                            );
                        }
                    }

                    if attempt < MAX_ATTEMPTS {
                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                        let cached = *self.hyperliquid_prices.lock();
                        hl_bid = cached.0;
                        hl_ask = cached.1;
                        if hl_bid > 0.0 && hl_ask > 0.0 {
                            tprintln!("{} {} Hyperliquid prices populated by feed during wait",
                                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                "✓".green().bold()
                            );
                            break;
                        }
                    }
                }

                if hl_bid <= 0.0 || hl_ask <= 0.0 {
                    tprintln!("{} {} Unable to obtain Hyperliquid prices - aborting hedge for safety",
                        format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                        "✗".red().bold()
                    );

                    let mut state = self.bot_state.write().await;
                    state.set_error("Hyperliquid prices unavailable for hedge".to_string());

                    self.shutdown_tx.send(()).await.ok();
                    return;
                }
            }

            tprintln!(
                "{} Executing {} {} on Hyperliquid",
                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                if is_buy { "BUY".green().bold() } else { "SELL".red().bold() },
                size
            );

            let hedge_result = if use_ws_for_hedge {
                // Ensure we have an active trading WebSocket
                if ws_write.is_none() || ws_read.is_none() {
                    match self.connect_hyperliquid_ws().await {
                        Ok((write, read)) => {
                            tprintln!(
                                "{} {} Reconnected Hyperliquid trading WebSocket for hedge execution",
                                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                "✓".green().bold()
                            );
                            ws_write = Some(write);
                            ws_read = Some(read);
                        }
                        Err(e) => {
                            tprintln!(
                                "{} {} Failed to connect Hyperliquid trading WebSocket, falling back to REST for this hedge: {}",
                                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                "⚠".yellow().bold(),
                                e
                            );
                        }
                    }
                }

                if let (Some(write), Some(read)) = (ws_write.as_mut(), ws_read.as_mut()) {
                    match self
                        .place_market_order_ws(write, read, &mut ws_request_id, is_buy, size, hl_bid, hl_ask)
                        .await
                    {
                        Ok(response) => Ok(response),
                        Err(e) => {
                            tprintln!(
                                "{} {} WebSocket hedge execution failed, falling back to REST: {}",
                                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                "⚠".yellow().bold(),
                                e
                            );
                            // Drop WS so next hedge attempts a clean reconnect
                            ws_write = None;
                            ws_read = None;

                            self.hyperliquid_trading
                                .place_market_order(
                                    &self.config.symbol,
                                    is_buy,
                                    size,
                                    self.config.hyperliquid_slippage,
                                    false, // reduce_only
                                    Some(hl_bid),
                                    Some(hl_ask),
                                )
                                .await
                        }
                    }
                } else {
                    // No WS connection available – use REST for this hedge
                    self.hyperliquid_trading
                        .place_market_order(
                            &self.config.symbol,
                            is_buy,
                            size,
                            self.config.hyperliquid_slippage,
                            false, // reduce_only
                            Some(hl_bid),
                            Some(hl_ask),
                        )
                        .await
                }
            } else {
                // WS disabled via config – use REST only
                self.hyperliquid_trading
                    .place_market_order(
                        &self.config.symbol,
                        is_buy,
                        size,
                        self.config.hyperliquid_slippage,
                        false, // reduce_only
                        Some(hl_bid),
                        Some(hl_ask),
                    )
                    .await
            };

            match hedge_result {
                Ok(response) => {
                    // Extract success data from response
                    let response_data = match &response.response {
                        crate::connector::hyperliquid::OrderResponseContent::Success(data) => data,
                        crate::connector::hyperliquid::OrderResponseContent::Error(error) => {
                            // This should not happen as trading.rs already handles errors,
                            // but handle it defensively
                            tprintln!("{} {} Hedge response contains error: {}",
                                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                "✗".red().bold(),
                                error
                            );

                            let mut state = self.bot_state.write().await;
                            state.set_error(format!("Hedge failed: {}", error));
                            drop(state);

                            self.shutdown_tx.send(()).await.ok();
                            return;
                        }
                    };

                    // Calculate ACTUAL end-to-end latency from fill detection to hedge completion
                    let end_to_end_latency = fill_timestamp.elapsed();

                    // Validate and extract order status
                    let hedge_fill_price = if let Some(status) = response_data.data.statuses.first() {
                        match status {
                            crate::connector::hyperliquid::OrderStatus::Filled { filled } => {
                                let filled_size: f64 = parse(&filled.totalSz).unwrap_or(0.0);
                                let requested_size = size;
                                let fill_ratio = if requested_size > 0.0 { filled_size / requested_size } else { 0.0 };

                                // Check for partial fill (more than 0.5% difference)
                                if fill_ratio < 0.995 {
                                    let unfilled_amount = requested_size - filled_size;
                                    let unfilled_notional = unfilled_amount * parse(&filled.avgPx).unwrap_or(0.0);

                                    tprintln!("{} {} ⚠️ PARTIAL HEDGE FILL DETECTED!",
                                        format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                        "⚠".yellow().bold()
                                    );
                                    tprintln!("{} Requested: {:.6} | Filled: {} ({:.1}%) | Unfilled: {:.6} (~${:.2})",
                                        format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                        requested_size,
                                        filled.totalSz,
                                        fill_ratio * 100.0,
                                        unfilled_amount,
                                        unfilled_notional
                                    );

                                    // If unfilled notional > $5, attempt to hedge the remainder
                                    if unfilled_notional > 5.0 {
                                        tprintln!("{} {} Attempting to hedge unfilled remainder: {:.6}",
                                            format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                            "⚡".yellow().bold(),
                                            unfilled_amount
                                        );

                                        // Re-fetch current prices for remainder hedge
                                        let (remainder_bid, remainder_ask) = *self.hyperliquid_prices.lock();

                                        if remainder_bid > 0.0 && remainder_ask > 0.0 {
                                            // Attempt remainder hedge with 1.5x slippage (conservative per user preference)
                                            let remainder_slippage = self.config.hyperliquid_slippage * 1.5;
                                            match self.hyperliquid_trading
                                                .place_market_order(
                                                    &self.config.symbol,
                                                    is_buy,
                                                    unfilled_amount,
                                                    remainder_slippage,
                                                    false,
                                                    Some(remainder_bid),
                                                    Some(remainder_ask),
                                                )
                                                .await
                                            {
                                                Ok(remainder_response) => {
                                                    if let crate::connector::hyperliquid::OrderResponseContent::Success(data) = &remainder_response.response {
                                                        if let Some(crate::connector::hyperliquid::OrderStatus::Filled { filled: rem_filled }) = data.data.statuses.first() {
                                                            tprintln!("{} {} Remainder hedge successful: {} @ ${}",
                                                                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                                                "✓".green().bold(),
                                                                rem_filled.totalSz,
                                                                rem_filled.avgPx
                                                            );
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    tprintln!("{} {} CRITICAL: Remainder hedge FAILED: {} - POSITION MAY BE UNHEDGED!",
                                                        format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                                        "✗✗✗".red().bold(),
                                                        e
                                                    );
                                                    tprintln!("{} {} Manual intervention required - check positions immediately!",
                                                        format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                                        "⚠".yellow().bold()
                                                    );
                                                }
                                            }
                                        } else {
                                            tprintln!("{} {} Cannot hedge remainder - prices unavailable",
                                                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                                "✗".red().bold()
                                            );
                                        }
                                    } else {
                                        tprintln!("{} {} Unfilled notional ${:.2} below $5 threshold, accepting partial hedge",
                                            format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                            "ℹ".blue().bold(),
                                            unfilled_notional
                                        );
                                    }
                                }

                                tprintln!("{} {} Hedge executed: Filled {} @ ${} | Total latency: {:.1}ms",
                                    format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                    "✓".green().bold(),
                                    filled.totalSz,
                                    filled.avgPx,
                                    end_to_end_latency.as_secs_f64() * 1000.0
                                );
                                filled.avgPx.parse::<f64>().ok()
                            }
                            crate::connector::hyperliquid::OrderStatus::Error { error } => {
                                tprintln!("{} {} Hedge order FAILED: {}",
                                    format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                    "✗".red().bold(),
                                    error
                                );

                                // Set error state
                                {
                                    let mut state = self.bot_state.write().await;
                                    state.set_error(format!("Hedge order failed: {}", error));
                                }

                                // Signal shutdown with error
                                self.shutdown_tx.send(()).await.ok();
                                return;  // Exit hedge service immediately
                            }
                            crate::connector::hyperliquid::OrderStatus::Resting { resting } => {
                                tprintln!("{} {} Hedge order is RESTING (oid: {}) - unexpected for IOC market order",
                                    format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                    "⚠".yellow().bold(),
                                    resting.oid
                                );

                                // Treat as error - IOC orders should never rest
                                {
                                    let mut state = self.bot_state.write().await;
                                    state.set_error(format!("Hedge order resting (unexpected for IOC): oid {}", resting.oid));
                                }

                                self.shutdown_tx.send(()).await.ok();
                                return;
                            }
                        }
                    } else {
                        tprintln!("{} {} Hedge response has no statuses - unexpected API response",
                            format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                            "⚠".yellow().bold()
                        );
                        None
                    };

                    // Validate we got a fill price before continuing
                    let hedge_fill_price = match hedge_fill_price {
                        Some(price) => price,
                        None => {
                            tprintln!("{} {} No hedge fill price available - hedge may have failed",
                                format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                                "✗".red().bold()
                            );

                            {
                                let mut state = self.bot_state.write().await;
                                state.set_error("Hedge execution failed - no fill price".to_string());
                            }

                            self.shutdown_tx.send(()).await.ok();
                            return;
                        }
                    };

                    // Get expected profit from active order before marking complete
                    let expected_profit_bps = {
                        let state = self.bot_state.read().await;
                        state.active_order.as_ref().map(|o| o.initial_profit_bps)
                    };

                    // Wait for trades to propagate to exchange APIs (20 seconds)
                    tprintln!("{} Waiting 20 seconds for trades to propagate to APIs...",
                        format!("[{} PROFIT]", self.config.symbol).bright_blue().bold()
                    );
                    tokio::time::sleep(Duration::from_secs(20)).await;

                    // Get client_order_id from bot state
                    let client_order_id = {
                        let state = self.bot_state.read().await;
                        state.active_order.as_ref().map(|o| o.client_order_id.clone())
                    };

                    // Fetch Pacifica trade history with retry logic
                    let (pacifica_fill_price, pacifica_actual_fee, pacifica_notional): (Option<f64>, Option<f64>, Option<f64>) = if let Some(cloid) = &client_order_id {
                        let result = trade_fetcher::fetch_pacifica_trade(
                            self.pacifica_trading.clone(),
                            &self.config.symbol,
                            &cloid,
                            3, // max_attempts
                            |msg| {
                                tprintln!("{} {}",
                                    format!("[{} PROFIT]", self.config.symbol).bright_blue().bold(),
                                    msg
                                );
                            }
                        ).await;
                        (result.fill_price, result.actual_fee, result.total_notional)
                    } else {
                        (None, None, None)
                    };

                    // Fetch Hyperliquid user fills with retry logic
                    let hl_wallet = std::env::var("HL_WALLET").unwrap_or_default();
                    let (hl_fill_price, hl_actual_fee, hl_notional): (Option<f64>, Option<f64>, Option<f64>) = {
                        let result = trade_fetcher::fetch_hyperliquid_fills(
                            &self.hyperliquid_trading,
                            &hl_wallet,
                            &self.config.symbol,
                            3, // max_attempts
                            30, // time_window_secs
                            |msg| {
                                tprintln!("{} {}",
                                    format!("[{} PROFIT]", self.config.symbol).bright_blue().bold(),
                                    msg
                                );
                            }
                        ).await;
                        (result.fill_price, result.actual_fee, result.total_notional)
                    };

                    // Calculate actual profitability using real fill data and actual fees
                    let (actual_profit_bps, actual_profit_usd, pacifica_actual_price, hl_actual_price, pac_fee_usd, hl_fee_usd) =
                        match (pacifica_notional, hl_notional, pacifica_fill_price, hl_fill_price) {
                            (Some(pac_notional), Some(hl_notional), pac_price_opt, hl_price_opt) => {
                                // Use ACTUAL notional values from exchanges (not recalculated!)
                                // This handles multi-fill trades correctly and avoids Pacifica API bugs

                                // Use actual fees from trade history, or fall back to theoretical
                                let pac_fee = pacifica_actual_fee.unwrap_or_else(|| {
                                    // Fallback: 1.5 bps on notional
                                    pac_notional * (self.config.pacifica_maker_fee_bps / 10000.0)
                                });

                                let hl_fee = hl_actual_fee.unwrap_or_else(|| {
                                    // Fallback: 4 bps on notional
                                    hl_notional * (self.config.hyperliquid_taker_fee_bps / 10000.0)
                                });

                                // Use the shared profit calculation function (same as test utility!)
                                let is_pacifica_buy = matches!(side, OrderSide::Buy);
                                let profit = trade_fetcher::calculate_hedge_profit(
                                    pac_notional,
                                    hl_notional,
                                    pac_fee,
                                    hl_fee,
                                    is_pacifica_buy,
                                );

                                (profit.profit_bps, profit.net_profit, pac_price_opt, hl_price_opt, pac_fee, hl_fee)
                            }
                            _ => {
                                // Fallback to fill event data if trade history unavailable
                                tprintln!("{} {} Using fill event data (trade history unavailable)",
                                    format!("[{} PROFIT]", self.config.symbol).bright_blue().bold(),
                                    "⚠".yellow().bold()
                                );

                                // Calculate profit using fill event prices and estimated fees
                                let hl_price = hedge_fill_price;
                                let pac_price = avg_price;

                                // Estimate fees using configured rates
                                let pac_fee = pac_price * size * (self.config.pacifica_maker_fee_bps / 10000.0);
                                let hl_fee = hl_price * size * (self.config.hyperliquid_taker_fee_bps / 10000.0);

                                // Calculate profit
                                let (profit_usd, cost, _revenue) = match side {
                                    OrderSide::Buy => {
                                        // Bought on Pacifica (maker), Sold on Hyperliquid (taker)
                                        let cost = (pac_price * size) + pac_fee;
                                        let revenue = (hl_price * size) - hl_fee;
                                        (revenue - cost, cost, revenue)
                                    }
                                    OrderSide::Sell => {
                                        // Sold on Pacifica (maker), Bought on Hyperliquid (taker)
                                        let revenue = (pac_price * size) - pac_fee;
                                        let cost = (hl_price * size) + hl_fee;
                                        (revenue - cost, cost, revenue)
                                    }
                                };

                                let profit_rate = if cost > 0.0 { profit_usd / cost } else { 0.0 };
                                let profit_bps = profit_rate * 10000.0;

                                (profit_bps, profit_usd, Some(pac_price), Some(hl_price), pac_fee, hl_fee)
                            }
                        };

                    // Log trade to CSV file
                    if pacifica_actual_price.is_some() && hl_actual_price.is_some() {
                        let trade_record = csv_logger::TradeRecord::new(
                            Utc::now(),
                            end_to_end_latency.as_secs_f64() * 1000.0,  // Convert to milliseconds
                            self.config.symbol.clone(),
                            side,
                            pacifica_actual_price.unwrap(),
                            size,
                            pacifica_notional.unwrap_or(pacifica_actual_price.unwrap() * size),
                            pac_fee_usd,
                            hl_actual_price.unwrap(),
                            size,
                            hl_notional.unwrap_or(hl_actual_price.unwrap() * size),
                            hl_fee_usd,
                            expected_profit_bps.unwrap_or(0.0),
                            actual_profit_bps,
                            actual_profit_usd,
                        );

                        let csv_file = format!("{}_trades.csv", self.config.symbol.to_lowercase());
                        if let Err(e) = csv_logger::log_trade(&csv_file, &trade_record) {
                            tprintln!("{} {} Failed to log trade to CSV: {}",
                                format!("[{} CSV]", self.config.symbol).bright_yellow().bold(),
                                "⚠".yellow().bold(),
                                e
                            );
                        } else {
                            tprintln!("{} {} Trade logged to {}",
                                format!("[{} CSV]", self.config.symbol).bright_green().bold(),
                                "✓".green().bold(),
                                csv_file
                            );
                        }
                    }

                    // Display comprehensive summary
                    tprintln!("{}", "═══════════════════════════════════════════════════".green().bold());
                    tprintln!("{}", "  BOT CYCLE COMPLETE!".green().bold());
                    tprintln!("{}", "═══════════════════════════════════════════════════".green().bold());
                    tprintln!("");
                    tprintln!("{}", "📊 TRADE SUMMARY:".bright_white().bold());
                    if let Some(pac_price) = pacifica_actual_price {
                        tprintln!("  {}: {} {} {} @ {} {}",
                            "Pacifica".bright_magenta(),
                            side.as_str().bright_yellow(),
                            format!("{:.4}", size).bright_white(),
                            self.config.symbol.bright_white().bold(),
                            format!("${:.6}", pac_price).cyan().bold(),
                            "(actual fill)".bright_black()
                        );
                    }
                    if let Some(hl_price) = hl_actual_price {
                        tprintln!("  {}: {} {} {} @ {} {}",
                            "Hyperliquid".bright_magenta(),
                            if is_buy { "BUY".green() } else { "SELL".red() },
                            format!("{:.4}", size).bright_white(),
                            self.config.symbol.bright_white().bold(),
                            format!("${:.6}", hl_price).cyan().bold(),
                            "(actual fill)".bright_black()
                        );
                    }
                    tprintln!("");
                    tprintln!("{}", "💰 PROFITABILITY:".bright_white().bold());
                    if let Some(expected) = expected_profit_bps {
                        tprintln!("  Expected: {} bps", format!("{:.2}", expected).bright_white());
                    }
                    if pacifica_actual_price.is_some() && hl_actual_price.is_some() {
                        let profit_color = if actual_profit_bps > 0.0 { format!("{:.2}", actual_profit_bps).green().bold() } else { format!("{:.2}", actual_profit_bps).red().bold() };
                        let usd_color = if actual_profit_usd > 0.0 { format!("${:.4}", actual_profit_usd).green().bold() } else { format!("${:.4}", actual_profit_usd).red().bold() };
                        tprintln!("  Actual:   {} bps ({})", profit_color, usd_color);
                        if let Some(expected) = expected_profit_bps {
                            let diff = actual_profit_bps - expected;
                            let diff_sign = if diff >= 0.0 { "+" } else { "" };
                            let diff_color = if diff >= 0.0 { format!("{}{:.2}", diff_sign, diff).green() } else { format!("{:.2}", diff).red() };
                            tprintln!("  Difference: {} bps", diff_color);
                        }
                    } else {
                        tprintln!("  {} Unable to calculate actual profit (trade history unavailable)", "⚠".yellow().bold());
                    }
                    tprintln!("");
                    tprintln!("{}", "📈 FEES:".bright_white().bold());
                    if pacifica_actual_price.is_some() && hl_actual_price.is_some() {
                        // Show actual fees paid
                        tprintln!("  Pacifica: {} {}",
                            format!("${:.4}", pac_fee_usd).yellow(),
                            if pacifica_actual_fee.is_some() { "(actual)" } else { "(estimated)" }.bright_black()
                        );
                        tprintln!("  Hyperliquid: {} {}",
                            format!("${:.4}", hl_fee_usd).yellow(),
                            if hl_actual_fee.is_some() { "(actual)" } else { "(estimated)" }.bright_black()
                        );
                        tprintln!("  Total: {}", format!("${:.4}", pac_fee_usd + hl_fee_usd).yellow().bold());
                    } else {
                        // Fallback to theoretical fees
                        tprintln!("  Pacifica (maker): {} bps", format!("{:.2}", self.config.pacifica_maker_fee_bps).yellow());
                        tprintln!("  Hyperliquid (taker): {} bps", format!("{:.2}", self.config.hyperliquid_taker_fee_bps).yellow());
                        tprintln!("  Total fees: {} bps", format!("{:.2}", self.config.pacifica_maker_fee_bps + self.config.hyperliquid_taker_fee_bps).yellow().bold());
                    }
                    tprintln!("{}", "═══════════════════════════════════════════════════".green().bold());

                    // *** CRITICAL: FINAL SAFETY CANCELLATION ***
                    // Cancel all orders one last time before marking complete
                    // *** PHASE 4: Monitor async pre-hedge cancel results and retry if needed ***
                    // Spawn background task to check pre-hedge cancel results and fallback retry
                    let pac_fallback = self.pacifica_trading.clone();
                    let symbol_fallback = self.config.symbol.clone();
                    tokio::spawn(async move {
                        // Check cancel result
                        let cancel_ok = match cancel_handle.await {
                            Ok(Ok(_)) => {
                                debug!("[HEDGE] Pre-hedge cancel succeeded");
                                true
                            }
                            _ => {
                                debug!("[HEDGE] Pre-hedge cancel failed or incomplete");
                                false
                            }
                        };

                        // If failed, attempt synchronous fallback retry
                        if !cancel_ok {
                            tprintln!("{} {} Async pre-hedge cancel incomplete, attempting sync fallback retry...",
                                format!("[{} HEDGE]", symbol_fallback).bright_magenta().bold(),
                                "⚠".yellow().bold()
                            );

                            // Retry
                            match pac_fallback.cancel_all_orders(false, Some(&symbol_fallback), false).await {
                                Ok(count) => {
                                    tprintln!("{} {} Fallback cancel succeeded (cancelled {} orders)",
                                        format!("[{} HEDGE]", symbol_fallback).bright_magenta().bold(),
                                        "✓".green().bold(),
                                        count
                                    );
                                }
                                Err(e) => {
                                    tprintln!("{} {} Fallback cancel also failed: {}",
                                        format!("[{} HEDGE]", symbol_fallback).bright_magenta().bold(),
                                        "⚠".yellow().bold(),
                                        e
                                    );
                                }
                            }
                        }
                    });

                    // This ensures no stray orders remain active
                    tprintln!("");
                    tprintln!("{} {} Post-hedge safety: Final cancellation of all Pacifica orders...",
                        format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                        "⚡".yellow().bold()
                    );

                    if let Err(e) = self.pacifica_trading
                        .cancel_all_orders(false, Some(&self.config.symbol), false)
                        .await
                    {
                        tprintln!("{} {} Failed to cancel orders after hedge completion: {}",
                            format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                            "⚠".yellow().bold(),
                            e
                        );
                    } else {
                        tprintln!("{} {} Final cancellation complete",
                            format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                            "✓".green().bold()
                        );
                    }

                    // *** POST-HEDGE POSITION VERIFICATION ***
                    // Wait for positions to propagate and verify net position is neutral
                    tprintln!("");
                    tprintln!("{} {} Post-hedge verification: Waiting 8 seconds for positions to propagate...",
                        format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                        "⏱".cyan().bold()
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(8)).await;

                    tprintln!("{} Verifying final positions on both exchanges...",
                        format!("[{} VERIFY]", self.config.symbol).cyan().bold()
                    );

                    // Check Pacifica position
                    let pacifica_position = match self.pacifica_trading.get_positions().await {
                        Ok(positions) => {
                            if let Some(pos) = positions.iter().find(|p| p.symbol == self.config.symbol) {
                                let amount: f64 = parse(&pos.amount).unwrap_or(0.0);
                                let signed_amount = if pos.side == "bid" { amount } else { -amount };

                                tprintln!("{} Pacifica: {} {} (signed: {:.4})",
                                    format!("[{} VERIFY]", self.config.symbol).cyan().bold(),
                                    amount,
                                    pos.side,
                                    signed_amount
                                );
                                Some(signed_amount)
                            } else {
                                tprintln!("{} Pacifica: No position (flat)",
                                    format!("[{} VERIFY]", self.config.symbol).cyan().bold()
                                );
                                Some(0.0)
                            }
                        }
                        Err(e) => {
                            tprintln!("{} {} Failed to fetch Pacifica position: {}",
                                format!("[{} VERIFY]", self.config.symbol).yellow().bold(),
                                "⚠".yellow().bold(),
                                e
                            );
                            None
                        }
                    };

                    // Check Hyperliquid position
                    let hl_wallet = std::env::var("HL_WALLET").unwrap_or_default();
                    let mut hyperliquid_position: Option<f64> = None;

                    // Try up to 3 times with delays if position not found
                    for retry in 0..3 {
                        if retry > 0 {
                            tprintln!("{} Retry {} - waiting 3 more seconds for Hyperliquid position...",
                                format!("[{} VERIFY]", self.config.symbol).cyan().bold(),
                                retry
                            );
                            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        }

                        match self.hyperliquid_trading.get_user_state(&hl_wallet).await {
                            Ok(user_state) => {
                                if let Some(asset_pos) = user_state.asset_positions.iter().find(|ap| ap.position.coin == self.config.symbol) {
                                    let szi: f64 = parse(&asset_pos.position.szi).unwrap_or(0.0);
                                    tprintln!("{} Hyperliquid: {} (signed: {:.4})",
                                        format!("[{} VERIFY]", self.config.symbol).cyan().bold(),
                                        if szi > 0.0 { "LONG".green() } else if szi < 0.0 { "SHORT".red() } else { "FLAT".bright_white() },
                                        szi
                                    );
                                    hyperliquid_position = Some(szi);
                                    break;
                                } else if retry == 2 {
                                    tprintln!("{} Hyperliquid: No position found after 3 attempts (flat)",
                                        format!("[{} VERIFY]", self.config.symbol).cyan().bold()
                                    );
                                    hyperliquid_position = Some(0.0);
                                }
                            }
                            Err(e) => {
                                if retry == 2 {
                                    tprintln!("{} {} Failed to fetch Hyperliquid position after 3 attempts: {}",
                                        format!("[{} VERIFY]", self.config.symbol).yellow().bold(),
                                        "⚠".yellow().bold(),
                                        e
                                    );
                                    hyperliquid_position = None;
                                }
                            }
                        }
                    }

                    // Calculate net position across both exchanges
                    if let (Some(pac_pos), Some(hl_pos)) = (pacifica_position, hyperliquid_position) {
                        let net_position = pac_pos + hl_pos;

                        tprintln!("");
                        tprintln!("{} Net Position: {:.4} (Pacifica: {:.4} + Hyperliquid: {:.4})",
                            format!("[{} VERIFY]", self.config.symbol).cyan().bold(),
                            net_position,
                            pac_pos,
                            hl_pos
                        );

                        // Calculate dynamic threshold based on order size and current price
                        // Use 0.5% of the hedge size as tolerance, with minimum of 0.0001 for dust
                        let (current_bid, current_ask) = *self.hyperliquid_prices.lock();
                        let current_price = if current_bid > 0.0 && current_ask > 0.0 {
                            (current_bid + current_ask) / 2.0
                        } else {
                            // Fallback to using the fill price from hedge
                            hedge_fill_price
                        };

                        // Dynamic threshold: max(0.5% of hedge size, $0.10 worth, 0.0001 units)
                        let size_based_threshold = size * 0.005;  // 0.5% of order size
                        let notional_threshold = if current_price > 0.0 { 0.10 / current_price } else { 0.0001 };  // $0.10 worth
                        let position_tolerance = size_based_threshold
                            .max(notional_threshold)
                            .max(0.0001);  // Absolute minimum for dust

                        tprintln!("{} Position tolerance: {:.6} {} (based on size {:.4} @ ${:.2})",
                            format!("[{} VERIFY]", self.config.symbol).cyan().bold(),
                            position_tolerance,
                            self.config.symbol,
                            size,
                            current_price
                        );

                        // Check if net position is close to neutral
                        if net_position.abs() < position_tolerance {
                            tprintln!("{} {} Net position {:.6} is within tolerance {:.6} (properly hedged)",
                                format!("[{} VERIFY]", self.config.symbol).cyan().bold(),
                                "✓".green().bold(),
                                net_position.abs(),
                                position_tolerance
                            );
                        } else {
                            let unhedged_notional = net_position.abs() * current_price;
                            tprintln!("");
                            tprintln!("{}", "⚠".repeat(80).yellow());
                            tprintln!("{} {} WARNING: Net position NOT neutral!",
                                format!("[{} VERIFY]", self.config.symbol).red().bold(),
                                "⚠".yellow().bold()
                            );
                            tprintln!("{} Position delta: {:.6} {} (~${:.2} unhedged exposure)",
                                format!("[{} VERIFY]", self.config.symbol).red().bold(),
                                net_position.abs(),
                                self.config.symbol,
                                unhedged_notional
                            );
                            tprintln!("{} Tolerance was: {:.6} {}",
                                format!("[{} VERIFY]", self.config.symbol).red().bold(),
                                position_tolerance,
                                self.config.symbol
                            );
                            tprintln!("{} This indicates a potential hedge failure or partial fill.",
                                format!("[{} VERIFY]", self.config.symbol).red().bold()
                            );
                            tprintln!("{} Please check positions manually and rebalance if needed!",
                                format!("[{} VERIFY]", self.config.symbol).red().bold()
                            );
                            tprintln!("{}", "⚠".repeat(80).yellow());
                            tprintln!("");
                        }
                    } else {
                        tprintln!("");
                        tprintln!("{} {} WARNING: Could not verify net position!",
                            format!("[{} VERIFY]", self.config.symbol).yellow().bold(),
                            "⚠".yellow().bold()
                        );
                        tprintln!("{} Failed to fetch positions from one or both exchanges.",
                            format!("[{} VERIFY]", self.config.symbol).yellow().bold()
                        );
                        tprintln!("{} Please check positions manually!",
                            format!("[{} VERIFY]", self.config.symbol).yellow().bold()
                        );
                        tprintln!("");
                    }

                    // Mark cycle as complete AFTER displaying profit AND final cancellation
                    let mut state = self.bot_state.write().await;
                    state.mark_complete();
                    drop(state);

                    // Signal shutdown
                    self.shutdown_tx.send(()).await.ok();
                }
                Err(e) => {
                    tprintln!("{} {} Hedge failed: {}",
                        format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                        "✗".red().bold(),
                        e.to_string().red()
                    );

                    // *** CRITICAL: CANCEL ALL ORDERS ON ERROR ***
                    // Even if hedge fails, cancel all orders to prevent stray positions
                    tprintln!("{} {} Error recovery: Cancelling all Pacifica orders...",
                        format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                        "⚡".yellow().bold()
                    );

                    if let Err(cancel_err) = self.pacifica_trading
                        .cancel_all_orders(false, Some(&self.config.symbol), false)
                        .await
                    {
                        tprintln!("{} {} Failed to cancel orders after hedge error: {}",
                            format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                            "⚠".yellow().bold(),
                            cancel_err
                        );
                    } else {
                        tprintln!("{} {} Error recovery cancellation complete",
                            format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                            "✓".green().bold()
                        );
                    }

                    let mut state = self.bot_state.write().await;
                    state.set_error(format!("Hedge failed: {}", e));

                    // Signal shutdown with error
                    self.shutdown_tx.send(()).await.ok();
                }
            }
                } // Close Some((side, size, avg_price, fill_timestamp)) arm
            } // Close tokio::select!
        } // Close loop
    }

    /// Establish a Hyperliquid trading WebSocket connection for hedging.
    async fn connect_hyperliquid_ws(&self) -> anyhow::Result<(WsWrite, WsRead)> {
        let ws_url = if self.hyperliquid_trading.is_testnet() {
            "wss://api.hyperliquid-testnet.xyz/ws"
        } else {
            "wss://api.hyperliquid.xyz/ws"
        };

        let (ws_stream, _) = connect_async(ws_url).await?;
        let (write, read) = ws_stream.split();
        Ok((write, read))
    }

    /// Place a market IOC order over Hyperliquid WebSocket using the shared
    /// REST signing and request-building logic.
    async fn place_market_order_ws(
        &self,
        write: &mut WsWrite,
        read: &mut WsRead,
        request_id_counter: &mut u64,
        is_buy: bool,
        size: f64,
        bid: f64,
        ask: f64,
    ) -> anyhow::Result<crate::connector::hyperliquid::OrderResponse> {
        // Build signed order payload (same as REST)
        let payload = self
            .hyperliquid_trading
            .build_market_order_request(
                &self.config.symbol,
                is_buy,
                size,
                self.config.hyperliquid_slippage,
                false,
                Some(bid),
                Some(ask),
            )
            .await?;

        *request_id_counter += 1;
        let request_id = *request_id_counter;

        let ws_request = WsPostRequest {
            method: "post".to_string(),
            id: request_id,
            request: WsPostRequestInner {
                type_: "action".to_string(),
                payload,
            },
        };

        let request_json = serde_json::to_string(&ws_request)?;
        tprintln!(
            "{} Sending Hyperliquid hedge order via WebSocket (id={})",
            format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
            request_id
        );
        write.send(Message::Text(request_json)).await?;

        // Wait for the matching post response
        loop {
            match read.next().await {
                Some(Ok(Message::Text(text))) => {
                    // Try to parse as a generic post response
                    let ws_resp: WsPostResponse = match serde_json::from_str(&text) {
                        Ok(r) => r,
                        Err(_) => {
                            // Ignore unrelated/non-standard messages
                            continue;
                        }
                    };

                    if ws_resp.channel != "post" || ws_resp.data.id != request_id {
                        // Response for another request or channel – ignore
                        continue;
                    }

                    let resp_type = ws_resp.data.response.type_;
                    let payload = ws_resp.data.response.payload;

                    return match resp_type.as_str() {
                        "action" => {
                            let order_response: crate::connector::hyperliquid::OrderResponse =
                                serde_json::from_value(payload)?;
                            Ok(order_response)
                        }
                        "error" => {
                            let msg = payload
                                .as_str()
                                .unwrap_or("Unknown Hyperliquid WebSocket error")
                                .to_string();
                            anyhow::bail!("Hyperliquid WebSocket order error: {}", msg);
                        }
                        other => {
                            anyhow::bail!("Unexpected Hyperliquid WebSocket response type: {}", other);
                        }
                    };
                }
                Some(Ok(Message::Ping(data))) => {
                    // Respond to low-level WebSocket ping frames
                    write.send(Message::Pong(data)).await?;
                }
                Some(Ok(Message::Pong(_))) => {
                    // Ignore
                }
                Some(Ok(Message::Close(frame))) => {
                    anyhow::bail!("Hyperliquid WebSocket closed: {:?}", frame);
                }
                Some(Err(e)) => {
                    anyhow::bail!("Hyperliquid WebSocket error: {}", e);
                }
                None => {
                    anyhow::bail!("Hyperliquid WebSocket stream ended unexpectedly");
                }
                _ => {}
            }
        }
    }
}
