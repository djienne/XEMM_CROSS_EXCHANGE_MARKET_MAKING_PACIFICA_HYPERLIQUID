use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use colored::Colorize;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};
use fast_float::parse;
use tracing::{info, warn, error, debug};

use crate::bot::BotState;
use crate::config::Config;
use crate::connector::hyperliquid::HyperliquidTrading;
use crate::connector::hyperliquid::types::{OrderResponseData, WsPostRequest, WsPostRequestInner, WsPostResponse};
use crate::connector::pacifica::PacificaTrading;
use crate::services::HedgeEvent;
use crate::strategy::OrderSide;
use crate::util::atomic_price::AtomicPrice;
use crate::trade_fetcher;
use crate::csv_logger;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWrite = futures_util::stream::SplitSink<WsStream, Message>;
type WsRead = futures_util::stream::SplitStream<WsStream>;

/// Result of profit calculation from actual trade data.
struct ProfitCalculation {
    profit_bps: f64,
    profit_usd: f64,
    pac_price: Option<f64>,
    hl_price: Option<f64>,
    pac_fee_usd: f64,
    hl_fee_usd: f64,
}

/// Pre-cached asset metadata for low-latency hedge execution
#[derive(Debug, Clone)]
pub struct CachedAssetMeta {
    pub asset_id: u32,
    pub sz_decimals: i32,
}

/// Hedge execution service
///
/// Receives hedge triggers via mpsc channel and executes the hedge flow:
/// 1. Pre-hedge cancellation of all Pacifica orders
/// 2. Execute market order on Hyperliquid (opposite direction)
/// 3. Wait for trade propagation
/// 4. Fetch trade history and calculate actual profit
/// 5. Display trade summary and verify positions
/// 6. Mark cycle complete and signal shutdown
/// Status value for Complete state
const STATUS_COMPLETE: u8 = 4;

pub struct HedgeService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_rx: mpsc::UnboundedReceiver<HedgeEvent>,
    pub hyperliquid_prices: Arc<AtomicPrice>,
    pub config: Config,
    pub hyperliquid_trading: Arc<HyperliquidTrading>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub shutdown_tx: mpsc::Sender<()>,
    /// Pre-cached asset metadata (asset_id, sz_decimals) for low-latency hedge
    pub cached_asset_meta: Option<CachedAssetMeta>,
    /// Pre-cached log prefix (avoid allocation in hot path)
    pub log_prefix_cached: String,
    /// Atomic status for fast-path state checks
    pub atomic_status: Arc<AtomicU8>,
}

impl HedgeService {
    pub async fn run(mut self) {
        let use_ws_for_hedge = self.config.hyperliquid_use_ws_for_hedge;
        let mut ws_write: Option<WsWrite> = None;
        let mut ws_read: Option<WsRead> = None;
        let mut ws_request_id: u64 = 0;

        // Local dedup set - no Arc, no Mutex needed (single consumer)
        let mut processed_orders: HashSet<String> = HashSet::new();

        // Pre-connect WebSocket if enabled
        if use_ws_for_hedge {
            if let Ok((write, read)) = self.connect_hyperliquid_ws().await {
                info!("{} {} Hyperliquid trading WebSocket connected",
                    self.log_prefix(), "✓".green().bold());
                ws_write = Some(write);
                ws_read = Some(read);
            } else {
                warn!("{} {} WebSocket pre-connect failed, will use REST",
                    self.log_prefix(), "⚠".yellow().bold());
            }
        }

        // Keep-alive interval for WebSocket pings
        let mut keepalive_interval = tokio::time::interval(Duration::from_secs(5));
        keepalive_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = keepalive_interval.tick() => {
                    if let Some(write) = ws_write.as_mut() {
                        if write.send(Message::Ping(vec![])).await.is_err() {
                            ws_write = None;
                        }
                    }
                }

                Some(event) = self.hedge_rx.recv() => {
                    // Dedup: skip if already processed (multiple fill detectors may send same fill)
                    if processed_orders.contains(&event.client_order_id) {
                        debug!("{} Duplicate hedge event for {}, skipping",
                            self.log_prefix(), event.client_order_id);
                        continue;
                    }
                    processed_orders.insert(event.client_order_id.clone());

                    let reception_latency = event.fill_timestamp.elapsed();
                    info!("{} HEDGE RECEIVED: {} {} @ {} | Latency: {:.1}ms",
                        self.log_prefix(),
                        event.side.as_str().bright_yellow(),
                        event.size,
                        format!("${:.4}", event.avg_price).cyan(),
                        reception_latency.as_secs_f64() * 1000.0
                    );

                    // Spawn background pre-hedge cancellation
                    self.spawn_pre_hedge_cancellation();

                    // Update status to Hedging
                    {
                        let mut state = self.bot_state.write().await;
                        state.mark_hedging();
                    }

                    // Ensure we have valid Hyperliquid prices
                    let (hl_bid, hl_ask) = match self.ensure_hyperliquid_prices().await {
                        Some(prices) => prices,
                        None => {
                            self.handle_fatal_error("Hyperliquid prices unavailable for hedge").await;
                            return;
                        }
                    };

                    // Determine hedge direction (opposite of fill)
                    let is_buy = matches!(event.side, OrderSide::Sell);

                    info!("{} Executing {} {} on Hyperliquid",
                        self.log_prefix(),
                        if is_buy { "BUY".green().bold() } else { "SELL".red().bold() },
                        event.size
                    );

                    // Execute hedge order
                    let hedge_result = self.execute_hedge_order(
                        &mut ws_write, &mut ws_read, &mut ws_request_id,
                        use_ws_for_hedge, is_buy, event.size, hl_bid, hl_ask
                    ).await;

                    match hedge_result {
                        Ok(response) => {
                            self.handle_successful_hedge(
                                response, event.fill_timestamp, event.side, event.size, event.avg_price, is_buy
                            ).await;
                        }
                        Err(e) => {
                            self.handle_hedge_error(&e.to_string()).await;
                        }
                    }
                }
            }
        }
    }

    fn log_prefix(&self) -> &str {
        &self.log_prefix_cached
    }

    fn spawn_pre_hedge_cancellation(&self) {
        let pacifica_trading = self.pacifica_trading.clone();
        let symbol = self.config.symbol.clone();
        let prefix = self.log_prefix_cached.clone();

        tokio::spawn(async move {
            info!("{} {} Pre-hedge: Cancelling all Pacifica orders...",
                prefix, "⚡".yellow().bold());

            match pacifica_trading.cancel_all_orders(false, Some(&symbol), false).await {
                Ok(_) => info!("{} {} Pre-hedge cancellation complete", prefix, "✓".green().bold()),
                Err(e) => warn!("{} {} Pre-hedge cancellation failed: {}", prefix, "⚠".yellow().bold(), e),
            }
        });
    }

    async fn ensure_hyperliquid_prices(&self) -> Option<(f64, f64)> {
        let (hl_bid, hl_ask) = self.hyperliquid_prices.load_bid_ask();

        if hl_bid > 0.0 && hl_ask > 0.0 {
            return Some((hl_bid, hl_ask));
        }

        warn!("{} {} Price cache empty - fetching fresh snapshot",
            self.log_prefix(), "⚠".yellow().bold());

        for attempt in 1..=5 {
            match self.hyperliquid_trading.get_l2_snapshot(&self.config.symbol).await {
                Ok(Some((bid, ask))) if bid > 0.0 && ask > 0.0 => {
                    self.hyperliquid_prices.store(bid, ask, 0);
                    info!("{} {} Refreshed prices: bid ${:.4}, ask ${:.4}",
                        self.log_prefix(), "✓".green().bold(), bid, ask);
                    return Some((bid, ask));
                }
                Ok(_) => warn!("{} {} Snapshot missing data (attempt {}/5)",
                    self.log_prefix(), "⚠".yellow().bold(), attempt),
                Err(e) => warn!("{} {} Snapshot fetch failed (attempt {}/5): {}",
                    self.log_prefix(), "⚠".yellow().bold(), attempt, e),
            }

            if attempt < 5 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let cached = self.hyperliquid_prices.load_bid_ask();
                if cached.0 > 0.0 && cached.1 > 0.0 {
                    return Some(cached);
                }
            }
        }

        error!("{} {} Unable to obtain prices - aborting hedge",
            self.log_prefix(), "✗".red().bold());
        None
    }

    async fn execute_hedge_order(
        &self,
        ws_write: &mut Option<WsWrite>,
        ws_read: &mut Option<WsRead>,
        ws_request_id: &mut u64,
        use_ws: bool,
        is_buy: bool,
        size: f64,
        hl_bid: f64,
        hl_ask: f64,
    ) -> anyhow::Result<crate::connector::hyperliquid::OrderResponse> {
        if !use_ws {
            return self.place_hedge_order_rest(is_buy, size, hl_bid, hl_ask).await;
        }

        // Ensure WebSocket connection
        if ws_write.is_none() || ws_read.is_none() {
            if let Ok((write, read)) = self.connect_hyperliquid_ws().await {
                info!("{} {} WebSocket reconnected", self.log_prefix(), "✓".green().bold());
                *ws_write = Some(write);
                *ws_read = Some(read);
            }
        }

        // Try WebSocket, fall back to REST
        if let (Some(write), Some(read)) = (ws_write.as_mut(), ws_read.as_mut()) {
            match self.place_market_order_ws(write, read, ws_request_id, is_buy, size, hl_bid, hl_ask).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    warn!("{} {} WebSocket failed, falling back to REST: {}",
                        self.log_prefix(), "⚠".yellow().bold(), e);
                    *ws_write = None;
                    *ws_read = None;
                }
            }
        }

        self.place_hedge_order_rest(is_buy, size, hl_bid, hl_ask).await
    }

    async fn place_hedge_order_rest(
        &self,
        is_buy: bool,
        size: f64,
        hl_bid: f64,
        hl_ask: f64,
    ) -> anyhow::Result<crate::connector::hyperliquid::OrderResponse> {
        self.hyperliquid_trading
            .place_market_order(
                &self.config.symbol,
                is_buy,
                size,
                self.config.hyperliquid_slippage,
                false,
                Some(hl_bid),
                Some(hl_ask),
            )
            .await
    }

    async fn handle_successful_hedge(
        &self,
        response: crate::connector::hyperliquid::OrderResponse,
        fill_timestamp: std::time::Instant,
        side: OrderSide,
        size: f64,
        avg_price: f64,
        is_buy: bool,
    ) {
        // Extract and validate response
        let response_data = match &response.response {
            crate::connector::hyperliquid::OrderResponseContent::Success(data) => data,
            crate::connector::hyperliquid::OrderResponseContent::Error(error) => {
                self.handle_hedge_error(error).await;
                return;
            }
        };

        let end_to_end_latency = fill_timestamp.elapsed();

        // Extract fill price from response
        let hedge_fill_price = match self.extract_fill_price(response_data, end_to_end_latency).await {
            Some(price) => price,
            None => return,
        };

        // Get expected profit from active order
        let expected_profit_bps = {
            let state = self.bot_state.read().await;
            state.active_order.as_ref().map(|o| o.initial_profit_bps)
        };

        // Wait for trades to propagate
        info!("{} Waiting {}s for trades to propagate...",
            format!("[{} PROFIT]", self.config.symbol).bright_blue().bold(),
            self.config.trade_propagation_wait_secs
        );
        tokio::time::sleep(Duration::from_secs(self.config.trade_propagation_wait_secs)).await;

        // Calculate actual profit from trade history
        let profit = self.calculate_actual_profit(side, size, avg_price, hedge_fill_price).await;

        // Log trade to CSV
        self.log_trade_to_csv(
            end_to_end_latency, side, size, &profit, expected_profit_bps
        );

        // Display summary
        self.display_trade_summary(side, size, is_buy, &profit, expected_profit_bps);

        // Post-hedge cleanup
        self.post_hedge_cancellation().await;
        self.verify_final_positions().await;

        // Mark complete and shutdown
        {
            let mut state = self.bot_state.write().await;
            state.mark_complete();
        }
        // Sync atomic status after RwLock state update
        self.atomic_status.store(STATUS_COMPLETE, Ordering::Release);
        self.shutdown_tx.send(()).await.ok();
    }

    async fn extract_fill_price(
        &self,
        response_data: &OrderResponseData,
        latency: Duration,
    ) -> Option<f64> {
        if let Some(status) = response_data.data.statuses.first() {
            match status {
                crate::connector::hyperliquid::OrderStatus::Filled { filled } => {
                    info!("{} {} Hedge filled: {} @ ${} | Latency: {:.1}ms",
                        self.log_prefix(), "✓".green().bold(),
                        filled.totalSz, filled.avgPx,
                        latency.as_secs_f64() * 1000.0
                    );
                    return filled.avgPx.parse::<f64>().ok();
                }
                crate::connector::hyperliquid::OrderStatus::Error { error } => {
                    self.handle_hedge_error(&error).await;
                    return None;
                }
                crate::connector::hyperliquid::OrderStatus::Resting { resting } => {
                    self.handle_fatal_error(&format!("Order resting (unexpected): oid {}", resting.oid)).await;
                    return None;
                }
            }
        }

        warn!("{} {} No statuses in response", self.log_prefix(), "⚠".yellow().bold());
        None
    }

    async fn calculate_actual_profit(
        &self,
        side: OrderSide,
        size: f64,
        fill_event_price: f64,
        hedge_fill_price: f64,
    ) -> ProfitCalculation {
        let client_order_id = {
            let state = self.bot_state.read().await;
            state.active_order.as_ref().map(|o| o.client_order_id.clone())
        };

        // Fetch Pacifica trade
        let (pac_price, pac_fee, pac_notional) = if let Some(cloid) = &client_order_id {
            let result = trade_fetcher::fetch_pacifica_trade(
                self.pacifica_trading.clone(),
                &self.config.symbol,
                cloid,
                3,
                |msg| info!("{} {}", format!("[{} PROFIT]", self.config.symbol).bright_blue().bold(), msg),
            ).await;
            (result.fill_price, result.actual_fee, result.total_notional)
        } else {
            (None, None, None)
        };

        // Fetch Hyperliquid fills
        let hl_wallet = std::env::var("HL_WALLET").unwrap_or_default();
        let hl_result = trade_fetcher::fetch_hyperliquid_fills(
            &self.hyperliquid_trading,
            &hl_wallet,
            &self.config.symbol,
            3,
            30,
            |msg| info!("{} {}", format!("[{} PROFIT]", self.config.symbol).bright_blue().bold(), msg),
        ).await;
        let (hl_price, hl_fee, hl_notional) = (hl_result.fill_price, hl_result.actual_fee, hl_result.total_notional);

        // Calculate profit
        match (pac_notional, hl_notional) {
            (Some(pac_not), Some(hl_not)) => {
                let pac_fee_usd = pac_fee.unwrap_or(pac_not * (self.config.pacifica_maker_fee_bps / 10000.0));
                let hl_fee_usd = hl_fee.unwrap_or(hl_not * (self.config.hyperliquid_taker_fee_bps / 10000.0));

                let is_pacifica_buy = matches!(side, OrderSide::Buy);
                let profit = trade_fetcher::calculate_hedge_profit(pac_not, hl_not, pac_fee_usd, hl_fee_usd, is_pacifica_buy);

                ProfitCalculation {
                    profit_bps: profit.profit_bps,
                    profit_usd: profit.net_profit,
                    pac_price,
                    hl_price,
                    pac_fee_usd,
                    hl_fee_usd,
                }
            }
            _ => {
                // Fallback to fill event data
                warn!("{} {} Using fill event data (trade history unavailable)",
                    format!("[{} PROFIT]", self.config.symbol).bright_blue().bold(), "⚠".yellow().bold());

                let pac_fee_usd = fill_event_price * size * (self.config.pacifica_maker_fee_bps / 10000.0);
                let hl_fee_usd = hedge_fill_price * size * (self.config.hyperliquid_taker_fee_bps / 10000.0);

                let (profit_usd, cost) = match side {
                    OrderSide::Buy => {
                        let cost = (fill_event_price * size) + pac_fee_usd;
                        let revenue = (hedge_fill_price * size) - hl_fee_usd;
                        (revenue - cost, cost)
                    }
                    OrderSide::Sell => {
                        let revenue = (fill_event_price * size) - pac_fee_usd;
                        let cost = (hedge_fill_price * size) + hl_fee_usd;
                        (revenue - cost, cost)
                    }
                };

                let profit_bps = if cost > 0.0 { (profit_usd / cost) * 10000.0 } else { 0.0 };

                ProfitCalculation {
                    profit_bps,
                    profit_usd,
                    pac_price: Some(fill_event_price),
                    hl_price: Some(hedge_fill_price),
                    pac_fee_usd,
                    hl_fee_usd,
                }
            }
        }
    }

    fn log_trade_to_csv(
        &self,
        latency: Duration,
        side: OrderSide,
        size: f64,
        profit: &ProfitCalculation,
        expected_profit_bps: Option<f64>,
    ) {
        if profit.pac_price.is_none() || profit.hl_price.is_none() {
            return;
        }

        let pac_price = profit.pac_price.unwrap();
        let hl_price = profit.hl_price.unwrap();

        let trade_record = csv_logger::TradeRecord::new(
            Utc::now(),
            latency.as_secs_f64() * 1000.0,
            self.config.symbol.clone(),
            side,
            pac_price,
            size,
            pac_price * size,
            profit.pac_fee_usd,
            hl_price,
            size,
            hl_price * size,
            profit.hl_fee_usd,
            expected_profit_bps.unwrap_or(0.0),
            profit.profit_bps,
            profit.profit_usd,
        );

        let csv_file = format!("{}_trades.csv", self.config.symbol.to_lowercase());
        match csv_logger::log_trade(&csv_file, &trade_record) {
            Ok(_) => info!("{} {} Trade logged to {}",
                format!("[{} CSV]", self.config.symbol).bright_green().bold(),
                "✓".green().bold(), csv_file),
            Err(e) => warn!("{} {} Failed to log trade: {}",
                format!("[{} CSV]", self.config.symbol).bright_yellow().bold(),
                "⚠".yellow().bold(), e),
        }
    }

    fn display_trade_summary(
        &self,
        side: OrderSide,
        size: f64,
        is_buy: bool,
        profit: &ProfitCalculation,
        expected_profit_bps: Option<f64>,
    ) {
        info!("{}", "═══════════════════════════════════════════════════".green().bold());
        info!("{}", "  BOT CYCLE COMPLETE!".green().bold());
        info!("{}", "═══════════════════════════════════════════════════".green().bold());
        info!("");

        // Trade summary
        info!("{}", "TRADE SUMMARY:".bright_white().bold());
        if let Some(pac_price) = profit.pac_price {
            info!("  Pacifica: {} {:.4} {} @ ${:.6}",
                side.as_str().bright_yellow(),
                size, self.config.symbol.bright_white().bold(),
                pac_price);
        }
        if let Some(hl_price) = profit.hl_price {
            info!("  Hyperliquid: {} {:.4} {} @ ${:.6}",
                if is_buy { "BUY".green() } else { "SELL".red() },
                size, self.config.symbol.bright_white().bold(),
                hl_price);
        }

        // Profitability
        info!("");
        info!("{}", "PROFITABILITY:".bright_white().bold());
        if let Some(expected) = expected_profit_bps {
            info!("  Expected: {:.2} bps", expected);
        }
        if profit.pac_price.is_some() && profit.hl_price.is_some() {
            let profit_color = if profit.profit_bps > 0.0 {
                format!("{:.2}", profit.profit_bps).green().bold()
            } else {
                format!("{:.2}", profit.profit_bps).red().bold()
            };
            let usd_color = if profit.profit_usd > 0.0 {
                format!("${:.4}", profit.profit_usd).green().bold()
            } else {
                format!("${:.4}", profit.profit_usd).red().bold()
            };
            info!("  Actual:   {} bps ({})", profit_color, usd_color);

            if let Some(expected) = expected_profit_bps {
                let diff = profit.profit_bps - expected;
                let diff_str = if diff >= 0.0 {
                    format!("+{:.2}", diff).green()
                } else {
                    format!("{:.2}", diff).red()
                };
                info!("  Difference: {} bps", diff_str);
            }
        }

        // Fees
        info!("");
        info!("{}", "FEES:".bright_white().bold());
        info!("  Pacifica: ${:.4}", profit.pac_fee_usd);
        info!("  Hyperliquid: ${:.4}", profit.hl_fee_usd);
        info!("  Total: ${:.4}", profit.pac_fee_usd + profit.hl_fee_usd);
        info!("{}", "═══════════════════════════════════════════════════".green().bold());
    }

    async fn post_hedge_cancellation(&self) {
        info!("");
        info!("{} {} Post-hedge: Final cancellation...", self.log_prefix(), "⚡".yellow().bold());

        match self.pacifica_trading.cancel_all_orders(false, Some(&self.config.symbol), false).await {
            Ok(_) => info!("{} {} Final cancellation complete", self.log_prefix(), "✓".green().bold()),
            Err(e) => warn!("{} {} Final cancellation failed: {}", self.log_prefix(), "⚠".yellow().bold(), e),
        }
    }

    async fn verify_final_positions(&self) {
        info!("");
        info!("{} {} Verifying positions (waiting 8s for propagation)...",
            format!("[{} VERIFY]", self.config.symbol).cyan().bold(), "⏱".cyan().bold());
        tokio::time::sleep(Duration::from_secs(8)).await;

        // Check Pacifica position
        let pacifica_position = match self.pacifica_trading.get_positions().await {
            Ok(positions) => {
                if let Some(pos) = positions.iter().find(|p| p.symbol == self.config.symbol) {
                    let amount: f64 = parse(&pos.amount).unwrap_or(0.0);
                    let signed = if pos.side == "bid" { amount } else { -amount };
                    info!("{} Pacifica: {:.4} ({})",
                        format!("[{} VERIFY]", self.config.symbol).cyan().bold(), signed, pos.side);
                    Some(signed)
                } else {
                    info!("{} Pacifica: flat", format!("[{} VERIFY]", self.config.symbol).cyan().bold());
                    Some(0.0)
                }
            }
            Err(e) => {
                warn!("{} {} Pacifica position fetch failed: {}",
                    format!("[{} VERIFY]", self.config.symbol).yellow().bold(), "⚠".yellow().bold(), e);
                None
            }
        };

        // Check Hyperliquid position (with retries)
        let hl_wallet = std::env::var("HL_WALLET").unwrap_or_default();
        let mut hl_position: Option<f64> = None;

        for retry in 0..3 {
            if retry > 0 {
                tokio::time::sleep(Duration::from_secs(3)).await;
            }

            match self.hyperliquid_trading.get_user_state(&hl_wallet).await {
                Ok(state) => {
                    if let Some(ap) = state.asset_positions.iter().find(|ap| ap.position.coin == self.config.symbol) {
                        let szi: f64 = parse(&ap.position.szi).unwrap_or(0.0);
                        info!("{} Hyperliquid: {:.4}",
                            format!("[{} VERIFY]", self.config.symbol).cyan().bold(), szi);
                        hl_position = Some(szi);
                        break;
                    } else if retry == 2 {
                        info!("{} Hyperliquid: flat",
                            format!("[{} VERIFY]", self.config.symbol).cyan().bold());
                        hl_position = Some(0.0);
                    }
                }
                Err(e) if retry == 2 => {
                    warn!("{} {} Hyperliquid position fetch failed: {}",
                        format!("[{} VERIFY]", self.config.symbol).yellow().bold(), "⚠".yellow().bold(), e);
                }
                _ => {}
            }
        }

        // Report net position
        if let (Some(pac), Some(hl)) = (pacifica_position, hl_position) {
            let net = pac + hl;
            info!("");
            info!("{} Net Position: {:.4}", format!("[{} VERIFY]", self.config.symbol).cyan().bold(), net);

            if net.abs() < 0.01 {
                info!("{} {} Position is NEUTRAL", format!("[{} VERIFY]", self.config.symbol).cyan().bold(), "✓".green().bold());
            } else {
                warn!("{}", "WARNING: Net position NOT neutral!".red().bold());
                warn!("Position delta: {:.4} {} - please check manually!", net.abs(), self.config.symbol);
            }
        }
    }

    async fn handle_hedge_error(&self, error: &str) {
        error!("{} {} Hedge failed: {}", self.log_prefix(), "✗".red().bold(), error.red());

        // Cancel all orders on error
        info!("{} {} Error recovery: Cancelling orders...", self.log_prefix(), "⚡".yellow().bold());
        if let Err(e) = self.pacifica_trading.cancel_all_orders(false, Some(&self.config.symbol), false).await {
            warn!("{} {} Error recovery cancellation failed: {}", self.log_prefix(), "⚠".yellow().bold(), e);
        }

        {
            let mut state = self.bot_state.write().await;
            state.set_error(format!("Hedge failed: {}", error));
        }

        self.shutdown_tx.send(()).await.ok();
    }

    async fn handle_fatal_error(&self, error: &str) {
        error!("{} {} {}", self.log_prefix(), "✗".red().bold(), error);

        {
            let mut state = self.bot_state.write().await;
            state.set_error(error.to_string());
        }

        self.shutdown_tx.send(()).await.ok();
    }

    /// Establish a Hyperliquid trading WebSocket connection.
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

    /// Place a market IOC order over Hyperliquid WebSocket.
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
        // Use pre-cached metadata if available (avoids RwLock on hot path)
        let payload = if let Some(ref meta) = self.cached_asset_meta {
            self.hyperliquid_trading
                .build_market_order_request_with_meta(
                    &self.config.symbol, is_buy, size,
                    self.config.hyperliquid_slippage, false,
                    Some(bid), Some(ask),
                    meta.asset_id, meta.sz_decimals,
                )
                .await?
        } else {
            self.hyperliquid_trading
                .build_market_order_request(
                    &self.config.symbol, is_buy, size,
                    self.config.hyperliquid_slippage, false,
                    Some(bid), Some(ask),
                )
                .await?
        };

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
        info!("{} Sending hedge order via WebSocket (id={})", self.log_prefix(), request_id);
        write.send(Message::Text(request_json)).await?;

        // Wait for matching response
        loop {
            match read.next().await {
                Some(Ok(Message::Text(text))) => {
                    let ws_resp: WsPostResponse = match serde_json::from_str(&text) {
                        Ok(r) => r,
                        Err(_) => continue,
                    };

                    if ws_resp.channel != "post" || ws_resp.data.id != request_id {
                        continue;
                    }

                    return match ws_resp.data.response.type_.as_str() {
                        "action" => {
                            let order_response = serde_json::from_value(ws_resp.data.response.payload)?;
                            Ok(order_response)
                        }
                        "error" => {
                            let msg = ws_resp.data.response.payload.as_str()
                                .unwrap_or("Unknown error").to_string();
                            anyhow::bail!("WebSocket order error: {}", msg);
                        }
                        other => anyhow::bail!("Unexpected response type: {}", other),
                    };
                }
                Some(Ok(Message::Ping(data))) => {
                    write.send(Message::Pong(data)).await?;
                }
                Some(Ok(Message::Close(frame))) => anyhow::bail!("WebSocket closed: {:?}", frame),
                Some(Err(e)) => anyhow::bail!("WebSocket error: {}", e),
                None => anyhow::bail!("WebSocket ended unexpectedly"),
                _ => {}
            }
        }
    }
}
