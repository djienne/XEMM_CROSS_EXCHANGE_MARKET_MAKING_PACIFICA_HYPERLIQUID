use crate::util::log::{tag, Color};
use crate::util::price::SharedQuote;
use colored::Colorize;
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, info, warn};

use crate::bot::BotState;
use crate::config::Config;
use crate::connector::hyperliquid::types::{
    OrderResponse, OrderResponseContent, OrderStatus, OrderStatusQuery, WsPostRequest,
    WsPostRequestInner, WsPostResponse,
};
use crate::connector::hyperliquid::HyperliquidTrading;
use crate::market_rules::fallback_rules;
use crate::services::maker::MakerExchange;
use crate::services::cancel_manager::{request_cancel, CancelDemand, CancelIntent, CancelReason};
use crate::services::fill_aggregator::{FillAggregator, HedgeSettlement};
use crate::services::hedge_store::{self, HedgeIntentStatus, HedgeLifecycleUpdate};
use crate::services::metrics;
use crate::services::post_trade_auditor::PostTradeAuditEvent;
use crate::services::{HedgeIntent, HedgeSource, HedgeVenueSide};
use crate::strategy::OrderSide;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWrite = futures_util::stream::SplitSink<WsStream, Message>;
type WsRead = futures_util::stream::SplitStream<WsStream>;

/// Hot hedge execution service.
///
/// Receives hedge intents, cancels maker exposure in the background, submits
/// Hyperliquid IOC hedges with deterministic CLOIDs, confirms filled size, and
/// retries residuals. Slow trade-history, PnL, CSV, and final verification work
/// is handed to `PostTradeAuditorService`.
pub struct HedgeService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_rx: mpsc::Receiver<HedgeIntent>,
    pub hyperliquid_prices: Arc<SharedQuote>,
    pub config: Config,
    pub hyperliquid_trading: Arc<HyperliquidTrading>,
    /// Used only to revalidate Reconciler-sourced intents against fresh venue
    /// positions before execution; maker-fill intents never touch it.
    pub maker: Arc<dyn MakerExchange>,
    pub fill_aggregator: Arc<FillAggregator>,
    pub audit_tx: mpsc::Sender<PostTradeAuditEvent>,
    pub cancel_tx: mpsc::Sender<CancelIntent>,
    pub cancel_demand: Arc<CancelDemand>,
    pub shutdown_tx: mpsc::Sender<()>,
    /// Fires when the main loop wants the hedge service to stop. The service
    /// drains any events already in `hedge_rx` and then exits cleanly. This
    /// prevents a fill that races with SIGINT from being abandoned.
    pub shutdown_signal: Arc<tokio::sync::Notify>,
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
                    info!(
                        "{} {} Hyperliquid trading WebSocket connected (hedge execution via WS)",
                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                        "OK".green().bold(),
                    );
                    ws_write = Some(write);
                    ws_read = Some(read);
                }
                Err(e) => {
                    warn!(
                        "{} {} Failed to pre-connect Hyperliquid trading WebSocket (using REST until reconnect succeeds): {}",
                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                        "WARN".yellow().bold(),
                        e
                    );
                }
            }
        }

        // Keep-alive interval for WebSocket pings (5s to keep connection warm)
        let mut keepalive_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        keepalive_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let shutdown_signal = self.shutdown_signal.clone();
        let mut draining = false;

        loop {
            tokio::select! {
                // Shutdown requested by main loop - stop accepting new events and
                // process whatever is already queued. One tick later we'll see
                // hedge_rx return None / empty and exit.
                _ = shutdown_signal.notified(), if !draining => {
                    info!(
                        "{} {} Shutdown signal received, draining {} pending hedge event(s)",
                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                        "WAIT".yellow().bold(),
                        self.hedge_rx.len()
                    );
                    draining = true;
                    // If queue is empty right now, we can exit immediately.
                    if self.hedge_rx.is_empty() {
                        info!(
                            "{} {} Hedge queue already empty, exiting",
                            tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                            "OK".green().bold()
                        );
                        let _ = self.shutdown_tx.send(()).await;
                        return;
                    }
                }

                // Send periodic pings to keep WebSocket connection warm
                _ = keepalive_interval.tick() => {
                    if use_ws_for_hedge && (ws_write.is_none() || ws_read.is_none()) {
                        match self.connect_hyperliquid_ws().await {
                            Ok((write, read)) => {
                                info!(
                                    "{} {} Hyperliquid trading WebSocket restored in background",
                                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                    "OK".green().bold()
                                );
                                ws_write = Some(write);
                                ws_read = Some(read);
                            }
                            Err(e) => {
                                debug!(
                                    "{} Hyperliquid background reconnect failed: {}",
                                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                    e
                                );
                            }
                        }
                    } else if let Some(write) = ws_write.as_mut() {
                        if let Err(e) = write.send(Message::Ping(vec![])).await {
                            warn!("{} {} Failed to send keepalive ping: {}",
                                tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                "WARN".yellow().bold(),
                                e
                            );
                            ws_write = None;
                            ws_read = None;
                        }
                    }

                    // Drain any buffered inbound frames while idle so server pings
                    // are answered and a dead/closed socket is noticed now, rather
                    // than costing the next hedge its full response timeout (N6).
                    if ws_read.is_some() {
                        loop {
                            let next = tokio::time::timeout(
                                Duration::from_millis(5),
                                async { ws_read.as_mut().unwrap().next().await },
                            )
                            .await;
                            match next {
                                Ok(Some(Ok(Message::Ping(data)))) => {
                                    if let Some(w) = ws_write.as_mut() {
                                        let _ = w.send(Message::Pong(data)).await;
                                    }
                                }
                                Ok(Some(Ok(Message::Close(_)))) => {
                                    debug!(
                                        "{} Idle trading WS received Close; dropping for reconnect",
                                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta)
                                    );
                                    ws_write = None;
                                    ws_read = None;
                                    break;
                                }
                                Ok(Some(Ok(_))) => {
                                    // Pong / other: ignore and keep draining.
                                }
                                Ok(Some(Err(_))) | Ok(None) => {
                                    debug!(
                                        "{} Idle trading WS read ended; dropping for reconnect",
                                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta)
                                    );
                                    ws_write = None;
                                    ws_read = None;
                                    break;
                                }
                                Err(_) => break, // timeout: no more buffered frames
                            }
                        }
                    }
                }

                // Main hedge event processing
                Some(event) = self.hedge_rx.recv() => {
            // Reconciler-sourced intents carry no aggregator reservation and can
            // be stale by the time the serial executor reaches them (the exposure
            // they were minted for may already be covered). Revalidate against
            // fresh venue positions and clamp or skip before submitting.
            // Maker-fill / position-monitor intents are reservation-protected and
            // skip this REST round-trip entirely.
            let event = match self.revalidate_reconciler_intent(event).await {
                Some(event) => event,
                None => continue,
            };
            let hedge_side = event.hedge_side;
            let side = event.audit_maker_side();
            let size = event.size;
            let avg_price = event.avg_price;
            let fill_timestamp = event.detected_at;
            let reception_latency = fill_timestamp.elapsed();
            metrics::risk_metrics()
                .hedge_enqueue_to_submit_us
                .store(
                    reception_latency
                        .as_micros()
                        .min(u64::MAX as u128) as u64,
                    std::sync::atomic::Ordering::Release,
                );
            metrics::store_f64(&metrics::risk_metrics().hedge_target_qty_bits, size);
            if !self.config.low_latency_mode {
                info!("{} FAST HEDGE RECEIVED: {} {} @ {} | Reception latency: {:.1}ms",
                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                    hedge_side.as_str().bright_yellow(),
                    size,
                    format!("${:.4}", avg_price).cyan(),
                    reception_latency.as_secs_f64() * 1000.0
                );
            }

            // *** CRITICAL: CANCEL ALL ORDERS BEFORE HEDGE ***
            // Extra safety: cancel again in case fill detection missed anything
            // or there was a race condition. `request_cancel` is two atomic ops
            // plus a non-blocking try_send (the cancel manager does the actual
            // I/O), so it runs inline - no spawn needed.
            if !self.config.low_latency_mode {
                info!("{} {} Pre-hedge safety: Cancelling all Pacifica orders (background)...",
                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                    "FAST".yellow().bold()
                );
            }
            request_cancel(
                &self.cancel_tx,
                &self.cancel_demand,
                self.config.symbol.clone(),
                CancelReason::Safety,
            );

            // Update status
            {
                let mut state = self.bot_state.write();
                state.mark_hedging();
            }

            // Execute opposite direction on Hyperliquid
            let is_buy = hedge_side == HedgeVenueSide::Buy;

            let (mut hl_bid, mut hl_ask) = self
                .hyperliquid_prices
                .usable_snapshot(Duration::from_millis(self.config.hedge_quote_max_age_ms))
                .map(|q| (q.bid, q.ask))
                .unwrap_or((0.0, 0.0));

            if hl_bid <= 0.0 || hl_ask <= 0.0 {
                warn!("{} {} Hyperliquid price cache empty - fetching fresh snapshot before hedging",
                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                    "WARN".yellow().bold()
                );

                match self.hyperliquid_trading.get_l2_snapshot(&self.config.symbol).await {
                    Ok(Some((bid, ask))) if crate::util::price::prices_valid(bid, ask) => {
                        hl_bid = bid;
                        hl_ask = ask;
                        self.hyperliquid_prices.store(bid, ask);
                        if !self.config.low_latency_mode {
                            info!("{} {} Refreshed Hyperliquid prices: bid ${:.4}, ask ${:.4}",
                                tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                "OK".green().bold(),
                                hl_bid,
                                hl_ask
                            );
                        }
                    }
                    Ok(_) => {
                        warn!(
                            "{} {} Snapshot missing bid/ask data",
                            tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                            "WARN".yellow().bold()
                        );
                    }
                    Err(err) => {
                        warn!(
                            "{} {} Failed to fetch Hyperliquid snapshot: {}",
                            tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                            "WARN".yellow().bold(),
                            err
                        );
                    }
                }

                if hl_bid <= 0.0 || hl_ask <= 0.0 {
                    error!("{} {} Unable to obtain Hyperliquid prices - aborting hedge for safety",
                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                        "FAIL".red().bold()
                    );

                    let mut failed_update =
                        HedgeLifecycleUpdate::new(&event, HedgeIntentStatus::Error);
                    failed_update.target_qty = Some(size);
                    failed_update.residual_qty = Some(size);
                    failed_update.reason =
                        Some("Hyperliquid prices unavailable for hedge".to_string());
                    let _ = hedge_store::append_lifecycle_update(failed_update).await;

                    // Nothing was submitted: this is a definitive non-fill, not an
                    // uncertain outcome. Settle as rejected so the residual
                    // re-exposes immediately (idle flush / reconciler retry)
                    // instead of entering the unknown quarantine.
                    self.fill_aggregator.settle_hedge(HedgeSettlement::rejected(
                        event.source_order_id,
                        size,
                        0.0,
                    ));

                    {
                        let mut state = self.bot_state.write();
                        state.mark_reconciling();
                    }

                    continue;
                }
            }

            if !self.config.low_latency_mode {
                info!(
                    "{} Executing {} {} on Hyperliquid",
                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                    if is_buy { "BUY".green().bold() } else { "SELL".red().bold() },
                    size
                );
            }

            // *** HEDGE WITH EXPONENTIAL RETRY ***
            // Transient failures on Hyperliquid (429, 5xx, WS disconnect) must not leave
            // a Pacifica fill un-hedged. Retry up to 3 times alternating WS/REST before
            // falling through to the error-shutdown path.
            let max_attempts = self.config.max_consecutive_hedge_failures.max(1) as usize;
            let mut hedge_result = Err(anyhow::anyhow!("hedge not attempted"));
            let mut request_group = 0u64;
            for attempt in 0..max_attempts {
                if attempt > 0 {
                    let delay_ms = (self.config.hedge_retry_base_delay_ms
                        * 2u64.saturating_pow((attempt - 1) as u32))
                    .min(self.config.hedge_retry_max_delay_ms);
                    warn!(
                        "{} {} Hedge attempt {} failed, retrying in {}ms: {}",
                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                        "retry".yellow().bold(),
                        attempt,
                        delay_ms,
                        hedge_result.as_ref().err().map(|e| e.to_string()).unwrap_or_default()
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                    // Refresh prices in case slippage ceiling would now reject the order.
                    if let Some(cached) = self.hyperliquid_prices.usable_snapshot(Duration::from_millis(self.config.hedge_quote_max_age_ms)) {
                        hl_bid = cached.bid;
                        hl_ask = cached.ask;
                    }
                }

                let cloid =
                    Self::idempotency_cloid(event.source_order_id, event.hedge_seq, request_group);
                let mut submitted = HedgeLifecycleUpdate::new(&event, HedgeIntentStatus::Submitted);
                submitted.cloid = Some(&cloid);
                submitted.target_qty = Some(size);
                submitted.residual_qty = Some(size);
                submitted.attempt = Some(attempt as u64);
                if let Err(e) = hedge_store::append_lifecycle_update(submitted).await {
                    warn!(
                        "{} Failed to persist hedge submission lifecycle event: {}",
                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                        e
                    );
                }

                let submit_start = std::time::Instant::now();
                hedge_result = if use_ws_for_hedge {
                    // Ensure we have an active trading WebSocket
                    if ws_write.is_none() || ws_read.is_none() {
                        match self.connect_hyperliquid_ws().await {
                            Ok((write, read)) => {
                                info!(
                                    "{} {} Reconnected Hyperliquid trading WebSocket for hedge execution",
                                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                    "OK".green().bold()
                                );
                                ws_write = Some(write);
                                ws_read = Some(read);
                            }
                            Err(e) => {
                                warn!(
                                    "{} {} Failed to connect Hyperliquid trading WebSocket, falling back to REST for this hedge: {}",
                                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                    "WARN".yellow().bold(),
                                    e
                                );
                            }
                        }
                    }

                    if let (Some(write), Some(read)) = (ws_write.as_mut(), ws_read.as_mut()) {
                        match tokio::time::timeout(
                            Duration::from_millis(self.config.hedge_ws_response_timeout_ms),
                            self.place_market_order_ws(
                                write,
                                read,
                                &mut ws_request_id,
                                is_buy,
                                size,
                                hl_bid,
                                hl_ask,
                                Some(cloid.clone()),
                                self.slippage_for_attempt(attempt),
                            ),
                        )
                        .await
                        {
                            Ok(Ok(response)) => Ok(response),
                            Err(_) => {
                                warn!(
                                    "{} {} Hyperliquid WebSocket hedge timed out after submit; retrying only with the same CLOID",
                                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                    "timeout".yellow().bold()
                                );
                                ws_write = None;
                                ws_read = None;
                                Err(anyhow::anyhow!(
                                    "Hyperliquid WS submit response timed out; placement unknown"
                                ))
                            }
                            Ok(Err(e)) => {
                                warn!(
                                    "{} {} WebSocket hedge execution failed after submit; retrying only with the same CLOID: {}",
                                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                    "WARN".yellow().bold(),
                                    e
                                );
                                // Drop WS so next hedge attempts a clean reconnect
                                ws_write = None;
                                ws_read = None;
                                Err(anyhow::anyhow!("Hyperliquid WS submit uncertain: {}", e))
                            }
                        }
                    } else {
                        // No WS connection available - use REST for this hedge
                        self.hyperliquid_trading
                            .place_market_order_with_cloid(
                                &self.config.symbol,
                                is_buy,
                                size,
                                self.slippage_for_attempt(attempt),
                                false, // reduce_only
                                Some(hl_bid),
                                Some(hl_ask),
                                Some(cloid.clone()),
                            )
                            .await
                    }
                } else {
                    // WS disabled via config - use REST only
                    self.hyperliquid_trading
                        .place_market_order_with_cloid(
                            &self.config.symbol,
                            is_buy,
                            size,
                            self.slippage_for_attempt(attempt),
                            false, // reduce_only
                            Some(hl_bid),
                            Some(hl_ask),
                            Some(cloid.clone()),
                        )
                        .await
                };
                metrics::risk_metrics().hedge_submit_to_ack_us.store(
                    submit_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
                    std::sync::atomic::Ordering::Release,
                );

                if hedge_result.is_err() {
                    match self.confirm_hedge_by_cloid(&cloid).await {
                        Ok(Some(response)) => {
                            warn!(
                                "{} {} Confirmed hedge fill via orderStatus(cloid) after submit uncertainty",
                                tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                "OK".green().bold()
                            );
                            hedge_result = Ok(response);
                        }
                        Ok(None) => {}
                        Err(e) => {
                            warn!(
                                "{} {} Could not confirm uncertain hedge via orderStatus: {}",
                                tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                "WARN".yellow().bold(),
                                e
                            );
                        }
                    }
                }

                let mut attempt_update = HedgeLifecycleUpdate::new(&event, HedgeIntentStatus::Unknown);
                attempt_update.cloid = Some(&cloid);
                attempt_update.target_qty = Some(size);
                attempt_update.attempt = Some(attempt as u64);
                match &hedge_result {
                    Ok(resp) => match &resp.response {
                        OrderResponseContent::Success(data) => match data.data.statuses.first() {
                            Some(OrderStatus::Filled { .. }) => {
                                if let Some((_, filled_qty)) = Self::filled_qty_from_response(resp) {
                                    let residual = (size - filled_qty).max(0.0);
                                    attempt_update.status =
                                        if residual <= self.config.neutral_dust_base.max(1e-9) {
                                            HedgeIntentStatus::Filled
                                        } else {
                                            HedgeIntentStatus::PartiallyFilled
                                        };
                                    attempt_update.filled_qty = Some(filled_qty);
                                    attempt_update.residual_qty = Some(residual);
                                } else {
                                    attempt_update.reason =
                                        Some("response fill values were not parseable".to_string());
                                }
                            }
                            Some(OrderStatus::Error { error }) => {
                                attempt_update.status = HedgeIntentStatus::Error;
                                attempt_update.reason =
                                    Some(format!("HL order status error: {}", error));
                            }
                            Some(OrderStatus::Resting { resting }) => {
                                attempt_update.status = HedgeIntentStatus::Error;
                                attempt_update.reason = Some(format!(
                                    "HL order unexpectedly resting (oid {})",
                                    resting.oid
                                ));
                            }
                            None => {
                                attempt_update.status = HedgeIntentStatus::Error;
                                attempt_update.reason =
                                    Some("HL hedge response missing statuses".to_string());
                            }
                        },
                        OrderResponseContent::Error(e) => {
                            attempt_update.status = HedgeIntentStatus::Error;
                            attempt_update.reason = Some(format!("HL response error: {}", e));
                        }
                    },
                    Err(e) => {
                        attempt_update.status = HedgeIntentStatus::Error;
                        attempt_update.reason = Some(e.to_string());
                    }
                }
                if let Err(e) = hedge_store::append_lifecycle_update(attempt_update).await {
                    warn!(
                        "{} Failed to persist hedge attempt lifecycle event: {}",
                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                        e
                    );
                }

                // Classify this attempt, then decide whether to escalate to a NEW
                // cloid. Critical invariant: a fresh cloid (request_group += 1) is
                // only minted AFTER `orderStatus` proves the current cloid produced
                // no fill. Otherwise a WS-timeout-but-filled hedge (whose retry hits
                // a duplicate-cloid rejection) would be re-submitted and double-fill.
                //
                // Phase 1: classify under the immutable borrow (no await here).
                enum Outcome {
                    Filled,
                    DupCloid,
                    Reject,
                    Retry,
                }
                let outcome = match &hedge_result {
                    Ok(resp) => match &resp.response {
                        OrderResponseContent::Success(data) => match data.data.statuses.first() {
                            Some(OrderStatus::Filled { .. }) => Outcome::Filled,
                            Some(OrderStatus::Error { error })
                                if Self::is_duplicate_cloid_error(error) =>
                            {
                                Outcome::DupCloid
                            }
                            Some(OrderStatus::Error { .. }) => Outcome::Reject,
                            Some(OrderStatus::Resting { .. }) | None => Outcome::Reject,
                        },
                        OrderResponseContent::Error(e) if Self::is_duplicate_cloid_error(e) => {
                            Outcome::DupCloid
                        }
                        OrderResponseContent::Error(_) => Outcome::Reject,
                    },
                    Err(_) => Outcome::Retry,
                };

                // Phase 2: act (borrow released, awaits allowed).
                match outcome {
                    Outcome::Filled => break,
                    Outcome::DupCloid | Outcome::Reject => {
                        // The cloid is now "used" at HL. orderStatus is the source of
                        // truth: if it actually filled, adopt that fill instead of
                        // re-submitting; only escalate once it is proven unfilled.
                        match self.confirm_hedge_by_cloid(&cloid).await {
                            Ok(Some(resp)) => {
                                warn!(
                                    "{} {} Adopted confirmed fill for cloid after reject/dup; not re-submitting",
                                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                    "OK".green().bold()
                                );
                                hedge_result = Ok(resp);
                                break;
                            }
                            _ => {
                                if self.cloid_definitively_unfilled(&cloid).await {
                                    request_group = request_group.saturating_add(1);
                                }
                                hedge_result =
                                    Err(anyhow::anyhow!("HL rejected/duplicate cloid, not filled"));
                            }
                        }
                    }
                    Outcome::Retry => {
                        // Transient/uncertain (network, timeout): keep the SAME cloid
                        // so the next attempt is idempotent at Hyperliquid.
                    }
                }
            }

            match hedge_result {
                Ok(response) => {
                    // Extract success data from response
                    let response_data = match &response.response {
                        crate::connector::hyperliquid::OrderResponseContent::Success(data) => data,
                        crate::connector::hyperliquid::OrderResponseContent::Error(error) => {
                            // This should not happen as trading.rs already handles errors,
                            // but handle it defensively
                            error!("{} {} Hedge response contains error: {}",
                                tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                "FAIL".red().bold(),
                                error
                            );

                            {
                                let mut state = self.bot_state.write();
                                state.mark_reconciling();
                            }
                            self.fill_aggregator.settle_hedge(HedgeSettlement::rejected(
                                event.source_order_id,
                                size,
                                0.0,
                            ));

                            continue;
                        }
                    };

                    // Calculate ACTUAL end-to-end latency from fill detection to hedge completion
                    let end_to_end_latency = fill_timestamp.elapsed();

                    // Validate and extract order status
                    let hedge_fill = if let Some(status) = response_data.data.statuses.first() {
                        match status {
                            crate::connector::hyperliquid::OrderStatus::Filled { filled } => {
                                if !self.config.low_latency_mode {
                                    info!("{} {} Hedge executed successfully: Filled {} @ ${} | Total latency: {:.1}ms",
                                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                        "OK".green().bold(),
                                        filled.total_sz,
                                        filled.avg_px,
                                        end_to_end_latency.as_secs_f64() * 1000.0
                                    );
                                }
                                match (filled.avg_px.parse::<f64>().ok(), filled.total_sz.parse::<f64>().ok()) {
                                    (Some(px), Some(sz)) => Some((px, sz)),
                                    _ => None,
                                }
                            }
                            crate::connector::hyperliquid::OrderStatus::Error { error } => {
                                error!("{} {} Hedge order FAILED: {}",
                                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                    "FAIL".red().bold(),
                                    error
                                );

                                // Set error state
                                {
                                    let mut state = self.bot_state.write();
                                    state.mark_reconciling();
                                }
                                self.fill_aggregator.settle_hedge(HedgeSettlement::rejected(
                                    event.source_order_id,
                                    size,
                                    0.0,
                                ));

                                continue;
                            }
                            crate::connector::hyperliquid::OrderStatus::Resting { resting } => {
                                warn!("{} {} Hedge order is RESTING (oid: {}) - unexpected for IOC market order",
                                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                    "WARN".yellow().bold(),
                                    resting.oid
                                );

                                // Treat as error - IOC orders should never rest
                                {
                                    let mut state = self.bot_state.write();
                                    state.mark_reconciling();
                                }
                                self.fill_aggregator.settle_hedge(HedgeSettlement::unknown(
                                    event.source_order_id,
                                    size,
                                    0.0,
                                ));

                                continue;
                            }
                        }
                    } else {
                        warn!("{} {} Hedge response has no statuses - unexpected API response",
                            tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                            "WARN".yellow().bold()
                        );
                        None
                    };

                    // Validate we got a fill price before continuing
                    let (hedge_fill_price, mut confirmed_hedge_size) = match hedge_fill {
                        Some(fill) => fill,
                        None => {
                            error!("{} {} No hedge fill price available - hedge may have failed",
                                tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                "FAIL".red().bold()
                            );

                            {
                                let mut state = self.bot_state.write();
                                state.mark_reconciling();
                            }
                            self.fill_aggregator.settle_hedge(HedgeSettlement::unknown(
                                event.source_order_id,
                                size,
                                0.0,
                            ));
                            metrics::risk_metrics()
                                .hedge_unknown_count
                                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);

                            continue;
                        }
                    };

                    let hedge_dust = self.config.neutral_dust_base.max(1e-9);
                    // A residual smaller than one Hyperliquid size step can never be
                    // hedged (it floors to 0), so treat it as un-hedgeable dust: do
                    // not retry it forever, and let the position reconciler net the
                    // bounded remainder instead of wedging in Reconciling.
                    let hl_step = match self
                        .hyperliquid_trading
                        .get_asset_info(&self.config.symbol)
                        .await
                    {
                        Ok(info) => 10_f64.powi(-info.sz_decimals.max(0)),
                        Err(_) => 0.0,
                    };
                    let unhedgeable = hedge_dust.max(hl_step);
                    if confirmed_hedge_size + unhedgeable < size {
                        let mut residual = size - confirmed_hedge_size;
                        warn!(
                            "{} {} Hyperliquid filled only {} of {}; retrying residual {}",
                            tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                            "partial".yellow().bold(),
                            confirmed_hedge_size,
                            size,
                            residual
                        );

                        for residual_attempt in 0..self.config.max_consecutive_hedge_failures {
                            if residual <= unhedgeable {
                                break;
                            }

                            // Back off between residual submits (mirrors the primary
                            // loop) and refresh the HL quote so the IOC limit is not
                            // anchored to a stale mid during a fast move.
                            if residual_attempt > 0 {
                                let delay_ms = (self.config.hedge_retry_base_delay_ms
                                    * 2u64.saturating_pow((residual_attempt - 1) as u32))
                                .min(self.config.hedge_retry_max_delay_ms);
                                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            }
                            if let Some(cached) = self.hyperliquid_prices.usable_snapshot(
                                Duration::from_millis(self.config.hedge_quote_max_age_ms),
                            ) {
                                if cached.bid > 0.0 && cached.ask > 0.0 {
                                    hl_bid = cached.bid;
                                    hl_ask = cached.ask;
                                }
                            } else if let Ok(Some((bid, ask))) = self
                                .hyperliquid_trading
                                .get_l2_snapshot(&self.config.symbol)
                                .await
                            {
                                if bid > 0.0 && ask > 0.0 {
                                    hl_bid = bid;
                                    hl_ask = ask;
                                    self.hyperliquid_prices.store(bid, ask);
                                }
                            }
                            let attempt_no = max_attempts as u64 + residual_attempt as u64;
                            let cloid = Self::residual_cloid(
                                event.source_order_id,
                                event.hedge_seq,
                                residual_attempt as u64 + 1,
                                attempt_no,
                            );
                            let mut residual_update =
                                HedgeLifecycleUpdate::new(&event, HedgeIntentStatus::Submitted);
                            residual_update.cloid = Some(&cloid);
                            residual_update.target_qty = Some(size);
                            residual_update.residual_qty = Some(residual);
                            residual_update.attempt = Some(attempt_no);
                            if let Err(e) =
                                hedge_store::append_lifecycle_update(residual_update).await
                            {
                                warn!(
                                    "{} Failed to persist residual hedge submission event: {}",
                                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                    e
                                );
                            }
                            // Pre-submit idempotency guard: if this residual cloid
                            // already landed at HL (uncertain ack on a prior pass),
                            // adopt that fill instead of submitting it again.
                            if let Ok(Some(resp)) = self.confirm_hedge_by_cloid(&cloid).await {
                                if let Some((_, filled_qty)) = Self::filled_qty_from_response(&resp)
                                {
                                    confirmed_hedge_size += filled_qty;
                                    residual = (size - confirmed_hedge_size).max(0.0);
                                    continue;
                                }
                            }

                            let mut residual_response = self
                                .hyperliquid_trading
                                .place_market_order_with_cloid(
                                    &self.config.symbol,
                                    is_buy,
                                    residual,
                                    self.slippage_for_attempt(attempt_no as usize),
                                    false,
                                    Some(hl_bid),
                                    Some(hl_ask),
                                    Some(cloid.clone()),
                                )
                                .await;

                            if residual_response.is_err() {
                                match self.confirm_hedge_by_cloid(&cloid).await {
                                    Ok(Some(resp)) => {
                                        warn!(
                                            "{} {} Confirmed residual hedge fill via orderStatus(cloid) after submit uncertainty",
                                            tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                            "OK".green().bold()
                                        );
                                        residual_response = Ok(resp);
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        warn!(
                                            "{} {} Could not confirm uncertain residual hedge via orderStatus: {}",
                                            tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                            "WARN".yellow().bold(),
                                            e
                                        );
                                    }
                                }
                            }

                            match residual_response {
                                Ok(resp) => {
                                    if let Some((_, filled_qty)) = Self::filled_qty_from_response(&resp) {
                                        confirmed_hedge_size += filled_qty;
                                        residual = (size - confirmed_hedge_size).max(0.0);
                                        let mut filled_update = HedgeLifecycleUpdate::new(
                                            &event,
                                            if residual <= unhedgeable {
                                                HedgeIntentStatus::Filled
                                            } else {
                                                HedgeIntentStatus::PartiallyFilled
                                            },
                                        );
                                        filled_update.cloid = Some(&cloid);
                                        filled_update.target_qty = Some(size);
                                        filled_update.filled_qty = Some(filled_qty);
                                        filled_update.residual_qty = Some(residual);
                                        filled_update.attempt = Some(attempt_no);
                                        let _ =
                                            hedge_store::append_lifecycle_update(filled_update).await;
                                        continue;
                                    }
                                    warn!("{} Residual hedge response did not confirm a fill", tag(&self.config.symbol, "HEDGE", Color::BrightMagenta));
                                    let mut unknown_update = HedgeLifecycleUpdate::new(
                                        &event,
                                        HedgeIntentStatus::Unknown,
                                    );
                                    unknown_update.cloid = Some(&cloid);
                                    unknown_update.target_qty = Some(size);
                                    unknown_update.residual_qty = Some(residual);
                                    unknown_update.attempt = Some(attempt_no);
                                    unknown_update.reason =
                                        Some("residual response did not confirm fill".to_string());
                                    let _ =
                                        hedge_store::append_lifecycle_update(unknown_update).await;
                                }
                                Err(e) => {
                                    warn!("{} Residual hedge attempt failed: {}", tag(&self.config.symbol, "HEDGE", Color::BrightMagenta), e);
                                    let mut failed_update =
                                        HedgeLifecycleUpdate::new(&event, HedgeIntentStatus::Error);
                                    failed_update.cloid = Some(&cloid);
                                    failed_update.target_qty = Some(size);
                                    failed_update.residual_qty = Some(residual);
                                    failed_update.attempt = Some(attempt_no);
                                    failed_update.reason = Some(e.to_string());
                                    let _ =
                                        hedge_store::append_lifecycle_update(failed_update).await;
                                }
                            }
                        }

                        if confirmed_hedge_size + unhedgeable < size {
                            let mut failed_update =
                                HedgeLifecycleUpdate::new(&event, HedgeIntentStatus::Error);
                            failed_update.target_qty = Some(size);
                            failed_update.filled_qty = Some(confirmed_hedge_size);
                            failed_update.residual_qty = Some(size - confirmed_hedge_size);
                            failed_update.reason = Some("hedge residual remained unfilled".to_string());
                            let _ = hedge_store::append_lifecycle_update(failed_update).await;
                            {
                                let mut state = self.bot_state.write();
                                state.mark_reconciling();
                            }
                            self.fill_aggregator.settle_hedge(HedgeSettlement::unknown(
                                event.source_order_id,
                                size,
                                confirmed_hedge_size,
                            ));
                            metrics::risk_metrics()
                                .hedge_unknown_count
                                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                            warn!(
                                "{} {} Hedge residual still unfilled (target {}, confirmed {}); leaving bot in Reconciling for slow retry",
                                tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                                "WARN".yellow().bold(),
                                size,
                                confirmed_hedge_size
                            );
                            continue;
                        }
                    }

                    self.fill_aggregator.settle_hedge(HedgeSettlement::filled(
                        event.source_order_id,
                        size,
                        confirmed_hedge_size,
                    ));
                    metrics::store_f64(
                        &metrics::risk_metrics().hedge_confirmed_qty_bits,
                        confirmed_hedge_size,
                    );
                    metrics::store_f64(
                        &metrics::risk_metrics().hedge_residual_qty_bits,
                        (size - confirmed_hedge_size).max(0.0),
                    );

                    let mut complete_update =
                        HedgeLifecycleUpdate::new(&event, HedgeIntentStatus::Complete);
                    complete_update.target_qty = Some(size);
                    complete_update.filled_qty = Some(confirmed_hedge_size);
                    complete_update.residual_qty = Some((size - confirmed_hedge_size).max(0.0));
                    if let Err(e) = hedge_store::append_lifecycle_update(complete_update).await {
                        warn!(
                            "{} Failed to persist hedge completion lifecycle event: {}",
                            tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                            e
                        );
                    }

                    if !event.terminal {
                        let mut state = self.bot_state.write();
                        if state.active_order.is_some() {
                            state.status = crate::bot::BotStatus::OrderPlaced;
                            state.store_status();
                        } else {
                            state.mark_reconciling();
                        }
                        continue;
                    }

                    let (expected_profit_bps, client_order_id) = {
                        let state = self.bot_state.read();
                        state.active_order.as_ref().map_or((None, None), |order| {
                            (Some(order.initial_profit_bps), Some(order.client_order_id.clone()))
                        })
                    };

                    let audit_event = PostTradeAuditEvent {
                        source_order_id: event.source_order_id,
                        hedge_seq: event.hedge_seq,
                        side,
                        size,
                        maker_avg_price: avg_price,
                        hedge_fill_price,
                        confirmed_hedge_size,
                        latency_ms: end_to_end_latency.as_secs_f64() * 1000.0,
                        expected_profit_bps,
                        client_order_id,
                    };

                    if let Err(e) = self.audit_tx.try_send(audit_event) {
                        let mut failed_update =
                            HedgeLifecycleUpdate::new(&event, HedgeIntentStatus::Error);
                        failed_update.reason = Some(format!("post-trade audit queue failed: {}", e));
                        let _ = hedge_store::append_lifecycle_update(failed_update).await;
                        {
                            let mut state = self.bot_state.write();
                            state.set_error(format!("post-trade audit queue failed: {}", e));
                        }
                        self.shutdown_tx.send(()).await.ok();
                        return;
                    }

                    if !self.config.low_latency_mode {
                        info!(
                            "{} {} Hedge confirmed; terminal audit handed to cold auditor",
                            tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                            "OK".green().bold()
                        );
                    }
                    continue;
                }
                Err(e) => {
                    error!("{} {} Hedge failed: {}",
                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                        "FAIL".red().bold(),
                        e.to_string().red()
                    );

                    // *** CRITICAL: CANCEL ALL ORDERS ON ERROR ***
                    // Even if hedge fails, cancel all orders to prevent stray positions
                    info!("{} {} Error recovery: Cancelling all Pacifica orders...",
                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                        "FAST".yellow().bold()
                    );

                    request_cancel(
                        &self.cancel_tx,
                        &self.cancel_demand,
                        self.config.symbol.clone(),
                        CancelReason::Safety,
                    );

                    {
                        let mut state = self.bot_state.write();
                        state.mark_reconciling();
                    }
                    self.fill_aggregator.settle_hedge(HedgeSettlement::unknown(
                        event.source_order_id,
                        size,
                        0.0,
                    ));
                    metrics::risk_metrics()
                        .hedge_unknown_count
                        .fetch_add(1, std::sync::atomic::Ordering::AcqRel);

                    let mut failed_update =
                        HedgeLifecycleUpdate::new(&event, HedgeIntentStatus::Error);
                    failed_update.target_qty = Some(size);
                    failed_update.residual_qty = Some(size);
                    failed_update.reason = Some(format!("hedge attempts exhausted: {}", e));
                    let _ = hedge_store::append_lifecycle_update(failed_update).await;

                    warn!(
                        "{} {} Hedge attempts exhausted; maker placement is paused while the reconciler retries exposure",
                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                        "WARN".yellow().bold()
                    );
                }
            }

            // If we were asked to shut down and the queue is now empty, exit so
            // main can finish cleanup. We intentionally finish the current event
            // first - dropping a hedge mid-flight is worse than taking ~1s longer.
            if draining && self.hedge_rx.is_empty() {
                info!(
                    "{} {} Drain complete, exiting hedge service",
                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                    "OK".green().bold()
                );
                let _ = self.shutdown_tx.send(()).await;
                return;
            }
                } // Close Some((side, size, avg_price, fill_timestamp)) arm
            } // Close tokio::select!
        } // Close loop
    }

    /// Authoritatively confirm whether the order placed under `cloid` filled,
    /// using Hyperliquid's `orderStatus` (by cloid). Returns a synthetic `Filled`
    /// response only when HL says it filled (fully, or canceled-with-partial above
    /// dust); otherwise `None`. This replaces the old time/side/size heuristic,
    /// which could never confirm and left the retry loop able to double-submit.
    async fn confirm_hedge_by_cloid(&self, cloid: &str) -> anyhow::Result<Option<OrderResponse>> {
        let dust = self.config.neutral_dust_base.max(1e-9);
        match self.hyperliquid_trading.query_order_status(cloid).await? {
            OrderStatusQuery::Filled {
                filled_sz, avg_px, ..
            } if filled_sz > 0.0 => Ok(Some(Self::filled_response(avg_px, filled_sz))),
            OrderStatusQuery::Canceled {
                filled_sz, avg_px, ..
            } if filled_sz > dust => Ok(Some(Self::filled_response(avg_px, filled_sz))),
            _ => Ok(None),
        }
    }

    /// Pure classification: true only when an `orderStatus` result PROVES the
    /// cloid produced no fill (rejected, or canceled with at most dust filled).
    /// `Unknown` is never proof: after a duplicate-cloid rejection the order
    /// demonstrably exists at HL, so "unknownOid" can only be index lag — and
    /// escalating on it is exactly the double-submit hazard. Liveness without
    /// escalation: resubmitting the same cloid yields DupCloid until orderStatus
    /// resolves to a real terminal status; if it never resolves, attempts
    /// exhaust into the unknown-settlement quarantine and the position
    /// reconciler recovers from venue truth.
    fn definitively_unfilled(query: &OrderStatusQuery, dust: f64) -> bool {
        match query {
            OrderStatusQuery::Rejected { .. } => true,
            OrderStatusQuery::Canceled { filled_sz, .. } => *filled_sz <= dust,
            OrderStatusQuery::Filled { .. }
            | OrderStatusQuery::Resting { .. }
            | OrderStatusQuery::Unknown => false,
        }
    }

    /// True only when Hyperliquid authoritatively confirms the cloid produced no
    /// fill, so it is safe to escalate to a fresh cloid. Returns `false` on any
    /// uncertainty (filled/resting/unknown/network error) so we never escalate
    /// into a double-submit. A single delayed re-query absorbs orderStatus
    /// propagation lag so a genuinely-rejected order still escalates promptly.
    async fn cloid_definitively_unfilled(&self, cloid: &str) -> bool {
        let dust = self.config.neutral_dust_base.max(1e-9);
        let first = match self.hyperliquid_trading.query_order_status(cloid).await {
            Ok(query) => query,
            Err(_) => return false,
        };
        if !matches!(first, OrderStatusQuery::Unknown) {
            return Self::definitively_unfilled(&first, dust);
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        match self.hyperliquid_trading.query_order_status(cloid).await {
            Ok(query) => Self::definitively_unfilled(&query, dust),
            Err(_) => false,
        }
    }

    /// Pure clamp/skip decision for a Reconciler-sourced intent against fresh
    /// venue truth. Returns the size still worth hedging, or `None` when the
    /// exposure is neutral, covered by same-side in-flight reservations, or the
    /// required direction no longer matches the intent.
    fn validate_reconciler_size(
        net: f64,
        pending_same_side: f64,
        hedge_side: HedgeVenueSide,
        intent_size: f64,
        dust: f64,
    ) -> Option<f64> {
        if net.abs() <= dust {
            return None;
        }
        let required_side = if net > 0.0 {
            HedgeVenueSide::Sell
        } else {
            HedgeVenueSide::Buy
        };
        if hedge_side != required_side {
            return None;
        }
        let uncovered = (net.abs() - pending_same_side.max(0.0)).max(0.0);
        let size = intent_size.min(uncovered);
        if size <= dust {
            return None;
        }
        Some(size)
    }

    /// Executor-side guard for Reconciler-sourced intents (which carry no
    /// aggregator reservation). Returns the (possibly clamped) intent to
    /// execute, or `None` when it should be skipped. On a position-fetch error
    /// the intent passes through at its original size: exposure recovery must
    /// not stall on a transient REST failure (a duplicate then requires BOTH a
    /// >5s-busy executor AND a simultaneous REST outage, and the reconciler
    /// nets any over-hedge from venue truth).
    async fn revalidate_reconciler_intent(&self, mut event: HedgeIntent) -> Option<HedgeIntent> {
        if event.source != HedgeSource::Reconciler {
            return Some(event);
        }

        let (pacifica, hyperliquid) = match crate::services::signed_positions(
            self.maker.as_ref(),
            &self.hyperliquid_trading,
            &self.config.symbol,
        )
        .await
        {
            Ok(positions) => positions,
            Err(e) => {
                warn!(
                    "{} {} Reconciler-intent revalidation could not fetch positions ({}); executing at original size {}",
                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                    "WARN".yellow().bold(),
                    e,
                    event.size
                );
                return Some(event);
            }
        };

        let net = pacifica + hyperliquid;
        let maker_side = if net > 0.0 {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let pending = self.fill_aggregator.pending_qty_for_side(maker_side);
        let dust = self
            .config
            .neutral_dust_base
            .max(fallback_rules(&self.config.symbol).min_size);

        match Self::validate_reconciler_size(net, pending, event.hedge_side, event.size, dust) {
            Some(size) => {
                if (size - event.size).abs() > f64::EPSILON {
                    warn!(
                        "{} {} Reconciler intent clamped from {} to {} (net {:.8}, pending {:.8})",
                        tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                        "WARN".yellow().bold(),
                        event.size,
                        size,
                        net,
                        pending
                    );
                }
                event.size = size;
                Some(event)
            }
            None => {
                let mut update = HedgeLifecycleUpdate::new(&event, HedgeIntentStatus::Skipped);
                update.target_qty = Some(event.size);
                update.reason = Some(format!(
                    "reconciler intent stale: net={:.8}, pending_same_side={:.8}",
                    net, pending
                ));
                hedge_store::try_append_lifecycle_update(update).await;
                info!(
                    "{} {} Skipping stale reconciler intent for {} (net {:.8} already neutral/covered)",
                    tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                    "OK".green().bold(),
                    event.size,
                    net
                );
                None
            }
        }
    }

    /// Heuristic for Hyperliquid's (undocumented) duplicate-cloid rejection text.
    /// Only affects log wording / which branch we take; correctness does not
    /// depend on it because escalation is always gated by an authoritative
    /// `orderStatus` check (`cloid_definitively_unfilled`).
    fn is_duplicate_cloid_error(msg: &str) -> bool {
        let m = msg.to_ascii_lowercase();
        let mentions_cloid = m.contains("cloid")
            || m.contains("clientorderid")
            || m.contains("client order id");
        let mentions_reuse = m.contains("already")
            || m.contains("reused")
            || m.contains("duplicate")
            || m.contains("in use");
        mentions_cloid && mentions_reuse
    }

    /// Build a synthetic `Filled` `OrderResponse` from a confirmed price+size.
    ///
    /// Used when `orderStatus`/`userFills` proves a hedge actually filled after
    /// an uncertain submit, so the downstream profit/verification logic can treat
    /// it identically to a direct fill response.
    fn filled_response(avg_px: f64, total_sz: f64) -> OrderResponse {
        use crate::connector::hyperliquid::types::{
            FilledOrder, OrderResponseData, OrderStatusData,
        };

        OrderResponse {
            status: "ok".to_string(),
            response: OrderResponseContent::Success(OrderResponseData {
                type_: Some("order".to_string()),
                data: OrderStatusData {
                    statuses: vec![OrderStatus::Filled {
                        filled: FilledOrder {
                            total_sz: total_sz.to_string(),
                            avg_px: avg_px.to_string(),
                            oid: 0,
                        },
                    }],
                },
            }),
        }
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

    fn slippage_for_attempt(&self, attempt: usize) -> f64 {
        let bps = if attempt == 0 {
            self.config.hedge_slippage_bps_initial
        } else if attempt + 1 >= self.config.max_consecutive_hedge_failures as usize {
            self.config.hedge_slippage_bps_emergency
        } else {
            self.config.hedge_slippage_bps_retry
        };
        bps / 10_000.0
    }

    #[cfg(test)]
    fn hedge_cloid(source_order_id: u64, hedge_seq: u64) -> String {
        Self::idempotency_cloid(source_order_id, hedge_seq, 0)
    }

    fn idempotency_cloid(source_order_id: u64, hedge_seq: u64, request_group: u64) -> String {
        let hi = source_order_id ^ (hedge_seq << 32);
        format!("0x{:016x}{:016x}", hi, request_group)
    }

    fn residual_cloid(
        source_order_id: u64,
        hedge_seq: u64,
        residual_seq: u64,
        submit_attempt: u64,
    ) -> String {
        let hi = source_order_id ^ (hedge_seq << 32) ^ (residual_seq << 48);
        format!(
            "0x{:016x}{:016x}",
            hi,
            submit_attempt | (residual_seq << 32)
        )
    }

    fn filled_qty_from_response(
        response: &crate::connector::hyperliquid::OrderResponse,
    ) -> Option<(f64, f64)> {
        let crate::connector::hyperliquid::OrderResponseContent::Success(data) = &response.response
        else {
            return None;
        };
        let status = data.data.statuses.first()?;
        if let crate::connector::hyperliquid::OrderStatus::Filled { filled } = status {
            let px = filled.avg_px.parse::<f64>().ok()?;
            let qty = filled.total_sz.parse::<f64>().ok()?;
            Some((px, qty))
        } else {
            None
        }
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
        cloid: Option<String>,
        slippage: f64,
    ) -> anyhow::Result<crate::connector::hyperliquid::OrderResponse> {
        // Build signed order payload (same as REST)
        let payload = self
            .hyperliquid_trading
            .build_market_order_request_with_cloid(
                &self.config.symbol,
                is_buy,
                size,
                slippage,
                false,
                Some(bid),
                Some(ask),
                cloid,
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
        if !self.config.low_latency_mode {
            info!(
                "{} Sending Hyperliquid hedge order via WebSocket (id={})",
                tag(&self.config.symbol, "HEDGE", Color::BrightMagenta),
                request_id
            );
        }
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
                        // Response for another request or channel - ignore
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
                            anyhow::bail!(
                                "Unexpected Hyperliquid WebSocket response type: {}",
                                other
                            );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hedge_cloid_is_deterministic_and_intent_scoped() {
        let first = HedgeService::hedge_cloid(42, 3);
        let same = HedgeService::hedge_cloid(42, 3);
        let different_intent = HedgeService::hedge_cloid(42, 4);

        assert_eq!(first, same);
        assert_ne!(first, different_intent);
        assert!(first.starts_with("0x"));
    }

    #[test]
    fn uncertain_retry_and_residual_cloids_have_different_scopes() {
        let same_unknown = HedgeService::idempotency_cloid(42, 3, 0);
        let same_unknown_again = HedgeService::idempotency_cloid(42, 3, 0);
        let new_after_reject = HedgeService::idempotency_cloid(42, 3, 1);
        let residual = HedgeService::residual_cloid(42, 3, 1, 4);

        assert_eq!(same_unknown, same_unknown_again);
        assert_ne!(same_unknown, new_after_reject);
        assert_ne!(same_unknown, residual);
    }

    #[test]
    fn filled_qty_from_response_extracts_actual_filled_size() {
        let response = HedgeService::filled_response(123.45, 0.67);
        let (px, qty) =
            HedgeService::filled_qty_from_response(&response).expect("filled response expected");

        assert!((px - 123.45).abs() < 1e-9);
        assert!((qty - 0.67).abs() < 1e-9);
    }

    #[test]
    fn filled_qty_from_response_rejects_error_response() {
        let response = OrderResponse {
            status: "ok".to_string(),
            response: OrderResponseContent::Error("rejected".to_string()),
        };

        assert!(HedgeService::filled_qty_from_response(&response).is_none());
    }

    #[test]
    fn duplicate_cloid_error_is_recognized_case_insensitively() {
        assert!(HedgeService::is_duplicate_cloid_error(
            "Order rejected by exchange: ClientOrderId already used"
        ));
        assert!(HedgeService::is_duplicate_cloid_error("cloid already in use"));
        assert!(HedgeService::is_duplicate_cloid_error(
            "duplicate client order id"
        ));
        // Unrelated errors must not match (they fall through to the Reject path,
        // which still confirms via orderStatus before escalating).
        assert!(!HedgeService::is_duplicate_cloid_error(
            "Order could not immediately match against any resting orders."
        ));
        assert!(!HedgeService::is_duplicate_cloid_error("tick rejected"));
    }

    #[test]
    fn definitively_unfilled_classification_matrix() {
        let dust = 0.01;
        assert!(HedgeService::definitively_unfilled(
            &OrderStatusQuery::Rejected {
                oid: 1,
                status: "rejected".to_string()
            },
            dust
        ));
        assert!(HedgeService::definitively_unfilled(
            &OrderStatusQuery::Canceled {
                oid: 2,
                status: "canceled".to_string(),
                filled_sz: 0.005,
                avg_px: 100.0
            },
            dust
        ));
        // Canceled with a real partial fill is NOT unfilled.
        assert!(!HedgeService::definitively_unfilled(
            &OrderStatusQuery::Canceled {
                oid: 3,
                status: "canceled".to_string(),
                filled_sz: 0.2,
                avg_px: 100.0
            },
            dust
        ));
        assert!(!HedgeService::definitively_unfilled(
            &OrderStatusQuery::Filled {
                filled_sz: 0.5,
                avg_px: 100.0,
                oid: 4
            },
            dust
        ));
        assert!(!HedgeService::definitively_unfilled(
            &OrderStatusQuery::Resting {
                oid: 5,
                remaining_sz: 0.5
            },
            dust
        ));
        // Unknown is never proof of no-fill: escalating on it after a
        // duplicate-cloid rejection is the double-submit hazard.
        assert!(!HedgeService::definitively_unfilled(
            &OrderStatusQuery::Unknown,
            dust
        ));
    }

    #[test]
    fn reconciler_size_validation_clamps_and_skips() {
        use HedgeVenueSide::{Buy, Sell};
        let dust = 0.01;

        // Net long 1.0, nothing pending: Sell intent passes at full size.
        assert_eq!(
            HedgeService::validate_reconciler_size(1.0, 0.0, Sell, 1.0, dust),
            Some(1.0)
        );
        // Net shrank to 0.4 since the intent was minted: clamp.
        assert_eq!(
            HedgeService::validate_reconciler_size(0.4, 0.0, Sell, 1.0, dust),
            Some(0.4)
        );
        // Same-side pending already covers most of it: clamp to the remainder.
        let clamped = HedgeService::validate_reconciler_size(1.0, 0.7, Sell, 1.0, dust).unwrap();
        assert!((clamped - 0.3).abs() < 1e-9);
        // Fully covered by pending: skip.
        assert_eq!(
            HedgeService::validate_reconciler_size(1.0, 1.0, Sell, 1.0, dust),
            None
        );
        // Exposure already neutral: skip.
        assert_eq!(
            HedgeService::validate_reconciler_size(0.005, 0.0, Sell, 1.0, dust),
            None
        );
        // Direction flipped (net short now requires a Buy hedge): skip the Sell.
        assert_eq!(
            HedgeService::validate_reconciler_size(-1.0, 0.0, Sell, 1.0, dust),
            None
        );
        assert_eq!(
            HedgeService::validate_reconciler_size(-1.0, 0.0, Buy, 1.0, dust),
            Some(1.0)
        );
        // Remainder below dust: skip rather than submit an unfillable sliver.
        assert_eq!(
            HedgeService::validate_reconciler_size(1.0, 0.995, Sell, 1.0, dust),
            None
        );
    }

    #[test]
    fn order_status_query_parses_filled_canceled_and_unknown() {
        use crate::connector::hyperliquid::types::OrderStatusResponse;

        // status:"order" + filled
        let filled: OrderStatusResponse = serde_json::from_str(
            r#"{"status":"order","order":{"order":{"coin":"SOL","side":"B","limitPx":"100.0","sz":"0.0","oid":7,"origSz":"0.5","cloid":"0x00000000000000000000000000000001"},"status":"filled","statusTimestamp":1}}"#,
        )
        .unwrap();
        assert_eq!(filled.status, "order");
        let w = filled.order.unwrap();
        assert_eq!(w.status, "filled");
        let orig: f64 = w.order.orig_sz.parse().unwrap();
        let rem: f64 = w.order.sz.parse().unwrap();
        assert!((orig - rem - 0.5).abs() < 1e-9);
        assert_eq!(w.order.oid, 7);

        // status:"order" + canceled with a partial fill (remaining < orig)
        let canceled: OrderStatusResponse = serde_json::from_str(
            r#"{"status":"order","order":{"order":{"coin":"SOL","side":"A","limitPx":"100.0","sz":"0.2","oid":9,"origSz":"0.5","cloid":null},"status":"canceled","statusTimestamp":1}}"#,
        )
        .unwrap();
        let w = canceled.order.unwrap();
        assert_eq!(w.status, "canceled");
        let orig: f64 = w.order.orig_sz.parse().unwrap();
        let rem: f64 = w.order.sz.parse().unwrap();
        assert!((orig - rem - 0.3).abs() < 1e-9);

        // unknownOid
        let unknown: OrderStatusResponse =
            serde_json::from_str(r#"{"status":"unknownOid"}"#).unwrap();
        assert_eq!(unknown.status, "unknownOid");
        assert!(unknown.order.is_none());
    }
}
