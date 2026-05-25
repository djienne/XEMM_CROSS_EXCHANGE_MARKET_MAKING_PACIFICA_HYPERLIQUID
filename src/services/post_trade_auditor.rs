use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use colored::Colorize;
use fast_float::parse;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::bot::BotState;
use crate::config::Config;
use crate::connector::hyperliquid::HyperliquidTrading;
use crate::connector::pacifica::PacificaTrading;
use crate::csv_logger;
use crate::strategy::OrderSide;
use crate::trade_fetcher;
use crate::util::log::{tag, Color};

#[derive(Debug, Clone)]
pub struct PostTradeAuditEvent {
    pub source_order_id: u64,
    pub hedge_seq: u64,
    pub side: OrderSide,
    pub size: f64,
    pub maker_avg_price: f64,
    pub hedge_fill_price: f64,
    pub confirmed_hedge_size: f64,
    pub latency_ms: f64,
    pub expected_profit_bps: Option<f64>,
    pub client_order_id: Option<String>,
}

pub struct PostTradeAuditorService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub audit_rx: mpsc::Receiver<PostTradeAuditEvent>,
    pub config: Config,
    pub hyperliquid_trading: Arc<HyperliquidTrading>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub shutdown_tx: mpsc::Sender<()>,
}

impl PostTradeAuditorService {
    pub async fn run(mut self) {
        while let Some(event) = self.audit_rx.recv().await {
            self.audit(event).await;
        }
    }

    async fn audit(&self, event: PostTradeAuditEvent) {
        info!(
            "{} Cold audit started for hedge intent {}-{}",
            tag(&self.config.symbol, "AUDIT", Color::Blue),
            event.source_order_id,
            event.hedge_seq
        );

        tokio::time::sleep(Duration::from_secs(20)).await;

        let pacifica = self.fetch_pacifica_trade(&event).await;
        let hyperliquid = self.fetch_hyperliquid_trade().await;

        let (
            actual_profit_bps,
            actual_profit_usd,
            pacifica_actual_price,
            hl_actual_price,
            pac_fee_usd,
            hl_fee_usd,
        ) = self.calculate_profit(&event, &pacifica, &hyperliquid);

        self.queue_csv_record(
            &event,
            pacifica_actual_price,
            hl_actual_price,
            pacifica.total_notional,
            hyperliquid.total_notional,
            pac_fee_usd,
            hl_fee_usd,
            actual_profit_bps,
            actual_profit_usd,
        );

        self.log_summary(
            &event,
            pacifica_actual_price,
            hl_actual_price,
            actual_profit_bps,
            actual_profit_usd,
            pac_fee_usd,
            hl_fee_usd,
            pacifica.actual_fee.is_some(),
            hyperliquid.actual_fee.is_some(),
        );

        self.final_cancel().await;

        if self.verify_neutral().await {
            {
                let mut state = self.bot_state.write();
                state.mark_complete();
            }
            let _ = self.shutdown_tx.send(()).await;
        } else {
            {
                let mut state = self.bot_state.write();
                state.mark_reconciling();
            }
            warn!(
                "{} {} Final position verification failed; leaving bot in Reconciling for residual hedge retry",
                tag(&self.config.symbol, "VERIFY", Color::Yellow),
                "WARN".yellow().bold()
            );
        }
    }

    async fn fetch_pacifica_trade(
        &self,
        event: &PostTradeAuditEvent,
    ) -> trade_fetcher::TradeFetchResult {
        if let Some(cloid) = &event.client_order_id {
            trade_fetcher::fetch_pacifica_trade(
                self.pacifica_trading.clone(),
                &self.config.symbol,
                cloid,
                3,
                |msg| {
                    info!(
                        "{} {}",
                        tag(&self.config.symbol, "PROFIT", Color::Blue),
                        msg
                    )
                },
            )
            .await
        } else {
            trade_fetcher::TradeFetchResult {
                fill_price: None,
                actual_fee: None,
                total_size: None,
                total_notional: None,
            }
        }
    }

    async fn fetch_hyperliquid_trade(&self) -> trade_fetcher::TradeFetchResult {
        let hl_wallet = std::env::var("HL_WALLET")
            .unwrap_or_else(|_| self.hyperliquid_trading.get_wallet_address());
        trade_fetcher::fetch_hyperliquid_fills(
            &self.hyperliquid_trading,
            &hl_wallet,
            &self.config.symbol,
            3,
            30,
            |msg| {
                info!(
                    "{} {}",
                    tag(&self.config.symbol, "PROFIT", Color::Blue),
                    msg
                )
            },
        )
        .await
    }

    fn calculate_profit(
        &self,
        event: &PostTradeAuditEvent,
        pacifica: &trade_fetcher::TradeFetchResult,
        hyperliquid: &trade_fetcher::TradeFetchResult,
    ) -> (f64, f64, Option<f64>, Option<f64>, f64, f64) {
        match (
            pacifica.total_notional,
            hyperliquid.total_notional,
            pacifica.fill_price,
            hyperliquid.fill_price,
        ) {
            (Some(pac_notional), Some(hl_notional), pac_price_opt, hl_price_opt) => {
                let pac_fee = pacifica.actual_fee.unwrap_or_else(|| {
                    pac_notional * (self.config.pacifica_maker_fee_bps / 10000.0)
                });
                let hl_fee = hyperliquid.actual_fee.unwrap_or_else(|| {
                    hl_notional * (self.config.hyperliquid_taker_fee_bps / 10000.0)
                });
                let profit = trade_fetcher::calculate_hedge_profit(
                    pac_notional,
                    hl_notional,
                    pac_fee,
                    hl_fee,
                    matches!(event.side, OrderSide::Buy),
                );
                (
                    profit.profit_bps,
                    profit.net_profit,
                    pac_price_opt,
                    hl_price_opt,
                    pac_fee,
                    hl_fee,
                )
            }
            _ => {
                warn!(
                    "{} {} Using fill event data (trade history unavailable)",
                    tag(&self.config.symbol, "PROFIT", Color::Blue),
                    "WARN".yellow().bold()
                );
                let pac_price = event.maker_avg_price;
                let hl_price = event.hedge_fill_price;
                let qty = event.confirmed_hedge_size.min(event.size);
                let pac_fee = pac_price * qty * (self.config.pacifica_maker_fee_bps / 10000.0);
                let hl_fee = hl_price * qty * (self.config.hyperliquid_taker_fee_bps / 10000.0);
                let (profit_usd, cost) = match event.side {
                    OrderSide::Buy => {
                        let cost = (pac_price * qty) + pac_fee;
                        let revenue = (hl_price * qty) - hl_fee;
                        (revenue - cost, cost)
                    }
                    OrderSide::Sell => {
                        let revenue = (pac_price * qty) - pac_fee;
                        let cost = (hl_price * qty) + hl_fee;
                        (revenue - cost, cost)
                    }
                };
                let profit_bps = if cost > 0.0 {
                    (profit_usd / cost) * 10000.0
                } else {
                    0.0
                };
                (
                    profit_bps,
                    profit_usd,
                    Some(pac_price),
                    Some(hl_price),
                    pac_fee,
                    hl_fee,
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn queue_csv_record(
        &self,
        event: &PostTradeAuditEvent,
        pacifica_actual_price: Option<f64>,
        hl_actual_price: Option<f64>,
        pacifica_notional: Option<f64>,
        hl_notional: Option<f64>,
        pac_fee_usd: f64,
        hl_fee_usd: f64,
        actual_profit_bps: f64,
        actual_profit_usd: f64,
    ) {
        let (Some(pac_price), Some(hl_price)) = (pacifica_actual_price, hl_actual_price) else {
            return;
        };
        let trade_record = csv_logger::TradeRecord::new(
            Utc::now(),
            event.latency_ms,
            self.config.symbol.clone(),
            event.side,
            pac_price,
            event.size,
            pacifica_notional.unwrap_or(pac_price * event.size),
            pac_fee_usd,
            hl_price,
            event.confirmed_hedge_size,
            hl_notional.unwrap_or(hl_price * event.confirmed_hedge_size),
            hl_fee_usd,
            event.expected_profit_bps.unwrap_or(0.0),
            actual_profit_bps,
            actual_profit_usd,
        );
        let csv_file = format!("{}_trades.csv", self.config.symbol.to_lowercase());
        csv_logger::log_trade_async(csv_file.clone(), trade_record);
        info!(
            "{} {} Trade queued to {}",
            tag(&self.config.symbol, "CSV", Color::BrightGreen),
            "OK".green().bold(),
            csv_file
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn log_summary(
        &self,
        event: &PostTradeAuditEvent,
        pacifica_actual_price: Option<f64>,
        hl_actual_price: Option<f64>,
        actual_profit_bps: f64,
        actual_profit_usd: f64,
        pac_fee_usd: f64,
        hl_fee_usd: f64,
        pac_fee_actual: bool,
        hl_fee_actual: bool,
    ) {
        info!(
            "{}",
            "==================================================="
                .green()
                .bold()
        );
        info!("{}", "  BOT CYCLE AUDIT COMPLETE".green().bold());
        info!(
            "{}",
            "==================================================="
                .green()
                .bold()
        );
        if let Some(pac_price) = pacifica_actual_price {
            info!(
                "  Pacifica: {} {:.4} {} @ ${:.6}",
                event.side.as_str().bright_yellow(),
                event.size,
                self.config.symbol.bright_white().bold(),
                pac_price
            );
        }
        if let Some(hl_price) = hl_actual_price {
            let hl_side = match event.side {
                OrderSide::Buy => "SELL",
                OrderSide::Sell => "BUY",
            };
            info!(
                "  Hyperliquid: {} {:.4} {} @ ${:.6}",
                hl_side.bright_yellow(),
                event.confirmed_hedge_size,
                self.config.symbol.bright_white().bold(),
                hl_price
            );
        }
        if let Some(expected) = event.expected_profit_bps {
            info!("  Expected: {:.2} bps", expected);
        }
        info!(
            "  Actual: {:.2} bps ({})",
            actual_profit_bps,
            format!("${:.4}", actual_profit_usd).bright_white()
        );
        info!(
            "  Fees: Pacifica ${:.4} {}, Hyperliquid ${:.4} {}, total ${:.4}",
            pac_fee_usd,
            if pac_fee_actual {
                "(actual)"
            } else {
                "(estimated)"
            },
            hl_fee_usd,
            if hl_fee_actual {
                "(actual)"
            } else {
                "(estimated)"
            },
            pac_fee_usd + hl_fee_usd
        );
        info!(
            "{}",
            "==================================================="
                .green()
                .bold()
        );
    }

    async fn final_cancel(&self) {
        info!(
            "{} Post-hedge safety: final Pacifica cancellation",
            tag(&self.config.symbol, "AUDIT", Color::Blue)
        );
        if let Err(e) = self
            .pacifica_trading
            .cancel_all_orders(false, Some(&self.config.symbol), false)
            .await
        {
            warn!(
                "{} {} Failed to cancel orders after hedge completion: {}",
                tag(&self.config.symbol, "AUDIT", Color::Yellow),
                "WARN".yellow().bold(),
                e
            );
        }
    }

    async fn verify_neutral(&self) -> bool {
        info!(
            "{} Waiting 8 seconds before final position verification",
            tag(&self.config.symbol, "VERIFY", Color::Cyan)
        );
        tokio::time::sleep(Duration::from_secs(8)).await;

        let pacifica_position = self.fetch_pacifica_position().await;
        let hyperliquid_position = self.fetch_hyperliquid_position().await;

        if let (Some(pac_pos), Some(hl_pos)) = (pacifica_position, hyperliquid_position) {
            let net_position = pac_pos + hl_pos;
            info!(
                "{} Net Position: {:.4} (Pacifica: {:.4} + Hyperliquid: {:.4})",
                tag(&self.config.symbol, "VERIFY", Color::Cyan),
                net_position,
                pac_pos,
                hl_pos
            );
            if net_position.abs() <= self.config.neutral_dust_base {
                match self.pacifica_trading.get_open_orders().await {
                    Ok(orders)
                        if orders
                            .iter()
                            .any(|order| order.symbol == self.config.symbol) =>
                    {
                        warn!(
                            "{} {} Net position is neutral but Pacifica still has open orders",
                            tag(&self.config.symbol, "VERIFY", Color::Yellow),
                            "WARN".yellow().bold()
                        );
                        return false;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!(
                            "{} {} Could not verify Pacifica open orders are clear: {}",
                            tag(&self.config.symbol, "VERIFY", Color::Yellow),
                            "WARN".yellow().bold(),
                            e
                        );
                        return false;
                    }
                }
                info!(
                    "{} {} Net position is neutral",
                    tag(&self.config.symbol, "VERIFY", Color::Cyan),
                    "OK".green().bold()
                );
                true
            } else {
                warn!(
                    "{} {} Net position is not neutral: {:.4}",
                    tag(&self.config.symbol, "VERIFY", Color::Red),
                    "WARN".yellow().bold(),
                    net_position
                );
                false
            }
        } else {
            warn!(
                "{} {} Could not verify net position",
                tag(&self.config.symbol, "VERIFY", Color::Yellow),
                "WARN".yellow().bold()
            );
            false
        }
    }

    async fn fetch_pacifica_position(&self) -> Option<f64> {
        match self.pacifica_trading.get_positions().await {
            Ok(positions) => {
                if let Some(pos) = positions.iter().find(|p| p.symbol == self.config.symbol) {
                    let amount: f64 = parse(&pos.amount).unwrap_or(0.0);
                    let signed = if pos.side == "bid" { amount } else { -amount };
                    info!(
                        "{} Pacifica signed position: {:.4}",
                        tag(&self.config.symbol, "VERIFY", Color::Cyan),
                        signed
                    );
                    Some(signed)
                } else {
                    Some(0.0)
                }
            }
            Err(e) => {
                warn!(
                    "{} Failed to fetch Pacifica position: {}",
                    tag(&self.config.symbol, "VERIFY", Color::Yellow),
                    e
                );
                None
            }
        }
    }

    async fn fetch_hyperliquid_position(&self) -> Option<f64> {
        let hl_wallet = std::env::var("HL_WALLET")
            .unwrap_or_else(|_| self.hyperliquid_trading.get_wallet_address());
        for retry in 0..3 {
            if retry > 0 {
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            match self.hyperliquid_trading.get_user_state(&hl_wallet).await {
                Ok(user_state) => {
                    if let Some(asset_pos) = user_state
                        .asset_positions
                        .iter()
                        .find(|ap| ap.position.coin == self.config.symbol)
                    {
                        let signed = parse(&asset_pos.position.szi).unwrap_or(0.0);
                        info!(
                            "{} Hyperliquid signed position: {:.4}",
                            tag(&self.config.symbol, "VERIFY", Color::Cyan),
                            signed
                        );
                        return Some(signed);
                    }
                }
                Err(e) if retry == 2 => {
                    warn!(
                        "{} Failed to fetch Hyperliquid position: {}",
                        tag(&self.config.symbol, "VERIFY", Color::Yellow),
                        e
                    );
                    return None;
                }
                Err(_) => {}
            }
        }
        Some(0.0)
    }
}
