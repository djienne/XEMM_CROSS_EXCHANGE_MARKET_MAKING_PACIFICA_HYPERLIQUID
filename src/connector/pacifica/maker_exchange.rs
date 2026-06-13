//! Pacifica implementation of the maker-venue abstraction.
//!
//! `PacificaMaker` / `PacificaFillStream` are **thin delegations** to the
//! existing, tested `PacificaTrading` / `PacificaWsTrading` / `FillDetectionClient`
//! code: behaviour is identical to calling those directly. All Pacifica-specific
//! concerns — Ed25519/base58 signing, `"bid"`/`"ask"` side strings, the
//! WS-preferred-with-REST-fallback placement choice, the canonical-JSON field
//! names — stay inside this connector. Services only ever see normalized types.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;

use crate::services::maker::types::{
    MakerFillEvent, MakerFillSummary, MakerOpenOrder, MakerOrderAck, MakerPosition,
    MakerSymbolRules, MakerTrade,
};
use crate::services::maker::{
    MakerBaselineUpdater, MakerExchange, MakerFillCallback, MakerFillStream, MakerReconcileHook,
};
use crate::strategy::OrderSide;
use crate::util::cancel::dual_cancel;

use super::trading::{
    OpenOrderItem, OrderData, OrderSide as PacOrderSide, PositionItem, TradeHistoryItem,
};
use super::{FillDetectionClient, FillEvent, PacificaTrading, PacificaWsTrading, PositionBaselineUpdater};

// ---------------------------------------------------------------------------
// Boundary conversions (pure; unit-tested below)
// ---------------------------------------------------------------------------

/// Strategy side -> Pacifica's bid/ask side enum.
#[inline]
fn pac_side(side: OrderSide) -> PacOrderSide {
    match side {
        OrderSide::Buy => PacOrderSide::Buy,
        OrderSide::Sell => PacOrderSide::Sell,
    }
}

/// Pacifica order/position side string -> strategy side. `"buy"`/`"bid"` -> Buy,
/// `"sell"`/`"ask"` -> Sell, anything else -> `None`. Mirrors the existing
/// `order_side_from_str` used by the fill pipeline, so unknown sides are skipped
/// exactly as before rather than mis-hedged.
#[inline]
fn side_from_str(side: &str) -> Option<OrderSide> {
    match side {
        "buy" | "bid" => Some(OrderSide::Buy),
        "sell" | "ask" => Some(OrderSide::Sell),
        _ => None,
    }
}

#[inline]
fn parse_f64(s: &str) -> f64 {
    fast_float::parse(s).unwrap_or(0.0)
}

/// `"bid"` -> +amount (long), `"ask"` -> -amount (short), else 0. Matches the
/// sign convention in `services::signed_positions`.
#[inline]
fn signed_base(side: &str, amount: f64) -> f64 {
    match side {
        "bid" => amount,
        "ask" => -amount,
        _ => 0.0,
    }
}

fn ack_from_order_data(d: OrderData) -> MakerOrderAck {
    MakerOrderAck {
        // Pacifica returns the id under either `order_id` or `i`.
        order_id: d.order_id.or(d.i),
        client_order_id: d.client_order_id,
        submitted_price: d.submitted_price,
        submitted_size: d.submitted_size,
    }
}

/// Normalize an open order; `None` (skipped) on an unrecognized side string,
/// matching the existing reconcile-hook behaviour.
fn open_order_from(o: OpenOrderItem) -> Option<MakerOpenOrder> {
    let side = side_from_str(&o.side)?;
    Some(MakerOpenOrder {
        order_id: o.order_id,
        client_order_id: o.client_order_id,
        symbol: o.symbol,
        side,
        price: parse_f64(&o.price),
        initial_amount: parse_f64(&o.initial_amount),
        filled_amount: parse_f64(&o.filled_amount),
        cancelled_amount: parse_f64(&o.cancelled_amount),
        reduce_only: o.reduce_only,
        created_at: o.created_at,
        updated_at: o.updated_at,
    })
}

fn trade_from(t: &TradeHistoryItem) -> MakerTrade {
    MakerTrade {
        order_id: t.order_id,
        client_order_id: t.client_order_id.clone(),
        amount: parse_f64(&t.amount),
        entry_price: parse_f64(&t.entry_price),
        fee: parse_f64(&t.fee),
        is_maker_fill: t.event_type == "fulfill_maker",
        created_at: t.created_at,
    }
}

/// Convert a connector `FillEvent` into the normalized event. `None` (skipped)
/// on an unrecognized side string, preserving the current "unknown side -> do
/// not hedge" behaviour of `FillEventProcessor`.
///
/// The `symbol` field is carried through as the venue's wire string. Downstream
/// it is used only for logging - every venue operation triggered by a fill uses
/// the service's canonical symbol - so it is intentionally not mapped back to
/// canonical here (a differently-tickered venue simply logs its own ticker).
fn maker_fill_event_from(fe: FillEvent) -> Option<MakerFillEvent> {
    Some(match fe {
        FillEvent::PartialFill {
            order_id,
            client_order_id,
            symbol,
            side,
            filled_amount,
            original_amount,
            avg_price,
            timestamp,
        } => MakerFillEvent::Partial {
            order_id,
            client_order_id,
            symbol,
            side: side_from_str(&side)?,
            filled: parse_f64(&filled_amount),
            original: parse_f64(&original_amount),
            avg_price: parse_f64(&avg_price),
            ts: timestamp,
        },
        FillEvent::FullFill {
            order_id,
            client_order_id,
            symbol,
            side,
            filled_amount,
            avg_price,
            timestamp,
        } => MakerFillEvent::Full {
            order_id,
            client_order_id,
            symbol,
            side: side_from_str(&side)?,
            filled: parse_f64(&filled_amount),
            avg_price: parse_f64(&avg_price),
            ts: timestamp,
        },
        FillEvent::Cancelled {
            order_id,
            client_order_id,
            symbol,
            side,
            filled_amount,
            original_amount,
            reason,
            timestamp,
        } => MakerFillEvent::Cancelled {
            order_id,
            client_order_id,
            symbol,
            side: side_from_str(&side)?,
            filled: parse_f64(&filled_amount),
            original: parse_f64(&original_amount),
            reason,
            ts: timestamp,
        },
        FillEvent::PositionFill {
            symbol,
            side,
            filled_amount,
            avg_price,
            timestamp,
            position_delta,
            prev_position,
            new_position,
            cross_validated,
        } => MakerFillEvent::Position {
            symbol,
            side: side_from_str(&side)?,
            filled: parse_f64(&filled_amount),
            avg_price: parse_f64(&avg_price),
            ts: timestamp,
            position_delta: parse_f64(&position_delta),
            prev_position: parse_f64(&prev_position),
            new_position: parse_f64(&new_position),
            cross_validated,
        },
    })
}

// ---------------------------------------------------------------------------
// Symbol mapping
// ---------------------------------------------------------------------------

/// Maps between the **canonical** symbol the services (and the Hyperliquid taker
/// leg) use and the **maker venue's own wire symbol**. When the maker venue
/// names the asset the same way - the default, `wire == canonical` - every
/// method here is a no-op, so behaviour is byte-identical. A maker venue that
/// uses a different ticker (e.g. `"SOL-PERP"` vs `"SOL"`) is then a pure config
/// change (`maker_symbol`): the adapter translates on the way in and out, so no
/// service ever sees the wire string.
#[derive(Clone, Debug)]
struct SymbolMap {
    canonical: String,
    wire: String,
}

impl SymbolMap {
    fn new(canonical: impl Into<String>, wire: impl Into<String>) -> Self {
        Self {
            canonical: canonical.into(),
            wire: wire.into(),
        }
    }

    /// Canonical (what services pass in) -> maker wire (what the venue expects).
    /// Anything that is not the canonical symbol passes through unchanged.
    #[inline]
    fn to_wire<'a>(&'a self, canonical: &'a str) -> &'a str {
        if canonical == self.canonical {
            &self.wire
        } else {
            canonical
        }
    }

    /// Maker wire (what the venue returns) -> canonical (what services compare
    /// against). Orders for other symbols pass through unchanged.
    #[inline]
    fn to_canonical(&self, wire: &str) -> String {
        if wire == self.wire {
            self.canonical.clone()
        } else {
            wire.to_string()
        }
    }
}

// ---------------------------------------------------------------------------
// MakerExchange
// ---------------------------------------------------------------------------

/// Maker adapter over Pacifica's REST + WS trading clients.
pub struct PacificaMaker {
    pub rest: Arc<PacificaTrading>,
    pub ws: Arc<PacificaWsTrading>,
    /// Canonical <-> Pacifica wire symbol translation (identity unless
    /// `maker_symbol` is configured).
    symbols: SymbolMap,
}

impl PacificaMaker {
    pub fn new(
        rest: Arc<PacificaTrading>,
        ws: Arc<PacificaWsTrading>,
        canonical_symbol: impl Into<String>,
        wire_symbol: impl Into<String>,
    ) -> Self {
        Self {
            rest,
            ws,
            symbols: SymbolMap::new(canonical_symbol, wire_symbol),
        }
    }
}

#[async_trait]
impl MakerExchange for PacificaMaker {
    fn label(&self) -> &'static str {
        "PACIFICA"
    }

    async fn symbol_rules(&self, symbol: &str) -> Result<MakerSymbolRules> {
        let wire = self.symbols.to_wire(symbol);
        let info = self.rest.get_market_info().await?;
        let m = info
            .get(wire)
            .with_context(|| format!("Market info not found for {}", wire))?;
        Ok(MakerSymbolRules {
            tick_size: m.tick_size.clone(),
            lot_size: m.lot_size.clone(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        size: f64,
        price: f64,
        rules: &MakerSymbolRules,
        client_order_id: String,
        current_bid: f64,
        current_ask: f64,
    ) -> Result<MakerOrderAck> {
        let ps = pac_side(side);
        let wire = self.symbols.to_wire(symbol);
        // WS-preferred with REST fallback — identical to the previous inline
        // hot-path branch (app.rs). The impl owns this choice now.
        let order_data = if self.ws.is_connected() {
            self.ws
                .place_limit_order_ws(
                    wire,
                    ps,
                    size,
                    price,
                    &rules.tick_size,
                    &rules.lot_size,
                    client_order_id,
                )
                .await?
        } else {
            self.rest
                .place_limit_order_with_client_order_id(
                    wire,
                    ps,
                    size,
                    Some(price),
                    0.0,
                    Some(current_bid),
                    Some(current_ask),
                    Some(client_order_id),
                )
                .await?
        };
        Ok(ack_from_order_data(order_data))
    }

    async fn cancel_all(&self, symbol: &str) -> Result<(u32, u32)> {
        dual_cancel(&self.rest, &self.ws, self.symbols.to_wire(symbol)).await
    }

    async fn open_orders(&self) -> Result<Vec<MakerOpenOrder>> {
        let orders = self.rest.get_open_orders().await?;
        // Map each order's wire symbol back to the canonical one so the services
        // (which compare against the canonical `config.symbol`) still match.
        Ok(orders
            .into_iter()
            .filter_map(open_order_from)
            .map(|mut o| {
                o.symbol = self.symbols.to_canonical(&o.symbol);
                o
            })
            .collect())
    }

    async fn position(&self, symbol: &str) -> Result<MakerPosition> {
        Ok(self.position_opt(symbol).await?.unwrap_or_default())
    }

    async fn position_opt(&self, symbol: &str) -> Result<Option<MakerPosition>> {
        let wire = self.symbols.to_wire(symbol);
        let positions = self.rest.get_positions().await?;
        Ok(positions
            .iter()
            .find(|p: &&PositionItem| p.symbol == wire)
            .map(|p| MakerPosition {
                signed_base: signed_base(&p.side, parse_f64(&p.amount)),
                entry_price: parse_f64(&p.entry_price),
            }))
    }

    async fn recent_trades(&self, symbol: &str, limit: u32) -> Result<Vec<MakerTrade>> {
        let trades = self
            .rest
            .get_trade_history(Some(self.symbols.to_wire(symbol)), Some(limit), None, None)
            .await?;
        Ok(trades.iter().map(trade_from).collect())
    }

    async fn maker_fill_summary(
        &self,
        symbol: &str,
        client_order_id: &str,
        max_attempts: u32,
    ) -> MakerFillSummary {
        let r = crate::trade_fetcher::fetch_pacifica_trade(
            self.rest.clone(),
            self.symbols.to_wire(symbol),
            client_order_id,
            max_attempts,
            |_| {},
        )
        .await;
        MakerFillSummary {
            fill_price: r.fill_price,
            actual_fee: r.actual_fee,
            total_size: r.total_size,
            total_notional: r.total_notional,
        }
    }
}

// ---------------------------------------------------------------------------
// MakerFillStream
// ---------------------------------------------------------------------------

/// Adapter that maps the connector's `PositionBaselineUpdater` (side as
/// `"buy"`/`"sell"`) onto the trait's `OrderSide`-typed interface.
struct PacificaBaselineUpdater(PositionBaselineUpdater);

impl MakerBaselineUpdater for PacificaBaselineUpdater {
    fn update_baseline(&self, symbol: &str, side: OrderSide, filled: f64, avg_price: f64) {
        // Pass Pacifica's native side string ("bid"/"ask") — the exact value the
        // legacy order-fill path fed to `PositionBaselineUpdater`, so the
        // downstream snapshot math is byte-for-byte unchanged.
        let s = match side {
            OrderSide::Buy => "bid",
            OrderSide::Sell => "ask",
        };
        self.0.update_baseline(symbol, s, filled, avg_price);
    }
}

/// Maker fill stream over Pacifica's `FillDetectionClient`.
pub struct PacificaFillStream {
    client: FillDetectionClient,
}

impl PacificaFillStream {
    pub fn new(client: FillDetectionClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl MakerFillStream for PacificaFillStream {
    fn label(&self) -> &'static str {
        "PACIFICA_FILL"
    }

    fn ready_flag(&self) -> Arc<AtomicBool> {
        self.client.ready_flag()
    }

    fn set_reconcile_hook(&self, hook: MakerReconcileHook) {
        // `MakerReconcileHook` and the connector's `ReconcileHook` are the same
        // type, so this passes straight through.
        self.client.set_reconcile_hook(hook);
    }

    fn baseline_updater(&self) -> Arc<dyn MakerBaselineUpdater> {
        Arc::new(PacificaBaselineUpdater(self.client.get_baseline_updater()))
    }

    async fn run_with(&mut self, mut cb: MakerFillCallback) -> Result<()> {
        self.client
            .start(move |fe| {
                if let Some(mfe) = maker_fill_event_from(fe) {
                    cb(mfe);
                }
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_folds_order_id_and_carries_server_echo() {
        // `i` is used when `order_id` is absent.
        let d = OrderData {
            order_id: None,
            i: Some(42),
            client_order_id: Some("server-echo".to_string()),
            symbol: Some("SOL".to_string()),
            requested_size: None,
            submitted_size: Some(2.5),
            submitted_price: Some(100.25),
        };
        let ack = ack_from_order_data(d);
        assert_eq!(ack.order_id, Some(42));
        assert_eq!(ack.client_order_id.as_deref(), Some("server-echo"));
        assert_eq!(ack.submitted_size, Some(2.5));
        assert_eq!(ack.submitted_price, Some(100.25));

        // `order_id` wins when present.
        let d2 = OrderData {
            order_id: Some(7),
            i: Some(42),
            client_order_id: None,
            symbol: None,
            requested_size: None,
            submitted_size: None,
            submitted_price: None,
        };
        assert_eq!(ack_from_order_data(d2).order_id, Some(7));
    }

    fn open_order(side: &str) -> OpenOrderItem {
        OpenOrderItem {
            order_id: 1,
            client_order_id: "c".to_string(),
            symbol: "SOL".to_string(),
            side: side.to_string(),
            price: "100.5".to_string(),
            initial_amount: "2.0".to_string(),
            filled_amount: "0.5".to_string(),
            cancelled_amount: "0.0".to_string(),
            stop_price: None,
            order_type: "limit".to_string(),
            stop_parent_order_id: None,
            reduce_only: false,
            created_at: 10,
            updated_at: 20,
        }
    }

    #[test]
    fn open_order_maps_side_and_parses() {
        let o = open_order_from(open_order("bid")).unwrap();
        assert_eq!(o.side, OrderSide::Buy);
        assert_eq!(o.price, 100.5);
        assert_eq!(o.initial_amount, 2.0);
        assert_eq!(o.filled_amount, 0.5);

        assert_eq!(open_order_from(open_order("ask")).unwrap().side, OrderSide::Sell);
        // Unknown side is dropped (skipped), never mis-hedged.
        assert!(open_order_from(open_order("weird")).is_none());
    }

    #[test]
    fn signed_base_sign_convention() {
        assert_eq!(signed_base("bid", 3.0), 3.0);
        assert_eq!(signed_base("ask", 3.0), -3.0);
        assert_eq!(signed_base("???", 3.0), 0.0);
    }

    #[test]
    fn symbol_map_identity_is_passthrough() {
        // The default (wire == canonical): every translation is a no-op.
        let m = SymbolMap::new("SOL", "SOL");
        assert_eq!(m.to_wire("SOL"), "SOL");
        assert_eq!(m.to_canonical("SOL"), "SOL");
        // Other symbols pass through untouched in both directions.
        assert_eq!(m.to_wire("BTC"), "BTC");
        assert_eq!(m.to_canonical("BTC"), "BTC");
    }

    #[test]
    fn symbol_map_translates_distinct_wire_symbol() {
        // A maker venue that names the asset "SOL-PERP" while services use "SOL".
        let m = SymbolMap::new("SOL", "SOL-PERP");
        // Outbound: services pass canonical, venue receives wire.
        assert_eq!(m.to_wire("SOL"), "SOL-PERP");
        // Inbound: venue returns wire, services compare against canonical.
        assert_eq!(m.to_canonical("SOL-PERP"), "SOL");
        // Round-trips.
        assert_eq!(m.to_canonical(m.to_wire("SOL")), "SOL");
        // An unrelated symbol (e.g. a stray account order) is never rewritten.
        assert_eq!(m.to_wire("BTC"), "BTC");
        assert_eq!(m.to_canonical("BTC"), "BTC");
    }

    #[test]
    fn trade_maps_maker_flag_and_amounts() {
        let t = TradeHistoryItem {
            history_id: 1,
            order_id: 9,
            client_order_id: Some("c".to_string()),
            symbol: "SOL".to_string(),
            amount: "1.5".to_string(),
            price: "101.0".to_string(),
            entry_price: "100.0".to_string(),
            fee: "0.01".to_string(),
            pnl: "0.0".to_string(),
            event_type: "fulfill_maker".to_string(),
            side: "open_long".to_string(),
            created_at: 5,
            cause: "normal".to_string(),
            value: None,
        };
        let m = trade_from(&t);
        assert_eq!(m.order_id, 9);
        assert_eq!(m.amount, 1.5);
        assert_eq!(m.entry_price, 100.0);
        assert_eq!(m.fee, 0.01);
        assert!(m.is_maker_fill);

        let mut taker = t.clone();
        taker.event_type = "fulfill_taker".to_string();
        assert!(!trade_from(&taker).is_maker_fill);
    }

    #[test]
    fn full_fill_maps_to_normalized_full() {
        let fe = FillEvent::FullFill {
            order_id: 3,
            client_order_id: Some("c".to_string()),
            symbol: "SOL".to_string(),
            side: "bid".to_string(),
            filled_amount: "2.0".to_string(),
            avg_price: "100.0".to_string(),
            timestamp: 7,
        };
        match maker_fill_event_from(fe).unwrap() {
            MakerFillEvent::Full { side, filled, avg_price, order_id, ts, .. } => {
                assert_eq!(side, OrderSide::Buy);
                assert_eq!(filled, 2.0);
                assert_eq!(avg_price, 100.0);
                assert_eq!(order_id, 3);
                assert_eq!(ts, 7);
            }
            other => panic!("expected Full, got {:?}", other),
        }
    }

    #[test]
    fn position_fill_uses_buy_sell_strings() {
        let fe = FillEvent::PositionFill {
            symbol: "SOL".to_string(),
            side: "sell".to_string(),
            filled_amount: "1.0".to_string(),
            avg_price: "100.0".to_string(),
            timestamp: 1,
            position_delta: "-1.0".to_string(),
            prev_position: "0.0".to_string(),
            new_position: "-1.0".to_string(),
            cross_validated: true,
        };
        match maker_fill_event_from(fe).unwrap() {
            MakerFillEvent::Position { side, filled, position_delta, cross_validated, .. } => {
                assert_eq!(side, OrderSide::Sell);
                assert_eq!(filled, 1.0);
                assert_eq!(position_delta, -1.0);
                assert!(cross_validated);
            }
            other => panic!("expected Position, got {:?}", other),
        }
    }

    #[test]
    fn unknown_side_is_skipped() {
        let fe = FillEvent::FullFill {
            order_id: 3,
            client_order_id: None,
            symbol: "SOL".to_string(),
            side: "sideways".to_string(),
            filled_amount: "2.0".to_string(),
            avg_price: "100.0".to_string(),
            timestamp: 7,
        };
        assert!(maker_fill_event_from(fe).is_none());
    }
}
