use async_trait::async_trait;
use anyhow::Result;
use crate::connector::pacifica::trading::{OrderData, OrderSide};

#[async_trait]
pub trait PacificaTradingClient: Send + Sync {
    async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        size: f64,
        price: Option<f64>,
        mid_price_offset_pct: f64,
        current_bid: Option<f64>,
        current_ask: Option<f64>,
    ) -> Result<OrderData>;
}
