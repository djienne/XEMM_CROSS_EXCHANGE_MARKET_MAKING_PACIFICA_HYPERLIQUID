use crate::strategy::OrderSide;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SymbolRules {
    pub price_tick: f64,
    pub size_step: f64,
    pub min_size: f64,
    pub min_notional_usd: f64,
}

impl SymbolRules {
    pub const fn new(
        price_tick: f64,
        size_step: f64,
        min_size: f64,
        min_notional_usd: f64,
    ) -> Self {
        Self {
            price_tick,
            size_step,
            min_size,
            min_notional_usd,
        }
    }
}

pub fn fallback_rules(symbol: &str) -> SymbolRules {
    match symbol {
        "BTC" => SymbolRules::new(0.1, 0.00001, 0.00001, 5.0),
        "ETH" => SymbolRules::new(0.01, 0.0001, 0.0001, 5.0),
        "SOL" => SymbolRules::new(0.001, 0.01, 0.01, 5.0),
        "ENA" => SymbolRules::new(0.0001, 1.0, 1.0, 5.0),
        "PUMP" => SymbolRules::new(0.0000001, 1.0, 1.0, 5.0),
        _ => SymbolRules::new(0.0001, 0.0001, 0.0001, 5.0),
    }
}

#[inline]
pub fn decimals_from_step_text(step: &str) -> usize {
    let trimmed = step.trim_end_matches('0');
    trimmed
        .split_once('.')
        .map(|(_, frac)| frac.len())
        .unwrap_or(0)
}

#[inline]
fn decimal_factor(decimals: usize) -> f64 {
    10_f64.powi(decimals.min(12) as i32)
}

#[inline]
pub fn round_to_decimals(value: f64, decimals: usize) -> f64 {
    let factor = decimal_factor(decimals);
    (value * factor).round() / factor
}

#[inline]
pub fn floor_to_step(value: f64, step: f64, decimals: usize) -> f64 {
    if step <= 0.0 || !value.is_finite() {
        return 0.0;
    }
    round_to_decimals((value / step).floor() * step, decimals)
}

#[inline]
pub fn ceil_to_step(value: f64, step: f64, decimals: usize) -> f64 {
    if step <= 0.0 || !value.is_finite() {
        return 0.0;
    }
    round_to_decimals((value / step).ceil() * step, decimals)
}

pub fn pacifica_maker_price_for_is_buy(
    is_buy: bool,
    price: f64,
    tick_text: &str,
) -> anyhow::Result<f64> {
    let tick: f64 = tick_text.parse()?;
    let decimals = decimals_from_step_text(tick_text);
    Ok(if is_buy {
        floor_to_step(price, tick, decimals)
    } else {
        ceil_to_step(price, tick, decimals)
    })
}

pub fn pacifica_maker_price(side: OrderSide, price: f64, tick_text: &str) -> anyhow::Result<f64> {
    pacifica_maker_price_for_is_buy(matches!(side, OrderSide::Buy), price, tick_text)
}

pub fn pacifica_size_floor(size: f64, lot_text: &str) -> anyhow::Result<f64> {
    let lot: f64 = lot_text.parse()?;
    let decimals = decimals_from_step_text(lot_text);
    Ok(floor_to_step(size, lot, decimals))
}

pub fn hyperliquid_size_floor(size: f64, sz_decimals: i32) -> f64 {
    let decimals = sz_decimals.max(0) as usize;
    let step = 10_f64.powi(-(decimals as i32));
    floor_to_step(size, step, decimals)
}

pub fn is_dust_or_below_min(size: f64, price: f64, rules: SymbolRules, dust: f64) -> bool {
    size.abs() <= dust
        || size.abs() < rules.min_size
        || (price > 0.0 && size.abs() * price < rules.min_notional_usd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pacifica_price_rounding_is_side_aware() {
        assert_eq!(
            pacifica_maker_price(OrderSide::Buy, 100.019, "0.01").unwrap(),
            100.01
        );
        assert_eq!(
            pacifica_maker_price(OrderSide::Sell, 100.011, "0.01").unwrap(),
            100.02
        );
    }

    #[test]
    fn hyperliquid_size_is_floored() {
        assert_eq!(hyperliquid_size_floor(1.239, 2), 1.23);
        assert_eq!(hyperliquid_size_floor(0.00019, 4), 0.0001);
    }
}
