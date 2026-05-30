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

/// Derive the number of fractional decimals from the parsed step VALUE. Format-
/// independent, so it is correct even when the exchange returns a tick in
/// scientific notation ("1e-4") or as an integer string ("1") — cases where
/// `decimals_from_step_text` would wrongly return 0 and snap a tick-aligned price
/// to an integer (destroying sub-dollar prices).
#[inline]
pub fn decimals_from_step_value(step: f64) -> usize {
    if !step.is_finite() || step <= 0.0 || step >= 1.0 {
        return 0;
    }
    for d in 0..=12usize {
        let f = 10_f64.powi(d as i32);
        if ((step * f).round() / f - step).abs() < step * 1e-9 {
            return d;
        }
    }
    12
}

/// Decimals for a step given both its source string and parsed value: trust the
/// string only when it is plain decimal; otherwise fall back to the value.
#[inline]
pub fn step_decimals(step_text: &str, step: f64) -> usize {
    if step_text.contains('.') && !step_text.contains(['e', 'E']) {
        decimals_from_step_text(step_text)
    } else {
        decimals_from_step_value(step)
    }
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
    let snapped = (value / step).floor() * step;
    let r = round_to_decimals(snapped, decimals);
    // Guard: the final decimal rounding must never move the price more than half a
    // tick away from the tick-aligned value (catches a mismatched `decimals`).
    debug_assert!(
        (r - snapped).abs() <= step * 0.5 + f64::EPSILON,
        "tick re-round drifted: step={step} snapped={snapped} r={r}"
    );
    r
}

#[inline]
pub fn ceil_to_step(value: f64, step: f64, decimals: usize) -> f64 {
    if step <= 0.0 || !value.is_finite() {
        return 0.0;
    }
    let snapped = (value / step).ceil() * step;
    let r = round_to_decimals(snapped, decimals);
    debug_assert!(
        (r - snapped).abs() <= step * 0.5 + f64::EPSILON,
        "tick re-round drifted: step={step} snapped={snapped} r={r}"
    );
    r
}

pub fn pacifica_maker_price_for_is_buy(
    is_buy: bool,
    price: f64,
    tick_text: &str,
) -> anyhow::Result<f64> {
    let tick: f64 = tick_text.parse()?;
    let decimals = step_decimals(tick_text, tick);
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
    let decimals = step_decimals(lot_text, lot);
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

    #[test]
    fn decimals_from_value_handles_non_plain_strings() {
        // Plain decimal string still works.
        assert_eq!(decimals_from_step_value(0.01), 2);
        assert_eq!(decimals_from_step_value(0.0001), 4);
        // Integer-valued and >= 1 steps have zero fractional decimals.
        assert_eq!(decimals_from_step_value(1.0), 0);
        assert_eq!(decimals_from_step_value(10.0), 0);
    }

    #[test]
    fn maker_price_survives_scientific_notation_tick() {
        // Scientific-notation tick would previously yield decimals=0 and snap the
        // price to an integer. step_decimals must derive 4 from the value.
        let p = pacifica_maker_price(OrderSide::Buy, 0.12345, "1e-4").unwrap();
        assert!((p - 0.1234).abs() < 1e-9, "got {p}");
        let p = pacifica_maker_price(OrderSide::Sell, 0.12341, "1e-4").unwrap();
        assert!((p - 0.1235).abs() < 1e-9, "got {p}");
    }
}
