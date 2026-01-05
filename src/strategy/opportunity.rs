/// Order side (Buy or Sell)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OrderSide {
    Buy = 0,
    Sell = 1,
}

/// Trading opportunity with calculated parameters
#[derive(Debug, Clone)]
pub struct Opportunity {
    /// Direction of the Pacifica order
    pub direction: OrderSide,
    /// Limit price for Pacifica order
    pub pacifica_price: f64,
    /// Expected hedge price on Hyperliquid
    pub hyperliquid_price: f64,
    /// Order size in base currency
    pub size: f64,
    /// Calculated profit in basis points
    pub initial_profit_bps: f64,
    /// Timestamp when opportunity was evaluated (milliseconds)
    pub timestamp: u64,
}

/// Precomputed fee factors to avoid repeated calculations
#[derive(Debug, Clone, Copy)]
struct FeeFactors {
    /// 1.0 + maker_fee
    one_plus_maker: f64,
    /// 1.0 - maker_fee
    one_minus_maker: f64,
    /// 1.0 + taker_fee
    one_plus_taker: f64,
    /// 1.0 - taker_fee
    one_minus_taker: f64,
    /// 1.0 + maker_fee + profit_rate (buy denominator)
    buy_denominator: f64,
    /// 1.0 - maker_fee - profit_rate (sell denominator)
    sell_denominator: f64,
}

/// Opportunity evaluator for XEMM strategy
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields stored for Debug trait and documentation
pub struct OpportunityEvaluator {
    /// Pacifica maker fee (as decimal, e.g., 0.0001 for 1 bps)
    maker_fee: f64,
    /// Hyperliquid taker fee (as decimal, e.g., 0.00025 for 2.5 bps)
    taker_fee: f64,
    /// Target profit rate (as decimal, e.g., 0.001 for 10 bps)
    profit_rate: f64,
    /// Pacifica tick size (minimum price increment)
    pacifica_tick_size: f64,
    /// Inverse tick size for faster division
    inv_tick_size: f64,
    /// Precomputed fee factors
    fee_factors: FeeFactors,
}

impl OpportunityEvaluator {
    /// Create a new opportunity evaluator
    ///
    /// # Arguments
    /// * `maker_fee_bps` - Pacifica maker fee in basis points (e.g., 1.0 = 0.01%)
    /// * `taker_fee_bps` - Hyperliquid taker fee in basis points (e.g., 2.5 = 0.025%)
    /// * `profit_rate_bps` - Target profit in basis points (e.g., 10.0 = 0.1%)
    /// * `pacifica_tick_size` - Minimum price increment on Pacifica
    pub fn new(
        maker_fee_bps: f64,
        taker_fee_bps: f64,
        profit_rate_bps: f64,
        pacifica_tick_size: f64,
    ) -> Self {
        let maker_fee = maker_fee_bps * 0.0001; // Multiply instead of divide
        let taker_fee = taker_fee_bps * 0.0001;
        let profit_rate = profit_rate_bps * 0.0001;

        let fee_factors = FeeFactors {
            one_plus_maker: 1.0 + maker_fee,
            one_minus_maker: 1.0 - maker_fee,
            one_plus_taker: 1.0 + taker_fee,
            one_minus_taker: 1.0 - taker_fee,
            buy_denominator: 1.0 + maker_fee + profit_rate,
            sell_denominator: 1.0 - maker_fee - profit_rate,
        };

        Self {
            maker_fee,
            taker_fee,
            profit_rate,
            pacifica_tick_size,
            inv_tick_size: 1.0 / pacifica_tick_size, // Precompute for faster rounding
            fee_factors,
        }
    }

    /// Evaluate BUY opportunity on Pacifica
    ///
    /// Strategy: BUY on Pacifica → SELL (taker) on Hyperliquid
    ///
    /// # Arguments
    /// * `hl_bid` - Current Hyperliquid best bid
    /// * `notional_usd` - Notional order size in USD
    /// * `timestamp_ms` - Current timestamp in milliseconds (pass from caller to avoid syscall)
    ///
    /// # Returns
    /// Some(Opportunity) if profitable, None otherwise
    #[inline]
    pub fn evaluate_buy_opportunity(
        &self,
        hl_bid: f64,
        notional_usd: f64,
        timestamp_ms: u64,
    ) -> Option<Opportunity> {
        // Calculate ideal limit price using precomputed factors
        // buy_limit_price = (HL_bid * (1 - takerFee)) / (1 + makerFee + profitRate)
        let buy_limit_price = (hl_bid * self.fee_factors.one_minus_taker) / self.fee_factors.buy_denominator;

        // Round DOWN to tick (conservative for buy)
        let buy_limit_rounded = self.round_price_down(buy_limit_price);

        // Calculate order size from notional
        let size = notional_usd / buy_limit_rounded;

        // Calculate actual profit after rounding (in bps)
        let buy_cost = buy_limit_rounded * self.fee_factors.one_plus_maker;
        let buy_revenue = hl_bid * self.fee_factors.one_minus_taker;
        let buy_profit_bps = ((buy_revenue - buy_cost) / buy_cost) * 10000.0;

        // Only return if profitable
        if buy_profit_bps > 0.0 {
            Some(Opportunity {
                direction: OrderSide::Buy,
                pacifica_price: buy_limit_rounded,
                hyperliquid_price: hl_bid,
                size,
                initial_profit_bps: buy_profit_bps,
                timestamp: timestamp_ms,
            })
        } else {
            None
        }
    }

    /// Evaluate SELL opportunity on Pacifica
    ///
    /// Strategy: SELL on Pacifica → BUY (taker) on Hyperliquid
    ///
    /// # Arguments
    /// * `hl_ask` - Current Hyperliquid best ask
    /// * `notional_usd` - Notional order size in USD
    /// * `timestamp_ms` - Current timestamp in milliseconds (pass from caller to avoid syscall)
    ///
    /// # Returns
    /// Some(Opportunity) if profitable, None otherwise
    #[inline]
    pub fn evaluate_sell_opportunity(
        &self,
        hl_ask: f64,
        notional_usd: f64,
        timestamp_ms: u64,
    ) -> Option<Opportunity> {
        // Calculate ideal limit price using precomputed factors
        // sell_limit_price = (HL_ask * (1 + takerFee)) / (1 - makerFee - profitRate)
        let sell_limit_price = (hl_ask * self.fee_factors.one_plus_taker) / self.fee_factors.sell_denominator;

        // Round UP to tick (conservative for sell)
        let sell_limit_rounded = self.round_price_up(sell_limit_price);

        // Calculate order size from notional
        let size = notional_usd / sell_limit_rounded;

        // Calculate actual profit after rounding (in bps)
        let sell_revenue = sell_limit_rounded * self.fee_factors.one_minus_maker;
        let sell_cost = hl_ask * self.fee_factors.one_plus_taker;
        let sell_profit_bps = ((sell_revenue - sell_cost) / sell_cost) * 10000.0;

        // Only return if profitable
        if sell_profit_bps > 0.0 {
            Some(Opportunity {
                direction: OrderSide::Sell,
                pacifica_price: sell_limit_rounded,
                hyperliquid_price: hl_ask,
                size,
                initial_profit_bps: sell_profit_bps,
                timestamp: timestamp_ms,
            })
        } else {
            None
        }
    }

    /// Recalculate profit for an existing opportunity based on current market prices
    ///
    /// Used for monitoring if the opportunity is still profitable
    ///
    /// # Arguments
    /// * `opportunity` - The original opportunity
    /// * `current_hl_bid` - Current Hyperliquid best bid
    /// * `current_hl_ask` - Current Hyperliquid best ask
    ///
    /// # Returns
    /// Current profit in basis points
    #[inline]
    pub fn recalculate_profit(
        &self,
        opportunity: &Opportunity,
        current_hl_bid: f64,
        current_hl_ask: f64,
    ) -> f64 {
        self.recalculate_profit_raw(
            opportunity.direction,
            opportunity.pacifica_price,
            current_hl_bid,
            current_hl_ask,
        )
    }

    /// Recalculate profit without requiring an Opportunity struct
    ///
    /// Avoids temporary allocations in hot paths
    ///
    /// # Arguments
    /// * `direction` - Order direction (Buy or Sell)
    /// * `pacifica_price` - The Pacifica limit price
    /// * `current_hl_bid` - Current Hyperliquid best bid
    /// * `current_hl_ask` - Current Hyperliquid best ask
    ///
    /// # Returns
    /// Current profit in basis points
    #[inline(always)]
    pub fn recalculate_profit_raw(
        &self,
        direction: OrderSide,
        pacifica_price: f64,
        current_hl_bid: f64,
        current_hl_ask: f64,
    ) -> f64 {
        match direction {
            OrderSide::Buy => {
                // BUY on Pacifica (at pacifica_price) → SELL on Hyperliquid (at current_hl_bid)
                let buy_cost = pacifica_price * self.fee_factors.one_plus_maker;
                let buy_revenue = current_hl_bid * self.fee_factors.one_minus_taker;
                ((buy_revenue - buy_cost) / buy_cost) * 10000.0
            }
            OrderSide::Sell => {
                // SELL on Pacifica (at pacifica_price) → BUY on Hyperliquid (at current_hl_ask)
                let sell_revenue = pacifica_price * self.fee_factors.one_minus_maker;
                let sell_cost = current_hl_ask * self.fee_factors.one_plus_taker;
                ((sell_revenue - sell_cost) / sell_cost) * 10000.0
            }
        }
    }

    /// Pick the best opportunity from two options
    ///
    /// Chooses the one closer to mid price, or with higher profit if equidistant
    ///
    /// # Arguments
    /// * `buy_opp` - Buy opportunity (if any)
    /// * `sell_opp` - Sell opportunity (if any)
    /// * `pac_mid` - Pacifica mid price
    ///
    /// # Returns
    /// The best opportunity, or None if both are None
    #[inline]
    pub fn pick_best_opportunity(
        buy_opp: Option<Opportunity>,
        sell_opp: Option<Opportunity>,
        pac_mid: f64,
    ) -> Option<Opportunity> {
        match (buy_opp, sell_opp) {
            (Some(buy), Some(sell)) => {
                // Calculate distance from mid price
                let buy_distance = (pac_mid - buy.pacifica_price).abs();
                let sell_distance = (sell.pacifica_price - pac_mid).abs();

                // Choose the one closer to mid price
                if buy_distance < sell_distance {
                    Some(buy)
                } else if sell_distance < buy_distance {
                    Some(sell)
                } else {
                    // If equidistant, choose the one with higher profit
                    if buy.initial_profit_bps > sell.initial_profit_bps {
                        Some(buy)
                    } else {
                        Some(sell)
                    }
                }
            }
            (Some(buy), None) => Some(buy),
            (None, Some(sell)) => Some(sell),
            (None, None) => None,
        }
    }

    /// Round price down to nearest tick size (for BUY orders)
    #[inline(always)]
    fn round_price_down(&self, price: f64) -> f64 {
        (price * self.inv_tick_size).floor() * self.pacifica_tick_size
    }

    /// Round price up to nearest tick size (for SELL orders)
    #[inline(always)]
    fn round_price_up(&self, price: f64) -> f64 {
        (price * self.inv_tick_size).ceil() * self.pacifica_tick_size
    }
}

impl OrderSide {
    /// Convert to string representation
    #[inline(always)]
    pub const fn as_str(&self) -> &'static str {
        match self {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
    }

    /// Convert to API string representation ("bid" or "ask")
    #[inline(always)]
    pub const fn as_api_str(&self) -> &'static str {
        match self {
            OrderSide::Buy => "bid",
            OrderSide::Sell => "ask",
        }
    }

    /// Get opposite side (for hedging)
    #[inline(always)]
    pub const fn opposite(&self) -> OrderSide {
        match self {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fee_factors_precomputation() {
        let evaluator = OpportunityEvaluator::new(1.0, 2.5, 10.0, 0.01);
        
        // Verify precomputed factors
        assert!((evaluator.fee_factors.one_plus_maker - 1.0001).abs() < 1e-10);
        assert!((evaluator.fee_factors.one_minus_taker - 0.99975).abs() < 1e-10);
    }

    #[test]
    fn test_recalculate_profit_raw_matches_struct_version() {
        let evaluator = OpportunityEvaluator::new(1.0, 2.5, 10.0, 0.01);
        
        let opp = Opportunity {
            direction: OrderSide::Buy,
            pacifica_price: 100.0,
            hyperliquid_price: 100.5,
            size: 1.0,
            initial_profit_bps: 5.0,
            timestamp: 0,
        };
        
        let hl_bid = 100.3;
        let hl_ask = 100.4;
        
        let profit_struct = evaluator.recalculate_profit(&opp, hl_bid, hl_ask);
        let profit_raw = evaluator.recalculate_profit_raw(
            opp.direction,
            opp.pacifica_price,
            hl_bid,
            hl_ask,
        );
        
        assert!((profit_struct - profit_raw).abs() < 1e-10);
    }
}
