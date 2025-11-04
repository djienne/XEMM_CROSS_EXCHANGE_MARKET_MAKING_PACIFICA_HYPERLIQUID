
/// Order side (Buy or Sell)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
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

/// Opportunity evaluator for XEMM strategy
#[derive(Debug, Clone)]
pub struct OpportunityEvaluator {
    /// Pacifica maker fee (as decimal, e.g., 0.0001 for 1 bps)
    maker_fee: f64,
    /// Hyperliquid taker fee (as decimal, e.g., 0.00025 for 2.5 bps)
    taker_fee: f64,
    /// Target profit rate (as decimal, e.g., 0.001 for 10 bps)
    profit_rate: f64,
    /// Pacifica tick size (minimum price increment)
    pacifica_tick_size: f64,
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
        Self {
            maker_fee: maker_fee_bps / 10000.0,
            taker_fee: taker_fee_bps / 10000.0,
            profit_rate: profit_rate_bps / 10000.0,
            pacifica_tick_size,
        }
    }

    /// Evaluate BUY opportunity on Pacifica
    ///
    /// Strategy: BUY on Pacifica → SELL (taker) on Hyperliquid
    ///
    /// # Arguments
    /// * `hl_bid` - Current Hyperliquid best bid
    /// * `notional_usd` - Notional order size in USD
    ///
    /// # Returns
    /// Some(Opportunity) if profitable, None otherwise
    pub fn evaluate_buy_opportunity(
        &self,
        hl_bid: f64,
        notional_usd: f64,
    ) -> Option<Opportunity> {
        // Calculate ideal limit price
        // buy_limit_price = (HL_bid * (1 - takerFee)) / (1 + makerFee + profitRate)
        let buy_limit_price = (hl_bid * (1.0 - self.taker_fee)) / (1.0 + self.maker_fee + self.profit_rate);

        // Round DOWN to tick (conservative for buy)
        let buy_limit_rounded = self.round_price_down(buy_limit_price);

        // Calculate order size from notional
        let size = notional_usd / buy_limit_rounded;

        // Calculate actual profit after rounding (in bps)
        let buy_cost = buy_limit_rounded * (1.0 + self.maker_fee);
        let buy_revenue = hl_bid * (1.0 - self.taker_fee);
        let buy_profit_rate = (buy_revenue - buy_cost) / buy_cost;
        let buy_profit_bps = buy_profit_rate * 10000.0;

        // Only return if profitable
        if buy_profit_bps > 0.0 {
            Some(Opportunity {
                direction: OrderSide::Buy,
                pacifica_price: buy_limit_rounded,
                hyperliquid_price: hl_bid,
                size,
                initial_profit_bps: buy_profit_bps,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
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
    ///
    /// # Returns
    /// Some(Opportunity) if profitable, None otherwise
    pub fn evaluate_sell_opportunity(
        &self,
        hl_ask: f64,
        notional_usd: f64,
    ) -> Option<Opportunity> {
        // Calculate ideal limit price
        // sell_limit_price = (HL_ask * (1 + takerFee)) / (1 - makerFee - profitRate)
        let sell_limit_price = (hl_ask * (1.0 + self.taker_fee)) / (1.0 - self.maker_fee - self.profit_rate);

        // Round UP to tick (conservative for sell)
        let sell_limit_rounded = self.round_price_up(sell_limit_price);

        // Calculate order size from notional
        let size = notional_usd / sell_limit_rounded;

        // Calculate actual profit after rounding (in bps)
        let sell_revenue = sell_limit_rounded * (1.0 - self.maker_fee);
        let sell_cost = hl_ask * (1.0 + self.taker_fee);
        let sell_profit_rate = (sell_revenue - sell_cost) / sell_cost;
        let sell_profit_bps = sell_profit_rate * 10000.0;

        // Only return if profitable
        if sell_profit_bps > 0.0 {
            Some(Opportunity {
                direction: OrderSide::Sell,
                pacifica_price: sell_limit_rounded,
                hyperliquid_price: hl_ask,
                size,
                initial_profit_bps: sell_profit_bps,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
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
    pub fn recalculate_profit(
        &self,
        opportunity: &Opportunity,
        current_hl_bid: f64,
        current_hl_ask: f64,
    ) -> f64 {
        match opportunity.direction {
            OrderSide::Buy => {
                // BUY on Pacifica (at opportunity.pacifica_price) → SELL on Hyperliquid (at current_hl_bid)
                let buy_cost = opportunity.pacifica_price * (1.0 + self.maker_fee);
                let buy_revenue = current_hl_bid * (1.0 - self.taker_fee);
                let buy_profit_rate = (buy_revenue - buy_cost) / buy_cost;
                buy_profit_rate * 10000.0
            }
            OrderSide::Sell => {
                // SELL on Pacifica (at opportunity.pacifica_price) → BUY on Hyperliquid (at current_hl_ask)
                let sell_revenue = opportunity.pacifica_price * (1.0 - self.maker_fee);
                let sell_cost = current_hl_ask * (1.0 + self.taker_fee);
                let sell_profit_rate = (sell_revenue - sell_cost) / sell_cost;
                sell_profit_rate * 10000.0
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
    fn round_price_down(&self, price: f64) -> f64 {
        (price / self.pacifica_tick_size).floor() * self.pacifica_tick_size
    }

    /// Round price up to nearest tick size (for SELL orders)
    fn round_price_up(&self, price: f64) -> f64 {
        (price / self.pacifica_tick_size).ceil() * self.pacifica_tick_size
    }
}

impl OrderSide {
    /// Convert to string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
    }

    /// Get opposite side (for hedging)
    pub fn opposite(&self) -> OrderSide {
        match self {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        }
    }
}
