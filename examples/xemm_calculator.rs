use xemm_rust::connector::pacifica::{OrderbookClient as PacificaClient, OrderbookConfig as PacificaConfig};
use xemm_rust::connector::hyperliquid::{OrderbookClient as HyperliquidClient, OrderbookConfig as HyperliquidConfig};
use xemm_rust::Config;
use std::sync::{Arc, Mutex};
use tracing::info;
use tokio::time::{sleep, Duration};

/// Cross-Exchange Market Making (XEMM) Price Calculator
/// Continuously calculates optimal limit prices for arbitrage between Pacifica and Hyperliquid
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    // Load configuration
    let config = Config::load_default()?;
    config.validate()?;

    info!("═══════════════════════════════════════════════════");
    info!("  XEMM Price Calculator");
    info!("═══════════════════════════════════════════════════");
    info!("Symbol: {}", config.symbol);
    info!("Pacifica Maker Fee: {} bps", config.pacifica_maker_fee_bps);
    info!("Hyperliquid Taker Fee: {} bps", config.hyperliquid_taker_fee_bps);
    info!("Target Profit: {} bps", config.profit_rate_bps);
    info!("═══════════════════════════════════════════════════");
    info!("");

    // Convert fees from bps to decimal
    let maker_fee = config.pacifica_maker_fee_bps / 10000.0;
    let taker_fee = config.hyperliquid_taker_fee_bps / 10000.0;
    let profit_rate = config.profit_rate_bps / 10000.0;

    // Shared state for orderbook prices
    let pacifica_prices = Arc::new(Mutex::new((0.0, 0.0))); // (bid, ask)
    let hyperliquid_prices = Arc::new(Mutex::new((0.0, 0.0))); // (bid, ask)

    let pac_prices_clone = pacifica_prices.clone();
    let hl_prices_clone = hyperliquid_prices.clone();

    // Start Pacifica orderbook client
    let pacifica_config = PacificaConfig {
        symbol: config.symbol.clone(),
        agg_level: config.agg_level,
        reconnect_attempts: config.reconnect_attempts,
        ping_interval_secs: config.ping_interval_secs,
    };

    let mut pacifica_client = PacificaClient::new(pacifica_config)?;

    tokio::spawn(async move {
        pacifica_client.start(move |bid, ask, _symbol, _timestamp| {
            let bid_price: f64 = bid.parse().unwrap_or(0.0);
            let ask_price: f64 = ask.parse().unwrap_or(0.0);

            let mut p = pac_prices_clone.lock().unwrap();
            *p = (bid_price, ask_price);
        }).await.ok();
    });

    // Start Hyperliquid orderbook client
    let hyperliquid_config = HyperliquidConfig {
        coin: config.symbol.clone(),
        reconnect_attempts: config.reconnect_attempts,
        ping_interval_secs: config.ping_interval_secs,
        request_interval_ms: 50,  // Request every 50ms (20 Hz)
    };

    let mut hyperliquid_client = HyperliquidClient::new(hyperliquid_config)?;

    tokio::spawn(async move {
        hyperliquid_client.start(move |bid, ask, _coin, _timestamp| {
            let bid_price: f64 = bid.parse().unwrap_or(0.0);
            let ask_price: f64 = ask.parse().unwrap_or(0.0);

            let mut p = hl_prices_clone.lock().unwrap();
            *p = (bid_price, ask_price);
        }).await.ok();
    });

    // Wait for initial data
    info!("[XEMM] Waiting for orderbook data...");
    sleep(Duration::from_secs(3)).await;

    info!("[XEMM] Starting continuous calculation...");
    info!("Update frequency: ~100 Hz (every 10ms)");
    info!("Display: Updates shown when opportunity changes or every 100 iterations");
    info!("");

    // Track last state for change detection
    let mut last_direction = String::new();
    let mut last_price = 0.0;
    let mut iteration_count = 0u64;

    // Main calculation loop - runs every 10ms
    loop {
        sleep(Duration::from_millis(10)).await;
        iteration_count += 1;

        // Get current prices
        let (pac_bid, pac_ask) = *pacifica_prices.lock().unwrap();
        let (hl_bid, hl_ask) = *hyperliquid_prices.lock().unwrap();

        // Validate prices
        if pac_bid == 0.0 || pac_ask == 0.0 || hl_bid == 0.0 || hl_ask == 0.0 {
            continue;
        }

        let pac_mid = (pac_bid + pac_ask) / 2.0;

        // Calculate BUY limit price on Pacifica
        // BUY on Pacifica → SELL (taker) on Hyperliquid
        // buyLimitPrice = (HL_bid * (1 - takerFee)) / (1 + makerFee + profitRate)
        let buy_limit_price = (hl_bid * (1.0 - taker_fee)) / (1.0 + maker_fee + profit_rate);
        let buy_limit_rounded = round_price_down(buy_limit_price, 0.01); // Assuming 0.01 tick size

        // Calculate SELL limit price on Pacifica
        // SELL on Pacifica → BUY (taker) on Hyperliquid
        // sellLimitPrice = (HL_ask * (1 + takerFee)) / (1 - makerFee - profitRate)
        let sell_limit_price = (hl_ask * (1.0 + taker_fee)) / (1.0 - maker_fee - profit_rate);
        let sell_limit_rounded = round_price_up(sell_limit_price, 0.01); // Assuming 0.01 tick size

        // Calculate actual profitability after rounding (in bps)
        // BUY on Pacifica → SELL on Hyperliquid
        let buy_cost = buy_limit_rounded * (1.0 + maker_fee); // Cost to buy on Pacifica
        let buy_revenue = hl_bid * (1.0 - taker_fee); // Revenue from selling on Hyperliquid
        let buy_profit_rate = (buy_revenue - buy_cost) / buy_cost;
        let buy_profit_bps = buy_profit_rate * 10000.0;

        // SELL on Pacifica → BUY on Hyperliquid
        let sell_revenue = sell_limit_rounded * (1.0 - maker_fee); // Revenue from selling on Pacifica
        let sell_cost = hl_ask * (1.0 + taker_fee); // Cost to buy on Hyperliquid
        let sell_profit_rate = (sell_revenue - sell_cost) / sell_cost;
        let sell_profit_bps = sell_profit_rate * 10000.0;

        // Determine which is closer to mid price
        let buy_distance = (pac_mid - buy_limit_rounded).abs();
        let sell_distance = (sell_limit_rounded - pac_mid).abs();

        let (selected_direction, selected_price, selected_profit_bps) = if buy_distance < sell_distance {
            ("BUY", buy_limit_rounded, buy_profit_bps)
        } else {
            ("SELL", sell_limit_rounded, sell_profit_bps)
        };

        // Calculate spreads
        let pac_spread = pac_ask - pac_bid;
        let hl_spread = hl_ask - hl_bid;

        // Only display if something meaningful changed or every 100 iterations (~1 second)
        let direction_changed = selected_direction != last_direction;
        let price_changed = (selected_price - last_price).abs() > 0.01;
        let should_display = direction_changed || price_changed || iteration_count % 100 == 0;

        if should_display {
            info!("═══════════════════════════════════════════════════");
            info!("PACIFICA   | Bid: ${:.2} | Ask: ${:.2} | Mid: ${:.2} | Spread: ${:.3}",
                  pac_bid, pac_ask, pac_mid, pac_spread);
            info!("HYPERLIQUID| Bid: ${:.2} | Ask: ${:.2} | Mid: ${:.2} | Spread: ${:.3}",
                  hl_bid, hl_ask, (hl_bid + hl_ask) / 2.0, hl_spread);
            info!("───────────────────────────────────────────────────");
            info!("BUY  Limit: ${:.2} | Distance: ${:.3} | Profit: {:.2} bps",
                  buy_limit_rounded, buy_distance, buy_profit_bps);
            info!("SELL Limit: ${:.2} | Distance: ${:.3} | Profit: {:.2} bps",
                  sell_limit_rounded, sell_distance, sell_profit_bps);
            info!("───────────────────────────────────────────────────");
            info!(">>> SELECTED: {} @ ${:.2} | Profit: {:.2} bps | Iter: {} <<<",
                  selected_direction, selected_price, selected_profit_bps, iteration_count);
            info!("═══════════════════════════════════════════════════");
            info!("");
        }

        // Update last state
        last_direction = selected_direction.to_string();
        last_price = selected_price;
    }
}

/// Round price down to nearest tick size (for BUY orders)
fn round_price_down(price: f64, tick_size: f64) -> f64 {
    (price / tick_size).floor() * tick_size
}

/// Round price up to nearest tick size (for SELL orders)
fn round_price_up(price: f64, tick_size: f64) -> f64 {
    (price / tick_size).ceil() * tick_size
}
