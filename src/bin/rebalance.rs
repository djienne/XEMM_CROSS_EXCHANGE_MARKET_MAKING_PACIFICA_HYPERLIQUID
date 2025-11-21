use anyhow::{Context, Result};
use dotenv::dotenv;
use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use xemm_rust::connector::hyperliquid::{
    client::{OrderbookClient as HlOrderbookClient, OrderbookConfig as HlOrderbookConfig},
    trading::{HyperliquidCredentials, HyperliquidTrading},
};
use xemm_rust::connector::pacifica::{
    OrderbookClient as PacOrderbookClient, OrderbookConfig as PacOrderbookConfig,
    trading::{PacificaCredentials, PacificaTrading, OrderSide},
};

#[derive(Debug, Deserialize)]
struct Config {
    symbol: String,
    hyperliquid_slippage: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // Load .env
    dotenv().ok();

    // Load config
    let mut file = File::open("config.json").context("Failed to open config.json")?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let config: Config = serde_json::from_str(&contents)?;

    info!("Starting rebalancer for symbol: {}", config.symbol);

    // Initialize clients
    let hl_creds = HyperliquidCredentials::from_env()?;
    let hl_trading = HyperliquidTrading::new(hl_creds, false)?; // Mainnet

    let pac_creds = PacificaCredentials::from_env()?;
    let pac_trading = PacificaTrading::new(pac_creds)?;

    // Initialize orderbook clients for price feeds
    // Use std::sync::Mutex for synchronous access in callbacks
    let hl_price = Arc::new(Mutex::new(None));
    let pac_price = Arc::new(Mutex::new(None));

    let hl_price_clone = hl_price.clone();
    let mut hl_ob_client = HlOrderbookClient::new(HlOrderbookConfig {
        coin: config.symbol.clone(),
        ..Default::default()
    })?;

    let pac_price_clone = pac_price.clone();
    let mut pac_ob_client = PacOrderbookClient::new(PacOrderbookConfig {
        symbol: config.symbol.clone(),
        ..Default::default()
    })?;

    // Spawn orderbook tasks
    tokio::spawn(async move {
        if let Err(e) = hl_ob_client.start(move |bid, ask, _, _| {
            let bid_f: f64 = bid.parse().unwrap_or(0.0);
            let ask_f: f64 = ask.parse().unwrap_or(0.0);
            if bid_f > 0.0 && ask_f > 0.0 {
                let mid = (bid_f + ask_f) / 2.0;
                if let Ok(mut lock) = hl_price_clone.lock() {
                    *lock = Some(mid);
                }
            }
        }).await {
            error!("HL Orderbook error: {}", e);
        }
    });

    tokio::spawn(async move {
        if let Err(e) = pac_ob_client.start(move |bid, ask, _, _| {
            let bid_f: f64 = bid.parse().unwrap_or(0.0);
            let ask_f: f64 = ask.parse().unwrap_or(0.0);
            if bid_f > 0.0 && ask_f > 0.0 {
                let mid = (bid_f + ask_f) / 2.0;
                if let Ok(mut lock) = pac_price_clone.lock() {
                    *lock = Some(mid);
                }
            }
        }).await {
            error!("Pacifica Orderbook error: {}", e);
        }
    });

    // Wait for prices
    info!("Waiting for price data...");
    loop {
        let hl_p = *hl_price.lock().unwrap();
        let pac_p = *pac_price.lock().unwrap();
        if hl_p.is_some() && pac_p.is_some() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    info!("Price data received. Starting rebalance sequence.");

    let max_retries = 5;
    for i in 1..=max_retries {
        info!("--- Rebalance Attempt {}/{} ---", i, max_retries);
        match rebalance_step(&config, &hl_trading, &pac_trading, &hl_price, &pac_price).await {
            Ok(is_balanced) => {
                if is_balanced {
                    info!("✅ System is balanced. Exiting successfully.");
                    return Ok(());
                } else {
                    info!("Action taken. Waiting 6s for state propagation before verification...");
                    sleep(Duration::from_secs(6)).await;
                }
            }
            Err(e) => {
                error!("Error during rebalance step: {}", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }

    warn!("⚠️ Max retries reached. System may still be imbalanced. Please check manually.");
    Ok(())
}

async fn rebalance_step(
    config: &Config,
    hl_trading: &HyperliquidTrading,
    pac_trading: &PacificaTrading,
    hl_price_store: &Arc<Mutex<Option<f64>>>,
    pac_price_store: &Arc<Mutex<Option<f64>>>,
) -> Result<bool> {
    // 1. Fetch Positions
    let wallet_address = std::env::var("HL_WALLET").unwrap_or_else(|_| hl_trading.get_wallet_address());
    let hl_state = hl_trading.get_user_state(&wallet_address).await?;
    let pac_positions = pac_trading.get_positions().await?;

    // Find target positions
    let hl_pos = hl_state.asset_positions.iter()
        .find(|p| p.position.coin == config.symbol)
        .map(|p| p.position.szi.parse::<f64>().unwrap_or(0.0))
        .unwrap_or(0.0);

    let pac_pos = pac_positions.iter()
        .find(|p| p.symbol == config.symbol)
        .map(|p| {
            let amt = p.amount.parse::<f64>().unwrap_or(0.0);
            if p.side == "bid" { amt } else { -amt }
        })
        .unwrap_or(0.0);

    info!("Positions - HL: {:.4}, Pacifica: {:.4}", hl_pos, pac_pos);

    // 2. Calculate Imbalance
    let net_delta = hl_pos + pac_pos;
    let threshold = 0.005; // Ignore dust, but catch 0.01 imbalances

    if net_delta.abs() < threshold {
        info!("Net delta {:.4} is negligible. No action needed.", net_delta);
        return Ok(true);
    }

    // 3. Get Prices
    let hl_mid = (*hl_price_store.lock().unwrap()).context("No HL price")?;
    let pac_mid = (*pac_price_store.lock().unwrap()).context("No Pacifica price")?;

    // Safety check: Price deviation
    let price_diff_pct = (hl_mid - pac_mid).abs() / pac_mid;
    if price_diff_pct > 0.05 {
        warn!("Price deviation too high ({:.2}%). Aborting rebalance.", price_diff_pct * 100.0);
        return Ok(false); // Not balanced, but aborted
    }

    let avg_price = (hl_mid + pac_mid) / 2.0;
    
    info!("Imbalance detected: Net Delta = {:.4}. Rebalancing needed.", net_delta);

    // 4. Determine Action Plan
    // We need to change Net Delta by -net_delta (to get to 0).
    // Let target_change = -net_delta.
    
    let target_change = -net_delta;
    let min_hl_value = 12.0; // Buffer for $10 limit
    let min_hl_size = min_hl_value / avg_price;

    let mut hl_trade: Option<(f64, bool)> = None; // (size, is_buy)
    let mut pac_trade: Option<(f64, bool)> = None; // (size, is_buy)

    // Helper to check if a trade is reduce-only
    let is_reduce_hl = |change: f64| -> bool {
        if hl_pos > 0.0 { change < 0.0 } else { change > 0.0 }
    };
    let is_reduce_pac = |change: f64| -> bool {
        if pac_pos > 0.0 { change < 0.0 } else { change > 0.0 }
    };

    // Strategy 1: Single Leg on HL
    // Requirement: |target_change| > min_hl_size AND is_reduce_hl(target_change)
    if target_change.abs() > min_hl_size && is_reduce_hl(target_change) {
        info!("Strategy: Single Leg HL");
        hl_trade = Some((target_change.abs(), target_change > 0.0));
    }
    // Strategy 2: Single Leg on Pac
    // Requirement: is_reduce_pac(target_change) (Strict reduce-only for Pac as requested)
    else if is_reduce_pac(target_change) {
        info!("Strategy: Single Leg Pacifica");
        pac_trade = Some((target_change.abs(), target_change > 0.0));
    }
    // Strategy 3: Dual Leg (Unwind/Rebalance)
    // We want Change_HL + Change_Pac = target_change.
    // And we want both to be reduce-only.
    // And |Change_HL| > min_hl_size.
    else {
        // Try to construct a dual leg trade
        // Direction of HL trade to be reduce-only:
        let hl_reduce_dir = if hl_pos > 0.0 { -1.0 } else { 1.0 }; // -1 for Sell, 1 for Buy
        
        // Min size for HL
        let x_size = min_hl_size.max(target_change.abs() * 2.0); // Start with min size or double target
        let x = x_size * hl_reduce_dir;
        
        // Calculate necessary Y on Pac
        let y = target_change - x;
        
        // Check if Y is reduce-only on Pac
        if is_reduce_pac(y) {
             // Check if we have enough position to reduce
             let hl_enough = hl_pos.abs() >= x.abs();
             let pac_enough = pac_pos.abs() >= y.abs();
             
             if hl_enough && pac_enough {
                 info!("Strategy: Dual Leg Rebalance (HL: {:.4}, Pac: {:.4})", x, y);
                 hl_trade = Some((x.abs(), x > 0.0));
                 pac_trade = Some((y.abs(), y > 0.0));
             } else {
                 warn!("Dual leg strategy found but insufficient positions (HL: {}, Pac: {}).", hl_enough, pac_enough);
             }
        } else {
            warn!("Could not find valid dual-leg rebalance strategy.");
        }
    }

    // 6. Execute Trades
    // We need to handle the dependency: In Dual Leg, Pac depends on HL.
    // We can track if we are in Dual Leg mode by checking if both are Some.
    let is_dual_leg = hl_trade.is_some() && pac_trade.is_some();

    let mut hl_success = false;

    if let Some((size, is_buy)) = hl_trade {
        info!("Executing HL: {} {:.4}", if is_buy { "BUY" } else { "SELL" }, size);
        let bid = Some(hl_mid); 
        let ask = Some(hl_mid);
        
        // Cap size to position if reduce_only to avoid rejection
        // Although we checked `hl_enough`, slight precision diffs could cause rejection.
        // HL API `reduce_only` might reject if size > position.
        // Let's use the minimum of size and current position abs if we are reducing.
        // (We know we are reducing because of our strategy logic).
        let safe_size = if hl_pos.abs() < size { hl_pos.abs() } else { size };
        
        let res = hl_trading.place_market_order(
            &config.symbol,
            is_buy,
            safe_size,
            config.hyperliquid_slippage,
            true, // reduce_only
            bid,
            ask
        ).await;
        
        match res {
            Ok(r) => {
                info!("HL Order sent: {:?}", r);
                // Check if response indicates success (order_id present)
                // The struct has `response: OrderResponseContent`.
                // We need to check if it's Success.
                // `place_market_order` returns `OrderResponse`.
                // We should inspect it.
                // Assuming `place_market_order` throws error if API returns error (it does bail on "error" status).
                // But let's be sure.
                hl_success = true;
            },
            Err(e) => {
                error!("HL Order failed: {}", e);
                hl_success = false;
            }
        }
    } else {
        // No HL trade, so "HL success" is irrelevant or implicitly true for the purpose of "proceeding" 
        // if we were independent. But here we only care if is_dual_leg.
        hl_success = true; 
    }

    if let Some((size, is_buy)) = pac_trade {
        // If Dual Leg, only proceed if HL succeeded
        if is_dual_leg && !hl_success {
            warn!("Skipping Pacifica leg because Hyperliquid leg failed in Dual Leg strategy.");
        } else {
            info!("Executing Pac: {} {:.4}", if is_buy { "BUY" } else { "SELL" }, size);
            
            // Cap size for Pacifica reduce_only as well
            let safe_size = if pac_pos.abs() < size { pac_pos.abs() } else { size };
            
            let res = pac_trading.place_market_order(
                &config.symbol,
                if is_buy { OrderSide::Buy } else { OrderSide::Sell },
                safe_size,
                config.hyperliquid_slippage,
                true, // reduce_only - enforced
            ).await;
            match res {
                Ok(r) => info!("Pac Order sent: {:?}", r),
                Err(e) => error!("Pac Order failed: {}", e),
            }
        }
    }

    Ok(false) // Action taken (or attempted), so not "already balanced"
}
