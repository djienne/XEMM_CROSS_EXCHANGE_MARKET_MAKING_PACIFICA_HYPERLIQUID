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
    trading::{PacificaCredentials, PacificaTrading},
};

#[derive(Debug, Deserialize)]
struct Config {
    symbol: String,
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

    info!("Starting balance check for symbol: {}", config.symbol);

    // Initialize clients
    let hl_creds = HyperliquidCredentials::from_env()?;
    let hl_trading = HyperliquidTrading::new(hl_creds, false)?; // Mainnet

    let pac_creds = PacificaCredentials::from_env()?;
    let pac_trading = PacificaTrading::new(pac_creds)?;

    // Initialize orderbook clients for price feeds (needed for value calculation)
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

    info!("Price data received. Checking positions...");

    check_balance(&config, &hl_trading, &pac_trading, &hl_price, &pac_price).await?;

    Ok(())
}

async fn check_balance(
    config: &Config,
    hl_trading: &HyperliquidTrading,
    pac_trading: &PacificaTrading,
    hl_price_store: &Arc<Mutex<Option<f64>>>,
    pac_price_store: &Arc<Mutex<Option<f64>>>,
) -> Result<()> {
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

    // 2. Get Prices
    let hl_mid = (*hl_price_store.lock().unwrap()).context("No HL price")?;
    let pac_mid = (*pac_price_store.lock().unwrap()).context("No Pacifica price")?;
    let avg_price = (hl_mid + pac_mid) / 2.0;

    // 3. Calculate Imbalance
    let net_delta = hl_pos + pac_pos;
    let threshold = 0.005; 

    info!("--------------------------------------------------");
    info!("📊 POSITION REPORT: {}", config.symbol);
    info!("--------------------------------------------------");
    info!("Hyperliquid: {:>10.4} (Value: ${:.2})", hl_pos, hl_pos.abs() * hl_mid);
    info!("Pacifica:    {:>10.4} (Value: ${:.2})", pac_pos, pac_pos.abs() * pac_mid);
    info!("--------------------------------------------------");
    info!("Net Delta:   {:>10.4} (Value: ${:.2})", net_delta, net_delta.abs() * avg_price);
    info!("--------------------------------------------------");

    if net_delta.abs() < threshold {
        info!("✅ STATUS: BALANCED (Delta < {})", threshold);
    } else {
        warn!("⚠️ STATUS: IMBALANCED");
        info!("Action Required: {} {:.4} to reach neutral.", 
            if net_delta > 0.0 { "SELL" } else { "BUY" }, 
            net_delta.abs()
        );
    }

    Ok(())
}
