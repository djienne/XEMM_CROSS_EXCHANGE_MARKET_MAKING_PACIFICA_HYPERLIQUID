use anyhow::Result;
use xemm_rust::connector::hyperliquid::{HyperliquidCredentials, HyperliquidTrading};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("Closing ENA short position...");
    dotenv::dotenv().ok();
    let credentials = HyperliquidCredentials::from_env()?;

    let mut trading = HyperliquidTrading::new(credentials, false)?;

    // Close short position: BUY 50 ENA with 10% slippage
    // NOTE: reduce_only should NEVER be used on Hyperliquid
    let result = trading
        .place_market_order(
            "ENA",
            true,          // is_buy = true (closes short)
            50.0,          // size
            0.10,          // 10% slippage (higher to ensure fill)
            false,         // reduce_only = false (NEVER true on Hyperliquid)
            Some(0.39),    // bid
            Some(0.40),    // ask (manually set to avoid rounding issues)
        )
        .await?;

    println!("Result: {:?}", result);
    Ok(())
}
