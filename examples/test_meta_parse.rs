use xemm_rust::connector::hyperliquid::{HyperliquidTrading, HyperliquidCredentials};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    
    let credentials = HyperliquidCredentials::from_env()?;
    let trading = HyperliquidTrading::new(credentials, false)?;
    
    println!("Fetching meta...");
    let meta = trading.get_meta().await?;
    
    println!("Successfully parsed meta!");
    println!("Total assets: {}", meta.universe.len());
    println!("\nFirst 5 assets:");
    for (i, asset) in meta.universe.iter().take(5).enumerate() {
        println!("  {}: {} (szDecimals: {}, maxLeverage: {:?})", 
            i, asset.name, asset.sz_decimals, asset.max_leverage);
    }
    
    // Find XPL
    if let Some(xpl) = meta.universe.iter().find(|a| a.name == "XPL") {
        println!("\nFound XPL:");
        println!("  szDecimals: {}", xpl.sz_decimals);
        println!("  maxLeverage: {:?}", xpl.max_leverage);
        println!("  isDelisted: {:?}", xpl.is_delisted);
    }
    
    Ok(())
}
