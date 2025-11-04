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

    println!("Loading Hyperliquid credentials...");
    dotenv::dotenv().ok();
    let credentials = HyperliquidCredentials::from_env()?;

    println!("Creating Hyperliquid client...");
    let trading = HyperliquidTrading::new(credentials, false)?;

    println!("\nFetching available symbols from Hyperliquid...\n");
    let meta = trading.get_meta().await?;

    println!("Found {} available symbols:\n", meta.universe.len());

    // Search for ENA
    let ena_found = meta.universe.iter().any(|asset| asset.name == "ENA");

    if ena_found {
        println!("✓ ENA is available on Hyperliquid");
    } else {
        println!("✗ ENA is NOT available on Hyperliquid");
        println!("\nSearching for similar symbols...");

        let similar: Vec<_> = meta.universe.iter()
            .filter(|asset| asset.name.contains("EN") || asset.name.starts_with("E"))
            .map(|asset| &asset.name)
            .collect();

        if !similar.is_empty() {
            println!("Similar symbols found: {:?}", similar);
        }
    }

    println!("\n--- All Available Symbols ---");
    for (i, asset) in meta.universe.iter().enumerate() {
        print!("{:8}", asset.name);
        if (i + 1) % 8 == 0 {
            println!();
        }
    }
    println!("\n");

    // Check specific symbols
    let test_symbols = vec!["BTC", "ETH", "SOL", "XPL", "ENA"];
    println!("--- Checking Specific Symbols ---");
    for symbol in test_symbols {
        let found = meta.universe.iter().any(|asset| asset.name == symbol);
        if found {
            println!("✓ {} is available", symbol);
        } else {
            println!("✗ {} is NOT available", symbol);
        }
    }

    Ok(())
}
