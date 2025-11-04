use ethers::signers::{LocalWallet, Signer};
use std::str::FromStr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env
    dotenv::dotenv().ok();

    let hl_wallet = std::env::var("HL_WALLET")?;
    let hl_private_key = std::env::var("HL_PRIVATE_KEY")?;

    println!("HL_WALLET from .env: {}", hl_wallet);
    println!("HL_PRIVATE_KEY from .env: {}", hl_private_key);
    println!();

    // Try parsing with LocalWallet::from_str
    let wallet1 = LocalWallet::from_str(&hl_private_key)?;
    println!("Method 1 - LocalWallet::from_str:");
    println!("  Derived address: {:?}", wallet1.address());
    println!();

    // Try parsing with parse
    let wallet2: LocalWallet = hl_private_key.parse()?;
    println!("Method 2 - parse:");
    println!("  Derived address: {:?}", wallet2.address());
    println!();

    // Check if they match
    let derived_addr = format!("{:?}", wallet1.address());
    println!("Match with HL_WALLET: {}", derived_addr.to_lowercase() == hl_wallet.to_lowercase());

    Ok(())
}
