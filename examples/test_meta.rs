use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let client = reqwest::Client::new();
    
    let response = client
        .post("https://api.hyperliquid.xyz/info")
        .json(&serde_json::json!({
            "type": "meta"
        }))
        .send()
        .await?;
    
    let text = response.text().await?;
    
    println!("Meta response:");
    println!("{}", &text[..text.len().min(1000)]);
    
    Ok(())
}
