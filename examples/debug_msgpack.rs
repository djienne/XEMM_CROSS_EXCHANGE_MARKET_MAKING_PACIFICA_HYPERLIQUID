use serde_json::json;
use xemm_rust::connector::hyperliquid::types::*;

fn main() -> anyhow::Result<()> {
    // Create a simple order action
    let order = Order {
        a: 4,  // Asset ID
        b: true,  // Is buy
        p: "1100".to_string(),  // Price
        s: "0.2".to_string(),  // Size
        r: false,  // Reduce only
        t: OrderType {
            limit: LimitOrderType {
                tif: TimeInForce::Gtc,
            },
        },
        c: None,  // No client order ID
    };

    let action = Action {
        type_: "order".to_string(),
        orders: vec![order],
        grouping: "na".to_string(),
    };

    // Encode with msgpack (using named encoding)
    let msgpack_bytes_named = rmp_serde::encode::to_vec_named(&action)?;
    let msgpack_bytes_compact = rmp_serde::to_vec(&action)?;

    println!("=== NAMED ENCODING (correct) ===");
    let msgpack_bytes = msgpack_bytes_named;

    println!("Action JSON:");
    println!("{}", serde_json::to_string_pretty(&action)?);
    println!();
    println!("Msgpack bytes ({} bytes):", msgpack_bytes.len());
    println!("{}", hex::encode(&msgpack_bytes));
    println!();
    println!("Msgpack bytes (debug):");
    for (i, byte) in msgpack_bytes.iter().enumerate() {
        print!("{:02x} ", byte);
        if (i + 1) % 16 == 0 {
            println!();
        }
    }
    println!();
    println!();

    println!("=== COMPACT ENCODING (incorrect - encodes as array) ===");
    println!("Msgpack bytes ({} bytes):", msgpack_bytes_compact.len());
    println!("{}", hex::encode(&msgpack_bytes_compact));
    println!();

    Ok(())
}
