/// Test price rounding function
fn round_price(price: f64, sz_decimals: i32, is_spot: bool) -> String {
    let max_decimals = ((if is_spot { 8 } else { 6 }) - sz_decimals).max(0);

    // Round to 5 significant figures
    // Calculate the scale factor for 5 sig figs
    let magnitude = price.abs().log10().floor();
    let scale = 10_f64.powf(magnitude - 4.0);
    let rounded_sig_figs = (price / scale).round() * scale;

    // Then limit decimal places
    let capped = format!("{:.prec$}", rounded_sig_figs, prec = max_decimals as usize)
        .parse::<f64>()
        .unwrap();

    // Remove trailing zeros
    let result = capped.to_string();

    // Remove trailing zeros after decimal point
    if result.contains('.') {
        result.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        result
    }
}

fn main() {
    println!("Testing price rounding with szDecimals=2 (SOL perp):");
    println!();

    let test_cases = vec![
        (186.66 * 1.05, "BUY with 5% slippage"),
        (186.66 * 0.95, "SELL with 5% slippage"),
        (1234.56, "1234.56"),
        (1234.5, "1234.5"),
        (0.001234, "0.001234"),
        (0.0012345, "0.0012345"),
        (123456.0, "123456 (integer)"),
        (12345.6, "12345.6"),
        (195.998, "195.998 (6 sig figs)"),
        (177.322, "177.322 (6 sig figs)"),
    ];

    for (price, label) in test_cases {
        let rounded = round_price(price, 2, false); // SOL has szDecimals=2, is perp
        println!("{:12} -> {:12} ({})", format!("{:.6}", price), rounded, label);
    }
}
