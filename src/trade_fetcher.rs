/// Trade fetching and profit calculation utilities
///
/// This module contains the logic for fetching actual trade fills from
/// both exchanges after a hedge execution. This is used by the main bot
/// for profit calculation and can be tested independently.

use std::sync::Arc;
use std::time::Duration;

use crate::connector::pacifica::trading::{PacificaTrading, TradeHistoryItem};
use crate::connector::hyperliquid::trading::HyperliquidTrading;
use crate::connector::hyperliquid::types::UserFill;

/// Result of fetching trade data from an exchange
#[derive(Debug, Clone)]
pub struct TradeFetchResult {
    pub fill_price: Option<f64>,
    pub actual_fee: Option<f64>,
    pub total_size: Option<f64>,
    pub total_notional: Option<f64>,  // Actual total USD value from all fills
}

/// Result of profit calculation
#[derive(Debug, Clone)]
pub struct ProfitResult {
    pub gross_pnl: f64,
    pub net_profit: f64,
    pub profit_bps: f64,
    pub pac_fee: f64,
    pub hl_fee: f64,
}

/// Calculate profit from hedge trade using actual notional values
///
/// # Arguments
/// * `pac_notional` - Actual USD notional value from Pacifica
/// * `hl_notional` - Actual USD notional value from Hyperliquid
/// * `pac_fee` - Actual fee paid on Pacifica
/// * `hl_fee` - Actual fee paid on Hyperliquid
/// * `is_pacifica_buy` - True if bought on Pacifica, false if sold
///
/// # Returns
/// ProfitResult with gross P&L, net profit, and profit in basis points
pub fn calculate_hedge_profit(
    pac_notional: f64,
    hl_notional: f64,
    pac_fee: f64,
    hl_fee: f64,
    is_pacifica_buy: bool,
) -> ProfitResult {
    // Calculate gross P&L based on direction
    let gross_pnl = if is_pacifica_buy {
        // Bought on Pacifica, sold on Hyperliquid
        hl_notional - pac_notional
    } else {
        // Sold on Pacifica, bought on Hyperliquid
        pac_notional - hl_notional
    };

    // Calculate net profit after fees
    let net_profit = gross_pnl - pac_fee - hl_fee;

    // Calculate profit in basis points (based on Pacifica notional)
    let profit_bps = if pac_notional > 0.0 {
        (net_profit / pac_notional) * 10000.0
    } else {
        0.0
    };

    ProfitResult {
        gross_pnl,
        net_profit,
        profit_bps,
        pac_fee,
        hl_fee,
    }
}

/// Fetch Pacifica trade history with retry logic
///
/// Attempts to find trades matching the given client_order_id.
/// Retries up to max_attempts times with 10-second delays between attempts.
///
/// Returns: (fill_price, actual_fee)
pub async fn fetch_pacifica_trade(
    trading: Arc<PacificaTrading>,
    symbol: &str,
    client_order_id: &str,
    max_attempts: u32,
    log_fn: impl Fn(&str),
) -> TradeFetchResult {
    let mut attempt = 1;
    let mut result = TradeFetchResult {
        fill_price: None,
        actual_fee: None,
        total_size: None,
        total_notional: None,
    };

    while attempt <= max_attempts {
        log_fn(&format!(
            "Fetching Pacifica trade history (attempt {}/{})",
            attempt, max_attempts
        ));

        match trading
            .get_trade_history(Some(symbol), Some(20), None, None)
            .await
        {
            Ok(trades) => {
                // Find all MAKER trades matching our client_order_id
                // Filter out taker fills (market orders) to only get limit order fills
                let matching_trades: Vec<_> = trades
                    .iter()
                    .filter(|t| {
                        &t.client_order_id == client_order_id &&
                        t.event_type == "fulfill_maker"
                    })
                    .collect();

                if !matching_trades.is_empty() {
                    log_fn(&format!(
                        "✓ Found {} matching Pacifica trade(s)",
                        matching_trades.len()
                    ));

                    result = calculate_pacifica_trade_result(&matching_trades);
                    break; // Found the trade, exit retry loop
                } else {
                    log_fn("⚠ No matching Pacifica trades found yet");

                    if attempt < max_attempts {
                        log_fn("Waiting 10 seconds before retry...");
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                }
            }
            Err(e) => {
                log_fn(&format!("✗ Failed to fetch Pacifica trade history: {}", e));

                if attempt < max_attempts {
                    log_fn("Waiting 10 seconds before retry...");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        }

        attempt += 1;
    }

    result
}

/// Calculate weighted average price and total fees from Pacifica trades
pub fn calculate_pacifica_trade_result(trades: &[&TradeHistoryItem]) -> TradeFetchResult {
    if trades.len() == 1 {
        // Single fill
        let price = trades[0].entry_price.parse::<f64>().ok();
        let size = trades[0].amount.parse::<f64>().ok();
        let notional = match (price, size) {
            (Some(p), Some(s)) => Some(p * s),
            _ => None,
        };

        TradeFetchResult {
            fill_price: price,
            actual_fee: trades[0].fee.parse().ok(),
            total_size: size,
            total_notional: notional,
        }
    } else {
        // Multiple fills with same client_order_id
        // BUGFIX: Pacifica API sometimes reports wrong entry_price for "close" side trades
        // For fills with the same client_order_id and timestamp, use the price from the FIRST fill
        // (which is typically the "open" side trade with the correct limit order price)

        // Get the reference price from the first fill
        let reference_price = trades[0].entry_price.parse::<f64>().ok();

        let mut total_notional = 0.0;
        let mut total_size = 0.0;
        let mut total_fee = 0.0;

        if let Some(ref_price) = reference_price {
            // Use the same price for all fills (they came from the same limit order)
            for trade in trades {
                if let Ok(size) = trade.amount.parse::<f64>() {
                    total_notional += ref_price * size;
                    total_size += size;
                }
                if let Ok(fee) = trade.fee.parse::<f64>() {
                    total_fee += fee;
                }
            }
        }

        TradeFetchResult {
            fill_price: reference_price,
            actual_fee: if total_fee > 0.0 {
                Some(total_fee)
            } else {
                None
            },
            total_size: if total_size > 0.0 {
                Some(total_size)
            } else {
                None
            },
            total_notional: if total_notional > 0.0 {
                Some(total_notional)
            } else {
                None
            },
        }
    }
}

/// Fetch Hyperliquid user fills with retry logic
///
/// Attempts to find recent fills for the given symbol.
/// Retries up to max_attempts times with 10-second delays between attempts.
///
/// Returns: (fill_price, actual_fee)
pub async fn fetch_hyperliquid_fills(
    trading: &HyperliquidTrading,
    wallet: &str,
    symbol: &str,
    max_attempts: u32,
    time_window_secs: u64,
    log_fn: impl Fn(&str),
) -> TradeFetchResult {
    let mut attempt = 1;
    let mut result = TradeFetchResult {
        fill_price: None,
        actual_fee: None,
        total_size: None,
        total_notional: None,
    };

    while attempt <= max_attempts {
        log_fn(&format!(
            "Fetching Hyperliquid fills (attempt {}/{})",
            attempt, max_attempts
        ));

        match trading.get_user_fills(wallet, true).await {
            Ok(fills) => {
                // Find recent fills for this symbol
                let now = chrono::Utc::now().timestamp_millis() as u64;
                let time_window_ms = time_window_secs * 1000;

                let recent_fills: Vec<_> = fills
                    .iter()
                    .filter(|f| {
                        f.coin == symbol && now.saturating_sub(f.time) < time_window_ms
                    })
                    .collect();

                if !recent_fills.is_empty() {
                    log_fn(&format!(
                        "✓ Found {} matching Hyperliquid fill(s)",
                        recent_fills.len()
                    ));

                    result = calculate_hyperliquid_fill_result(&recent_fills);
                    break; // Found the fills, exit retry loop
                } else {
                    log_fn("⚠ No matching Hyperliquid fills found yet");

                    if attempt < max_attempts {
                        log_fn("Waiting 10 seconds before retry...");
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                }
            }
            Err(e) => {
                log_fn(&format!("✗ Failed to fetch Hyperliquid fills: {}", e));

                if attempt < max_attempts {
                    log_fn("Waiting 10 seconds before retry...");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        }

        attempt += 1;
    }

    result
}

/// Calculate weighted average price and total fees from Hyperliquid fills
pub fn calculate_hyperliquid_fill_result(fills: &[&UserFill]) -> TradeFetchResult {
    if fills.len() == 1 {
        // Single fill
        let price = fills[0].px.parse::<f64>().ok();
        let size = fills[0].sz.parse::<f64>().ok();
        let notional = match (price, size) {
            (Some(p), Some(s)) => Some(p * s),
            _ => None,
        };

        TradeFetchResult {
            fill_price: price,
            actual_fee: fills[0].fee.parse().ok(),
            total_size: size,
            total_notional: notional,
        }
    } else {
        // Multiple fills - calculate weighted average and sum fees
        let mut total_notional = 0.0;
        let mut total_size = 0.0;
        let mut total_fee = 0.0;

        for fill in fills {
            if let (Ok(price), Ok(size)) = (fill.px.parse::<f64>(), fill.sz.parse::<f64>()) {
                total_notional += price * size;
                total_size += size;
            }
            if let Ok(fee) = fill.fee.parse::<f64>() {
                total_fee += fee;
            }
        }

        TradeFetchResult {
            fill_price: if total_size > 0.0 {
                Some(total_notional / total_size)
            } else {
                None
            },
            actual_fee: if total_fee > 0.0 {
                Some(total_fee)
            } else {
                None
            },
            total_size: if total_size > 0.0 {
                Some(total_size)
            } else {
                None
            },
            total_notional: if total_notional > 0.0 {
                Some(total_notional)
            } else {
                None
            },
        }
    }
}
