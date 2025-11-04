# XEMM Rust - Cross-Exchange Market Making Bot

A high-performance Rust trading bot that performs single-cycle arbitrage between Pacifica (maker) and Hyperliquid (taker). The bot continuously monitors orderbook feeds from both exchanges, places limit orders on Pacifica when profitable opportunities arise, and immediately hedges fills on Hyperliquid.

## Features

### Core Trading
- ✅ **Real-time Arbitrage Detection** - Monitors both exchanges simultaneously
- ✅ **Automatic Order Placement** - Places limit orders on Pacifica when profitable
- ✅ **Instant Hedging** - Executes market orders on Hyperliquid after fills
- ✅ **Profit Monitoring** - Tracks and cancels orders if profit deviates
- ✅ **Order Refresh** - Auto-cancels stale orders based on age
- ✅ **Single-cycle Mode** - Exits after one successful arbitrage cycle

### Exchange Connectivity

**Pacifica:**
- WebSocket orderbook (real-time bid/ask feed)
- WebSocket fill detection (monitors order fills/cancellations)
- WebSocket trading (ultra-fast order cancellation, no rate limits)
- REST API trading (authenticated order placement/cancellation)
- REST API polling (fallback orderbook data)
- Ed25519 authenticated operations (both WebSocket and REST)
- **Dual cancellation safety** - Uses both REST + WebSocket for redundancy

**Hyperliquid:**
- WebSocket orderbook (real-time L2 book)
- EIP-712 authenticated trading (market orders)
- Automatic slippage protection

### Performance & Reliability
- ✅ **Multi-source Orderbook** - WebSocket primary, REST API fallback
- ✅ **Dual Cancellation** - REST + WebSocket cancellation on fill (defense in depth)
- ✅ **Auto-reconnect** - Exponential backoff on connection failures
- ✅ **Concurrent Tasks** - 7 async tasks running in parallel
- ✅ **High-frequency Monitoring** - 25ms profit checks, 100ms opportunity evaluation
- ✅ **Zero Rate Limits** - WebSocket cancellation bypasses API rate limits
- ✅ **Graceful Shutdown** - Cancels orders on Ctrl+C

### User Interface
- ✅ **Colorized Terminal Output** - Easy-to-read colored logs with section labels
- ✅ **Visual Status Indicators** - Green ✓ for success, Red ✗ for errors, Yellow ⚠ for warnings
- ✅ **Real-time Profit Display** - Color-coded profit changes (green = increasing, red = decreasing)
- ✅ **Comprehensive Trade Summary** - Beautiful formatted summary with emojis and colors on completion

## Quick Start

### 1. Set up credentials

Create a `.env` file with your API credentials:

```bash
# Pacifica credentials
PACIFICA_API_KEY=your_api_key
PACIFICA_SECRET_KEY=your_secret_key_base58
PACIFICA_ACCOUNT=your_account_address

# Hyperliquid credentials
HL_WALLET=your_wallet_address
HL_PRIVATE_KEY=your_private_key_hex
```

### 2. Configure bot parameters

Edit `config.json`:

```json
{
  "symbol": "ENA",
  "agg_level": 1,
  "reconnect_attempts": 5,
  "ping_interval_secs": 15,
  "low_latency_mode": false,
  "pacifica_maker_fee_bps": 1.5,
  "hyperliquid_taker_fee_bps": 4.0,
  "profit_rate_bps": 10.0,
  "order_notional_usd": 20.0,
  "profit_cancel_threshold_bps": 3.0,
  "order_refresh_interval_secs": 30,
  "hyperliquid_slippage": 0.05,
  "pacifica_rest_poll_interval_secs": 4
}
```

### 3. Run the bot

```bash
# Run the XEMM trading bot
cargo run

# With debug logging
RUST_LOG=debug cargo run

# In release mode (optimized)
cargo run --release
```

## Architecture

### Project Structure

```
src/
├── main.rs             # Main trading bot binary
├── lib.rs              # Library exports
├── config.rs           # Config management (loads config.json)
├── trade_fetcher.rs    # Post-hedge trade fetching and profit calculation
├── bot/
│   ├── mod.rs
│   └── state.rs        # Bot state machine (Idle/Active/Filled/Hedged/Error)
├── strategy/
│   ├── mod.rs
│   └── opportunity.rs  # Opportunity evaluation and profit calculation
└── connector/
    ├── pacifica/
    │   ├── mod.rs
    │   ├── types.rs           # WebSocket/REST message types
    │   ├── client.rs          # Orderbook WebSocket client
    │   ├── trading.rs         # REST API trading (place/cancel orders)
    │   ├── ws_trading.rs      # WebSocket trading (ultra-fast cancel_all)
    │   └── fill_detection.rs  # WebSocket fill monitoring client
    └── hyperliquid/
        ├── mod.rs
        ├── types.rs           # Data structures
        ├── client.rs          # Orderbook WebSocket client
        └── trading.rs         # REST API trading (market orders)

examples/
├── pacifica_orderbook_rest_test.rs    # Test REST API orderbook
├── fill_detection_test.rs             # Test fill detection
├── hyperliquid_market_test.rs         # Test Hyperliquid trading
└── xemm_calculator.rs                 # Price calculator (no trading)
```

### Bot State Machine

The bot uses a state machine to track lifecycle:

- **Idle** - Waiting for opportunity, no active order
- **Active** - Order placed on Pacifica, monitoring for fill
- **Filled** - Order filled, waiting for hedge execution
- **Hedged** - Hedge executed successfully
- **Complete** - Cycle complete, bot exits
- **Error** - Unrecoverable error occurred

### 7 Concurrent Tasks

The XEMM bot orchestrates 7 async tasks running in parallel:

1. **Pacifica Orderbook (WebSocket)** - Real-time bid/ask feed
2. **Hyperliquid Orderbook (WebSocket)** - Real-time bid/ask feed
3. **Fill Detection (WebSocket)** - Monitors Pacifica order fills/cancellations
4. **Pacifica REST API Polling** - Fallback orderbook data (every 4s)
5. **Order Monitoring** - Profit tracking and order refresh (every 25ms)
6. **Hedge Execution** - Executes Hyperliquid hedge after fill
7. **Main Opportunity Loop** - Evaluates and places orders (every 100ms)

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `symbol` | "ENA" | Trading symbol (must exist on both exchanges) |
| `agg_level` | 1 | Orderbook aggregation level (1, 2, 5, 10, 100, 1000) |
| `pacifica_maker_fee_bps` | 1.5 | Pacifica maker fee in basis points |
| `hyperliquid_taker_fee_bps` | 4.0 | Hyperliquid taker fee in basis points |
| `profit_rate_bps` | 10.0 | Target profit in basis points (0.1%) |
| `order_notional_usd` | 20.0 | Order size in USD |
| `profit_cancel_threshold_bps` | 3.0 | Cancel if profit deviates ±3 bps |
| `order_refresh_interval_secs` | 30 | Auto-cancel orders older than 30s |
| `hyperliquid_slippage` | 0.05 | Maximum slippage for market orders (5%) |
| `pacifica_rest_poll_interval_secs` | 4 | REST API fallback polling interval |
| `ping_interval_secs` | 15 | WebSocket ping interval (max 30s) |

## Trading Workflow

1. **Startup**: Cancel all existing Pacifica orders
2. **Wait**: Gather initial orderbook data (3s warmup)
3. **Evaluate**: Check both BUY and SELL opportunities every 100ms
4. **Place**: If profitable (>target profit), place limit order on Pacifica
5. **Monitor**: Track profit every 25ms, cancel if deviation >3 bps or age >30s
6. **Fill**: Fill detection WebSocket notifies when order fills
   - **Dual Cancellation**: Immediately cancel all orders via REST + WebSocket
7. **Hedge**: Execute market order on Hyperliquid (opposite direction)
8. **Complete**: Display profit summary and exit

## Opportunity Calculation

**Buy Opportunity** (Buy on Pacifica, Sell on Hyperliquid):
```
price = (HL_bid * (1 - taker_fee)) / (1 + maker_fee + profit_rate)
```

**Sell Opportunity** (Sell on Pacifica, Buy on Hyperliquid):
```
price = (HL_ask * (1 + taker_fee)) / (1 - maker_fee - profit_rate)
```

Prices are rounded to tick_size (buy rounds down, sell rounds up).

## Examples & Testing

### Core Examples

Essential examples for understanding and testing the system:

```bash
# View Pacifica orderbook (WebSocket live stream)
cargo run --example pacifica_orderbook --release

# Test Pacifica orderbook REST API
cargo run --example pacifica_orderbook_rest_test --release

# Test fill detection WebSocket
cargo run --example fill_detection_test --release

# Test Hyperliquid market orders
cargo run --example hyperliquid_market_test --release

# View Hyperliquid orderbook
cargo run --example hyperliquid_orderbook --release

# Calculate opportunities without trading
cargo run --example xemm_calculator --release

# Advanced orderbook usage with statistics
cargo run --example advanced_usage --release

# Low-latency orderbook mode
cargo run --example low_latency --release
```

### Trading Examples

Generic trading examples (educational):

```bash
# Simple trading example
cargo run --example simple_trade --release

# More complex trading example
cargo run --example trading_example --release
```

### Utility Examples

Helper tools and utilities:

```bash
# Cancel all open orders (REST API)
cargo run --example cancel_all_test --release

# Test WebSocket cancel all orders
cargo run --example ws_cancel_all_test --release

# Check available Hyperliquid symbols
cargo run --example check_hyperliquid_symbols --release

# Close ENA position helper
cargo run --example close_ena_position --release

# Verify wallet/credentials
cargo run --example verify_wallet --release

# Debug MessagePack serialization
cargo run --example debug_msgpack --release

# Test metadata parsing
cargo run --example test_meta --release
cargo run --example test_meta_parse --release

# Test price rounding logic
cargo run --example test_price_rounding --release
```

### Symbol-Specific Test Examples

Order placement tests for specific coins:

```bash
# Test orders for different symbols
cargo run --example test_btc_orders --release
cargo run --example test_eth_orders --release
cargo run --example test_pengu_orders --release
cargo run --example test_pump_orders --release
cargo run --example test_xpl_orders --release
```

## Development Commands

```bash
# Check compilation
cargo check

# Build (debug)
cargo build

# Build (release/optimized)
cargo build --release

# Run main XEMM bot
cargo run
RUST_LOG=debug cargo run  # With debug logging

# Run tests
cargo test
cargo test --lib  # Library tests only
```

## Terminal Output

The bot features colorized terminal output for easy monitoring:

### Color Scheme
- **Section Labels**: Color-coded by task type
  - `[CONFIG]` - Blue (configuration values)
  - `[INIT]` - Cyan (initialization steps)
  - `[PACIFICA_OB]` / `[HYPERLIQUID_OB]` - Magenta (orderbook feeds)
  - `[FILL_DETECTION]` - Magenta (fill events)
  - `[MONITOR]` - Yellow (profit monitoring)
  - `[PROFIT]` - Bright blue (profit updates)
  - `[OPPORTUNITY]` - Bright green (arbitrage opportunities)
  - `[ORDER]` - Bright yellow (order placement)
  - `[HEDGE]` - Bright magenta (hedge execution)
  - `[SHUTDOWN]` - Yellow (cleanup)

- **Status Indicators**:
  - ✓ Green - Success
  - ✗ Red - Error/Failure
  - ⚠ Yellow - Warning

- **Trading Data**:
  - Prices: Cyan
  - BUY orders: Green
  - SELL orders: Red
  - Profit increasing: Green
  - Profit decreasing: Red
  - Symbols: Bright white bold
  - Fees: Yellow

### Example Output
```
[INIT] ✓ Credentials loaded successfully
[OPPORTUNITY] BUY @ $0.391200 → HL $0.392500 | Profit: 12.50 bps
[ORDER] ✓ Placed BUY #12345 @ $0.3912 | cloid: abc123...xyz9
[FILL_DETECTION] ✓ FULL FILL: buy 51.2048 ENA @ $0.3912
[ENA HEDGE] Executing SELL 51.2048 on Hyperliquid
[ENA HEDGE] ✓ Hedge executed successfully
═══════════════════════════════════════════════════
  BOT CYCLE COMPLETE!
═══════════════════════════════════════════════════
```

## Important Notes

- **Mainnet only**: Production system, uses real funds
- **Single-cycle**: Bot exits after one successful hedge
- **No position accumulation**: Always hedges immediately after fill
- **Graceful shutdown**: Ctrl+C cancels remaining orders before exit
- **Credentials**: Never commit `.env` file to version control
- **Testing**: Always test with small `order_notional_usd` first (e.g., 5.0)

## Troubleshooting

**Signature verification failed (Pacifica):**
- Check Ed25519 key is correct (first 32 bytes of Solana key)
- Verify JSON canonicalization
- Ensure timestamp is current (within 5s)

**Order rejected:**
- Verify price is rounded to tick_size
- Verify size is rounded to lot_size
- Check market info is up to date

**WebSocket disconnects:**
- Ensure ping interval ≤30 seconds (default: 15s)
- Check network stability
- Review logs for specific errors

**No opportunities detected:**
- Verify both orderbook feeds are connected
- Check fee configuration (maker + taker fees)
- Ensure profit_rate_bps is realistic (default: 10 bps)
- Review spread between exchanges

**Fill not detected:**
- Check fill detection WebSocket is connected
- Verify account address matches credentials
- Enable debug logging: `RUST_LOG=debug`

## Deployment

See `DEPLOYMENT.md` for Docker deployment instructions.

## Documentation

For detailed architecture and development guidelines, see `CLAUDE.md`.

## License

MIT

## Contributing

Contributions welcome! Please open an issue or PR.
