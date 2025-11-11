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

### Fill Detection (4 Layers)

The bot uses a multi-layered fill detection system for maximum reliability:

1. **WebSocket Fill Detection** (primary, real-time) - Monitors Pacifica's `account_order_updates` channel
2. **REST API Order Polling** (backup, 500ms) - Polls order status via REST API
3. **Position Monitor** (ground truth, 500ms) - Detects fills by monitoring position changes
4. **Monitor Safety Check** (defensive) - Pre-cancellation verification in monitoring task

All methods deduplicate via shared HashSet to ensure only one hedge executes per fill.

### Exchange Connectivity

**Pacifica:**
- WebSocket orderbook (real-time bid/ask feed)
- WebSocket fill detection (monitors order fills/cancellations)
- WebSocket trading (ultra-fast order cancellation, no rate limits)
- REST API trading (authenticated order placement/cancellation)
- REST API polling (fallback orderbook data)
- REST API positions (position monitoring for fill detection)
- Ed25519 authenticated operations (both WebSocket and REST)
- **Dual cancellation safety** - Uses both REST + WebSocket for redundancy

**Hyperliquid:**
- WebSocket orderbook (real-time L2 book)
- REST API positions (clearinghouse state for position verification)
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
  "symbol": "SOL",
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
├── pacifica_orderbook.rs                      # View Pacifica orderbook (live)
├── pacifica_orderbook_rest_test.rs            # Test REST API orderbook
├── fill_detection_test.rs                     # Test fill detection
├── test_aggressive_fill_detection.rs          # Test all 4 fill detection methods
├── hyperliquid_market_test.rs                 # Test Hyperliquid trading
├── hyperliquid_orderbook.rs                   # View Hyperliquid orderbook
├── xemm_calculator.rs                         # Price calculator (no trading)
├── test_pacifica_positions.rs                 # View Pacifica positions
├── check_positions_debug.rs                   # Debug Hyperliquid positions
├── test_hyperliquid_trade_history.rs          # Test trade history API
├── rebalancer.rs                              # Position rebalancer (single exchange)
├── rebalancer_cross_exchange.rs               # Cross-exchange rebalancer
├── cancel_all_test.rs                         # Test REST cancel all
├── ws_cancel_all_test.rs                      # Test WebSocket cancel all
└── ... (30+ more examples for testing)
```

### Bot State Machine

The bot uses a state machine to track lifecycle:

- **Idle** - Waiting for opportunity, no active order
- **Active** - Order placed on Pacifica, monitoring for fill
- **Filled** - Order filled, waiting for hedge execution
- **Hedged** - Hedge executed successfully
- **Complete** - Cycle complete, bot exits
- **Error** - Unrecoverable error occurred

### 8 Concurrent Tasks

The XEMM bot orchestrates 8 async tasks running in parallel:

1. **Pacifica Orderbook (WebSocket)** - Real-time bid/ask feed
2. **Hyperliquid Orderbook (WebSocket)** - Real-time bid/ask feed
3. **Fill Detection (WebSocket)** - Monitors Pacifica order fills/cancellations
4. **Pacifica REST API Polling** - Fallback orderbook data (every 4s)
5. **Order Monitoring** - Profit tracking and order refresh (every 25ms)
5.5. **Position Monitor** - Position-based fill detection (every 500ms, ground truth)
6. **Hedge Execution** - Executes Hyperliquid hedge after fill
7. **Main Opportunity Loop** - Evaluates and places orders (every 100ms)

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `symbol` | "SOL" | Trading symbol (must exist on both exchanges) |
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
6. **Fill Detection**: 4-layer system detects when order fills
   - WebSocket fill detection (primary, real-time)
   - REST API order polling (backup, 500ms)
   - Position monitor (ground truth, 500ms)
   - Monitor safety check (pre-cancellation)
   - **Dual Cancellation**: Immediately cancel all orders via REST + WebSocket
7. **Hedge**: Execute market order on Hyperliquid (opposite direction)
8. **Wait**: 20-second delay for trades to propagate to exchange APIs
9. **Fetch**: Retrieve actual fill data from both exchanges with retry logic
10. **Calculate**: Compute actual profit using real fills and fees
11. **Complete**: Display comprehensive profit summary and exit

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

### Fill Detection Testing

Comprehensive test for the 4-layer fill detection system:

```bash
# Test all 4 fill detection methods with aggressive limit order
# Places order at 0.05% spread to ensure quick fill
# Verifies deduplication and position verification on both exchanges
cargo run --example test_aggressive_fill_detection --release
```

This test:
- Places an aggressive post-only limit order (5 bps spread)
- Monitors all 4 detection methods simultaneously
- Tracks which method detects first with timing analysis
- Verifies only one hedge executes (deduplication works)
- Checks positions on both Pacifica and Hyperliquid after hedge
- Shows comprehensive detection method summary

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

# View current positions on Pacifica
cargo run --example test_pacifica_positions --release

# Debug Hyperliquid positions (raw API response)
cargo run --example check_positions_debug --release

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

# Fetch and analyze recent trade history
cargo run --example fetch_recent_trades --release
cargo run --example test_hyperliquid_trade_history --release

# Cross-exchange position rebalancer
cargo run --example rebalancer --release
cargo run --example rebalancer_cross_exchange --release
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
[OPPORTUNITY] BUY @ $156.12 → HL $156.35 | Profit: 12.50 bps
[ORDER] ✓ Placed BUY #12345 @ $156.12 | cloid: abc123...xyz9
[FILL_DETECTION] ✓ FULL FILL: buy 0.1281 SOL @ $156.12
[SOL HEDGE] Executing SELL 0.1281 on Hyperliquid
[SOL HEDGE] ✓ Hedge executed successfully
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
- The bot uses 4 independent fill detection methods for redundancy
- Check fill detection WebSocket is connected (Task 3)
- Verify REST API order polling is working (Task 5)
- Check position monitor is running (Task 5.5)
- Verify account address matches credentials
- Enable debug logging: `RUST_LOG=debug`
- Check that deduplication HashSet isn't preventing detection

## Deployment

See `DEPLOYMENT.md` for Docker deployment instructions.

## Documentation

For detailed architecture and development guidelines, see `CLAUDE.md`.

## License

MIT

## Contributing

Contributions welcome! Please open an issue or PR.
