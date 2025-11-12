# Standalone Utilities

This folder contains **completely self-contained** utility scripts that can run independently anywhere. You can copy this entire folder to another location and it will work without any dependencies on parent directories.

All necessary code (connectors, utilities) is included within this folder.

## Prerequisites

### For JavaScript Utilities
```bash
# Install Node.js dependencies
cd standalone-utils/
npm install
```

Required dependencies:
- `dotenv` - Environment variable loading
- `ethers` - Hyperliquid signing
- `tweetnacl` - Pacifica signing
- `bs58` - Base58 encoding/decoding

### For Python Dashboard
```bash
# Navigate to dashboard folder
cd dashboard/

# Install Python dependencies
pip install -r requirements.txt
```

## Utilities Overview

### 1. Get Recent Fills (`get-recent-fills.js`)

**Purpose:** Fetch and display recent filled orders from both Hyperliquid and Pacifica exchanges.

**Setup:**
```bash
# First time setup
cd standalone-utils/
npm install
cp .env.example .env
# Edit .env with your credentials
```

**Usage:**
```bash
# From standalone-utils folder
node get-recent-fills.js [limit]

# Examples:
node get-recent-fills.js         # Show last 10 fills
node get-recent-fills.js 20      # Show last 20 fills
```

**Features:**
- Fetches fills from both exchanges in parallel
- Displays formatted tables with:
  - Timestamp
  - Symbol/Coin
  - Side (BUY/SELL, OPEN LONG/SHORT, etc.)
  - Size/Amount
  - Price and entry price
  - Fees
  - PnL (color-coded: green for profit, red for loss)
  - Order ID
- Summary statistics with combined totals

**Requirements:**
- `.env` file in standalone-utils folder with exchange credentials
- See `.env.example` for template

---

### 2. Cleanup All Positions (`cleanup-all-positions.js`)

**Purpose:** Emergency script to flatten every open position on both exchanges.

**⚠️ WARNING:** This script will close ALL positions immediately. Use with caution!

**Usage:**
```bash
# From standalone-utils folder
node cleanup-all-positions.js
```

**What It Does:**
1. Connects to both exchanges
2. Cancels all outstanding Pacifica orders (prevents new fills during cleanup)
3. Displays initial positions summary (NET exposure per symbol)
4. Closes Pacifica positions with reduce-only IOC market orders
5. Closes Hyperliquid positions with reduce-only market orders
6. Retries up to 3 times (configurable)
7. Reports any residual exposure

**Configuration (via environment variables):**
```bash
# .env file
CLEANUP_MAX_ATTEMPTS=3              # Max retry attempts
CLEANUP_RETRY_DELAY_MS=1500         # Delay between retries
CLEANUP_ORDERBOOK_WARMUP_MS=1000    # Wait for orderbook after subscribe
CLEANUP_SLEEP_AFTER_ORDER_MS=400    # Sleep between order executions
CLEANUP_MIN_POSITION=0.0000001      # Skip dust positions below this
```

**When to Use:**
- Bot crashes with orphaned positions
- Emergency shutdown required
- Testing cleanup after development
- Rebalancing before strategy changes

**Safety Features:**
- Reduce-only orders (won't open new positions)
- IOC (Immediate or Cancel) time-in-force
- Multiple retry attempts
- Detailed logging of success/failures
- Residual position reporting

---

### 3. Python Dashboard (`dashboard/`)

**Purpose:** Real-time monitoring dashboard for cross-exchange trading activity.

**Navigation:**
```bash
cd dashboard/
```

**Setup:**
```bash
# Install dependencies
pip install -r requirements.txt

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your credentials

# Copy and configure settings
cp config.json.example config.json  # if needed
# Edit config.json for your symbols and settings
```

**Usage:**
```bash
python dashboard.py
```

**Features:**
- Real-time orderbook monitoring
- Position tracking across exchanges
- PnL calculation and display
- Trade history
- WebSocket-based live updates
- Cross-exchange spread visualization

**Dashboard Controls:**
- `q` or `Ctrl+C` - Exit dashboard
- Auto-refresh on data updates

**Configuration:**
Edit `config.json` to customize:
- Monitored symbols
- Update intervals
- Display preferences
- Exchange settings

**See Also:**
- `dashboard/README.md` - Detailed dashboard documentation
- `dashboard/requirements.txt` - Python dependencies

---

## Environment Variables

All utilities require a `.env` file in the **standalone-utils folder**:

```bash
# Copy the example template
cp .env.example .env

# Then edit .env with your credentials
nano .env  # or use any text editor
```

Example `.env` contents:
```bash
# Hyperliquid
HL_WALLET=0x1234567890abcdef...
HL_PRIVATE_KEY=0xabcdef1234567890...

# Pacifica
PACIFICA_WALLET=YourSolanaWalletAddress
PACIFICA_PRIVATE_KEY=YourBase58EncodedPrivateKey
API_PUBLIC=YourPacificaApiPublicKey
API_PRIVATE=YourPacificaApiPrivateKey
```

**Security Note:** Never commit `.env` files to version control. The `.gitignore` already excludes them.

---

## Directory Structure

```
standalone-utils/
├── README.md                      # This file
├── .env.example                   # Environment template
├── .gitignore                     # Git ignore rules
├── config.json                    # Configuration file
├── package.json                   # Node.js dependencies
├── get-recent-fills.js           # Recent fills viewer
├── cleanup-all-positions.js      # Emergency position closer
├── connectors/                    # Exchange connector code
│   ├── hyperliquid.js            # Hyperliquid connector
│   └── pacifica.js               # Pacifica connector
├── utils/                         # Utility functions
│   ├── config.js                 # Config loader
│   ├── rate-limiter.js           # Rate limiting
│   └── pnl-tracker.js            # PnL tracking
└── dashboard/                     # Python dashboard
    ├── dashboard.py              # Main dashboard script
    ├── README.md                 # Dashboard documentation
    ├── requirements.txt          # Python dependencies
    ├── config.json               # Dashboard configuration
    ├── .env.example              # Environment template
    ├── connectors/               # Exchange connectors
    ├── pacifica_sdk/             # Pacifica SDK
    └── utils/                    # Utility functions
```

---

## Quick Start

1. **Navigate to folder:**
   ```bash
   cd standalone-utils/
   ```

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Setup credentials:**
   ```bash
   cp .env.example .env
   nano .env  # Edit with your API keys
   ```

4. **Run utilities:**
   ```bash
   node get-recent-fills.js
   node cleanup-all-positions.js
   ```

## Troubleshooting

### JavaScript Utilities

**Error: "Cannot find module"**
```bash
# Make sure you've installed dependencies in standalone-utils folder
cd standalone-utils/
npm install
```

**Error: "Missing environment variables"**
```bash
# Ensure .env file exists in standalone-utils folder
ls .env
# Copy from example if needed
cp .env.example .env
# Edit with your credentials
nano .env
```

**Error: "Connection failed"**
- Check your internet connection
- Verify exchange API endpoints are accessible
- Confirm credentials in `.env` are correct

### Python Dashboard

**Error: "No module named 'websockets'"**
```bash
cd dashboard/
pip install -r requirements.txt
```

**Error: "Connection refused"**
- Check if exchange WebSocket endpoints are accessible
- Verify your IP is not rate-limited
- Try restarting the dashboard

**Dashboard freezes or shows stale data**
- Exit with `q` or `Ctrl+C`
- Check your internet connection
- Restart the dashboard

---

## Portability

This folder is **completely portable** - you can:
- Copy it anywhere on your system
- Move it to another server
- Share it with others (without `.env` file!)
- Run it without any parent dependencies

All necessary code is self-contained within this folder.

**To move to another location:**
```bash
# Copy entire folder
cp -r standalone-utils /path/to/new/location/

# Navigate and setup
cd /path/to/new/location/standalone-utils/
npm install
cp .env.example .env
# Edit .env with credentials

# Ready to use!
node get-recent-fills.js
```

---

## Support

For issues or questions:
1. Check the main project README: `../README.md`
2. Review exchange API documentation in `../docs/`
3. Check bot logs: `../bot.log`
4. Verify configuration: `../config.json`

---

## Safety Reminders

⚠️ **Before using cleanup-all-positions.js:**
- Understand it will close ALL positions
- Make sure you actually want to flatten everything
- Check positions first with `node get-recent-fills.js`
- Consider using the bot's graceful shutdown instead (Ctrl+C)

✅ **Best Practices:**
- Always check positions before and after operations
- Keep `.env` secure and never share credentials
- Test on small positions first if unsure
- Monitor rate limits to avoid temporary bans
