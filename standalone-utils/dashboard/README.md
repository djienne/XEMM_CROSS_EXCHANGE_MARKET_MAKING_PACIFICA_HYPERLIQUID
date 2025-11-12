# XEMM Live Dashboard - Standalone Version

A standalone real-time monitoring dashboard for the XEMM (Cross-Exchange Market Making) bot, showing live positions, balances, orders, and fills across Hyperliquid and Pacifica exchanges.

## Features

- **Real-time Data**: WebSocket-driven updates with 3-second REST polling
- **Account Balances**: View available balance and total equity for both exchanges
- **Net Position Summary**: Monitor position balances across exchanges
- **Open Orders**: Track active orders with age indicators
- **Recent Orders**: View last 8 orders with status (filled, cancelled, etc.)
- **Recent Fills**: See latest 10 fills with timestamps and PnL

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Credentials

The `.env` file is already configured with your credentials. If you need to modify it:

```bash
# Edit .env with your exchange API credentials
# See .env.example for the template
```

Required environment variables:
- `HL_WALLET`: Hyperliquid wallet address
- `HL_PRIVATE_KEY`: Hyperliquid private key
- `SOL_WALLET`: Pacifica Solana wallet
- `API_PUBLIC`: Pacifica API public key
- `API_PRIVATE`: Pacifica API private key (base58)

### 3. Run Dashboard

```bash
python dashboard.py
```

Or with custom config:

```bash
python dashboard.py --config my_config.json
```

### 4. Exit

Press `Ctrl+C` to exit the dashboard cleanly.

## Configuration

Edit `config.json` to customize:

```json
{
  "symbols": ["SOL", "BTC", "ETH"],
  "order_size_usd": 20.0,
  "maker_exchange_preference": "auto"
}
```

Key parameters:
- `symbols`: List of trading symbols to monitor
- `order_size_usd`: Order size in USD
- `maker_exchange_preference`: "auto", "hyperliquid", or "pacifica"

See full config for all available parameters.

## Dashboard Layout

```
+----------------------------------------------------------+
| XEMM Live Dashboard - 2025-01-25 12:34:56 UTC           |
+----------------------------------------------------------+
| Account Balances                                         |
| Exchange      | Available         | Total Equity         |
| Hyperliquid   | $1,234.56        | $1,500.00           |
| Pacifica      | $987.65          | $1,200.00           |
+----------------------------------------------------------+
| Net Position Summary                                     |
| Symbol | HL Qty    | PAC Qty   | Net Qty   | Status    |
| SOL    | +10.0000  | -10.0000  | 0.0000    | BALANCED  |
+----------------------------------------------------------+
| Open Orders                                              |
| Exchange | Symbol | Side | Quantity | Price | Age       |
+----------------------------------------------------------+
| Recent Orders (Last 8)                                   |
| Time     | Ex  | Symbol | Side | Quantity | Status     |
+----------------------------------------------------------+
| Recent Fills                                             |
| Time     | Ex  | Symbol | Side | Quantity | Price | PnL |
+----------------------------------------------------------+
```

## File Structure

```
isolated/
├── dashboard.py           # Main dashboard application
├── config.json           # Trading configuration
├── .env                  # Exchange credentials (DO NOT COMMIT)
├── .env.example          # Template for credentials
├── requirements.txt      # Python dependencies
├── README.md            # This file
├── connectors/          # Exchange connector modules
│   ├── __init__.py
│   ├── base_exchange.py
│   ├── hyperliquid_connector.py
│   └── pacifica_connector.py
├── utils/               # Utility modules
│   ├── __init__.py
│   ├── config.py
│   ├── enums.py
│   ├── exceptions.py
│   └── logger.py
└── logs/                # Log files (auto-created)
```

## Troubleshooting

### Connection Issues

If you see connection errors:

1. Check your `.env` file has correct credentials
2. Ensure you have internet connectivity
3. Verify exchange API keys are valid
4. Check firewall settings allow WebSocket connections

### Missing Data

If positions/orders aren't showing:

1. Wait a few seconds for initial data load
2. Check logs in `logs/` directory for errors
3. Verify symbols in `config.json` exist on both exchanges
4. Ensure you have active positions/orders to display

### Unicode Errors (Windows)

If you see Unicode errors on Windows console:

- The dashboard uses ASCII-only formatting to avoid these issues
- If errors persist, set `PYTHONIOENCODING=utf-8` environment variable

## Logs

Logs are automatically written to:
- `logs/dashboard.log` - Main dashboard logs
- `logs/hyperliquid_connector.log` - Hyperliquid exchange logs
- `logs/pacifica_connector.log` - Pacifica exchange logs

View logs for debugging:

```bash
# Windows
type logs\dashboard.log

# Linux/Mac
tail -f logs/dashboard.log
```

## Security Notes

- **NEVER commit `.env` file** - it contains your private keys
- Keep `.env.example` as a template only
- Store `.env` securely and restrict file permissions
- Use separate API keys for testing vs production

## Performance

- **Update Frequency**: Dashboard updates every 3 seconds via REST polling
- **Rate Limiting**: Built-in rate limiter (max 5 concurrent API calls)
- **Memory Usage**: ~50-100 MB typical
- **CPU Usage**: <5% on modern systems

## Dependencies

Core packages:
- `hyperliquid-python-sdk` - Hyperliquid exchange SDK
- `websockets` - WebSocket client
- `rich` - Terminal UI framework
- `solders` - Solana/Pacifica crypto utilities
- `python-dotenv` - Environment variable management

See `requirements.txt` for full list.

## Support

For issues or questions:
1. Check logs in `logs/` directory
2. Review configuration in `config.json`
3. Verify credentials in `.env`
4. Check main XEMM documentation

## License

Part of the XEMM Cross-Exchange Market Making bot.
