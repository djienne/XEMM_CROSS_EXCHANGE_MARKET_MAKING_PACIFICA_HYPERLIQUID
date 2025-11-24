# XEMM Dashboard

A Node.js web dashboard for monitoring and controlling the XEMM trading bot remotely.

## Features

- **Real-time Status Monitoring** - View bot status (Running/Stopped) with zombie process filtering
- **Remote Control** - Start, stop, and deploy the bot from your browser
- **Live Logs** - View and auto-refresh bot logs (tail output)
- **Trade History** - Display recent trades with PnL and statistics
- **Configuration Viewer** - View current bot configuration
- **Auto-refresh** - Status updates every 10s, logs every 15s

## Quick Start

### 1. Install Dependencies

```bash
cd dashboard_js
npm install
```

### 2. Configure Remote Access

Edit `server.js` to set your remote server details:

```javascript
const REMOTE_USER = "ubuntu";
const REMOTE_HOST = "54.95.246.213";
const REMOTE_PATH = "/home/ubuntu/XEMM_rust";
const SSH_KEY_NAME = "lighter.pem";
```

### 3. Ensure SSH Key Access

Make sure your SSH key (`lighter.pem` or similar) is in the parent directory and has proper permissions:

```bash
chmod 600 ../lighter.pem
```

### 4. Start the Dashboard

```bash
node server.js
```

The dashboard will be available at `http://localhost:3000`

## Dashboard Interface

### Status Panel
- **Green Badge**: Bot is running
- **Red Badge**: Bot is stopped
- **Yellow Badge**: Connection error or unknown status

### Control Buttons
- **Start Bot** - Executes `run_nohup.sh` on remote server
- **Stop Bot** - Kills all bot processes
- **Deploy** - Runs `deploy.py` to sync code to remote
- **Refresh** - Manual refresh of logs and trades

### Log Viewer
- Shows last 100 lines by default (configurable)
- ANSI color codes rendered for easy reading
- Auto-scrolls to bottom
- Auto-refreshes every 15 seconds

### Trade History
- Recent trades from CSV files (synced from remote)
- Statistics: Total trades, PnL, avg profit, win rate, latency
- Sync button to download latest trades from remote

## API Endpoints

The server exposes the following REST API:

- `GET /api/status` - Get bot status (RUNNING/STOPPED)
- `POST /api/start` - Start the bot
- `POST /api/stop` - Stop the bot
- `POST /api/deploy` - Deploy code to remote
- `GET /api/logs?lines=100` - Fetch logs
- `GET /api/config` - Get bot configuration
- `GET /api/trades` - Get trade history
- `POST /api/sync_trades` - Sync trades from remote

## Status Detection

The dashboard uses a robust status detection system:

```bash
ps aux | grep '[x]emm_rust' | grep -v grep | awk '{print $8}' | grep -E '^[^ZD]'
```

This ensures:
- Only actual running processes are detected (not zombie/defunct)
- Process states Z (zombie) and D (defunct) are filtered out
- Accurate status even with orphaned processes

## File Structure

```
dashboard_js/
├── server.js           # Express server with SSH remote control
├── public/
│   ├── index.html      # Dashboard UI
│   ├── script.js       # Frontend logic
│   └── style.css       # Styling
├── package.json        # Dependencies
└── README.md          # This file
```

## Dependencies

- `express` - Web server framework
- `body-parser` - JSON request parsing
- `cors` - Cross-origin resource sharing
- `csv-parser` - Parse trade CSV files

## Security Notes

- The dashboard connects to remote server via SSH
- SSH key must be properly secured (600 permissions)
- Dashboard runs on localhost by default (not exposed to internet)
- For production, add authentication and use HTTPS

## Troubleshooting

**Status always shows "Running" even when stopped:**
- Check for zombie processes: `ssh remote "ps aux | grep xemm"`
- Manually kill: `ssh remote "bash kill_process.sh"`

**Cannot connect to remote:**
- Verify SSH key path and permissions
- Test SSH manually: `ssh -i ../lighter.pem ubuntu@host`
- Check firewall and security group settings

**Logs not loading:**
- Ensure `output.log` exists on remote: `ssh remote "ls -la /home/ubuntu/XEMM_rust/output.log"`
- Check remote path configuration in `server.js`

**Trades not showing:**
- Run "Sync Trades" to download from remote
- Check `downloaded_trades/` directory exists in parent folder
