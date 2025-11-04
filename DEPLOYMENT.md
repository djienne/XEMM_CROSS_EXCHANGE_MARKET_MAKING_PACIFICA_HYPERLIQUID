# XEMM Bot Deployment Guide

This guide covers deploying the XEMM bot to a VPS using Docker.

## Prerequisites

On your VPS, you need:
- Docker Engine (20.10+)
- Docker Compose (2.0+)
- At least 1GB RAM
- At least 2GB disk space

### Installing Docker on Ubuntu/Debian

```bash
# Update packages
sudo apt-get update

# Install dependencies
sudo apt-get install -y ca-certificates curl gnupg

# Add Docker's official GPG key
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Add repository
echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Add your user to docker group (optional - allows running docker without sudo)
sudo usermod -aG docker $USER
newgrp docker
```

## Deployment Steps

### 1. Transfer Files to VPS

From your local machine:

```bash
# Create directory on VPS
ssh user@your-vps "mkdir -p ~/xemm-bot"

# Copy project files (adjust path as needed)
rsync -avz --exclude 'target' --exclude '.git' \
  /path/to/xemm_rust/ user@your-vps:~/xemm-bot/
```

Or clone from git:

```bash
ssh user@your-vps
cd ~
git clone <your-repo-url> xemm-bot
cd xemm-bot
```

### 2. Configure Environment

On the VPS:

```bash
cd ~/xemm-bot

# Create .env file with your credentials
cat > .env << 'ENVEOF'
# Pacifica Credentials
PACIFICA_API_KEY=your_pacifica_api_key_here
PACIFICA_SECRET_KEY=your_pacifica_secret_key_here
PACIFICA_ACCOUNT=your_pacifica_account_address_here

# Hyperliquid Credentials
HL_WALLET=your_hyperliquid_wallet_address_here
HL_PRIVATE_KEY=your_hyperliquid_private_key_here
ENVEOF

# Secure the .env file
chmod 600 .env

# Verify config.json exists and is correct
cat config.json
```

### 3. Build and Run

```bash
# Build the Docker image (first time only, or after code changes)
docker compose build

# Start the bot
docker compose up -d

# View logs
docker compose logs -f

# Stop following logs: Ctrl+C
```

### 4. Management Commands

```bash
# Check status
docker compose ps

# View logs (last 100 lines)
docker compose logs --tail=100

# Follow logs in real-time
docker compose logs -f

# Stop the bot
docker compose stop

# Start the bot
docker compose start

# Restart the bot
docker compose restart

# Stop and remove container
docker compose down

# Rebuild after code changes
docker compose build --no-cache
docker compose up -d

# View resource usage
docker stats xemm-bot
```

### 5. Updating the Bot

When you have code changes:

```bash
cd ~/xemm-bot

# Pull latest changes (if using git)
git pull

# Or rsync new files from local machine
# rsync -avz --exclude 'target' /local/path/ user@vps:~/xemm-bot/

# Rebuild and restart
docker compose build --no-cache
docker compose up -d

# Verify it's running
docker compose logs -f
```

## Monitoring

### View Logs

```bash
# Real-time logs
docker compose logs -f

# Last N lines
docker compose logs --tail=50

# Logs from specific time
docker compose logs --since=30m

# Save logs to file
docker compose logs > bot-logs.txt
```

### Check Health

```bash
# Container status
docker compose ps

# Resource usage
docker stats xemm-bot

# Check if process is running inside container
docker exec xemm-bot ps aux
```

## Troubleshooting

### Bot Won't Start

```bash
# Check logs for errors
docker compose logs

# Check container status
docker compose ps

# Verify environment variables
docker exec xemm-bot env | grep -E "(PACIFICA|HL)_"

# Verify config file
docker exec xemm-bot cat /app/config.json
```

### Connection Issues

```bash
# Check if bot can reach external APIs
docker exec xemm-bot ping -c 3 api.pacifica.fi
docker exec xemm-bot ping -c 3 api.hyperliquid.xyz

# Check DNS resolution
docker exec xemm-bot nslookup api.pacifica.fi
```

### High Memory Usage

Adjust resource limits in `docker-compose.yml`:

```yaml
deploy:
  resources:
    limits:
      memory: 1G  # Increase if needed
```

Then restart:

```bash
docker compose down
docker compose up -d
```

### Permission Issues

```bash
# Fix ownership of logs directory
sudo chown -R 1000:1000 logs/

# Rebuild with correct permissions
docker compose build --no-cache
docker compose up -d
```

## Production Best Practices

### 1. Use a Process Manager

For automatic restarts after server reboot, Docker Compose's `restart: unless-stopped` policy is already configured.

### 2. Set Up Log Rotation

Log rotation is configured in `docker-compose.yml`:
- Max size: 10MB per file
- Max files: 3 (30MB total)

### 3. Monitor with External Tools

Consider using:
- **Prometheus + Grafana** for metrics
- **Uptime monitoring** services (e.g., UptimeRobot)
- **Log aggregation** (e.g., ELK stack, Loki)

### 4. Backup Configuration

```bash
# Backup critical files
tar -czf xemm-backup-$(date +%Y%m%d).tar.gz \
  config.json .env docker-compose.yml
```

### 5. Security

```bash
# Secure .env file
chmod 600 .env

# Use firewall to restrict access
sudo ufw allow 22/tcp  # SSH only
sudo ufw enable

# Keep Docker updated
sudo apt-get update
sudo apt-get upgrade docker-ce docker-ce-cli
```

## Environment Variables Reference

| Variable | Description | Example |
|----------|-------------|---------|
| `PACIFICA_API_KEY` | Pacifica API key | `pac_xxxxx...` |
| `PACIFICA_SECRET_KEY` | Pacifica secret key | `sec_xxxxx...` |
| `PACIFICA_ACCOUNT` | Pacifica account address | `0x123...` |
| `HL_WALLET` | Hyperliquid wallet address | `0xabc...` |
| `HL_PRIVATE_KEY` | Hyperliquid private key | `0xdef...` |
| `RUST_LOG` | Logging level | `info` / `debug` / `warn` |
| `RUST_BACKTRACE` | Enable backtraces on errors | `1` / `0` |

## Configuration File (config.json)

Adjust these settings based on your strategy:

```json
{
  "symbol": "SOL",
  "notional_usd": 50.0,
  "maker_fee_bps": 1.0,
  "taker_fee_bps": 2.5,
  "profit_rate_bps": 10.0,
  "order_refresh_interval_secs": 5,
  "max_profit_loss_bps": -5.0
}
```

## Support

For issues or questions:
1. Check logs: `docker compose logs -f`
2. Review this deployment guide
3. Check the main README for bot configuration details
