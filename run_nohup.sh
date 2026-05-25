#!/bin/bash
# Start the market maker bot in background with nohup

# Check if xemm_rust is already running
if pgrep -f "xemm_rust" > /dev/null 2>&1; then
    echo "WARN  WARNING: xemm_rust process is already running!"
    echo "Please stop the existing process before starting a new one."
    echo ""
    echo "To view running processes:"
    echo "  ps aux | grep xemm_rust"
    echo ""
    echo "To stop all bot processes:"
    echo "  bash kill_process.sh"
    exit 1
fi

echo "OK No existing xemm_rust process found. Starting bot..."
# Source cargo environment to ensure cargo is in PATH
source "$HOME/.cargo/env"
nohup cargo run --release > output.log 2>&1 &
echo "OK Bot started in background (PID: $!)"
echo "OK Output logged to: output.log"
echo ""
echo "To monitor the bot:"
echo "  tail -f output.log"
