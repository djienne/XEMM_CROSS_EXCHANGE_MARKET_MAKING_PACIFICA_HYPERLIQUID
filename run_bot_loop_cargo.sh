#!/bin/bash

# XEMM Bot Continuous Runner (using cargo run)
# Runs the bot in a loop using cargo run, waiting 20 seconds between cycles
# Each cycle completes one fill + hedge, then the bot restarts
#
# This version rebuilds on each run (slower but picks up code changes)

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
DELAY_SECONDS=20
USE_RELEASE=true  # Set to false for debug builds

# Build flag
if [ "$USE_RELEASE" = true ]; then
    BUILD_FLAG="--release"
    BUILD_MODE="Release (optimized)"
else
    BUILD_FLAG=""
    BUILD_MODE="Debug"
fi

# Check if xemm_rust is already running
if pgrep -f "xemm_rust" > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  WARNING: xemm_rust process is already running!${NC}"
    echo "Please stop the existing process before starting a new loop."
    echo ""
    echo "To view running processes:"
    echo "  ps aux | grep xemm_rust"
    echo ""
    echo "To stop all bot processes:"
    echo "  bash kill_process.sh"
    exit 1
fi

# Check if another instance of this loop script is already running
SCRIPT_NAME=$(basename "$0")
RUNNING_INSTANCES=$(pgrep -f "$SCRIPT_NAME" | grep -v $$ | wc -l)
if [ "$RUNNING_INSTANCES" -gt 0 ]; then
    echo -e "${YELLOW}⚠️  WARNING: Another instance of $SCRIPT_NAME is already running!${NC}"
    echo "Please stop the existing loop before starting a new one."
    echo ""
    echo "To view running loop processes:"
    echo "  ps aux | grep $SCRIPT_NAME"
    echo ""
    echo "To stop all bot processes:"
    echo "  bash kill_process.sh"
    exit 1
fi

echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  XEMM Bot - Continuous Runner (cargo run)${NC}"
echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
echo -e "${BLUE}Build mode:${NC} ${BUILD_MODE}"
echo -e "${BLUE}Delay between cycles:${NC} ${DELAY_SECONDS}s"
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
echo ""

# Trap Ctrl+C to exit gracefully
trap 'echo -e "\n${YELLOW}Stopping bot loop...${NC}"; exit 0' INT TERM

# Cycle counter
CYCLE=1

# Main loop
while true; do
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  Starting Cycle #${CYCLE}${NC}"
    echo -e "${CYAN}  $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
    echo ""

    # Run the bot with cargo run
    if cargo run $BUILD_FLAG; then
        echo ""
        echo -e "${GREEN}✓ Cycle #${CYCLE} completed successfully${NC}"
    else
        EXIT_CODE=$?
        echo ""
        echo -e "${RED}✗ Cycle #${CYCLE} failed with exit code ${EXIT_CODE}${NC}"
        echo -e "${YELLOW}Continuing to next cycle after delay...${NC}"
    fi

    # Increment cycle counter
    ((CYCLE++))

    # Wait before next cycle
    echo ""
    echo -e "${BLUE}Waiting ${DELAY_SECONDS} seconds before next cycle...${NC}"
    sleep $DELAY_SECONDS
    echo ""
done
