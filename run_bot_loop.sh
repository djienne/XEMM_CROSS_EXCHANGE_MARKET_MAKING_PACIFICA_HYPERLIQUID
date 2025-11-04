#!/bin/bash

# XEMM Bot Continuous Runner
# Runs the bot in a loop, waiting 20 seconds between cycles
# Each cycle completes one fill + hedge, then the bot restarts

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

# Build mode
if [ "$USE_RELEASE" = true ]; then
    BUILD_MODE="--release"
    BINARY_PATH="./target/release/xemm_rust"
else
    BUILD_MODE=""
    BINARY_PATH="./target/debug/xemm_rust"
fi

echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  XEMM Bot - Continuous Runner${NC}"
echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
echo -e "${BLUE}Build mode:${NC} $([ "$USE_RELEASE" = true ] && echo "Release (optimized)" || echo "Debug")"
echo -e "${BLUE}Delay between cycles:${NC} ${DELAY_SECONDS}s"
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
echo ""

# Trap Ctrl+C to exit gracefully
trap 'echo -e "\n${YELLOW}Stopping bot loop...${NC}"; exit 0' INT TERM

# Build the bot once before starting
echo -e "${CYAN}Building bot...${NC}"
if cargo build $BUILD_MODE; then
    echo -e "${GREEN}✓ Build successful${NC}"
    echo ""
else
    echo -e "${RED}✗ Build failed${NC}"
    exit 1
fi

# Cycle counter
CYCLE=1

# Main loop
while true; do
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  Starting Cycle #${CYCLE}${NC}"
    echo -e "${CYAN}  $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
    echo ""

    # Run the bot
    if $BINARY_PATH; then
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
