#!/bin/bash
# Kill all XEMM bot processes
# This kills:
# 1. The xemm_rust binary (single run or loop)
# 2. The run_bot_loop_cargo.sh script (infinite loop)
# 3. Any cargo processes spawned by the loop

echo "Stopping all XEMM bot processes..."

# Kill the bot loop script (if running)
pkill -f "run_bot_loop_cargo.sh" 2>/dev/null && echo "✓ Stopped run_bot_loop_cargo.sh"

# Kill any cargo processes running xemm_rust
pkill -f "cargo run.*xemm" 2>/dev/null && echo "✓ Stopped cargo processes"

# Kill the xemm_rust binary
killall xemm_rust 2>/dev/null && echo "✓ Stopped xemm_rust binary"

echo "All XEMM processes stopped."
