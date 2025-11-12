/**
 * NetPositionCache - Real-time position tracking across exchanges
 *
 * Tracks positions on both Hyperliquid and Pacifica exchanges
 * - Real-time updates via delta applications (fills)
 * - Thread-safe by design (Node.js single-threaded event loop)
 */
export default class NetPositionCache {
  constructor(hyperliquidConnector, pacificaConnector, config) {
    this.hyperliquid = hyperliquidConnector;
    this.pacifica = pacificaConnector;
    this.config = config;

    // Position state: { symbol: { hyperliquid: qty, pacifica: qty } }
    this.positions = {};

    // Initialize positions for configured symbols
    for (const symbol of config.symbols) {
      this.positions[symbol] = {
        hyperliquid: 0,
        pacifica: 0
      };
    }
  }

  /**
   * Sync cache with actual positions from exchanges
   * Used by Position Validator to initialize or reset cache
   * @param {object} actualPositions - Positions fetched from exchanges
   */
  syncWithActual(actualPositions) {
    console.log('[POSITION CACHE] Syncing with actual positions...');

    for (const symbol in actualPositions) {
      if (this.positions[symbol]) {
        const actual = actualPositions[symbol];

        // Update cache with actual positions
        const oldHl = this.positions[symbol].hyperliquid;
        const oldPac = this.positions[symbol].pacifica;

        this.positions[symbol].hyperliquid = actual.hyperliquid;
        this.positions[symbol].pacifica = actual.pacifica;

        // Log if there was a significant change
        if (Math.abs(oldHl - actual.hyperliquid) > 0.01 || Math.abs(oldPac - actual.pacifica) > 0.01) {
          console.log(`[POSITION CACHE] Synced ${symbol}: HL ${oldHl.toFixed(4)} → ${actual.hyperliquid.toFixed(4)}, PAC ${oldPac.toFixed(4)} → ${actual.pacifica.toFixed(4)}`);
        }
      }
    }

    console.log('[POSITION CACHE] Sync complete');
  }

  /**
   * Get net position across both exchanges for a symbol
   * @param {string} symbol - Symbol to check
   * @returns {number} Net position (positive = long, negative = short)
   */
  getNetPosition(symbol) {
    const positions = this.positions[symbol] || { hyperliquid: 0, pacifica: 0 };
    return positions.hyperliquid + positions.pacifica;
  }

  /**
   * Get position on specific exchange
   * @param {string} symbol - Symbol to check
   * @param {string} exchange - 'hyperliquid' or 'pacifica'
   * @returns {number} Position quantity
   */
  getPosition(symbol, exchange) {
    return this.positions[symbol]?.[exchange] || 0;
  }

  /**
   * Get full position state for a symbol
   * @param {string} symbol - Symbol to check
   * @returns {object} { hyperliquid: qty, pacifica: qty }
   */
  getState(symbol) {
    return this.positions[symbol] || { hyperliquid: 0, pacifica: 0 };
  }

  /**
   * Apply position delta (from fill or trade)
   * @param {string} symbol - Symbol
   * @param {string} exchange - 'hyperliquid' or 'pacifica'
   * @param {number} deltaQty - Quantity change (positive = buy, negative = sell)
   */
  applyDelta(symbol, exchange, deltaQty) {
    if (!this.positions[symbol]) {
      this.positions[symbol] = { hyperliquid: 0, pacifica: 0 };
    }

    const oldPosition = this.positions[symbol][exchange];
    this.positions[symbol][exchange] += deltaQty;

    console.log(
      `[POSITION] ${symbol} ${exchange}: ${oldPosition.toFixed(4)} → ${this.positions[symbol][exchange].toFixed(4)} (Δ ${deltaQty > 0 ? '+' : ''}${deltaQty.toFixed(4)})`
    );
  }

  /**
   * Set absolute position (from REST API fetch)
   * @param {string} symbol - Symbol
   * @param {string} exchange - 'hyperliquid' or 'pacifica'
   * @param {number} quantity - Absolute position quantity
   */
  setPosition(symbol, exchange, quantity) {
    if (!this.positions[symbol]) {
      this.positions[symbol] = { hyperliquid: 0, pacifica: 0 };
    }

    const oldPosition = this.positions[symbol][exchange];
    this.positions[symbol][exchange] = quantity;

    if (Math.abs(oldPosition - quantity) > 0.01) {
      console.log(
        `[POSITION SYNC] ${symbol} ${exchange}: ${oldPosition.toFixed(4)} → ${quantity.toFixed(4)} (REST API)`
      );
    }
  }

  /**
   * Get net position in USD terms
   * @param {string} symbol - Symbol
   * @param {number} currentPrice - Current price
   * @returns {number} Net position notional value (absolute)
   */
  getNetPositionNotional(symbol, currentPrice) {
    const netQty = this.getNetPosition(symbol);
    return Math.abs(netQty * currentPrice);
  }

  /**
   * Get summary of all positions
   * @returns {object} Summary with net positions
   */
  getSummary() {
    const summary = {};

    for (const [symbol, pos] of Object.entries(this.positions)) {
      const netQty = pos.hyperliquid + pos.pacifica;

      summary[symbol] = {
        hyperliquid: pos.hyperliquid,
        pacifica: pos.pacifica,
        net: netQty,
        imbalanced: Math.abs(netQty) > 0.01
      };
    }

    return summary;
  }

  /**
   * Log current positions
   */
  logPositions() {
    console.log('\n' + '='.repeat(80));
    console.log('Current Positions:');
    console.log('-'.repeat(80));

    for (const [symbol, pos] of Object.entries(this.positions)) {
      const netQty = pos.hyperliquid + pos.pacifica;
      const netStr = netQty >= 0 ? `+${netQty.toFixed(4)}` : netQty.toFixed(4);

      console.log(
        `${symbol.padEnd(12)} | HL: ${pos.hyperliquid.toFixed(4).padStart(10)} | PAC: ${pos.pacifica.toFixed(4).padStart(10)} | NET: ${netStr.padStart(10)}`
      );
    }

    console.log('='.repeat(80) + '\n');
  }
}
