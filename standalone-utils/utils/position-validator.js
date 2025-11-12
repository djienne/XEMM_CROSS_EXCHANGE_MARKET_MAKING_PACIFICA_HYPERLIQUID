import EventEmitter from 'events';

/**
 * Position Validator - Process 4
 *
 * Periodically validates that actual exchange positions match expected positions
 * from the internal cache. Detects and alerts on imbalances caused by failed hedges,
 * missed fills, or other issues.
 */
class PositionValidator extends EventEmitter {
  constructor(config, positionCache, connectors) {
    super();

    this.config = config;
    this.positionCache = positionCache;
    this.hyperliquid = connectors.hyperliquid;
    this.pacifica = connectors.pacifica;

    // Configuration
    this.checkIntervalMs = config.bot.positionCheckIntervalMs || 15000; // Default 15s
    this.imbalanceThreshold = config.bot.imbalanceThresholdUsd || 1.0; // Alert if >$1 imbalance

    // State
    this.running = false;
    this.intervalId = null;
    this.lastCheck = null;

    // Stats
    this.stats = {
      totalChecks: 0,
      imbalancesDetected: 0,
      lastCheckTime: null,
      lastImbalance: null
    };
  }

  /**
   * Initialize position cache with actual positions from exchanges
   * Should be called once at bot startup before trading begins
   */
  async initializeCache() {
    console.log('[POSITION VALIDATOR] Initializing position cache...');

    try {
      // Fetch actual positions from exchanges
      const actualPositions = await this.fetchActualPositions();

      // Sync the position cache with actual positions
      this.positionCache.syncWithActual(actualPositions);

      console.log('[POSITION VALIDATOR] ✅ Position cache initialized');
    } catch (error) {
      console.error('[POSITION VALIDATOR] Failed to initialize cache:', error.message);
      throw error;
    }
  }

  /**
   * Start the position validator
   */
  start() {
    if (this.running) {
      console.log('[POSITION VALIDATOR] Already running');
      return;
    }

    console.log(`[POSITION VALIDATOR] Starting (check interval: ${this.checkIntervalMs / 1000}s)`);
    this.running = true;

    // Run first check after 30 seconds to let bot stabilize
    setTimeout(() => {
      this.runCheck();

      // Then run periodically
      this.intervalId = setInterval(() => {
        this.runCheck();
      }, this.checkIntervalMs);
    }, 30000);
  }

  /**
   * Stop the position validator
   */
  stop() {
    if (!this.running) {
      return;
    }

    console.log('[POSITION VALIDATOR] Stopping...');
    this.running = false;

    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  /**
   * Manually trigger a position check (e.g., after fills/hedges)
   * @param {string} reason - Reason for the manual check
   */
  async triggerCheck(reason = 'manual') {
    if (!this.running) {
      return;
    }

    console.log(`[POSITION VALIDATOR] Triggered check (${reason})`);
    await this.runCheck();
  }

  /**
   * Run position validation check
   */
  async runCheck() {
    if (!this.running) {
      return;
    }

    try {
      this.stats.totalChecks++;
      this.stats.lastCheckTime = new Date().toISOString();
      this.lastCheck = Date.now();

      // Fetch actual positions from exchanges
      const actualPositions = await this.fetchActualPositions();

      // Get expected positions from cache
      const expectedPositions = this.positionCache.getSummary();

      // Compare and detect imbalances
      const imbalances = this.detectImbalances(actualPositions, expectedPositions);

      if (imbalances.length > 0) {
        this.handleImbalances(imbalances);
      }

    } catch (error) {
      console.error('[POSITION VALIDATOR] Check failed:', error.message);
    }
  }

  /**
   * Fetch actual positions from both exchanges
   */
  async fetchActualPositions() {
    try {
      // Fetch from Hyperliquid (REST API)
      const hlResponse = await fetch('https://api.hyperliquid.xyz/info', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type: 'clearinghouseState',
          user: process.env.HL_WALLET
        })
      });
      const hlState = await hlResponse.json();

      // Fetch from Pacifica (REST API)
      const pacPositions = await this.pacifica.getPositions();

      // Parse Hyperliquid positions
      const positions = {};
      for (const symbol of this.config.symbols) {
        positions[symbol] = {
          hyperliquid: 0,
          pacifica: 0,
          total: 0
        };
      }

      // Add Hyperliquid positions (szi = signed size: + is long, - is short)
      if (hlState.assetPositions) {
        for (const assetPos of hlState.assetPositions) {
          const coin = assetPos.position.coin;
          const szi = parseFloat(assetPos.position.szi);

          if (positions[coin] !== undefined) {
            positions[coin].hyperliquid = szi;
          }
        }
      }

      // Add Pacifica positions
      for (const pos of pacPositions) {
        const symbol = pos.symbol;
        if (positions[symbol] !== undefined) {
          // Pacifica stores side separately: 'long' = positive, 'short' = negative
          const signedAmount = pos.side === 'long' ? pos.amount : -pos.amount;
          positions[symbol].pacifica = signedAmount;
        }
      }

      // Calculate totals
      for (const symbol in positions) {
        positions[symbol].total = positions[symbol].hyperliquid + positions[symbol].pacifica;
      }

      return positions;

    } catch (error) {
      console.error('[POSITION VALIDATOR] Failed to fetch positions:', error.message);
      throw error;
    }
  }

  /**
   * Detect imbalances between actual and expected positions
   */
  detectImbalances(actualPositions, expectedPositions) {
    const imbalances = [];

    for (const symbol of this.config.symbols) {
      const actual = actualPositions[symbol];
      const expected = expectedPositions[symbol];

      // Calculate discrepancies
      const hlDiscrepancy = Math.abs(actual.hyperliquid - expected.hyperliquid);
      const pacDiscrepancy = Math.abs(actual.pacifica - expected.pacifica);
      const netDiscrepancy = Math.abs(actual.total - expected.net);

      // Estimate USD value (rough approximation)
      // We'd need price data for exact calculation, but this gives us magnitude
      const netNotional = Math.abs(actual.total) * 1.0; // Assume $1/unit as baseline

      // Check if discrepancy exceeds threshold
      if (netNotional > this.imbalanceThreshold || netDiscrepancy > 0.01) {
        imbalances.push({
          symbol,
          actual: {
            hyperliquid: actual.hyperliquid,
            pacifica: actual.pacifica,
            net: actual.total
          },
          expected: {
            hyperliquid: expected.hyperliquid,
            pacifica: expected.pacifica,
            net: expected.net
          },
          discrepancy: {
            hyperliquid: hlDiscrepancy,
            pacifica: pacDiscrepancy,
            net: netDiscrepancy
          },
          netNotional
        });
      }
    }

    return imbalances;
  }

  /**
   * Handle detected imbalances
   */
  handleImbalances(imbalances) {
    this.stats.imbalancesDetected += imbalances.length;
    this.stats.lastImbalance = {
      time: new Date().toISOString(),
      count: imbalances.length,
      symbols: imbalances.map(i => i.symbol)
    };

    console.log('\n' + '='.repeat(80));
    console.log('⚠️  POSITION IMBALANCE DETECTED');
    console.log('='.repeat(80));

    for (const imbalance of imbalances) {
      console.log(`\nSymbol: ${imbalance.symbol}`);
      console.log('-'.repeat(80));
      console.log('ACTUAL POSITIONS:');
      console.log(`  Hyperliquid:  ${imbalance.actual.hyperliquid.toFixed(4)}`);
      console.log(`  Pacifica:     ${imbalance.actual.pacifica.toFixed(4)}`);
      console.log(`  Net:          ${imbalance.actual.net.toFixed(4)}`);
      console.log('');
      console.log('EXPECTED POSITIONS (Cache):');
      console.log(`  Hyperliquid:  ${imbalance.expected.hyperliquid.toFixed(4)}`);
      console.log(`  Pacifica:     ${imbalance.expected.pacifica.toFixed(4)}`);
      console.log(`  Net:          ${imbalance.expected.net.toFixed(4)}`);
      console.log('');
      console.log('DISCREPANCY:');
      console.log(`  Hyperliquid:  ${imbalance.discrepancy.hyperliquid.toFixed(4)}`);
      console.log(`  Pacifica:     ${imbalance.discrepancy.pacifica.toFixed(4)}`);
      console.log(`  Net:          ${imbalance.discrepancy.net.toFixed(4)}`);
      console.log(`  Estimated USD: ~$${imbalance.netNotional.toFixed(2)}`);
    }

    console.log('\n' + '='.repeat(80));
    console.log('⚠️  ACTION REQUIRED: Review positions and investigate cause');
    console.log('    Use: node tests/close-all-positions.js to fix imbalances');
    console.log('='.repeat(80) + '\n');

    // Emit event so other components can react
    this.emit('imbalance', imbalances);
  }

  /**
   * Get validator statistics
   */
  getStats() {
    return {
      ...this.stats,
      running: this.running,
      checkInterval: this.checkIntervalMs
    };
  }
}

export default PositionValidator;
