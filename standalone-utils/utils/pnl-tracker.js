import fs from 'fs/promises';
import path from 'path';

/**
 * PnL Tracker - Records and analyzes trade profitability
 *
 * Tracks completed maker-taker trades with:
 * - Gross profit (price spread)
 * - Trading fees (maker + taker)
 * - Net profit (after fees)
 * - Persistent storage to JSON file
 */
export default class PnLTracker {
  constructor(config = {}) {
    this.config = config;
    this.filePath = config.pnlFilePath || 'pnl_history.json';

    // Fee rates (basis points -> decimal)
    this.makerFeeBps = 1.5; // Pacifica: 1.5 bps
    this.takerFeeBps = 4.5; // Hyperliquid: 4.5 bps
    this.makerFeeRate = this.makerFeeBps / 10000; // 0.00015 (1.5 bps)
    this.takerFeeRate = this.takerFeeBps / 10000; // 0.00045 (4.5 bps)

    // In-memory trade history
    this.trades = [];

    // Summary statistics
    this.stats = {
      totalTrades: 0,
      totalGrossProfit: 0,
      totalNetProfit: 0,
      totalMakerFees: 0,
      totalTakerFees: 0,
      totalFees: 0,
      winningTrades: 0,
      losingTrades: 0,
      avgNetProfit: 0,
      avgGrossProfit: 0,
      winRate: 0
    };
  }

  /**
   * Initialize - load existing trade history from disk
   */
  async initialize() {
    try {
      const data = await fs.readFile(this.filePath, 'utf8');
      this.trades = JSON.parse(data);

      // Recalculate statistics
      this.recalculateStats();

      console.log(`[PNL] Loaded ${this.trades.length} historical trades from ${this.filePath}`);
    } catch (error) {
      if (error.code === 'ENOENT') {
        console.log(`[PNL] No existing history found, starting fresh`);
        this.trades = [];
      } else {
        console.error('[PNL] Failed to load trade history:', error.message);
        this.trades = [];
      }
    }
  }

  /**
   * Record a completed trade
   *
   * @param {object} tradeData - Trade information
   * @param {string} tradeData.symbol - Symbol traded
   * @param {string} tradeData.makerExchange - Exchange where limit order was placed
   * @param {string} tradeData.takerExchange - Exchange where hedge was executed
   * @param {string} tradeData.makerSide - 'buy' or 'sell' on maker
   * @param {string} tradeData.takerSide - 'buy' or 'sell' on taker
   * @param {number} tradeData.makerQty - Quantity filled on maker
   * @param {number} tradeData.makerPrice - Average price on maker
   * @param {number} tradeData.takerQty - Quantity executed on taker
   * @param {number} tradeData.takerPrice - Average price on taker
   * @param {number} tradeData.latencyMs - Time from fill to hedge completion
   */
  async recordTrade(tradeData) {
    const {
      symbol,
      makerExchange,
      takerExchange,
      makerSide,
      takerSide,
      makerQty,
      makerPrice,
      takerQty,
      takerPrice,
      latencyMs = 0
    } = tradeData;

    // Calculate notionals
    const makerNotional = makerQty * makerPrice;
    const takerNotional = takerQty * takerPrice;

    // Calculate fees
    const makerFee = makerNotional * this.makerFeeRate;
    const takerFee = takerNotional * this.takerFeeRate;
    const totalFees = makerFee + takerFee;

    // Calculate profit
    let grossProfit;
    if (makerSide === 'buy') {
      // Bought on maker, sold on taker
      grossProfit = takerNotional - makerNotional;
    } else {
      // Sold on maker, bought on taker
      grossProfit = makerNotional - takerNotional;
    }

    const netProfit = grossProfit - totalFees;

    // Construct trade record
    const trade = {
      timestamp: Date.now(),
      date: new Date().toISOString(),
      symbol,
      makerExchange,
      takerExchange,
      makerSide,
      takerSide,
      makerQty,
      makerPrice,
      makerNotional,
      makerFee,
      takerQty,
      takerPrice,
      takerNotional,
      takerFee,
      totalFees,
      grossProfit,
      netProfit,
      netProfitBps: (netProfit / makerNotional) * 10000,
      latencyMs,
      isWinner: netProfit > 0
    };

    // Add to history
    this.trades.push(trade);

    // Update statistics
    this.updateStats(trade);

    // Save to disk
    await this.save();

    // Log trade
    this.logTrade(trade);

    return trade;
  }

  /**
   * Update running statistics
   */
  updateStats(trade) {
    this.stats.totalTrades++;
    this.stats.totalGrossProfit += trade.grossProfit;
    this.stats.totalNetProfit += trade.netProfit;
    this.stats.totalMakerFees += trade.makerFee;
    this.stats.totalTakerFees += trade.takerFee;
    this.stats.totalFees += trade.totalFees;

    if (trade.netProfit > 0) {
      this.stats.winningTrades++;
    } else {
      this.stats.losingTrades++;
    }

    this.stats.avgNetProfit = this.stats.totalNetProfit / this.stats.totalTrades;
    this.stats.avgGrossProfit = this.stats.totalGrossProfit / this.stats.totalTrades;
    this.stats.winRate = this.stats.winningTrades / this.stats.totalTrades;
  }

  /**
   * Recalculate statistics from scratch (after loading from disk)
   */
  recalculateStats() {
    this.stats = {
      totalTrades: 0,
      totalGrossProfit: 0,
      totalNetProfit: 0,
      totalMakerFees: 0,
      totalTakerFees: 0,
      totalFees: 0,
      winningTrades: 0,
      losingTrades: 0,
      avgNetProfit: 0,
      avgGrossProfit: 0,
      winRate: 0
    };

    for (const trade of this.trades) {
      this.updateStats(trade);
    }
  }

  /**
   * Save trade history to disk
   */
  async save() {
    try {
      await fs.writeFile(this.filePath, JSON.stringify(this.trades, null, 2), 'utf8');
    } catch (error) {
      console.error('[PNL] Failed to save trade history:', error.message);
    }
  }

  /**
   * Log trade to console
   */
  logTrade(trade) {
    const profitSign = trade.netProfit >= 0 ? '✅' : '❌';
    const profitColor = trade.netProfit >= 0 ? 'green' : 'red';

    console.log('\n' + '='.repeat(80));
    console.log(`${profitSign} TRADE COMPLETED - ${trade.symbol}`);
    console.log('-'.repeat(80));
    console.log(`Maker (${trade.makerExchange}):  ${trade.makerSide.toUpperCase().padEnd(4)} ${trade.makerQty.toFixed(4)} @ $${trade.makerPrice.toFixed(4)} = $${trade.makerNotional.toFixed(2)}`);
    console.log(`Taker (${trade.takerExchange}):  ${trade.takerSide.toUpperCase().padEnd(4)} ${trade.takerQty.toFixed(4)} @ $${trade.takerPrice.toFixed(4)} = $${trade.takerNotional.toFixed(2)}`);
    console.log('-'.repeat(80));
    console.log(`Gross Profit:   $${trade.grossProfit.toFixed(4)}`);
    console.log(`Maker Fee:     -$${trade.makerFee.toFixed(4)} (${this.makerFeeBps} bps)`);
    console.log(`Taker Fee:     -$${trade.takerFee.toFixed(4)} (${this.takerFeeBps} bps)`);
    console.log(`Total Fees:    -$${trade.totalFees.toFixed(4)}`);
    console.log('-'.repeat(80));
    console.log(`Net Profit:     $${trade.netProfit.toFixed(4)} (${trade.netProfitBps.toFixed(2)} bps)`);
    console.log(`Latency:        ${trade.latencyMs.toFixed(0)}ms`);
    console.log('='.repeat(80) + '\n');
  }

  /**
   * Get summary statistics
   */
  getSummary() {
    return {
      ...this.stats,
      totalTradesCount: this.trades.length,
      lastTradeTime: this.trades.length > 0 ? this.trades[this.trades.length - 1].date : null
    };
  }

  /**
   * Get recent trades
   * @param {number} count - Number of recent trades to return
   */
  getRecentTrades(count = 10) {
    return this.trades.slice(-count);
  }

  /**
   * Get trades for specific symbol
   * @param {string} symbol - Symbol to filter by
   */
  getTradesBySymbol(symbol) {
    return this.trades.filter(t => t.symbol === symbol);
  }

  /**
   * Get trades within time range
   * @param {number} startTime - Start timestamp
   * @param {number} endTime - End timestamp
   */
  getTradesByTimeRange(startTime, endTime) {
    return this.trades.filter(t => t.timestamp >= startTime && t.timestamp <= endTime);
  }

  /**
   * Log summary to console
   */
  logSummary() {
    console.log('\n' + '='.repeat(80));
    console.log('PnL Summary:');
    console.log('-'.repeat(80));
    console.log(`Total Trades:      ${this.stats.totalTrades}`);
    console.log(`Winning Trades:    ${this.stats.winningTrades} (${(this.stats.winRate * 100).toFixed(1)}%)`);
    console.log(`Losing Trades:     ${this.stats.losingTrades}`);
    console.log('-'.repeat(80));
    console.log(`Total Gross:       $${this.stats.totalGrossProfit.toFixed(2)}`);
    console.log(`Total Fees:       -$${this.stats.totalFees.toFixed(2)}`);
    console.log(`  Maker Fees:     -$${this.stats.totalMakerFees.toFixed(2)}`);
    console.log(`  Taker Fees:     -$${this.stats.totalTakerFees.toFixed(2)}`);
    console.log('-'.repeat(80));
    console.log(`Total Net Profit:  $${this.stats.totalNetProfit.toFixed(2)}`);
    console.log(`Avg Net Profit:    $${this.stats.avgNetProfit.toFixed(4)}`);
    console.log('='.repeat(80) + '\n');
  }
}
