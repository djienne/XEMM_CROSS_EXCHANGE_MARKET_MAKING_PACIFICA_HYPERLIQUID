#!/usr/bin/env node

/**
 * Scan All Opportunities - Universal Symbol Scanner
 *
 * READ-ONLY SCANNER - NO TRADING, NO CREDENTIALS REQUIRED
 *
 * Scans ALL available symbols across both exchanges for XEMM opportunities
 * - Discovers symbols from both Hyperliquid and Pacifica (public API)
 * - Finds common symbols between exchanges
 * - Subscribes to orderbooks and evaluates opportunities (read-only)
 * - Runs for configurable duration (default: 5 minutes)
 * - Shows summary of best pairs with most profitable opportunities
 *
 * Respects rate limits and subscription limits:
 * - Hyperliquid: Max 1000 subscriptions, 2000 msgs/min
 * - Pacifica: Max 20 subscriptions per connection
 *
 * IMPORTANT: This script only reads public market data. No orders are placed.
 *            No API credentials required.
 *
 * Usage:
 *   node utils/scan-all-opportunities.js
 */

import HyperliquidConnector from '../connectors/hyperliquid.js';
import PacificaConnector from '../connectors/pacifica.js';
import fs from 'fs';
import path from 'path';

// ============================================================================
// CONFIGURATION - Adjust these parameters as needed
// ============================================================================

const SCAN_DURATION_MINUTES = 5;  // How long to scan for opportunities (ADJUSTABLE)
const MAX_SYMBOLS = 20;            // Max symbols to scan (stay within subscription limits)
const EVALUATION_INTERVAL_MS = 100; // How often to evaluate opportunities (0.1 seconds)
const MIN_PROFITABILITY_BPS = 7;   // Minimum profitability threshold (basis points)
const MAX_PROFITABILITY_BPS = 30;  // Maximum profitability threshold (filter outliers)
const MAKER_FEE_BPS = 3;           // Maker fee (basis points) - Pacifica
const TAKER_FEE_BPS = 4.5;         // Taker fee (basis points) - Hyperliquid
const SLIPPAGE_BUFFER_BPS = 1;     // Slippage buffer (basis points)

// ============================================================================
// OPPORTUNITY STATISTICS TRACKER
// ============================================================================

class OpportunityStats {
  constructor() {
    this.symbolStats = {}; // { symbol: { count, totalProfit, maxProfit, avgProfit, opportunities: [] } }
  }

  recordOpportunity(symbol, profitabilityBps, opportunity) {
    if (!this.symbolStats[symbol]) {
      this.symbolStats[symbol] = {
        count: 0,
        totalProfit: 0,
        maxProfit: 0,
        minProfit: Infinity,
        opportunities: []
      };
    }

    const stats = this.symbolStats[symbol];
    stats.count++;
    stats.totalProfit += profitabilityBps;
    stats.maxProfit = Math.max(stats.maxProfit, profitabilityBps);
    stats.minProfit = Math.min(stats.minProfit, profitabilityBps);

    // Keep only top 5 opportunities per symbol
    stats.opportunities.push({
      profitabilityBps,
      timestamp: Date.now(),
      ...opportunity
    });
    stats.opportunities.sort((a, b) => b.profitabilityBps - a.profitabilityBps);
    if (stats.opportunities.length > 5) {
      stats.opportunities = stats.opportunities.slice(0, 5);
    }
  }

  getSummary() {
    const summary = [];
    for (const [symbol, stats] of Object.entries(this.symbolStats)) {
      summary.push({
        symbol,
        count: stats.count,
        avgProfit: stats.totalProfit / stats.count,
        maxProfit: stats.maxProfit,
        minProfit: stats.minProfit,
        topOpportunities: stats.opportunities
      });
    }
    // Sort by count (most opportunities first)
    summary.sort((a, b) => b.count - a.count);
    return summary;
  }
}

// ============================================================================
// OPPORTUNITY EVALUATOR
// ============================================================================

class OpportunityEvaluator {
  constructor(hyperliquid, pacifica, stats) {
    this.hyperliquid = hyperliquid;
    this.pacifica = pacifica;
    this.stats = stats;
  }

  /**
   * Evaluate XEMM opportunities for a symbol
   * @param {string} symbol - Symbol to evaluate
   */
  evaluateSymbol(symbol) {
    // Get bid/ask from both exchanges
    const hlBidAsk = this.hyperliquid.getBidAsk(symbol);
    const pacBidAsk = this.pacifica.getBidAsk(symbol);

    if (!hlBidAsk || !pacBidAsk) {
      return null; // No data available
    }

    if (!hlBidAsk.bid || !hlBidAsk.ask || !pacBidAsk.bid || !pacBidAsk.ask) {
      return null; // Incomplete data
    }

    // Calculate both directions
    const opportunities = [];

    // BID Opportunity: Buy on Pacifica (maker), Sell on Hyperliquid (taker)
    const bidGross = ((hlBidAsk.bid - pacBidAsk.bid) / pacBidAsk.bid) * 10000; // Convert to bps
    const bidNet = bidGross - MAKER_FEE_BPS - TAKER_FEE_BPS - SLIPPAGE_BUFFER_BPS;

    if (bidNet >= MIN_PROFITABILITY_BPS && bidNet <= MAX_PROFITABILITY_BPS) {
      opportunities.push({
        symbol,
        direction: 'BID',
        makerExchange: 'pacifica',
        takerExchange: 'hyperliquid',
        makerSide: 'buy',
        takerSide: 'sell',
        makerPrice: pacBidAsk.bid,
        takerPrice: hlBidAsk.bid,
        profitabilityBps: bidNet,
        grossBps: bidGross
      });
      this.stats.recordOpportunity(symbol, bidNet, opportunities[0]);
    }

    // ASK Opportunity: Sell on Pacifica (maker), Buy on Hyperliquid (taker)
    const askGross = ((pacBidAsk.ask - hlBidAsk.ask) / hlBidAsk.ask) * 10000; // Convert to bps
    const askNet = askGross - MAKER_FEE_BPS - TAKER_FEE_BPS - SLIPPAGE_BUFFER_BPS;

    if (askNet >= MIN_PROFITABILITY_BPS && askNet <= MAX_PROFITABILITY_BPS) {
      const opp = {
        symbol,
        direction: 'ASK',
        makerExchange: 'pacifica',
        takerExchange: 'hyperliquid',
        makerSide: 'sell',
        takerSide: 'buy',
        makerPrice: pacBidAsk.ask,
        takerPrice: hlBidAsk.ask,
        profitabilityBps: askNet,
        grossBps: askGross
      };
      opportunities.push(opp);
      this.stats.recordOpportunity(symbol, askNet, opp);
    }

    return opportunities.length > 0 ? opportunities : null;
  }
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

function center(text, width) {
  const padding = Math.max(0, width - text.length);
  const leftPad = Math.floor(padding / 2);
  const rightPad = padding - leftPad;
  return ' '.repeat(leftPad) + text + ' '.repeat(rightPad);
}

/**
 * Generate Markdown report from scan results
 */
function generateMarkdownReport(summary, evaluationCount, symbolsScanned, scanDuration) {
  const timestamp = new Date().toISOString();
  const totalOpps = summary.reduce((sum, s) => sum + s.count, 0);

  let md = `# XEMM Opportunity Scan Report\n\n`;
  md += `**Generated:** ${timestamp}\n\n`;
  md += `---\n\n`;

  // Scan Configuration
  md += `## Scan Configuration\n\n`;
  md += `| Parameter | Value |\n`;
  md += `|-----------|-------|\n`;
  md += `| Scan Duration | ${scanDuration} minutes |\n`;
  md += `| Evaluation Interval | ${EVALUATION_INTERVAL_MS}ms (${1000/EVALUATION_INTERVAL_MS} per second) |\n`;
  md += `| Min Profitability | ${MIN_PROFITABILITY_BPS} bps |\n`;
  md += `| Max Profitability | ${MAX_PROFITABILITY_BPS} bps |\n`;
  md += `| Symbols Scanned | ${symbolsScanned} |\n`;
  md += `| Total Evaluations | ${evaluationCount.toLocaleString()} |\n\n`;

  // Summary
  md += `## Summary\n\n`;
  md += `- **Symbols with Opportunities:** ${summary.length}\n`;
  md += `- **Total Opportunities Found:** ${totalOpps.toLocaleString()}\n`;
  md += `- **Average Opportunities per Symbol:** ${(totalOpps / summary.length).toFixed(1)}\n\n`;

  md += `---\n\n`;

  // Top Symbols by Opportunity Count
  md += `## Top Symbols by Opportunity Count\n\n`;
  md += `| Rank | Symbol | Count | Avg Profit (bps) | Max Profit (bps) | Min Profit (bps) |\n`;
  md += `|------|--------|-------|------------------|------------------|------------------|\n`;

  const top10 = summary.slice(0, 10);
  top10.forEach((s, idx) => {
    md += `| ${idx + 1} | **${s.symbol}** | ${s.count.toLocaleString()} | ${s.avgProfit.toFixed(2)} | ${s.maxProfit.toFixed(2)} | ${s.minProfit.toFixed(2)} |\n`;
  });

  md += `\n`;

  // Top Symbols by Average Profitability
  md += `## Top Symbols by Average Profitability\n\n`;
  md += `| Rank | Symbol | Avg Profit (bps) | Count | Max Profit (bps) |\n`;
  md += `|------|--------|------------------|-------|------------------|\n`;

  const topByProfit = [...summary].sort((a, b) => b.avgProfit - a.avgProfit).slice(0, 5);
  topByProfit.forEach((s, idx) => {
    md += `| ${idx + 1} | **${s.symbol}** | ${s.avgProfit.toFixed(2)} | ${s.count.toLocaleString()} | ${s.maxProfit.toFixed(2)} |\n`;
  });

  md += `\n`;

  // Top Individual Opportunities
  md += `## Top 3 Individual Opportunities\n\n`;

  const allOpps = [];
  for (const s of summary) {
    for (const opp of s.topOpportunities) {
      allOpps.push(opp);
    }
  }
  allOpps.sort((a, b) => b.profitabilityBps - a.profitabilityBps);
  const top3Opps = allOpps.slice(0, 3);

  top3Opps.forEach((opp, idx) => {
    md += `### ${idx + 1}. ${opp.symbol} (${opp.direction})\n\n`;
    md += `- **Net Profit:** ${opp.profitabilityBps.toFixed(2)} bps\n`;
    md += `- **Gross Profit:** ${opp.grossBps.toFixed(2)} bps\n`;
    md += `- **Maker Side:** ${opp.makerSide.toUpperCase()} @ $${opp.makerPrice.toFixed(4)} on ${opp.makerExchange}\n`;
    md += `- **Taker Side:** ${opp.takerSide.toUpperCase()} @ $${opp.takerPrice.toFixed(4)} on ${opp.takerExchange}\n\n`;
  });

  // Full Summary Table
  md += `---\n\n`;
  md += `## Full Summary - All Symbols with Opportunities\n\n`;
  md += `| Symbol | Count | Avg Profit (bps) | Max Profit (bps) | Min Profit (bps) |\n`;
  md += `|--------|-------|------------------|------------------|------------------|\n`;

  summary.forEach(s => {
    md += `| ${s.symbol} | ${s.count.toLocaleString()} | ${s.avgProfit.toFixed(2)} | ${s.maxProfit.toFixed(2)} | ${s.minProfit.toFixed(2)} |\n`;
  });

  md += `\n---\n\n`;
  md += `*Report generated by XEMM Opportunity Scanner*\n`;

  return md;
}

/**
 * Save markdown report to file
 */
function saveMarkdownReport(summary, evaluationCount, symbolsScanned, scanDuration) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
  const reportsDir = './reports';

  // Create reports directory if it doesn't exist
  if (!fs.existsSync(reportsDir)) {
    fs.mkdirSync(reportsDir, { recursive: true });
  }

  const filename = path.join(reportsDir, `scan_report_${timestamp}.md`);
  const markdown = generateMarkdownReport(summary, evaluationCount, symbolsScanned, scanDuration);

  fs.writeFileSync(filename, markdown, 'utf8');

  return filename;
}

// ============================================================================
// MAIN SCANNER
// ============================================================================

async function main() {
  console.log('\n' + '='.repeat(80));
  console.log(center('XEMM OPPORTUNITY SCANNER - ALL SYMBOLS', 80));
  console.log('='.repeat(80));
  console.log(`\nScan Duration:      ${SCAN_DURATION_MINUTES} minutes`);
  console.log(`Max Symbols:        ${MAX_SYMBOLS}`);
  console.log(`Min Profitability:  ${MIN_PROFITABILITY_BPS} bps`);
  console.log(`Max Profitability:  ${MAX_PROFITABILITY_BPS} bps`);
  console.log(`Evaluation Every:   ${EVALUATION_INTERVAL_MS}ms`);
  console.log('='.repeat(80) + '\n');

  // Initialize connectors
  const hyperliquid = new HyperliquidConnector({ testnet: false });
  const pacifica = new PacificaConnector({ testnet: false });
  const stats = new OpportunityStats();
  const evaluator = new OpportunityEvaluator(hyperliquid, pacifica, stats);

  try {
    // Step 1: Connect to both exchanges
    console.log('[1/5] Connecting to exchanges...');
    await hyperliquid.connect();
    await pacifica.connect();
    console.log('✅ Connected to Hyperliquid and Pacifica\n');

    // Step 2: Fetch available symbols from both exchanges
    console.log('[2/5] Fetching available symbols...');

    // Get Hyperliquid symbols
    const hlMeta = await hyperliquid.getMeta();
    const hlSymbols = new Set(hlMeta.universe.map(u => u.name));
    console.log(`  Hyperliquid: ${hlSymbols.size} symbols`);

    // Get Pacifica symbols
    const pacMeta = await pacifica.getMarketInfo();
    const pacSymbols = new Set(pacMeta.keys());
    console.log(`  Pacifica: ${pacSymbols.size} symbols`);

    // Find common symbols
    const commonSymbols = [...hlSymbols].filter(s => pacSymbols.has(s));
    console.log(`✅ Found ${commonSymbols.length} common symbols\n`);

    if (commonSymbols.length === 0) {
      console.error('❌ No common symbols found between exchanges');
      process.exit(1);
    }

    // Step 3: Select symbols to scan (limit to MAX_SYMBOLS)
    let symbolsToScan = commonSymbols;
    if (commonSymbols.length > MAX_SYMBOLS) {
      console.log(`[3/5] Limiting to ${MAX_SYMBOLS} symbols (subscription limit)...`);
      // TODO: Could prioritize by volume/liquidity if that data is available
      symbolsToScan = commonSymbols.slice(0, MAX_SYMBOLS);
    } else {
      console.log(`[3/5] Scanning all ${commonSymbols.length} common symbols...`);
    }

    console.log('Symbols to scan:', symbolsToScan.join(', '));
    console.log('');

    // Step 4: Subscribe to orderbooks
    console.log('[4/5] Subscribing to orderbooks...');
    for (const symbol of symbolsToScan) {
      try {
        await hyperliquid.subscribeOrderbook(symbol);
        await pacifica.subscribeOrderbook(symbol);
      } catch (error) {
        console.error(`  ⚠️  Failed to subscribe to ${symbol}: ${error.message}`);
      }
    }

    // Wait for initial orderbook data
    console.log('Waiting 3 seconds for orderbook data...');
    await new Promise(resolve => setTimeout(resolve, 3000));
    console.log('✅ Subscriptions complete\n');

    // Step 5: Scan for opportunities
    console.log('[5/5] Scanning for opportunities...');
    console.log(`Will run for ${SCAN_DURATION_MINUTES} minutes...\n`);

    const startTime = Date.now();
    const endTime = startTime + (SCAN_DURATION_MINUTES * 60 * 1000);
    let evaluationCount = 0;

    const evaluationTimer = setInterval(() => {
      evaluationCount++;
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      const remaining = Math.max(0, Math.ceil((endTime - Date.now()) / 1000));

      // Evaluate all symbols
      let opportunitiesFound = 0;
      for (const symbol of symbolsToScan) {
        const opps = evaluator.evaluateSymbol(symbol);
        if (opps) {
          opportunitiesFound += opps.length;
        }
      }

      // Progress update every 10 evaluations
      if (evaluationCount % 10 === 0) {
        console.log(`[${elapsed}s] Evaluated ${evaluationCount} times, found ${Object.keys(stats.symbolStats).length} symbols with opportunities (${remaining}s remaining)`);
      }
    }, EVALUATION_INTERVAL_MS);

    // Wait for scan duration
    await new Promise(resolve => setTimeout(resolve, SCAN_DURATION_MINUTES * 60 * 1000));

    // Stop evaluation
    clearInterval(evaluationTimer);

    // Step 6: Display results
    console.log('\n' + '='.repeat(80));
    console.log(center('SCAN COMPLETE - RESULTS', 80));
    console.log('='.repeat(80));

    const summary = stats.getSummary();

    if (summary.length === 0) {
      console.log('\n❌ No opportunities found during scan period\n');
    } else {
      console.log(`\n✅ Found opportunities in ${summary.length} symbols\n`);

      // Top 10 symbols by opportunity count
      console.log('─'.repeat(80));
      console.log('TOP SYMBOLS BY OPPORTUNITY COUNT');
      console.log('─'.repeat(80));
      console.log(`${'Symbol'.padEnd(10)} ${'Count'.padStart(8)} ${'Avg Profit'.padStart(12)} ${'Max Profit'.padStart(12)} ${'Min Profit'.padStart(12)}`);
      console.log('─'.repeat(80));

      const top10 = summary.slice(0, 10);
      for (const s of top10) {
        console.log(
          `${s.symbol.padEnd(10)} ${s.count.toString().padStart(8)} ${(s.avgProfit.toFixed(2) + ' bps').padStart(12)} ${(s.maxProfit.toFixed(2) + ' bps').padStart(12)} ${(s.minProfit.toFixed(2) + ' bps').padStart(12)}`
        );
      }

      // Top 5 by average profitability
      console.log('\n' + '─'.repeat(80));
      console.log('TOP SYMBOLS BY AVERAGE PROFITABILITY');
      console.log('─'.repeat(80));
      console.log(`${'Symbol'.padEnd(10)} ${'Avg Profit'.padStart(12)} ${'Count'.padStart(8)} ${'Max Profit'.padStart(12)}`);
      console.log('─'.repeat(80));

      const topByProfit = [...summary].sort((a, b) => b.avgProfit - a.avgProfit).slice(0, 5);
      for (const s of topByProfit) {
        console.log(
          `${s.symbol.padEnd(10)} ${(s.avgProfit.toFixed(2) + ' bps').padStart(12)} ${s.count.toString().padStart(8)} ${(s.maxProfit.toFixed(2) + ' bps').padStart(12)}`
        );
      }

      // Top 3 individual opportunities
      console.log('\n' + '─'.repeat(80));
      console.log('TOP 3 INDIVIDUAL OPPORTUNITIES');
      console.log('─'.repeat(80));

      const allOpps = [];
      for (const s of summary) {
        for (const opp of s.topOpportunities) {
          allOpps.push(opp);
        }
      }
      allOpps.sort((a, b) => b.profitabilityBps - a.profitabilityBps);
      const top3Opps = allOpps.slice(0, 3);

      for (let i = 0; i < top3Opps.length; i++) {
        const opp = top3Opps[i];
        console.log(`\n#${i + 1}: ${opp.symbol} (${opp.direction})`);
        console.log(`  Net Profit:    ${opp.profitabilityBps.toFixed(2)} bps`);
        console.log(`  Gross Profit:  ${opp.grossBps.toFixed(2)} bps`);
        console.log(`  Maker Side:    ${opp.makerSide.toUpperCase()} @ $${opp.makerPrice.toFixed(4)} on ${opp.makerExchange}`);
        console.log(`  Taker Side:    ${opp.takerSide.toUpperCase()} @ $${opp.takerPrice.toFixed(4)} on ${opp.takerExchange}`);
      }

      // Full summary table
      console.log('\n' + '─'.repeat(80));
      console.log('FULL SUMMARY (All symbols with opportunities)');
      console.log('─'.repeat(80));
      console.log(`${'Symbol'.padEnd(10)} ${'Count'.padStart(8)} ${'Avg'.padStart(10)} ${'Max'.padStart(10)} ${'Min'.padStart(10)}`);
      console.log('─'.repeat(80));

      for (const s of summary) {
        console.log(
          `${s.symbol.padEnd(10)} ${s.count.toString().padStart(8)} ${s.avgProfit.toFixed(2).padStart(10)} ${s.maxProfit.toFixed(2).padStart(10)} ${s.minProfit.toFixed(2).padStart(10)}`
        );
      }
    }

    console.log('\n' + '='.repeat(80));
    console.log(`Total Evaluations: ${evaluationCount}`);
    console.log(`Symbols Scanned: ${symbolsToScan.length}`);
    console.log(`Symbols with Opportunities: ${summary.length}`);
    console.log(`Total Opportunities Found: ${summary.reduce((sum, s) => sum + s.count, 0)}`);
    console.log('='.repeat(80) + '\n');

    // Save markdown report
    if (summary.length > 0) {
      try {
        const reportFile = saveMarkdownReport(summary, evaluationCount, symbolsToScan.length, SCAN_DURATION_MINUTES);
        console.log(`📄 Report saved: ${reportFile}\n`);
      } catch (error) {
        console.error(`⚠️  Failed to save report: ${error.message}`);
      }
    }

  } catch (error) {
    console.error('\n❌ Error:', error.message);
    console.error(error.stack);
  } finally {
    // Cleanup
    console.log('Disconnecting...');
    hyperliquid.disconnect();
    pacifica.disconnect();
    console.log('✅ Done\n');
  }
}

// Run scanner
main().catch(console.error);
