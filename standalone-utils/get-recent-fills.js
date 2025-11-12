import HyperliquidConnector from './connectors/hyperliquid.js';
import PacificaConnector from './connectors/pacifica.js';
import dotenv from 'dotenv';

// Load .env from current directory
dotenv.config();

// Helper function to format a date timestamp
function formatTime(timestamp) {
  const date = new Date(timestamp);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
}

// Helper function to color PnL values
function colorPnL(pnl) {
  const pnlNum = parseFloat(pnl);
  if (pnlNum > 0) return `\x1b[32m+${pnl}\x1b[0m`; // Green for profit
  if (pnlNum < 0) return `\x1b[31m${pnl}\x1b[0m`; // Red for loss
  return pnl;
}

// Fetch Hyperliquid fills
async function getHyperliquidFills(limit = 10) {
  const hl = new HyperliquidConnector();

  try {
    const response = await fetch(hl.restUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        type: 'userFills',
        user: hl.wallet
      })
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const fills = await response.json();
    return fills.slice(0, limit);
  } catch (error) {
    console.error('[Hyperliquid] Error fetching fills:', error.message);
    return [];
  }
}

// Fetch Pacifica fills
async function getPacificaFills(limit = 10) {
  const pacifica = new PacificaConnector();

  try {
    await pacifica.restRateLimiter.waitForSlot();

    const url = `${pacifica.restUrl}/api/v1/positions/history?account=${pacifica.account}&limit=${limit}`;
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Accept': 'application/json'
      }
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const result = await response.json();
    return result.data || [];
  } catch (error) {
    console.error('[Pacifica] Error fetching fills:', error.message);
    return [];
  }
}

// Format and display fills in a table
function displayFills(exchange, fills) {
  console.log(`\n${'='.repeat(120)}`);
  console.log(`  ${exchange.toUpperCase()} - Recent Filled Orders (${fills.length} trades)`);
  console.log('='.repeat(120));

  if (fills.length === 0) {
    console.log('  No filled orders found.');
    console.log('='.repeat(120));
    return;
  }

  if (exchange === 'hyperliquid') {
    // Format Hyperliquid fills
    const formattedFills = fills.map(fill => {
      const pnl = parseFloat(fill.closedPnl || 0).toFixed(4);
      return {
        Time: formatTime(fill.time),
        Coin: fill.coin,
        Side: fill.side === 'B' ? 'BUY' : 'SELL',
        Direction: fill.dir,
        Size: parseFloat(fill.sz).toFixed(4),
        Price: `$${parseFloat(fill.px).toFixed(6)}`,
        Fee: `$${parseFloat(fill.fee || 0).toFixed(4)}`,
        'Closed PnL': `$${pnl}`,
        'Order ID': fill.oid
      };
    });

    console.table(formattedFills);
  } else if (exchange === 'pacifica') {
    // Format Pacifica fills
    const formattedFills = fills.map(fill => {
      const pnl = parseFloat(fill.pnl || 0).toFixed(4);
      // Format side display
      let sideDisplay;
      if (fill.side.includes('long')) {
        sideDisplay = fill.side.includes('open') ? 'OPEN LONG' : 'CLOSE LONG';
      } else {
        sideDisplay = fill.side.includes('open') ? 'OPEN SHORT' : 'CLOSE SHORT';
      }

      return {
        Time: formatTime(fill.created_at),
        Symbol: fill.symbol,
        Side: sideDisplay,
        Amount: parseFloat(fill.amount).toFixed(4),
        Price: `$${parseFloat(fill.price).toFixed(6)}`,
        'Entry Price': `$${parseFloat(fill.entry_price).toFixed(6)}`,
        Fee: `$${parseFloat(fill.fee || 0).toFixed(4)}`,
        PnL: `$${pnl}`,
        Type: fill.event_type === 'fulfill_maker' ? 'MAKER' : 'TAKER',
        'Order ID': fill.order_id
      };
    });

    console.table(formattedFills);
  }

  console.log('='.repeat(120));
}

// Main function
async function main() {
  console.log('\n\x1b[1m📊 Fetching Recent Filled Orders from Both Exchanges...\x1b[0m\n');

  const limit = parseInt(process.argv[2]) || 10; // Allow custom limit via command line argument

  // Fetch fills from both exchanges in parallel
  const [hlFills, pacificaFills] = await Promise.all([
    getHyperliquidFills(limit),
    getPacificaFills(limit)
  ]);

  // Display results
  displayFills('hyperliquid', hlFills);
  displayFills('pacifica', pacificaFills);

  // Summary statistics
  console.log('\n\x1b[1m📈 Summary\x1b[0m');
  console.log('='.repeat(120));

  // Calculate totals for Hyperliquid
  const hlTotalPnL = hlFills.reduce((sum, fill) => sum + parseFloat(fill.closedPnl || 0), 0);
  const hlTotalFees = hlFills.reduce((sum, fill) => sum + parseFloat(fill.fee || 0), 0);

  // Calculate totals for Pacifica
  const pacificaTotalPnL = pacificaFills.reduce((sum, fill) => sum + parseFloat(fill.pnl || 0), 0);
  const pacificaTotalFees = pacificaFills.reduce((sum, fill) => sum + parseFloat(fill.fee || 0), 0);

  console.log(`  Hyperliquid:  ${hlFills.length} trades  |  Total PnL: ${colorPnL(hlTotalPnL.toFixed(4))}  |  Total Fees: $${hlTotalFees.toFixed(4)}`);
  console.log(`  Pacifica:     ${pacificaFills.length} trades  |  Total PnL: ${colorPnL(pacificaTotalPnL.toFixed(4))}  |  Total Fees: $${pacificaTotalFees.toFixed(4)}`);
  console.log(`  Combined:     ${hlFills.length + pacificaFills.length} trades  |  Net PnL:   ${colorPnL((hlTotalPnL + pacificaTotalPnL).toFixed(4))}  |  Total Fees: $${(hlTotalFees + pacificaTotalFees).toFixed(4)}`);
  console.log('='.repeat(120));
  console.log('\n✅ Done!\n');
}

main().catch(error => {
  console.error('\n❌ Error:', error.message);
  process.exit(1);
});
