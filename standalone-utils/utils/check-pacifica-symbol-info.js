import PacificaConnector from '../connectors/pacifica.js';

/**
 * Check lot size and tick size information for all Pacifica symbols
 * Displays minimum order value (lot_size * current_price) for each symbol
 */

async function checkSymbolInfo() {
  console.log('Connecting to Pacifica...\n');

  const pacifica = new PacificaConnector();

  try {
    // No need to connect via WebSocket, we'll use REST API only
    console.log('Fetching market info...');
    const marketInfo = await pacifica.getMarketInfo();

    console.log(`Found ${marketInfo.size} symbols\n`);

    // Prepare data array for table
    const tableData = [];

    // Fetch orderbook for each symbol to get current price
    console.log('Fetching current prices...\n');

    for (const [symbol, info] of marketInfo) {
      try {
        // Get current orderbook via REST
        const orderbook = await pacifica.requestOrderbookRest(symbol);

        // Parse Pacifica orderbook format:
        // response.data.l[0] = bids array of objects with {p: price, a: amount, n: num_orders}
        // response.data.l[1] = asks array of objects with {p: price, a: amount, n: num_orders}
        let bestBid = null;
        let bestAsk = null;

        const levels = orderbook.data?.l;
        if (levels && Array.isArray(levels)) {
          // Bids are in levels[0]
          if (levels[0] && levels[0].length > 0) {
            bestBid = parseFloat(levels[0][0].p);
          }
          // Asks are in levels[1]
          if (levels[1] && levels[1].length > 0) {
            bestAsk = parseFloat(levels[1][0].p);
          }
        }

        // Calculate mid price
        let midPrice = null;
        if (bestBid && bestAsk) {
          midPrice = (bestBid + bestAsk) / 2;
        } else if (bestBid) {
          midPrice = bestBid;
        } else if (bestAsk) {
          midPrice = bestAsk;
        }

        const lotSize = parseFloat(info.lot_size);
        const tickSize = parseFloat(info.tick_size);
        const minOrderSize = parseFloat(info.min_order_size || 0); // Min order size in USD when placing order

        // Calculate minimum partial fill value (lot_size * price)
        const minPartialFillValue = midPrice ? lotSize * midPrice : null;

        // Calculate minimum order placement quantity (min USD order / current price)
        const minOrderQty = midPrice ? minOrderSize / midPrice : null;

        tableData.push({
          symbol,
          lotSize,
          tickSize,
          minOrderSize, // Min order size in USD when placing (from API)
          currentPrice: midPrice,
          minPartialFillValue, // lot_size * price = smallest partial fill
          minOrderQty, // Minimum quantity when placing order
          bestBid,
          bestAsk
        });

        // Small delay to respect rate limits
        await new Promise(resolve => setTimeout(resolve, 200));

      } catch (error) {
        // Show first 3 errors in detail
        if (tableData.length < 3) {
          console.error(`\n[ERROR] Symbol ${symbol}:`, error.message);
        }

        // Add symbol with partial data
        tableData.push({
          symbol,
          lotSize: parseFloat(info.lot_size),
          tickSize: parseFloat(info.tick_size),
          minOrderSize: parseFloat(info.min_order_size || 0),
          currentPrice: null,
          minPartialFillValue: null,
          minOrderQty: null,
          bestBid: null,
          bestAsk: null
        });
      }
    }

    // Sort by symbol name
    tableData.sort((a, b) => a.symbol.localeCompare(b.symbol));

    // Display nicely formatted table
    console.log('\n' + '='.repeat(160));
    console.log('PACIFICA SYMBOL INFORMATION - PARTIAL FILL INCREMENTS (POST-ONLY ORDERS)');
    console.log('='.repeat(160));
    console.log(
      padRight('SYMBOL', 10) +
      padRight('LOT SIZE', 12) +
      padRight('TICK SIZE', 12) +
      padRight('PRICE', 12) +
      padRight('LOT×PRICE', 13) +
      padRight('MIN ORDER $', 13) +
      padRight('MIN ORDER QTY', 15) +
      padRight('BID', 12) +
      padRight('ASK', 12)
    );
    console.log('-'.repeat(160));

    for (const data of tableData) {
      console.log(
        padRight(data.symbol, 10) +
        padRight(data.lotSize.toString(), 12) +
        padRight(data.tickSize.toString(), 12) +
        padRight(data.currentPrice ? data.currentPrice.toFixed(4) : 'N/A', 12) +
        padRight(data.minPartialFillValue ? `$${data.minPartialFillValue.toFixed(4)}` : 'N/A', 13) +
        padRight(data.minOrderSize ? `$${data.minOrderSize.toFixed(0)}` : 'N/A', 13) +
        padRight(data.minOrderQty ? data.minOrderQty.toFixed(8).replace(/\.?0+$/, '') : 'N/A', 15) +
        padRight(data.bestBid ? data.bestBid.toFixed(4) : 'N/A', 12) +
        padRight(data.bestAsk ? data.bestAsk.toFixed(4) : 'N/A', 12)
      );
    }

    console.log('='.repeat(160));
    console.log('\n⚠️  WARNING: API lot_size values may NOT be accurate!');
    console.log('  - API reports SOL lot_size = 0.01, but actual minimum partial fill is 0.1');
    console.log('  - API reports BTC lot_size = 0.00001, but actual minimum is much higher');
    console.log('  - XPL lot_size = 1 appears to be correct');
    console.log('  ➜ Verify actual minimum fills through trading before relying on these values!\n');
    console.log('Legend:');
    console.log('  LOT SIZE: Minimum partial fill increment from API (⚠️ MAY BE INACCURATE)');
    console.log('  TICK SIZE: Minimum price increment');
    console.log('  PRICE: Current mid price');
    console.log('  LOT×PRICE: Value of smallest partial fill (based on API lot_size)');
    console.log('  MIN ORDER $: Minimum order size when placing order (USD)');
    console.log('  MIN ORDER QTY: Minimum quantity when placing order');
    console.log('\n  ** For post-only orders, you can receive partial fills as small as LOT SIZE **');
    console.log('  ** Example: XPL has lot_size=1, confirmed correct (~$0.35 per fill) **');
    console.log('  ** Example: SOL actual minimum is 0.1 (NOT 0.01 as API reports!) **');
    console.log('\n');

    // Calculate some statistics
    const validPartialFills = tableData.filter(d => d.minPartialFillValue !== null);
    if (validPartialFills.length > 0) {
      const partialFillValues = validPartialFills.map(d => d.minPartialFillValue);
      const avgPartialFill = partialFillValues.reduce((a, b) => a + b, 0) / partialFillValues.length;
      const maxPartialFill = Math.max(...partialFillValues);
      const minPartialFill = Math.min(...partialFillValues);

      console.log('Statistics:');
      console.log(`  Average partial fill value: $${avgPartialFill.toFixed(4)}`);
      console.log(`  Highest partial fill value: $${maxPartialFill.toFixed(4)} (smallest increment for expensive assets)`);
      console.log(`  Lowest partial fill value: $${minPartialFill.toFixed(4)} (smallest increment for cheap assets)`);
      console.log(`  Total symbols: ${tableData.length}`);
      console.log(`  Symbols with price data: ${validPartialFills.length}`);
    }

  } catch (error) {
    console.error('Error:', error.message);
    console.error(error.stack);
  }
}

// Helper function to pad strings for table formatting
function padRight(str, length) {
  return String(str).padEnd(length, ' ');
}

// Run the script
checkSymbolInfo().catch(console.error);
