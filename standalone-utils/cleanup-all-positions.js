#!/usr/bin/env node

/**
 * cleanup_all.js
 *
 * Emergency script to flatten every open position on Pacifica and Hyperliquid.
 * - Cancels outstanding Pacifica orders
 * - Executes reduce-only IOC market orders to close Pacifica positions
 * - Executes reduce-only market orders to close Hyperliquid positions
 * - Retries a few times and reports any residual exposure
 *
 * Usage: node cleanup_all.js
 *
 * Requires:
 *   - config.json (for exchange settings)
 *   - HL_WALLET, HL_PRIVATE_KEY, SOL_WALLET, API_PUBLIC, API_PRIVATE in .env
 */

import dotenv from 'dotenv';
import HyperliquidConnector from './connectors/hyperliquid.js';
import PacificaConnector from './connectors/pacifica.js';
import { loadConfig } from './utils/config.js';

// Load .env from current directory
dotenv.config();

const MAX_ATTEMPTS = parseInt(process.env.CLEANUP_MAX_ATTEMPTS || '3', 10);
const RETRY_DELAY_MS = parseInt(process.env.CLEANUP_RETRY_DELAY_MS || '1500', 10);
const ORDERBOOK_WARMUP_MS = parseInt(process.env.CLEANUP_ORDERBOOK_WARMUP_MS || '1000', 10);
const SLEEP_AFTER_ORDER_MS = parseInt(process.env.CLEANUP_SLEEP_AFTER_ORDER_MS || '400', 10);
const MIN_POSITION = parseFloat(process.env.CLEANUP_MIN_POSITION || '0.0000001'); // skip dust

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function formatQty(value) {
  return value >= 0 ? `+${value.toFixed(6)}` : value.toFixed(6);
}

async function fetchPacificaMap(pacifica) {
  const positions = await pacifica.getPositions();
  const map = new Map();

  for (const pos of positions) {
    const signedQty = pos.side === 'long' ? pos.amount : -pos.amount;
    if (Math.abs(signedQty) < MIN_POSITION) {
      continue;
    }

    map.set(pos.symbol, {
      qty: signedQty,
      entryPrice: pos.entryPrice || 0,
      notional: Math.abs(signedQty * (pos.entryPrice || 0))
    });
  }

  return map;
}

async function fetchHyperliquidMap(hyperliquid) {
  const balance = await hyperliquid.getBalance();
  const map = new Map();

  for (const asset of balance.assetPositions || []) {
    const position = asset.position || {};
    const coin = position.coin;
    const qty = parseFloat(position.szi || '0');

    if (!coin || Math.abs(qty) < MIN_POSITION) {
      continue;
    }

    const entryPx = parseFloat(position.entryPx || '0');
    map.set(coin, {
      qty,
      entryPrice: entryPx,
      notional: Math.abs(qty * entryPx)
    });
  }

  return map;
}

function logSummary(title, pacMap, hlMap) {
  const symbols = new Set([...pacMap.keys(), ...hlMap.keys()]);
  console.log('\n' + '='.repeat(80));
  console.log(title);
  console.log('='.repeat(80));

  if (symbols.size === 0) {
    console.log('No open positions detected.');
    return;
  }

  for (const symbol of Array.from(symbols).sort()) {
    const pac = pacMap.get(symbol)?.qty || 0;
    const hl = hlMap.get(symbol)?.qty || 0;
    const net = pac + hl;
    console.log(
      `${symbol.padEnd(10)} HL: ${formatQty(hl)} | PAC: ${formatQty(pac)} | NET: ${formatQty(net)}`
    );
  }
}

async function closePacificaPositions(pacifica) {
  const marketInfo = await pacifica.getMarketInfo();
  const subscribedBooks = new Set();

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    const positions = await pacifica.getPositions();
    const actionable = positions.filter(pos => Math.abs(pos.amount) >= MIN_POSITION);

    if (actionable.length === 0) {
      console.log('[Pacifica] ✅ All positions flat.');
      return;
    }

    console.log(`\n[Pacifica] Attempt ${attempt}/${MAX_ATTEMPTS} - closing ${actionable.length} positions`);

    for (const pos of actionable) {
      const signedQty = pos.side === 'long' ? pos.amount : -pos.amount;
      const side = signedQty > 0 ? 'sell' : 'buy';
      const amount = Math.abs(signedQty);
      const info = marketInfo.get(pos.symbol);
      const lotSize = info?.lot_size;
      const rounded = lotSize ? pacifica.roundToLotSize(amount, lotSize) : amount;

      if (!rounded || rounded < MIN_POSITION) {
        console.log(`[Pacifica] ⚠️  Skipping ${pos.symbol} (amount ${amount} below lot size ${lotSize})`);
        continue;
      }

      if (!subscribedBooks.has(pos.symbol)) {
        try {
          await pacifica.subscribeOrderbook(pos.symbol);
          await sleep(ORDERBOOK_WARMUP_MS);
          subscribedBooks.add(pos.symbol);
        } catch (error) {
          console.error(`[Pacifica] ⚠️  Failed to subscribe orderbook ${pos.symbol}: ${error.message}`);
        }
      }

      try {
        console.log(`[Pacifica] ${side.toUpperCase()} ${rounded} ${pos.symbol} (reduce-only IOC)`);
        await pacifica.createMarketOrder(pos.symbol, side, rounded, {
          reduce_only: true,
          tif: 'IOC'
        });
      } catch (error) {
        console.error(`[Pacifica] ❌ Failed to close ${pos.symbol}: ${error.message}`);
      }

      await sleep(SLEEP_AFTER_ORDER_MS);
    }

    await sleep(RETRY_DELAY_MS);
  }

  const residual = await pacifica.getPositions();
  const remaining = residual.filter(pos => Math.abs(pos.amount) >= MIN_POSITION);
  if (remaining.length > 0) {
    console.log('\n[Pacifica] ⚠️  Residual positions still open:');
    for (const pos of remaining) {
      const signedQty = pos.side === 'long' ? pos.amount : -pos.amount;
      console.log(`  ${pos.symbol}: ${formatQty(signedQty)} @ ${pos.entryPrice}`);
    }
  } else {
    console.log('[Pacifica] ✅ Positions closed after retries.');
  }
}

async function closeHyperliquidPositions(hyperliquid) {
  const subscribedBooks = new Set();
  // Ensure metadata is cached (required for market orders)
  try {
    await hyperliquid.getMeta();
  } catch (error) {
    console.error('[Hyperliquid] ⚠️  Failed to load asset metadata:', error.message);
  }

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    const balance = await hyperliquid.getBalance();
    const actionable = (balance.assetPositions || [])
      .map(asset => {
        const position = asset.position || {};
        return {
          coin: position.coin,
          qty: parseFloat(position.szi || '0')
        };
      })
      .filter(p => p.coin && Math.abs(p.qty) >= MIN_POSITION);

    if (actionable.length === 0) {
      console.log('[Hyperliquid] ✅ All positions flat.');
      return;
    }

    console.log(`\n[Hyperliquid] Attempt ${attempt}/${MAX_ATTEMPTS} - closing ${actionable.length} positions`);

    for (const pos of actionable) {
      if (!subscribedBooks.has(pos.coin)) {
        try {
          await hyperliquid.subscribeOrderbook(pos.coin);
          console.log(`[Hyperliquid] Subscribed to ${pos.coin} orderbook`);
          await sleep(ORDERBOOK_WARMUP_MS);
          subscribedBooks.add(pos.coin);
        } catch (error) {
          console.error(`[Hyperliquid] ⚠️  Failed to subscribe orderbook ${pos.coin}: ${error.message}`);
        }
      }

      const side = pos.qty > 0 ? 'sell' : 'buy';
      const size = Math.abs(pos.qty);
      try {
        console.log(`[Hyperliquid] ${side.toUpperCase()} ${size} ${pos.coin} (reduce-only)`);
        await hyperliquid.createMarketOrder(pos.coin, side, size, { reduceOnly: true });
      } catch (error) {
        console.error(`[Hyperliquid] ❌ Failed to close ${pos.coin}: ${error.message}`);
      }

      await sleep(SLEEP_AFTER_ORDER_MS);
    }

    await sleep(RETRY_DELAY_MS);
  }

  const balance = await hyperliquid.getBalance();
  const remaining = (balance.assetPositions || [])
    .map(asset => {
      const position = asset.position || {};
      return {
        coin: position.coin,
        qty: parseFloat(position.szi || '0')
      };
    })
    .filter(p => p.coin && Math.abs(p.qty) >= MIN_POSITION);

  if (remaining.length > 0) {
    console.log('\n[Hyperliquid] ⚠️  Residual positions still open:');
    for (const pos of remaining) {
      console.log(`  ${pos.coin}: ${formatQty(pos.qty)}`);
    }
  } else {
    console.log('[Hyperliquid] ✅ Positions closed after retries.');
  }
}

async function main() {
  console.log('='.repeat(80));
  console.log('Emergency Cleanup: Flatten Pacifica & Hyperliquid Positions');
  console.log('='.repeat(80));

  const config = loadConfig();

  const hyperliquid = new HyperliquidConnector({
    testnet: config.exchanges?.hyperliquid?.testnet
  });

  const pacifica = new PacificaConnector({
    testnet: config.exchanges?.pacifica?.testnet
  });

  try {
    await Promise.all([pacifica.connect(), hyperliquid.connect()]);
    await sleep(1000);

    // Cancel any outstanding Pacifica orders to avoid new fills mid-cleanup
    try {
      console.log('\n[Pacifica] Cancelling open orders before flattening...');
      const cancelResult = await pacifica.cancelAllOrdersSmart({ allSymbols: true });
      console.log('[Pacifica] Cancel result:', JSON.stringify(cancelResult));
    } catch (error) {
      console.error('[Pacifica] ⚠️  Failed to cancel orders:', error.message);
    }

    const [pacBefore, hlBefore] = await Promise.all([
      fetchPacificaMap(pacifica),
      fetchHyperliquidMap(hyperliquid)
    ]);
    logSummary('Initial Positions', pacBefore, hlBefore);

    console.log('\n[SYS] Running Pacifica and Hyperliquid cleanup concurrently...');
    await Promise.all([
      closePacificaPositions(pacifica),
      closeHyperliquidPositions(hyperliquid)
    ]);

    const [pacAfter, hlAfter] = await Promise.all([
      fetchPacificaMap(pacifica),
      fetchHyperliquidMap(hyperliquid)
    ]);
    logSummary('Post-Cleanup Positions', pacAfter, hlAfter);

    const stillOpen = new Set([...pacAfter.keys(), ...hlAfter.keys()]);
    if (stillOpen.size > 0) {
      console.log('\n⚠️  Cleanup complete, but residual positions remain. Review logs above.');
    } else {
      console.log('\n✅ Cleanup complete. All positions appear flat.');
    }
  } catch (error) {
    console.error('\n❌ Cleanup failed:', error.message);
    console.error(error.stack);
  } finally {
    try {
      pacifica.intentionalDisconnect = true;
      pacifica.disconnect();
    } catch {}

    try {
      hyperliquid.intentionalDisconnect = true;
      hyperliquid.disconnect();
    } catch {}

    console.log('\nDisconnected from exchanges.');
  }
}

main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
