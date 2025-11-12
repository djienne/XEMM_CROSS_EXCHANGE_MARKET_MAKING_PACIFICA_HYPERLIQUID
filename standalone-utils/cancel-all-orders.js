#!/usr/bin/env node

/**
 * cancel-all-orders.js
 *
 * Simple script to cancel all open orders on Pacifica and Hyperliquid exchanges.
 * Does NOT close positions - only cancels pending orders.
 *
 * Usage: node cancel-all-orders.js
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

const RETRY_DELAY_MS = parseInt(process.env.CANCEL_RETRY_DELAY_MS || '1000', 10);
const MAX_ATTEMPTS = parseInt(process.env.CANCEL_MAX_ATTEMPTS || '2', 10);

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Cancel all orders on Pacifica exchange
 */
async function cancelPacificaOrders(pacifica) {
  console.log('\n[Pacifica] Cancelling all open orders...');

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      const result = await pacifica.cancelAllOrdersSmart({ allSymbols: true });
      console.log(`[Pacifica] ✅ Cancel result (attempt ${attempt}/${MAX_ATTEMPTS}):`, JSON.stringify(result));

      const cancelledCount = result?.cancelled_count || result?.data?.cancelled_count || 0;
      if (cancelledCount > 0) {
        console.log(`[Pacifica] Successfully cancelled ${cancelledCount} orders`);
      } else {
        console.log('[Pacifica] No orders to cancel');
      }

      return result;
    } catch (error) {
      console.error(`[Pacifica] ❌ Failed to cancel orders (attempt ${attempt}/${MAX_ATTEMPTS}):`, error.message);
      if (attempt < MAX_ATTEMPTS) {
        console.log(`[Pacifica] Retrying in ${RETRY_DELAY_MS}ms...`);
        await sleep(RETRY_DELAY_MS);
      }
    }
  }

  console.log('[Pacifica] ⚠️  All cancel attempts failed');
}

/**
 * Cancel all orders on Hyperliquid exchange
 */
async function cancelHyperliquidOrders(hyperliquid) {
  console.log('\n[Hyperliquid] Fetching open orders...');

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      const openOrders = await hyperliquid.getOpenOrders();

      if (!openOrders || openOrders.length === 0) {
        console.log('[Hyperliquid] No open orders to cancel');
        return;
      }

      console.log(`[Hyperliquid] Found ${openOrders.length} open orders`);

      // Build cancel action for all orders
      const cancels = openOrders.map(order => ({
        a: order.coin, // asset index
        o: order.oid   // order id
      }));

      const action = {
        type: 'cancel',
        cancels: cancels
      };

      const nonce = Date.now();
      const signature = await hyperliquid.signAction(action, nonce);

      const payload = {
        action: action,
        nonce: nonce,
        signature: signature
      };

      console.log(`[Hyperliquid] Cancelling ${cancels.length} orders...`);

      const response = await fetch(hyperliquid.exchangeUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });

      const result = await response.json();

      if (result.status === 'ok') {
        const statuses = result.response?.data?.statuses || [];
        const successCount = statuses.filter(s => s === 'success').length;
        console.log(`[Hyperliquid] ✅ Successfully cancelled ${successCount}/${cancels.length} orders`);

        // Log any failures
        statuses.forEach((status, index) => {
          if (status !== 'success') {
            console.log(`[Hyperliquid] ⚠️  Order ${index} cancel failed:`, status);
          }
        });

        return result;
      } else {
        throw new Error(`Cancel failed: ${JSON.stringify(result)}`);
      }
    } catch (error) {
      console.error(`[Hyperliquid] ❌ Failed to cancel orders (attempt ${attempt}/${MAX_ATTEMPTS}):`, error.message);
      if (attempt < MAX_ATTEMPTS) {
        console.log(`[Hyperliquid] Retrying in ${RETRY_DELAY_MS}ms...`);
        await sleep(RETRY_DELAY_MS);
      }
    }
  }

  console.log('[Hyperliquid] ⚠️  All cancel attempts failed');
}

async function main() {
  console.log('='.repeat(80));
  console.log('Cancel All Orders: Pacifica & Hyperliquid');
  console.log('='.repeat(80));

  const config = loadConfig();

  const hyperliquid = new HyperliquidConnector({
    testnet: config.exchanges?.hyperliquid?.testnet
  });

  const pacifica = new PacificaConnector({
    testnet: config.exchanges?.pacifica?.testnet
  });

  try {
    // Connect to both exchanges
    console.log('\n[SYS] Connecting to exchanges...');
    await Promise.all([pacifica.connect(), hyperliquid.connect()]);
    await sleep(1000);
    console.log('[SYS] ✅ Connected to both exchanges');

    // Cancel orders on both exchanges concurrently
    console.log('\n[SYS] Cancelling orders on both exchanges...');
    await Promise.all([
      cancelPacificaOrders(pacifica),
      cancelHyperliquidOrders(hyperliquid)
    ]);

    console.log('\n' + '='.repeat(80));
    console.log('✅ Order cancellation complete');
    console.log('='.repeat(80));
  } catch (error) {
    console.error('\n❌ Cancellation failed:', error.message);
    console.error(error.stack);
  } finally {
    // Clean disconnect
    try {
      pacifica.intentionalDisconnect = true;
      pacifica.disconnect();
    } catch (e) {
      // Ignore disconnect errors
    }

    try {
      hyperliquid.intentionalDisconnect = true;
      hyperliquid.disconnect();
    } catch (e) {
      // Ignore disconnect errors
    }

    console.log('\n[SYS] Disconnected from exchanges');
  }
}

main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
