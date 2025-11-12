import { describe, it, before } from 'node:test';
import assert from 'node:assert';
import HyperliquidConnector from '../../connectors/hyperliquid.js';
import dotenv from 'dotenv';

dotenv.config();

/**
 * Tests for new Hyperliquid connector methods
 * These tests use the real Hyperliquid API with public endpoints
 *
 * Note: These tests require a valid HL_WALLET in .env
 */

describe('Hyperliquid Connector Extensions', () => {
  let connector;
  const testWallet = process.env.HL_WALLET;

  before(async () => {
    // Initialize connector (no need to connect to WebSocket for REST-only tests)
    connector = new HyperliquidConnector({
      wallet: testWallet
    });

    console.log(`[Test] Using wallet: ${testWallet}`);
  });

  describe('getOpenOrders()', () => {
    it('should fetch open orders successfully', async () => {
      const orders = await connector.getOpenOrders(testWallet);

      // Validate response structure
      assert.ok(Array.isArray(orders), 'Response should be an array');

      console.log(`[Test] Found ${orders.length} open orders`);

      // If there are orders, validate structure
      if (orders.length > 0) {
        const order = orders[0];

        // Required fields
        assert.ok('orderId' in order, 'Order should have orderId');
        assert.ok('coin' in order, 'Order should have coin');
        assert.ok('side' in order, 'Order should have side');
        assert.ok('limitPrice' in order, 'Order should have limitPrice');
        assert.ok('size' in order, 'Order should have size');
        assert.ok('timestamp' in order, 'Order should have timestamp');
        assert.ok('raw' in order, 'Order should have raw data');

        // Validate types
        assert.strictEqual(typeof order.orderId, 'number', 'orderId should be a number');
        assert.strictEqual(typeof order.coin, 'string', 'coin should be a string');
        assert.ok(['buy', 'sell'].includes(order.side), 'side should be buy or sell');
        assert.strictEqual(typeof order.limitPrice, 'number', 'limitPrice should be a number');
        assert.strictEqual(typeof order.size, 'number', 'size should be a number');
        assert.strictEqual(typeof order.timestamp, 'number', 'timestamp should be a number');

        console.log(`[Test] Sample order: ${order.side} ${order.size} ${order.coin} @ ${order.limitPrice}`);
      } else {
        console.log('[Test] ℹ️  No open orders found (this is ok)');
      }
    });

    it('should handle empty results', async () => {
      // Even with no orders, should return empty array, not throw
      const orders = await connector.getOpenOrders(testWallet);
      assert.ok(Array.isArray(orders), 'Should return array even when empty');
    });

    it('should throw error for invalid wallet address', async () => {
      await assert.rejects(
        async () => {
          await connector.getOpenOrders('0xinvalid');
        },
        /HTTP/,
        'Should throw error for invalid wallet'
      );
    });
  });

  describe('getUserFills()', () => {
    it('should fetch user fills successfully', async () => {
      const fills = await connector.getUserFills(testWallet);

      // Validate response structure
      assert.ok(Array.isArray(fills), 'Response should be an array');

      console.log(`[Test] Found ${fills.length} fills`);

      // If there are fills, validate structure
      if (fills.length > 0) {
        const fill = fills[0];

        // Required fields
        assert.ok('tradeId' in fill, 'Fill should have tradeId');
        assert.ok('orderId' in fill, 'Fill should have orderId');
        assert.ok('coin' in fill, 'Fill should have coin');
        assert.ok('side' in fill, 'Fill should have side');
        assert.ok('price' in fill, 'Fill should have price');
        assert.ok('size' in fill, 'Fill should have size');
        assert.ok('timestamp' in fill, 'Fill should have timestamp');
        assert.ok('closedPnl' in fill, 'Fill should have closedPnl');
        assert.ok('fee' in fill, 'Fill should have fee');
        assert.ok('hash' in fill, 'Fill should have hash');
        assert.ok('raw' in fill, 'Fill should have raw data');

        // Validate types
        assert.strictEqual(typeof fill.tradeId, 'number', 'tradeId should be a number');
        assert.strictEqual(typeof fill.orderId, 'number', 'orderId should be a number');
        assert.strictEqual(typeof fill.coin, 'string', 'coin should be a string');
        assert.ok(['buy', 'sell'].includes(fill.side), 'side should be buy or sell');
        assert.strictEqual(typeof fill.price, 'number', 'price should be a number');
        assert.strictEqual(typeof fill.size, 'number', 'size should be a number');
        assert.strictEqual(typeof fill.timestamp, 'number', 'timestamp should be a number');
        assert.strictEqual(typeof fill.closedPnl, 'number', 'closedPnl should be a number');
        assert.strictEqual(typeof fill.fee, 'number', 'fee should be a number');

        console.log(`[Test] Sample fill: ${fill.side} ${fill.size} ${fill.coin} @ ${fill.price} (PnL: ${fill.closedPnl})`);
      } else {
        console.log('[Test] ℹ️  No fills found (this is ok for new accounts)');
      }
    });

    it('should fetch fills with startTime parameter', async () => {
      // Get fills from 24 hours ago
      const oneDayAgo = Date.now() - (24 * 60 * 60 * 1000);
      const fills = await connector.getUserFills(testWallet, oneDayAgo);

      assert.ok(Array.isArray(fills), 'Response should be an array');
      console.log(`[Test] Found ${fills.length} fills in last 24 hours`);

      // All fills should be after startTime
      fills.forEach(fill => {
        assert.ok(fill.timestamp >= oneDayAgo, 'Fill timestamp should be >= startTime');
      });
    });

    it('should handle empty results', async () => {
      // Request fills from far future (should return empty)
      const futureTime = Date.now() + (365 * 24 * 60 * 60 * 1000);
      const fills = await connector.getUserFills(testWallet, futureTime);

      assert.ok(Array.isArray(fills), 'Should return array even when empty');
      assert.strictEqual(fills.length, 0, 'Should return no fills for future time');
    });

    it('should respect 2000 fill limit', async () => {
      const fills = await connector.getUserFills(testWallet);

      // API returns at most 2000 fills
      assert.ok(fills.length <= 2000, 'Should not return more than 2000 fills');
    });
  });

  describe('getUserState()', () => {
    it('should fetch user state successfully', async () => {
      const state = await connector.getUserState(testWallet);

      // Validate response structure
      assert.ok(typeof state === 'object', 'Response should be an object');

      // Required top-level fields
      assert.ok('balance' in state, 'State should have balance');
      assert.ok('positions' in state, 'State should have positions');
      assert.ok('crossMargin' in state, 'State should have crossMargin');
      assert.ok('timestamp' in state, 'State should have timestamp');
      assert.ok('raw' in state, 'State should have raw data');

      console.log(`[Test] Account Value: $${state.balance.accountValue.toFixed(2)}`);
      console.log(`[Test] Withdrawable: $${state.balance.withdrawable.toFixed(2)}`);
      console.log(`[Test] Positions: ${state.positions.length}`);

      // Validate balance structure
      assert.ok('accountValue' in state.balance, 'Balance should have accountValue');
      assert.ok('withdrawable' in state.balance, 'Balance should have withdrawable');
      assert.ok('totalMarginUsed' in state.balance, 'Balance should have totalMarginUsed');
      assert.ok('totalRawUsd' in state.balance, 'Balance should have totalRawUsd');
      assert.ok('totalNtlPos' in state.balance, 'Balance should have totalNtlPos');

      // Validate types
      assert.strictEqual(typeof state.balance.accountValue, 'number', 'accountValue should be a number');
      assert.strictEqual(typeof state.balance.withdrawable, 'number', 'withdrawable should be a number');

      // Validate positions structure
      assert.ok(Array.isArray(state.positions), 'Positions should be an array');

      if (state.positions.length > 0) {
        const pos = state.positions[0];

        assert.ok('coin' in pos, 'Position should have coin');
        assert.ok('side' in pos, 'Position should have side');
        assert.ok('size' in pos, 'Position should have size');
        assert.ok('entryPrice' in pos, 'Position should have entryPrice');
        assert.ok('unrealizedPnl' in pos, 'Position should have unrealizedPnl');

        assert.ok(['long', 'short'].includes(pos.side), 'side should be long or short');
        assert.strictEqual(typeof pos.size, 'number', 'size should be a number');
        assert.ok(pos.size > 0, 'size should be positive');

        console.log(`[Test] Sample position: ${pos.side} ${pos.size} ${pos.coin} @ ${pos.entryPrice} (PnL: ${pos.unrealizedPnl})`);
      } else {
        console.log('[Test] ℹ️  No open positions');
      }

      // Validate cross margin structure
      assert.ok('accountValue' in state.crossMargin, 'crossMargin should have accountValue');
      assert.strictEqual(typeof state.crossMargin.accountValue, 'number', 'crossMargin.accountValue should be a number');
    });

    it('should filter out zero positions', async () => {
      const state = await connector.getUserState(testWallet);

      // All returned positions should have size > 0
      state.positions.forEach(pos => {
        assert.ok(pos.size > 0, 'All positions should have size > 0');
      });
    });

    it('should handle accounts with no positions', async () => {
      const state = await connector.getUserState(testWallet);

      // Should still have valid balance even with no positions
      assert.ok(typeof state.balance.accountValue === 'number', 'Should have account value');
      assert.ok(Array.isArray(state.positions), 'Positions should be an array');
    });

    it('should include timestamp', async () => {
      const state = await connector.getUserState(testWallet);

      assert.strictEqual(typeof state.timestamp, 'number', 'timestamp should be a number');

      // Timestamp should be recent (within last 10 seconds)
      const now = Date.now();
      assert.ok(Math.abs(now - state.timestamp) < 10000, 'Timestamp should be recent');
    });
  });

  describe('Rate Limiting', () => {
    it('should handle multiple rapid requests', async () => {
      // Make 5 rapid requests
      const promises = [];

      for (let i = 0; i < 5; i++) {
        promises.push(connector.getOpenOrders(testWallet));
      }

      // All should succeed (rate limiter handles this)
      const results = await Promise.all(promises);

      assert.strictEqual(results.length, 5, 'All requests should complete');
      results.forEach(result => {
        assert.ok(Array.isArray(result), 'Each result should be an array');
      });

      console.log('[Test] ✓ Rate limiting handled 5 rapid requests');
    });
  });

  describe('Error Handling', () => {
    it('should throw error when wallet is not provided', async () => {
      const connectorNoWallet = new HyperliquidConnector();

      await assert.rejects(
        async () => {
          await connectorNoWallet.getOpenOrders();
        },
        /User address required/,
        'Should throw error when no wallet provided'
      );
    });

    it('should handle network errors gracefully', async () => {
      // Create connector with invalid URL to simulate network error
      const badConnector = new HyperliquidConnector({
        restUrl: 'https://invalid-url-that-does-not-exist.com/info',
        wallet: testWallet
      });

      await assert.rejects(
        async () => {
          await badConnector.getOpenOrders(testWallet);
        },
        Error,
        'Should throw error for network failures'
      );
    });
  });
});

// Run tests if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  console.log('Running Hyperliquid Connector Extension Tests...');
  console.log('='.repeat(50));
}
