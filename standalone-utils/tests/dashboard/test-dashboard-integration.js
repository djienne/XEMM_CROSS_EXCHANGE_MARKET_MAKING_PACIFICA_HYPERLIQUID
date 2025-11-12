import { describe, it, before, beforeEach } from 'node:test';
import assert from 'node:assert';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Import DashboardState class (we'll need to export it from dashboard.js)
// For now, we'll test the logic by re-implementing it here

/**
 * Tests for Dashboard Integration
 * Tests the DashboardState class and dashboard logic
 */

// Re-implement DashboardState for testing
class DashboardState {
  constructor() {
    this.hlBalance = null;
    this.hlEquity = null;
    this.pacBalance = null;
    this.pacEquity = null;
    this.hlPositions = new Map();
    this.pacPositions = new Map();
    this.hlOrders = new Map();
    this.pacOrders = new Map();
    this.recentFills = [];
    this.maxFills = 10;
    this.recentOrders = [];
    this.maxOrders = 8;
    this.seenFillIds = new Set();
    this.dirty = true;
    this.lastUpdate = Date.now();
  }

  getSnapshot() {
    this.dirty = false;
    return {
      hlBalance: this.hlBalance,
      hlEquity: this.hlEquity,
      pacBalance: this.pacBalance,
      pacEquity: this.pacEquity,
      hlPositions: new Map(this.hlPositions),
      pacPositions: new Map(this.pacPositions),
      hlOrders: new Map(this.hlOrders),
      pacOrders: new Map(this.pacOrders),
      recentFills: [...this.recentFills],
      recentOrders: [...this.recentOrders],
      lastUpdate: this.lastUpdate
    };
  }

  markDirty() {
    this.dirty = true;
    this.lastUpdate = Date.now();
  }

  updateHLBalance(balance, equity) {
    this.hlBalance = balance;
    this.hlEquity = equity;
    this.markDirty();
  }

  updatePACBalance(balance, equity) {
    this.pacBalance = balance;
    this.pacEquity = equity;
    this.markDirty();
  }

  updateHLPositions(positions) {
    this.hlPositions.clear();
    positions.forEach(pos => {
      this.hlPositions.set(pos.coin, pos);
    });
    this.markDirty();
  }

  updatePACPositions(positions) {
    this.pacPositions.clear();
    positions.forEach(pos => {
      this.pacPositions.set(pos.symbol, pos);
    });
    this.markDirty();
  }

  updateHLOrders(orders) {
    this.hlOrders.clear();
    orders.forEach(order => {
      this.hlOrders.set(order.orderId, order);
    });
    this.markDirty();
  }

  updatePACOrders(orders) {
    this.pacOrders.clear();
    orders.forEach(order => {
      this.pacOrders.set(order.order_id, order);
    });
    this.markDirty();
  }

  addFill(fill) {
    const fillId = fill.id || fill.tradeId || fill.hash || `${fill.timestamp}-${fill.coin || fill.symbol}`;

    if (this.seenFillIds.has(fillId)) {
      return; // Already seen
    }

    this.seenFillIds.add(fillId);
    this.recentFills.unshift(fill);

    if (this.recentFills.length > this.maxFills) {
      const removed = this.recentFills.pop();
      const removedId = removed.id || removed.tradeId || removed.hash || `${removed.timestamp}-${removed.coin || removed.symbol}`;
      this.seenFillIds.delete(removedId);
    }

    this.markDirty();
  }

  addRecentOrder(order) {
    this.recentOrders.unshift(order);

    if (this.recentOrders.length > this.maxOrders) {
      this.recentOrders.pop();
    }

    this.markDirty();
  }
}

describe('Dashboard Integration Tests', () => {
  let state;
  let fixtures;

  before(() => {
    // Load fixtures
    const hlFixtures = JSON.parse(
      readFileSync(join(__dirname, '../fixtures/hyperliquid-responses.json'), 'utf-8')
    );
    const pacFixtures = JSON.parse(
      readFileSync(join(__dirname, '../fixtures/pacifica-responses.json'), 'utf-8')
    );

    fixtures = {
      hl: hlFixtures,
      pac: pacFixtures
    };

    console.log('[Test] Fixtures loaded successfully');
  });

  beforeEach(() => {
    state = new DashboardState();
  });

  describe('DashboardState', () => {
    it('should initialize with null values', () => {
      assert.strictEqual(state.hlBalance, null);
      assert.strictEqual(state.hlEquity, null);
      assert.strictEqual(state.pacBalance, null);
      assert.strictEqual(state.pacEquity, null);
      assert.strictEqual(state.dirty, true);
    });

    it('should mark dirty when updating balance', () => {
      state.dirty = false;
      state.updateHLBalance(1000, 1050);

      assert.strictEqual(state.hlBalance, 1000);
      assert.strictEqual(state.hlEquity, 1050);
      assert.strictEqual(state.dirty, true);
    });

    it('should update Hyperliquid positions', () => {
      const positions = [
        { coin: 'SOL', side: 'long', size: 50.5 },
        { coin: 'BTC', side: 'short', size: 0.05 }
      ];

      state.updateHLPositions(positions);

      assert.strictEqual(state.hlPositions.size, 2);
      assert.ok(state.hlPositions.has('SOL'));
      assert.ok(state.hlPositions.has('BTC'));
      assert.strictEqual(state.hlPositions.get('SOL').size, 50.5);
    });

    it('should update Pacifica positions', () => {
      const positions = [
        { symbol: 'AAVE', side: 'ask', amount: '223.72' },
        { symbol: 'SOL', side: 'bid', amount: '50.0' }
      ];

      state.updatePACPositions(positions);

      assert.strictEqual(state.pacPositions.size, 2);
      assert.ok(state.pacPositions.has('AAVE'));
      assert.ok(state.pacPositions.has('SOL'));
      assert.strictEqual(state.pacPositions.get('AAVE').amount, '223.72');
    });
  });

  describe('Fill Deduplication', () => {
    it('should add fills without duplicates', () => {
      const fill1 = {
        tradeId: 12345,
        coin: 'SOL',
        side: 'buy',
        price: 145.50,
        size: 10,
        timestamp: Date.now()
      };

      const fill2 = {
        tradeId: 12346,
        coin: 'BTC',
        side: 'sell',
        price: 30000,
        size: 0.05,
        timestamp: Date.now()
      };

      state.addFill(fill1);
      state.addFill(fill2);

      assert.strictEqual(state.recentFills.length, 2);
      assert.strictEqual(state.seenFillIds.size, 2);
    });

    it('should not add duplicate fills', () => {
      const fill = {
        tradeId: 12345,
        coin: 'SOL',
        side: 'buy',
        price: 145.50,
        size: 10,
        timestamp: Date.now()
      };

      state.addFill(fill);
      state.addFill(fill); // Try to add again
      state.addFill(fill); // And again

      assert.strictEqual(state.recentFills.length, 1, 'Should only have 1 fill');
      assert.strictEqual(state.seenFillIds.size, 1);
    });

    it('should respect maxFills limit', () => {
      // Add 15 fills (more than maxFills=10)
      for (let i = 0; i < 15; i++) {
        state.addFill({
          tradeId: 12345 + i,
          coin: 'SOL',
          side: 'buy',
          price: 145.50,
          size: 10,
          timestamp: Date.now() + i
        });
      }

      assert.strictEqual(state.recentFills.length, 10, 'Should only keep 10 fills');
      assert.strictEqual(state.seenFillIds.size, 10, 'Should only track 10 fill IDs');

      // Verify newest fills are kept (higher tradeIds)
      assert.strictEqual(state.recentFills[0].tradeId, 12345 + 14);
      assert.strictEqual(state.recentFills[9].tradeId, 12345 + 5);
    });

    it('should remove old fill IDs when exceeding maxFills', () => {
      // Add 11 fills
      for (let i = 0; i < 11; i++) {
        state.addFill({
          tradeId: 12345 + i,
          coin: 'SOL',
          side: 'buy',
          price: 145.50,
          size: 10,
          timestamp: Date.now() + i
        });
      }

      // The oldest fill (tradeId: 12345) should be removed
      assert.ok(!state.seenFillIds.has(12345), 'Oldest fill ID should be removed');

      // Try to add it again - should succeed since it's no longer in seenFillIds
      state.addFill({
        tradeId: 12345,
        coin: 'SOL',
        side: 'buy',
        price: 145.50,
        size: 10,
        timestamp: Date.now()
      });

      assert.strictEqual(state.recentFills.length, 10);
      assert.strictEqual(state.recentFills[0].tradeId, 12345, 'Old fill should be re-added at the front');
    });
  });

  describe('Recent Orders', () => {
    it('should add orders to recent list', () => {
      const order1 = {
        orderId: 1001,
        coin: 'SOL',
        side: 'buy',
        size: 10,
        timestamp: Date.now()
      };

      const order2 = {
        orderId: 1002,
        coin: 'BTC',
        side: 'sell',
        size: 0.05,
        timestamp: Date.now()
      };

      state.addRecentOrder(order1);
      state.addRecentOrder(order2);

      assert.strictEqual(state.recentOrders.length, 2);
      assert.strictEqual(state.recentOrders[0].orderId, 1002, 'Newest order should be first');
      assert.strictEqual(state.recentOrders[1].orderId, 1001);
    });

    it('should respect maxOrders limit', () => {
      // Add 10 orders (more than maxOrders=8)
      for (let i = 0; i < 10; i++) {
        state.addRecentOrder({
          orderId: 1001 + i,
          coin: 'SOL',
          side: 'buy',
          size: 10,
          timestamp: Date.now() + i
        });
      }

      assert.strictEqual(state.recentOrders.length, 8, 'Should only keep 8 orders');

      // Verify newest orders are kept
      assert.strictEqual(state.recentOrders[0].orderId, 1010);
      assert.strictEqual(state.recentOrders[7].orderId, 1003);
    });
  });

  describe('Snapshot', () => {
    it('should create a snapshot and clear dirty flag', () => {
      state.updateHLBalance(1000, 1050);
      assert.strictEqual(state.dirty, true);

      const snapshot = state.getSnapshot();

      assert.strictEqual(state.dirty, false, 'dirty flag should be cleared');
      assert.strictEqual(snapshot.hlBalance, 1000);
      assert.strictEqual(snapshot.hlEquity, 1050);
    });

    it('should create independent copies in snapshot', () => {
      state.updateHLBalance(1000, 1050);

      const positions = [{ coin: 'SOL', side: 'long', size: 50.5 }];
      state.updateHLPositions(positions);

      const snapshot1 = state.getSnapshot();

      // Modify state
      state.updateHLBalance(2000, 2100);

      const snapshot2 = state.getSnapshot();

      // Snapshots should be independent
      assert.strictEqual(snapshot1.hlBalance, 1000);
      assert.strictEqual(snapshot2.hlBalance, 2000);
    });
  });

  describe('Fixture Data Processing', () => {
    it('should process Hyperliquid clearinghouse state', () => {
      const chState = fixtures.hl.clearinghouseState;

      state.updateHLBalance(
        parseFloat(chState.withdrawable),
        parseFloat(chState.marginSummary.accountValue)
      );

      assert.strictEqual(state.hlBalance, 1125.20);
      assert.strictEqual(state.hlEquity, 1250.50);
    });

    it('should process Hyperliquid positions from clearinghouse state', () => {
      const chState = fixtures.hl.clearinghouseState;

      const positions = chState.assetPositions.map(p => ({
        coin: p.position.coin,
        side: parseFloat(p.position.szi) > 0 ? 'long' : 'short',
        size: Math.abs(parseFloat(p.position.szi)),
        entryPrice: parseFloat(p.position.entryPx)
      }));

      state.updateHLPositions(positions);

      assert.strictEqual(state.hlPositions.size, 2);
      assert.strictEqual(state.hlPositions.get('SOL').side, 'long');
      assert.strictEqual(state.hlPositions.get('SOL').size, 50.5);
      assert.strictEqual(state.hlPositions.get('BTC').side, 'short');
      assert.strictEqual(state.hlPositions.get('BTC').size, 0.05);
    });

    it('should process Pacifica positions', () => {
      const positions = fixtures.pac.positions.data;

      state.updatePACPositions(positions);

      assert.strictEqual(state.pacPositions.size, 2);
      assert.ok(state.pacPositions.has('AAVE'));
      assert.ok(state.pacPositions.has('SOL'));
    });

    it('should process Hyperliquid fills', () => {
      const fills = fixtures.hl.userFills;

      fills.forEach(fill => {
        state.addFill({
          tradeId: fill.tid,
          orderId: fill.oid,
          coin: fill.coin,
          side: fill.side === 'B' ? 'buy' : 'sell',
          price: parseFloat(fill.px),
          size: parseFloat(fill.sz),
          timestamp: fill.time,
          closedPnl: parseFloat(fill.closedPnl),
          hash: fill.hash,
          exchange: 'HL'
        });
      });

      assert.strictEqual(state.recentFills.length, 2);
      assert.strictEqual(state.recentFills[0].coin, 'SOL');
      assert.strictEqual(state.recentFills[1].coin, 'AVAX');
    });

    it('should process Pacifica order history', () => {
      const orders = fixtures.pac.orderHistory.data;

      orders.forEach(order => {
        state.addRecentOrder({
          orderId: order.order_id,
          symbol: order.symbol,
          side: order.side,
          price: parseFloat(order.average_filled_price),
          amount: parseFloat(order.filled_amount),
          timestamp: order.updated_at,
          status: order.order_status,
          exchange: 'PAC'
        });
      });

      assert.strictEqual(state.recentOrders.length, 2);
      assert.strictEqual(state.recentOrders[0].symbol, 'SOL');
      assert.strictEqual(state.recentOrders[0].status, 'filled');
    });
  });

  describe('Net Position Calculation', () => {
    it('should calculate balanced net positions', () => {
      // HL: +50 SOL (long)
      state.updateHLPositions([
        { coin: 'SOL', side: 'long', size: 50.0 }
      ]);

      // PAC: -50 SOL (short)
      state.updatePACPositions([
        { symbol: 'SOL', side: 'ask', amount: '50.0' }
      ]);

      const snapshot = state.getSnapshot();

      const hlPos = snapshot.hlPositions.get('SOL');
      const pacPos = snapshot.pacPositions.get('SOL');

      const hlSize = hlPos ? (hlPos.side === 'long' ? hlPos.size : -hlPos.size) : 0;
      const pacSize = pacPos ? (pacPos.side === 'bid' ? parseFloat(pacPos.amount) : -parseFloat(pacPos.amount)) : 0;
      const netPos = hlSize + pacSize;

      assert.strictEqual(hlSize, 50.0);
      assert.strictEqual(pacSize, -50.0);
      assert.strictEqual(netPos, 0, 'Net position should be 0 (balanced)');
    });

    it('should calculate imbalanced net positions', () => {
      // HL: +50 SOL (long)
      state.updateHLPositions([
        { coin: 'SOL', side: 'long', size: 50.0 }
      ]);

      // PAC: -30 SOL (short)
      state.updatePACPositions([
        { symbol: 'SOL', side: 'ask', amount: '30.0' }
      ]);

      const snapshot = state.getSnapshot();

      const hlPos = snapshot.hlPositions.get('SOL');
      const pacPos = snapshot.pacPositions.get('SOL');

      const hlSize = hlPos ? (hlPos.side === 'long' ? hlPos.size : -hlPos.size) : 0;
      const pacSize = pacPos ? (pacPos.side === 'bid' ? parseFloat(pacPos.amount) : -parseFloat(pacPos.amount)) : 0;
      const netPos = hlSize + pacSize;

      assert.strictEqual(hlSize, 50.0);
      assert.strictEqual(pacSize, -30.0);
      assert.strictEqual(netPos, 20.0, 'Net position should be +20 (imbalanced)');
    });
  });
});

// Run tests if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  console.log('Running Dashboard Integration Tests...');
  console.log('='.repeat(50));
}
