#!/usr/bin/env node
import React, { useState, useEffect, createElement as h } from 'react';
import { render, Box, Text } from 'ink';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import dotenv from 'dotenv';
import HyperliquidConnector from './connectors/hyperliquid.js';
import PacificaConnector from './connectors/pacifica.js';

// Simple table component (replacing ink-table)
const Table = ({ data }) => {
  if (!data || data.length === 0) {
    return h(Text, { color: 'gray' }, 'No data');
  }

  const headers = Object.keys(data[0]);
  const columnWidths = {};

  // Calculate column widths with padding
  headers.forEach(header => {
    columnWidths[header] = Math.max(
      header.length,
      ...data.map(row => String(row[header] || '').length)
    ) + 4; // Add 4 spaces padding for better column separation
  });

  // Render header with proper alignment
  const headerText = headers.map(h => {
    const colName = String(h);
    return colName.padEnd(columnWidths[h]);
  }).join('  '); // Add explicit 2-space separator

  const separator = headers.map(h => '─'.repeat(columnWidths[h])).join('  '); // Match header separator

  return h(Box, { flexDirection: 'column' },
    h(Text, { bold: true, color: 'cyan' }, headerText),
    h(Text, { color: 'gray' }, separator),
    ...data.map((row, i) => {
      const rowText = headers.map(h => {
        const value = row[h] !== undefined && row[h] !== null ? String(row[h]) : '';
        // Right-align numbers, left-align text
        const isNumber = value && !isNaN(parseFloat(value)) && value.match(/^-?\d+\.?\d*$/);
        return isNumber ? value.padStart(columnWidths[h]) : value.padEnd(columnWidths[h]);
      }).join('  '); // Add explicit 2-space separator
      return h(Text, { key: i }, rowText);
    })
  );
};

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ============================================================================
// DASHBOARD STATE CLASS
// ============================================================================

class DashboardState {
  constructor() {
    // Balances and equity
    this.hlBalance = null;
    this.hlEquity = null;
    this.pacBalance = null;
    this.pacEquity = null;

    // Positions (Map: symbol -> position)
    this.hlPositions = new Map();
    this.pacPositions = new Map();

    // Orders (Map: orderId -> order)
    this.hlOrders = new Map();
    this.pacOrders = new Map();

    // Recent fills (last 10)
    this.recentFills = [];
    this.maxFills = 10;

    // Recent orders (last 8)
    this.recentOrders = [];
    this.maxOrders = 8;

    // Track seen fill IDs for deduplication
    this.seenFillIds = new Set();

    // Track seen order IDs for deduplication
    this.seenOrderIds = new Set();

    // Dirty flag
    this.dirty = true;

    // Last update timestamp
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

  // Update balances (only update if non-zero to avoid displaying temporary API issues)
  updateHLBalance(balance, equity) {
    // Only update if values are valid (non-zero)
    if (balance > 0 || equity > 0) {
      this.hlBalance = balance;
      this.hlEquity = equity;
      this.markDirty();
    }
    // If both are zero, keep previous values (likely a temporary API issue)
  }

  updatePACBalance(balance, equity) {
    // Only update if values are valid (non-zero)
    if (balance > 0 || equity > 0) {
      this.pacBalance = balance;
      this.pacEquity = equity;
      this.markDirty();
    }
    // If both are zero, keep previous values (likely a temporary API issue)
  }

  // Update positions
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

  // Update orders
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

  // Add fill with deduplication
  addFill(fill) {
    const fillId = fill.id || fill.tradeId || fill.hash || `${fill.timestamp}-${fill.coin || fill.symbol}`;

    if (this.seenFillIds.has(fillId)) {
      return; // Already seen
    }

    this.seenFillIds.add(fillId);
    this.recentFills.unshift(fill);

    // Keep only maxFills
    if (this.recentFills.length > this.maxFills) {
      const removed = this.recentFills.pop();
      const removedId = removed.id || removed.tradeId || removed.hash || `${removed.timestamp}-${removed.coin || removed.symbol}`;
      this.seenFillIds.delete(removedId);
    }

    this.markDirty();
  }

  // Add order to recent orders with deduplication
  addRecentOrder(order) {
    // Create unique ID for this order (prioritize order_id, use created_at consistently for fallback)
    const orderId = order.orderId || order.order_id ||
                   `${order.exchange}-${order.symbol}-${order.created_at || order.timestamp}-${order.side}`;

    // Skip if already seen
    if (this.seenOrderIds.has(orderId)) {
      return;
    }

    this.seenOrderIds.add(orderId);
    this.recentOrders.unshift(order);

    // Keep only maxOrders
    if (this.recentOrders.length > this.maxOrders) {
      const removed = this.recentOrders.pop();
      const removedId = removed.orderId || removed.order_id ||
                       `${removed.exchange}-${removed.symbol}-${removed.timestamp || removed.created_at}-${removed.side}`;
      this.seenOrderIds.delete(removedId);
    }

    this.markDirty();
  }

  // Replace recent orders with a fresh snapshot (used for polling to avoid accumulation issues)
  replaceRecentOrders(orders) {
    // Clear existing orders and deduplication set
    this.recentOrders = [];
    this.seenOrderIds.clear();

    // Add orders in reverse to maintain newest-first order (appendleft behavior)
    for (let i = orders.length - 1; i >= 0; i--) {
      const order = orders[i];
      const orderId = order.orderId || order.order_id ||
                     `${order.exchange}-${order.symbol}-${order.created_at || order.timestamp}-${order.side}`;

      this.seenOrderIds.add(orderId);
      this.recentOrders.unshift(order);
    }

    this.markDirty();
  }
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function formatAge(timestamp) {
  const ageMs = Date.now() - timestamp;
  const ageSec = Math.floor(ageMs / 1000);

  if (ageSec < 60) {
    return `${ageSec}s`;
  } else if (ageSec < 3600) {
    return `${Math.floor(ageSec / 60)}m`;
  } else {
    return `${Math.floor(ageSec / 3600)}h`;
  }
}

function formatTimestamp(timestamp) {
  return new Date(timestamp).toLocaleTimeString();
}

function formatNumber(num, decimals = 2) {
  if (num === null || num === undefined) return '-';
  return Number(num).toFixed(decimals);
}

// ============================================================================
// INK UI COMPONENTS
// ============================================================================

const Header = ({ lastUpdate }) =>
  h(Box, { flexDirection: 'column' },
    h(Text, { bold: true, color: 'cyan' }, 'XEMM Dashboard - Live'),
    h(Text, { color: 'gray' }, `Updated: ${formatTimestamp(lastUpdate)}`)
  );

const BalanceTable = ({ hlBalance, hlEquity, pacBalance, pacEquity }) => {
  const data = [
    {
      Exchange: 'Hyperliquid',
      Available: formatNumber(hlBalance),
      Equity: formatNumber(hlEquity),
      Status: hlBalance !== null ? ' ✓' : ' ⏳'
    },
    {
      Exchange: 'Pacifica',
      Available: formatNumber(pacBalance),
      Equity: formatNumber(pacEquity),
      Status: pacBalance !== null ? ' ✓' : ' ⏳'
    }
  ];

  return h(Box, { flexDirection: 'column' },
    h(Text, { bold: true, color: 'yellow' }, '\nAccount Balances'),
    h(Table, { data })
  );
};

const PositionTable = ({ hlPositions, pacPositions, symbols }) => {
  const data = [];

  symbols.forEach(symbol => {
    const hlPos = hlPositions.get(symbol);
    const pacPos = pacPositions.get(symbol);

    const hlSize = hlPos ? (hlPos.side === 'long' ? hlPos.size : -hlPos.size) : 0;
    // Fixed: Pacifica connector returns 'long'/'short', not 'bid'/'ask'
    const pacSize = pacPos ? (pacPos.side === 'long' ? parseFloat(pacPos.amount) : -parseFloat(pacPos.amount)) : 0;
    const netPos = hlSize + pacSize;

    const isBalanced = Math.abs(netPos) < 0.01;

    data.push({
      Symbol: symbol,
      'HL Pos': formatNumber(hlSize, 4),
      'PAC Pos': formatNumber(pacSize, 4),
      'Net Pos': formatNumber(netPos, 4),
      Status: isBalanced ? ' ✓ BALANCED' : ' ⚠️  IMBALANCED'
    });
  });

  return h(Box, { flexDirection: 'column' },
    h(Text, { bold: true, color: 'green' }, '\nNet Positions'),
    h(Table, { data })
  );
};

const OpenOrdersTable = ({ hlOrders, pacOrders }) => {
  const data = [];

  // Add HL orders
  hlOrders.forEach((order) => {
    data.push({
      Exchange: 'HL',
      Symbol: order.coin,
      Side: order.side.toUpperCase(),
      Price: formatNumber(order.limitPrice, 4),
      Size: formatNumber(order.size, 4),
      Age: formatAge(order.timestamp)
    });
  });

  // Add PAC orders
  pacOrders.forEach((order) => {
    data.push({
      Exchange: 'PAC',
      Symbol: order.symbol || '-',
      Side: (order.side || '').toUpperCase(),
      Price: formatNumber(parseFloat(order.initial_price || order.price || 0), 4),
      Size: formatNumber(parseFloat(order.initial_amount || order.amount || order.size || 0), 4),
      Age: formatAge(order.created_at || order.timestamp || Date.now())
    });
  });

  // Sort by age (newest first)
  data.sort((a, b) => {
    const ageA = parseInt(a.Age);
    const ageB = parseInt(b.Age);
    return ageA - ageB;
  });

  // Pad with empty rows to maintain consistent height (minimum 3 rows)
  const minRows = 3;
  const displayData = data.slice(0, 10);
  while (displayData.length < minRows) {
    displayData.push({
      Exchange: '',
      Symbol: '',
      Side: '',
      Price: '',
      Size: '',
      Age: ''
    });
  }

  return h(Box, { flexDirection: 'column' },
    h(Text, { bold: true, color: 'blue' }, `\nOpen Orders (${data.length})`),
    h(Table, { data: displayData })
  );
};

const RecentOrdersTable = ({ recentOrders }) => {
  // Data is already sorted by the snapshot approach (newest first)
  const data = recentOrders.slice(0, 8).map(order => ({
    Time: formatTimestamp(order.timestamp || order.created_at),
    Exchange: order.exchange,
    Symbol: order.coin || order.symbol,
    Side: (order.side || '').toUpperCase(),
    Size: formatNumber(order.size || order.amount, 4),
    Status: order.status || 'FILLED'
  }));

  // Pad with empty rows to maintain consistent height (always 8 rows)
  while (data.length < 8) {
    data.push({
      Time: '',
      Exchange: '',
      Symbol: '',
      Side: '',
      Size: '',
      Status: ''
    });
  }

  return h(Box, { flexDirection: 'column' },
    h(Text, { bold: true, color: 'magenta' }, '\nRecent Orders (Last 8)'),
    h(Table, { data })
  );
};

const RecentFillsTable = ({ recentFills }) => {
  const data = recentFills.slice(0, 10).map(fill => ({
    Time: formatTimestamp(fill.timestamp || fill.created_at),
    Exchange: fill.exchange,
    Symbol: fill.coin || fill.symbol,
    Side: (fill.side || '').toUpperCase(),
    Price: formatNumber(fill.price, 4),
    Size: formatNumber(fill.size || fill.amount, 4),
    PnL: formatNumber(fill.closedPnl || fill.pnl || 0, 2)
  }));

  // Pad with empty rows to maintain consistent height (always 10 rows)
  while (data.length < 10) {
    data.push({
      Time: '',
      Exchange: '',
      Symbol: '',
      Side: '',
      Price: '',
      Size: '',
      PnL: ''
    });
  }

  return h(Box, { flexDirection: 'column' },
    h(Text, { bold: true, color: 'cyan' }, '\nRecent Fills (Last 10)'),
    h(Table, { data })
  );
};

// Main Dashboard Component
const DashboardUI = ({ snapshot, symbols }) => {
  return h(Box, { flexDirection: 'column' },
    h(Header, { lastUpdate: snapshot.lastUpdate }),
    h(BalanceTable, {
      hlBalance: snapshot.hlBalance,
      hlEquity: snapshot.hlEquity,
      pacBalance: snapshot.pacBalance,
      pacEquity: snapshot.pacEquity
    }),
    h(PositionTable, {
      hlPositions: snapshot.hlPositions,
      pacPositions: snapshot.pacPositions,
      symbols: symbols
    }),
    h(OpenOrdersTable, {
      hlOrders: snapshot.hlOrders,
      pacOrders: snapshot.pacOrders
    }),
    h(RecentOrdersTable, { recentOrders: snapshot.recentOrders }),
    h(RecentFillsTable, { recentFills: snapshot.recentFills })
  );
};

// ============================================================================
// DASHBOARD CONTROLLER CLASS
// ============================================================================

class Dashboard {
  constructor(configPath) {
    // Load configuration
    this.config = JSON.parse(readFileSync(configPath, 'utf-8'));
    this.state = new DashboardState();

    // Connectors
    this.hlConnector = null;
    this.pacConnector = null;

    // Polling intervals
    this.pollingTasks = [];

    // Shutdown flag
    this.shutdown = false;
  }

  async initialize() {
    // Permanently suppress all console output to prevent dashboard movement
    const originalLog = console.log;
    const originalError = console.error;
    const originalWarn = console.warn;

    console.log = () => {};
    console.error = () => {};
    console.warn = () => {};

    try {
      // Initialize Hyperliquid connector
      this.hlConnector = new HyperliquidConnector({
        wallet: process.env.HL_WALLET,
        privateKey: process.env.HL_PRIVATE_KEY,
        silent: true
      });

      // Initialize Pacifica connector
      this.pacConnector = new PacificaConnector({
        wallet: process.env.SOL_WALLET,
        apiPublic: process.env.API_PUBLIC,
        apiPrivate: process.env.API_PRIVATE,
        silent: true
      });

      // Connect to exchanges (WebSocket)
      await this.hlConnector.connect();
      await this.pacConnector.connect();
    } finally {
      // Keep console suppressed - don't restore
      // This prevents connector reconnection messages from disrupting the UI
    }

    // Set up WebSocket event handlers
    this.setupEventHandlers();

    // Bootstrap initial data
    await this.bootstrapData();

    // Start polling tasks
    this.startPolling();
  }

  setupEventHandlers() {
    // Pacifica WebSocket events - real-time order and fill updates
    this.pacConnector.on('fill', (data) => {
      // Only add to fills (don't add to recent orders - fills are shown in Recent Fills section)
      this.state.addFill({
        exchange: 'PAC',
        symbol: data.symbol,
        side: data.tradeSide,
        price: data.price,
        amount: data.amount,
        timestamp: data.timestamp,
        pnl: data.pnl || 0
      });
    });

    this.pacConnector.on('orderUpdate', (data) => {
      // Update open orders map based on order status
      if (data.orderStatus === 'open' || data.orderStatus === 'partially_filled') {
        // Add/update open order
        this.state.pacOrders.set(data.orderId, {
          order_id: data.orderId,
          client_order_id: data.clientOrderId,
          symbol: data.symbol,
          side: data.side,
          price: data.initialPrice,
          initial_price: data.initialPrice,
          initial_amount: data.amount,
          filled_amount: data.filledAmount,
          order_status: data.orderStatus,
          order_type: data.orderType,
          created_at: data.createdAt,
          updated_at: data.updatedAt
        });
      } else {
        // Remove from open orders if filled/cancelled
        this.state.pacOrders.delete(data.orderId);

        // Add to recent orders for cancelled/rejected orders
        if (data.orderStatus === 'cancelled' || data.orderStatus === 'rejected') {
          this.state.addRecentOrder({
            exchange: 'PAC',
            symbol: data.symbol,
            side: data.side,
            amount: data.amount,
            status: data.orderStatus.toUpperCase(),
            timestamp: data.updatedAt,
            created_at: data.createdAt,
            orderId: data.orderId
          });
        }
      }
      this.state.markDirty();
    });

    this.pacConnector.on('orderFilled', (data) => {
      // Remove from open orders
      this.state.pacOrders.delete(data.orderId);

      // Don't add filled orders to recent orders (they're in Recent Fills)
      this.state.markDirty();
    });
  }

  async bootstrapData() {
    try {
      // Fetch initial HL data
      await this.pollHLData();

      // Fetch initial PAC data
      await this.pollPACData();

      // Subscribe to Pacifica WebSocket order updates (real-time)
      await this.pacConnector.subscribeOrderUpdates(process.env.SOL_WALLET);
    } catch (error) {
      console.error('[Dashboard] Bootstrap error:', error);
    }
  }

  async pollHLData() {
    try {
      // Fetch user state (balance + positions)
      const userState = await this.hlConnector.getUserState();

      // Update balance
      this.state.updateHLBalance(
        userState.balance.withdrawable,
        userState.balance.accountValue
      );

      // Update positions
      this.state.updateHLPositions(userState.positions);

      // Fetch open orders
      const orders = await this.hlConnector.getOpenOrders();
      this.state.updateHLOrders(orders);

      // Fetch recent fills
      const oneDayAgo = Date.now() - (24 * 60 * 60 * 1000);
      const fills = await this.hlConnector.getUserFills(null, oneDayAgo);

      // Add new fills (don't add to recent orders - fills are shown in Recent Fills section)
      fills.forEach(fill => {
        this.state.addFill({
          ...fill,
          exchange: 'HL'
        });
      });

    } catch (error) {
      console.error('[Dashboard] Error polling HL data:', error.message);
    }
  }

  async pollPACData() {
    try {
      // Fetch account balance via WebSocket
      const balanceData = await this.pacConnector.getBalance();

      // Update balance (using correct field names from Pacifica connector)
      const available = balanceData.availableToSpend || balanceData.balance || 0;
      const equity = balanceData.accountEquity || 0;

      this.state.updatePACBalance(available, equity);

      // Fetch positions
      const positions = await this.pacConnector.getPositions();

      // Update positions
      this.state.updatePACPositions(positions);

      // Fetch open orders (requestOrderStatusRest returns data array directly)
      const pacOrders = await this.pacConnector.requestOrderStatusRest(process.env.SOL_WALLET);
      if (Array.isArray(pacOrders)) {
        this.state.updatePACOrders(pacOrders);
      }

      // Fetch recent fills from order history (returns data array directly)
      // Fetch 200 orders to catch more history and reduce gaps
      const orderHistory = await this.pacConnector.requestOrderHistoryRest(process.env.SOL_WALLET, 200);

      if (Array.isArray(orderHistory)) {
        // First, separate filled orders and cancelled/rejected orders
        const filledOrders = [];
        const cancelledOrders = [];

        orderHistory.forEach(order => {
          if (parseFloat(order.filled_amount || 0) > 0) {
            filledOrders.push(order);
          }
          if (order.order_status === 'cancelled' || order.order_status === 'rejected') {
            cancelledOrders.push(order);
          }
        });

        // Sort cancelled orders by updated_at DESC (newest first)
        cancelledOrders.sort((a, b) => (b.updated_at || 0) - (a.updated_at || 0));

        // Add filled orders to fills
        filledOrders.forEach(order => {
          this.state.addFill({
            id: order.order_id,
            exchange: 'PAC',
            symbol: order.symbol,
            side: order.side,
            price: parseFloat(order.average_filled_price || order.initial_price),
            amount: parseFloat(order.filled_amount),
            timestamp: order.updated_at,
            pnl: 0
          });
        });

        // SNAPSHOT APPROACH: Replace recent orders with the latest 8 (don't accumulate)
        // This ensures we always show the TRUE latest 8 orders from the API
        const recentOrdersSnapshot = cancelledOrders.slice(0, 8).map(order => ({
          exchange: 'PAC',
          symbol: order.symbol,
          side: order.side,
          amount: parseFloat(order.amount || order.initial_amount || 0),
          status: order.order_status.toUpperCase(),
          timestamp: order.updated_at,
          created_at: order.created_at,
          order_id: order.order_id
        }));

        // Replace the entire recent orders list with this snapshot
        this.state.replaceRecentOrders(recentOrdersSnapshot);
      }

    } catch (error) {
      console.error('[Dashboard] Error polling PAC data:', error.message);
    }
  }

  startPolling() {
    // Poll HL data every 30 seconds (WebSocket not available for user data)
    this.pollingTasks.push(
      setInterval(() => {
        if (!this.shutdown) {
          this.pollHLData();
        }
      }, 30000)
    );

    // Poll PAC data every 15 seconds (faster backup to catch orders during WebSocket delays)
    this.pollingTasks.push(
      setInterval(() => {
        if (!this.shutdown) {
          this.pollPACData();
        }
      }, 15000)
    );
  }

  stopPolling() {
    this.pollingTasks.forEach(task => clearInterval(task));
    this.pollingTasks = [];
  }

  async cleanup() {
    this.shutdown = true;
    this.stopPolling();

    if (this.hlConnector) {
      this.hlConnector.disconnect();
    }

    if (this.pacConnector) {
      this.pacConnector.disconnect();
    }
  }
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

async function main() {
  const configPath = join(__dirname, 'dashboard', 'config.json');
  const dashboard = new Dashboard(configPath);

  try {
    // Initialize dashboard
    await dashboard.initialize();

    // Create a React component that polls the state
    const App = () => {
      const [snapshot, setSnapshot] = useState(dashboard.state.getSnapshot());

      useEffect(() => {
        // Update UI every 2 seconds if state is dirty (reduces flashing and movement)
        const interval = setInterval(() => {
          if (dashboard.state.dirty) {
            setSnapshot(dashboard.state.getSnapshot());
          }
        }, 2000);

        return () => clearInterval(interval);
      }, []);

      return h(DashboardUI, { snapshot, symbols: dashboard.config.symbols });
    };

    // Render the UI with patchConsole disabled to prevent output interference
    const { unmount } = render(h(App), { patchConsole: false });

    // Handle graceful shutdown
    const handleShutdown = async () => {
      unmount();
      await dashboard.cleanup();
      process.exit(0);
    };

    process.on('SIGINT', handleShutdown);
    process.on('SIGTERM', handleShutdown);

  } catch (error) {
    console.error('[Dashboard] Fatal error:', error);
    await dashboard.cleanup();
    process.exit(1);
  }
}

// Run if executed directly
const isMainModule = import.meta.url === `file:///${__filename.replace(/\\/g, '/')}` ||
                     import.meta.url.endsWith('/dashboard.js') ||
                     import.meta.url.endsWith('\\dashboard.js');

if (isMainModule) {
  main().catch((error) => {
    console.error('[Dashboard] Fatal error:', error);
    process.exit(1);
  });
}

export default Dashboard;
