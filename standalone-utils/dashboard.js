#!/usr/bin/env node
import React, { useState, useEffect, createElement as h } from 'react';
import { render, Box, Text, Spacer } from 'ink';
import { readFileSync, existsSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import dotenv from 'dotenv';
import HyperliquidConnector from './connectors/hyperliquid.js';
import PacificaConnector from './connectors/pacifica.js';

// ============================================================================
// DEBUG MODE
// ============================================================================
const DEBUG_MODE = process.env.DEBUG === 'true' || process.env.DEBUG === '1';

const logger = {
  log: (...args) => DEBUG_MODE && console.log('[Dashboard]', ...args),
  error: (...args) => console.error('[Dashboard ERROR]', ...args),
  warn: (...args) => DEBUG_MODE && console.warn('[Dashboard WARN]', ...args),
  info: (...args) => DEBUG_MODE && console.log('[Dashboard INFO]', ...args)
};

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ============================================================================
// UI COMPONENTS (STYLED)
// ============================================================================

// A wrapper that creates a bordered "Card" with a title on the border
const Section = ({ title, children, color = 'cyan', width = '100%', height }) => {
  return h(Box, { 
    flexDirection: 'column', 
    borderStyle: 'round', 
    borderColor: color, 
    paddingX: 1,
    width: width,
    height: height,
    marginLeft: 0,
    marginRight: 1,
    marginBottom: 0
  },
    h(Box, { marginTop: -1, marginLeft: 1 },
      h(Text, { color: color, bold: true }, ` ${title} `)
    ),
    children
  );
};

const Table = ({ data, highlightFn }) => {
  if (!data || data.length === 0) {
    return h(Box, { height: 3, justifyContent: 'center', alignItems: 'center' },
      h(Text, { color: 'gray', italic: true }, 'No data available')
    );
  }

  const headers = Object.keys(data[0]);
  const columnWidths = {};

  // Calculate column widths
  headers.forEach(header => {
    columnWidths[header] = Math.max(
      header.length,
      ...data.map(row => String(row[header] || '').length)
    ) + 2; // Add padding
  });

  // Render Header
  const headerRow = headers.map(h => {
    return h.padEnd(columnWidths[h]);
  }).join(' ');

  return h(Box, { flexDirection: 'column' },
    h(Box, { borderStyle: 'single', borderTop: false, borderLeft: false, borderRight: false, borderColor: 'gray', marginBottom: 0 },
      h(Text, { bold: true, color: 'white' }, headerRow)
    ),
    ...data.map((row, i) => {
      // Allow custom coloring of rows based on data
      const rowColor = highlightFn ? highlightFn(row) : undefined;
      
      const rowText = headers.map(header => {
        const value = row[header] !== undefined && row[header] !== null ? String(row[header]) : '';
        const isNumber = value && !isNaN(parseFloat(value)) && value.match(/^-?\d+\.?\d*$/);
        return isNumber ? value.padStart(columnWidths[header]) : value.padEnd(columnWidths[header]);
      }).join(' ');

      return h(Text, { key: i, color: rowColor }, rowText);
    })
  );
};

// ============================================================================
// STATE & LOGIC
// ============================================================================

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
    this.maxSeenIds = 1000;
    this.seenOrderIds = new Set();
    this.hlConnected = false;
    this.hlReconnecting = false;
    this.pacConnected = false;
    this.pacReconnecting = false;
    this.hlLastFetch = null;
    this.pacLastFetch = null;
    this.lastError = null;
    this.errorCount = 0;
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
      hlConnected: this.hlConnected,
      hlReconnecting: this.hlReconnecting,
      pacConnected: this.pacConnected,
      pacReconnecting: this.pacReconnecting,
      hlLastFetch: this.hlLastFetch,
      pacLastFetch: this.pacLastFetch,
      lastError: this.lastError,
      errorCount: this.errorCount,
      lastUpdate: this.lastUpdate
    };
  }

  markDirty() {
    this.dirty = true;
    this.lastUpdate = Date.now();
  }

  updateHLBalance(balance, equity) {
    const isValidUpdate = balance !== undefined && equity !== undefined && (balance > 0 || equity > 0 || this.hlBalance === null);
    if (isValidUpdate) {
      this.hlBalance = balance;
      this.hlEquity = equity;
      this.hlLastFetch = Date.now();
      this.markDirty();
    }
  }

  updatePACBalance(balance, equity) {
    const isValidUpdate = balance !== undefined && equity !== undefined && (balance > 0 || equity > 0 || this.pacBalance === null);
    if (isValidUpdate) {
      this.pacBalance = balance;
      this.pacEquity = equity;
      this.pacLastFetch = Date.now();
      this.markDirty();
    }
  }

  updateHLPositions(positions) {
    this.hlPositions.clear();
    positions.forEach(pos => this.hlPositions.set(pos.coin, pos));
    this.markDirty();
  }

  updatePACPositions(positions) {
    this.pacPositions.clear();
    positions.forEach(pos => this.pacPositions.set(pos.symbol, pos));
    this.markDirty();
  }

  updateHLOrders(orders) {
    this.hlOrders.clear();
    orders.forEach(order => this.hlOrders.set(order.orderId, order));
    this.markDirty();
  }

  updatePACOrders(orders) {
    this.pacOrders.clear();
    orders.forEach(order => this.pacOrders.set(order.order_id, order));
    this.markDirty();
  }

  addFill(fill) {
    const fillId = fill.id || fill.tradeId || fill.hash || `${fill.timestamp}-${fill.coin || fill.symbol}`;
    if (this.seenFillIds.has(fillId)) return;
    if (this.seenFillIds.size >= this.maxSeenIds) {
      const toRemove = Array.from(this.seenFillIds).slice(0, Math.floor(this.maxSeenIds / 2));
      toRemove.forEach(id => this.seenFillIds.delete(id));
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
    const orderId = order.orderId || order.order_id || `${order.exchange}-${order.symbol}-${order.created_at || order.timestamp}-${order.side}`;
    if (this.seenOrderIds.has(orderId)) return;
    if (this.seenOrderIds.size >= this.maxSeenIds) {
      const toRemove = Array.from(this.seenOrderIds).slice(0, Math.floor(this.maxSeenIds / 2));
      toRemove.forEach(id => this.seenOrderIds.delete(id));
    }
    this.seenOrderIds.add(orderId);
    this.recentOrders.unshift(order);
    if (this.recentOrders.length > this.maxOrders) {
      const removed = this.recentOrders.pop();
      const removedId = removed.orderId || removed.order_id || `${removed.exchange}-${removed.symbol}-${removed.timestamp || removed.created_at}-${removed.side}`;
      this.seenOrderIds.delete(removedId);
    }
    this.markDirty();
  }

  replaceRecentOrders(orders) {
    this.recentOrders = [];
    this.seenOrderIds.clear();
    for (let i = orders.length - 1; i >= 0; i--) {
      const order = orders[i];
      const orderId = order.orderId || order.order_id || `${order.exchange}-${order.symbol}-${order.created_at || order.timestamp}-${order.side}`;
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
  if (ageSec < 60) return `${ageSec}s`;
  if (ageSec < 3600) return `${Math.floor(ageSec / 60)}m`;
  return `${Math.floor(ageSec / 3600)}h`;
}

function formatTimestamp(timestamp) {
  return new Date(timestamp).toLocaleTimeString();
}

function formatNumber(num, decimals = 2) {
  if (num === null || num === undefined) return '-';
  return Number(num).toFixed(decimals);
}

// ============================================================================
// UI SECTIONS
// ============================================================================

const Header = ({ lastUpdate, hlConnected, hlReconnecting, pacConnected, pacReconnecting, errorCount }) => {
  const StatusBadge = ({ label, connected, reconnecting }) => {
    const color = reconnecting ? 'yellow' : connected ? 'green' : 'red';
    const icon = reconnecting ? '' : connected ? '*' : 'o';
    return h(Text, { color }, `${icon} ${label} `);
  };

  return h(Box, { flexDirection: 'column', marginBottom: 1 },
    h(Box, { justifyContent: 'space-between', borderStyle: 'double', borderColor: 'blue', paddingX: 1 },
      h(Text, { bold: true, color: 'blueBright' }, ' XEMM DASHBOARD '),
      h(Text, { color: 'gray' }, `Last Update: ${formatTimestamp(lastUpdate)}`)
    ),
    h(Box, { paddingX: 1, marginTop: 0 },
      h(StatusBadge, { label: 'Hyperliquid', connected: hlConnected, reconnecting: hlReconnecting }),
      h(Text, { color: 'gray' }, '| '),
      h(StatusBadge, { label: 'Pacifica', connected: pacConnected, reconnecting: pacReconnecting }),
      h(Spacer),
      errorCount > 0 && h(Text, { color: 'red', bold: true }, `WARN Errors: ${errorCount}`)
    )
  );
};

const BalanceTable = ({ hlBalance, hlEquity, pacBalance, pacEquity }) => {
  const data = [
    { Exch: 'HL', Avail: formatNumber(hlBalance), Equity: formatNumber(hlEquity) },
    { Exch: 'PAC', Avail: formatNumber(pacBalance), Equity: formatNumber(pacEquity) }
  ];

  return h(Section, { title: 'Balances', color: 'yellow', width: '40%' },
    h(Table, { data })
  );
};

const PositionTable = ({ hlPositions, pacPositions, symbols }) => {
  const data = symbols.map(symbol => {
    const hlPos = hlPositions.get(symbol);
    const pacPos = pacPositions.get(symbol);
    const hlSize = hlPos ? (hlPos.side === 'long' ? hlPos.size : -hlPos.size) : 0;
    const pacSize = pacPos ? (pacPos.side === 'long' ? parseFloat(pacPos.amount) : -parseFloat(pacPos.amount)) : 0;
    const netPos = hlSize + pacSize;
    const isBalanced = Math.abs(netPos) < 0.01;

    return {
      Sym: symbol,
      'HL': formatNumber(hlSize, 2),
      'PAC': formatNumber(pacSize, 2),
      'Net': formatNumber(netPos, 2),
      Stat: isBalanced ? 'OK' : 'IMBAL'
    };
  });

  const highlightFn = (row) => row.Stat === 'IMBAL' ? 'red' : 'green';

  return h(Section, { title: 'Net Positions', color: 'green', width: '60%' },
    h(Table, { data, highlightFn })
  );
};

const OpenOrdersTable = ({ hlOrders, pacOrders }) => {
  const data = [];
  
  hlOrders.forEach(o => data.push({
    Ex: 'HL', Sym: o.coin, Side: o.side.toUpperCase(), Px: formatNumber(o.limitPrice), Sz: formatNumber(o.size), Age: formatAge(o.timestamp)
  }));
  
  pacOrders.forEach(o => data.push({
    Ex: 'PAC', Sym: o.symbol || '-', Side: (o.side || '').toUpperCase(), Px: formatNumber(o.initial_price || o.price), Sz: formatNumber(o.initial_amount || o.amount), Age: formatAge(o.created_at || o.timestamp)
  }));

  data.sort((a, b) => parseInt(a.Age) - parseInt(b.Age));
  const displayData = data.slice(0, 5); // Show max 5 open orders

  return h(Section, { title: `Open Orders (${data.length})`, color: 'blue' },
    h(Table, { data: displayData })
  );
};

const RecentOrdersTable = ({ recentOrders }) => {
  const data = recentOrders.slice(0, 5).map(o => ({
    Time: formatTimestamp(o.timestamp || o.created_at),
    Ex: o.exchange,
    Sym: o.coin || o.symbol,
    Side: (o.side || '').toUpperCase(),
    Sz: formatNumber(o.size || o.amount, 2),
    Stat: o.status || 'FILLED'
  }));

  const highlightFn = (row) => row.Stat === 'CANCELLED' ? 'gray' : 'white';

  return h(Section, { title: 'Recent Activity', color: 'magenta' },
    h(Table, { data, highlightFn })
  );
};

const RecentFillsTable = ({ recentFills }) => {
  const data = recentFills.slice(0, 5).map(f => ({
    Time: formatTimestamp(f.timestamp || f.created_at),
    Ex: f.exchange,
    Sym: f.coin || f.symbol,
    Side: (f.side || '').toUpperCase(),
    Px: formatNumber(f.price),
    Sz: formatNumber(f.size || f.amount, 2),
    PnL: formatNumber(f.closedPnl || f.pnl || 0, 2)
  }));

  const highlightFn = (row) => parseFloat(row.PnL) > 0 ? 'green' : parseFloat(row.PnL) < 0 ? 'red' : undefined;

  return h(Section, { title: 'Fills & PnL', color: 'cyan' },
    h(Table, { data, highlightFn })
  );
};

const DashboardUI = ({ snapshot, symbols }) => {
  return h(Box, { flexDirection: 'column', padding: 1 },
    h(Header, {
      lastUpdate: snapshot.lastUpdate,
      hlConnected: snapshot.hlConnected,
      hlReconnecting: snapshot.hlReconnecting,
      pacConnected: snapshot.pacConnected,
      pacReconnecting: snapshot.pacReconnecting,
      errorCount: snapshot.errorCount
    }),
    
    // Grid Layout: Balances (Left) + Positions (Right)
    h(Box, { flexDirection: 'row', width: '100%', marginBottom: 1 },
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
      })
    ),

    h(OpenOrdersTable, { hlOrders: snapshot.hlOrders, pacOrders: snapshot.pacOrders }),
    
    // Grid Layout: Recent Orders + Fills
    h(Box, { flexDirection: 'column' },
      h(RecentOrdersTable, { recentOrders: snapshot.recentOrders }),
      h(RecentFillsTable, { recentFills: snapshot.recentFills })
    )
  );
};

// ============================================================================
// CONTROLLER
// ============================================================================

class Dashboard {
  constructor(configPath) {
    if (!existsSync(configPath)) throw new Error(`Config not found: ${configPath}`);
    this.config = JSON.parse(readFileSync(configPath, 'utf-8'));
    this.state = new DashboardState();
    this.hlConnector = null;
    this.pacConnector = null;
    this.pollingTasks = [];
    this.shutdown = false;
  }

  async initialize() {
    if (!DEBUG_MODE) {
      console.log = () => {};
      console.error = () => {};
      console.warn = () => {};
    }

    this.hlConnector = new HyperliquidConnector({
      wallet: process.env.HL_WALLET,
      privateKey: process.env.HL_PRIVATE_KEY,
      silent: !DEBUG_MODE
    });

    this.pacConnector = new PacificaConnector({
      wallet: process.env.SOL_WALLET,
      apiPublic: process.env.API_PUBLIC,
      apiPrivate: process.env.API_PRIVATE,
      silent: !DEBUG_MODE
    });

    this.setupConnectionListeners();
    await this.hlConnector.connect();
    await this.pacConnector.connect();
    this.state.hlConnected = true;
    this.state.pacConnected = true;
  }

  setupConnectionListeners() {
    this.hlConnector.on('connected', () => { this.state.hlConnected = true; this.state.hlReconnecting = false; this.state.markDirty(); });
    this.hlConnector.on('disconnected', () => { this.state.hlConnected = false; this.state.markDirty(); });
    this.hlConnector.on('error', (e) => { this.state.lastError = `HL: ${e.message}`; this.state.errorCount++; this.state.markDirty(); });
    this.pacConnector.on('connected', () => { this.state.pacConnected = true; this.state.pacReconnecting = false; this.state.markDirty(); });
    this.pacConnector.on('disconnected', () => { this.state.pacConnected = false; this.state.markDirty(); });
    this.pacConnector.on('error', (e) => { this.state.lastError = `PAC: ${e.message}`; this.state.errorCount++; this.state.markDirty(); });
  }

  async setupAndStart() {
    // Pacifica Event Listeners
    this.pacConnector.on('fill', (d) => this.state.addFill({ exchange: 'PAC', symbol: d.symbol, side: d.tradeSide, price: d.price, amount: d.amount, timestamp: d.timestamp, pnl: d.pnl || 0 }));
    
    this.pacConnector.on('orderUpdate', (d) => {
      if (d.orderStatus === 'open' || d.orderStatus === 'partially_filled') {
        this.state.pacOrders.set(d.orderId, { order_id: d.orderId, symbol: d.symbol, side: d.side, price: d.initialPrice, initial_amount: d.amount, created_at: d.createdAt });
      } else {
        this.state.pacOrders.delete(d.orderId);
        if (d.orderStatus === 'cancelled' || d.orderStatus === 'rejected') {
          this.state.addRecentOrder({ exchange: 'PAC', symbol: d.symbol, side: d.side, amount: d.amount, status: d.orderStatus.toUpperCase(), timestamp: d.updatedAt, orderId: d.orderId });
        }
      }
      this.state.markDirty();
    });
    
    this.pacConnector.on('orderFilled', (d) => { this.state.pacOrders.delete(d.orderId); this.state.markDirty(); });

    // New PAC Listeners for Account Info & Positions
    this.pacConnector.on('accountInfo', (info) => {
      this.state.updatePACBalance(info.availableToSpend, info.equity);
    });

    this.pacConnector.on('accountPositions', (positions) => {
      this.state.updatePACPositions(positions);
    });

    // Hyperliquid Event Listeners
    this.hlConnector.on('userState', (state) => {
      this.state.updateHLBalance(state.balance.withdrawable, state.balance.accountValue);
      this.state.updateHLPositions(state.positions);
      // userState update doesn't include open orders list directly in the same format, 
      // but HL clearinghouseState usually implies we might want to poll open orders separately 
      // or assume the WS poller might be extended. 
      // For now, let's keep polling orders via REST slowly, or we could add openOrders to the WS poll if we wanted.
      // Actually, getUserState REST returns positions. requestClearinghouseStateWs returns positions.
      // So positions are covered.
    });

    // Initial Data Fetch
    await this.pollHLData();
    await this.pollPACData();

    // Start Subscriptions
    try {
      await this.pacConnector.subscribeOrderUpdates(process.env.SOL_WALLET);
      await this.pacConnector.subscribeAccountInfo(process.env.SOL_WALLET);
      await this.pacConnector.subscribeAccountPositions(process.env.SOL_WALLET);
    } catch (e) {
      console.error('[Dashboard] Failed to subscribe PAC channels:', e.message);
    }

    // Start HL WS Polling (Fast)
    this.hlConnector.startUserPollingWs(2000, process.env.HL_WALLET);

    // Safety Net Polling (Slow)
    this.pollingTasks.push(setInterval(() => !this.shutdown && this.pollHLData(), 60000));
    this.pollingTasks.push(setInterval(() => !this.shutdown && this.pollPACData(), 60000));
  }

  async pollHLData() {
    try {
      const userState = await this.hlConnector.getUserState();
      this.state.updateHLBalance(userState.balance.withdrawable, userState.balance.accountValue);
      this.state.updateHLPositions(userState.positions);
      const orders = await this.hlConnector.getOpenOrders();
      this.state.updateHLOrders(orders);
      const fills = await this.hlConnector.getUserFills(null, Date.now() - 86400000);
      fills.forEach(f => this.state.addFill({ ...f, exchange: 'HL' }));
    } catch (e) {
      this.state.lastError = `HL Poll: ${e.message}`; this.state.errorCount++; this.state.markDirty();
    }
  }

  async pollPACData() {
    try {
      const balanceData = await this.pacConnector.getBalance();
      this.state.updatePACBalance(balanceData.availableToSpend || balanceData.balance || 0, balanceData.accountEquity || 0);
      const positions = await this.pacConnector.getPositions();
      this.state.updatePACPositions(positions);
      const pacOrders = await this.pacConnector.requestOrderStatusRest(process.env.SOL_WALLET);
      if (Array.isArray(pacOrders)) this.state.updatePACOrders(pacOrders);
      
      const orderHistory = await this.pacConnector.requestOrderHistoryRest(process.env.SOL_WALLET, 50);
      if (Array.isArray(orderHistory)) {
        const cancelled = [];
        orderHistory.forEach(order => {
            if (parseFloat(order.filled_amount || 0) > 0) {
                 this.state.addFill({ id: order.order_id, exchange: 'PAC', symbol: order.symbol, side: order.side, price: parseFloat(order.average_filled_price || order.initial_price), amount: parseFloat(order.filled_amount), timestamp: order.updated_at, pnl: 0 });
            }
            if (order.order_status === 'cancelled' || order.order_status === 'rejected') cancelled.push(order);
        });
        cancelled.sort((a, b) => (b.updated_at || 0) - (a.updated_at || 0));
        this.state.replaceRecentOrders(cancelled.slice(0, 8).map(o => ({ exchange: 'PAC', symbol: o.symbol, side: o.side, amount: parseFloat(o.amount || o.initial_amount || 0), status: o.order_status.toUpperCase(), timestamp: o.updated_at, order_id: o.order_id })));
      }
    } catch (e) {
      this.state.lastError = `PAC Poll: ${e.message}`; this.state.errorCount++; this.state.markDirty();
    }
  }

  stopPolling() { this.pollingTasks.forEach(clearInterval); }

  async cleanup() {
    this.shutdown = true;
    this.stopPolling();
    if (this.hlConnector) {
        this.hlConnector.stopUserPollingWs();
        this.hlConnector.disconnect();
    }
    if (this.pacConnector) this.pacConnector.disconnect();
  }
}

// ============================================================================
// MAIN ENTRY
// ============================================================================

async function main() {
  const configPath = join(__dirname, 'dashboard', 'config.json');
  try {
    const dashboard = new Dashboard(configPath);
    await dashboard.initialize();
    await dashboard.setupAndStart();

    const App = () => {
      const [snapshot, setSnapshot] = useState(dashboard.state.getSnapshot());
      useEffect(() => {
        const interval = setInterval(() => dashboard.state.dirty && setSnapshot(dashboard.state.getSnapshot()), 1000);
        return () => clearInterval(interval);
      }, []);
      return h(DashboardUI, { snapshot, symbols: dashboard.config.symbols });
    };

    const { unmount } = render(h(App), { patchConsole: false });
    
    const handleShutdown = async () => {
      unmount();
      await dashboard.cleanup();
      process.exit(0);
    };
    process.on('SIGINT', handleShutdown);
    process.on('SIGTERM', handleShutdown);

  } catch (error) {
    console.error('Fatal error:', error.message);
    process.exit(1);
  }
}

const isMainModule = import.meta.url === `file:///${__filename.replace(/\\/g, '/')}`;
if (isMainModule) main().catch(console.error);

export default Dashboard;