import WebSocket from 'ws';
import fetch from 'node-fetch';
import { EventEmitter } from 'events';
import { SlidingWindowRateLimiter } from '../utils/rate-limiter.js';
import nacl from 'tweetnacl';
import bs58 from 'bs58';
import { randomUUID } from 'crypto';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

class PacificaConnector extends EventEmitter {
  constructor(options = {}) {
    super();

    this.wsUrl = options.wsUrl || 'wss://ws.pacifica.fi/ws';
    this.restUrl = options.restUrl || 'https://api.pacifica.fi';
    this.testnet = options.testnet || false;

    if (this.testnet) {
      this.wsUrl = 'wss://test-ws.pacifica.fi/ws';
      this.restUrl = 'https://test-api.pacifica.fi';
    }

    // Load credentials from environment
    // For API Agent Keys:
    // - account: main wallet address (SOL_WALLET)
    // - agentWallet: agent wallet public key (API_PUBLIC)
    // - privateKey: agent wallet private key (API_PRIVATE)
    this.account = options.account || process.env.SOL_WALLET;
    this.agentWallet = options.agentWallet || process.env.API_PUBLIC;
    this.privateKey = options.privateKey || process.env.API_PRIVATE;

    // Connection state
    this.ws = null;
    this.connected = false;
    this.reconnecting = false;
    this.intentionalDisconnect = false; // Flag to prevent auto-reconnect after manual disconnect
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = options.maxReconnectAttempts || 10;
    this.reconnectDelay = options.reconnectDelay || 1000;
    this.maxReconnectDelay = options.maxReconnectDelay || 30000;

    // Health monitoring (ping every 50 seconds to avoid 60s timeout)
    this.pingInterval = options.pingInterval || 50000;
    this.pongTimeout = options.pongTimeout || 10000;
    this.pingTimer = null;
    this.pongTimer = null;
    this.lastPongReceived = Date.now();

    // REST API fallback
    this.useRestFallback = false;
    this.restPollInterval = options.restPollInterval || 1000;
    this.restPollTimer = null;

    // Periodic REST refresh (every 5s) to supplement WebSocket
    this.restRefreshInterval = options.restRefreshInterval || 5000; // 5 seconds
    this.restRefreshTimer = null;

    // Orderbook data cache
    this.orderbooks = new Map();

    // Market info cache (tick_size, lot_size, etc.)
    this.marketInfoCache = null;

    // Subscriptions
    this.subscriptions = new Set();
    this.maxSubscriptionsPerChannel = 20; // Pacifica limit

    // Order update subscriptions
    this.orderUpdateSubscriptions = new Set(); // Set of account addresses subscribed to
    this.orders = new Map(); // Map of orderId -> order data
    this.ordersByClientId = new Map(); // Map of clientOrderId -> order data

    // REST API order fill polling
    this.orderFillPollingEnabled = options.orderFillPolling || false;
    this.orderFillPollInterval = options.orderFillPollInterval || 300; // Poll every 0.3 seconds for faster fill detection
    this.orderFillPollTimer = null;
    this.lastKnownOrderStates = new Map(); // Track order states for fill detection
    this.activeOrderSymbol = null; // Track symbol with active order for optimized polling

    // Rate limit protection
    this.rateLimitExceeded = false; // Flag when HTTP 429 is received
    this.rateLimitCooldownMs = options.rateLimitCooldownMs || 120000; // Default 120 seconds cooldown after rate limit
    this.rateLimitCooldownTimer = null;

    // Aggregation level for orderbook (1, 2, 5, 10, 100, 1000)
    this.aggLevel = options.aggLevel || 1;

    // Rate limiters
    // REST: 300 requests per 60 seconds
    this.restRateLimiter = new SlidingWindowRateLimiter({
      maxRequests: 280, // Use 280 for buffer
      windowMs: 60000 // 1 minute
    });

    // Track last update times for throttling
    this.lastUpdateTime = new Map();
  }

  /**
   * Connect to Pacifica WebSocket
   */
  async connect() {
    return new Promise((resolve, reject) => {
      try {
        this.intentionalDisconnect = false; // Reset flag when connecting
        this.ws = new WebSocket(this.wsUrl);

        this.ws.on('open', () => {
          console.log('[Pacifica] WebSocket connected');
          this.connected = true;
          this.reconnecting = false;
          this.reconnectAttempts = 0;
          this.useRestFallback = false;

          // Start health monitoring
          this.startHealthMonitoring();

          // Resubscribe to previous subscriptions
          this.resubscribe();

          this.emit('connected');
          resolve();
        });

        this.ws.on('message', (data) => {
          this.handleMessage(data);
        });

        this.ws.on('error', (error) => {
          console.error('[Pacifica] WebSocket error:', error.message);
          this.emit('error', error);
        });

        this.ws.on('close', () => {
          console.log('[Pacifica] WebSocket closed');
          this.connected = false;
          this.stopHealthMonitoring();

          this.emit('disconnected');

          // Attempt to reconnect only if not intentionally disconnected
          if (!this.reconnecting && !this.intentionalDisconnect) {
            this.handleReconnect();
          }
        });

        // Connection timeout (guarded against intentional disconnects and null ws)
        setTimeout(() => {
          try {
            if (!this.connected) {
              if (!this.intentionalDisconnect) {
                try { reject(new Error('Connection timeout')); } catch (_) {}
              }
              if (this.ws && typeof this.ws.terminate === 'function') {
                try { this.ws.terminate(); } catch (_) {}
              }
            }
          } catch (_) {}
        }, 10000);

      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Handle WebSocket messages
   */
  handleMessage(data) {
    try {
      const message = JSON.parse(data.toString());

      // Handle pong response
      if (message.channel === 'pong') {
        this.lastPongReceived = Date.now();
        if (this.pongTimer) {
          clearTimeout(this.pongTimer);
          this.pongTimer = null;
        }
        return;
      }

      // Handle orderbook updates
      if (message.channel === 'book') {
        this.updateOrderbook(message.data);
        return;
      }

      // Handle order updates
      if (message.channel === 'account_order_updates') {
        this.handleOrderUpdates(message.data);
        return;
      }

      // Handle account trades (fills)
      if (message.channel === 'account_trades') {
        this.handleAccountTrades(message.data);
        return;
      }

      // Handle subscription responses (if any)
      if (message.channel === 'subscription') {
        console.log('[Pacifica] Subscription confirmed:', message.data);
        return;
      }

    } catch (error) {
      console.error('[Pacifica] Error parsing message:', error);
    }
  }

  /**
   * Update orderbook cache
   */
  updateOrderbook(data) {
    const { l, s, t } = data;

    if (!s || !l || l.length < 2) {
      return;
    }

    const [bids, asks] = l;

    // Extract best bid and ask
    let bestBid = null;
    let bestAsk = null;

    if (bids && bids.length > 0) {
      bestBid = {
        price: parseFloat(bids[0].p),
        size: parseFloat(bids[0].a),
        numOrders: bids[0].n
      };
    }

    if (asks && asks.length > 0) {
      bestAsk = {
        price: parseFloat(asks[0].p),
        size: parseFloat(asks[0].a),
        numOrders: asks[0].n
      };
    }

    const orderbook = {
      coin: s,
      bestBid,
      bestAsk,
      bids,
      asks,
      timestamp: t || Date.now()
    };

    this.orderbooks.set(s, orderbook);
    this.emit('orderbook', orderbook);
  }

  /**
   * Handle order update messages from WebSocket
   */
  handleOrderUpdates(updates) {
    if (!Array.isArray(updates)) {
      updates = [updates];
    }

    for (const update of updates) {
      const orderData = {
        orderId: update.i,
        clientOrderId: update.I,
        account: update.u,
        symbol: update.s,
        side: update.d, // 'bid' or 'ask'
        price: parseFloat(update.p || 0), // Average filled price
        initialPrice: parseFloat(update.ip || 0),
        amount: parseFloat(update.a),
        filledAmount: parseFloat(update.f || 0),
        orderEvent: update.oe, // 'make', 'fulfill_limit', 'fulfill_market', 'cancel', etc.
        orderStatus: update.os, // 'open', 'partially_filled', 'filled', 'cancelled', 'rejected'
        orderType: update.ot, // 'limit', 'market'
        reduceOnly: update.r,
        stopPrice: update.sp,
        createdAt: update.ct,
        updatedAt: update.ut,
        timestamp: update.ut || Date.now()
      };

      // Update order caches
      if (orderData.orderId) {
        this.orders.set(orderData.orderId, orderData);
      }
      if (orderData.clientOrderId) {
        this.ordersByClientId.set(orderData.clientOrderId, orderData);
      }

      // Emit event for order update
      this.emit('orderUpdate', orderData);

      // Emit specific events based on status
      if (orderData.orderStatus === 'filled') {
        this.emit('orderFilled', orderData);
        console.log(`[Pacifica] Order filled: ${orderData.symbol} ${orderData.side} ${orderData.filledAmount} @ ${orderData.price}`);
      } else if (orderData.orderStatus === 'partially_filled') {
        this.emit('orderPartiallyFilled', orderData);
        console.log(`[Pacifica] Order partially filled: ${orderData.symbol} ${orderData.side} ${orderData.filledAmount}/${orderData.amount} @ ${orderData.price}`);
      } else if (orderData.orderStatus === 'cancelled') {
        this.emit('orderCancelled', orderData);
        console.log(`[Pacifica] Order cancelled: ${orderData.symbol} ${orderData.side} (${orderData.orderEvent})`);
      } else if (orderData.orderStatus === 'rejected') {
        this.emit('orderRejected', orderData);
        console.log(`[Pacifica] Order rejected: ${orderData.symbol} ${orderData.side} (${orderData.orderEvent})`);
      }
    }
  }

  /**
   * Handle account trades (individual fills) from WebSocket
   */
  handleAccountTrades(trades) {
    if (!Array.isArray(trades)) {
      trades = [trades];
    }

    for (const trade of trades) {
      const tradeData = {
        historyId: trade.h,
        orderId: trade.i,
        clientOrderId: trade.I,
        symbol: trade.s,
        tradeSide: trade.ts, // 'open_long', 'open_short', 'close_long', 'close_short'
        price: parseFloat(trade.p),
        amount: parseFloat(trade.a),
        account: trade.u,
        timestamp: trade.t,
        entryPrice: parseFloat(trade.o || 0),
        tradeExecution: trade.te, // 'fulfill_maker' or 'fulfill_taker'
        tradeCause: trade.tc, // 'normal', 'market_liquidation', 'backstop_liquidation', 'settlement'
        pnl: parseFloat(trade.n || 0),
        fee: parseFloat(trade.f || 0)
      };

      // Emit event for trade/fill
      this.emit('trade', tradeData);
      this.emit('fill', tradeData);

      console.log(`[Pacifica] Fill: ${tradeData.symbol} ${tradeData.tradeSide} ${tradeData.amount} @ ${tradeData.price} (${tradeData.tradeExecution}, fee: ${tradeData.fee})`);
    }
  }

  /**
   * Get bid and ask prices for a coin
   */
  getBidAsk(coin) {
    const orderbook = this.orderbooks.get(coin);

    if (!orderbook) {
      return null;
    }

    return {
      coin,
      bid: orderbook.bestBid?.price || null,
      ask: orderbook.bestAsk?.price || null,
      bidSize: orderbook.bestBid?.size || null,
      askSize: orderbook.bestAsk?.size || null,
      timestamp: orderbook.timestamp
    };
  }

  /**
   * Subscribe to orderbook updates for a coin
   */
  async subscribeOrderbook(coin) {
    if (!this.connected && !this.useRestFallback) {
      throw new Error('Not connected');
    }

    // Check subscription limit
    if (this.subscriptions.size >= this.maxSubscriptionsPerChannel) {
      throw new Error(`Maximum ${this.maxSubscriptionsPerChannel} subscriptions per channel`);
    }

    // Add to subscriptions
    this.subscriptions.add(coin);

    if (this.useRestFallback) {
      // Start REST polling if not already started
      if (!this.restPollTimer) {
        this.startRestPolling();
      }
      return;
    }

    // Start periodic REST refresh if this is the first subscription
    // This supplements WebSocket with periodic REST updates every 5s
    if (this.subscriptions.size === 1 && !this.restRefreshTimer) {
      this.startPeriodicRestRefresh();
    }

    // Subscribe via WebSocket
    try {
      const subscribeMessage = {
        method: 'subscribe',
        params: {
          source: 'book',
          symbol: coin,
          agg_level: this.aggLevel
        }
      };

      this.ws.send(JSON.stringify(subscribeMessage));
      console.log(`[Pacifica] Subscribed to ${coin} orderbook`);

    } catch (error) {
      console.error(`[Pacifica] Failed to subscribe to ${coin}:`, error.message);
      throw error;
    }
  }

  /**
   * Subscribe to order updates for an account
   */
  async subscribeOrderUpdates(account = null) {
    account = account || this.account;

    if (!account) {
      throw new Error('Account address required for order update subscription');
    }

    if (this.orderUpdateSubscriptions.has(account)) {
      console.log(`[Pacifica] Already subscribed to order updates for ${account}`);
      return;
    }

    this.orderUpdateSubscriptions.add(account);

    if (this.useRestFallback) {
      // Start REST polling if not already started
      if (!this.restPollTimer) {
        this.startRestPolling();
      }
      return;
    }

    if (!this.connected) {
      throw new Error('Not connected');
    }

    // Subscribe via WebSocket - subscribe to both order updates and trades
    try {
      // Subscribe to order updates
      const subscribeOrderUpdates = {
        method: 'subscribe',
        params: {
          source: 'account_order_updates',
          account: account
        }
      };

      this.ws.send(JSON.stringify(subscribeOrderUpdates));
      console.log(`[Pacifica] Subscribed to order updates for ${account}`);

      // Subscribe to account trades (fills)
      const subscribeTrades = {
        method: 'subscribe',
        params: {
          source: 'account_trades',
          account: account
        }
      };

      this.ws.send(JSON.stringify(subscribeTrades));
      console.log(`[Pacifica] Subscribed to account trades for ${account}`);

    } catch (error) {
      console.error(`[Pacifica] Failed to subscribe to order updates:`, error.message);
      throw error;
    }
  }

  /**
   * Unsubscribe from order updates for an account
   */
  async unsubscribeOrderUpdates(account = null) {
    account = account || this.account;

    if (!this.orderUpdateSubscriptions.has(account)) {
      return;
    }

    this.orderUpdateSubscriptions.delete(account);

    if (this.connected && !this.useRestFallback) {
      try {
        // Unsubscribe from order updates
        const unsubscribeOrderUpdates = {
          method: 'unsubscribe',
          params: {
            source: 'account_order_updates',
            account: account
          }
        };

        this.ws.send(JSON.stringify(unsubscribeOrderUpdates));

        // Unsubscribe from account trades
        const unsubscribeTrades = {
          method: 'unsubscribe',
          params: {
            source: 'account_trades',
            account: account
          }
        };

        this.ws.send(JSON.stringify(unsubscribeTrades));

        console.log(`[Pacifica] Unsubscribed from order updates and trades for ${account}`);
      } catch (error) {
        console.error(`[Pacifica] Failed to unsubscribe from order updates:`, error.message);
      }
    }
  }

  /**
   * Unsubscribe from orderbook updates
   */
  async unsubscribe(coin) {
    if (!this.subscriptions.has(coin)) {
      return;
    }

    this.subscriptions.delete(coin);
    this.orderbooks.delete(coin);

    if (this.connected && !this.useRestFallback) {
      try {
        const unsubscribeMessage = {
          method: 'unsubscribe',
          params: {
            source: 'book',
            symbol: coin
          }
        };

        this.ws.send(JSON.stringify(unsubscribeMessage));
        console.log(`[Pacifica] Unsubscribed from ${coin} orderbook`);
      } catch (error) {
        console.error(`[Pacifica] Failed to unsubscribe from ${coin}:`, error.message);
      }
    }
  }

  /**
   * Request orderbook via REST API
   */
  async requestOrderbookRest(coin) {
    // Wait for rate limit slot
    await this.restRateLimiter.waitForSlot();

    try {
      const url = `${this.restUrl}/api/v1/book?symbol=${coin}&agg_level=${this.aggLevel}`;
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Accept': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();
      return data;
    } catch (error) {
      console.error(`[Pacifica] REST API error for ${coin}:`, error.message);

      // Check if this is a rate limit error (HTTP 429)
      if (error.message && error.message.includes('429')) {
        this.handleRateLimitExceeded();
      }

      throw error;
    }
  }

  /**
   * Request order status via REST API (for specific accounts)
   */
  async requestOrderStatusRest(account) {
    await this.restRateLimiter.waitForSlot();

    try {
      const url = `${this.restUrl}/api/v1/orders?account=${account}`;
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Accept': 'application/json'
        }
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }

      const result = await response.json();
      return result.data || [];
    } catch (error) {
      console.error(`[Pacifica] REST API error for order status:`, error.message);

      // Check if this is a rate limit error (HTTP 429)
      if (error.message && error.message.includes('429')) {
        this.handleRateLimitExceeded();
      }

      throw error;
    }
  }

  /**
   * Request order history via REST API (includes filled/cancelled orders)
   */
  async requestOrderHistoryRest(account, limit = 100) {
    await this.restRateLimiter.waitForSlot();

    try {
      const url = `${this.restUrl}/api/v1/orders/history?account=${account}&limit=${limit}`;
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Accept': 'application/json'
        }
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }

      const result = await response.json();
      return result.data || [];
    } catch (error) {
      console.error(`[Pacifica] REST API error for order history:`, error.message);

      // Check if this is a rate limit error (HTTP 429)
      if (error.message && error.message.includes('429')) {
        this.handleRateLimitExceeded();
      }

      throw error;
    }
  }

  /**
   * Get current open positions via REST API
   * @param {string} account - Account address (defaults to configured account)
   * @returns {Promise<Array>} Array of position objects
   */
  async getPositions(account = null) {
    account = account || this.account;

    if (!account) {
      throw new Error('Account address required to get positions');
    }

    await this.restRateLimiter.waitForSlot();

    try {
      const url = `${this.restUrl}/api/v1/positions?account=${account}`;
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
      const positions = result.data || [];

      // Transform positions to more readable format
      return positions.map(pos => ({
        symbol: pos.symbol,
        side: pos.side === 'bid' ? 'long' : 'short', // bid = long, ask = short
        amount: parseFloat(pos.amount),
        entryPrice: parseFloat(pos.entry_price),
        margin: pos.margin ? parseFloat(pos.margin) : null,
        funding: parseFloat(pos.funding || 0),
        isolated: pos.isolated,
        createdAt: pos.created_at,
        updatedAt: pos.updated_at,
        // Calculate notional value
        notionalValue: parseFloat(pos.amount) * parseFloat(pos.entry_price)
      }));
    } catch (error) {
      console.error(`[Pacifica] Error fetching positions:`, error.message);
      throw error;
    }
  }

  /**
   * Start REST API polling fallback
   */
  startRestPolling() {
    if (this.restPollTimer) {
      return;
    }

    console.log('[Pacifica] Starting REST API fallback polling');

    this.restPollTimer = setInterval(async () => {
      if (!this.useRestFallback) {
        this.stopRestPolling();
        return;
      }

      // Poll all subscribed coins for orderbook
      for (const coin of this.subscriptions) {
        try {
          const data = await this.requestOrderbookRest(coin);

          // Transform REST response to match WebSocket format
          if (data && data.l) {
            this.updateOrderbook({
              l: data.l,
              s: coin,
              t: Date.now()
            });
          }
        } catch (error) {
          console.error(`[Pacifica] REST polling error for ${coin}:`, error.message);
        }
      }

      // Poll all subscribed accounts for order updates
      for (const account of this.orderUpdateSubscriptions) {
        try {
          const orders = await this.requestOrderStatusRest(account);

          // Transform REST response to match WebSocket format
          for (const order of orders) {
            const update = {
              i: order.order_id,
              I: order.client_order_id,
              u: account,
              s: order.symbol,
              d: order.side, // 'bid' or 'ask'
              p: order.price || '0',
              ip: order.price || '0',
              a: order.initial_amount,
              f: order.filled_amount,
              oe: 'make', // Not provided by REST, assume 'make'
              os: order.order_status || 'open',
              ot: order.order_type,
              r: order.reduce_only,
              sp: order.stop_price,
              ct: order.created_at,
              ut: order.updated_at
            };

            this.handleOrderUpdates([update]);
          }
        } catch (error) {
          console.error(`[Pacifica] REST polling error for order updates:`, error.message);
        }
      }
    }, this.restPollInterval);
  }

  /**
   * Stop REST API polling
   */
  stopRestPolling() {
    if (this.restPollTimer) {
      clearInterval(this.restPollTimer);
      this.restPollTimer = null;
    }
  }

  /**
   * Start health monitoring with ping/pong
   */
  startHealthMonitoring() {
    this.stopHealthMonitoring();

    this.pingTimer = setInterval(() => {
      if (!this.connected || !this.ws) {
        return;
      }

      // Check if we received a pong recently
      const timeSinceLastPong = Date.now() - this.lastPongReceived;
      if (timeSinceLastPong > this.pongTimeout + this.pingInterval) {
        console.error('[Pacifica] Pong timeout, connection may be dead');
        if (this.ws && typeof this.ws.terminate === 'function') {
          try { this.ws.terminate(); } catch (_) {}
        }
        return;
      }

      // Send ping
      try {
        const pingMessage = { method: 'ping' };
        this.ws.send(JSON.stringify(pingMessage));

        // Set pong timeout
        this.pongTimer = setTimeout(() => {
          console.error('[Pacifica] Pong timeout');
          if (this.ws) {
            this.ws.terminate();
          }
        }, this.pongTimeout);
      } catch (error) {
        console.error('[Pacifica] Error sending ping:', error);
      }
    }, this.pingInterval);
  }

  /**
   * Stop health monitoring
   */
  stopHealthMonitoring() {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }

    if (this.pongTimer) {
      clearTimeout(this.pongTimer);
      this.pongTimer = null;
    }
  }

  /**
   * Handle reconnection with exponential backoff
   */
  async handleReconnect() {
    if (this.reconnecting) {
      return;
    }

    this.reconnecting = true;
    this.reconnectAttempts++;

    if (this.reconnectAttempts > this.maxReconnectAttempts) {
      console.error('[Pacifica] Max reconnect attempts reached, switching to REST fallback');
      this.useRestFallback = true;
      this.startRestPolling();
      this.emit('fallback', 'rest');
      return;
    }

    // Calculate backoff delay
    const delay = Math.min(
      this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1),
      this.maxReconnectDelay
    );

    console.log(`[Pacifica] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

    setTimeout(async () => {
      try {
        await this.connect();
        console.log('[Pacifica] Reconnected successfully');
      } catch (error) {
        console.error('[Pacifica] Reconnection failed:', error);
        this.reconnecting = false;
        this.handleReconnect();
      }
    }, delay);
  }

  /**
   * Resubscribe to all previous subscriptions
   */
  resubscribe() {
    console.log(`[Pacifica] Resubscribing to ${this.subscriptions.size} coins`);

    // Resubscribe to orderbooks
    for (const coin of this.subscriptions) {
      this.subscribeOrderbook(coin).catch(error => {
        console.error(`[Pacifica] Error resubscribing to ${coin}:`, error);
      });
    }

    // CRITICAL: Resubscribe to order updates (for fill detection)
    if (this.orderUpdateSubscriptions.size > 0) {
      console.log(`[Pacifica] Resubscribing to order updates for ${this.orderUpdateSubscriptions.size} accounts`);
      for (const account of this.orderUpdateSubscriptions) {
        this.subscribeOrderUpdates(account).catch(error => {
          console.error(`[Pacifica] Error resubscribing to order updates for ${account}:`, error);
        });
      }
    }
  }

  /**
   * Track a new order for REST API fill polling
   * Call this after placing an order to enable polling for that order
   * @param {string|object} orderIdOrResult - Order ID, client order ID, or order result object {i, I, s}
   * @param {string} symbol - Trading symbol
   * @param {number} initialFilledAmount - Initial filled amount
   */
  trackOrderForPolling(orderIdOrResult, symbol, initialFilledAmount = 0) {
    this.activeOrderSymbol = symbol;

    // Extract both server order ID and client order ID if available
    let serverOrderId = null;
    let clientOrderId = null;

    if (typeof orderIdOrResult === 'object' && orderIdOrResult !== null) {
      // Order result object with both IDs
      serverOrderId = orderIdOrResult.i;
      clientOrderId = orderIdOrResult.I;
    } else {
      // Just an ID string - could be either
      // We'll track it as both to be safe
      serverOrderId = orderIdOrResult;
      clientOrderId = orderIdOrResult;
    }

    const state = {
      filled_amount: initialFilledAmount,
      status: 'open',
      timestamp: Date.now(),
      serverOrderId: serverOrderId,
      clientOrderId: clientOrderId,
      symbol: symbol
    };

    // Track by both IDs if available
    if (serverOrderId) {
      this.lastKnownOrderStates.set(serverOrderId.toString(), state);
    }
    if (clientOrderId && clientOrderId !== serverOrderId) {
      this.lastKnownOrderStates.set(clientOrderId.toString(), state);
    }

    // Auto-start polling if enabled
    if (this.orderFillPollingEnabled && !this.orderFillPollTimer) {
      this.startOrderFillPolling();
    }
  }

  /**
   * Start REST API order fill polling (fallback/primary fill detection)
   */
  startOrderFillPolling() {
    if (!this.orderFillPollingEnabled) {
      return;
    }

    this.stopOrderFillPolling();

    console.log(`[Pacifica] Starting order fill polling (every ${this.orderFillPollInterval}ms)`);

    this.orderFillPollTimer = setInterval(async () => {
      await this.pollOrderFills().catch(err => {
        console.error('[Pacifica] Order fill polling error:', err.message);
      });
    }, this.orderFillPollInterval);
  }

  /**
   * Stop REST API order fill polling
   */
  stopOrderFillPolling() {
    if (this.orderFillPollTimer) {
      clearInterval(this.orderFillPollTimer);
      this.orderFillPollTimer = null;
    }
  }

  /**
   * Poll for order fills via REST API
   * Compares current order status with last known state to detect fills
   * Only polls when there are active orders being tracked
   */
  async pollOrderFills() {
    if (!this.account) {
      return;
    }

    // Optimization: skip polling if no active orders being tracked
    if (this.lastKnownOrderStates.size === 0 && !this.activeOrderSymbol) {
      return;
    }

    try {
      // Fetch current open orders
      const currentOrders = await this.requestOrderStatusRest(this.account);

      // Also fetch recent order history to catch filled/cancelled orders
      const recentHistory = await this.requestOrderHistoryRest(this.account, 20);

      // Process current orders
      for (const order of currentOrders) {
        const serverOrderId = order.id || order.order_id;
        const clientOrderId = order.client_order_id;

        // Try to find tracked state by either server order ID or client order ID
        let lastState = this.lastKnownOrderStates.get(serverOrderId?.toString());
        if (!lastState && clientOrderId) {
          lastState = this.lastKnownOrderStates.get(clientOrderId.toString());
        }

        // New order or updated fill amount
        if (!lastState || order.filled_amount !== lastState.filled_amount) {
          // Store new state (by both IDs if available)
          const newState = {
            filled_amount: order.filled_amount,
            status: order.status,
            timestamp: Date.now(),
            serverOrderId: serverOrderId,
            clientOrderId: clientOrderId,
            symbol: order.symbol
          };

          if (serverOrderId) {
            this.lastKnownOrderStates.set(serverOrderId.toString(), newState);
          }
          if (clientOrderId && clientOrderId !== serverOrderId) {
            this.lastKnownOrderStates.set(clientOrderId.toString(), newState);
          }

          // If filled amount increased, emit fill event
          if (lastState && order.filled_amount > lastState.filled_amount) {
            const incrementalFill = order.filled_amount - lastState.filled_amount;

            // Emit trade event (incremental fill)
            this.emit('trade', {
              historyId: null,
              orderId: serverOrderId,
              clientOrderId: clientOrderId,
              symbol: order.symbol,
              tradeSide: order.side === 'bid' ? 'open_long' : 'open_short',
              price: parseFloat(order.average_price || order.price),
              amount: incrementalFill,
              account: this.account,
              timestamp: Date.now(),
              entryPrice: parseFloat(order.average_price || order.price),
              tradeExecution: 'fulfill_maker',
              tradeCause: 'normal',
              pnl: 0,
              fee: 0
            });

            // Also emit orderPartiallyFilled to trigger the bot's partial fill logic
            this.emit('orderPartiallyFilled', {
              orderId: serverOrderId,
              clientOrderId: clientOrderId,
              account: this.account,
              symbol: order.symbol,
              side: order.side,
              price: parseFloat(order.average_price || order.price),
              amount: parseFloat(order.initial_amount),
              filledAmount: parseFloat(order.filled_amount),
              orderStatus: 'partially_filled',
              orderType: order.order_type,
              createdAt: order.created_at,
              updatedAt: order.updated_at || Date.now(),
              timestamp: Date.now(),
            });

            console.log(`[Pacifica] REST API fill detected: ${order.symbol} ${order.side} ${incrementalFill} @ ${order.average_price || order.price}`);
          }
        }
      }

      // Process recent history for fully filled/cancelled orders that we're tracking
      for (const histOrder of recentHistory) {
        const serverOrderId = histOrder.id || histOrder.order_id;
        const clientOrderId = histOrder.client_order_id;

        // Try to find tracked state by either server order ID or client order ID
        let lastState = this.lastKnownOrderStates.get(serverOrderId?.toString());
        if (!lastState && clientOrderId) {
          lastState = this.lastKnownOrderStates.get(clientOrderId.toString());
        }

        // Use the ID we found in tracking, or fall back to server order ID
        const orderId = lastState ?
          (lastState.serverOrderId || lastState.clientOrderId || serverOrderId) :
          serverOrderId;

        // Check if this is a tracked order that filled (either not tracked yet, or tracked but not filled)
        if (histOrder.status === 'filled' && (!lastState || lastState.status !== 'filled')) {
          const filledAmount = parseFloat(histOrder.filled_amount);
          const lastFilledAmount = lastState ? lastState.filled_amount : 0;

          // Emit incremental fill event (trade) if there's new fill amount
          if (filledAmount > lastFilledAmount) {
            const incrementalFill = filledAmount - lastFilledAmount;

            this.emit('trade', {
              historyId: null,
              orderId: orderId,
              clientOrderId: histOrder.client_order_id,
              symbol: histOrder.symbol,
              tradeSide: histOrder.side === 'bid' ? 'open_long' : 'open_short',
              price: parseFloat(histOrder.average_price || histOrder.price),
              amount: incrementalFill,
              account: this.account,
              timestamp: Date.now(),
              entryPrice: parseFloat(histOrder.average_price || histOrder.price),
              tradeExecution: 'fulfill_maker',
              tradeCause: 'normal',
              pnl: 0,
              fee: 0
            });

            console.log(`[Pacifica] REST API fill detected (history): ${histOrder.symbol} ${histOrder.side} ${incrementalFill} @ ${histOrder.average_price || histOrder.price}`);
          }

          // Emit orderFilled event for fully filled orders
          this.emit('orderFilled', {
            orderId: orderId,
            clientOrderId: histOrder.client_order_id,
            account: this.account,
            symbol: histOrder.symbol,
            side: histOrder.side,
            price: parseFloat(histOrder.average_price || histOrder.price),
            initialPrice: parseFloat(histOrder.price),
            amount: parseFloat(histOrder.amount),
            filledAmount: filledAmount,
            orderEvent: 'fulfill_limit',
            orderStatus: 'filled',
            orderType: histOrder.order_type || 'limit',
            reduceOnly: histOrder.reduce_only || false,
            stopPrice: histOrder.stop_price || null,
            createdAt: histOrder.created_at,
            updatedAt: histOrder.updated_at || Date.now(),
            timestamp: histOrder.updated_at || Date.now()
          });

          console.log(`[Pacifica] REST API order filled (history): ${histOrder.symbol} ${histOrder.side} ${filledAmount} @ ${histOrder.average_price || histOrder.price}`);

          // Mark as processed (store by both IDs if available)
          const filledState = {
            filled_amount: filledAmount,
            status: 'filled',
            timestamp: Date.now(),
            serverOrderId: serverOrderId,
            clientOrderId: clientOrderId,
            symbol: histOrder.symbol
          };

          if (serverOrderId) {
            this.lastKnownOrderStates.set(serverOrderId.toString(), filledState);
          }
          if (clientOrderId && clientOrderId !== serverOrderId) {
            this.lastKnownOrderStates.set(clientOrderId.toString(), filledState);
          }
        }
      }

      // Cleanup old states (older than 5 minutes)
      const fiveMinutesAgo = Date.now() - 5 * 60 * 1000;
      for (const [orderId, state] of this.lastKnownOrderStates.entries()) {
        if (state.timestamp < fiveMinutesAgo) {
          this.lastKnownOrderStates.delete(orderId);
        }
      }

    } catch (error) {
      console.error('[Pacifica] Error polling order fills:', error.message);

      // Check if this is a rate limit error (HTTP 429)
      if (error.message && error.message.includes('429')) {
        this.handleRateLimitExceeded();
      }
    }
  }

  /**
   * Start periodic REST refresh (every 5s) to supplement WebSocket
   * This ensures prices stay fresh even if WebSocket updates miss or stale
   */
  startPeriodicRestRefresh() {
    if (this.restRefreshTimer) {
      return; // Already running
    }

    console.log('[Pacifica] Starting periodic REST refresh (every 5s)');

    this.restRefreshTimer = setInterval(async () => {
      // Refresh all subscribed coins
      for (const coin of this.subscriptions) {
        try {
          const data = await this.requestOrderbookRest(coin);
          this.updateOrderbook({
            symbol: coin,
            bids: data.bids,
            asks: data.asks,
            timestamp: Date.now()
          });
        } catch (error) {
          // Silently fail - WebSocket is primary, this is just a supplement
          if (!error.message.includes('429')) {
            console.error(`[Pacifica] REST refresh error for ${coin}:`, error.message);
          }
        }
      }
    }, this.restRefreshInterval);
  }

  /**
   * Stop periodic REST refresh
   */
  stopPeriodicRestRefresh() {
    if (this.restRefreshTimer) {
      clearInterval(this.restRefreshTimer);
      this.restRefreshTimer = null;
      console.log('[Pacifica] Stopped periodic REST refresh');
    }
  }

  /**
   * Disconnect from Pacifica
   */
  disconnect() {
    console.log('[Pacifica] Disconnecting');

    this.intentionalDisconnect = true; // Prevent auto-reconnect
    this.reconnecting = false;
    this.stopHealthMonitoring();
    this.stopRestPolling();
    this.stopPeriodicRestRefresh();
    this.stopOrderFillPolling();

    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }

    this.connected = false;
    this.subscriptions.clear();
    this.orderbooks.clear();
  }

  /**
   * Get connection status
   */
  getStatus() {
    return {
      connected: this.connected,
      reconnecting: this.reconnecting,
      reconnectAttempts: this.reconnectAttempts,
      useRestFallback: this.useRestFallback,
      subscriptions: Array.from(this.subscriptions),
      orderbooks: Array.from(this.orderbooks.keys())
    };
  }

  /**
   * Sign a message using Ed25519
   * @param {object} header - {type, timestamp, expiry_window}
   * @param {object} payload - Order/cancel parameters (without account, signature, timestamp, expiry_window)
   */
  signMessage(header, payload) {
    if (!this.privateKey) {
      throw new Error('Private key not configured');
    }

    // Validate header
    if (!header.type || !header.timestamp || !header.expiry_window) {
      throw new Error('Header must have type, timestamp, and expiry_window');
    }

    // Construct message for signing: {...header, data: payload}
    const messageForSigning = {
      ...header,
      data: payload
    };

    // Convert private key from base58 to Uint8Array
    const privateKeyBytes = bs58.decode(this.privateKey);

    // Canonicalize JSON by sorting keys alphabetically
    const canonicalMessage = this.canonicalizeJSON(messageForSigning);

    // Convert to bytes
    const messageBytes = new TextEncoder().encode(canonicalMessage);

    // Sign the message
    const signature = nacl.sign.detached(messageBytes, privateKeyBytes);

    // Encode signature as base58
    return bs58.encode(signature);
  }

  /**
   * Canonicalize JSON by sorting keys alphabetically (deterministic)
   */
  canonicalizeJSON(obj) {
    if (typeof obj !== 'object' || obj === null) {
      return JSON.stringify(obj);
    }

    if (Array.isArray(obj)) {
      return '[' + obj.map(item => this.canonicalizeJSON(item)).join(',') + ']';
    }

    // Sort keys alphabetically
    const sortedKeys = Object.keys(obj).sort();
    const pairs = sortedKeys.map(key => {
      const value = obj[key];
      const valueStr = this.canonicalizeJSON(value);
      return `"${key}":${valueStr}`;
    });

    return '{' + pairs.join(',') + '}';
  }

  /**
   * Get market info from Pacifica API (includes tick_size and lot_size)
   * @returns {Promise<Map>} Map of symbol -> market info
   */
  async getMarketInfo() {
    if (this.marketInfoCache) {
      return this.marketInfoCache;
    }

    await this.restRateLimiter.waitForSlot();

    try {
      const url = `${this.restUrl}/api/v1/info`;
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

      // Create map for quick lookup by symbol
      this.marketInfoCache = new Map();

      // API returns {data: [{symbol, tick_size, lot_size, ...}, ...]}
      const markets = result.data || result;
      for (const market of markets) {
        this.marketInfoCache.set(market.symbol, market);
      }

      console.log(`[Pacifica] Cached market info for ${this.marketInfoCache.size} symbols`);
      return this.marketInfoCache;
    } catch (error) {
      console.error('[Pacifica] Error fetching market info:', error.message);
      throw error;
    }
  }

  /**
   * Round price to tick size (all prices must be multiples of tick_size)
   * @param {number} price - Price to round
   * @param {string} tickSize - Tick size from market info (e.g., "0.1", "0.01")
   * @returns {number} Rounded price
   */
  roundToTickSize(price, tickSize) {
    const tick = parseFloat(tickSize);
    const rounded = Math.round(price / tick) * tick;

    // Determine decimal places from tick size to avoid floating point issues
    const decimalPlaces = tickSize.includes('.') ? tickSize.split('.')[1].length : 0;

    return parseFloat(rounded.toFixed(decimalPlaces));
  }

  /**
   * Round size to lot size (all order sizes must be multiples of lot_size)
   * @param {number} size - Size to round
   * @param {string} lotSize - Lot size from market info (e.g., "0.0001", "1")
   * @returns {number} Rounded size
   */
  roundToLotSize(size, lotSize) {
    const lot = parseFloat(lotSize);
    const rounded = Math.round(size / lot) * lot;

    // Determine decimal places from lot size to avoid floating point issues
    const decimalPlaces = lotSize.includes('.') ? lotSize.split('.')[1].length : 0;

    return parseFloat(rounded.toFixed(decimalPlaces));
  }

  /**
   * Create a limit order with spread or exact price
   * @param {string} symbol - Trading symbol (e.g., 'BTC', 'SOL')
   * @param {string} side - Order side: 'buy' (bid/long) or 'sell' (ask/short)
   * @param {number} amount - Order amount
   * @param {number} spreadOrPrice - Spread percentage (default 0.5%) OR exact price if options.exactPrice=true
   * @param {object} options - Additional options (tif, reduce_only, client_order_id, exactPrice)
   * @returns {Promise} Order result
   */
  async createLimitOrder(symbol, side, amount, spreadOrPrice = 0.5, options = {}) {
    if (!this.account) {
      throw new Error('Account not configured');
    }

    // Get market info for proper tick/lot size rounding
    const marketInfo = await this.getMarketInfo();
    const symbolInfo = marketInfo.get(symbol);

    if (!symbolInfo) {
      throw new Error(`Market info not found for ${symbol}`);
    }

    const tickSize = symbolInfo.tick_size;
    const lotSize = symbolInfo.lot_size;

    if (!tickSize || !lotSize) {
      throw new Error(`tick_size or lot_size not available for ${symbol}`);
    }

    // Calculate order price based on side and spread OR use exact price
    let price;
    let orderSide;

    if (options.exactPrice) {
      // Use exact price provided
      price = spreadOrPrice;
      orderSide = side === 'buy' ? 'bid' : 'ask';
    } else {
      // Get current market price for spread-based pricing
      const bidAsk = this.getBidAsk(symbol);
      if (!bidAsk || !bidAsk.bid || !bidAsk.ask) {
        throw new Error(`No market data available for ${symbol}`);
      }

      const spread = spreadOrPrice;

      if (side === 'buy') {
        // Buy order (bid/long): place below current ask
        price = bidAsk.ask * (1 - spread / 100);
        orderSide = 'bid';
      } else if (side === 'sell') {
        // Sell order (ask/short): place above current bid
        price = bidAsk.bid * (1 + spread / 100);
        orderSide = 'ask';
      } else {
        throw new Error(`Invalid side: ${side}. Must be 'buy' or 'sell'`);
      }
    }

    // Round price to tick size (robust for any symbol)
    price = this.roundToTickSize(price, tickSize);

    // Round amount to lot size (robust for any symbol)
    amount = this.roundToLotSize(amount, lotSize);

    console.log(`[Pacifica] ${side} ${amount} ${symbol} at ${price} (tick_size: ${tickSize}, lot_size: ${lotSize})`);

    // Generate client order ID if not provided (must be a valid UUID)
    const clientOrderId = options.client_order_id || randomUUID();

    // Build signature header and payload
    const timestamp = Date.now();
    const expiryWindow = options.expiry_window || 5000; // 5 seconds default

    const signatureHeader = {
      type: 'create_order',
      timestamp: timestamp,
      expiry_window: expiryWindow
    };

    const signaturePayload = {
      symbol: symbol,
      price: price.toString(),
      amount: amount.toString(),
      side: orderSide,
      tif: options.tif || 'ALO', // ALO = Add Liquidity Only (post-only for market making)
      reduce_only: options.reduce_only || false,
      client_order_id: clientOrderId
    };

    // Sign the message
    const signature = this.signMessage(signatureHeader, signaturePayload);

    // Build request parameters (flat structure for sending)
    const orderParams = {
      account: this.account,
      signature: signature,
      timestamp: timestamp,
      expiry_window: expiryWindow,
      ...signaturePayload // Spread the payload
    };

    // Add agent_wallet if using agent keys
    if (this.agentWallet) {
      orderParams.agent_wallet = this.agentWallet;
    }

    // ALWAYS use REST API for order placement (more reliable than WebSocket)
    return await this.createOrderRest(orderParams);
  }

  /**
   * Create a market order (using IOC with aggressive pricing)
   * @param {string} symbol - Trading symbol (e.g., 'BTC', 'SOL')
   * @param {string} side - Order side: 'buy' or 'sell'
   * @param {number} amount - Order amount
   * @param {object} options - Additional options (reduce_only, client_order_id)
   * @returns {Promise} Order result
   */
  async createMarketOrder(symbol, side, amount, options = {}) {
    // Use aggressive spread to cross the orderbook and IOC to fill immediately
    const aggressiveSpread = -4.0; // -4% to ensure we cross the spread

    return await this.createLimitOrder(symbol, side, amount, aggressiveSpread, {
      ...options,
      tif: 'IOC' // Immediate or Cancel
    });
  }

  /**
   * Send order via WebSocket
   */
  async createOrderWebSocket(orderParams) {
    return new Promise((resolve, reject) => {
      if (!this.connected || !this.ws) {
        reject(new Error('WebSocket not connected'));
        return;
      }

      const id = randomUUID();
      const message = {
        id: id,
        params: {
          create_order: orderParams
        }
      };

      // Set up response handler with timeout
      const timeout = setTimeout(() => {
        reject(new Error('Order request timeout'));
      }, 10000);

      const messageHandler = (data) => {
        try {
          const response = JSON.parse(data.toString());

          if (response.id === id) {
            clearTimeout(timeout);
            this.ws.removeListener('message', messageHandler);

            if (response.code && response.code !== 200) {
              reject(new Error(`Order failed with code ${response.code}: ${JSON.stringify(response.data)}`));
            } else if (response.code === 200) {
              console.log('[Pacifica] Order placed via WebSocket:', response.data);
              resolve(response.data);
            } else if (response.error) {
              reject(new Error(response.error.message || 'Order failed'));
            } else {
              console.log('[Pacifica] Order placed via WebSocket:', response);
              resolve(response);
            }
          }
        } catch (error) {
          // Ignore parsing errors for other messages
        }
      };

      this.ws.on('message', messageHandler);

      try {
        this.ws.send(JSON.stringify(message));
        console.log(`[Pacifica] Sent ${orderParams.side} order for ${orderParams.symbol} at ${orderParams.price}`);
      } catch (error) {
        clearTimeout(timeout);
        this.ws.removeListener('message', messageHandler);
        reject(error);
      }
    });
  }

  /**
   * Send order via REST API
   */
  async createOrderRest(orderParams) {
    await this.restRateLimiter.waitForSlot();

    try {
      const url = `${this.restUrl}/api/v1/orders/create`;

      // For REST API, include agent_wallet in request body if using agent keys
      const requestBody = { ...orderParams };
      if (this.agentWallet) {
        requestBody.agent_wallet = this.agentWallet;
      }

      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify(requestBody)
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }

      const result = await response.json();
      console.log('[Pacifica] Order placed via REST:', result);

      // Normalize REST API response to match WebSocket format
      // REST returns: {success: true, data: {order_id: 123}, ...}
      // WebSocket returns: {s: "SOL", i: 123, I: "uuid"}
      const data = result.data || result;

      // If REST API format, normalize to WebSocket format
      if (data.order_id && !data.i) {
        return {
          i: data.order_id,
          I: orderParams.client_order_id, // Include the client order ID we sent
          s: orderParams.symbol
        };
      }

      return data;
    } catch (error) {
      console.error('[Pacifica] REST order error:', error.message);
      throw error;
    }
  }

  /**
   * Cancel an order
   * @param {string} symbol - Trading symbol
   * @param {string} orderId - Order ID or client order ID to cancel
   * @param {boolean} isClientOrderId - Whether orderId is a client_order_id (default: true)
   * @param {object} options - Additional options (expiry_window)
   * @returns {Promise} Cancellation result
   */
  async cancelOrder(symbol, orderId, isClientOrderId = true, options = {}) {
    if (!this.account) {
      throw new Error('Account not configured');
    }

    const timestamp = Date.now();
    const expiryWindow = options.expiry_window || 5000;

    // Build signature header and payload
    const signatureHeader = {
      type: 'cancel_order',
      timestamp: timestamp,
      expiry_window: expiryWindow
    };

    const signaturePayload = {
      symbol: symbol
    };

    if (isClientOrderId) {
      signaturePayload.client_order_id = orderId;
    } else {
      signaturePayload.order_id = parseInt(orderId);
    }

    // Sign the message
    const signature = this.signMessage(signatureHeader, signaturePayload);

    // Build request parameters (flat structure for sending)
    const cancelParams = {
      account: this.account,
      signature: signature,
      timestamp: timestamp,
      expiry_window: expiryWindow,
      ...signaturePayload
    };

    // Add agent_wallet if using agent keys
    if (this.agentWallet) {
      cancelParams.agent_wallet = this.agentWallet;
    }

    // ALWAYS use REST API for cancellation (more reliable than WebSocket)
    return await this.cancelOrderRest(cancelParams);
  }

  /**
   * Cancel order via WebSocket
   */
  async cancelOrderWebSocket(cancelParams) {
    return new Promise((resolve, reject) => {
      if (!this.connected || !this.ws) {
        reject(new Error('WebSocket not connected'));
        return;
      }

      const id = randomUUID();
      const message = {
        id: id,
        params: {
          cancel_order: cancelParams
        }
      };

      // Set up response handler with timeout
      const timeout = setTimeout(() => {
        reject(new Error('Cancel request timeout'));
      }, 10000);

      const messageHandler = (data) => {
        try {
          const response = JSON.parse(data.toString());

          if (response.id === id) {
            clearTimeout(timeout);
            this.ws.removeListener('message', messageHandler);

            if (response.code && response.code !== 200) {
              reject(new Error(`Cancel failed with code ${response.code}: ${JSON.stringify(response.data)}`));
            } else if (response.code === 200) {
              console.log('[Pacifica] Order cancelled via WebSocket:', response.data);
              resolve(response.data);
            } else if (response.error) {
              reject(new Error(response.error.message || 'Cancel failed'));
            } else {
              console.log('[Pacifica] Order cancelled via WebSocket:', response);
              resolve(response);
            }
          }
        } catch (error) {
          // Ignore parsing errors for other messages
        }
      };

      this.ws.on('message', messageHandler);

      try {
        this.ws.send(JSON.stringify(message));
        console.log('[Pacifica] Sent cancel request:', cancelParams);
      } catch (error) {
        clearTimeout(timeout);
        this.ws.removeListener('message', messageHandler);
        reject(error);
      }
    });
  }

  /**
   * Cancel order via REST API
   */
  async cancelOrderRest(cancelParams) {
    await this.restRateLimiter.waitForSlot();

    try {
      const url = `${this.restUrl}/api/v1/orders/cancel`;

      // For REST API, include agent_wallet in request body if using agent keys
      const requestBody = { ...cancelParams };
      if (this.agentWallet) {
        requestBody.agent_wallet = this.agentWallet;
      }

      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify(requestBody)
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }

      const result = await response.json();
      console.log('[Pacifica] Order cancelled via REST:', result);
      return result.data || result;
    } catch (error) {
      console.error('[Pacifica] REST cancel error:', error.message);
      throw error;
    }
  }

  /**
   * Cancel all orders
   * @param {object} options - { allSymbols: true/false, symbol: 'SOL', excludeReduceOnly: false }
   * @returns {Promise} Cancellation result with cancelled_count
   */
  async cancelAllOrders(options = {}) {
    // Build cancel all parameters using the helper method (DRY principle)
    const cancelAllParams = await this.buildCancelAllParams(options);

    // ALWAYS use REST API for cancellation (more reliable than WebSocket)
    return await this.cancelAllOrdersRest(cancelAllParams);
  }

  /**
   * Cancel all orders via WebSocket
   */
  async cancelAllOrdersWebSocket(cancelAllParams) {
    return new Promise((resolve, reject) => {
      if (!this.connected || !this.ws) {
        reject(new Error('WebSocket not connected'));
        return;
      }

      const id = randomUUID();
      const message = {
        id: id,
        params: {
          cancel_all_orders: cancelAllParams
        }
      };

      // Set up response handler with timeout
      const timeout = setTimeout(() => {
        reject(new Error('Cancel all request timeout'));
      }, 10000);

      const messageHandler = (data) => {
        try {
          const response = JSON.parse(data.toString());

          if (response.id === id) {
            clearTimeout(timeout);
            this.ws.removeListener('message', messageHandler);

            if (response.code && response.code !== 200) {
              reject(new Error(`Cancel all failed with code ${response.code}: ${JSON.stringify(response.data)}`));
            } else if (response.code === 200) {
              console.log('[Pacifica] All orders cancelled via WebSocket:', response.data);
              resolve(response.data);
            } else if (response.error) {
              reject(new Error(response.error.message || 'Cancel all failed'));
            } else {
              console.log('[Pacifica] All orders cancelled via WebSocket:', response);
              resolve(response);
            }
          }
        } catch (error) {
          // Ignore parsing errors for other messages
        }
      };

      this.ws.on('message', messageHandler);

      try {
        this.ws.send(JSON.stringify(message));
        console.log('[Pacifica] Sent cancel all request:', cancelAllParams);
      } catch (error) {
        clearTimeout(timeout);
        this.ws.removeListener('message', messageHandler);
        reject(error);
      }
    });
  }

  /**
   * Cancel all orders via REST API
   */
  async cancelAllOrdersRest(cancelAllParams) {
    await this.restRateLimiter.waitForSlot();

    try {
      const url = `${this.restUrl}/api/v1/orders/cancel_all`;

      // For REST API, include agent_wallet in request body if using agent keys
      const requestBody = { ...cancelAllParams };
      if (this.agentWallet) {
        requestBody.agent_wallet = this.agentWallet;
      }

      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify(requestBody)
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }

      const result = await response.json();
      console.log('[Pacifica] All orders cancelled via REST:', result);
      return result.data || result;
    } catch (error) {
      console.error('[Pacifica] REST cancel all error:', error.message);

      // Check if this is a rate limit error (HTTP 429)
      if (error.message && error.message.includes('429')) {
        this.handleRateLimitExceeded();
      }

      throw error;
    }
  }

  /**
   * Handle rate limit exceeded (HTTP 429)
   * - Sets rate limit flag
   * - Emits event to halt trading
   * - Attempts WebSocket order cancellation (doesn't count against REST limits!)
   * - Starts cooldown timer for auto-recovery
   */
  async handleRateLimitExceeded() {
    if (this.rateLimitExceeded) {
      return; // Already in rate limit state
    }

    console.error('\n' + '='.repeat(80));
    console.error('🚨 [PACIFICA] RATE LIMIT EXCEEDED (HTTP 429)');
    console.error('='.repeat(80));
    console.error('⚠️  Trading HALTED to prevent imbalances');
    console.error(`⏱️  Cooldown period: ${this.rateLimitCooldownMs / 1000}s`);
    console.error('='.repeat(80) + '\n');

    // Set flag to halt trading
    this.rateLimitExceeded = true;

    // Emit event for bot to stop order placement
    this.emit('rateLimitExceeded', {
      timestamp: Date.now(),
      cooldownMs: this.rateLimitCooldownMs
    });

    // CRITICAL: Try to cancel all orders via WebSocket (doesn't count against REST rate limit!)
    if (this.connected && this.ws) {
      console.log('🔄 [PACIFICA] Attempting WebSocket order cancellation (safe - no REST API calls)...');
      try {
        const cancelParams = await this.buildCancelAllParams();
        const result = await this.cancelAllOrdersWebSocket(cancelParams);
        console.log('✅ [PACIFICA] Orders cancelled via WebSocket:', result);
      } catch (wsError) {
        console.error('❌ [PACIFICA] WebSocket cancellation failed:', wsError.message);
        console.error('⚠️  Open orders may still be active - monitor positions carefully!');
      }
    } else {
      console.error('❌ [PACIFICA] WebSocket not connected - cannot cancel orders safely');
      console.error('⚠️  Open orders may still be active - monitor positions carefully!');
    }

    console.log();

    // Clear any existing cooldown timer
    if (this.rateLimitCooldownTimer) {
      clearTimeout(this.rateLimitCooldownTimer);
    }

    // Start cooldown timer for auto-recovery
    this.rateLimitCooldownTimer = setTimeout(() => {
      console.log('\n' + '='.repeat(80));
      console.log('✅ [PACIFICA] Rate limit cooldown complete');
      console.log('='.repeat(80));
      console.log('🔄 Trading RESUMED');
      console.log('='.repeat(80) + '\n');

      this.rateLimitExceeded = false;
      this.emit('rateLimitRecovered', { timestamp: Date.now() });
    }, this.rateLimitCooldownMs);
  }

  /**
   * Build cancel all orders parameters (signature + payload)
   * Used by both REST and WebSocket cancellation methods
   */
  async buildCancelAllParams(options = {}) {
    if (!this.account) {
      throw new Error('Account not configured');
    }

    const timestamp = Date.now();
    const expiryWindow = options.expiry_window || 5000;

    // Build signature header and payload
    const signatureHeader = {
      type: 'cancel_all_orders',
      timestamp: timestamp,
      expiry_window: expiryWindow
    };

    const signaturePayload = {
      all_symbols: options.allSymbols !== undefined ? options.allSymbols : true,
      exclude_reduce_only: options.excludeReduceOnly !== undefined ? options.excludeReduceOnly : false
    };

    if (!options.allSymbols && options.symbol) {
      signaturePayload.symbol = options.symbol;
    }

    // Sign the action
    const signature = this.signMessage(signatureHeader, signaturePayload);

    // Build complete params
    const cancelAllParams = {
      account: this.account,
      signature: signature,
      timestamp: timestamp,
      expiry_window: expiryWindow,
      ...signaturePayload
    };

    // Add agent_wallet if using agent keys
    if (this.agentWallet) {
      cancelAllParams.agent_wallet = this.agentWallet;
    }

    return cancelAllParams;
  }

  /**
   * Check if currently rate limited
   * @returns {boolean} True if rate limited
   */
  isRateLimited() {
    return this.rateLimitExceeded;
  }

  /**
   * Smart cancel all orders - automatically uses WebSocket when rate limited, REST otherwise
   * @param {object} options - { allSymbols: true/false, symbol: 'SOL', excludeReduceOnly: false }
   * @returns {Promise} Cancellation result with cancelled_count
   */
  async cancelAllOrdersSmart(options = {}) {
    // Build parameters first
    const cancelAllParams = await this.buildCancelAllParams(options);

    // Use WebSocket if rate limited (doesn't count against REST rate limits)
    if (this.rateLimitExceeded) {
      console.log('[PACIFICA] 🔄 Using WebSocket cancel (rate limited)');
      return await this.cancelAllOrdersWebSocket(cancelAllParams);
    }

    // Use REST API normally (more reliable)
    return await this.cancelAllOrdersRest(cancelAllParams);
  }

  /**
   * Get account balance and margin information
   * Attempts WebSocket first, falls back to REST API if needed
   * @param {string} account - Account address (defaults to configured account)
   * @param {object} options - Options (timeout, forceRest)
   * @returns {Promise<object>} Balance information
   */
  async getBalance(account = null, options = {}) {
    account = account || this.account;
    const timeout = options.timeout || 10000;
    const forceRest = options.forceRest || false;

    if (!account) {
      throw new Error('Account address required to get balance');
    }

    // Try REST fallback if forced or if not connected
    if (forceRest || !this.connected) {
      console.log('[Pacifica] Using REST fallback for balance');
      return await this.getBalanceRest(account);
    }

    // Try WebSocket first
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        console.log('[Pacifica] WebSocket timeout, falling back to REST');
        // Fallback to REST on timeout
        this.getBalanceRest(account)
          .then(resolve)
          .catch(reject);
      }, timeout);

      // Set up one-time listener for account_info
      const messageHandler = (data) => {
        try {
          const message = JSON.parse(data.toString());

          if (message.channel === 'account_info') {
            const info = message.data;

            clearTimeout(timeoutId);
            this.ws.removeListener('message', messageHandler);

            // Parse and return balance information
            resolve({
              accountEquity: parseFloat(info.ae || '0'),
              availableToSpend: parseFloat(info.as || '0'),
              availableToWithdraw: parseFloat(info.aw || '0'),
              balance: parseFloat(info.b || '0'),
              totalMarginUsed: parseFloat(info.mu || '0'),
              maintenanceMargin: parseFloat(info.cm || '0'),
              feeTier: parseInt(info.f || '1'),
              ordersCount: parseInt(info.oc || '0'),
              positionsCount: parseInt(info.pc || '0'),
              stopOrdersCount: parseInt(info.sc || '0'),
              pendingBalance: parseFloat(info.pb || '0'),
              timestamp: info.t || Date.now(),
              source: 'websocket'
            });
          }
        } catch (error) {
          // Ignore parsing errors for other messages
        }
      };

      this.ws.on('message', messageHandler);

      try {
        const subscribeMessage = {
          method: 'subscribe',
          params: {
            source: 'account_info',
            account: account
          }
        };

        this.ws.send(JSON.stringify(subscribeMessage));
        console.log(`[Pacifica] Subscribed to account_info for ${account}`);
      } catch (error) {
        clearTimeout(timeoutId);
        this.ws.removeListener('message', messageHandler);
        console.log('[Pacifica] WebSocket error, falling back to REST');
        this.getBalanceRest(account)
          .then(resolve)
          .catch(reject);
      }
    });
  }

  /**
   * Get account balance via REST API (fallback method)
   * Derives balance information from positions
   * @param {string} account - Account address
   * @returns {Promise<object>} Approximate balance information
   */
  async getBalanceRest(account) {
    await this.restRateLimiter.waitForSlot();

    try {
      const positionsUrl = `${this.restUrl}/api/v1/positions?account=${account}`;
      const positionsResponse = await fetch(positionsUrl, {
        method: 'GET',
        headers: { 'Accept': 'application/json' }
      });

      if (!positionsResponse.ok) {
        throw new Error(`HTTP ${positionsResponse.status}: ${positionsResponse.statusText}`);
      }

      const positionsResult = await positionsResponse.json();
      const positions = positionsResult.data || [];

      let totalMarginUsed = 0;
      for (const pos of positions) {
        if (pos.margin) {
          totalMarginUsed += parseFloat(pos.margin || '0');
        }
      }

      console.log('[Pacifica] REST balance: Derived from positions (limited data)');

      return {
        accountEquity: 0,
        availableToSpend: 0,
        availableToWithdraw: 0,
        balance: 0,
        totalMarginUsed: totalMarginUsed,
        maintenanceMargin: 0,
        feeTier: 1,
        ordersCount: 0,
        positionsCount: positions.length,
        stopOrdersCount: 0,
        pendingBalance: 0,
        timestamp: Date.now(),
        source: 'rest',
        warning: 'Limited data - REST fallback only provides position info. Use WebSocket for full balance.'
      };
    } catch (error) {
      console.error('[Pacifica] Error fetching balance via REST:', error.message);
      throw error;
    }
  }
}

export default PacificaConnector;
