import WebSocket from 'ws';
import fetch from 'node-fetch';
import { EventEmitter } from 'events';
import { SlidingWindowRateLimiter } from '../utils/rate-limiter.js';
import { ethers } from 'ethers';
import dotenv from 'dotenv';
import { encode as msgpackEncode } from '@msgpack/msgpack';

dotenv.config();

class HyperliquidConnector extends EventEmitter {
  constructor(options = {}) {
    super();

    this.wsUrl = options.wsUrl || 'wss://api.hyperliquid.xyz/ws';
    this.restUrl = options.restUrl || 'https://api.hyperliquid.xyz/info';
    this.exchangeUrl = options.exchangeUrl || 'https://api.hyperliquid.xyz/exchange';
    this.testnet = options.testnet || false;

    if (this.testnet) {
      this.wsUrl = 'wss://api.hyperliquid-testnet.xyz/ws';
      this.restUrl = 'https://api.hyperliquid-testnet.xyz/info';
      this.exchangeUrl = 'https://api.hyperliquid-testnet.xyz/exchange';
    }

    // Load credentials from environment or options
    this.wallet = options.wallet || process.env.HL_WALLET;
    this.privateKey = options.privateKey || process.env.HL_PRIVATE_KEY;

    // Initialize signer if private key is provided
    if (this.privateKey) {
      this.signer = new ethers.Wallet(this.privateKey);
    }

    // Connection state
    this.ws = null;
    this.connected = false;
    this.reconnecting = false;
    this.intentionalDisconnect = false; // Flag to prevent auto-reconnect after manual disconnect
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = options.maxReconnectAttempts || 10;
    this.reconnectDelay = options.reconnectDelay || 1000;
    this.maxReconnectDelay = options.maxReconnectDelay || 30000;

    // Health monitoring
    this.pingInterval = options.pingInterval || 30000;
    this.pongTimeout = options.pongTimeout || 10000;
    this.pingTimer = null;
    this.pongTimer = null;
    this.connectionTimeout = null; // Connection timeout timer
    this.lastPongReceived = Date.now();

    // REST API fallback
    this.useRestFallback = false;
    this.restPollInterval = options.restPollInterval || 1000;
    this.restPollTimer = null;

    // Staleness monitoring for automatic REST fallback
    this.stalenessThreshold = options.stalenessThreshold || 60000; // 60 seconds
    this.stalenessTimer = null;

    // Periodic REST refresh (every 5s) to supplement WebSocket
    this.restRefreshInterval = options.restRefreshInterval || 5000; // 5 seconds
    this.restRefreshTimer = null;

    // Orderbook data cache
    this.orderbooks = new Map();

    // Request tracking
    this.requestId = 0;
    this.pendingRequests = new Map();

    // Subscriptions
    this.subscriptions = new Set();

    // Track polling requests per coin to avoid overlapping
    this.pollingInProgress = new Map();

    // Rate limiters
    // WebSocket: max 2000 messages per minute, max 100 inflight
    this.wsRateLimiter = new SlidingWindowRateLimiter({
      maxRequests: 1800, // Set to 1800 to have some buffer
      windowMs: 60000 // 1 minute
    });

    // REST: max 1200 weight per minute (l2Book has weight 2)
    this.restRateLimiter = new SlidingWindowRateLimiter({
      maxRequests: 600, // 600 requests × 2 weight = 1200
      windowMs: 60000 // 1 minute
    });

    // Track inflight WebSocket requests
    this.maxInflightRequests = options.maxInflightRequests || 90; // Max 100, use 90 for buffer
  }

  /**
   * Connect to Hyperliquid WebSocket
   */
  async connect() {
    return new Promise((resolve, reject) => {
      try {
        this.intentionalDisconnect = false; // Reset flag when connecting
        this.ws = new WebSocket(this.wsUrl);

        this.ws.on('open', () => {
          console.log('[Hyperliquid] WebSocket connected');
          this.connected = true;
          this.reconnecting = false;
          this.reconnectAttempts = 0;
          this.useRestFallback = false;

          // Clear connection timeout
          if (this.connectionTimeout) {
            clearTimeout(this.connectionTimeout);
            this.connectionTimeout = null;
          }

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

        this.ws.on('pong', () => {
          this.lastPongReceived = Date.now();
          if (this.pongTimer) {
            clearTimeout(this.pongTimer);
            this.pongTimer = null;
          }
        });

        this.ws.on('error', (error) => {
          console.error('[Hyperliquid] WebSocket error:', error.message);
          this.emit('error', error);
        });

        this.ws.on('close', () => {
          console.log('[Hyperliquid] WebSocket closed');
          this.connected = false;
          this.stopHealthMonitoring();

          this.emit('disconnected');

          // Attempt to reconnect only if not intentionally disconnected
          if (!this.reconnecting && !this.intentionalDisconnect) {
            this.handleReconnect();
          }
        });

        // Connection timeout
        this.connectionTimeout = setTimeout(() => {
          if (!this.connected) {
            reject(new Error('Connection timeout'));
            if (this.ws) {
              this.ws.terminate();
            }
          }
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

      // Handle subscription responses
      if (message.channel === 'subscriptionResponse') {
        console.log('[Hyperliquid] Subscription confirmed:', message.data);
        return;
      }

      // Handle post responses
      if (message.channel === 'post') {
        const { id, response } = message.data;

        if (this.pendingRequests.has(id)) {
          const { resolve, reject, timeout } = this.pendingRequests.get(id);

          // Clear the timeout
          if (timeout) {
            clearTimeout(timeout);
          }

          this.pendingRequests.delete(id);

          if (response.type === 'error') {
            reject(new Error(response.payload));
          } else {
            resolve(response.payload);
          }
        }

        // Handle l2Book responses
        if (response.type === 'info' && response.payload?.type === 'l2Book') {
          const bookData = response.payload.data;
          this.updateOrderbook(bookData);
        }

        return;
      }

      // Handle subscription data updates
      if (message.channel === 'l2Book') {
        this.updateOrderbook(message.data);
        return;
      }

    } catch (error) {
      console.error('[Hyperliquid] Error parsing message:', error);
    }
  }

  /**
   * Update orderbook cache
   */
  updateOrderbook(data) {
    const { coin, levels, time } = data;

    if (!coin || !levels || levels.length < 2) {
      return;
    }

    const [bids, asks] = levels;

    // Extract best bid and ask
    let bestBid = null;
    let bestAsk = null;

    if (bids && bids.length > 0) {
      bestBid = {
        price: parseFloat(bids[0].px),
        size: parseFloat(bids[0].sz),
        numOrders: bids[0].n
      };
    }

    if (asks && asks.length > 0) {
      bestAsk = {
        price: parseFloat(asks[0].px),
        size: parseFloat(asks[0].sz),
        numOrders: asks[0].n
      };
    }

    const orderbook = {
      coin,
      bestBid,
      bestAsk,
      bids,
      asks,
      timestamp: time || Date.now()
    };

    this.orderbooks.set(coin, orderbook);
    this.emit('orderbook', orderbook);
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

    // Add to subscriptions
    this.subscriptions.add(coin);

    if (this.useRestFallback) {
      // Start REST polling if not already started
      if (!this.restPollTimer) {
        this.startRestPolling();
      }
      return;
    }

    // Request initial snapshot via WebSocket
    try {
      await this.requestL2Book(coin);
    } catch (error) {
      console.error(`[Hyperliquid] Failed to get initial orderbook for ${coin}:`, error.message);
    }

    // Start staleness monitoring if not already running
    // This will detect if WebSocket stops sending updates and fall back to REST
    if (!this.stalenessTimer) {
      this.startStalenessMonitoring();
    }

    // Start periodic REST refresh if this is the first subscription
    // This supplements WebSocket with periodic REST updates every 5s
    if (this.subscriptions.size === 1 && !this.restRefreshTimer) {
      this.startPeriodicRestRefresh();
    }
  }

  /**
   * Request L2 orderbook via WebSocket post
   */
  async requestL2Book(coin, nSigFigs = 5) {
    // Check inflight request limit
    if (this.pendingRequests.size >= this.maxInflightRequests) {
      throw new Error('Too many inflight requests');
    }

    // Check rate limit
    if (!this.wsRateLimiter.canRequest()) {
      throw new Error('Rate limit exceeded');
    }

    return new Promise((resolve, reject) => {
      if (!this.connected) {
        reject(new Error('Not connected'));
        return;
      }

      // Consume rate limit token
      if (!this.wsRateLimiter.tryRequest()) {
        reject(new Error('Rate limit exceeded'));
        return;
      }

      const id = ++this.requestId;

      const request = {
        method: 'post',
        id,
        request: {
          type: 'info',
          payload: {
            type: 'l2Book',
            coin,
            nSigFigs,
            mantissa: null
          }
        }
      };

      // Store pending request with timeout
      const timeoutId = setTimeout(() => {
        if (this.pendingRequests.has(id)) {
          this.pendingRequests.delete(id);
          reject(new Error('Request timeout'));
        }
      }, 10000); // Increased to 10 seconds

      this.pendingRequests.set(id, {
        resolve,
        reject,
        timeout: timeoutId
      });

      // Send request
      try {
        this.ws.send(JSON.stringify(request));
      } catch (error) {
        if (this.pendingRequests.has(id)) {
          clearTimeout(this.pendingRequests.get(id).timeout);
          this.pendingRequests.delete(id);
        }
        reject(error);
      }
    });
  }

  /**
   * Request clearinghouse state (balances) via WebSocket post
   * @param {string} user - Wallet address
   * @returns {Promise<object>} Payload object { type: 'clearinghouseState', data: { ... } }
   */
  async requestClearinghouseStateWs(user) {
    if (!user && !this.wallet) {
      throw new Error('User address required for clearinghouse state');
    }

    // Check inflight request limit
    if (this.pendingRequests.size >= this.maxInflightRequests) {
      throw new Error('Too many inflight requests');
    }

    // Check rate limit
    if (!this.wsRateLimiter.canRequest()) {
      throw new Error('Rate limit exceeded');
    }

    return new Promise((resolve, reject) => {
      if (!this.connected) {
        reject(new Error('Not connected'));
        return;
      }

      // Consume rate limit token
      if (!this.wsRateLimiter.tryRequest()) {
        reject(new Error('Rate limit exceeded'));
        return;
      }

      const id = ++this.requestId;

      const request = {
        method: 'post',
        id,
        request: {
          type: 'info',
          payload: {
            type: 'clearinghouseState',
            user: user || this.wallet
          }
        }
      };

      // Store pending request with timeout
      const timeoutId = setTimeout(() => {
        if (this.pendingRequests.has(id)) {
          this.pendingRequests.delete(id);
          reject(new Error('Request timeout'));
        }
      }, 10000);

      this.pendingRequests.set(id, {
        resolve,
        reject,
        timeout: timeoutId
      });

      // Send request
      try {
        this.ws.send(JSON.stringify(request));
      } catch (error) {
        if (this.pendingRequests.has(id)) {
          clearTimeout(this.pendingRequests.get(id).timeout);
          this.pendingRequests.delete(id);
        }
        reject(error);
      }
    });
  }

  /**
   * Poll orderbook via WebSocket
   * Uses a shared polling loop for all symbols to respect rate limits
   */
  startWebSocketPolling(coin) {
    // Don't start individual polling loops
    // Polling is handled by the global polling loop started in subscribeOrderbook
  }

  /**
   * Start global polling loop for all subscribed symbols
   */
  startGlobalPolling() {
    if (this.globalPollingTimer) {
      return; // Already running
    }

    // Calculate polling interval based on number of subscriptions
    // Target: ~20 requests/second for safety (1200/minute)
    const calculateInterval = () => {
      const numSymbols = this.subscriptions.size;
      if (numSymbols === 0) return 1000;

      // Aim for ~20 total requests per second
      // If we have 5 symbols, interval = 5 * 50ms = 250ms per cycle
      return Math.max(50, numSymbols * 50);
    };

    let symbolIterator = null;

    this.globalPollingTimer = setInterval(async () => {
      if (!this.connected || this.useRestFallback) {
        return;
      }

      if (this.subscriptions.size === 0) {
        return;
      }

      // Create or reset iterator
      if (!symbolIterator || symbolIterator.done) {
        symbolIterator = this.subscriptions.values();
      }

      // Get next symbol
      const next = symbolIterator.next();
      if (next.done) {
        symbolIterator = this.subscriptions.values();
        return;
      }

      const coin = next.value;

      // Skip if already polling this coin
      if (this.pollingInProgress.get(coin)) {
        return;
      }

      this.pollingInProgress.set(coin, true);

      try {
        await this.requestL2Book(coin);
      } catch (error) {
        // Silently ignore rate limit and inflight errors
        if (!error.message.includes('Rate limit') &&
            !error.message.includes('inflight') &&
            !error.message.includes('timeout')) {
          console.error(`[Hyperliquid] Error polling orderbook for ${coin}:`, error.message);
        }
      } finally {
        this.pollingInProgress.set(coin, false);
      }
    }, 50); // Poll every 50ms, cycling through symbols
  }

  /**
   * Stop global polling loop
   */
  stopGlobalPolling() {
    if (this.globalPollingTimer) {
      clearInterval(this.globalPollingTimer);
      this.globalPollingTimer = null;
    }
  }

  /**
   * Request L2 orderbook via REST API
   */
  async requestL2BookRest(coin, nSigFigs = 5) {
    // Wait for rate limit slot (l2Book has weight 2)
    await this.restRateLimiter.waitForSlot();

    try {
      const response = await fetch(this.restUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          type: 'l2Book',
          coin,
          nSigFigs,
          mantissa: null
        })
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();
      return data;
    } catch (error) {
      console.error(`[Hyperliquid] REST API error for ${coin}:`, error.message);
      throw error;
    }
  }

  /**
   * Start REST API polling fallback
   * @param {boolean} temporary - If true, don't check useRestFallback flag (for staleness fallback)
   */
  startRestPolling(temporary = false) {
    if (this.restPollTimer) {
      return;
    }

    console.log('[Hyperliquid] Starting REST API fallback polling' + (temporary ? ' (temporary)' : ''));

    this.restPollTimer = setInterval(async () => {
      // For permanent fallback, check the flag
      // For temporary fallback (staleness), always run
      if (!temporary && !this.useRestFallback) {
        this.stopRestPolling();
        return;
      }

      // Poll all subscribed coins
      for (const coin of this.subscriptions) {
        try {
          const data = await this.requestL2BookRest(coin);
          this.updateOrderbook({
            coin,
            levels: data.levels,
            time: data.time
          });
        } catch (error) {
          console.error(`[Hyperliquid] REST polling error for ${coin}:`, error);
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
   * Start periodic REST refresh (every 5s) to supplement WebSocket
   * This ensures prices stay fresh even if WebSocket updates miss or stale
   */
  startPeriodicRestRefresh() {
    if (this.restRefreshTimer) {
      return; // Already running
    }

    console.log('[Hyperliquid] Starting periodic REST refresh (every 5s)');

    this.restRefreshTimer = setInterval(async () => {
      // Refresh all subscribed coins
      for (const coin of this.subscriptions) {
        try {
          const data = await this.requestL2BookRest(coin);
          this.updateOrderbook({
            coin,
            levels: data.levels,
            time: data.time
          });
        } catch (error) {
          // Silently fail - WebSocket is primary, this is just a supplement
          if (!error.message.includes('Rate limit')) {
            console.error(`[Hyperliquid] REST refresh error for ${coin}:`, error.message);
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
      console.log('[Hyperliquid] Stopped periodic REST refresh');
    }
  }

  /**
   * Start monitoring for stale orderbook data
   * Falls back to REST polling if WebSocket data becomes stale
   */
  startStalenessMonitoring() {
    if (this.stalenessTimer) {
      return;
    }

    console.log('[Hyperliquid] Starting staleness monitoring');

    this.stalenessTimer = setInterval(() => {
      if (!this.connected || this.useRestFallback) {
        return;
      }

      const now = Date.now();
      let anyStale = false;

      // Check if any subscribed orderbook is stale
      for (const coin of this.subscriptions) {
        const orderbook = this.orderbooks.get(coin);

        if (!orderbook) {
          continue;
        }

        const age = now - orderbook.timestamp;

        if (age > this.stalenessThreshold) {
          console.warn(`[Hyperliquid] Orderbook for ${coin} is stale (${Math.round(age / 1000)}s old)`);
          anyStale = true;
        }
      }

      // If any orderbook is stale, start temporary REST fallback
      if (anyStale && !this.restPollTimer) {
        console.log('[Hyperliquid] WebSocket data stale, starting temporary REST fallback');
        this.startRestPolling(true); // temporary=true
      }

      // If all orderbooks are fresh and REST polling is active (and not permanent), stop it
      if (!anyStale && this.restPollTimer && !this.useRestFallback) {
        console.log('[Hyperliquid] WebSocket data resumed, stopping temporary REST fallback');
        this.stopRestPolling();
      }
    }, 10000); // Check every 10 seconds
  }

  /**
   * Stop staleness monitoring
   */
  stopStalenessMonitoring() {
    if (this.stalenessTimer) {
      clearInterval(this.stalenessTimer);
      this.stalenessTimer = null;
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
        console.error('[Hyperliquid] Pong timeout, connection may be dead');
        if (this.ws) {
          this.ws.terminate();
        }
        return;
      }

      // Send ping
      try {
        this.ws.ping();

        // Set pong timeout
        this.pongTimer = setTimeout(() => {
          console.error('[Hyperliquid] Pong timeout');
          if (this.ws) {
            this.ws.terminate();
          }
        }, this.pongTimeout);
      } catch (error) {
        console.error('[Hyperliquid] Error sending ping:', error);
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

    if (this.connectionTimeout) {
      clearTimeout(this.connectionTimeout);
      this.connectionTimeout = null;
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
      console.error('[Hyperliquid] Max reconnect attempts reached, switching to REST fallback');
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

    console.log(`[Hyperliquid] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

    setTimeout(async () => {
      try {
        await this.connect();
        console.log('[Hyperliquid] Reconnected successfully');
      } catch (error) {
        console.error('[Hyperliquid] Reconnection failed:', error);
        this.reconnecting = false;
        this.handleReconnect();
      }
    }, delay);
  }

  /**
   * Resubscribe to all previous subscriptions
   */
  resubscribe() {
    console.log(`[Hyperliquid] Resubscribing to ${this.subscriptions.size} coins`);

    for (const coin of this.subscriptions) {
      this.subscribeOrderbook(coin).catch(error => {
        console.error(`[Hyperliquid] Error resubscribing to ${coin}:`, error);
      });
    }
  }

  /**
   * Unsubscribe from orderbook updates
   */
  unsubscribe(coin) {
    this.subscriptions.delete(coin);
    this.orderbooks.delete(coin);
  }

  /**
   * Disconnect from Hyperliquid
   */
  disconnect() {
    console.log('[Hyperliquid] Disconnecting');

    this.intentionalDisconnect = true; // Prevent auto-reconnect
    this.reconnecting = false;
    this.stopHealthMonitoring();
    this.stopRestPolling();
    this.stopPeriodicRestRefresh();
    this.stopStalenessMonitoring();
    this.stopGlobalPolling();

    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }

    this.connected = false;
    this.subscriptions.clear();
    this.orderbooks.clear();
    this.pendingRequests.clear();
    this.pollingInProgress.clear();
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
   * Get asset metadata (to find asset IDs and szDecimals)
   */
  async getMeta() {
    try {
      const response = await fetch(this.restUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          type: 'meta'
        })
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();
      return data;
    } catch (error) {
      console.error('[Hyperliquid] Error fetching meta:', error.message);
      throw error;
    }
  }

  /**
   * Get asset ID from coin name
   */
  async getAssetId(coin) {
    if (!this.metaCache) {
      this.metaCache = await this.getMeta();
    }

    const assetIndex = this.metaCache.universe.findIndex(asset => asset.name === coin);
    if (assetIndex === -1) {
      throw new Error(`Asset ${coin} not found in meta`);
    }

    return assetIndex;
  }

  /**
   * Get asset metadata (szDecimals, etc.)
   */
  getAssetInfo(coin) {
    if (!this.metaCache) {
      throw new Error('Meta cache not loaded. Call getAssetId first.');
    }

    const asset = this.metaCache.universe.find(a => a.name === coin);
    if (!asset) {
      throw new Error(`Asset ${coin} not found in meta`);
    }

    return asset;
  }

  /**
   * Round price to proper tick size
   * Prices can have up to 5 significant figures
   * Max decimals = MAX_DECIMALS - szDecimals (6 for perps, 8 for spot)
   */
  roundPrice(price, szDecimals, isSpot = false) {
    const maxDecimals = (isSpot ? 8 : 6) - szDecimals;

    // Round to 5 significant figures
    let rounded = parseFloat(price.toPrecision(5));

    // Then limit decimal places
    rounded = parseFloat(rounded.toFixed(maxDecimals));

    // Remove trailing zeros by converting to string and back
    return rounded.toString();
  }

  /**
   * Round size to proper lot size (szDecimals)
   */
  roundSize(size, szDecimals) {
    const rounded = parseFloat(size.toFixed(szDecimals));
    return rounded.toString();
  }

  /**
   * Sign an action using EIP-712
   * Based on Hyperliquid Python SDK sign_l1_action
   */
  async signAction(action, nonce, vaultAddress = null, expiresAfter = null) {
    if (!this.signer) {
      throw new Error('Signer not initialized. Private key required.');
    }

    // Construct EIP-712 domain
    // chainId is always 1337 for Hyperliquid
    const domain = {
      chainId: 1337,
      name: 'Exchange',
      verifyingContract: '0x0000000000000000000000000000000000000000',
      version: '1'
    };

    // Construct EIP-712 types
    const types = {
      Agent: [
        { name: 'source', type: 'string' },
        { name: 'connectionId', type: 'bytes32' }
      ]
    };

    // Construct phantom agent
    // source: "a" for mainnet, "b" for testnet
    const source = this.testnet ? 'b' : 'a';

    // Create connection ID hash
    // This is keccak256 of msgpack encoded data + nonce + vault indicator + optional expiration
    const connectionId = await this.constructConnectionId(action, nonce, vaultAddress, expiresAfter);

    const agentMessage = {
      source,
      connectionId
    };

    // Sign the typed data
    const signature = await this.signer.signTypedData(domain, types, agentMessage);
    const sig = ethers.Signature.from(signature);

    return {
      r: sig.r,
      s: sig.s,
      v: sig.v
    };
  }

  /**
   * Construct connection ID for EIP-712 signing
   * This is a keccak256 hash of msgpack-encoded action + nonce + vault + expiration
   */
  async constructConnectionId(action, nonce, vaultAddress, expiresAfter) {
    // Encode action with msgpack
    const actionBytes = msgpackEncode(action);

    // Create data array to hash
    const dataToHash = [];

    // Add action bytes
    dataToHash.push(...actionBytes);

    // Add nonce as bytes
    const nonceHex = nonce.toString(16).padStart(16, '0');
    const nonceBytes = ethers.getBytes('0x' + nonceHex);
    dataToHash.push(...nonceBytes);

    // Add vault address indicator (1 if vault, 0 if not)
    dataToHash.push(vaultAddress ? 1 : 0);

    // Add optional expiration
    if (expiresAfter) {
      const expiryHex = expiresAfter.toString(16).padStart(16, '0');
      const expiryBytes = ethers.getBytes('0x' + expiryHex);
      dataToHash.push(...expiryBytes);
    }

    // Hash the combined data
    const hash = ethers.keccak256(new Uint8Array(dataToHash));

    return hash;
  }

  /**
   * Create a market order
   * For Hyperliquid, market orders are IOC (Immediate or Cancel) limit orders with aggressive pricing
   *
   * @param {string} coin - Coin symbol (e.g., 'SOL', 'BTC')
   * @param {string} side - 'buy' or 'sell'
   * @param {number} size - Order size
   * @param {object} options - Additional options (slippage, reduceOnly, cloid, vaultAddress)
   * @returns {Promise<object>} Order result
   */
  async createMarketOrder(coin, side, size, options = {}) {
    if (!this.wallet || !this.signer) {
      throw new Error('Wallet and private key required for trading');
    }

    // Get asset ID and metadata
    const assetId = await this.getAssetId(coin);
    const assetInfo = this.getAssetInfo(coin);

    const isBuy = side === 'buy';
    const reduceOnly = options.reduceOnly || false;
    const slippage = options.slippage || 0.05; // Default 5% slippage

    let midPrice;
    if (options.overrideMidPrice && Number.isFinite(options.overrideMidPrice)) {
      midPrice = options.overrideMidPrice;
      console.log(`[Hyperliquid] ℹ️ Using provided override mid-price: ${midPrice}`);
    } else {
      // Use cached prices from the REST poller
      let bidAsk = this.getBidAsk(coin);

      if (!bidAsk || !bidAsk.bid || !bidAsk.ask) {
        // If cache is empty, wait a moment and retry once. This can happen on startup.
        await new Promise(resolve => setTimeout(resolve, 500));
        bidAsk = this.getBidAsk(coin);
        if (!bidAsk || !bidAsk.bid || !bidAsk.ask) {
          throw new Error(`No cached orderbook data available for ${coin} to create market order.`);
        }
      }
      
      console.log(`[Hyperliquid] ℹ️ Using cached prices (age: ${Date.now() - bidAsk.timestamp}ms): bid=${bidAsk.bid}, ask=${bidAsk.ask}`);

      // Calculate mid price
      midPrice = (bidAsk.bid + bidAsk.ask) / 2;
    }

    // Calculate limit price with slippage (matching Python SDK logic):
    // - For buy: midPrice * (1 + slippage)
    // - For sell: midPrice * (1 - slippage)
    // IOC ensures immediate execution at best available price
    let limitPrice;
    if (isBuy) {
      limitPrice = midPrice * (1 + slippage);
    } else {
      limitPrice = midPrice * (1 - slippage);
    }

    // Round price to proper tick size (5 sig figs, max decimals based on szDecimals)
    const isSpot = assetId >= 10000;
    const limitPriceStr = this.roundPrice(limitPrice, assetInfo.szDecimals, isSpot);

    // Round size to proper lot size (szDecimals)
    const sizeStr = this.roundSize(size, assetInfo.szDecimals);

    console.log(`[Hyperliquid] Market order ${side} ${sizeStr} ${coin} at limit ${limitPriceStr} (mid: ${midPrice.toFixed(2)}, slippage: ${slippage * 100}%, szDecimals: ${assetInfo.szDecimals})`);

    // Construct order
    const order = {
      a: assetId,          // asset
      b: isBuy,            // is buy
      p: limitPriceStr,    // limit price with slippage (properly rounded)
      s: sizeStr,          // size (properly rounded to szDecimals)
      r: reduceOnly,       // reduce only
      t: {
        limit: {
          tif: 'Ioc'  // Immediate or Cancel - executes immediately at market price
        }
      }
    };

    if (options.cloid) {
      order.c = options.cloid;
    }

    const action = {
      type: 'order',
      orders: [order],
      grouping: 'na'
    };

    const nonce = Date.now();

    // Try WebSocket first if connected, otherwise use REST
    if (this.connected && !options.useRest) {
      return await this.createOrderWebSocket(action, nonce, options.vaultAddress);
    } else {
      return await this.createOrderRest(action, nonce, options.vaultAddress);
    }
  }

  /**
   * Create order via WebSocket
   */
  async createOrderWebSocket(action, nonce, vaultAddress = null) {
    if (!this.connected) {
      throw new Error('Not connected to WebSocket');
    }

    const signature = await this.signAction(action, nonce, vaultAddress);

    const payload = {
      action,
      nonce,
      signature
    };

    if (vaultAddress) {
      payload.vaultAddress = vaultAddress;
    }

    return new Promise((resolve, reject) => {
      const id = ++this.requestId;

      const request = {
        method: 'post',
        id,
        request: {
          type: 'action',
          payload
        }
      };

      // Store pending request with timeout
      const timeoutId = setTimeout(() => {
        if (this.pendingRequests.has(id)) {
          this.pendingRequests.delete(id);
          reject(new Error('Request timeout'));
        }
      }, 10000);

      this.pendingRequests.set(id, {
        resolve,
        reject,
        timeout: timeoutId
      });

      // Send request
      try {
        this.ws.send(JSON.stringify(request));
        console.log('[Hyperliquid] Order sent via WebSocket');
      } catch (error) {
        if (this.pendingRequests.has(id)) {
          clearTimeout(this.pendingRequests.get(id).timeout);
          this.pendingRequests.delete(id);
        }
        reject(error);
      }
    });
  }

  /**
   * Create order via REST API
   */
  async createOrderRest(action, nonce, vaultAddress = null) {
    const signature = await this.signAction(action, nonce, vaultAddress);

    const payload = {
      action,
      nonce,
      signature
    };

    if (vaultAddress) {
      payload.vaultAddress = vaultAddress;
    }

    try {
      const response = await fetch(this.exchangeUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }

      const result = await response.json();
      console.log('[Hyperliquid] Order placed via REST:', result);
      return result;
    } catch (error) {
      console.error('[Hyperliquid] REST order error:', error.message);
      throw error;
    }
  }

  /**
   * Close position (market order with reduce-only)
   *
   * @param {string} coin - Coin symbol
   * @param {string} side - 'buy' to close short, 'sell' to close long
   * @param {number} size - Position size to close
   * @returns {Promise<object>} Order result
   */
  async closePosition(coin, side, size) {
    return await this.createMarketOrder(coin, side, size, { reduceOnly: true });
  }

  /**
   * Get account balance and margin information
   * @param {string} user - User address (defaults to configured wallet)
   * @returns {Promise<object>} Balance information
   */
  async getBalance(user = null) {
    user = user || this.wallet;

    if (!user) {
      throw new Error('User address required to get balance');
    }

    try {
      const response = await fetch(this.restUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          type: 'clearinghouseState',
          user: user
        })
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();

      // Parse and return balance information
      const marginSummary = data.marginSummary || {};
      const crossMarginSummary = data.crossMarginSummary || {};

      return {
        // Total account value (equity)
        accountValue: parseFloat(marginSummary.accountValue || '0'),

        // Available to withdraw
        withdrawable: parseFloat(data.withdrawable || '0'),

        // Total margin used
        totalMarginUsed: parseFloat(marginSummary.totalMarginUsed || '0'),

        // Total raw USD balance
        totalRawUsd: parseFloat(marginSummary.totalRawUsd || '0'),

        // Total notional position value
        totalNtlPos: parseFloat(marginSummary.totalNtlPos || '0'),

        // Cross margin summary
        crossMargin: {
          accountValue: parseFloat(crossMarginSummary.accountValue || '0'),
          totalMarginUsed: parseFloat(crossMarginSummary.totalMarginUsed || '0'),
          totalRawUsd: parseFloat(crossMarginSummary.totalRawUsd || '0'),
          totalNtlPos: parseFloat(crossMarginSummary.totalNtlPos || '0')
        },

        // Available for trading (withdrawable + margin used)
        availableForTrading: parseFloat(data.withdrawable || '0') + parseFloat(marginSummary.totalMarginUsed || '0'),

        // Cross maintenance margin
        crossMaintenanceMarginUsed: parseFloat(data.crossMaintenanceMarginUsed || '0'),

        // Asset positions (for position management)
        assetPositions: data.assetPositions || [],

        // Timestamp
        timestamp: data.time || Date.now()
      };
    } catch (error) {
      console.error('[Hyperliquid] Error fetching balance:', error.message);
      throw error;
    }
  }
}

export default HyperliquidConnector;
