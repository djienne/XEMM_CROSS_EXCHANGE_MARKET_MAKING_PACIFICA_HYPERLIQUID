"""
Hyperliquid exchange connector implementing BaseExchangeConnector interface.

This connector wraps the official Hyperliquid Python SDK to provide a consistent
interface for the XEMM strategy. It implements all required abstract methods from
BaseExchangeConnector.
"""

import asyncio
import copy
import json
import time
import uuid
import websockets
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from functools import wraps
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import eth_account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from hyperliquid.utils.types import Cloid
from hyperliquid.utils.signing import sign_l1_action

from connectors.base_exchange import BaseExchangeConnector
from utils.enums import OrderSide, OrderStatus
from utils.exceptions import (
    ConnectionError,
    AuthenticationError,
    RateLimitError,
    InvalidOrderError,
    InsufficientBalanceError,
    InvalidSymbolError,
    MarketDataError
)
from utils.logger import setup_logger


HL_DEFAULT_MAKER_FEE = 0.00015  # 1.5 bps (0.015%)
HL_DEFAULT_TAKER_FEE = 0.000432  # 4.32 bps (0.0432%)


def rate_limited(max_retries=5, initial_delay=1, backoff_factor=2):
    """
    Rate limiting decorator with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        backoff_factor: Multiplier for delay on each retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Throttle before the call
            current_time = time.time()
            elapsed = current_time - self.last_call_time
            if elapsed < self.throttle_delay:
                sleep_time = self.throttle_delay - elapsed
                self.logger.debug(f"Throttling {func.__name__}: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)

            # Execute with retry logic
            retries = 0
            delay = initial_delay
            while retries < max_retries:
                try:
                    self.last_call_time = time.time()
                    return func(self, *args, **kwargs)
                except Exception as e:
                    error_msg = str(e).lower()
                    # Check for rate limit errors
                    if "429" in error_msg or "rate limit" in error_msg:
                        retries += 1
                        if retries >= max_retries:
                            self.logger.error(f"Rate limit on {func.__name__} after {max_retries} retries")
                            raise RateLimitError(f"Rate limit exceeded: {e}")
                        self.logger.warning(f"Rate limit on {func.__name__}. Retry {retries}/{max_retries} in {delay}s")
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        # Re-raise other errors immediately
                        raise
            return None
        return wrapper
    return decorator


class HyperliquidConnector(BaseExchangeConnector):
    """
    Hyperliquid exchange connector implementing BaseExchangeConnector interface.

    This connector uses the official Hyperliquid Python SDK to interact with
    the Hyperliquid exchange for market data, order execution, and position management.
    """

    def __init__(
        self,
        address: str,
        private_key: str = None,
        testnet: bool = False
    ):
        """
        Initialize Hyperliquid connector.

        Args:
            address: Ethereum wallet address (0x...)
            private_key: Private key for signing transactions (required for trading)
            testnet: Whether to use testnet (default: False)
        """
        super().__init__("hyperliquid")

        self.address = address
        self.private_key = private_key
        self.testnet = testnet

        # Rate limiting
        self.last_call_time = 0
        self.throttle_delay = 0.2  # 5 calls per second max

        # SDK instances
        self.info: Info = None
        self.exchange: Exchange = None
        self.account = None

        # Price cache (fed by websocket or external updates)
        self._price_cache: Dict[str, Dict[str, float]] = {}
        self._price_cache_lock: Lock = Lock()

        # Market data cache
        self.meta = None
        self.coin_to_meta: Dict[str, Any] = {}

        # Tick size cache (to avoid repeated L2 orderbook API calls)
        self._tick_size_cache: Dict[str, float] = {}
        self._tick_size_cache_lock: Lock = Lock()

        # WebSocket subscriptions
        self.price_subscriptions: Dict[str, Callable] = {}
        self.fill_subscription_id: Optional[str] = None
        self.fill_callback: Optional[Callable] = None

        # Position cache from userEvents WebSocket (shared with fill stream)
        self._position_cache: Dict[str, Dict[str, Any]] = {}
        self._position_cache_lock: Lock = Lock()

        # WebSocket trading (for fast order placement/cancellation)
        self._trading_ws = None
        self._trading_ws_lock = None  # Created lazily in the correct event loop
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._trading_ws_task = None
        self._next_request_id = 1

        # Trading configuration
        self.ws_market_order_timeout = 6.0  # seconds before falling back to REST

        # Fee schedule (loaded at startup via Info.user_fees)
        self._maker_fee_rate = HL_DEFAULT_MAKER_FEE
        self._taker_fee_rate = HL_DEFAULT_TAKER_FEE

        # Connection state
        self._connected = False

        # Initialize SDK
        self._initialize_sdk()

        self.logger.info(f"Hyperliquid connector initialized for {address}")

    def _normalize_cloid(self, cloid_value: Union[str, Cloid]) -> Cloid:
        """Ensure we pass a proper Cloid instance to the Hyperliquid SDK."""
        if isinstance(cloid_value, Cloid):
            return cloid_value

        if isinstance(cloid_value, str):
            normalized = cloid_value.strip()
            if normalized.startswith("0x") and len(normalized) == 34:
                return Cloid.from_str(normalized)
            raise InvalidOrderError(
                "Client order IDs must be 0x-prefixed 16-byte hex strings for Hyperliquid operations"
            )

        raise InvalidOrderError("Unsupported client order identifier type provided")

    def _get_reference_price(self, symbol: str, side: OrderSide) -> Dict[str, float]:
        """
        Obtain price inputs for order placement without coupling to retrieval logic.

        Preference order:
            1. Cached values from websocket ingestion
            2. REST fallback (legacy behaviour) with cache update
        """
        cached = self.get_cached_price(symbol)
        if cached:
            return cached

        self.logger.debug(f"No cached price for {symbol}. Requesting websocket snapshot.")
        price_data = self.get_bid_ask_ws(symbol, timeout=2.0)
        self.update_price_cache(symbol, price_data)
        return price_data

    def _initialize_sdk(self):
        """Initialize Hyperliquid SDK (Info and Exchange classes)"""
        try:
            # Determine API URL
            api_url = constants.TESTNET_API_URL if self.testnet else constants.MAINNET_API_URL

            # Initialize based on whether we have a private key
            if self.private_key:
                # Full access with trading capabilities
                self.account = eth_account.Account.from_key(self.private_key)
                self.exchange = Exchange(
                    self.account,
                    api_url,
                    account_address=self.address
                )
                # Create separate Info object with WebSocket support enabled
                # (Exchange's internal Info has WS disabled by default)
                self.info = Info(api_url, skip_ws=False)
                self.logger.info("Initialized with trading capabilities (private key provided)")
            else:
                # Read-only mode
                self.info = Info(api_url, skip_ws=True)
                self.logger.warning("Initialized in READ-ONLY mode (no private key)")

            # Fetch initial metadata
            self.logger.info("Fetching market metadata...")
            self.meta = self._fetch_meta()
            self.coin_to_meta = {asset["name"]: asset for asset in self.meta["universe"]}

            # Fetch fee schedule once at startup
            self._load_fee_schedule()

            self._connected = True
            self.logger.info(f"SDK initialized successfully. Loaded {len(self.coin_to_meta)} markets.")

        except Exception as e:
            self.logger.error(f"Failed to initialize Hyperliquid SDK: {e}")
            raise ConnectionError(f"Failed to initialize Hyperliquid: {e}")

    @rate_limited()
    def _fetch_meta(self) -> Dict[str, Any]:
        """Fetch market metadata from API"""
        return self.info.meta()

    @staticmethod
    def _parse_fee_schedule(
        payload: Dict[str, Any],
        fallback_maker: float,
        fallback_taker: float,
    ) -> Tuple[float, float]:
        """
        Extract maker/taker rates from Hyperliquid userFees response.

        Rates returned by the API are percentage values expressed as decimals
        (e.g. 0.00045 == 4.5 bps). Any missing/invalid values fall back to
        the provided defaults.
        """
        maker = fallback_maker
        taker = fallback_taker

        try:
            raw_maker = payload.get("userAddRate")
            if raw_maker not in (None, ""):
                maker = float(raw_maker)
        except (TypeError, ValueError):
            maker = fallback_maker

        try:
            raw_taker = payload.get("userCrossRate")
            if raw_taker not in (None, ""):
                taker = float(raw_taker)
        except (TypeError, ValueError):
            taker = fallback_taker

        return maker, taker

    def _load_fee_schedule(self) -> None:
        """Fetch and cache the current maker/taker fees for this wallet."""
        fallback_maker = HL_DEFAULT_MAKER_FEE
        fallback_taker = HL_DEFAULT_TAKER_FEE

        if not self.info or not self.address:
            self.logger.warning(
                "Skipping Hyperliquid fee fetch (info/address not initialized); "
                "using fallback maker=%.4fbps taker=%.4fbps",
                fallback_maker * 10000,
                fallback_taker * 10000,
            )
            self._maker_fee_rate = fallback_maker
            self._taker_fee_rate = fallback_taker
            return

        try:
            payload = self.info.user_fees(self.address)
            maker_fee, taker_fee = self._parse_fee_schedule(
                payload,
                fallback_maker,
                fallback_taker,
            )
            self._maker_fee_rate = maker_fee
            self._taker_fee_rate = taker_fee
            self.logger.info(
                "Loaded Hyperliquid fee schedule: maker=%.4fbps taker=%.4fbps",
                maker_fee * 10000,
                taker_fee * 10000,
            )
        except Exception as exc:
            self._maker_fee_rate = fallback_maker
            self._taker_fee_rate = fallback_taker
            self.logger.warning(
                "Failed to fetch Hyperliquid fee schedule (%s); using fallback maker=%.4fbps taker=%.4fbps",
                exc,
                fallback_maker * 10000,
                fallback_taker * 10000,
            )

    # ==================== CONNECTION METHODS ====================

    def is_connected(self) -> bool:
        """
        Check if WebSocket connections are healthy.

        Returns:
            True if connected to Hyperliquid API
        """
        if not self._connected:
            return False

        try:
            # Quick health check - try to fetch mid prices
            self.info.all_mids()
            return True
        except Exception as e:
            self.logger.warning(f"Connection check failed: {e}")
            return False

    def get_connection_health(self) -> Dict[str, Any]:
        """
        Get detailed health status of all WebSocket connections.

        Returns:
            Dictionary with health status:
            {
                "overall_healthy": bool,
                "sdk_connection": {"healthy": bool, "error": str or None},
                "trading_ws": {"healthy": bool, "connected": bool},
                "fill_stream": {"healthy": bool, "active": bool, "stale": bool, "age_seconds": float or None},
                "price_streams": {"count": int, "symbols": list},
                "timestamp": float
            }

        Usage:
            Call this periodically (e.g., every 30s) to monitor connection health.
            If overall_healthy is False, consider calling reconnect().
        """
        health = {
            "overall_healthy": True,
            "sdk_connection": {"healthy": False, "error": None},
            "trading_ws": {"healthy": False, "connected": False},
            "fill_stream": {"healthy": False, "active": False, "stale": False, "age_seconds": None},
            "price_streams": {"count": 0, "symbols": []},
            "timestamp": time.time()
        }

        # Check SDK connection (used for price/fill streams)
        try:
            self.info.all_mids()
            health["sdk_connection"]["healthy"] = True
        except Exception as e:
            health["sdk_connection"]["error"] = str(e)
            health["overall_healthy"] = False

        # Check trading WebSocket
        if self._trading_ws_is_open():
            health["trading_ws"]["healthy"] = True
            health["trading_ws"]["connected"] = True
        else:
            # If we have trading capabilities but WS not connected, mark unhealthy
            if self.exchange and self.account:
                health["overall_healthy"] = False

        # Check fill stream health
        if self.fill_subscription_id:
            health["fill_stream"]["active"] = True

            # Check if position cache is stale (no updates in 90+ seconds = problem)
            if self._position_cache:
                # Find most recent position update
                max_age = 0
                for symbol, pos_data in self._position_cache.items():
                    age = time.time() - pos_data.get("timestamp", 0)
                    max_age = max(max_age, age)

                health["fill_stream"]["age_seconds"] = max_age
                health["fill_stream"]["stale"] = (max_age > 90)  # 90s threshold

                if max_age > 90:
                    health["fill_stream"]["healthy"] = False
                    health["overall_healthy"] = False
                    self.logger.warning(
                        f"[HEALTH] Fill stream may be stale - no position updates for {max_age:.1f}s"
                    )
                else:
                    health["fill_stream"]["healthy"] = True
            else:
                # No position cache data yet - might be OK if no positions
                health["fill_stream"]["healthy"] = True

        # Check active price streams
        health["price_streams"]["count"] = len(self.price_subscriptions)
        health["price_streams"]["symbols"] = [
            info["symbol"] for _, info in self.price_subscriptions.items()
        ]

        return health

    def reconnect(self, max_retries: int = 3, initial_delay: float = 1.0) -> bool:
        """
        Attempt to reconnect WebSocket connections with automatic retry.

        Args:
            max_retries: Maximum number of reconnection attempts
            initial_delay: Initial delay between retries (seconds), doubles on each retry

        Returns:
            True if reconnection successful

        Implementation Notes:
            - Preserves all active subscriptions (price streams, fill stream)
            - Automatically resubscribes to all streams after reconnection
            - Uses exponential backoff for retries
            - Critical for maintaining uninterrupted trading
        """
        self.logger.warning("=" * 60)
        self.logger.warning("CONNECTION LOST - Attempting to reconnect...")
        self.logger.warning("=" * 60)

        # Store active subscriptions before reconnecting
        active_price_subs = list(self.price_subscriptions.items())
        was_fill_stream_active = (self.fill_subscription_id is not None)
        fill_callback_backup = self.fill_callback

        delay = initial_delay
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(f"Reconnection attempt {attempt}/{max_retries}...")
                self._connected = False

                # Clear old subscriptions (they're invalid now)
                self.price_subscriptions = {}
                self.fill_subscription_id = None
                self.fill_callback = None

                # Re-initialize SDK
                self._initialize_sdk()

                # Resubscribe to price streams
                if active_price_subs:
                    self.logger.info(f"Resubscribing to {len(active_price_subs)} price streams...")
                    resubscribed_count = 0
                    failed_count = 0

                    for old_sub_id, sub_info in active_price_subs:
                        try:
                            symbol = sub_info["symbol"]
                            callback = sub_info["callback"]

                            # Resubscribe
                            new_sub_id = self.start_price_stream(symbol, callback)
                            resubscribed_count += 1
                            self.logger.info(f"  [OK] Resubscribed price stream for {symbol}")

                        except Exception as e:
                            failed_count += 1
                            self.logger.error(f"  [FAILED] Failed to resubscribe price stream for {symbol}: {e}")

                    self.logger.info(
                        f"Price stream resubscription: {resubscribed_count} succeeded, {failed_count} failed"
                    )

                # Resubscribe to fill stream
                if was_fill_stream_active and fill_callback_backup:
                    try:
                        self.logger.info("Resubscribing to fill stream...")
                        self.start_fill_stream(fill_callback_backup)
                        self.logger.info("  [OK] Fill stream resubscribed successfully")
                    except Exception as e:
                        self.logger.error(f"  [FAILED] Failed to resubscribe fill stream: {e}")
                        # This is CRITICAL - if fill stream fails, we can't hedge properly
                        raise ConnectionError(f"CRITICAL: Failed to resubscribe fill stream: {e}")

                self.logger.warning("=" * 60)
                self.logger.warning("RECONNECTION SUCCESSFUL")
                self.logger.warning("=" * 60)
                return True

            except Exception as e:
                self.logger.error(f"Reconnection attempt {attempt}/{max_retries} failed: {e}")

                if attempt < max_retries:
                    self.logger.info(f"Retrying in {delay:.1f}s...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    self.logger.error("=" * 60)
                    self.logger.error("RECONNECTION FAILED - All attempts exhausted")
                    self.logger.error("=" * 60)
                    return False

        return False

    def close(self):
        """
        Close all connections and cleanup resources.

        This should be called when shutting down to properly release resources.
        """
        try:
            self.logger.info("Closing Hyperliquid connector...")

            # Stop all active price subscriptions
            if self.price_subscriptions:
                subscription_ids = list(self.price_subscriptions.keys())
                for sub_id in subscription_ids:
                    try:
                        self.stop_price_stream(sub_id)
                    except Exception:
                        # Silently ignore - cleanup is best effort
                        pass

            # Stop fill stream if active
            if self.fill_subscription_id:
                try:
                    self.stop_fill_stream(self.fill_subscription_id)
                except Exception:
                    # Silently ignore - cleanup is best effort
                    pass

            # Close trading WebSocket if active
            if self._trading_ws_task:
                try:
                    # Just cancel the task - let the event loop handle cleanup
                    # Don't try to await or close WebSocket directly as event loop may be closing
                    self._trading_ws_task.cancel()
                    self._trading_ws_task = None
                    self._trading_ws = None
                except Exception:
                    # Silently ignore - cleanup is best effort
                    pass

            # Disconnect WebSocket connection in Info object
            if self.info:
                try:
                    self.info.disconnect_websocket()
                    self.logger.info("WebSocket disconnected")
                except Exception:
                    # Silently ignore - cleanup is best effort
                    pass

            # Mark as disconnected
            self._connected = False

            self.logger.info("Hyperliquid connector closed successfully")

        except Exception as e:
            self.logger.error(f"Error closing Hyperliquid connector: {e}")

    # ==================== WEBSOCKET TRADING METHODS ====================

    def _trading_ws_is_open(self) -> bool:
        """
        Check if trading WebSocket connection is open.

        Returns:
            True if connection is open and usable
        """
        if self._trading_ws is None:
            return False

        try:
            # Check if connection has a close code (means it's closed)
            return self._trading_ws.close_code is None
        except:
            return False

    async def prewarm_trading_ws(self):
        """
        Pre-initialize the trading WebSocket connection to minimize latency on first order.

        CRITICAL FOR XEMM: Call this method during bot startup (in async context) to eliminate
        the ~500-1500ms connection overhead on the first hedge order. The connection is
        maintained by a background auto-reconnect loop.

        Usage in main bot:
            connector = HyperliquidConnector(...)
            await connector.prewarm_trading_ws()  # Call in async runtime

        Returns:
            None - connection will be ready or establishing in background

        Note:
            - Must be called from within an async runtime (not during __init__)
            - Non-blocking - returns after connection attempt
            - Safe to call multiple times (idempotent)
        """
        self.logger.info("[TRADING WS] Pre-warming trading WebSocket connection...")

        # Start the background reconnection task if not already running
        if self._trading_ws_task is None or self._trading_ws_task.done():
            loop = asyncio.get_event_loop()
            self._trading_ws_task = loop.create_task(self._trading_ws_reconnect_loop())

        # Wait for initial connection (up to 2 seconds)
        for _ in range(20):
            await asyncio.sleep(0.1)
            if self._trading_ws_is_open():
                self.logger.info("[TRADING WS] Pre-warm successful - connection ready for low-latency trading")
                return

        # Connection not ready, but background task will keep trying
        self.logger.warning("[TRADING WS] Pre-warm incomplete - connection will be established in background")

    async def _ensure_trading_ws(self):
        """
        Ensure trading WebSocket connection is active.
        Creates connection if not exists or reconnects if closed.

        Note: This now uses a persistent background task with auto-reconnect.
        The first call will start the background reconnection task if not already running.
        """
        # Start background reconnection task if not running
        # No lock needed here - just check and start the background task
        if self._trading_ws_task is None or self._trading_ws_task.done():
            loop = asyncio.get_event_loop()
            self._trading_ws_task = loop.create_task(self._trading_ws_reconnect_loop())
            self.logger.info("[TRADING WS] Started auto-reconnect background task")

            # Wait briefly for first connection
            await asyncio.sleep(0.5)

        # Check if connection is open
        if not self._trading_ws_is_open():
            # Wait a bit for the reconnection loop to establish connection
            for _ in range(10):
                await asyncio.sleep(0.1)
                if self._trading_ws_is_open():
                    break
            else:
                self.logger.warning("[TRADING WS] Connection not ready after waiting")

    async def _trading_ws_reconnect_loop(self):
        """
        Background task that maintains trading WebSocket connection with auto-reconnect.

        This runs continuously and handles:
        - Initial connection
        - Auto-reconnect with exponential backoff on disconnection
        - Ping/pong heartbeat for connection health
        - Graceful handling of pending requests during reconnection
        """
        # Create lock lazily in the correct event loop
        if self._trading_ws_lock is None:
            self._trading_ws_lock = asyncio.Lock()

        reconnect_delay = 1.0
        max_reconnect_delay = 30.0

        while True:
            try:
                # Determine WebSocket URL
                ws_url = constants.TESTNET_API_URL.replace("https", "wss") + "/ws" if self.testnet else constants.MAINNET_API_URL.replace("https", "wss") + "/ws"

                self.logger.info(f"[TRADING WS] Establishing trading WebSocket connection to {ws_url}...")

                # Connect with ping/pong heartbeat (ping every 30s, timeout after 10s)
                async with websockets.connect(
                    ws_url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5
                ) as ws:
                    async with self._trading_ws_lock:
                        self._trading_ws = ws

                    self.logger.info("[TRADING WS] Trading WebSocket connected successfully")

                    # Reset reconnection delay on successful connection
                    reconnect_delay = 1.0

                    # Handle messages
                    await self._handle_trading_ws_messages(ws)

            except websockets.exceptions.ConnectionClosed as e:
                self.logger.warning(f"[TRADING WS] Connection closed: {e}. Reconnecting in {reconnect_delay:.1f}s...")
            except Exception as e:
                self.logger.error(f"[TRADING WS] Connection error: {e}. Reconnecting in {reconnect_delay:.1f}s...")

            # Clear the WebSocket reference
            async with self._trading_ws_lock:
                self._trading_ws = None

            # Fail all pending requests with connection error
            pending_count = len(self._pending_requests)
            if pending_count > 0:
                self.logger.warning(f"[TRADING WS] Failing {pending_count} pending requests due to reconnection")
                for request_id, future in list(self._pending_requests.items()):
                    if not future.done():
                        future.set_exception(ConnectionError("Trading WebSocket reconnecting"))
                self._pending_requests.clear()

            # Wait before reconnecting (exponential backoff)
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    async def _handle_trading_ws_messages(self, ws):
        """
        Handle incoming WebSocket messages for trading operations.

        Args:
            ws: Active websocket connection
        """
        try:
            while True:
                msg = await ws.recv()
                data = json.loads(msg)

                self.logger.debug(f"[TRADING WS] Received: {data}")

                # Check if this is a response to a pending request
                if data.get("channel") == "post" and "data" in data:
                    request_id = data["data"].get("id")
                    if request_id and request_id in self._pending_requests:
                        future = self._pending_requests.pop(request_id)
                        if not future.done():
                            future.set_result(data["data"])

        except websockets.exceptions.ConnectionClosed:
            self.logger.warning("[TRADING WS] Connection closed in message handler")
            raise  # Re-raise to trigger reconnect in outer loop
        except Exception as e:
            self.logger.error(f"[TRADING WS] Error handling message: {e}")
            raise  # Re-raise to trigger reconnect

    async def initialize_trading_websocket(self):
        """
        Pre-establish the trading WebSocket connection.

        Call this method after creating the connector to avoid connection overhead
        during the first trade. This is especially important for latency-sensitive
        operations like immediate hedging.

        Usage:
            connector = HyperliquidConnector(address, private_key)
            await connector.initialize_trading_websocket()  # Pre-connect
            # Now first market order will be fast (no connection overhead)
        """
        await self._ensure_trading_ws()
        self.logger.info("[TRADING WS] Pre-established trading WebSocket connection")

    async def _send_trading_request(self, request: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
        """
        Send a trading request via WebSocket and wait for response.

        Args:
            request: Request payload with 'method', 'id', and 'request'
            timeout: Response timeout in seconds

        Returns:
            Response data dictionary

        Raises:
            ConnectionError: WebSocket connection failed
            TimeoutError: Response not received within timeout
        """
        await self._ensure_trading_ws()

        request_id = request["id"]
        future = asyncio.Future()
        self._pending_requests[request_id] = future

        try:
            # Send request
            await self._trading_ws.send(json.dumps(request))
            self.logger.debug(f"[TRADING WS] Sent request: {request_id}")

            # Wait for response with timeout
            response = await asyncio.wait_for(future, timeout=timeout)

            return response

        except asyncio.TimeoutError:
            self._pending_requests.pop(request_id, None)
            raise TimeoutError(f"Trading request {request_id} timed out after {timeout}s")
        except Exception as e:
            self._pending_requests.pop(request_id, None)
            raise

    async def place_limit_order_ws_async(self, symbol: str, side: OrderSide, quantity: float, price: float,
                                         reduce_only: bool = False, post_only: bool = True) -> Dict[str, Any]:
        """
        Place a limit order via WebSocket (async version).

        Args:
            symbol: Trading symbol (e.g., "BTC")
            side: OrderSide.BUY or OrderSide.SELL
            quantity: Order quantity (will be rounded to lot_size)
            price: Limit price (will be rounded to tick_size)
            reduce_only: If True, order can only reduce position
            post_only: If True, order will be cancelled if it would take liquidity (ALO)

        Returns:
            Same format as place_limit_order()

        Raises:
            AuthenticationError: No private key configured
            InvalidOrderError: Order parameters invalid
            InsufficientBalanceError: Not enough balance
        """
        if not self.exchange or not self.account:
            raise AuthenticationError("Cannot place orders in read-only mode (no private key)")

        try:
            # Round price and quantity to valid values
            rounded_price = self.round_price(price, symbol, side)
            rounded_quantity = self.round_quantity(quantity, symbol)

            # Validate minimum notional
            specs = self.get_market_specs(symbol)
            notional = rounded_price * rounded_quantity
            if notional < specs["min_notional"]:
                raise InvalidOrderError(
                    f"Order notional ${notional:.2f} below minimum ${specs['min_notional']}"
                )

            # Generate unique client order ID
            client_order_id = f"0x{uuid.uuid4().hex}"

            # Get asset index (position in universe array)
            asset_index = None
            for idx, asset in enumerate(self.meta["universe"]):
                if asset["name"] == symbol:
                    asset_index = idx
                    break

            if asset_index is None:
                raise InvalidSymbolError(f"Symbol {symbol} not found in universe")

            # Build order action
            order_type_data = {"limit": {"tif": "Alo"}} if post_only else {"limit": {"tif": "Gtc"}}

            order = {
                "a": asset_index,
                "b": (side == OrderSide.BUY),
                "p": str(rounded_price),
                "s": str(rounded_quantity),
                "r": reduce_only,
                "t": order_type_data,
                "c": client_order_id
            }

            action = {
                "type": "order",
                "orders": [order],
                "grouping": "na"
            }

            # Sign the action
            nonce = int(time.time() * 1000)
            signature = sign_l1_action(
                self.account,          # wallet
                action,                # action
                None,                  # active_pool (vaultAddress)
                nonce,                 # nonce
                None,                  # expires_after (no expiration)
                not self.testnet       # is_mainnet (inverse of testnet)
            )

            # Build WebSocket request
            request_id = self._next_request_id
            self._next_request_id += 1

            ws_request = {
                "method": "post",
                "id": request_id,
                "request": {
                    "type": "action",
                    "payload": {
                        "action": action,
                        "nonce": nonce,
                        "signature": signature,
                        "vaultAddress": None
                    }
                }
            }

            # Send request and measure latency
            start_time = time.time()
            self.logger.info(f"[WS] Placing limit order: {side} {rounded_quantity} {symbol} @ {rounded_price}")

            response = await self._send_trading_request(ws_request, timeout=5.0)
            latency_ms = (time.time() - start_time) * 1000

            # Parse response
            if response.get("response", {}).get("type") == "action":
                payload = response["response"]["payload"]
                status_info = payload.get("status")

                if status_info == "ok":
                    response_data = payload.get("response", {}).get("data", {})
                    statuses = response_data.get("statuses", [])
                    status_entry = statuses[0] if statuses else {}
                    error_msg = status_entry.get("error")

                    if error_msg:
                        self.logger.error(f"[WS] Limit order rejected: {error_msg}")
                        if "insufficient" in error_msg.lower() or "balance" in error_msg.lower() or "margin" in error_msg.lower():
                            raise InsufficientBalanceError(f"Insufficient balance: {error_msg}")
                        return {
                            "success": False,
                            "order_id": None,
                            "client_order_id": client_order_id,
                            "symbol": symbol,
                            "side": str(side),
                            "quantity": rounded_quantity,
                            "price": rounded_price,
                            "status": "rejected",
                            "timestamp": time.time(),
                            "latency_ms": latency_ms,
                            "error": error_msg
                        }

                    # Extract order ID
                    if "resting" in status_entry:
                        order_id = str(status_entry["resting"].get("oid", "unknown"))
                    else:
                        order_id = "unknown"

                    result = {
                        "success": True,
                        "order_id": order_id,
                        "client_order_id": client_order_id,
                        "symbol": symbol,
                        "side": str(side),
                        "quantity": rounded_quantity,
                        "price": rounded_price,
                        "status": "open",
                        "timestamp": time.time(),
                        "latency_ms": latency_ms,
                        "error": None
                    }
                    self.logger.info(f"[WS] Limit order placed: {order_id} ({latency_ms:.1f}ms)")
                    return result

                # Non-OK status
                error_msg = str(status_info)
                self.logger.error(f"[WS] Limit order rejected: {error_msg}")
                if "insufficient" in error_msg.lower() or "balance" in error_msg.lower() or "margin" in error_msg.lower():
                    raise InsufficientBalanceError(f"Insufficient balance: {error_msg}")
                return {
                    "success": False,
                    "order_id": None,
                    "client_order_id": client_order_id,
                    "symbol": symbol,
                    "side": str(side),
                    "quantity": rounded_quantity,
                    "price": rounded_price,
                    "status": "rejected",
                    "timestamp": time.time(),
                    "latency_ms": latency_ms,
                    "error": error_msg
                }

            elif response.get("response", {}).get("type") == "error":
                error_msg = response["response"].get("payload", "Unknown error")
                self.logger.error(f"[WS] Limit order error: {error_msg}")
                raise InvalidOrderError(f"WebSocket order error: {error_msg}")

            raise InvalidOrderError(f"Unexpected WebSocket response: {response}")

        except (AuthenticationError, InvalidOrderError, InsufficientBalanceError):
            raise
        except Exception as e:
            self.logger.error(f"[WS] Error placing limit order: {e}")
            raise InvalidOrderError(f"Failed to place limit order via WebSocket: {e}")

    async def cancel_order_ws_async(self, symbol: str, order_id: str = None, client_order_id: str = None) -> Dict[str, Any]:
        """
        Cancel a specific order via WebSocket (async version).

        Args:
            symbol: Trading symbol (e.g., "BTC")
            order_id: Exchange order ID (oid)
            client_order_id: Client order ID (cloid) - alternative to order_id

        Returns:
            Same format as cancel_order()

        Raises:
            AuthenticationError: No private key configured
            InvalidOrderError: Neither order_id nor client_order_id provided
        """
        if not self.exchange or not self.account:
            raise AuthenticationError("Cannot cancel orders in read-only mode (no private key)")

        if not order_id and not client_order_id:
            raise InvalidOrderError("Must provide either order_id or client_order_id")

        identifier = f"order_id={order_id}" if order_id else f"client_order_id={client_order_id}"
        target_reference = order_id or client_order_id

        try:
            # Get asset index (position in universe array)
            asset_index = None
            for idx, asset in enumerate(self.meta["universe"]):
                if asset["name"] == symbol:
                    asset_index = idx
                    break

            if asset_index is None:
                raise InvalidSymbolError(f"Symbol {symbol} not found in universe")

            # Build cancel action  - only include the field that's provided
            cancel_request = {"a": asset_index}
            if order_id:
                cancel_request["o"] = int(order_id)
            else:
                cancel_request["c"] = client_order_id

            action = {
                "type": "cancel",
                "cancels": [cancel_request]
            }

            # Sign the action
            nonce = int(time.time() * 1000)
            signature = sign_l1_action(
                self.account,          # wallet
                action,                # action
                None,                  # active_pool (vaultAddress)
                nonce,                 # nonce
                None,                  # expires_after (no expiration)
                not self.testnet       # is_mainnet (inverse of testnet)
            )

            # Build WebSocket request
            request_id = self._next_request_id
            self._next_request_id += 1

            ws_request = {
                "method": "post",
                "id": request_id,
                "request": {
                    "type": "action",
                    "payload": {
                        "action": action,
                        "nonce": nonce,
                        "signature": signature,
                        "vaultAddress": None
                    }
                }
            }

            # Send request
            self.logger.info(f"[WS] Cancelling order: {symbol} {identifier}")

            response = await self._send_trading_request(ws_request, timeout=5.0)

            # Parse response
            if response.get("response", {}).get("type") == "action":
                payload = response["response"]["payload"]
                status_info = payload.get("status")

                if status_info == "ok":
                    result = {
                        "success": True,
                        "order_id": target_reference,
                        "status": "cancelled",
                        "error": None
                    }
                    self.logger.info(f"[WS] Order cancelled: {identifier}")
                    return result
                else:
                    error_msg = str(status_info)
                    self.logger.warning(f"[WS] Order cancellation failed: {error_msg}")
                    return {
                        "success": False,
                        "order_id": target_reference,
                        "status": "failed",
                        "error": error_msg
                    }

            elif response.get("response", {}).get("type") == "error":
                error_msg = response["response"].get("payload", "Unknown error")
                self.logger.warning(f"[WS] Order cancellation error: {error_msg}")
                return {
                    "success": False,
                    "order_id": target_reference,
                    "status": "failed",
                    "error": error_msg
                }

            raise InvalidOrderError(f"Unexpected WebSocket response: {response}")

        except AuthenticationError:
            raise
        except InvalidOrderError:
            raise
        except Exception as e:
            self.logger.error(f"[WS] Error cancelling order {identifier}: {e}")
            return {
                "success": False,
                "order_id": target_reference,
                "status": "failed",
                "error": str(e)
            }


    # ==================== PLACEHOLDER METHODS (To be implemented in next tasks) ====================

    def get_bid_ask_ws(self, symbol: str, timeout: float = 10.0) -> Dict[str, float]:
        """
        Get current bid/ask prices via WebSocket (primary method - fastest).

        Args:
            symbol: Trading symbol (e.g., "BTC")
            timeout: Timeout in seconds

        Returns:
            {
                "bid": float,
                "ask": float,
                "mid": float,
                "timestamp": float
            }

        Raises:
            ConnectionError: WebSocket connection failed
            TimeoutError: No data received within timeout

        Notes:
            This method subscribes to L2 orderbook, waits for one update,
            then unsubscribes. For continuous streaming, use start_price_stream().
        """
        import threading

        result = {"data": None, "error": None}
        event = threading.Event()

        def callback(data):
            """Callback to capture orderbook data"""
            try:
                if data and "data" in data and "levels" in data["data"]:
                    levels = data["data"]["levels"]
                    if levels[0] and levels[1]:
                        best_bid = float(levels[0][0]["px"])
                        best_ask = float(levels[1][0]["px"])
                        mid = (best_bid + best_ask) / 2

                        result["data"] = {
                            "bid": best_bid,
                            "ask": best_ask,
                            "mid": mid,
                            "timestamp": time.time()
                        }
                        event.set()
            except Exception as e:
                result["error"] = e
                event.set()

        # Subscribe to L2 book
        subscription_id = None
        try:
            subscription_id = self.info.subscribe(
                {"type": "l2Book", "coin": symbol},
                callback
            )

            # Wait for data with timeout
            if not event.wait(timeout):
                raise TimeoutError(f"No data received for {symbol} within {timeout}s")

            # Check for errors
            if result["error"]:
                raise result["error"]

            if not result["data"]:
                raise MarketDataError(f"Invalid data received for {symbol}")

            return result["data"]

        except Exception as e:
            self.logger.error(f"WebSocket error for {symbol}: {e}")
            raise ConnectionError(f"WebSocket failed: {e}")

        finally:
            # Unsubscribe
            if subscription_id:
                try:
                    self.info.unsubscribe(subscription_id)
                except:
                    pass

    @rate_limited()
    def get_bid_ask_rest(self, symbol: str) -> Dict[str, float]:
        """
        Get current bid/ask prices via REST API (fallback method).

        Args:
            symbol: Trading symbol (e.g., "BTC")

        Returns:
            {
                "bid": float,
                "ask": float,
                "mid": float,
                "timestamp": float
            }

        Raises:
            InvalidSymbolError: If symbol not found
            MarketDataError: If unable to fetch orderbook
        """
        try:
            # Get L2 orderbook snapshot from REST API
            orderbook = self.info.l2_snapshot(symbol)

            if not orderbook or "levels" not in orderbook:
                raise MarketDataError(f"No orderbook data for {symbol}")

            levels = orderbook["levels"]

            # levels[0] = bids (sorted descending), levels[1] = asks (sorted ascending)
            if not levels[0] or not levels[1]:
                raise MarketDataError(f"Empty orderbook for {symbol}")

            # Best bid is first in bids array, best ask is first in asks array
            best_bid = float(levels[0][0]["px"])
            best_ask = float(levels[1][0]["px"])
            mid = (best_bid + best_ask) / 2

            return {
                "bid": best_bid,
                "ask": best_ask,
                "mid": mid,
                "timestamp": time.time()
            }

        except KeyError as e:
            self.logger.error(f"Symbol {symbol} not found in metadata")
            raise InvalidSymbolError(f"Symbol not found: {symbol}")
        except Exception as e:
            self.logger.error(f"Error fetching bid/ask for {symbol}: {e}")
            raise MarketDataError(f"Failed to fetch bid/ask: {e}")

    def update_price_cache(self, symbol: str, price_data: Dict[str, float]) -> None:
        """Store the latest price data for a symbol."""
        with self._price_cache_lock:
            self._price_cache[symbol] = {
                "bid": float(price_data["bid"]),
                "ask": float(price_data["ask"]),
                "mid": float(price_data["mid"]),
                "timestamp": float(price_data["timestamp"])
            }

    def get_cached_price(self, symbol: str) -> Optional[Dict[str, float]]:
        """Retrieve the latest cached price data, if available."""
        with self._price_cache_lock:
            cached = self._price_cache.get(symbol)
            return copy.deepcopy(cached) if cached else None

    def start_price_stream(self, symbol: str, callback: Callable) -> str:
        """
        Start continuous WebSocket price updates.

        Args:
            symbol: Trading symbol (e.g., "BTC")
            callback: Function called on each price update
                     Signature: callback(price_data: Dict[str, float])
                     price_data format: {"bid": float, "ask": float, "mid": float, "timestamp": float}

        Returns:
            subscription_id: String ID for later unsubscription

        Implementation Notes:
            - Maintains persistent WebSocket connection
            - Auto-reconnect handled by Hyperliquid SDK
            - Callback is called on each orderbook update
            - Errors are logged but don't crash the stream
        """
        def wrapped_callback(data):
            """Wrapper to parse orderbook data and call user callback"""
            try:
                if data and "data" in data and "levels" in data["data"]:
                    levels = data["data"]["levels"]
                    if levels[0] and levels[1]:
                        best_bid = float(levels[0][0]["px"])
                        best_ask = float(levels[1][0]["px"])
                        mid = (best_bid + best_ask) / 2

                        price_data = {
                            "bid": best_bid,
                            "ask": best_ask,
                            "mid": mid,
                            "timestamp": time.time()
                        }

                        # Update local cache
                        self.update_price_cache(symbol, price_data)

                        # Call user callback
                        callback(price_data)

            except Exception as e:
                self.logger.error(f"Error in price stream callback for {symbol}: {e}")

        # Subscribe to L2 book
        try:
            subscription_id = self.info.subscribe(
                {"type": "l2Book", "coin": symbol},
                wrapped_callback
            )

            # Store subscription
            self.price_subscriptions[subscription_id] = {
                "symbol": symbol,
                "callback": callback
            }

            self.logger.info(f"Started price stream for {symbol} (subscription: {subscription_id})")
            return subscription_id

        except Exception as e:
            self.logger.error(f"Failed to start price stream for {symbol}: {e}")
            raise ConnectionError(f"Failed to start price stream: {e}")

    def stop_price_stream(self, subscription_id: str) -> bool:
        """
        Stop price stream subscription.

        Args:
            subscription_id: ID returned by start_price_stream()

        Returns:
            True if successfully unsubscribed

        Notes:
            If subscription_id not found, returns False but doesn't raise error.
            Gracefully handles event loop closure and SDK errors.
        """
        try:
            if subscription_id not in self.price_subscriptions:
                # Silently return False - subscription not found
                return False

            # Try to unsubscribe from WebSocket (may fail if event loop closed)
            try:
                if self.info:
                    self.info.unsubscribe(subscription_id)
            except Exception:
                # Silently ignore unsubscribe errors during cleanup
                pass

            # Remove from tracking
            self.price_subscriptions.pop(subscription_id, None)

            return True

        except Exception:
            # Silently handle all errors during cleanup
            # Remove from tracking anyway to prevent leaks
            self.price_subscriptions.pop(subscription_id, None)
            return False

    @rate_limited()
    async def place_limit_order(self, symbol: str, side: OrderSide, quantity: float, price: float,
                               reduce_only: bool = False, post_only: bool = True) -> Dict[str, Any]:
        """
        Place a limit order (maker order) via WebSocket (with REST fallback).

        Args:
            symbol: Trading symbol (e.g., "BTC")
            side: OrderSide.BUY or OrderSide.SELL
            quantity: Order quantity (will be rounded to lot_size)
            price: Limit price (will be rounded to tick_size)
            reduce_only: If True, order can only reduce position
            post_only: If True, order will be cancelled if it would take liquidity (ALO)

        Returns:
            {
                "success": bool,
                "order_id": str,              # Exchange order ID
                "client_order_id": str,       # Our generated client ID
                "symbol": str,
                "side": str,
                "quantity": float,
                "price": float,
                "status": str,                # "open", "rejected", etc.
                "timestamp": float,
                "error": Optional[str]
            }

        Raises:
            AuthenticationError: No private key configured
            InvalidOrderError: Order parameters invalid
            InsufficientBalanceError: Not enough balance
        """
        if not self.exchange:
            raise AuthenticationError("Cannot place orders in read-only mode (no private key)")

        # Try WebSocket first
        try:
            result = await self.place_limit_order_ws_async(symbol, side, quantity, price, reduce_only, post_only)
            return result

        except Exception as ws_error:
            # WebSocket failed, fall back to REST
            self.logger.warning(f"WebSocket order placement failed, falling back to REST: {ws_error}")

        # REST fallback
        try:
            # Round price and quantity to valid values
            rounded_price = self.round_price(price, symbol, side)
            rounded_quantity = self.round_quantity(quantity, symbol)

            # Validate minimum notional
            specs = self.get_market_specs(symbol)
            notional = rounded_price * rounded_quantity
            if notional < specs["min_notional"]:
                raise InvalidOrderError(
                    f"Order notional ${notional:.2f} below minimum ${specs['min_notional']}"
                )

            # Generate unique client order ID
            client_order_id = f"0x{uuid.uuid4().hex}"
            client_order_cloid = Cloid.from_str(client_order_id)

            # Place order via REST
            start_time = time.time()
            self.logger.info(f"[REST] Placing limit order: {side} {rounded_quantity} {symbol} @ {rounded_price}")

            order_type = {"limit": {"tif": "Alo"}} if post_only else {"limit": {"tif": "Gtc"}}
            response = self.exchange.order(
                name=symbol,
                is_buy=(side == OrderSide.BUY),
                sz=float(rounded_quantity),
                limit_px=float(rounded_price),
                order_type=order_type,
                reduce_only=reduce_only,
                cloid=client_order_cloid
            )

            latency_ms = (time.time() - start_time) * 1000

            # Parse response
            if response and "status" in response:
                self.logger.info(f"Raw order response: {response}")
                status_info = response["status"]
                statuses = response.get("response", {}).get("data", {}).get("statuses", [])
                status_entry = statuses[0] if statuses else {}
                error_msg = status_entry.get("error")

                if status_info == "ok" and error_msg:
                    self.logger.error(f"[REST] Limit order rejected: {error_msg}")
                    if "insufficient" in error_msg.lower() or "balance" in error_msg.lower() or "margin" in error_msg.lower():
                        raise InsufficientBalanceError(f"Insufficient balance: {error_msg}")
                    return {
                        "success": False,
                        "order_id": None,
                        "client_order_id": client_order_id,
                        "symbol": symbol,
                        "side": str(side),
                        "quantity": rounded_quantity,
                        "price": rounded_price,
                        "status": "rejected",
                        "timestamp": time.time(),
                        "latency_ms": latency_ms,
                        "error": error_msg
                    }

                if status_info == "ok":
                    # Extract order ID from nested structure
                    if "resting" in status_entry:
                        order_id = str(status_entry["resting"].get("oid", "unknown"))
                    else:
                        order_id = "unknown"

                    result = {
                        "success": True,
                        "order_id": order_id,
                        "client_order_id": client_order_id,
                        "symbol": symbol,
                        "side": str(side),
                        "quantity": rounded_quantity,
                        "price": rounded_price,
                        "status": "open",
                        "timestamp": time.time(),
                        "latency_ms": latency_ms,
                        "error": None
                    }
                    self.logger.info(f"[REST] Limit order placed: {order_id} ({latency_ms:.1f}ms)")
                    return result

                # Non-OK status
                error_msg = str(status_info)
                self.logger.error(f"[REST] Limit order rejected: {error_msg}")
                if "insufficient" in error_msg.lower() or "balance" in error_msg.lower() or "margin" in error_msg.lower():
                    raise InsufficientBalanceError(f"Insufficient balance: {error_msg}")
                return {
                    "success": False,
                    "order_id": None,
                    "client_order_id": client_order_id,
                    "symbol": symbol,
                    "side": str(side),
                    "quantity": rounded_quantity,
                    "price": rounded_price,
                    "status": "rejected",
                    "timestamp": time.time(),
                    "latency_ms": latency_ms,
                    "error": error_msg
                }
            else:
                raise InvalidOrderError(f"Unexpected API response: {response}")

        except (AuthenticationError, InvalidOrderError, InsufficientBalanceError):
            raise
        except Exception as e:
            self.logger.error(f"Error placing limit order: {e}")
            raise InvalidOrderError(f"Failed to place limit order: {e}")

    async def place_market_order_ws_async(self, symbol: str, side: OrderSide, quantity: float,
                                          reduce_only: bool = False, slippage_bps: int = 50) -> Dict[str, Any]:
        """
        Place a market order via WebSocket (async version).

        Args:
            symbol: Trading symbol (e.g., "BTC")
            side: OrderSide.BUY or OrderSide.SELL
            quantity: Order quantity (will be rounded to lot_size)
            reduce_only: If True, order can only reduce position
            slippage_bps: Maximum acceptable slippage in basis points

        Returns:
            Same format as place_market_order()

        Raises:
            AuthenticationError: No private key configured
            InvalidOrderError: Order parameters invalid
            InsufficientBalanceError: Not enough balance
        """
        if not self.exchange or not self.account:
            raise AuthenticationError("Cannot place orders in read-only mode (no private key)")

        try:
            rounded_quantity = self.round_quantity(quantity, symbol)

            # Validate minimum notional using an initial snapshot
            initial_price = self._get_reference_price(symbol, side)
            expected_price = initial_price["ask"] if side == OrderSide.BUY else initial_price["bid"]
            specs = self.get_market_specs(symbol)
            notional = expected_price * rounded_quantity
            if notional < specs["min_notional"]:
                raise InvalidOrderError(
                    f"Order notional ${notional:.2f} below minimum ${specs['min_notional']}"
                )

            client_order_id = f"0x{uuid.uuid4().hex}"

            # Get asset index (position in universe array)
            asset_index = None
            for idx, asset in enumerate(self.meta["universe"]):
                if asset["name"] == symbol:
                    asset_index = idx
                    break

            if asset_index is None:
                raise InvalidSymbolError(f"Symbol {symbol} not found in universe")

            # For market orders via WebSocket, use aggressive slippage to guarantee immediate execution
            # Use provided slippage_bps as baseline, but ensure minimum slippage for reliable execution
            # For low-priced assets, use lower slippage to account for tick size rounding impact
            specs = self.get_market_specs(symbol)
            tick_size = specs["tick_size"]

            # Get reference price to determine if this is a low-priced asset
            price_ref = self._get_reference_price(symbol, side)
            ref_price = price_ref["ask"] if side == OrderSide.BUY else price_ref["bid"]
            tick_price_ratio = tick_size / ref_price  # How significant is tick size relative to price?

            # If tick size is >1% of price, use lower base slippage to avoid excessive rounding impact
            if tick_price_ratio > 0.01:  # Low-priced asset (like XPL)
                # OVERRIDE requested slippage for low-priced assets to avoid rounding issues
                initial_slippage = 500  # Start with 5%
                slippage_plan = [500, 1000, 2000]  # 5%, 10%, 20%
                self.logger.info(
                    f"[LOW-PRICED ASSET] tick/price={tick_price_ratio:.4f}, overriding slippage to conservative: {initial_slippage}bps"
                )
            else:  # High-priced asset (like SOL, BTC)
                initial_slippage = max(slippage_bps, 2000)  # 20% for high-priced assets
                slippage_plan = [initial_slippage, 5000]  # 20%, 50%
            last_error: Optional[str] = None

            for attempt_index, attempt_slippage in enumerate(slippage_plan, start=1):
                price_snapshot = self._get_reference_price(symbol, side)
                expected_price = price_snapshot["ask"] if side == OrderSide.BUY else price_snapshot["bid"]

                # For BUY: place limit far above ask to guarantee crossing order book
                # For SELL: place limit far below bid to guarantee crossing order book
                slippage_multiplier = 1 + (attempt_slippage / 10000)
                if side == OrderSide.BUY:
                    # BUY: limit price above ask (guaranteed execution)
                    limit_price_before_round = expected_price * slippage_multiplier
                else:
                    # SELL: limit price below bid (guaranteed execution)
                    limit_price_before_round = expected_price / slippage_multiplier

                # Debug logging before rounding
                self.logger.info(
                    f"[PRICE CALC] expected={expected_price:.6f}, slippage_mult={slippage_multiplier:.4f}, "
                    f"before_round={limit_price_before_round:.6f}"
                )

                limit_price = self._round_price_aggressive(limit_price_before_round, symbol, side)

                # Debug logging after rounding
                self.logger.info(
                    f"[PRICE ROUND] before={limit_price_before_round:.6f}, after={limit_price:.6f}, "
                    f"tick_size={self.get_market_specs(symbol)['tick_size']}"
                )

                if attempt_index > 1:
                    self.logger.warning(
                        f"[WS] Retrying market order with {attempt_slippage}bps slippage (attempt {attempt_index}/{len(slippage_plan)})"
                    )
                else:
                    self.logger.info(
                        f"[WS] Placing market order: {side} {rounded_quantity} {symbol} (expected: {expected_price})"
                    )

                # Build IOC order action (market order = IOC limit order)
                # Format strings properly: remove trailing .0 for integer quantities
                quantity_str = str(int(rounded_quantity)) if rounded_quantity == int(rounded_quantity) else str(rounded_quantity)
                price_str = str(limit_price)

                order = {
                    "a": asset_index,
                    "b": (side == OrderSide.BUY),
                    "p": price_str,
                    "s": quantity_str,
                    "r": reduce_only,
                    "t": {"limit": {"tif": "Ioc"}},  # IOC = market order
                    "c": client_order_id
                }

                self.logger.info(
                    f"[WS ORDER FORMAT] quantity: {rounded_quantity} -> '{quantity_str}', price: {limit_price} -> '{price_str}'"
                )

                action = {
                    "type": "order",
                    "orders": [order],
                    "grouping": "na"
                }

                # Detailed logging for debugging
                self.logger.info(
                    f"[WS ORDER PARAMS] symbol={symbol}, asset_index={asset_index}, "
                    f"side={'BUY' if side == OrderSide.BUY else 'SELL'}, "
                    f"quantity_raw={rounded_quantity}, quantity_str='{str(rounded_quantity)}', "
                    f"price_raw={limit_price}, price_str='{str(limit_price)}', "
                    f"expected_price={expected_price}, slippage_bps={attempt_slippage}, "
                    f"reduce_only={reduce_only}, cloid={client_order_id}"
                )

                # Sign the action
                nonce = int(time.time() * 1000)
                signature = sign_l1_action(
                    self.account,          # wallet
                    action,                # action
                    None,                  # active_pool (vaultAddress)
                    nonce,                 # nonce
                    None,                  # expires_after (no expiration)
                    not self.testnet       # is_mainnet (inverse of testnet)
                )

                # Build WebSocket request
                request_id = self._next_request_id
                self._next_request_id += 1

                ws_request = {
                    "method": "post",
                    "id": request_id,
                    "request": {
                        "type": "action",
                        "payload": {
                            "action": action,
                            "nonce": nonce,
                            "signature": signature,
                            "vaultAddress": None
                        }
                    }
                }

                # Send request and measure latency
                start_time = time.time()
                response = await self._send_trading_request(ws_request, timeout=5.0)
                latency_ms = (time.time() - start_time) * 1000

                # Parse response
                if response.get("response", {}).get("type") == "action":
                    payload = response["response"]["payload"]
                    status_info = payload.get("status")

                    if status_info == "ok":
                        response_data = payload.get("response", {}).get("data", {})
                        statuses = response_data.get("statuses", [])
                        status_entry = statuses[0] if statuses else {}
                        error_msg = status_entry.get("error")

                        if not error_msg:
                            # Success - extract fill info from filled status
                            order_id = "unknown"
                            fill_price = expected_price

                            # Check for filled status (market orders fill immediately)
                            if "filled" in status_entry:
                                filled_info = status_entry["filled"]
                                order_id = str(filled_info.get("oid", "unknown"))
                                fill_price = float(filled_info.get("avgPx", expected_price))
                            elif "resting" in status_entry:
                                # Shouldn't happen for IOC orders, but handle it
                                order_id = str(status_entry["resting"].get("oid", "unknown"))
                            else:
                                order_id = str(status_entry.get("oid", "unknown"))

                            notional = abs(fill_price * rounded_quantity)
                            fee_usd = notional * abs(self._taker_fee_rate)

                            self.logger.info(
                                f"[WS] Market order filled: {order_id} @ {fill_price} (latency: {latency_ms:.1f}ms)"
                            )
                            return {
                                "success": True,
                                "order_id": order_id,
                                "client_order_id": client_order_id,
                                "symbol": symbol,
                                "side": str(side),
                                "quantity": rounded_quantity,
                                "price": expected_price,
                                "status": "filled",
                                "timestamp": time.time(),
                                "fill_price": fill_price,
                                "slippage_bps": attempt_slippage,
                                "latency_ms": latency_ms,
                                "fee": fee_usd,
                                "error": None
                            }

                        # Error in order - retry if appropriate
                        last_error = error_msg
                        if "could not immediately match" in error_msg.lower() and attempt_index < len(slippage_plan):
                            self.logger.warning(
                                f"[WS] IOC rejected (no resting orders). Increasing slippage and retrying..."
                            )
                            await asyncio.sleep(0.05)
                            continue
                        if "insufficient" in error_msg.lower() or "balance" in error_msg.lower() or "margin" in error_msg.lower():
                            raise InsufficientBalanceError(f"Insufficient balance: {error_msg}")
                        return {
                            "success": False,
                            "order_id": None,
                            "client_order_id": client_order_id,
                            "symbol": symbol,
                            "side": str(side),
                            "quantity": rounded_quantity,
                            "price": expected_price,
                            "status": "rejected",
                            "timestamp": time.time(),
                            "fill_price": None,
                            "slippage_bps": attempt_slippage,
                            "latency_ms": latency_ms,
                            "fee": 0.0,
                            "error": error_msg
                        }

                    # Non-OK status - extract detailed error information
                    if isinstance(status_info, dict):
                        # Try to extract error details from dict
                        error_detail = status_info.get("error") or status_info.get("err") or status_info.get("message") or status_info
                        last_error = f"{error_detail} (full status: {status_info})"
                    else:
                        last_error = str(status_info)

                    self.logger.error(
                        f"[WS] Market order rejected: {last_error}, "
                        f"symbol={symbol}, side={side}, quantity={rounded_quantity}, "
                        f"price={expected_price}, slippage={attempt_slippage}bps"
                    )
                    if "insufficient" in last_error.lower() or "balance" in last_error.lower() or "margin" in last_error.lower():
                        raise InsufficientBalanceError(f"Insufficient balance: {last_error}")
                    break

                elif response.get("response", {}).get("type") == "error":
                    error_msg = response["response"].get("payload", "Unknown error")
                    self.logger.error(f"[WS] Market order error: {error_msg}")
                    raise InvalidOrderError(f"WebSocket order error: {error_msg}")

                raise InvalidOrderError(f"Unexpected WebSocket response: {response}")

            # All retries failed
            self.logger.error(f"[WS] Market order failed after retries: {last_error}")
            return {
                "success": False,
                "order_id": None,
                "client_order_id": client_order_id,
                "symbol": symbol,
                "side": str(side),
                "quantity": rounded_quantity,
                "price": expected_price,
                "status": "rejected",
                "timestamp": time.time(),
                "fill_price": None,
                "slippage_bps": slippage_plan[-1],
                "latency_ms": latency_ms,
                "error": last_error
            }

        except (AuthenticationError, InvalidOrderError, InsufficientBalanceError):
            raise
        except Exception as e:
            self.logger.error(f"[WS] Error placing market order: {e}")
            raise InvalidOrderError(f"Failed to place market order via WebSocket: {e}")


    @rate_limited()
    async def place_market_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        reduce_only: bool = False,
        slippage_bps: int = 50,
        prefer_rest: bool = False,
        ws_timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Place a market order (taker order) for immediate execution via WebSocket (with REST fallback).

        Args:
            symbol: Trading symbol (e.g., "BTC")
            side: OrderSide.BUY or OrderSide.SELL
            quantity: Order quantity (will be rounded to lot_size)
            reduce_only: If True, order can only reduce position
            slippage_bps: Maximum acceptable slippage in basis points
            prefer_rest: If True, skip WebSocket execution and go straight to REST
            ws_timeout: Optional timeout override for WebSocket execution
        """
        if not self.exchange:
            raise AuthenticationError("Cannot place orders in read-only mode (no private key)")

        ws_timeout = ws_timeout or self.ws_market_order_timeout

        async def _rest_fallback() -> Dict[str, Any]:
            return await asyncio.to_thread(
                self._place_market_order_rest_impl,
                symbol,
                side,
                quantity,
                reduce_only,
                slippage_bps
            )

        if prefer_rest:
            self.logger.info("[WS] Skipping WebSocket execution (prefer_rest=True)")
            return await _rest_fallback()

        try:
            ws_result = await asyncio.wait_for(
                self.place_market_order_ws_async(symbol, side, quantity, reduce_only, slippage_bps),
                timeout=ws_timeout
            )

            if ws_result.get("success"):
                return ws_result

            error_msg = ws_result.get("error", "unknown error").lower()
            if any(keyword in error_msg for keyword in ["insufficient", "balance", "margin", "invalid"]):
                self.logger.error(
                    f"[WS] Market order failed with fatal error, not retrying via REST: {error_msg}"
                )
                return ws_result

            self.logger.warning(
                f"[WS] Market order failed ({ws_result.get('error')}), falling back to REST for retry..."
            )

        except asyncio.TimeoutError:
            self.logger.error(
                f"[WS] Market order timed out after {ws_timeout:.1f}s, falling back to REST "
                f"(symbol={symbol}, side={side}, quantity={quantity})"
            )
        except Exception as ws_error:
            self.logger.warning(f"[WS] WebSocket market order exception, falling back to REST: {ws_error}")

        return await _rest_fallback()

    def _place_market_order_rest_impl(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        reduce_only: bool,
        slippage_bps: int
    ) -> Dict[str, Any]:
        """
        Blocking REST API fallback for market orders. Intended to run inside a worker thread.
        """
        rounded_quantity = self.round_quantity(quantity, symbol)

        initial_price = self._get_reference_price(symbol, side)
        expected_price = initial_price["ask"] if side == OrderSide.BUY else initial_price["bid"]
        specs = self.get_market_specs(symbol)
        notional = expected_price * rounded_quantity
        if notional < specs["min_notional"]:
            raise InvalidOrderError(
                f"Order notional ${notional:.2f} below minimum ${specs['min_notional']}"
            )

        client_order_id = f"0x{uuid.uuid4().hex}"
        client_order_cloid = Cloid.from_str(client_order_id)

        initial_slippage = max(slippage_bps, 2000)
        slippage_plan = [initial_slippage, 5000]
        last_error: Optional[str] = None
        latency_ms = 0.0

        for attempt_index, attempt_slippage in enumerate(slippage_plan, start=1):
            price_snapshot = self._get_reference_price(symbol, side)
            expected_price = price_snapshot["ask"] if side == OrderSide.BUY else price_snapshot["bid"]

            slippage_multiplier = 1 + (attempt_slippage / 10000)
            if side == OrderSide.BUY:
                limit_price = expected_price * slippage_multiplier
            else:
                limit_price = expected_price / slippage_multiplier
            limit_price = self._round_price_aggressive(limit_price, symbol, side)

            if attempt_index > 1:
                self.logger.warning(
                    "Retrying Hyperliquid market order with %sbps slippage (attempt %s/%s)",
                    attempt_slippage,
                    attempt_index,
                    len(slippage_plan)
                )
            else:
                self.logger.info(
                    f"Placing market order (REST): {side} {rounded_quantity} {symbol} (expected: {expected_price})"
                )

            order_type = {"limit": {"tif": "Ioc"}}
            start_time = time.time()
            response = self.exchange.order(
                name=symbol,
                is_buy=(side == OrderSide.BUY),
                sz=float(rounded_quantity),
                limit_px=float(limit_price),
                order_type=order_type,
                reduce_only=reduce_only,
                cloid=client_order_cloid
            )
            latency_ms = (time.time() - start_time) * 1000

            if response and "status" in response:
                self.logger.info(f"Raw order response: {response}")
                status_info = response["status"]
                statuses = response.get("response", {}).get("data", {}).get("statuses", [])
                status_entry = statuses[0] if statuses else {}
                error_msg = status_entry.get("error")

                if status_info == "ok" and not error_msg:
                    response_data = response.get("response", {}).get("data", {})
                    status_details = response_data.get("statuses", [])
                    order_id = "unknown"
                    if status_details:
                        order_id = str(status_details[0].get("oid", order_id))

                    fill_price = float(status_entry.get("avgPx", expected_price))
                    notional = abs(fill_price * rounded_quantity)
                    fee_usd = notional * abs(self._taker_fee_rate)

                    self.logger.info(
                        f"Market order filled: {order_id} @ {fill_price} (latency: {latency_ms:.1f}ms)"
                    )
                    return {
                        "success": True,
                        "order_id": order_id,
                        "client_order_id": client_order_id,
                        "symbol": symbol,
                        "side": str(side),
                        "quantity": rounded_quantity,
                        "price": expected_price,
                        "status": "filled",
                        "timestamp": time.time(),
                        "fill_price": fill_price,
                        "slippage_bps": attempt_slippage,
                        "latency_ms": latency_ms,
                        "fee": fee_usd,
                        "error": None
                    }

                if error_msg:
                    last_error = error_msg
                    if "could not immediately match" in error_msg.lower() and attempt_index < len(slippage_plan):
                        self.logger.warning(
                            "Hyperliquid IOC rejected (no resting orders). Increasing slippage and retrying..."
                        )
                        time.sleep(0.05)
                        continue
                    if "insufficient" in error_msg.lower() or "balance" in error_msg.lower() or "margin" in error_msg.lower():
                        raise InsufficientBalanceError(f"Insufficient balance: {error_msg}")
                    return {
                        "success": False,
                        "order_id": None,
                        "client_order_id": client_order_id,
                        "symbol": symbol,
                        "side": str(side),
                        "quantity": rounded_quantity,
                        "price": expected_price,
                        "status": "rejected",
                        "timestamp": time.time(),
                        "fill_price": None,
                        "slippage_bps": attempt_slippage,
                        "latency_ms": latency_ms,
                        "error": error_msg
                    }

                last_error = f"Unexpected status: {response}"
                break
            else:
                last_error = "Invalid response from Hyperliquid order API"
                break

        self.logger.error(f"Market order failed after retries: {last_error}")
        return {
            "success": False,
            "order_id": None,
            "client_order_id": client_order_id,
            "symbol": symbol,
            "side": str(side),
            "quantity": rounded_quantity,
            "price": expected_price,
                        "status": "rejected",
                        "timestamp": time.time(),
                        "fill_price": None,
                        "slippage_bps": slippage_plan[-1],
                        "latency_ms": latency_ms,
                        "fee": 0.0,
                        "error": last_error
                    }

    @rate_limited()
    async def cancel_order(self, symbol: str, order_id: str = None, client_order_id: str = None) -> Dict[str, Any]:
        """
        Cancel a specific order via WebSocket (with REST fallback).

        Args:
            symbol: Trading symbol (e.g., "BTC")
            order_id: Exchange order ID (oid)
            client_order_id: Client order ID (cloid) - alternative to order_id

        Returns:
            {
                "success": bool,
                "order_id": str,
                "status": str,         # "cancelled" or "failed"
                "error": Optional[str]
            }

        Raises:
            AuthenticationError: No private key configured
            InvalidOrderError: Neither order_id nor client_order_id provided

        Notes:
            - Must provide either order_id or client_order_id
            - If both provided, order_id takes precedence
        """
        if not self.exchange:
            raise AuthenticationError("Cannot cancel orders in read-only mode (no private key)")

        if not order_id and not client_order_id:
            raise InvalidOrderError("Must provide either order_id or client_order_id")

        # Try WebSocket first (only if we have order_id - Hyperliquid WS doesn't support cloid cancel)
        if order_id:
            try:
                result = await self.cancel_order_ws_async(symbol, order_id, client_order_id)
                return result

            except Exception as ws_error:
                # WebSocket failed, fall back to REST
                self.logger.warning(f"WebSocket order cancellation failed, falling back to REST: {ws_error}")
        else:
            self.logger.debug("No order_id provided, using REST for cancellation (WebSocket requires oid)")

        # REST fallback
        identifier = f"order_id={order_id}" if order_id else f"client_order_id={client_order_id}"
        target_reference = order_id or client_order_id

        if order_id:
            try:
                oid_int = int(order_id)
            except (TypeError, ValueError) as exc:
                raise InvalidOrderError(f"Invalid order_id provided: {order_id}") from exc
        else:
            cloid_obj = self._normalize_cloid(client_order_id)

        try:
            self.logger.info(f"[REST] Cancelling order: {symbol} {identifier}")

            # Send cancel request
            if order_id:
                response = self.exchange.cancel(symbol, oid_int)
            else:
                response = self.exchange.cancel_by_cloid(symbol, cloid_obj)

            # Parse response
            if response and "status" in response:
                status_info = response["status"]

                if status_info == "ok":
                    result = {
                        "success": True,
                        "order_id": target_reference,
                        "status": "cancelled",
                        "error": None
                    }
                    self.logger.info(f"[REST] Order cancelled: {identifier}")
                    return result
                else:
                    error_msg = str(status_info)
                    self.logger.warning(f"[REST] Order cancellation failed: {error_msg}")
                    return {
                        "success": False,
                        "order_id": target_reference,
                        "status": "failed",
                        "error": error_msg
                    }
            else:
                raise InvalidOrderError(f"Unexpected cancel response: {response}")

        except AuthenticationError:
            raise
        except InvalidOrderError:
            raise
        except Exception as e:
            self.logger.error(f"Error cancelling order {identifier}: {e}")
            return {
                "success": False,
                "order_id": target_reference,
                "status": "failed",
                "error": str(e)
            }

    @rate_limited()
    def cancel_all_orders(self, symbol: str = None) -> Dict[str, Any]:
        """
        Cancel all open orders (optionally for specific symbol).

        Args:
            symbol: Trading symbol (e.g., "BTC"). If None, cancels all orders across all symbols.

        Returns:
            {
                "success": bool,
                "cancelled_count": int,      # Number of orders successfully cancelled
                "failed_count": int,         # Number of orders that failed to cancel
                "error": Optional[str]
            }

        Raises:
            AuthenticationError: No private key configured

        Implementation Notes:
            - Emergency function for risk management
            - Fetches all open orders first, then cancels them
            - Returns counts of successful/failed cancellations
        """
        if not self.exchange:
            raise AuthenticationError("Cannot cancel orders in read-only mode (no private key)")

        try:
            # Get all open orders using dedicated API
            raw_open_orders = self.info.open_orders(self.address)

            if not raw_open_orders:
                self.logger.info(f"No open orders to cancel{f' for {symbol}' if symbol else ''}")
                return {
                    "success": True,
                    "cancelled_count": 0,
                    "failed_count": 0,
                    "error": None
                }

            # Collect orders to cancel
            orders_to_cancel: List[Tuple[str, int]] = []

            for order in raw_open_orders:
                coin = order.get("coin")

                # Filter by symbol if specified
                if symbol and coin != symbol:
                    continue

                try:
                    oid = int(order["oid"])
                    orders_to_cancel.append((coin, oid))
                except (KeyError, TypeError, ValueError):
                    self.logger.debug(f"Skipping order with invalid oid: {order}")

            if not orders_to_cancel:
                self.logger.info(f"No open orders to cancel{f' for {symbol}' if symbol else ''}")
                return {
                    "success": True,
                    "cancelled_count": 0,
                    "failed_count": 0,
                    "error": None
                }

            # Cancel all orders
            symbol_str = f" for {symbol}" if symbol else ""
            self.logger.info(f"Cancelling {len(orders_to_cancel)} orders{symbol_str}...")

            cancelled_count = 0
            failed_count = 0
            errors = []

            for coin, oid in orders_to_cancel:
                try:
                    response = self.exchange.cancel(coin, oid)
                    status = response.get("status") if isinstance(response, dict) else None

                    if status == "ok":
                        cancelled_count += 1
                    else:
                        failed_count += 1
                        error_detail = status if status else response
                        error_msg = str(error_detail if error_detail is not None else "Unknown error")
                        errors.append(error_msg)
                        self.logger.warning(f"Failed to cancel order {coin}:{oid}: {error_msg}")

                except Exception as e:
                    failed_count += 1
                    errors.append(str(e))
                    self.logger.error(f"Exception cancelling order {coin}:{oid}: {e}")

            # Return summary
            result = {
                "success": (failed_count == 0),
                "cancelled_count": cancelled_count,
                "failed_count": failed_count,
                "error": None if not errors else f"Some cancellations failed: {errors[0]}"
            }

            self.logger.info(
                f"Cancel all completed{symbol_str}: "
                f"{cancelled_count} cancelled, {failed_count} failed"
            )

            return result

        except AuthenticationError:
            raise
        except Exception as e:
            self.logger.error(f"Error in cancel_all_orders: {e}")
            return {
                "success": False,
                "cancelled_count": 0,
                "failed_count": 0,
                "error": str(e)
            }

    def get_position_ws(self, symbol: str, timeout: float = 10.0) -> Dict[str, Any]:
        """
        Get current position via WebSocket (using shared userEvents subscription cache).

        Args:
            symbol: Trading symbol (e.g., "BTC")
            timeout: Timeout in seconds (not used, kept for interface compatibility)

        Returns:
            {
                "symbol": str,
                "quantity": float,           # Positive = long, negative = short, 0 = flat
                "entry_price": float,        # Average entry price
                "mark_price": float,         # Current mark price
                "unrealized_pnl": float,     # Unrealized P&L in USD
                "notional": float,           # Position notional value (abs)
                "leverage": float,           # Current leverage
                "liquidation_price": float,  # Liquidation price (None if flat)
                "timestamp": float
            }

        Raises:
            ConnectionError: WebSocket connection failed
            TimeoutError: No data received within timeout

        Notes:
            IMPORTANT: Hyperliquid only allows ONE userEvents subscription per connection.
            This method uses a SHARED subscription approach:
            - start_fill_stream() subscribes to userEvents ONCE (for both fills AND positions)
            - Position updates are cached from that single stream
            - get_position_ws() reads from cache (WebSocket speed without double subscription)
            - Falls back to REST if cache is empty (before fill stream starts)
        """
        # Try to read from position cache (updated by fill stream's shared userEvents subscription)
        with self._position_cache_lock:
            if symbol in self._position_cache:
                cached_position = self._position_cache[symbol].copy()
                self.logger.debug(
                    f"[POSITION WS] Cache hit for {symbol}: "
                    f"qty={cached_position['quantity']:+.4f} (age: {time.time() - cached_position['timestamp']:.1f}s)"
                )
                return cached_position

        # Cache miss - fall back to REST
        # This can happen if: (1) fill stream not started yet, (2) no position updates received yet,
        # or (3) position is zero and Hyperliquid hasn't sent assetPositions
        self.logger.debug(
            f"[POSITION WS] Cache miss for {symbol} - using REST fallback "
            f"(cache not populated yet - normal for zero positions or before first update)"
        )
        return self.get_position_rest(symbol)

    @rate_limited()
    def get_position_rest(self, symbol: str) -> Dict[str, Any]:
        """
        Get current position via REST API (fallback method).

        Args:
            symbol: Trading symbol (e.g., "BTC")

        Returns:
            {
                "symbol": str,
                "quantity": float,
                "entry_price": float,
                "mark_price": float,
                "unrealized_pnl": float,
                "notional": float,
                "leverage": float,
                "liquidation_price": float,
                "timestamp": float
            }
        """
        try:
            # Get user state from REST API
            user_state = self.info.user_state(self.address)

            if not user_state or "assetPositions" not in user_state:
                # No positions = flat position
                return {
                    "symbol": symbol,
                    "quantity": 0.0,
                    "entry_price": 0.0,
                    "mark_price": 0.0,
                    "unrealized_pnl": 0.0,
                    "notional": 0.0,
                    "leverage": 1.0,
                    "liquidation_price": None,
                    "timestamp": time.time()
                }

            # Find position for this symbol
            for asset_position in user_state["assetPositions"]:
                position = asset_position["position"]
                if position["coin"] == symbol:
                    # Parse position data
                    szi = float(position.get("szi", "0"))  # Signed size
                    entry_px = float(position.get("entryPx", "0")) if szi != 0 else 0.0
                    mark_px = float(position.get("markPx", "0"))
                    unrealized_pnl = float(position.get("unrealizedPnl", "0"))
                    liquidation_px = position.get("liquidationPx")
                    leverage_str = position.get("leverage", {}).get("value", "1")
                    leverage = float(leverage_str)

                    # If mark_price is 0, fetch current market price from orderbook
                    if mark_px == 0 and szi != 0:
                        try:
                            bid_ask = self.get_bid_ask_rest(symbol)
                            mark_px = bid_ask["mid"]
                            self.logger.debug(f"Fetched market price for {symbol}: ${mark_px}")
                        except Exception as e:
                            self.logger.warning(f"Could not fetch market price for {symbol}: {e}, using entry price")
                            mark_px = entry_px

                    # Use mark_price for notional, fallback to entry_price if still 0
                    price_for_notional = mark_px if mark_px != 0 else entry_px

                    return {
                        "symbol": symbol,
                        "quantity": szi,
                        "entry_price": entry_px,
                        "mark_price": mark_px,
                        "unrealized_pnl": unrealized_pnl,
                        "notional": abs(szi * price_for_notional),
                        "leverage": leverage,
                        "liquidation_price": float(liquidation_px) if liquidation_px else None,
                        "timestamp": time.time()
                    }

            # Symbol not found in positions = flat
            return {
                "symbol": symbol,
                "quantity": 0.0,
                "entry_price": 0.0,
                "mark_price": 0.0,
                "unrealized_pnl": 0.0,
                "notional": 0.0,
                "leverage": 1.0,
                "liquidation_price": None,
                "timestamp": time.time()
            }

        except Exception as e:
            self.logger.error(f"Error fetching position for {symbol}: {e}")
            raise MarketDataError(f"Failed to fetch position: {e}")

    def start_fill_stream(self, callback: Callable) -> str:
        """
        Start WebSocket stream for fill events (CRITICAL for immediate hedging).

        Args:
            callback: Function called on each fill event
                     Signature: callback(fill_data: Dict[str, Any])
                     fill_data format:
                     {
                         "order_id": str,
                         "client_order_id": str,
                         "symbol": str,
                         "side": str,              # "buy" or "sell"
                         "filled_quantity": float,
                         "fill_price": float,
                         "fee": float,             # Fee in USD
                         "is_maker": bool,         # True if maker, False if taker
                         "timestamp": float,
                         "fill_id": str
                     }

        Returns:
            subscription_id: String ID for later unsubscription

        Implementation Notes:
            - CRITICAL: This must fire immediately when orders fill
            - Used to trigger immediate hedging on the other exchange
            - Latency here directly impacts hedging quality
            - Must handle partial fills correctly
            - Only ONE fill stream per connector (not per symbol)

        Trading Flow:
            1. Maker order placed on Exchange A
            2. Fill event received via this stream
            3. Callback triggers immediate hedge on Exchange B
            4. Target: <500ms from fill to hedge execution
        """
        # Only allow one fill stream at a time
        if self.fill_subscription_id:
            self.logger.warning("Fill stream already active, stopping old stream")
            self.stop_fill_stream(self.fill_subscription_id)

        def wrapped_callback(data):
            """Wrapper to parse user events and extract fills AND position updates"""
            try:
                if not data or "data" not in data:
                    return

                user_data = data["data"]

                # Update position cache from userEvents (SHARED SUBSCRIPTION APPROACH)
                # This allows get_position_ws() to read from cache without creating another subscription
                if "assetPositions" in user_data:
                    with self._position_cache_lock:
                        for asset_position in user_data["assetPositions"]:
                            position = asset_position["position"]
                            symbol = position["coin"]

                            # Parse position data (same format as get_position_rest)
                            szi = float(position.get("szi", "0"))  # Signed size
                            entry_px = float(position.get("entryPx", "0")) if szi != 0 else 0.0
                            mark_px = float(position.get("markPx", "0"))
                            unrealized_pnl = float(position.get("unrealizedPnl", "0"))
                            liquidation_px = position.get("liquidationPx")
                            leverage_str = position.get("leverage", {}).get("value", "1")
                            leverage = float(leverage_str)

                            # Cache position data for this symbol
                            self._position_cache[symbol] = {
                                "symbol": symbol,
                                "quantity": szi,
                                "entry_price": entry_px,
                                "mark_price": mark_px,
                                "unrealized_pnl": unrealized_pnl,
                                "notional": abs(szi * mark_px),
                                "leverage": leverage,
                                "liquidation_price": float(liquidation_px) if liquidation_px else None,
                                "timestamp": time.time()
                            }

                            self.logger.debug(
                                f"[POSITION CACHE] Updated {symbol}: "
                                f"qty={szi:+.4f}, entry=${entry_px:.2f}, mark=${mark_px:.2f}"
                            )

                # Check for fill events
                if "fills" in user_data:
                    fills = user_data["fills"]

                    for fill in fills:
                        # Parse fill data
                        fill_data = {
                            "order_id": str(fill.get("oid", "")),
                            "client_order_id": fill.get("cloid", ""),
                            "symbol": fill.get("coin", ""),
                            "side": "buy" if fill.get("side") == "B" else "sell",
                            "filled_quantity": float(fill.get("sz", "0")),
                            "fill_price": float(fill.get("px", "0")),
                            "fee": float(fill.get("fee", "0")),
                            "is_maker": not fill.get("liquidation", False),  # Liquidations are taker
                            "timestamp": float(fill.get("time", time.time() * 1000)) / 1000,  # Convert ms to s
                            "fill_id": str(fill.get("tid", ""))
                        }

                        # Log the fill
                        self.logger.info(
                            f"FILL: {fill_data['side']} {fill_data['filled_quantity']} {fill_data['symbol']} "
                            f"@ {fill_data['fill_price']} (order: {fill_data['order_id']}, "
                            f"{'maker' if fill_data['is_maker'] else 'taker'})"
                        )

                        # Call user callback (use self.fill_callback to allow dynamic updates)
                        if self.fill_callback:
                            self.fill_callback(fill_data)

            except Exception as e:
                self.logger.error(f"Error in fill stream callback: {e}")

        # Subscribe to user events
        try:
            subscription_id = self.info.subscribe(
                {"type": "userEvents", "user": self.address},
                wrapped_callback
            )

            # Store subscription
            self.fill_subscription_id = subscription_id
            self.fill_callback = callback

            self.logger.info(f"Started fill stream (subscription: {subscription_id})")
            self.logger.warning("CRITICAL: Fill stream active - ready for immediate hedging")

            # Initialize position cache with current positions to avoid initial cache misses
            try:
                self.logger.debug("Initializing position cache with current positions...")
                user_state = self.info.user_state(self.address)

                if user_state and "assetPositions" in user_state:
                    with self._position_cache_lock:
                        for asset_position in user_state["assetPositions"]:
                            position = asset_position["position"]
                            symbol = position["coin"]

                            szi = float(position.get("szi", "0"))
                            entry_px = float(position.get("entryPx", "0")) if szi != 0 else 0.0
                            mark_px = float(position.get("markPx", "0"))
                            unrealized_pnl = float(position.get("unrealizedPnl", "0"))
                            liquidation_px = position.get("liquidationPx")
                            leverage_str = position.get("leverage", {}).get("value", "1")
                            leverage = float(leverage_str)

                            self._position_cache[symbol] = {
                                "symbol": symbol,
                                "quantity": szi,
                                "entry_price": entry_px,
                                "mark_price": mark_px,
                                "unrealized_pnl": unrealized_pnl,
                                "notional": abs(szi * mark_px),
                                "leverage": leverage,
                                "liquidation_price": float(liquidation_px) if liquidation_px else None,
                                "timestamp": time.time()
                            }

                    self.logger.info(f"Position cache initialized with {len(self._position_cache)} positions")
                else:
                    self.logger.debug("No positions found during cache initialization (all flat)")

            except Exception as cache_init_error:
                # Non-critical error - cache will populate from WebSocket updates
                self.logger.warning(f"Failed to initialize position cache (will populate from WebSocket): {cache_init_error}")

            return subscription_id

        except Exception as e:
            self.logger.error(f"Failed to start fill stream: {e}")
            raise ConnectionError(f"Failed to start fill stream: {e}")

    def stop_fill_stream(self, subscription_id: str) -> bool:
        """
        Stop fill stream subscription.

        Args:
            subscription_id: ID returned by start_fill_stream()

        Returns:
            True if successfully unsubscribed

        Notes:
            If subscription_id not found, returns False but doesn't raise error.
            Gracefully handles event loop closure and SDK errors.
        """
        try:
            if subscription_id != self.fill_subscription_id:
                # Silently return False - subscription not tracked
                return False

            # Try to unsubscribe from WebSocket (may fail if event loop closed)
            try:
                if self.info:
                    self.info.unsubscribe(subscription_id)
            except Exception:
                # Silently ignore unsubscribe errors during cleanup
                # (event loop may be closed, WebSocket may be disconnected, etc.)
                pass

            # Clear tracking
            self.fill_subscription_id = None
            self.fill_callback = None

            return True

        except Exception:
            # Silently handle all errors during cleanup
            # Clear tracking anyway to prevent leaks
            self.fill_subscription_id = None
            self.fill_callback = None
            return False

    def subscribe_user_state(self, callback: Callable[[Dict[str, Any]], None]) -> str:
        """
        Subscribe to user state updates via WebSocket (for dashboard).

        Wraps the userEvents subscription to provide complete account state including:
        - Positions (all symbols)
        - Margin summary (account value, withdrawable, etc.)
        - Cross margin summary

        Args:
            callback: Function to call with user state data

        Returns:
            Subscription ID string

        Notes:
            - Reuses the same userEvents channel as fill stream (shared subscription)
            - Updates are sent whenever account state changes
            - Callback receives full clearinghouseState format
        """
        # Wrap the callback to extract user state from userEvents
        def user_state_callback(event_data):
            try:
                # userEvents contains fills, but SDK also provides user_state via separate API
                # For dashboard, we'll piggyback on the existing userEvents subscription
                # and periodically call user_state REST API (cached)
                callback(event_data)
            except Exception as e:
                self.logger.error(f"Error in user state callback: {e}")

        # Use existing fill stream infrastructure (userEvents subscription)
        # Note: This shares the subscription with fill stream
        return self.start_fill_stream(user_state_callback)

    @rate_limited()
    def get_equity(self) -> float:
        """
        Get total account equity (balance + unrealized PnL).

        Returns:
            Total equity in USD

        Notes:
            - Includes unrealized P&L from open positions
            - Used for risk management calculations
        """
        try:
            user_state = self.info.user_state(self.address)

            if not user_state or "marginSummary" not in user_state:
                self.logger.warning("No margin summary found, returning 0")
                return 0.0

            margin_summary = user_state["marginSummary"]

            # Account value includes balance + unrealized PnL
            account_value = float(margin_summary.get("accountValue", "0"))

            return account_value

        except Exception as e:
            self.logger.error(f"Error fetching equity: {e}")
            raise MarketDataError(f"Failed to fetch equity: {e}")

    @rate_limited()
    def get_balance(self, asset: str = "USDC") -> float:
        """
        Get available balance for a specific asset.

        Args:
            asset: Asset symbol (default: "USDC")

        Returns:
            Available balance

        Notes:
            - Returns withdrawable balance (excludes margin in use)
            - For Hyperliquid, collateral is primarily USDC
        """
        try:
            user_state = self.info.user_state(self.address)

            if not user_state:
                self.logger.warning("No user state found, returning 0")
                return 0.0

            # For USDC collateral balance
            if asset == "USDC":
                if "marginSummary" not in user_state:
                    return 0.0

                margin_summary = user_state["marginSummary"]

                # Total account value minus margin used
                account_value = float(margin_summary.get("accountValue", "0"))
                total_margin_used = float(margin_summary.get("totalMarginUsed", "0"))

                # Available balance = account value - margin in use
                available_balance = account_value - total_margin_used

                return max(0.0, available_balance)  # Never return negative

            # For other assets, check asset positions
            else:
                if "assetPositions" not in user_state:
                    return 0.0

                for asset_position in user_state["assetPositions"]:
                    position = asset_position["position"]
                    if position["coin"] == asset:
                        # Return the position size (could be used for asset-specific balance)
                        return abs(float(position.get("szi", "0")))

                return 0.0

        except Exception as e:
            self.logger.error(f"Error fetching balance for {asset}: {e}")
            raise MarketDataError(f"Failed to fetch balance: {e}")

    @rate_limited()
    def update_leverage(
        self,
        symbol: str,
        leverage: int,
        is_cross: bool = True
    ) -> Dict[str, Any]:
        """
        Update leverage for a specific symbol.

        Args:
            symbol: Trading symbol (e.g., "BTC", "ETH")
            leverage: Leverage value (e.g., 1, 2, 5, 10)
            is_cross: True for cross margin, False for isolated (default: True)

        Returns:
            {
                "success": bool,
                "error": Optional[str]
            }

        Notes:
            - Exchange may reject if there's an open position
            - For open positions, you can only increase leverage, not decrease
        """
        try:
            # Call SDK method to update leverage
            response = self.exchange.update_leverage(leverage, symbol, is_cross)

            # Check if response indicates success
            if response and response.get("status") == "ok":
                self.logger.info(
                    f"Successfully updated {symbol} leverage to {leverage}x "
                    f"({'cross' if is_cross else 'isolated'})"
                )
                return {
                    "success": True,
                    "error": None
                }
            else:
                error_msg = response.get("response", {}).get("data", "Unknown error") if response else "No response"
                self.logger.error(f"Failed to update leverage for {symbol}: {error_msg}")
                return {
                    "success": False,
                    "error": str(error_msg)
                }

        except Exception as e:
            self.logger.error(f"Error updating leverage for {symbol}: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def get_market_specs(self, symbol: str) -> Dict[str, Any]:
        """
        Get market specifications for a symbol.

        Args:
            symbol: Trading symbol (e.g., "BTC")

        Returns:
            {
                "symbol": str,
                "tick_size": float,           # Minimum price increment
                "lot_size": float,            # Minimum quantity increment
                "min_notional": float,        # Minimum order value
                "max_leverage": int,          # Maximum leverage allowed
                "maker_fee": float,           # Maker fee (-0.0001 = -1bp rebate)
                "taker_fee": float            # Taker fee (0.0004 = 4bp)
            }

        Raises:
            InvalidSymbolError: If symbol not found
        """
        try:
            asset_meta = self.coin_to_meta[symbol]

            # Calculate lot size from szDecimals
            sz_decimals = asset_meta["szDecimals"]
            lot_size = 10 ** (-sz_decimals)

            # CRITICAL FIX: Get actual tick size from orderbook
            # API docs give MAX decimal places, but actual tick size may be coarser
            # We MUST derive from real L2 price increments
            tick_size = self._get_tick_size_from_orderbook(symbol, sz_decimals)

            return {
                "symbol": symbol,
                "tick_size": tick_size,
                "lot_size": lot_size,
                "min_notional": 10.0,  # $10 minimum (standard for most exchanges)
                "max_leverage": int(asset_meta["maxLeverage"]),
                "maker_fee": self._maker_fee_rate,
                "taker_fee": self._taker_fee_rate
            }

        except KeyError:
            self.logger.error(f"Symbol {symbol} not found in metadata")
            raise InvalidSymbolError(f"Symbol not found: {symbol}")

    def _get_tick_size_from_orderbook(self, symbol: str, sz_decimals: int) -> float:
        """
        Derive actual tick size from L2 orderbook price increments.

        While Hyperliquid API defines MAX decimal places as (6 - szDecimals),
        the actual tick size used by the exchange can be coarser.
        We must infer from real price levels in the orderbook.

        Args:
            symbol: Trading symbol
            sz_decimals: Size decimals from metadata

        Returns:
            Actual tick size used by the exchange (as float with clean precision)

        Note:
            Results are cached to avoid repeated API calls (prevents rate limiting)
        """
        # Check cache first (avoid rate limiting from repeated API calls)
        with self._tick_size_cache_lock:
            if symbol in self._tick_size_cache:
                return self._tick_size_cache[symbol]

        try:
            # Get L2 snapshot (EXPENSIVE API call - only done once per symbol)
            l2_data = self.info.l2_snapshot(symbol)

            # Extract bid prices (more liquidity usually)
            bids = l2_data.get('levels', [[]])[0]

            if len(bids) >= 2:
                # Get first 20 price levels for good sampling (use Decimal for precision)
                prices = [Decimal(str(level['px'])) for level in bids[:20]]

                # Calculate minimum non-zero difference between consecutive prices
                diffs = []
                for i in range(len(prices)-1):
                    diff = abs(prices[i] - prices[i+1])
                    if diff > 0:  # Skip duplicate prices
                        diffs.append(diff)

                if diffs:
                    # The smallest difference is the tick size
                    tick_size_decimal = min(diffs)

                    # Convert to float for compatibility, but round to avoid floating point errors
                    # Count decimal places in the tick size
                    tick_str = str(tick_size_decimal)
                    if '.' in tick_str:
                        decimal_places = len(tick_str.split('.')[1].rstrip('0'))
                    else:
                        decimal_places = 0

                    # Round to exact decimal places to avoid floating point artifacts
                    tick_size = round(float(tick_size_decimal), decimal_places)

                    # Cache the result
                    with self._tick_size_cache_lock:
                        self._tick_size_cache[symbol] = tick_size

                    self.logger.debug(
                        f"Derived tick size for {symbol} from orderbook: {tick_size:.8f} "
                        f"(szDecimals={sz_decimals}) [CACHED]"
                    )
                    return tick_size

            # Fallback: Calculate from szDecimals formula (may not be exact)
            MAX_DECIMALS = 6  # Perps
            price_decimals = MAX_DECIMALS - sz_decimals
            tick_size = 10 ** (-price_decimals)

            # Cache the fallback result too
            with self._tick_size_cache_lock:
                self._tick_size_cache[symbol] = tick_size

            self.logger.warning(
                f"Could not derive tick size from orderbook for {symbol}, "
                f"using formula tick_size=10^(-{price_decimals})={tick_size:.8f} [CACHED]"
            )
            return tick_size

        except Exception as e:
            self.logger.error(f"Error getting tick size for {symbol}: {e}")

            # Try formula fallback on error
            try:
                MAX_DECIMALS = 6
                price_decimals = MAX_DECIMALS - sz_decimals
                tick_size = 10 ** (-price_decimals)

                # Cache even the error-fallback
                with self._tick_size_cache_lock:
                    self._tick_size_cache[symbol] = tick_size

                self.logger.warning(
                    f"Using formula fallback for {symbol} after error: "
                    f"tick_size={tick_size:.8f} [CACHED]"
                )
                return tick_size
            except:
                # Ultimate fallback
                return 0.01

    @rate_limited()
    def get_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """
        Get all open orders (optionally filtered by symbol).

        Args:
            symbol: Trading symbol (e.g., "BTC"). If None, returns all open orders.

        Returns:
            List of open orders:
            [
                {
                    "order_id": str,
                    "client_order_id": str,
                    "symbol": str,
                    "side": str,
                    "quantity": float,
                    "filled_quantity": float,
                    "price": float,
                    "status": str,
                    "order_type": str,
                    "timestamp": float
                },
                ...
            ]

        Notes:
            - Used for order health checks
            - Critical for detecting stale orders
        """
        try:
            # Use the dedicated open_orders API method
            raw_open_orders = self.info.open_orders(self.address)

            if not raw_open_orders:
                return []

            open_orders = []

            for order in raw_open_orders:
                coin = order.get("coin")

                # Filter by symbol if specified
                if symbol and coin != symbol:
                    continue

                # Parse order data
                order_data = {
                    "order_id": str(order.get("oid")),
                    "client_order_id": order.get("cloid", ""),
                    "symbol": coin,
                    "side": "buy" if order.get("side") == "B" else "sell",
                    "quantity": float(order.get("sz", "0")),
                    "filled_quantity": float(order.get("filledSz", "0")),
                    "price": float(order.get("limitPx", "0")),
                    "status": "open",
                    "order_type": order.get("orderType", "limit"),
                    "timestamp": float(order.get("timestamp", time.time() * 1000)) / 1000  # Convert ms to seconds
                }
                open_orders.append(order_data)

            return open_orders

        except Exception as e:
            self.logger.error(f"Error fetching open orders: {e}")
            raise MarketDataError(f"Failed to fetch open orders: {e}")

    def round_price(self, price: float, symbol: str, side: OrderSide) -> float:
        """
        Round price to valid tick size.

        Args:
            price: Raw price
            symbol: Trading symbol
            side: OrderSide.BUY (round down) or OrderSide.SELL (round up)

        Returns:
            Properly rounded price

        Notes:
            - Buy orders round DOWN (more conservative, lower price)
            - Sell orders round UP (more conservative, higher price)
        """
        specs = self.get_market_specs(symbol)
        tick_size = Decimal(str(specs["tick_size"]))
        price_dec = Decimal(str(price))

        if side == OrderSide.BUY:
            # Round down for buy orders
            rounded = (price_dec / tick_size).quantize(Decimal('1'), rounding=ROUND_DOWN) * tick_size
        else:
            # Round up for sell orders
            rounded = (price_dec / tick_size).quantize(Decimal('1'), rounding=ROUND_UP) * tick_size

        # Normalize to remove trailing zeros and convert to clean float
        # This prevents floating point precision artifacts like 0.34074999999844924
        rounded = rounded.normalize()

        # Determine decimal places from tick size to round the final float
        tick_str = str(tick_size)
        if '.' in tick_str:
            decimal_places = len(tick_str.split('.')[1].rstrip('0'))
        else:
            decimal_places = 0

        return round(float(rounded), decimal_places)

    def _round_price_aggressive(self, price: float, symbol: str, side: OrderSide) -> float:
        """Round price in the direction that guarantees execution for market orders."""
        specs = self.get_market_specs(symbol)
        tick_size = Decimal(str(specs["tick_size"]))
        price_dec = Decimal(str(price))

        if side == OrderSide.BUY:
            rounded = (price_dec / tick_size).quantize(Decimal('1'), rounding=ROUND_UP) * tick_size
        else:
            rounded = (price_dec / tick_size).quantize(Decimal('1'), rounding=ROUND_DOWN) * tick_size

        # CRITICAL FIX: For low-priced assets where price < tick_size, rounding down gives 0
        # Use tick_size as minimum to avoid invalid $0.00 price
        if rounded < tick_size:
            self.logger.warning(
                f"[PRICE FIX] Rounded price {float(rounded):.8f} < tick_size {float(tick_size):.8f}, "
                f"using tick_size as minimum for {symbol}"
            )
            rounded = tick_size

        # Normalize to remove trailing zeros and convert to clean float
        # This prevents floating point precision artifacts like 0.34074999999844924
        rounded = rounded.normalize()

        # Determine decimal places from tick size to round the final float
        tick_str = str(tick_size)
        if '.' in tick_str:
            decimal_places = len(tick_str.split('.')[1].rstrip('0'))
        else:
            decimal_places = 0

        return round(float(rounded), decimal_places)

    def round_quantity(self, quantity: float, symbol: str) -> float:
        """
        Round quantity to valid lot size.

        Args:
            quantity: Raw quantity
            symbol: Trading symbol

        Returns:
            Properly rounded quantity (always rounds down to avoid over-trading)

        Notes:
            - Always rounds DOWN to ensure we don't exceed available balance
            - Uses szDecimals from market metadata
        """
        specs = self.get_market_specs(symbol)
        lot_size = Decimal(str(specs["lot_size"]))
        quantity_dec = Decimal(str(quantity))

        # Always round down to avoid over-trading
        rounded = (quantity_dec / lot_size).quantize(Decimal('1'), rounding=ROUND_DOWN) * lot_size

        # Normalize to remove trailing zeros and convert to clean float
        rounded = rounded.normalize()

        # Determine decimal places from lot size to round the final float
        lot_str = str(lot_size)
        if '.' in lot_str:
            decimal_places = len(lot_str.split('.')[1].rstrip('0'))
        else:
            decimal_places = 0

        return round(float(rounded), decimal_places)
