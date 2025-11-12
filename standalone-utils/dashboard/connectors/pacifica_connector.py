"""
Pacifica exchange connector implementing BaseExchangeConnector interface.

This connector wraps the Pacifica client to provide a consistent interface
for the XEMM strategy. It implements all required abstract methods from
BaseExchangeConnector.
"""

import time
import asyncio
import websockets
import json
import uuid
import logging
from threading import Lock
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Dict, List, Any, Callable, Optional
from functools import wraps
from solders.keypair import Keypair
from websockets.protocol import State

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

# Import Pacifica SDK
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    from pacifica_sdk.common.constants import REST_URL, WS_URL
    from pacifica_sdk.common.utils import sign_message
    import requests
except ImportError:
    # Fallback for testing
    REST_URL = "https://api.pacifica.exchange"
    WS_URL = "wss://api.pacifica.exchange/ws"
    requests = None
    sign_message = None


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


class PacificaConnector(BaseExchangeConnector):
    """
    Pacifica exchange connector implementing BaseExchangeConnector interface.

    This connector uses the Pacifica SDK to interact with the Pacifica exchange
    for market data, order execution, and position management.
    """

    def __init__(
        self,
        sol_wallet: str,
        api_public: str,
        api_private: str,
        slippage_bps: int = 50,
        backup_poll_interval_seconds: float = 2.0
    ):
        """
        Initialize Pacifica connector.

        Args:
            sol_wallet: Solana wallet address
            api_public: API public key (agent wallet)
            api_private: API private key for signing
            slippage_bps: Default slippage in basis points for market orders
            backup_poll_interval_seconds: Backup fill polling interval in seconds (default: 2.0s)
        """
        super().__init__("pacifica")

        self.sol_wallet = sol_wallet
        self.api_public = api_public
        self.agent_keypair = Keypair.from_base58_string(api_private)
        self.slippage_bps = slippage_bps
        self.backup_poll_interval_seconds = backup_poll_interval_seconds

        # Rate limiting
        self.last_call_time = 0
        self.throttle_delay = 0.2  # 5 calls per second max

        # Market data cache
        self.market_info: Dict[str, Any] = {}
        self.market_info_decimal: Dict[str, Dict[str, Decimal]] = {}

        # WebSocket subscriptions
        self.price_subscriptions: Dict[str, Dict[str, Any]] = {}
        self.fill_subscription_id: Optional[str] = None
        self.fill_callback: Optional[Callable] = None
        self.position_update_callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None

        # Account state caches populated via WebSocket streams
        self._open_orders_cache: Dict[str, Dict[str, Any]] = {}
        self._open_orders_lock = Lock()
        self._account_info: Dict[str, Any] = {}
        self._account_info_lock = Lock()

        # Backup fill detection (REST API polling)
        self._backup_fill_enabled: bool = False  # Flag to control backup polling
        self._seen_fill_ids: set = set()  # Track fill IDs to prevent double-processing
        self._last_history_id: int = 0  # Track last seen history ID for incremental fetching
        self._fill_dedup_lock: asyncio.Lock = asyncio.Lock()  # Lock for atomic deduplication check

        # WebSocket trading (for fast order placement/cancellation)
        self._trading_ws = None
        self._trading_ws_lock = None  # Created lazily in the correct event loop
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._trading_ws_task = None
        self._fill_stream_task = None  # For tracking fill stream async task

        # WebSocket event loop
        self._ws_loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws_tasks: List[asyncio.Task] = []

        # Connection state
        self._connected = False

        # Connection health monitoring
        self._last_fill_message_time = 0.0
        self._last_price_message_time: Dict[str, float] = {}

        # Initialize market info
        self._load_market_info()

        self.logger.info(f"Pacifica connector initialized for {sol_wallet}")

    def _load_market_info(self):
        """Load market information from Pacifica API"""
        try:
            if not requests:
                raise ImportError("requests library not available")

            url = f"{REST_URL}/info"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                raise ValueError("API call to /info was not successful")

            markets = data.get("data", [])

            for market in markets:
                symbol = market.get("symbol")
                tick_size_raw = market.get("tick_size", 0.1)
                lot_size_raw = market.get("lot_size", 0.001)

                # Clean up tick size to avoid floating point errors
                tick_size_cleaned = round(float(tick_size_raw), 12)
                tick_size_dec = Decimal(str(tick_size_cleaned))
                if tick_size_dec <= 0:
                    tick_size_dec = Decimal('0.000001')

                lot_size_dec = Decimal(str(lot_size_raw))
                if lot_size_dec <= 0:
                    lot_size_dec = Decimal('0.001')

                self.market_info[symbol] = {
                    "tick_size": float(tick_size_dec),
                    "lot_size": float(lot_size_dec),
                    "min_notional": float(market.get("min_notional", 10.0)),
                    "max_leverage": int(market.get("max_leverage", 20)),
                    "maker_fee": float(market.get("maker_fee", 0.00015)),  # +1.5 bps (0.015%)
                    "taker_fee": float(market.get("taker_fee", 0.00040))   # +4.0 bps (0.040%)
                }

                self.market_info_decimal[symbol] = {
                    "tick_size_dec": tick_size_dec,
                    "lot_size_dec": lot_size_dec
                }

            if not self.market_info:
                raise ValueError("Failed to load any market info from API")

            self._connected = True
            self.logger.info(f"Loaded market info for {len(self.market_info)} symbols")

        except Exception as e:
            self.logger.error(f"Failed to load market info: {e}")
            raise ConnectionError(f"Failed to initialize Pacifica: {e}")

    # ==================== CONNECTION METHODS ====================

    def is_connected(self) -> bool:
        """
        Check if connection is healthy.

        Returns:
            True if connected to Pacifica API
        """
        if not self._connected:
            return False

        try:
            # Quick health check
            if not requests:
                return False
            response = requests.get(f"{REST_URL}/info", timeout=5)
            return response.status_code == 200
        except Exception as e:
            self.logger.warning(f"Connection check failed: {e}")
            return False

    def reconnect(self, max_retries: int = 3, initial_delay: float = 1.0) -> bool:
        """
        Attempt to reconnect to Pacifica.

        Args:
            max_retries: Maximum number of reconnection attempts
            initial_delay: Initial delay between retries

        Returns:
            True if reconnection successful
        """
        self.logger.warning("=" * 60)
        self.logger.warning("CONNECTION LOST - Attempting to reconnect...")
        self.logger.warning("=" * 60)

        # Store active subscriptions
        active_price_subs = list(self.price_subscriptions.items())
        was_fill_stream_active = (self.fill_subscription_id is not None)
        fill_callback_backup = self.fill_callback

        delay = initial_delay
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(f"Reconnection attempt {attempt}/{max_retries}...")
                self._connected = False

                # Clear old subscriptions
                self.price_subscriptions = {}
                self.fill_subscription_id = None
                self.fill_callback = None

                # Reload market info
                self._load_market_info()

                # Resubscribe to price streams
                if active_price_subs:
                    self.logger.info(f"Resubscribing to {len(active_price_subs)} price streams...")
                    for old_sub_id, sub_info in active_price_subs:
                        try:
                            symbol = sub_info["symbol"]
                            callback = sub_info["callback"]
                            new_sub_id = self.start_price_stream(symbol, callback)
                            self.logger.info(f"  [OK] Resubscribed price stream for {symbol}")
                        except Exception as e:
                            self.logger.error(f"  [FAILED] Failed to resubscribe price stream for {symbol}: {e}")

                # Resubscribe to fill stream
                if was_fill_stream_active and fill_callback_backup:
                    try:
                        self.logger.info("Resubscribing to fill stream...")
                        self.start_fill_stream(fill_callback_backup)
                        self.logger.info("  [OK] Fill stream resubscribed successfully")
                    except Exception as e:
                        self.logger.error(f"  [FAILED] Failed to resubscribe fill stream: {e}")
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
                    delay *= 2
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
            self.logger.info("Closing Pacifica connector...")

            # Stop fill stream if active
            if self.fill_subscription_id:
                self.stop_fill_stream(self.fill_subscription_id)

            # Clear price subscriptions
            self.price_subscriptions.clear()

            # Close trading WebSocket if active
            if self._trading_ws_task:
                self._trading_ws_task.cancel()
                self._trading_ws_task = None

            # Mark as disconnected
            self._connected = False

            self.logger.info("Pacifica connector closed successfully")

        except Exception as e:
            self.logger.error(f"Error closing Pacifica connector: {e}")

    # ==================== WEBSOCKET TRADING METHODS ====================

    def _trading_ws_is_open(self) -> bool:
        """
        Determine whether the trading WebSocket connection is currently usable.

        Works across websockets versions: newer releases expose ``state`` while older
        ones provided a ``closed`` attribute or callable.
        """
        ws = self._trading_ws
        if ws is None:
            return False

        state = getattr(ws, "state", None)
        if state is not None:
            return state is State.OPEN

        closed_attr = getattr(ws, "closed", None)
        if closed_attr is None:
            return True  # Assume open when the transport doesn't expose state information

        if callable(closed_attr):
            try:
                return not closed_attr()
            except TypeError:
                pass

        return not bool(closed_attr)

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
                self.logger.info("[TRADING WS] Establishing trading WebSocket connection...")

                # Connect with ping/pong heartbeat (ping every 30s, timeout after 30s)
                async with websockets.connect(
                    WS_URL,
                    ping_interval=30,
                    ping_timeout=30,
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
                request_id = data.get("id")
                if request_id and request_id in self._pending_requests:
                    future = self._pending_requests.pop(request_id)
                    if not future.done():
                        future.set_result(data)

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
            connector = PacificaConnector(sol_wallet, api_public, api_private)
            await connector.initialize_trading_websocket()  # Pre-connect
            # Now first market order will be fast (no connection overhead)
        """
        await self._ensure_trading_ws()
        self.logger.info("[TRADING WS] Pre-established trading WebSocket connection")

    async def _send_trading_request(self, request: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
        """
        Send a trading request via WebSocket and wait for response.

        Args:
            request: Request payload with 'id' and 'params'
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

    # ==================== MARKET DATA METHODS ====================

    @rate_limited()
    def get_bid_ask_rest(self, symbol: str) -> Dict[str, float]:
        """
        Get current bid/ask prices via REST API.

        Args:
            symbol: Trading symbol (e.g., "BTC")

        Returns:
            {
                "bid": float,
                "ask": float,
                "mid": float,
                "timestamp": float
            }
        """
        try:
            if not requests:
                raise ImportError("requests library not available")

            # Correct endpoint is /book, not /orderbook
            url = f"{REST_URL}/book"
            params = {"symbol": symbol, "agg_level": 1}
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                raise MarketDataError(f"Failed to fetch orderbook for {symbol}")

            orderbook = data.get("data", {})
            # Format: {"s": "BTC", "l": [[bids], [asks]], "t": timestamp}
            # Each level: {"p": price, "a": amount, "n": num_orders}
            levels = orderbook.get("l", [[], []])
            bids = levels[0] if len(levels) > 0 else []
            asks = levels[1] if len(levels) > 1 else []

            if not bids or not asks:
                raise MarketDataError(f"Empty orderbook for {symbol}")

            # Parse best bid/ask from format: {"p": "106504", "a": "0.26203", "n": 1}
            best_bid = float(bids[0].get("p", 0))
            best_ask = float(asks[0].get("p", 0))
            mid = (best_bid + best_ask) / 2

            return {
                "bid": best_bid,
                "ask": best_ask,
                "mid": mid,
                "timestamp": time.time()
            }

        except Exception as e:
            self.logger.error(f"Error fetching bid/ask for {symbol}: {e}")
            raise MarketDataError(f"Failed to fetch bid/ask: {e}")

    async def get_bid_ask_ws(self, symbol: str, timeout: float = 10.0) -> Dict[str, float]:
        """
        Get current bid/ask prices via WebSocket.

        Args:
            symbol: Trading symbol
            timeout: Timeout in seconds

        Returns:
            {"bid": float, "ask": float, "mid": float, "timestamp": float}
        """
        try:
            import asyncio
            import websockets
            import json

            self.logger.debug(f"Connecting to WebSocket: {WS_URL}")
            # Add ping/pong heartbeat for connection health
            async with websockets.connect(
                WS_URL,
                ping_interval=30,
                ping_timeout=30,
                close_timeout=5
            ) as ws:
                self.logger.debug(f"WebSocket connected for {symbol}")

                # Subscribe to orderbook (include agg_level parameter!)
                subscribe_msg = {
                    "method": "subscribe",
                    "params": {
                        "source": "book",
                        "symbol": symbol,
                        "agg_level": 1
                    }
                }
                await ws.send(json.dumps(subscribe_msg))
                self.logger.debug(f"Sent subscribe message: {subscribe_msg}")

                # Wait for orderbook data
                start_time = time.time()
                msg_count = 0
                while time.time() - start_time < timeout:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        data = json.loads(msg)
                        msg_count += 1

                        # Debug: log what we receive
                        self.logger.debug(f"Received WS message #{msg_count}: channel={data.get('channel')}, keys={list(data.keys())}")

                        if data.get("channel") == "book":
                            book_data = data.get("data", {})
                            symbol_received = book_data.get("s", "")

                            if symbol_received == symbol:
                                levels = book_data.get("l", [[], []])
                                bids = levels[0] if len(levels) > 0 else []
                                asks = levels[1] if len(levels) > 1 else []

                                if bids and asks:
                                    # Format: {"p": price, "a": amount}
                                    best_bid = float(bids[0].get("p", 0))
                                    best_ask = float(asks[0].get("p", 0))
                                    mid = (best_bid + best_ask) / 2

                                    return {
                                        "bid": best_bid,
                                        "ask": best_ask,
                                        "mid": mid,
                                        "timestamp": time.time()
                                    }

                    except asyncio.TimeoutError:
                        continue

                raise TimeoutError(f"Timeout waiting for {symbol} orderbook data")

        except Exception as e:
            self.logger.error(f"Error fetching bid/ask via WebSocket for {symbol}: {e}")
            raise MarketDataError(f"Failed to fetch bid/ask via WebSocket: {e}")

    def start_price_stream(self, symbol: str, callback: Callable) -> str:
        """
        Start continuous WebSocket price updates.

        Args:
            symbol: Trading symbol
            callback: Function called on each price update
                     Signature: callback(price_data: Dict[str, float])

        Returns:
            subscription_id: String ID for later unsubscription
        """
        subscription_id = f"price_{symbol}_{uuid.uuid4().hex[:8]}"

        # Create async task for WebSocket connection
        async def price_stream_task():
            """Async task to maintain WebSocket connection for prices with auto-reconnect"""
            reconnect_delay = 1.0
            max_reconnect_delay = 30.0

            while subscription_id in self.price_subscriptions:
                try:
                    # Add ping/pong heartbeat (ping every 30s, timeout after 30s)
                    async with websockets.connect(
                        WS_URL,
                        ping_interval=30,
                        ping_timeout=30,
                        close_timeout=5
                    ) as ws:
                        # Subscribe to orderbook
                        subscribe_msg = {
                            "method": "subscribe",
                            "params": {
                                "source": "book",
                                "symbol": symbol,
                                "agg_level": 1
                            }
                        }
                        await ws.send(json.dumps(subscribe_msg))
                        self.logger.info(f"Subscribed to Pacifica price stream for {symbol}")

                        # Reset reconnection delay on successful connection
                        reconnect_delay = 1.0

                        # Listen for price updates
                        while subscription_id in self.price_subscriptions:
                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
                                data = json.loads(msg)

                                # Update last message timestamp for health monitoring
                                self._last_price_message_time[symbol] = time.time()

                                if data.get("channel") == "book":
                                    book_data = data.get("data", {})
                                    symbol_received = book_data.get("s", "")

                                    if symbol_received == symbol:
                                        levels = book_data.get("l", [[], []])
                                        bids = levels[0] if len(levels) > 0 else []
                                        asks = levels[1] if len(levels) > 1 else []

                                        if bids and asks:
                                            # Format: {"p": price, "a": amount}
                                            best_bid = float(bids[0].get("p", 0))
                                            best_ask = float(asks[0].get("p", 0))
                                            mid = (best_bid + best_ask) / 2

                                            price_data = {
                                                "bid": best_bid,
                                                "ask": best_ask,
                                                "mid": mid,
                                                "timestamp": time.time()
                                            }

                                            # Call user callback
                                            callback(price_data)

                            except asyncio.TimeoutError:
                                # No message received in 30s, check if still subscribed
                                if subscription_id not in self.price_subscriptions:
                                    break
                            except websockets.exceptions.ConnectionClosed as e:
                                self.logger.warning(f"[PRICE STREAM {symbol}] Connection closed: {e}")
                                break  # Exit inner loop to trigger reconnect
                            except Exception as e:
                                self.logger.error(f"Error processing price update for {symbol}: {e}")

                except websockets.exceptions.ConnectionClosed as e:
                    self.logger.warning(f"[PRICE STREAM {symbol}] Connection closed: {e}. Reconnecting in {reconnect_delay:.1f}s...")
                except Exception as e:
                    self.logger.error(f"[PRICE STREAM {symbol}] WebSocket error: {e}")

                # Check if still subscribed before reconnecting
                if subscription_id not in self.price_subscriptions:
                    self.logger.info(f"[PRICE STREAM {symbol}] Subscription cancelled, exiting...")
                    break

                # Wait before reconnecting (exponential backoff)
                self.logger.info(f"[PRICE STREAM {symbol}] Reconnecting in {reconnect_delay:.1f}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

        # Run the async task in a background thread
        import threading

        def run_price_stream():
            """Run price stream in background thread"""
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(price_stream_task())
            except Exception as e:
                self.logger.error(f"Price stream thread error for {symbol}: {e}")

        price_thread = threading.Thread(target=run_price_stream, daemon=True)
        price_thread.start()

        # Store subscription
        self.price_subscriptions[subscription_id] = {
            "symbol": symbol,
            "callback": callback,
            "thread": price_thread
        }

        self.logger.info(f"Started price stream for {symbol} (subscription: {subscription_id})")
        return subscription_id

    def stop_price_stream(self, subscription_id: str) -> bool:
        """
        Stop price stream subscription.

        Args:
            subscription_id: ID returned by start_price_stream()

        Returns:
            True if successfully unsubscribed
        """
        try:
            if subscription_id not in self.price_subscriptions:
                self.logger.warning(f"Price subscription {subscription_id} not found")
                return False

            # Remove from tracking (this will stop the loop in the background task)
            sub_info = self.price_subscriptions.pop(subscription_id)
            self.logger.info(f"Stopped price stream for {sub_info['symbol']} (subscription: {subscription_id})")

            return True

        except Exception as e:
            self.logger.error(f"Error stopping price stream {subscription_id}: {e}")
            return False

    # ==================== ORDER EXECUTION METHODS ====================

    async def place_limit_order_ws_async(self, symbol: str, side: OrderSide, quantity: float, price: float,
                                         reduce_only: bool = False, post_only: bool = True) -> Dict[str, Any]:
        """
        Place limit order via WebSocket (faster than REST).

        Args:
            symbol: Trading symbol
            side: OrderSide.BUY or OrderSide.SELL
            quantity: Order quantity
            price: Limit price
            reduce_only: If True, order can only reduce position
            post_only: If True, uses ALO for maker-only orders

        Returns:
            Order result dictionary
        """
        try:
            # Round to exchange precision
            rounded_price = self.round_price(price, symbol, side)
            rounded_quantity = self.round_quantity(quantity, symbol)

            # Convert side to Pacifica format
            pacifica_side = "bid" if side == OrderSide.BUY else "ask"

            # Generate client order ID and request ID
            client_order_id = str(uuid.uuid4())
            request_id = str(uuid.uuid4())

            # Create signature
            timestamp = int(time.time() * 1000)
            signature_header = {
                "timestamp": timestamp,
                "expiry_window": 5000,
                "type": "create_order"
            }

            # Use ALO for post-only, GTC otherwise
            tif = "ALO" if post_only else "GTC"

            signature_payload = {
                "symbol": symbol,
                "price": str(rounded_price),
                "amount": str(rounded_quantity),
                "side": pacifica_side,
                "tif": tif,
                "client_order_id": client_order_id,
                "reduce_only": reduce_only
            }

            _message, signature = sign_message(signature_header, signature_payload, self.agent_keypair)

            # Build WebSocket request
            ws_request = {
                "id": request_id,
                "params": {
                    "create_order": {
                        "account": self.sol_wallet,
                        "agent_wallet": self.api_public,
                        "signature": signature,
                        "timestamp": timestamp,
                        "expiry_window": 5000,
                        **signature_payload
                    }
                }
            }

            start_time = time.time()
            self.logger.info(f"[WS] Placing limit order: {side} {rounded_quantity} {symbol} @ {rounded_price}")

            # Send via WebSocket
            response = await self._send_trading_request(ws_request, timeout=5.0)
            latency_ms = (time.time() - start_time) * 1000

            # Parse response
            if response.get("code") != 200:
                error = f"Order rejected: code {response.get('code')}"
                self.logger.error(f"[WS] {error}")
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
                    "error": error
                }

            order_data = response.get("data", {})
            order_id = order_data.get("i")  # Order ID

            result_data = {
                "success": True,
                "order_id": str(order_id) if order_id else None,
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
            self.logger.info(f"[WS] Limit order placed successfully: {order_id} ({latency_ms:.1f}ms)")
            return result_data

        except Exception as e:
            self.logger.error(f"[WS] Error placing limit order: {e}")
            raise

    async def cancel_order_ws_async(self, symbol: str, order_id: str = None, client_order_id: str = None) -> Dict[str, Any]:
        """
        Cancel order via WebSocket (faster than REST).

        Args:
            symbol: Trading symbol
            order_id: Exchange order ID (not used for Pacifica)
            client_order_id: Client order ID (REQUIRED for Pacifica)

        Returns:
            Cancellation result
        """
        try:
            if not order_id and not client_order_id:
                raise InvalidOrderError("Must provide either order_id or client_order_id for cancellation")

            identifier = order_id or client_order_id

            # Generate request ID
            request_id = str(uuid.uuid4())

            # Create signature
            timestamp = int(time.time() * 1000)
            signature_header = {
                "timestamp": timestamp,
                "expiry_window": 5000,
                "type": "cancel_order"
            }

            signature_payload = {
                "symbol": symbol
            }

            # Pacifica accepts numeric order_id (preferred) or client_order_id UUID.
            if order_id:
                try:
                    signature_payload["order_id"] = int(order_id)
                except (TypeError, ValueError):
                    signature_payload["order_id"] = order_id
            elif client_order_id:
                signature_payload["client_order_id"] = client_order_id

            _message, signature = sign_message(signature_header, signature_payload, self.agent_keypair)

            # Build WebSocket request
            ws_request = {
                "id": request_id,
                "params": {
                    "cancel_order": {
                        "account": self.sol_wallet,
                        "agent_wallet": self.api_public,
                        "signature": signature,
                        "timestamp": timestamp,
                        "expiry_window": 5000,
                        **signature_payload
                    }
                }
            }

            self.logger.info(f"[WS] Cancelling order: {symbol} order_id={identifier}")

            # Send via WebSocket
            response = await self._send_trading_request(ws_request, timeout=3.0)

            # Parse response
            code = response.get("code")
            if code == 200:
                return {
                    "success": True,
                    "order_id": identifier,
                    "status": "cancelled",
                    "error": None
                }
            if code == 420:
                self.logger.debug(
                    f"[WS] Cancel returned code 420 for {identifier}; assuming already cancelled"
                )
                return {
                    "success": True,
                    "order_id": identifier,
                    "status": "cancelled",
                    "error": None
                }

            error = f"Cancel failed: code {code}"
            self.logger.error(f"[WS] {error}")
            return {
                "success": False,
                "order_id": identifier,
                "status": "failed",
                "error": error
            }

        except Exception as e:
            self.logger.error(f"[WS] Error cancelling order: {e}")
            return {
                "success": False,
                "order_id": order_id or client_order_id,
                "status": "failed",
                "error": str(e)
            }

    @rate_limited()
    async def place_limit_order(self, symbol: str, side: OrderSide, quantity: float, price: float,
                               reduce_only: bool = False, post_only: bool = True) -> Dict[str, Any]:
        """
        Place a limit order (maker order).

        Tries WebSocket first (faster), falls back to REST if WebSocket fails.

        Args:
            symbol: Trading symbol
            side: OrderSide.BUY or OrderSide.SELL
            quantity: Order quantity
            price: Limit price
            reduce_only: If True, order can only reduce position
            post_only: If True, uses ALO for maker-only orders

        Returns:
            Order result dictionary
        """
        try:
            # Try WebSocket first (faster!)
            try:
                result = await self.place_limit_order_ws_async(
                    symbol, side, quantity, price, reduce_only, post_only
                )
                return result
            except Exception as ws_error:
                self.logger.warning(f"WebSocket order failed, falling back to REST: {ws_error}")
                # Fall through to REST API below

            # REST API fallback
            if not requests or not sign_message:
                raise ImportError("Pacifica SDK not available")

            # Round to exchange precision
            rounded_price = self.round_price(price, symbol, side)
            rounded_quantity = self.round_quantity(quantity, symbol)

            # Validate minimum notional
            specs = self.get_market_specs(symbol)
            notional = rounded_price * rounded_quantity
            if notional < specs["min_notional"]:
                raise InvalidOrderError(
                    f"Order notional ${notional:.2f} below minimum ${specs['min_notional']}"
                )

            # Convert side to Pacifica format
            pacifica_side = "bid" if side == OrderSide.BUY else "ask"

            # Generate client order ID
            client_order_id = str(uuid.uuid4())

            # Create signature
            timestamp = int(time.time() * 1000)
            signature_header = {
                "timestamp": timestamp,
                "expiry_window": 5000,
                "type": "create_order"
            }

            # Use ALO for post-only, GTC otherwise
            tif = "ALO" if post_only else "GTC"

            signature_payload = {
                "symbol": symbol,
                "price": str(rounded_price),
                "amount": str(rounded_quantity),
                "side": pacifica_side,
                "tif": tif,
                "client_order_id": client_order_id,
                "reduce_only": reduce_only
            }

            _message, signature = sign_message(signature_header, signature_payload, self.agent_keypair)

            # Build full payload
            full_payload = {
                "account": self.sol_wallet,
                "agent_wallet": self.api_public,
                "signature": signature,
                "timestamp": timestamp,
                "expiry_window": 5000,
                **signature_payload
            }

            # Place order
            start_time = time.time()
            self.logger.info(f"Placing limit order: {side} {rounded_quantity} {symbol} @ {rounded_price}")

            url = f"{REST_URL}/orders/create"
            response = requests.post(
                url,
                json=full_payload,
                timeout=30,
                headers={"Content-Type": "application/json"}
            )
            try:
                response.raise_for_status()
            except requests.HTTPError as http_err:
                error_detail = None
                try:
                    error_payload = response.json()
                    error_detail = error_payload.get("error") or error_payload
                except Exception:
                    error_detail = response.text

                if isinstance(error_detail, dict):
                    error_detail = (
                        error_detail.get("message")
                        or error_detail.get("detail")
                        or error_detail.get("error")
                        or str(error_detail)
                    )

                self.logger.error(
                    f"Market order rejected (HTTP {response.status_code}): {error_detail}"
                )

                raise InvalidOrderError(
                    f"HTTP {response.status_code} error while placing market order: {error_detail}"
                ) from http_err

            result = response.json()

            latency_ms = (time.time() - start_time) * 1000

            if not result.get("success"):
                error = result.get("error", "Unknown error")
                self.logger.error(f"Limit order rejected: {error}")

                # Check for specific errors
                if "insufficient" in error.lower() or "balance" in error.lower():
                    raise InsufficientBalanceError(f"Insufficient balance: {error}")

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
                    "error": error
                }

            order_id = result.get("data", {}).get("order_id")
            if not order_id:
                raise InvalidOrderError(f"No order_id in response: {result}")

            result_data = {
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
            self.logger.info(f"Limit order placed successfully: {order_id} ({latency_ms:.1f}ms)")
            return result_data

        except (AuthenticationError, InvalidOrderError, InsufficientBalanceError):
            raise
        except Exception as e:
            self.logger.error(f"Error placing limit order: {e}")
            raise InvalidOrderError(f"Failed to place limit order: {e}")

    @rate_limited()
    async def place_market_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        reduce_only: bool = False,
        slippage_bps: int = 50,
        prefer_rest: bool = False,
        **_: Any
    ) -> Dict[str, Any]:
        """
        Place a market order (taker order) for immediate execution.

        Args:
            symbol: Trading symbol
            side: OrderSide.BUY or OrderSide.SELL
            quantity: Order quantity
            reduce_only: If True, order can only reduce position
            slippage_bps: Maximum acceptable slippage

        Returns:
            Order result dictionary
        """
        try:
            if not requests or not sign_message:
                raise ImportError("Pacifica SDK not available")

            # Get current market price - use REST if prefer_rest=True, else try WebSocket with REST fallback
            if prefer_rest:
                # Use REST API directly for fresh, reliable price
                self.logger.debug(f"Using REST API for {symbol} price (prefer_rest=True)")
                price_data = self.get_bid_ask_rest(symbol)
                expected_price = price_data["ask"] if side == OrderSide.BUY else price_data["bid"]
            else:
                # Try WebSocket first, fallback to REST if failed
                try:
                    price_data = await self.get_bid_ask_ws(symbol, timeout=2.0)
                    expected_price = price_data["ask"] if side == OrderSide.BUY else price_data["bid"]
                except Exception as ws_error:
                    self.logger.warning(f"WebSocket price fetch failed for {symbol}, falling back to REST: {ws_error}")
                    # Fallback to REST API for fresh price
                    price_data = self.get_bid_ask_rest(symbol)
                    expected_price = price_data["ask"] if side == OrderSide.BUY else price_data["bid"]

            # Round quantity
            rounded_quantity = self.round_quantity(quantity, symbol)

            # Validate minimum notional (skip for reduce_only orders)
            specs = self.get_market_specs(symbol)
            notional = expected_price * rounded_quantity
            if not reduce_only and notional < specs["min_notional"]:
                raise InvalidOrderError(
                    f"Order notional ${notional:.2f} below minimum ${specs['min_notional']}"
                )

            # Convert side to Pacifica format
            pacifica_side = "bid" if side == OrderSide.BUY else "ask"

            # Generate client order ID
            client_order_id = str(uuid.uuid4())

            # Create signature
            timestamp = int(time.time() * 1000)
            signature_header = {
                "timestamp": timestamp,
                "expiry_window": 5000,
                "type": "create_market_order"
            }

            signature_payload = {
                "symbol": symbol,
                "amount": str(rounded_quantity),
                "side": pacifica_side,
                "client_order_id": client_order_id,
                "reduce_only": reduce_only,
                "slippage_percent": str(slippage_bps / 100)  # Convert bps to percent
            }

            _message, signature = sign_message(signature_header, signature_payload, self.agent_keypair)

            # Build full payload
            full_payload = {
                "account": self.sol_wallet,
                "agent_wallet": self.api_public,
                "signature": signature,
                "timestamp": timestamp,
                "expiry_window": 5000,
                **signature_payload
            }

            # Place order
            start_time = time.time()
            self.logger.info(f"Placing market order: {side} {rounded_quantity} {symbol} (expected: {expected_price})")

            url = f"{REST_URL}/orders/create_market"
            response = requests.post(
                url,
                json=full_payload,
                timeout=30,
                headers={"Content-Type": "application/json"}
            )
            try:
                response.raise_for_status()
            except requests.HTTPError as http_err:
                error_detail = None
                try:
                    error_payload = response.json()
                    error_detail = error_payload.get("error") or error_payload
                except Exception:
                    error_detail = response.text

                if isinstance(error_detail, dict):
                    error_detail = (
                        error_detail.get("message")
                        or error_detail.get("detail")
                        or error_detail.get("error")
                        or str(error_detail)
                    )

                self.logger.error(
                    f"Market order rejected (HTTP {response.status_code}): {error_detail}"
                )

                raise InvalidOrderError(
                    f"HTTP {response.status_code} error while placing market order: {error_detail}"
                ) from http_err

            result = response.json()

            latency_ms = (time.time() - start_time) * 1000

            if not result.get("success"):
                error = result.get("error", "Unknown error")
                self.logger.error(f"Market order rejected: {error}")

                if "insufficient" in error.lower() or "balance" in error.lower():
                    raise InsufficientBalanceError(f"Insufficient balance: {error}")

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
                    "slippage_bps": None,
                    "latency_ms": latency_ms,
                    "error": error
                }

            order_id = result.get("data", {}).get("order_id")
            fill_price = result.get("data", {}).get("fill_price", expected_price)

            # Calculate slippage
            if side == OrderSide.BUY:
                actual_slippage_bps = ((fill_price - expected_price) / expected_price) * 10000
            else:
                actual_slippage_bps = ((expected_price - fill_price) / expected_price) * 10000

            result_data = {
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
                "slippage_bps": actual_slippage_bps,
                "latency_ms": latency_ms,
                "error": None
            }
            self.logger.info(
                f"Market order filled: {order_id} @ {fill_price} "
                f"(slippage: {actual_slippage_bps:.2f}bps, latency: {latency_ms:.1f}ms)"
            )
            return result_data

        except (AuthenticationError, InvalidOrderError, InsufficientBalanceError):
            raise
        except Exception as e:
            self.logger.error(f"Error placing market order: {e}")
            raise InvalidOrderError(f"Failed to place market order: {e}")

    @rate_limited()
    async def cancel_order(self, symbol: str, order_id: str = None, client_order_id: str = None) -> Dict[str, Any]:
        """
        Cancel a specific order.

        Tries WebSocket first (faster), falls back to REST if WebSocket fails.

        Args:
            symbol: Trading symbol
            order_id: Exchange order ID (use client_order_id for Pacifica)
            client_order_id: Client order ID (preferred for Pacifica)

        Returns:
            Cancellation result
        """
        try:
            # Try WebSocket first (faster!)
            try:
                result = await self.cancel_order_ws_async(symbol, order_id, client_order_id)
                return result
            except Exception as ws_error:
                self.logger.warning(f"WebSocket cancel failed, falling back to REST: {ws_error}")
                # Fall through to REST API below

            # REST API fallback
            if not requests or not sign_message:
                raise ImportError("Pacifica SDK not available")

            if not order_id and not client_order_id:
                raise InvalidOrderError("Must provide either order_id or client_order_id")

            # Create signature
            timestamp = int(time.time() * 1000)
            signature_header = {
                "timestamp": timestamp,
                "expiry_window": 5000,
                "type": "cancel_order"
            }

            signature_payload = {
                "symbol": symbol
            }

            identifier = None
            if order_id:
                try:
                    signature_payload["order_id"] = int(order_id)
                    identifier = signature_payload["order_id"]
                except (TypeError, ValueError):
                    signature_payload["order_id"] = order_id
                    identifier = order_id

            if client_order_id:
                signature_payload["client_order_id"] = client_order_id
                identifier = identifier or client_order_id

            _message, signature = sign_message(signature_header, signature_payload, self.agent_keypair)

            # Build full payload
            full_payload = {
                "account": self.sol_wallet,
                "agent_wallet": self.api_public,
                "signature": signature,
                "timestamp": timestamp,
                "expiry_window": 5000,
                **signature_payload
            }

            self.logger.info(
                f"Cancelling order: {symbol} "
                f"order_id={signature_payload.get('order_id')} "
                f"client_order_id={signature_payload.get('client_order_id')}"
            )

            url = f"{REST_URL}/orders/cancel"
            response = requests.post(url, json=full_payload, timeout=30)
            try:
                response.raise_for_status()
            except requests.HTTPError as http_err:
                if response.status_code == 400 and "Failed to cancel" in response.text:
                    self.logger.info(
                        f"[REST] Cancel returned 400 for {identifier}; treating as already cancelled"
                    )
                else:
                    raise http_err

            return {
                "success": True,
                "order_id": identifier,
                "status": "cancelled",
                "error": None
            }

        except Exception as e:
            self.logger.error(f"Error cancelling order {identifier}: {e}")
            return {
                "success": False,
                "order_id": order_id or client_order_id,
                "status": "failed",
                "error": str(e)
            }

    @rate_limited()
    def cancel_all_orders(self, symbol: str = None) -> Dict[str, Any]:
        """
        Cancel all open orders (optionally for specific symbol).

        Args:
            symbol: Trading symbol. If None, cancels all orders.

        Returns:
            Cancellation summary
        """
        try:
            if not requests or not sign_message:
                raise ImportError("Pacifica SDK not available")

            timestamp = int(time.time() * 1000)
            signature_header = {
                "timestamp": timestamp,
                "expiry_window": 5000,
                "type": "cancel_all_orders"
            }

            signature_payload = {
                "all_symbols": symbol is None,
                "exclude_reduce_only": False
            }
            if symbol:
                signature_payload["symbol"] = symbol

            _message, signature = sign_message(signature_header, signature_payload, self.agent_keypair)

            request_header = {
                "account": self.sol_wallet,
                "agent_wallet": self.api_public,
                "signature": signature,
                "timestamp": signature_header["timestamp"],
                "expiry_window": signature_header["expiry_window"],
            }

            full_payload = {**request_header, **signature_payload}

            symbol_str = f" for {symbol}" if symbol else ""
            self.logger.info(f"Cancelling all orders{symbol_str}...")

            url = f"{REST_URL}/orders/cancel_all"
            response = requests.post(url, json=full_payload, timeout=30)
            response.raise_for_status()

            # Parse response to get counts
            result = response.json()
            cancelled_count = result.get("data", {}).get("cancelled_count", 0)

            return {
                "success": True,
                "cancelled_count": cancelled_count,
                "failed_count": 0,
                "error": None
            }

        except Exception as e:
            self.logger.error(f"Error in cancel_all_orders: {e}")
            return {
                "success": False,
                "cancelled_count": 0,
                "failed_count": 0,
                "error": str(e)
            }

    # ==================== POSITION & BALANCE METHODS ====================

    async def get_position_ws(self, symbol: str, timeout: float = 10.0) -> Dict[str, Any]:
        """
        Get current position via WebSocket.

        For now, falls back to REST API.
        """
        return await self.get_position_rest(symbol)

    def get_position(self, symbol: str) -> Dict[str, Any]:
        """
        Convenience method with automatic fallback (synchronous wrapper for async methods).

        NOTE: This method uses asyncio.run() which cannot be called from an async context.
        If calling from async code, use asyncio.to_thread(connector.get_position, symbol) or
        call get_position_ws()/get_position_rest() directly with await.

        Args:
            symbol: Trading symbol

        Returns:
            Position data (same format as get_position_ws())
        """
        try:
            return asyncio.run(self.get_position_ws(symbol, timeout=2.0))
        except (ConnectionError, TimeoutError) as e:
            self.logger.warning(f"WebSocket failed for {symbol} position, using REST: {e}")
            return asyncio.run(self.get_position_rest(symbol))

    @rate_limited()
    async def get_position_rest(self, symbol: str) -> Dict[str, Any]:
        """
        Get current position via REST API.

        Args:
            symbol: Trading symbol

        Returns:
            Position information dictionary
        """
        try:
            if not requests:
                raise ImportError("requests library not available")

            url = f"{REST_URL}/positions"
            params = {"account": self.sol_wallet}
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                raise MarketDataError("Failed to fetch positions")

            positions = data.get("data", [])

            # Find position for symbol
            for pos in positions:
                if pos.get("symbol") == symbol:
                    qty = float(pos.get("amount", 0))

                    # Pacifica uses "bid"/"ask" for position side
                    if pos.get("side") == "ask":
                        qty = -qty  # Short position is negative

                    entry_price = float(pos.get("entry_price", 0))

                    # Get current mark price via REST API
                    try:
                        price_data = self.get_bid_ask_rest(symbol)
                        mark_price = price_data["mid"]
                    except Exception:
                        mark_price = entry_price

                    # Calculate unrealized PnL
                    unrealized_pnl = 0.0
                    if entry_price > 0 and qty != 0:
                        unrealized_pnl = (mark_price - entry_price) * qty

                    return {
                        "symbol": symbol,
                        "quantity": qty,
                        "entry_price": entry_price,
                        "mark_price": mark_price,
                        "unrealized_pnl": unrealized_pnl,
                        "notional": abs(qty * mark_price),
                        "leverage": float(pos.get("leverage", 1.0)),
                        "liquidation_price": float(pos.get("liquidation_price")) if pos.get("liquidation_price") else None,
                        "timestamp": time.time()
                    }

            # No position found = flat
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

    def get_trade_history_rest(self, symbol: Optional[str] = None, since_history_id: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get trade history via REST API (backup fill detection).

        Args:
            symbol: Trading symbol (optional, gets all if None)
            since_history_id: Only return trades after this history ID
            limit: Maximum number of records to return

        Returns:
            List of trade records
        """
        try:
            if not requests:
                raise ImportError("requests library not available")

            url = f"{REST_URL}/positions/history"
            params = {
                "account": self.sol_wallet,
                "limit": limit
            }

            if symbol:
                params["symbol"] = symbol

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                raise MarketDataError("Failed to fetch trade history")

            trades = data.get("data", [])

            # Filter by history_id if provided
            if since_history_id is not None:
                trades = [t for t in trades if t.get("history_id", 0) > since_history_id]

            return trades

        except Exception as e:
            self.logger.error(f"Error fetching trade history: {e}")
            raise MarketDataError(f"Failed to fetch trade history: {e}")

    @rate_limited()
    def get_equity(self) -> float:
        """
        Get total account equity (balance + unrealized PnL).

        Returns:
            Total equity in USD
        """
        try:
            with self._account_info_lock:
                cached_equity = self._account_info.get("equity")
                if cached_equity is not None:
                    return float(cached_equity)

            if not requests:
                raise ImportError("requests library not available")

            url = f"{REST_URL}/account"
            params = {"account": self.sol_wallet}
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                raise MarketDataError("Failed to fetch account data")

            account_data = data.get("data", {})
            balance = float(account_data.get("balance", 0))
            unrealized_pnl = float(account_data.get("unrealized_pnl", 0))

            equity = balance + unrealized_pnl

            with self._account_info_lock:
                self._account_info.update(
                    {
                        "equity": equity,
                        "balance": balance,
                        "unrealized_pnl": unrealized_pnl,
                        "timestamp": time.time(),
                    }
                )

            return equity

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
        """
        import time as time_module

        max_retries = 3
        base_delay = 1.0

        for attempt in range(max_retries):
            try:
                if asset.upper() == "USDC":
                    with self._account_info_lock:
                        cached_available = self._account_info.get("available_to_spend")
                        if cached_available is not None:
                            return max(0.0, float(cached_available))

                if not requests:
                    raise ImportError("requests library not available")

                url = f"{REST_URL}/account"
                params = {"account": self.sol_wallet}
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if not data.get("success"):
                    raise MarketDataError("Failed to fetch account data")

                account_data = data.get("data", {})

                # Pacifica returns available_to_spend
                available_balance = float(account_data.get("available_to_spend", 0))

                with self._account_info_lock:
                    self._account_info.update(
                        {
                            "available_to_spend": available_balance,
                            "balance": float(account_data.get("balance", 0)),
                            "equity": float(account_data.get("balance", 0)) + float(account_data.get("unrealized_pnl", 0)),
                            "timestamp": time.time(),
                        }
                    )

                return max(0.0, available_balance)

            except requests.exceptions.ConnectionError as e:
                # Connection error - retry with exponential backoff
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    self.logger.warning(
                        f"Connection error fetching balance (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time_module.sleep(delay)
                    continue
                else:
                    self.logger.error(f"Error fetching balance for {asset} after {max_retries} attempts: {e}")
                    raise MarketDataError(f"Failed to fetch balance: {e}")
            except requests.exceptions.Timeout as e:
                # Timeout - retry with exponential backoff
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    self.logger.warning(
                        f"Timeout fetching balance (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time_module.sleep(delay)
                    continue
                else:
                    self.logger.error(f"Error fetching balance for {asset} after {max_retries} attempts: {e}")
                    raise MarketDataError(f"Failed to fetch balance: {e}")
            except Exception as e:
                # Other errors - fail immediately (don't retry for rate limits, etc.)
                self.logger.error(f"Error fetching balance for {asset}: {e}")
                raise MarketDataError(f"Failed to fetch balance: {e}")

        # Should never reach here
        raise MarketDataError("Failed to fetch balance after all retries")

    @rate_limited()
    def update_leverage(
        self,
        symbol: str,
        leverage: int
    ) -> Dict[str, Any]:
        """
        Update leverage for a specific symbol.

        Args:
            symbol: Trading symbol (e.g., "BTC", "ETH")
            leverage: Leverage value (e.g., 1, 2, 5, 10)

        Returns:
            {
                "success": bool,
                "error": Optional[str]
            }

        Notes:
            - Pacifica leverage is always cross margin (no isolated option)
            - Exchange may reject if there's an open position
            - For open positions, you can only increase leverage, not decrease
        """
        try:
            if not requests or not sign_message:
                raise ImportError("Required libraries not available")

            # Prepare signature header
            timestamp = int(time.time() * 1000)
            signature_header = {
                "timestamp": timestamp,
                "expiry_window": 5000,
                "type": "update_leverage"
            }

            # Prepare signature payload
            signature_payload = {
                "symbol": symbol,
                "leverage": leverage
            }

            # Sign the message
            message, signature = sign_message(
                signature_header,
                signature_payload,
                self.agent_keypair
            )

            # Construct request
            request_data = {
                "account": self.sol_wallet,  # User's Solana wallet address
                "agent_wallet": self.api_public,  # Agent wallet for signing
                "signature": signature,
                "timestamp": timestamp,
                "expiry_window": 5000,
                **signature_payload
            }

            # Send POST request
            url = f"{REST_URL}/account/leverage"
            headers = {"Content-Type": "application/json"}
            response = requests.post(url, json=request_data, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Check response
            if data.get("success"):
                self.logger.info(f"Successfully updated {symbol} leverage to {leverage}x (cross)")
                return {
                    "success": True,
                    "error": None
                }
            else:
                error_msg = data.get("error", "Unknown error")
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

    @rate_limited()
    def get_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """
        Get all open orders (optionally filtered by symbol).

        Args:
            symbol: Trading symbol. If None, returns all open orders.

        Returns:
            List of open orders
        """
        try:
            with self._open_orders_lock:
                if self._open_orders_cache:
                    orders = [dict(value) for value in self._open_orders_cache.values()]
                else:
                    orders = None

            if orders is not None:
                if symbol:
                    symbol_upper = symbol.upper()
                    orders = [
                        order for order in orders
                        if (order.get("symbol") or "").upper() == symbol_upper
                    ]
                return orders

            if not requests:
                raise ImportError("requests library not available")

            url = f"{REST_URL}/orders"
            params = {"account": self.sol_wallet}
            if symbol:
                params["symbol"] = symbol

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                raise MarketDataError("Failed to fetch open orders")

            orders = data.get("data", [])

            open_orders = []
            cache_update: Dict[str, Dict[str, Any]] = {}

            def _to_float(value) -> float:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return 0.0

            for order in orders:
                # Skip cancelled, filled, or rejected orders - only include active orders
                order_status = order.get("status", "open")
                if order_status in ["cancelled", "filled", "rejected", "canceled"]:
                    continue

                order_id = str(order.get("order_id"))
                symbol_value = order.get("symbol", "")
                order_symbol = str(symbol_value).upper() if symbol_value is not None else ""

                # Robust field extraction across API variants
                side_raw = str(order.get("side") or "").lower()
                side_norm = "buy" if side_raw in {"bid", "buy"} else "sell"

                # Amounts: prefer explicit remaining/original keys when present
                orig_amount = _to_float(
                    order.get("amount")
                    or order.get("initial_amount")
                    or order.get("original_amount")
                    or order.get("orig_amount")
                )
                filled_amt = _to_float(
                    order.get("filled")
                    or order.get("filled_amount")
                    or order.get("executed_amount")
                )
                cancelled_amt = _to_float(
                    order.get("cancelled_amount")
                    or order.get("canceled_amount")
                    or order.get("cancelled")
                )
                remaining_amt = _to_float(
                    order.get("remaining_quantity")
                    or order.get("remaining")
                    or order.get("remaining_amount")
                )

                # If remaining not provided, compute it
                if remaining_amt <= 0 and (orig_amount > 0 or filled_amt > 0 or cancelled_amt > 0):
                    remaining_amt = max(orig_amount - filled_amt - cancelled_amt, 0.0)

                # If original not provided but we have remaining/filled, infer it
                if orig_amount <= 0 and (remaining_amt > 0 or filled_amt > 0 or cancelled_amt > 0):
                    orig_amount = remaining_amt + filled_amt + cancelled_amt

                price = _to_float(order.get("price"))

                try:
                    ts = float(order.get("timestamp", time.time() * 1000))
                except (TypeError, ValueError):
                    ts = time.time() * 1000

                order_data = {
                    "order_id": order_id,
                    "client_order_id": order.get("client_order_id", ""),
                    "symbol": order_symbol,
                    "side": side_norm,
                    "amount": orig_amount,
                    "quantity": orig_amount,
                    "filled": filled_amt,
                    "filled_quantity": filled_amt,
                    "cancelled_quantity": cancelled_amt,
                    "remaining_quantity": max(remaining_amt, 0.0),
                    "price": price,
                    "status": order_status,
                    "order_type": str(order.get("type") or "limit").lower(),
                    "timestamp": ts,  # milliseconds
                    "reduce_only": bool(order.get("reduce_only", False)),
                    "source": "rest",
                }

                open_orders.append(order_data)
                cache_update[order_id] = dict(order_data)

            with self._open_orders_lock:
                if cache_update:
                    self._open_orders_cache = cache_update
                else:
                    self._open_orders_cache.clear()

            return open_orders

        except Exception as e:
            self.logger.error(f"Error fetching open orders: {e}")
            raise MarketDataError(f"Failed to fetch open orders: {e}")

    @rate_limited()
    def refresh_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """
        Force refresh of open orders from REST API, bypassing cached snapshot.

        Args:
            symbol: Optional trading symbol filter.

        Returns:
            Fresh list of open orders.
        """
        # Clear cached orders so the next call hits REST
        with self._open_orders_lock:
            self._open_orders_cache.clear()

        # Fetch updated list (will repopulate cache)
        return self.get_open_orders(symbol=symbol)

    @rate_limited()
    def get_order_history_rest(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get order history via REST API.

        Args:
            limit: Maximum number of records to return

        Returns:
            List of order records with all statuses (filled, cancelled, rejected, etc.)
        """
        try:
            if not requests:
                raise ImportError("requests library not available")

            url = f"{REST_URL}/orders/history"
            params = {
                "account": self.sol_wallet,
                "limit": limit
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                raise MarketDataError("Failed to fetch order history")

            orders = data.get("data", [])
            return orders

        except Exception as e:
            self.logger.error(f"Error fetching order history: {e}")
            raise MarketDataError(f"Failed to fetch order history: {e}")

    # ==================== FILL STREAM ====================

    def _handle_account_positions_message(self, positions: Any) -> None:
        """
        Process account_positions websocket payload and emit normalized snapshots.
        """
        try:
            parsed_positions: List[Dict[str, Any]] = []
            timestamp_now = time.time()

            if isinstance(positions, list):
                for entry in positions:
                    if not isinstance(entry, dict):
                        continue

                    symbol_value = entry.get("s")
                    if symbol_value is None:
                        continue
                    symbol = str(symbol_value).upper()
                    if not symbol:
                        continue

                    side = (entry.get("d") or "").lower()
                    try:
                        qty = float(entry.get("a", 0))
                    except (TypeError, ValueError):
                        qty = 0.0

                    if side == "ask":
                        qty = -qty  # short positions reported as ask

                    try:
                        entry_price = float(entry.get("p", 0))
                    except (TypeError, ValueError):
                        entry_price = 0.0

                    try:
                        pos_timestamp = float(entry.get("t", timestamp_now * 1000)) / 1000
                    except (TypeError, ValueError):
                        pos_timestamp = timestamp_now

                    parsed_positions.append(
                        {
                            "symbol": symbol,
                            "quantity": qty,
                            "side": "long" if qty > 0 else "short" if qty < 0 else "flat",
                            "entry_price": entry_price,
                            "raw_side": side,
                            "margin": entry.get("m"),
                            "funding": entry.get("f"),
                            "is_isolated": bool(entry.get("i", False)),
                            "timestamp": pos_timestamp,
                            "raw": entry,
                        }
                    )
            elif positions is None:
                parsed_positions = []
            else:
                self.logger.debug(f"[ACCOUNT POSITIONS] Unexpected payload type: {type(positions)}")

            callback = self.position_update_callback
            if callback:
                try:
                    callback(parsed_positions)
                except Exception as cb_exc:
                    self.logger.warning(f"[ACCOUNT POSITIONS] Callback error: {cb_exc}", exc_info=True)

        except Exception as exc:
            self.logger.warning(f"[ACCOUNT POSITIONS] Failed to process positions payload: {exc}", exc_info=True)

    def _handle_account_info_message(self, info: Any) -> None:
        """Process account_info websocket payload and update cached balances."""
        if not isinstance(info, dict):
            self.logger.debug(f"[ACCOUNT INFO] Unexpected payload type: {type(info)}")
            return

        try:
            parsed: Dict[str, Any] = {}

            def _safe_float(value: Any) -> Optional[float]:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None

            parsed["equity"] = _safe_float(info.get("ae"))
            parsed["available_to_spend"] = _safe_float(info.get("as"))
            parsed["available_to_withdraw"] = _safe_float(info.get("aw"))
            parsed["balance"] = _safe_float(info.get("b"))
            parsed["total_margin_used"] = _safe_float(info.get("mu"))
            parsed["cross_margin"] = _safe_float(info.get("cm"))
            parsed["orders_count"] = info.get("oc")
            parsed["positions_count"] = info.get("pc")
            parsed["stop_count"] = info.get("sc")

            try:
                timestamp = float(info.get("t", time.time() * 1000)) / 1000.0
            except (TypeError, ValueError):
                timestamp = time.time()

            parsed["timestamp"] = timestamp

            with self._account_info_lock:
                self._account_info.update({k: v for k, v in parsed.items() if v is not None})

        except Exception as exc:
            self.logger.warning(f"[ACCOUNT INFO] Failed to process payload: {exc}", exc_info=True)

    def _handle_account_orders_message(self, orders: Any) -> None:
        """Process account_orders websocket payload and maintain open order cache."""
        try:
            if orders is None:
                with self._open_orders_lock:
                    self._open_orders_cache.clear()
                return

            if not isinstance(orders, list):
                self.logger.debug(f"[ACCOUNT ORDERS] Unexpected payload type: {type(orders)}")
                return

            if len(orders) == 0:
                with self._open_orders_lock:
                    self._open_orders_cache.clear()
                return

            with self._open_orders_lock:
                for entry in orders:
                    if not isinstance(entry, dict):
                        continue

                    order_id = entry.get("i")
                    if order_id is None:
                        continue
                    order_id = str(order_id)

                    status_raw = entry.get("os") or entry.get("status") or "open"
                    status = str(status_raw).lower()
                    event = str(entry.get("oe", "")).lower() if entry.get("oe") else ""

                    # Remove orders that are no longer active
                    if status in {"filled", "cancelled", "canceled", "closed", "expired"} or event in {"cancel", "stop_triggered"}:
                        if order_id in self._open_orders_cache:
                            self._open_orders_cache.pop(order_id, None)
                        continue

                    symbol_value = entry.get("s")
                    if symbol_value is None:
                        continue
                    symbol = str(symbol_value).upper()
                    if not symbol:
                        continue

                    side = "buy" if (entry.get("d") or "").lower() == "bid" else "sell"

                    def _float(value: Any) -> float:
                        try:
                            return float(value)
                        except (TypeError, ValueError):
                            return 0.0

                    quantity = _float(entry.get("a"))
                    filled_quantity = _float(entry.get("f"))
                    cancelled_quantity = _float(entry.get("c"))
                    price = _float(entry.get("p") or entry.get("ip"))
                    reduce_only = bool(entry.get("ro", False))
                    order_type = str(entry.get("ot") or "limit").lower()
                    client_order_id = entry.get("I") or entry.get("client_order_id", "")

                    try:
                        timestamp = float(entry.get("t", time.time() * 1000)) / 1000.0
                    except (TypeError, ValueError):
                        timestamp = time.time()

                    normalized = {
                        "order_id": order_id,
                        "client_order_id": str(client_order_id) if client_order_id is not None else "",
                        "symbol": symbol,
                        "side": side,
                        "amount": quantity,  # Add 'amount' for dashboard compatibility
                        "quantity": quantity,
                        "filled": filled_quantity,  # Add 'filled' for dashboard compatibility
                        "filled_quantity": filled_quantity,
                        "cancelled_quantity": cancelled_quantity,
                        "price": price,
                        "status": status,
                        "order_type": order_type,
                        "reduce_only": reduce_only,
                        "timestamp": timestamp,
                        "remaining_quantity": max(quantity - filled_quantity - cancelled_quantity, 0.0),
                        "source": "websocket",
                    }

                    self._open_orders_cache[order_id] = normalized

        except Exception as exc:
            self.logger.warning(f"[ACCOUNT ORDERS] Failed to process payload: {exc}", exc_info=True)

    def start_fill_stream(
        self,
        callback: Callable,
        positions_callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    ) -> str:
        """
        Start WebSocket stream for fill events (CRITICAL for immediate hedging).

        Args:
            callback: Function called on each fill event
                      Signature: callback(fill_data: Dict[str, Any])
            positions_callback: Optional callback invoked with latest account position snapshots
                      Signature: callback(positions: List[Dict[str, Any]])

        Returns:
            subscription_id: String ID for later unsubscription

        Implementation Notes:
            - CRITICAL: This must fire immediately when orders fill
            - Used to trigger immediate hedging on the other exchange
            - Pacifica uses WebSocket for real-time fill notifications
            - Only ONE fill stream per connector
        """
        # Only allow one fill stream at a time
        if self.fill_subscription_id:
            self.logger.warning("Fill stream already active, stopping old stream")
            self.stop_fill_stream(self.fill_subscription_id)

        try:
            if not websockets:
                raise ImportError("websockets library not available")

            subscription_id = f"fill_{uuid.uuid4().hex[:8]}"

            # Initialize backup fill detection state to avoid processing historical fills
            # Fetch recent trades to get the current latest history ID
            try:
                recent_trades = self.get_trade_history_rest(since_history_id=0, limit=50)
                if recent_trades:
                    # Get the latest history ID
                    latest_history_id = max(trade.get("history_id", 0) for trade in recent_trades)
                    self._last_history_id = latest_history_id

                    # Mark recent fills as "seen" to prevent duplicate processing
                    for trade in recent_trades:
                        history_id = trade.get("history_id", 0)
                        if history_id:
                            self._seen_fill_ids.add(str(history_id))

                    self.logger.info(f"[BACKUP INIT] Initialized from history_id={latest_history_id}, marked {len(recent_trades)} recent fills as seen")
                else:
                    self.logger.warning("[BACKUP INIT] No recent trades found, starting from 0")
                    self._last_history_id = 0
                    self._seen_fill_ids.clear()
            except Exception as e:
                self.logger.warning(f"[BACKUP INIT] Failed to initialize backup state: {e}, starting from 0")
                self._last_history_id = 0
                self._seen_fill_ids.clear()

            # Create async task for backup fill polling
            async def backup_fill_polling():
                """Async task to poll REST API for missed fills"""
                poll_interval = self.backup_poll_interval_seconds  # Configurable poll interval

                while self._backup_fill_enabled:
                    try:
                        # Fetch recent trades
                        trades = self.get_trade_history_rest(
                            since_history_id=self._last_history_id,
                            limit=50
                        )

                        # Process new fills
                        for trade in reversed(trades):  # Process oldest first
                            history_id = trade.get("history_id", 0)
                            fill_id = str(history_id)

                            # Atomic deduplication check with lock
                            async with self._fill_dedup_lock:
                                # Skip if already seen (via WebSocket)
                                if fill_id in self._seen_fill_ids:
                                    continue

                                # Mark as seen
                                self._seen_fill_ids.add(fill_id)

                                # Update last history ID
                                if history_id > self._last_history_id:
                                    self._last_history_id = history_id

                                # Parse fill data (same format as WebSocket)
                                fill_data = {
                                    "order_id": str(trade.get("order_id")),
                                    "client_order_id": trade.get("client_order_id", ""),
                                    "symbol": trade.get("symbol"),
                                    "side": "buy" if "long" in trade.get("side", "") else "sell",
                                    "filled_quantity": float(trade.get("amount", 0)),
                                    "fill_price": float(trade.get("price", 0)),
                                    "fee": float(trade.get("fee", 0)),
                                    "is_maker": trade.get("event_type") == "fulfill_maker",
                                    "timestamp": trade.get("created_at", 0) / 1000.0,  # Convert ms to s
                                    "fill_id": fill_id,
                                    "detection_method": "rest_backup",  # Mark as REST API detection
                                    "detection_timestamp": time.time()  # When we detected it
                                }

                                # Log backup detection
                                self.logger.warning(f"[BACKUP FILL DETECTED] {fill_data['side']} {fill_data['filled_quantity']} {fill_data['symbol']} @ {fill_data['fill_price']} (order: {fill_data['order_id']}, via REST)")

                                # Trigger callback if registered
                                if self.fill_callback:
                                    try:
                                        self.fill_callback(fill_data)
                                    except Exception as cb_err:
                                        self.logger.error(f"Fill callback error (backup): {cb_err}")

                        # Clean up old seen fill IDs (keep last 1000)
                        if len(self._seen_fill_ids) > 1000:
                            # Convert to list, sort, keep latest 500
                            sorted_ids = sorted(self._seen_fill_ids, key=lambda x: int(x) if x.isdigit() else 0)
                            self._seen_fill_ids = set(sorted_ids[-500:])

                    except Exception as e:
                        # Check for rate limit errors
                        if "429" in str(e) or "rate limit" in str(e).lower():
                            self.logger.error(f"[BACKUP POLLING] Rate limit hit! Increasing interval to 1.0s: {e}")
                            poll_interval = 1.0  # Fall back to slower polling
                        else:
                            self.logger.debug(f"Backup fill polling error: {e}")

                    # Wait before next poll
                    await asyncio.sleep(poll_interval)

            # Create async task for WebSocket connection
            async def fill_stream_task():
                """Async task to maintain WebSocket connection for fills with auto-reconnect"""
                reconnect_delay = 1.0
                max_reconnect_delay = 30.0

                # Enable backup polling
                self._backup_fill_enabled = True
                backup_task = asyncio.create_task(backup_fill_polling())

                try:
                    while subscription_id == self.fill_subscription_id:
                        try:
                            self.logger.info(f"[FILL STREAM] Connecting to WebSocket: {WS_URL}")
                            # Add ping/pong heartbeat (ping every 30s, timeout after 30s)
                            async with websockets.connect(
                                WS_URL,
                                ping_interval=30,
                                ping_timeout=30,
                                close_timeout=5
                            ) as ws:
                                self.logger.info(f"[FILL STREAM] WebSocket connected successfully")

                                subscription_requests = [
                                    ("account_trades", {"source": "account_trades", "account": self.sol_wallet}),
                                    ("account_positions", {"source": "account_positions", "account": self.sol_wallet}),
                                    ("account_orders", {"source": "account_orders", "account": self.sol_wallet}),
                                    ("account_info", {"source": "account_info", "account": self.sol_wallet}),
                                ]

                                for channel_name, params in subscription_requests:
                                    subscribe_msg = {"method": "subscribe", "params": params}
                                    msg_str = json.dumps(subscribe_msg)
                                    self.logger.debug(f"[FILL STREAM] Sending subscription for {channel_name}: {msg_str}")
                                    await ws.send(msg_str)
                                    self.logger.info(f"Subscribed to Pacifica {channel_name} stream for {self.sol_wallet}")

                                # Reset reconnection delay on successful connection
                                reconnect_delay = 1.0

                                # Listen for fill events
                                msg_count = 0
                                while subscription_id == self.fill_subscription_id:
                                    try:
                                        msg = await ws.recv()
                                        msg_count += 1

                                        # Update last message timestamp for health monitoring
                                        self._last_fill_message_time = time.time()

                                        # DEBUG: Log raw message
                                        self.logger.debug(f"[FILL STREAM] Raw message #{msg_count}: {msg[:500]}{'...' if len(msg) > 500 else ''}")

                                        try:
                                            data = json.loads(msg)
                                        except json.JSONDecodeError as e:
                                            self.logger.error(f"[FILL STREAM] Failed to parse JSON: {e}")
                                            continue

                                        # DEBUG: Log parsed message structure
                                        self.logger.debug(f"[FILL STREAM] Parsed message #{msg_count}:")
                                        self.logger.debug(f"  - Type: {type(data)}")
                                        self.logger.debug(f"  - Keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
                                        self.logger.debug(f"  - Channel: {data.get('channel') if isinstance(data, dict) else 'N/A'}")

                                        # Log full data structure for debugging
                                        if isinstance(data, dict):
                                            self.logger.debug(f"  - Full data: {json.dumps(data, indent=2)}")

                                        if isinstance(data, dict):
                                            channel = data.get("channel")
                                            if channel == "account_positions":
                                                self.logger.debug(f"[ACCOUNT POSITIONS] Message: {json.dumps(data)[:500]}")
                                                self._handle_account_positions_message(data.get("data"))
                                                continue
                                            if channel == "account_orders":
                                                self.logger.debug(f"[ACCOUNT ORDERS] Message: {json.dumps(data)[:500]}")
                                                self._handle_account_orders_message(data.get("data"))
                                                continue
                                            if channel == "account_info":
                                                self.logger.debug(f"[ACCOUNT INFO] Message: {json.dumps(data)[:500]}")
                                                self._handle_account_info_message(data.get("data"))
                                                continue

                                        if isinstance(data, dict) and data.get("channel") == "account_trades":
                                            self.logger.info(f"[FILL STREAM] Received 'account_trades' channel message!")
                                            trades = data.get("data", [])
                                            self.logger.info(f"[FILL STREAM] Number of trades in message: {len(trades)}")
                                            self.logger.debug(f"[FILL STREAM] Trades data type: {type(trades)}")

                                            if not isinstance(trades, list):
                                                self.logger.warning(f"[FILL STREAM] 'data' is not a list: {type(trades)}, value: {trades}")
                                                # Try to handle if it's a single trade object
                                                if isinstance(trades, dict):
                                                    trades = [trades]
                                                else:
                                                    continue

                                            for i, trade in enumerate(trades):
                                                self.logger.debug(f"[FILL STREAM] Processing trade #{i+1}/{len(trades)}")
                                                self.logger.debug(f"[FILL STREAM] Trade raw data: {json.dumps(trade, indent=2)}")

                                                # Parse trade data per Pacifica API docs
                                                # Field mapping from docs:
                                                # 'i': Order ID, 'I': Client order ID, 's': Symbol
                                                # 'p': Price, 'a': Trade amount, 'f': Trade fee
                                                # 'te': 'fulfill_maker' or 'fulfill_taker'
                                                # 'ts': Trade side (open_long, open_short, close_long, close_short)
                                                # 't': Timestamp in milliseconds

                                                trade_side = trade.get("ts", "")  # open_long, open_short, close_long, close_short
                                                # Convert to buy/sell based on Pacifica's trade side semantics:
                                                # - open_long = buy (opening a long position)
                                                # - close_long = sell (closing a long position)
                                                # - open_short = sell (opening a short position)
                                                # - close_short = buy (closing a short position)
                                                if trade_side == "open_long" or trade_side == "close_short":
                                                    side = "buy"
                                                elif trade_side == "open_short" or trade_side == "close_long":
                                                    side = "sell"
                                                else:
                                                    side = "unknown"

                                                fill_data = {
                                                    "order_id": str(trade.get("i", "")),
                                                    "client_order_id": trade.get("I", ""),
                                                    "symbol": trade.get("s", ""),
                                                    "side": side,
                                                    "filled_quantity": float(trade.get("a", 0)),
                                                    "fill_price": float(trade.get("p", 0)),
                                                    "fee": float(trade.get("f", 0)),
                                                    "is_maker": trade.get("te", "") == "fulfill_maker",
                                                    "timestamp": float(trade.get("t", time.time() * 1000)) / 1000,
                                                    "fill_id": str(trade.get("h", "")),  # History ID
                                                    "detection_method": "websocket",  # Mark as WebSocket detection
                                                    "detection_timestamp": time.time()  # When we detected it
                                                }

                                                self.logger.debug(f"[FILL STREAM] Parsed fill data: {json.dumps(fill_data, indent=2)}")

                                                # Atomic deduplication check with lock
                                                fill_id = fill_data.get("fill_id", "")
                                                async with self._fill_dedup_lock:
                                                    # Skip if already seen (from REST backup)
                                                    if fill_id in self._seen_fill_ids:
                                                        self.logger.debug(f"[FILL STREAM] Skipping duplicate fill {fill_id} (already processed by REST backup)")
                                                        continue

                                                    # Mark as seen to prevent double-processing by backup polling
                                                    if fill_id:
                                                        self._seen_fill_ids.add(fill_id)
                                                        history_id = int(fill_id) if fill_id.isdigit() else 0
                                                        if history_id > self._last_history_id:
                                                            self._last_history_id = history_id

                                                    # Log the fill
                                                    self.logger.info(
                                                        f"FILL: {fill_data['side']} {fill_data['filled_quantity']} {fill_data['symbol']} "
                                                        f"@ {fill_data['fill_price']} (order: {fill_data['order_id']}, "
                                                        f"{'maker' if fill_data['is_maker'] else 'taker'})"
                                                    )

                                                    # Call user callback (use self.fill_callback to allow dynamic updates)
                                                    if self.fill_callback:
                                                        self.logger.debug(f"[FILL STREAM] Calling fill callback with data")
                                                        self.fill_callback(fill_data)
                                                        self.logger.debug(f"[FILL STREAM] Fill callback completed")
                                                    else:
                                                        self.logger.warning(f"[FILL STREAM] No fill callback registered!")
                                        else:
                                            # Log any non-trade messages for debugging
                                            if isinstance(data, dict):
                                                channel = data.get("channel", "unknown")
                                                if channel not in {"account_trades", "account_positions", "account_orders", "account_info"}:
                                                    self.logger.debug(f"[FILL STREAM] Non-trade message: channel='{channel}'")

                                    except websockets.exceptions.ConnectionClosed as e:
                                        self.logger.warning(f"[FILL STREAM] WebSocket connection closed: {e}")
                                        break  # Exit inner loop to trigger reconnect
                                    except Exception as e:
                                        self.logger.error(f"[FILL STREAM] Error processing fill event: {e}")
                                        import traceback
                                        self.logger.error(f"[FILL STREAM] Traceback: {traceback.format_exc()}")

                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.warning(f"[FILL STREAM] Connection closed: {e}. Reconnecting in {reconnect_delay:.1f}s...")
                        except Exception as e:
                            self.logger.error(f"[FILL STREAM] WebSocket error: {e}")
                            import traceback
                            self.logger.error(f"[FILL STREAM] Traceback: {traceback.format_exc()}")

                        # Check if still subscribed before reconnecting
                        if subscription_id != self.fill_subscription_id:
                            self.logger.info("[FILL STREAM] Subscription cancelled, exiting...")
                            break

                        # Wait before reconnecting (exponential backoff)
                        self.logger.info(f"[FILL STREAM] Reconnecting in {reconnect_delay:.1f}s...")
                        await asyncio.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                finally:
                    # Stop backup polling when fill stream ends
                    self._backup_fill_enabled = False
                    backup_task.cancel()
                    try:
                        await backup_task
                    except asyncio.CancelledError:
                        pass

            # Store subscription
            self.fill_subscription_id = subscription_id
            self.fill_callback = callback
            self.position_update_callback = positions_callback

            # Start fill stream task in the background using asyncio
            # Try to get the current running event loop
            try:
                loop = asyncio.get_running_loop()
                # Schedule the task on the running loop
                self._fill_stream_task = loop.create_task(fill_stream_task())
            except RuntimeError:
                # No running loop, create a new one and run in thread
                # This is necessary when called from sync context without an event loop
                import threading as _threading

                def run_fill_stream():
                    """Run fill stream in background thread with new event loop"""
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(fill_stream_task())
                    except Exception as e:
                        self.logger.error(f"Fill stream error: {e}")
                    finally:
                        loop.close()

                fill_thread = _threading.Thread(target=run_fill_stream, daemon=True)
                fill_thread.start()
                self._fill_stream_task = None  # No task reference when using thread

            self.logger.info(f"Started fill stream (subscription: {subscription_id})")
            self.logger.warning("CRITICAL: Fill stream active - ready for immediate hedging")
            req_per_sec = 1.0 / self.backup_poll_interval_seconds if self.backup_poll_interval_seconds > 0 else 0
            self.logger.warning(f"BACKUP: REST API fill polling active ({self.backup_poll_interval_seconds:.1f}s interval, ~{req_per_sec:.1f} req/s)")

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
            WebSocket connection will be closed automatically when thread exits.
        """
        try:
            if subscription_id != self.fill_subscription_id:
                self.logger.warning(f"Fill subscription {subscription_id} not found")
                return False

            # Clear tracking (backup polling will stop automatically via _backup_fill_enabled flag)
            self._backup_fill_enabled = False
            self.fill_subscription_id = None
            self.fill_callback = None
            self.position_update_callback = None

            with self._open_orders_lock:
                self._open_orders_cache.clear()
            with self._account_info_lock:
                self._account_info.clear()

            self.logger.info(f"Stopped fill stream (subscription: {subscription_id})")

            return True

        except Exception as e:
            self.logger.error(f"Error stopping fill stream {subscription_id}: {e}")
            return False

    def subscribe_account_positions(self, callback: Callable[[List[Dict[str, Any]]], None]) -> str:
        """
        Subscribe to account position updates via WebSocket (for dashboard).

        Args:
            callback: Function to call with position data (list of position dicts)

        Returns:
            Subscription ID string

        Notes:
            - Provides real-time position updates
            - Callback receives list of positions in account_positions format
        """
        import websockets
        import asyncio
        import threading

        subscription_id = f"positions_{int(time.time() * 1000)}"

        def ws_task():
            async def run_subscription():
                while True:
                    try:
                        async with websockets.connect(
                            WS_URL,
                            extra_headers={"Accept": "*/*"}
                        ) as ws:
                            # Subscribe to account positions
                            subscribe_msg = {
                                "method": "subscribe",
                                "params": {
                                    "source": "account_positions",
                                    "account": self.sol_wallet
                                }
                            }
                            await ws.send(json.dumps(subscribe_msg))

                            while True:
                                msg = await ws.recv()
                                data = json.loads(msg)

                                if data.get("channel") == "account_positions":
                                    positions_data = data.get("data", [])
                                    callback(positions_data)

                    except Exception as e:
                        self.logger.error(f"Position subscription error: {e}")
                        await asyncio.sleep(5)  # Reconnect after 5s

            asyncio.run(run_subscription())

        thread = threading.Thread(target=ws_task, daemon=True)
        thread.start()

        return subscription_id

    def subscribe_account_info(self, callback: Callable[[Dict[str, Any]], None]) -> str:
        """
        Subscribe to account info updates via WebSocket (for dashboard).

        Args:
            callback: Function to call with account info data

        Returns:
            Subscription ID string

        Notes:
            - Provides real-time balance and margin updates
            - Callback receives account_info format
        """
        import websockets
        import asyncio
        import threading

        subscription_id = f"info_{int(time.time() * 1000)}"

        def ws_task():
            async def run_subscription():
                while True:
                    try:
                        async with websockets.connect(
                            WS_URL,
                            extra_headers={"Accept": "*/*"}
                        ) as ws:
                            # Subscribe to account info
                            subscribe_msg = {
                                "method": "subscribe",
                                "params": {
                                    "source": "account_info",
                                    "account": self.sol_wallet
                                }
                            }
                            await ws.send(json.dumps(subscribe_msg))

                            while True:
                                msg = await ws.recv()
                                data = json.loads(msg)

                                if data.get("channel") == "account_info":
                                    info_data = data.get("data", {})
                                    callback(info_data)

                    except Exception as e:
                        self.logger.error(f"Account info subscription error: {e}")
                        await asyncio.sleep(5)  # Reconnect after 5s

            asyncio.run(run_subscription())

        thread = threading.Thread(target=ws_task, daemon=True)
        thread.start()

        return subscription_id

    def subscribe_account_orders(self, callback: Callable[[List[Dict[str, Any]]], None]) -> str:
        """
        Subscribe to account order updates via WebSocket (for dashboard).

        Args:
            callback: Function to call with order data (list of order dicts)

        Returns:
            Subscription ID string

        Notes:
            - Provides real-time order updates
            - Callback receives list of orders in account_orders format
        """
        import websockets
        import asyncio
        import threading

        subscription_id = f"orders_{int(time.time() * 1000)}"

        def ws_task():
            async def run_subscription():
                while True:
                    try:
                        async with websockets.connect(
                            WS_URL,
                            extra_headers={"Accept": "*/*"}
                        ) as ws:
                            # Subscribe to account orders
                            subscribe_msg = {
                                "method": "subscribe",
                                "params": {
                                    "source": "account_orders",
                                    "account": self.sol_wallet
                                }
                            }
                            await ws.send(json.dumps(subscribe_msg))

                            while True:
                                msg = await ws.recv()
                                data = json.loads(msg)

                                if data.get("channel") == "account_orders":
                                    orders_data = data.get("data", [])
                                    callback(orders_data)

                    except Exception as e:
                        self.logger.error(f"Orders subscription error: {e}")
                        await asyncio.sleep(5)  # Reconnect after 5s

            asyncio.run(run_subscription())

        thread = threading.Thread(target=ws_task, daemon=True)
        thread.start()

        return subscription_id

    def subscribe_account_trades(self, callback: Callable[[List[Dict[str, Any]]], None]) -> str:
        """
        Subscribe to account trade updates via WebSocket (for dashboard).

        Args:
            callback: Function to call with trade data (list of trade dicts)

        Returns:
            Subscription ID string

        Notes:
            - Provides real-time fill/trade notifications
            - Callback receives list of trades in account_trades format
        """
        import websockets
        import asyncio
        import threading

        subscription_id = f"trades_{int(time.time() * 1000)}"

        def ws_task():
            async def run_subscription():
                while True:
                    try:
                        async with websockets.connect(
                            WS_URL,
                            extra_headers={"Accept": "*/*"}
                        ) as ws:
                            # Subscribe to account trades
                            subscribe_msg = {
                                "method": "subscribe",
                                "params": {
                                    "source": "account_trades",
                                    "account": self.sol_wallet
                                }
                            }
                            await ws.send(json.dumps(subscribe_msg))

                            while True:
                                msg = await ws.recv()
                                data = json.loads(msg)

                                if data.get("channel") == "account_trades":
                                    trades_data = data.get("data", [])
                                    callback(trades_data)

                    except Exception as e:
                        self.logger.error(f"Trades subscription error: {e}")
                        await asyncio.sleep(5)  # Reconnect after 5s

            asyncio.run(run_subscription())

        thread = threading.Thread(target=ws_task, daemon=True)
        thread.start()

        return subscription_id

    def subscribe_account_order_updates(self, callback: Callable[[List[Dict[str, Any]]], None]) -> str:
        """
        Subscribe to account order update events via WebSocket (for dashboard).

        Args:
            callback: Function to call with order update data (list of order update dicts)

        Returns:
            Subscription ID string

        Notes:
            - Provides real-time order lifecycle events (placed, filled, cancelled, etc.)
            - Callback receives list of order updates in account_order_updates format
            - Includes order_event (oe) and order_status (os) fields
        """
        import websockets
        import asyncio
        import threading

        subscription_id = f"order_updates_{int(time.time() * 1000)}"

        def ws_task():
            async def run_subscription():
                while True:
                    try:
                        async with websockets.connect(
                            WS_URL,
                            extra_headers={"Accept": "*/*"}
                        ) as ws:
                            # Subscribe to account order updates
                            subscribe_msg = {
                                "method": "subscribe",
                                "params": {
                                    "source": "account_order_updates",
                                    "account": self.sol_wallet
                                }
                            }
                            await ws.send(json.dumps(subscribe_msg))

                            while True:
                                msg = await ws.recv()
                                data = json.loads(msg)

                                if data.get("channel") == "account_order_updates":
                                    updates_data = data.get("data", [])
                                    callback(updates_data)

                    except Exception as e:
                        self.logger.error(f"Order updates subscription error: {e}")
                        await asyncio.sleep(5)  # Reconnect after 5s

            asyncio.run(run_subscription())

        thread = threading.Thread(target=ws_task, daemon=True)
        thread.start()

        return subscription_id

    # ==================== UTILITY METHODS ====================

    def get_market_specs(self, symbol: str) -> Dict[str, Any]:
        """
        Get market specifications for a symbol.

        Args:
            symbol: Trading symbol (e.g., "BTC")

        Returns:
            Market specifications dictionary
        """
        if symbol not in self.market_info:
            raise InvalidSymbolError(f"Symbol not found: {symbol}")

        specs = self.market_info[symbol]
        return {
            "symbol": symbol,
            "tick_size": specs["tick_size"],
            "lot_size": specs["lot_size"],
            "min_notional": specs["min_notional"],
            "max_leverage": specs["max_leverage"],
            "maker_fee": specs["maker_fee"],
            "taker_fee": specs["taker_fee"]
        }

    def round_price(self, price: float, symbol: str, side: OrderSide) -> float:
        """
        Round price to valid tick size.

        Args:
            price: Raw price
            symbol: Trading symbol
            side: OrderSide.BUY (round down) or OrderSide.SELL (round up)

        Returns:
            Properly rounded price
        """
        if symbol not in self.market_info_decimal:
            raise InvalidSymbolError(f"Symbol not found: {symbol}")

        tick_size_dec = self.market_info_decimal[symbol]["tick_size_dec"]
        price_dec = Decimal(str(price))

        if side == OrderSide.BUY:
            # Round down for buy orders
            rounded = (price_dec / tick_size_dec).quantize(Decimal('1'), rounding=ROUND_DOWN) * tick_size_dec
        else:
            # Round up for sell orders
            rounded = (price_dec / tick_size_dec).quantize(Decimal('1'), rounding=ROUND_UP) * tick_size_dec

        return float(rounded)

    def round_quantity(self, quantity: float, symbol: str) -> float:
        """
        Round quantity to valid lot size.

        Args:
            quantity: Raw quantity
            symbol: Trading symbol

        Returns:
            Properly rounded quantity (always rounds down)
        """
        if symbol not in self.market_info_decimal:
            raise InvalidSymbolError(f"Symbol not found: {symbol}")

        lot_size_dec = self.market_info_decimal[symbol]["lot_size_dec"]
        quantity_dec = Decimal(str(quantity))

        # Always round down to avoid over-trading
        lots = (quantity_dec / lot_size_dec).quantize(Decimal('1'), rounding=ROUND_DOWN)
        rounded = lots * lot_size_dec

        return float(rounded)
