"""
XEMM Live Dashboard

Real-time monitoring dashboard showing:
- Account balances (Hyperliquid + Pacifica)
- Current positions (per symbol, per exchange)
- Net position summary
- Open orders
- Recent fills/trades

Uses REST API polling with rate limiting (respects Pacifica's 300 req/60s limit):
- HL: 15-second intervals for user state, orders, fills
- PAC: 15-second intervals for positions, balances, orders, fills
- PAC: 30-second intervals for order history

Usage:
    python -m utils.dashboard
    python -m utils.dashboard --config custom_config.json
"""

import asyncio
import argparse
import sys
import time
import logging
import os
from datetime import datetime, timezone
from collections import defaultdict, deque
from typing import Dict, Any, List, Optional, Deque, Tuple
from threading import Lock
from decimal import Decimal

# Completely disable all logging before any imports
logging.disable(logging.CRITICAL)

print("[Import] Starting imports...", file=sys.stderr)

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text

print("[Import] Rich library imported", file=sys.stderr)

# Temporarily suppress output for connector imports only
_original_stdout = sys.stdout
_original_stderr = sys.stderr
_devnull = open(os.devnull, 'w')
sys.stdout = _devnull
sys.stderr = _devnull

try:
    from connectors.hyperliquid_connector import HyperliquidConnector
    from connectors.pacifica_connector import PacificaConnector
    from utils.config import load_config, get_hyperliquid_credentials, get_pacifica_credentials
finally:
    # Restore stdout and stderr after imports
    _devnull.close()
    sys.stdout = _original_stdout
    sys.stderr = _original_stderr

print("[Import] All imports completed", file=sys.stderr)


class DashboardState:
    """Thread-safe container for dashboard data."""

    def __init__(self):
        self.lock = Lock()

        # Balances
        self.hl_balance: Optional[float] = None
        self.hl_equity: Optional[float] = None
        self.pac_balance: Optional[float] = None
        self.pac_equity: Optional[float] = None

        # Positions: {symbol: {'quantity': float, 'entry_price': float, 'unrealized_pnl': float, ...}}
        self.hl_positions: Dict[str, Dict[str, Any]] = {}
        self.pac_positions: Dict[str, Dict[str, Any]] = {}

        # Orders: {order_id: {'symbol': str, 'side': str, 'quantity': float, 'price': float, ...}}
        self.hl_orders: Dict[str, Dict[str, Any]] = {}
        self.pac_orders: Dict[str, Dict[str, Any]] = {}

        # Recent fills: deque of {timestamp, exchange, symbol, side, quantity, price, pnl}
        self.recent_fills: Deque[Dict[str, Any]] = deque(maxlen=10)

        # Recent orders: deque of {timestamp, exchange, symbol, side, quantity, status, order_id}
        self.recent_orders: Deque[Dict[str, Any]] = deque(maxlen=8)

        # Last update timestamps
        self.last_hl_update: Optional[datetime] = None
        self.last_pac_update: Optional[datetime] = None

        # Symbols being monitored
        self.symbols: List[str] = []

        # Dirty flag to track if data changed since last display
        self._dirty: bool = True

    def update_hl_balance(self, balance: float, equity: float):
        with self.lock:
            self.hl_balance = balance
            self.hl_equity = equity
            self.last_hl_update = datetime.now(timezone.utc)
            self._dirty = True

    def update_pac_balance(self, balance: float, equity: float):
        with self.lock:
            self.pac_balance = balance
            self.pac_equity = equity
            self.last_pac_update = datetime.now(timezone.utc)
            self._dirty = True

    def update_hl_positions(self, positions: Dict[str, Dict[str, Any]]):
        with self.lock:
            self.hl_positions = positions
            self.last_hl_update = datetime.now(timezone.utc)
            self._dirty = True

    def update_pac_positions(self, positions: Dict[str, Dict[str, Any]]):
        with self.lock:
            self.pac_positions = positions
            self.last_pac_update = datetime.now(timezone.utc)
            self._dirty = True

    def update_hl_orders(self, orders: Dict[str, Dict[str, Any]]):
        with self.lock:
            self.hl_orders = orders
            self.last_hl_update = datetime.now(timezone.utc)
            self._dirty = True

    def update_pac_orders(self, orders: Dict[str, Dict[str, Any]]):
        with self.lock:
            self.pac_orders = orders
            self.last_pac_update = datetime.now(timezone.utc)
            self._dirty = True

    def add_fill(self, exchange: str, symbol: str, side: str, quantity: float,
                 price: float, pnl: float = 0.0, timestamp: Optional[datetime] = None):
        with self.lock:
            entry_timestamp = timestamp or datetime.now(timezone.utc)
            new_entry = {
                'timestamp': entry_timestamp,
                'exchange': exchange,
                'symbol': symbol,
                'side': side,
                'quantity': float(quantity),
                'price': float(price),
                'pnl': float(pnl)
            }

            # Lightweight deduplication: skip if identical entry already present near the head
            for existing in list(self.recent_fills)[:5]:
                time_match = False
                existing_ts = existing.get('timestamp')
                if isinstance(existing_ts, datetime) and isinstance(entry_timestamp, datetime):
                    time_match = abs((existing_ts - entry_timestamp).total_seconds()) < 1e-6

                if (
                    existing.get('exchange') == new_entry['exchange']
                    and existing.get('symbol') == new_entry['symbol']
                    and existing.get('side') == new_entry['side']
                    and abs(existing.get('quantity', 0.0) - new_entry['quantity']) < 1e-9
                    and abs(existing.get('price', 0.0) - new_entry['price']) < 1e-9
                    and time_match
                ):
                    return

            self.recent_fills.appendleft(new_entry)
            self._dirty = True

    def add_recent_order(self, exchange: str, symbol: str, side: str, quantity: float,
                         status: str, order_id: int = None, timestamp: Optional[datetime] = None):
        """Add an order to recent orders history."""
        with self.lock:
            self.recent_orders.appendleft({
                'timestamp': timestamp or datetime.now(timezone.utc),
                'exchange': exchange,
                'symbol': symbol,
                'side': side,
                'quantity': quantity,
                'status': status,
                'order_id': order_id
            })
            self._dirty = True

    def is_dirty(self) -> bool:
        """Check if data has changed since last display."""
        with self.lock:
            return self._dirty

    def mark_clean(self):
        """Mark data as displayed (no longer dirty)."""
        with self.lock:
            self._dirty = False

    def get_snapshot(self) -> Dict[str, Any]:
        """Return a thread-safe snapshot of current state."""
        with self.lock:
            # Sort recent fills by timestamp (newest first)
            sorted_fills = sorted(
                list(self.recent_fills),
                key=lambda x: x.get('timestamp') or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True
            )

            return {
                'hl_balance': self.hl_balance,
                'hl_equity': self.hl_equity,
                'pac_balance': self.pac_balance,
                'pac_equity': self.pac_equity,
                'hl_positions': dict(self.hl_positions),
                'pac_positions': dict(self.pac_positions),
                'hl_orders': dict(self.hl_orders),
                'pac_orders': dict(self.pac_orders),
                'recent_fills': sorted_fills,
                'recent_orders': list(self.recent_orders),
                'last_hl_update': self.last_hl_update,
                'last_pac_update': self.last_pac_update,
                'symbols': list(self.symbols)
            }


class Dashboard:
    """Live dashboard with REST API polling (15-30 second intervals, rate-limited to respect 300 req/60s limit)."""

    def __init__(self, config_path: str = "config.json"):
        self.config = load_config(config_path)
        self.state = DashboardState()
        self.console = Console()
        self.running = True

        # Extract symbols from config
        config_symbols_raw = self.config.get("symbols", []) or []
        self.symbols = [
            str(symbol).strip().upper()
            for symbol in config_symbols_raw
            if isinstance(symbol, str) and symbol.strip()
        ]
        self.state.symbols = self.symbols

        # Connectors
        self.hl_connector: Optional[HyperliquidConnector] = None
        self.pac_connector: Optional[PacificaConnector] = None

        # Background tasks (keep references to prevent GC)
        self.background_tasks: List[asyncio.Task] = []
        self._last_pac_orders_refresh: float = 0.0
        self._last_hl_fill_timestamp_ms: int = 0
        self._last_pac_fill_history_id: int = 0
        self._last_pac_fill_timestamp_ms: int = 0

        # Rate limiter (max 5 concurrent API calls)
        self._rate_limiter: Optional[asyncio.Semaphore] = None

    async def initialize(self):
        """Initialize exchange connections and bootstrap data."""
        import sys
        print("[Dashboard] Starting initialization...", file=sys.stderr)

        # Initialize rate limiter
        self._rate_limiter = asyncio.Semaphore(5)
        print("[Dashboard] Rate limiter initialized", file=sys.stderr)

        # Suppress all output during initialization
        _devnull = open(os.devnull, 'w')
        _original_stdout = sys.stdout
        _original_stderr = sys.stderr

        try:
            sys.stdout = _devnull
            sys.stderr = _devnull

            # Get credentials
            hl_creds = get_hyperliquid_credentials()
            pac_creds = get_pacifica_credentials()

            # Initialize connectors
            self.hl_connector = HyperliquidConnector(
                address=hl_creds["address"],
                private_key=hl_creds["private_key"]
            )

            self.pac_connector = PacificaConnector(
                sol_wallet=pac_creds["sol_wallet"],
                api_public=pac_creds["api_public"],
                api_private=pac_creds["private_key"]
            )
        finally:
            sys.stdout = _original_stdout
            sys.stderr = _original_stderr
            _devnull.close()

        print("[Dashboard] Connectors initialized", file=sys.stderr)

        # Bootstrap initial data via REST (one-time)
        print("[Dashboard] Starting data bootstrap...", file=sys.stderr)
        await self._bootstrap_data()
        print("[Dashboard] Data bootstrap completed", file=sys.stderr)

        # Start REST polling tasks
        print("[Dashboard] Starting polling tasks...", file=sys.stderr)
        await self._start_websockets()
        print("[Dashboard] Polling tasks started", file=sys.stderr)

    async def _rate_limited_call(self, func, *args, **kwargs):
        """Execute a function with rate limiting."""
        async with self._rate_limiter:
            return await asyncio.to_thread(func, *args, **kwargs)

    def _add_pac_trade_fill(
        self,
        trade: Dict[str, Any],
        min_history_id: Optional[int] = None,
        min_timestamp_ms: Optional[int] = None
    ) -> Tuple[Optional[int], Optional[int]]:
        """Parse a Pacifica trade record and add it to recent fills.

        Returns:
            Tuple of (history_id, timestamp_ms) for tracking deduplication.
        """
        if not isinstance(trade, dict):
            return (None, None)

        def _safe_float(value: Any, default: float = 0.0) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        symbol_raw = trade.get('symbol') or trade.get('s')
        symbol = str(symbol_raw).upper() if symbol_raw else ''
        if not symbol:
            return (None, None)

        side_raw = (trade.get('side') or trade.get('ts') or '').lower()
        if side_raw in {'bid', 'buy', 'open_long', 'close_short', 'long'}:
            side_display = 'BUY'
        elif side_raw in {'ask', 'sell', 'open_short', 'close_long', 'short'}:
            side_display = 'SELL'
        else:
            side_display = side_raw.upper() or '-'

        qty_value = (
            trade.get('amount')
            or trade.get('filled_quantity')
            or trade.get('qty')
            or trade.get('size')
        )
        price_value = (
            trade.get('price')
            or trade.get('fill_price')
            or trade.get('average_price')
            or trade.get('px')
        )
        pnl_value = (
            trade.get('pnl')
            or trade.get('realized_pnl')
            or trade.get('realizedPnl')
            or trade.get('net_pnl')
            or trade.get('n')
        )

        quantity = _safe_float(qty_value, 0.0)
        price = _safe_float(price_value, 0.0)
        pnl = _safe_float(pnl_value, 0.0)

        history_id_fields = ['history_id', 'historyId', 'historyID', 'id']
        history_id: Optional[int] = None
        for field in history_id_fields:
            value = trade.get(field)
            if value is None:
                continue
            try:
                candidate = int(value)
            except (TypeError, ValueError):
                continue
            if candidate:
                history_id = candidate
                break

        timestamp_fields = [
            'timestamp',
            'ts',
            'filled_at',
            'filledTime',
            'updated_at',
            'updatedAt',
            'created_at',
            'createdAt',
            'time'
        ]
        timestamp_ms: Optional[int] = None
        for field in timestamp_fields:
            value = trade.get(field)
            if value is None or value == 0:
                continue
            try:
                timestamp_candidate = int(value)
            except (TypeError, ValueError):
                continue
            timestamp_ms = timestamp_candidate
            break

        if min_history_id is not None and history_id is not None and history_id <= min_history_id:
            return (history_id, timestamp_ms)

        if (
            history_id is None
            and min_timestamp_ms is not None
            and timestamp_ms is not None
            and timestamp_ms <= min_timestamp_ms
        ):
            return (history_id, timestamp_ms)

        timestamp_dt = None
        if timestamp_ms:
            if timestamp_ms > 10_000_000_000:
                timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            else:
                timestamp_dt = datetime.fromtimestamp(timestamp_ms, tz=timezone.utc)

        if quantity > 0 and price > 0:
            self.state.add_fill('PAC', symbol, side_display, quantity, price, pnl, timestamp_dt)

        return (history_id, timestamp_ms)

    async def _bootstrap_data(self):
        """Initial REST calls to get starting state."""
        import sys
        # Fetch balances
        print("[Dashboard Bootstrap] Fetching HL balance...", file=sys.stderr)
        try:
            hl_balance = await asyncio.wait_for(
                self._rate_limited_call(self.hl_connector.get_balance),
                timeout=10.0
            )
            hl_equity = await asyncio.wait_for(
                self._rate_limited_call(self.hl_connector.get_equity),
                timeout=10.0
            )
            self.state.update_hl_balance(hl_balance, hl_equity)
            print(f"[Dashboard Bootstrap] HL balance: ${hl_balance:.2f}, equity: ${hl_equity:.2f}", file=sys.stderr)
        except asyncio.TimeoutError:
            print("[Dashboard Bootstrap] HL balance fetch timed out", file=sys.stderr)
        except Exception as e:
            print(f"[Dashboard Bootstrap] HL balance fetch error: {e}", file=sys.stderr)

        try:
            pac_balance = await self._rate_limited_call(self.pac_connector.get_balance)
            pac_equity = await self._rate_limited_call(self.pac_connector.get_equity)
            self.state.update_pac_balance(pac_balance, pac_equity)
        except Exception:
            pass

        # Fetch positions
        hl_positions = {}
        pac_positions = {}

        for symbol in self.symbols:
            try:
                pos = await self._rate_limited_call(self.hl_connector.get_position_rest, symbol)
                if pos and abs(pos.get('quantity', 0.0)) > 1e-6:
                    hl_positions[symbol] = pos
            except Exception:
                pass

            try:
                pos = await self._rate_limited_call(self.pac_connector.get_position, symbol)
                if pos and abs(pos.get('quantity', 0.0)) > 1e-6:
                    pac_positions[symbol] = pos
            except Exception:
                pass

        self.state.update_hl_positions(hl_positions)
        self.state.update_pac_positions(pac_positions)

        # Fetch open orders
        try:
            hl_orders_list = await self._rate_limited_call(self.hl_connector.get_open_orders)
            hl_orders = {}
            for order in hl_orders_list:
                order_id = order.get('order_id') or order.get('oid') or order.get('id')
                if order_id is None:
                    continue
                hl_orders[str(order_id)] = order
            self.state.update_hl_orders(hl_orders)
        except Exception:
            pass

        try:
            pac_orders_list = await self._rate_limited_call(self.pac_connector.get_open_orders)
            pac_orders: Dict[str, Dict[str, Any]] = {}
            for order in pac_orders_list:
                order_id = order.get('order_id') or order.get('id')
                if order_id is None:
                    continue
                pac_orders[str(order_id)] = order
            self.state.update_pac_orders(pac_orders)
        except Exception:
            pass

        # Fetch recent fills from Hyperliquid
        try:
            hl_fills = await self._rate_limited_call(
                self.hl_connector.info.user_fills,
                self.hl_connector.address
            )
            latest_hl_fill_ts = self._last_hl_fill_timestamp_ms
            # Process fills (most recent first)
            if hl_fills and isinstance(hl_fills, list):
                import sys
                print(f"[Dashboard Bootstrap] Fetched {len(hl_fills)} HL fills", file=sys.stderr)
                fills_added = 0
                for fill in hl_fills[:10]:  # Get last 10 fills
                    try:
                        symbol = fill.get('coin', '')
                        side = 'BUY' if fill.get('side') == 'B' else 'SELL'
                        qty = float(fill.get('sz', 0))
                        price = float(fill.get('px', 0))
                        # Parse timestamp from fill (milliseconds)
                        timestamp_ms_raw = fill.get('time', 0)
                        timestamp_ms = int(timestamp_ms_raw) if timestamp_ms_raw else 0
                        timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc) if timestamp_ms else None
                        if timestamp_ms:
                            latest_hl_fill_ts = max(latest_hl_fill_ts, timestamp_ms)
                        # HL doesn't provide PnL in fills, set to 0
                        self.state.add_fill('HL', symbol, side, qty, price, 0.0, timestamp)
                        fills_added += 1
                    except Exception as e:
                        print(f"[Dashboard Bootstrap] Error processing HL fill: {e}", file=sys.stderr)
                print(f"[Dashboard Bootstrap] Added {fills_added} HL fills to state", file=sys.stderr)
            else:
                import sys
                print(f"[Dashboard Bootstrap] No HL fills returned (hl_fills={'None' if hl_fills is None else 'not a list'})", file=sys.stderr)
            if latest_hl_fill_ts > self._last_hl_fill_timestamp_ms:
                self._last_hl_fill_timestamp_ms = latest_hl_fill_ts
        except Exception as e:
            import sys
            print(f"[Dashboard Bootstrap] HL fills fetch error: {e}", file=sys.stderr)

        # Fetch recent order history from Pacifica (includes all order statuses)
        try:
            pac_orders = await self._rate_limited_call(
                self.pac_connector.get_order_history_rest,
                limit=20  # Get enough for both fills and recent orders
            )

            # Process orders for recent orders list (all statuses)
            orders_added = 0
            for order in pac_orders[:8]:  # Last 8 orders
                try:
                    symbol = order.get('symbol', '')
                    side_str = order.get('side', '')  # 'bid' or 'ask'
                    qty = float(order.get('amount', 0))
                    order_status = order.get('order_status', 'open')  # filled, partially_filled, cancelled, rejected, open

                    # Map side to display format
                    side = 'BUY' if side_str == 'bid' else 'SELL'

                    # Map status to display format
                    status_map = {
                        'open': 'OPEN',
                        'partially_filled': 'PARTIAL',
                        'filled': 'FILLED',
                        'cancelled': 'CANCELLED',
                        'rejected': 'REJECTED'
                    }
                    status_display = status_map.get(order_status, order_status.upper())
                    order_id = order.get('order_id')

                    # Parse timestamp (use updated_at if filled/cancelled, otherwise created_at)
                    timestamp_ms = order.get('updated_at') or order.get('created_at', 0)
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc) if timestamp_ms else None

                    self.state.add_recent_order('PAC', symbol, side, qty, status_display, order_id, timestamp)
                    orders_added += 1
                except Exception:
                    pass

            # Skip PAC fills in bootstrap - the polling task will populate them within 3 seconds
            # This prevents PAC fills from overwriting HL fills in the maxlen=10 deque

        except Exception:
            pass

    async def _start_websockets(self):
        """Start REST polling tasks for live updates."""
        # Hyperliquid: Poll user state periodically (SDK doesn't provide full user state via WS)
        async def poll_hl_user_state():
            """Poll Hyperliquid user state every 3 seconds."""
            while self.running:
                try:
                    user_state = await self._rate_limited_call(
                        self.hl_connector.info.user_state,
                        self.hl_connector.address
                    )

                    # Extract positions
                    positions = {}
                    for asset_pos in user_state.get('assetPositions', []):
                        pos = asset_pos.get('position', {})
                        symbol = pos.get('coin')
                        qty = float(pos.get('szi', 0))

                        if symbol and abs(qty) > 1e-6:
                            positions[symbol] = {
                                'quantity': qty,
                                'entry_price': float(pos.get('entryPx', 0)) if pos.get('entryPx') else 0,
                                'mark_price': 0.0,  # Will be filled from price stream if needed
                                'unrealized_pnl': float(pos.get('unrealizedPnl', 0)),
                                'notional': abs(qty * float(pos.get('positionValue', 0))) / qty if qty != 0 else 0,
                                'leverage': pos.get('leverage', {}).get('value', 1),
                                'liquidation_price': float(pos.get('liquidationPx', 0)) if pos.get('liquidationPx') and pos.get('liquidationPx') != 'NaN' else None
                            }

                    self.state.update_hl_positions(positions)

                    # Extract balances
                    margin_summary = user_state.get('marginSummary', {})
                    equity = float(margin_summary.get('accountValue', 0))
                    withdrawable = float(user_state.get('withdrawable', 0))
                    self.state.update_hl_balance(withdrawable, equity)

                    # Poll open orders
                    hl_orders_list = await self._rate_limited_call(self.hl_connector.get_open_orders)
                    hl_orders = {}
                    for order in hl_orders_list:
                        order_id = order.get('order_id') or order.get('oid') or order.get('id')
                        if order_id is None:
                            continue
                        hl_orders[str(order_id)] = order
                    self.state.update_hl_orders(hl_orders)

                    # Fetch recent fills and append any new ones
                    try:
                        hl_fills = await self._rate_limited_call(
                            self.hl_connector.info.user_fills,
                            self.hl_connector.address
                        )
                        if hl_fills:
                            new_fills = []
                            for fill in hl_fills:
                                try:
                                    timestamp_ms_raw = fill.get('time', 0)
                                    timestamp_ms = int(timestamp_ms_raw) if timestamp_ms_raw else 0
                                except (TypeError, ValueError):
                                    timestamp_ms = 0

                                if timestamp_ms and timestamp_ms <= self._last_hl_fill_timestamp_ms:
                                    continue
                                new_fills.append((timestamp_ms, fill))

                            if new_fills:
                                import sys
                                print(f"[Dashboard Poll] Found {len(new_fills)} new HL fills", file=sys.stderr)
                                # Process from oldest to newest to maintain chronological order
                                new_fills.sort(key=lambda item: item[0] or 0)
                                latest_ts = self._last_hl_fill_timestamp_ms
                                fills_added = 0
                                for timestamp_ms, fill in new_fills:
                                    try:
                                        symbol = fill.get('coin', '')
                                        side = 'BUY' if fill.get('side') == 'B' else 'SELL'
                                        qty = float(fill.get('sz', 0))
                                        price = float(fill.get('px', 0))
                                        timestamp = None
                                        if timestamp_ms:
                                            latest_ts = max(latest_ts, timestamp_ms)
                                            timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                                        self.state.add_fill('HL', symbol, side, qty, price, 0.0, timestamp)
                                        fills_added += 1
                                    except Exception as e:
                                        import sys
                                        print(f"[Dashboard Poll] Error adding HL fill: {e}", file=sys.stderr)

                                if latest_ts > self._last_hl_fill_timestamp_ms:
                                    self._last_hl_fill_timestamp_ms = latest_ts
                                import sys
                                print(f"[Dashboard Poll] Added {fills_added} HL fills to state", file=sys.stderr)
                    except Exception as e:
                        import sys
                        print(f"[Dashboard Poll] HL fills fetch error: {e}", file=sys.stderr)

                except Exception as e:
                    # Log errors but don't crash
                    import sys
                    print(f"[Dashboard] HL poll error: {e}", file=sys.stderr)

                await asyncio.sleep(15)  # Poll every 15 seconds (rate limit: 300 req/60s)

        # Start HL polling task (store reference to prevent GC)
        hl_poll_task = asyncio.create_task(poll_hl_user_state())
        self.background_tasks.append(hl_poll_task)

        # Pacifica: Poll open orders periodically (to catch cancellations)
        async def poll_pac_orders():
            """Poll Pacifica open orders every 15 seconds with periodic hard refresh."""
            hard_refresh_interval = 15.0  # seconds
            while self.running:
                try:
                    now = time.time()
                    use_hard_refresh = (now - self._last_pac_orders_refresh) >= hard_refresh_interval

                    # Always use hard refresh to force REST API call and bypass cache
                    # This ensures we get fresh data with 'amount' field populated
                    pac_orders_list = await self._rate_limited_call(self.pac_connector.refresh_open_orders)
                    self._last_pac_orders_refresh = now

                    pac_orders: Dict[str, Dict[str, Any]] = {}
                    for order in pac_orders_list:
                        order_id = order.get('order_id') or order.get('id')
                        if order_id is None:
                            continue
                        pac_orders[str(order_id)] = order

                    self.state.update_pac_orders(pac_orders)
                except Exception as e:
                    # Log errors but don't crash
                    import sys
                    print(f"[Dashboard] PAC order poll error: {e}", file=sys.stderr)

                await asyncio.sleep(15)  # Poll every 15 seconds

        # Start PAC polling task (store reference to prevent GC)
        pac_poll_task = asyncio.create_task(poll_pac_orders())
        self.background_tasks.append(pac_poll_task)

        # Pacifica: Poll positions and balances periodically
        async def poll_pac_positions_and_balances():
            """Poll Pacifica positions and balances every 15 seconds."""
            while self.running:
                try:
                    # Fetch balances
                    pac_balance = await self._rate_limited_call(self.pac_connector.get_balance)
                    pac_equity = await self._rate_limited_call(self.pac_connector.get_equity)
                    self.state.update_pac_balance(pac_balance, pac_equity)

                    # Fetch positions for all symbols
                    pac_positions = {}
                    for symbol in self.symbols:
                        try:
                            pos = await self._rate_limited_call(self.pac_connector.get_position, symbol)
                            if pos and abs(pos.get('quantity', 0.0)) > 1e-6:
                                pac_positions[symbol] = pos
                        except Exception:
                            pass

                    self.state.update_pac_positions(pac_positions)

                except Exception as e:
                    # Log errors but don't crash
                    import sys
                    print(f"[Dashboard] PAC positions/balances poll error: {e}", file=sys.stderr)

                await asyncio.sleep(15)  # Poll every 15 seconds

        # Start PAC positions/balances polling task (store reference to prevent GC)
        pac_positions_poll_task = asyncio.create_task(poll_pac_positions_and_balances())
        self.background_tasks.append(pac_positions_poll_task)

        # Pacifica: Poll trade history for new fills
        async def poll_pac_fills():
            """Poll Pacifica trade history for new fills (dedupe by history_id)."""
            poll_interval = 15.0
            while self.running:
                try:
                    trades = await self._rate_limited_call(
                        self.pac_connector.get_trade_history_rest,
                        None,
                        self._last_pac_fill_history_id,
                        50
                    )

                    if trades:
                        trades_sorted = sorted(
                            trades,
                            key=lambda trade: trade.get('history_id', 0) or trade.get('updated_at', 0) or trade.get('created_at', 0)
                        )
                        latest_history_id = self._last_pac_fill_history_id
                        latest_timestamp_ms = self._last_pac_fill_timestamp_ms

                        for trade in trades_sorted:
                            try:
                                history_id, timestamp_ms = self._add_pac_trade_fill(
                                    trade,
                                    min_history_id=latest_history_id,
                                    min_timestamp_ms=latest_timestamp_ms
                                )
                                if history_id:
                                    latest_history_id = max(latest_history_id, history_id)
                                if timestamp_ms:
                                    latest_timestamp_ms = max(latest_timestamp_ms, timestamp_ms)
                            except Exception:
                                pass

                        if latest_history_id > self._last_pac_fill_history_id:
                            self._last_pac_fill_history_id = latest_history_id
                        if latest_timestamp_ms > self._last_pac_fill_timestamp_ms:
                            self._last_pac_fill_timestamp_ms = latest_timestamp_ms

                except Exception as e:
                    import sys
                    print(f"[Dashboard] PAC fills poll error: {e}", file=sys.stderr)

                await asyncio.sleep(poll_interval)

        pac_fills_poll_task = asyncio.create_task(poll_pac_fills())
        self.background_tasks.append(pac_fills_poll_task)

        # Pacifica: Poll order history for recent orders list
        async def poll_pac_order_history():
            """Poll Pacifica order history every 30 seconds to update recent orders."""
            poll_interval = 30.0
            while self.running:
                try:
                    # Fetch recent order history
                    pac_orders = await self._rate_limited_call(
                        self.pac_connector.get_order_history_rest,
                        limit=8  # Get last 8 orders
                    )

                    # Clear and repopulate recent orders
                    # This ensures we show the latest 8 orders, not accumulate indefinitely
                    new_recent_orders = []
                    for order in pac_orders[:8]:
                        try:
                            symbol = order.get('symbol', '')
                            side_str = order.get('side', '')  # 'bid' or 'ask'
                            qty = float(order.get('amount', 0))
                            order_status = order.get('order_status', 'open')

                            # Map side to display format
                            side = 'BUY' if side_str == 'bid' else 'SELL'

                            # Map status to display format
                            status_map = {
                                'open': 'OPEN',
                                'partially_filled': 'PARTIAL',
                                'filled': 'FILLED',
                                'cancelled': 'CANCELLED',
                                'rejected': 'REJECTED'
                            }
                            status_display = status_map.get(order_status, order_status.upper())
                            order_id = order.get('order_id')

                            # Parse timestamp
                            timestamp_ms = order.get('updated_at') or order.get('created_at', 0)
                            timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc) if timestamp_ms else None

                            if timestamp:
                                new_recent_orders.append({
                                    'timestamp': timestamp,
                                    'exchange': 'PAC',
                                    'symbol': symbol,
                                    'side': side,
                                    'quantity': qty,
                                    'status': status_display,
                                    'order_id': order_id
                                })
                        except Exception:
                            pass

                    # Update recent orders with new snapshot
                    if new_recent_orders:
                        with self.state.lock:
                            self.state.recent_orders.clear()
                            # appendleft in reverse order to maintain newest-first
                            for order_entry in reversed(new_recent_orders):
                                self.state.recent_orders.appendleft(order_entry)
                            self.state._dirty = True

                except Exception as e:
                    import sys
                    print(f"[Dashboard] PAC order history poll error: {e}", file=sys.stderr)

                await asyncio.sleep(poll_interval)

        pac_order_history_poll_task = asyncio.create_task(poll_pac_order_history())
        self.background_tasks.append(pac_order_history_poll_task)

    def _build_layout(self, snapshot: Dict[str, Any]) -> Layout:
        """Build the dashboard layout from state snapshot."""
        layout = Layout()

        # Header
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        header = Panel(
            Text(f"XEMM Live Dashboard - {now}", style="bold cyan", justify="center"),
            style="bold white"
        )

        # Balances table
        balances_table = Table(title="Account Balances", show_header=True, header_style="bold magenta")
        balances_table.add_column("Exchange", style="cyan", width=12)
        balances_table.add_column("Available", justify="right", style="green", width=18)
        balances_table.add_column("Total Equity", justify="right", style="yellow", width=18)

        hl_bal = snapshot['hl_balance']
        hl_eq = snapshot['hl_equity']
        pac_bal = snapshot['pac_balance']
        pac_eq = snapshot['pac_equity']

        balances_table.add_row(
            "Hyperliquid",
            f"${hl_bal:,.2f}" if hl_bal is not None else "N/A",
            f"${hl_eq:,.2f}" if hl_eq is not None else "N/A"
        )
        balances_table.add_row(
            "Pacifica",
            f"${pac_bal:,.2f}" if pac_bal is not None else "N/A",
            f"${pac_eq:,.2f}" if pac_eq is not None else "N/A"
        )

        # Positions table
        positions_table = Table(title="Net Position Summary", show_header=True, header_style="bold magenta")
        positions_table.add_column("Symbol", style="cyan", width=8)
        positions_table.add_column("HL Qty", justify="right", width=12)
        positions_table.add_column("PAC Qty", justify="right", width=12)
        positions_table.add_column("Net Qty", justify="right", width=12)
        positions_table.add_column("Status", justify="center", width=10)
        positions_table.add_column("Net PnL", justify="right", width=12)

        hl_positions = snapshot['hl_positions']
        pac_positions = snapshot['pac_positions']

        # Use configured symbols for pre-allocation (or fallback to positions if not configured)
        configured_symbols = snapshot.get('symbols', [])
        position_symbols = set(hl_positions.keys()) | set(pac_positions.keys())
        all_symbols = configured_symbols if configured_symbols else sorted(position_symbols)

        # Always show at least 3 rows (pre-allocated) for configured symbols
        symbols_to_display = all_symbols[:max(3, len(all_symbols))]

        rows_added = 0
        for symbol in symbols_to_display:
            hl_qty = hl_positions.get(symbol, {}).get('quantity', 0.0)
            pac_qty = pac_positions.get(symbol, {}).get('quantity', 0.0)
            net_qty = hl_qty + pac_qty

            hl_pnl = hl_positions.get(symbol, {}).get('unrealized_pnl', 0.0)
            pac_pnl = pac_positions.get(symbol, {}).get('unrealized_pnl', 0.0)
            net_pnl = hl_pnl + pac_pnl

            # Status color (show BALANCED even if no position)
            if abs(net_qty) < 1e-6:
                status = "[green]BALANCED[/green]"
            elif abs(net_qty) < 0.1 * abs(hl_qty + pac_qty):
                status = "[yellow]SMALL[/yellow]"
            else:
                status = "[red]IMBALANCED[/red]"

            positions_table.add_row(
                symbol,
                f"{hl_qty:+.6f}" if hl_qty != 0 else "[dim]0.000000[/dim]",
                f"{pac_qty:+.6f}" if pac_qty != 0 else "[dim]0.000000[/dim]",
                f"{net_qty:+.6f}" if net_qty != 0 else "[dim]0.000000[/dim]",
                status,
                f"${net_pnl:+.2f}" if net_pnl != 0 else "[dim]$0.00[/dim]"
            )
            rows_added += 1

        # If no symbols configured, show placeholder
        if rows_added == 0:
            positions_table.add_row("[dim]-[/dim]", "", "", "", "", "")

        # Orders table
        orders_table = Table(title="Open Orders", show_header=True, header_style="bold magenta")
        orders_table.add_column("Exchange", style="cyan", width=10)
        orders_table.add_column("Symbol", width=8)
        orders_table.add_column("Side", width=6)
        orders_table.add_column("Quantity", justify="right", width=12)
        orders_table.add_column("Price", justify="right", width=12)
        orders_table.add_column("Age", justify="right", width=8)

        # Add HL orders
        hl_orders = snapshot['hl_orders']

        def _order_timestamp(order: Dict[str, Any]) -> float:
            try:
                return float(order.get('timestamp', 0) or 0.0)
            except (TypeError, ValueError):
                return 0.0

        for order in sorted(hl_orders.values(), key=_order_timestamp, reverse=True)[:5]:
            age = self._format_age(order.get('timestamp', 0))

            raw_side = order.get('side', '')
            side_upper = raw_side.upper() if isinstance(raw_side, str) else ''
            if side_upper in ('B', 'BUY'):
                side_str = "BUY"
            elif side_upper in ('S', 'SELL'):
                side_str = "SELL"
            else:
                side_str = side_upper or "-"

            symbol = order.get('symbol') or order.get('coin') or "-"

            qty_value = order.get('quantity')
            if qty_value is None:
                qty_value = order.get('sz') or order.get('size')
            qty_str = "-"
            try:
                if qty_value is not None:
                    qty_str = f"{float(qty_value):.6f}"
            except (TypeError, ValueError):
                pass

            price_value = order.get('price')
            if price_value is None:
                price_value = order.get('limitPx') or order.get('px')
            price_str = "-"
            try:
                if price_value is not None:
                    price_str = f"{float(price_value):,.6f}"
            except (TypeError, ValueError):
                pass

            orders_table.add_row(
                "HL",
                symbol,
                side_str,
                qty_str,
                price_str,
                age
            )

        # Add PAC orders
        pac_orders = snapshot['pac_orders']

        # DEBUG: Write order data to file (first order details + key fields)
        try:
            import os
            debug_path = os.path.join(os.getcwd(), 'pac_order_debug.txt')
            with open(debug_path, 'w') as f:
                f.write(f"Number of PAC orders: {len(pac_orders)}\n")
                if pac_orders:
                    first_order = list(pac_orders.values())[0]
                    f.write(f"\nPAC Order Keys: {list(first_order.keys())}\n")
                    f.write(f"\nFull Order Data:\n{first_order}\n")
                    # Key fields relevant to quantity display
                    f.write("\nKey Fields:\n")
                    f.write(f"  amount: {repr(first_order.get('amount'))}\n")
                    f.write(f"  quantity: {repr(first_order.get('quantity'))}\n")
                    f.write(f"  filled: {repr(first_order.get('filled'))}\n")
                    f.write(f"  filled_quantity: {repr(first_order.get('filled_quantity'))}\n")
                    f.write(f"  cancelled_quantity: {repr(first_order.get('cancelled_quantity'))}\n")
                    f.write(f"  remaining_quantity: {repr(first_order.get('remaining_quantity'))}\n")
                    f.write(f"  price: {repr(first_order.get('price'))}\n")
                    f.write(f"  side: {repr(first_order.get('side'))}\n")
                    f.write(f"  symbol: {repr(first_order.get('symbol'))}\n")
                else:
                    f.write("No PAC orders in snapshot\n")
        except Exception as e:
            import os
            debug_path = os.path.join(os.getcwd(), 'pac_order_debug.txt')
            with open(debug_path, 'w') as f:
                f.write(f"DEBUG ERROR: {e}\n")

        for order in sorted(pac_orders.values(), key=lambda x: x.get('timestamp', 0), reverse=True)[:5]:
            age = self._format_age(order.get('timestamp', 0))

            # PAC orders use these fields (from both REST and WebSocket):
            # 'side': 'buy' or 'sell' (lowercase)
            # 'amount': quantity
            # 'price': price
            # 'symbol': symbol name

            side_raw = order.get('side', '')
            side_str = side_raw.upper() if side_raw else '-'

            symbol = order.get('symbol', '-')

            # Get quantity for open orders:
            # Prefer remaining quantity when available; fall back to amount/quantity and other aliases
            qty_value = None

            # 1) Direct remaining field from connector (preferred for partial fills)
            rem = (
                order.get('remaining_quantity')
                or order.get('remaining')
                or order.get('remaining_amount')
                or order.get('remainingQty')
            )
            try:
                if rem is not None and float(rem) > 0:
                    qty_value = float(rem)
            except (TypeError, ValueError):
                pass

            # 2) Compute remaining from amount - filled - cancelled
            if qty_value is None:
                try:
                    amt = float(order.get('amount', 0) or 0)
                    filled = float(order.get('filled', order.get('filled_quantity', 0)) or 0)
                    cancelled = float(order.get('cancelled_quantity', 0) or 0)
                    computed_remaining = max(amt - filled - cancelled, 0.0)
                    if computed_remaining > 0:
                        qty_value = computed_remaining
                except (TypeError, ValueError):
                    qty_value = None

            # 3) Fall back to amount/quantity if still unknown (e.g., fresh open orders)
            if qty_value is None or qty_value == 0:
                try:
                    amt = (
                        order.get('amount')
                        or order.get('quantity')
                        or order.get('original_quantity')
                        or order.get('initial_amount')
                        or order.get('original_amount')
                        or order.get('size')
                        or order.get('sz')
                        or order.get('a')
                    )
                    qty_value = float(amt) if amt is not None else 0.0
                except (TypeError, ValueError):
                    qty_value = 0.0

            # Use higher precision for very small remaining sizes
            if qty_value > 0 and qty_value < 1e-6:
                qty_str = f"{qty_value:.10f}"
            else:
                qty_str = f"{qty_value:.6f}"

            # Get price
            price_value = order.get('price', 0)
            price_str = f"{float(price_value):,.6f}"

            orders_table.add_row(
                "PAC",
                symbol,
                side_str,
                qty_str,
                price_str,
                age
            )

        if not hl_orders and not pac_orders:
            orders_table.add_row("", "", "[dim]No open orders[/dim]", "", "", "")

        # Recent Orders table (pre-allocated 8 rows to prevent flickering)
        recent_orders_table = Table(title="Recent Orders (Last 8)", show_header=True, header_style="bold magenta")
        recent_orders_table.add_column("Time", width=12)
        recent_orders_table.add_column("Ex", width=5)
        recent_orders_table.add_column("Symbol", width=8)
        recent_orders_table.add_column("Side", width=6)
        recent_orders_table.add_column("Quantity", justify="right", width=12)
        recent_orders_table.add_column("Status", width=10)

        recent_orders = snapshot.get('recent_orders', [])

        # Always show 8 rows (pre-allocated space)
        for i in range(8):
            if i < len(recent_orders):
                order = recent_orders[i]
                timestamp = order['timestamp'].strftime("%H:%M:%S")
                side_display = order['side']
                status = order['status']

                # Color-code status
                if status == 'FILLED':
                    status_display = f"[green]{status}[/green]"
                elif status == 'CANCELLED' or status == 'REJECTED':
                    status_display = f"[red]{status}[/red]"
                elif status == 'PARTIAL':
                    status_display = f"[yellow]{status}[/yellow]"
                else:
                    status_display = status

                recent_orders_table.add_row(
                    timestamp,
                    order['exchange'],
                    order['symbol'],
                    side_display,
                    f"{order['quantity']:.4f}",
                    status_display
                )
            else:
                # Empty row (pre-allocated space) - use visible placeholder to prevent row collapse
                recent_orders_table.add_row("[dim]--:--:--[/dim]", "[dim]-[/dim]", "[dim]-[/dim]", "[dim]-[/dim]", "[dim]-[/dim]", "[dim]-[/dim]")

        # Recent fills table
        fills_table = Table(title="Recent Fills", show_header=True, header_style="bold magenta")
        fills_table.add_column("Time", width=12)
        fills_table.add_column("Ex", width=5)
        fills_table.add_column("Symbol", width=8)
        fills_table.add_column("Side", width=10)
        fills_table.add_column("Quantity", justify="right", width=12)
        fills_table.add_column("Price", justify="right", width=12)
        fills_table.add_column("PnL", justify="right", width=10)

        recent_fills = snapshot['recent_fills']
        for fill in recent_fills[:5]:
            timestamp = fill['timestamp'].strftime("%H:%M:%S")
            pnl_str = f"${fill['pnl']:+.2f}" if fill['pnl'] != 0 else "-"
            fills_table.add_row(
                timestamp,
                fill['exchange'],
                fill['symbol'],
                fill['side'],
                f"{fill['quantity']:.6f}",
                f"{fill['price']:,.6f}",
                pnl_str
            )

        if not recent_fills:
            fills_table.add_row("", "", "", "[dim]No recent fills[/dim]", "", "", "")

        # Combine into layout
        layout.split_column(
            Layout(header, size=3),
            Layout(balances_table, size=8),  # Increased for proper bottom border display
            Layout(positions_table, size=20),  # Increased to show more position slots
            Layout(orders_table, size=10),
            Layout(recent_orders_table, size=13),  # Increased to fit all 8 rows + header + title
            Layout(fills_table, size=10)
        )

        return layout

    def _format_age(self, timestamp_ms: int) -> str:
        """Format timestamp age as human-readable string.

        Handles both seconds and milliseconds timestamps automatically.
        """
        if not timestamp_ms or timestamp_ms == 0:
            return "-"

        # Auto-detect timestamp format:
        # If timestamp < 10 billion, it's in seconds (year 2286 if in seconds, year 1970 if in ms)
        # If timestamp >= 10 billion, it's in milliseconds
        if timestamp_ms < 10_000_000_000:
            # Timestamp is in seconds
            timestamp_seconds = timestamp_ms
        else:
            # Timestamp is in milliseconds
            timestamp_seconds = timestamp_ms / 1000

        # Validate timestamp is reasonable (after year 2000, before year 2100)
        if timestamp_seconds < 946684800 or timestamp_seconds > 4102444800:
            return "-"

        now = datetime.now(timezone.utc)
        event_time = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
        delta = now - event_time
        seconds = int(delta.total_seconds())

        # Handle negative deltas (future timestamps)
        if seconds < 0:
            return "-"

        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            return f"{seconds // 60}m"
        elif seconds < 86400:
            return f"{seconds // 3600}h"
        else:
            return f"{seconds // 86400}d"

    async def run(self):
        """Run the live dashboard."""
        import sys
        print("[Dashboard] Starting dashboard display...", file=sys.stderr)
        try:
            last_heartbeat = time.time()
            print("[Dashboard] Building initial layout...", file=sys.stderr)

            with Live(self._build_layout(self.state.get_snapshot()),
                     console=self.console,
                     refresh_per_second=2,  # Balance between responsiveness and flicker
                     screen=True) as live:  # Use full screen mode

                print("[Dashboard] Dashboard is now live!", file=sys.stderr)
                while self.running:
                    current_time = time.time()

                    # Update if:
                    # 1. Data changed (dirty flag) - immediate update
                    # 2. Every 5 seconds (heartbeat) to refresh timestamps/ages
                    should_update = False
                    if self.state.is_dirty():
                        should_update = True
                    elif (current_time - last_heartbeat) >= 5.0:
                        should_update = True

                    if should_update:
                        snapshot = self.state.get_snapshot()
                        layout = self._build_layout(snapshot)
                        live.update(layout)
                        self.state.mark_clean()

                        # Only update heartbeat on periodic refresh, not on data changes
                        if (current_time - last_heartbeat) >= 5.0:
                            last_heartbeat = current_time

                    # Check frequently for better responsiveness
                    # Rich's refresh_per_second=2 will handle actual render throttling
                    await asyncio.sleep(0.5)  # Check every 500ms

        except KeyboardInterrupt:
            pass
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Clean up connections."""
        self.running = False

        # Cancel background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Suppress output during cleanup
        _devnull = open(os.devnull, 'w')
        _original_stdout = sys.stdout
        _original_stderr = sys.stderr

        try:
            sys.stdout = _devnull
            sys.stderr = _devnull

            if self.hl_connector:
                try:
                    self.hl_connector.close()
                except Exception:
                    pass

            if self.pac_connector:
                try:
                    self.pac_connector.close()
                except Exception:
                    pass
        finally:
            sys.stdout = _original_stdout
            sys.stderr = _original_stderr
            _devnull.close()


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="XEMM Live Dashboard - Real-time monitoring with REST API polling (15-30s intervals)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m utils.dashboard                        # Use default config.json
  python -m utils.dashboard --config my_config.json # Use custom config

Controls:
  Ctrl+C - Exit dashboard
        """
    )

    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default="config.json",
        help="Path to configuration file (default: config.json)"
    )

    return parser.parse_args()


async def main():
    """Main entry point."""
    args = parse_arguments()

    try:
        print("[Dashboard] Creating dashboard instance...", file=sys.stderr)
        dashboard = Dashboard(config_path=args.config)
        print("[Dashboard] Initializing dashboard...", file=sys.stderr)
        await dashboard.initialize()
        print("[Dashboard] Running dashboard...", file=sys.stderr)
        await dashboard.run()
        sys.exit(0)

    except KeyboardInterrupt:
        print("\n[Dashboard] Interrupted by user", file=sys.stderr)
        sys.exit(0)
    except Exception as e:
        print(f"\n[Dashboard] Fatal error: {type(e).__name__}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
