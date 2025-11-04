"""
Abstract base class for exchange connectors.

This module defines the interface that both Hyperliquid and Pacifica
connectors must implement to ensure consistent behavior across exchanges.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Callable, Optional

from utils.enums import OrderSide
from utils.logger import setup_logger

logger = setup_logger(__name__)


class BaseExchangeConnector(ABC):
    """
    Abstract base class that both Hyperliquid and Pacifica connectors must implement.
    Ensures consistent interface for the XEMM strategy.
    """

    def __init__(self, exchange_name: str):
        """
        Initialize base connector.

        Args:
            exchange_name: Name of the exchange ("hyperliquid" or "pacifica")
        """
        self.exchange_name = exchange_name
        self.logger = setup_logger(
            name=f"xemm.connector.{exchange_name}",
            log_file=f"{exchange_name}_connector.log",
            level="DEBUG"  # Use DEBUG level for comprehensive logging
        )

    # ==================== MARKET DATA METHODS ====================

    @abstractmethod
    def get_bid_ask_ws(self, symbol: str, timeout: float = 10.0) -> Dict[str, float]:
        """
        Get current bid/ask prices via WebSocket (primary method - fastest).

        Args:
            symbol: Trading symbol (e.g., "BTC", "ETH")
            timeout: Timeout in seconds

        Returns:
            {
                "bid": float,      # Best bid price
                "ask": float,      # Best ask price
                "mid": float,      # (bid + ask) / 2
                "timestamp": float # Unix timestamp
            }

        Raises:
            ConnectionError: WebSocket connection failed
            TimeoutError: No data received within timeout
        """
        pass

    @abstractmethod
    def get_bid_ask_rest(self, symbol: str) -> Dict[str, float]:
        """
        Fallback method using REST API when WebSocket unavailable.

        Args:
            symbol: Trading symbol

        Returns:
            Same format as get_bid_ask_ws()

        Raises:
            ConnectionError: REST API connection failed
        """
        pass

    def get_bid_ask(self, symbol: str) -> Dict[str, float]:
        """
        Convenience method using REST API.
        For WebSocket prices, use get_bid_ask_ws() directly (requires await).

        Args:
            symbol: Trading symbol

        Returns:
            Bid/ask data (same format as get_bid_ask_ws())
        """
        return self.get_bid_ask_rest(symbol)

    @abstractmethod
    def start_price_stream(self, symbol: str, callback: Callable) -> str:
        """
        Start continuous WebSocket price updates.

        Args:
            symbol: Trading symbol
            callback: Function called on each update
                     Signature: callback(price_data: Dict[str, float])
                     price_data format: {"bid": float, "ask": float, "mid": float, "timestamp": float}

        Returns:
            subscription_id: String ID for later unsubscription

        Implementation Notes:
            - Maintain persistent WebSocket connection
            - Auto-reconnect on disconnection with exponential backoff
            - Call callback on each price update
            - Handle errors gracefully without crashing
        """
        pass

    @abstractmethod
    def stop_price_stream(self, subscription_id: str) -> bool:
        """
        Stop price stream subscription.

        Args:
            subscription_id: ID returned by start_price_stream()

        Returns:
            True if successfully unsubscribed
        """
        pass

    # ==================== ORDER EXECUTION METHODS ====================

    @abstractmethod
    def place_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        price: float,
        reduce_only: bool = False,
        post_only: bool = True
    ) -> Dict[str, Any]:
        """
        Place a limit order (used for maker orders in XEMM).

        Args:
            symbol: Trading symbol
            side: OrderSide.BUY or OrderSide.SELL
            quantity: Order size in base currency
            price: Limit price
            reduce_only: Only reduce existing position (default: False)
            post_only: Only place as maker order, reject if would be taker (default: True)

        Returns:
            {
                "success": bool,
                "order_id": str,              # Exchange order ID
                "client_order_id": str,       # Client-side order ID for tracking
                "symbol": str,
                "side": str,                  # "buy" or "sell"
                "quantity": float,
                "price": float,
                "status": str,                # "open", "filled", "rejected"
                "timestamp": float,
                "error": Optional[str]        # Error message if failed
            }

        Implementation Notes:
            - Round price to exchange tick size before submission
            - Round quantity to exchange lot size
            - Generate unique client_order_id (UUID)
            - Sign request with private key
            - Handle rate limits with retries
        """
        pass

    @abstractmethod
    def place_market_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        reduce_only: bool = False,
        slippage_bps: int = 50
    ) -> Dict[str, Any]:
        """
        Execute immediate market order (used for hedging in XEMM).
        **CRITICAL**: This must be FAST - every millisecond counts for hedging.

        Args:
            symbol: Trading symbol
            side: OrderSide.BUY or OrderSide.SELL
            quantity: Order size
            reduce_only: Only reduce position (default: False)
            slippage_bps: Maximum acceptable slippage in basis points (default: 50 = 0.5%)

        Returns:
            Same format as place_limit_order(), plus:
            {
                "fill_price": float,          # Actual execution price
                "slippage_bps": float,        # Actual slippage experienced
                "latency_ms": float           # Time taken to execute
            }

        Implementation Notes:
            - Round quantity to lot size
            - Use IOC (Immediate or Cancel) time-in-force
            - Optimize for speed - this is the hedge execution
            - Measure and return latency
        """
        pass

    @abstractmethod
    def cancel_order(
        self,
        symbol: str,
        order_id: str = None,
        client_order_id: str = None
    ) -> Dict[str, Any]:
        """
        Cancel an open order.

        Args:
            symbol: Trading symbol
            order_id: Exchange order ID (provide this OR client_order_id)
            client_order_id: Client order ID (provide this OR order_id)

        Returns:
            {
                "success": bool,
                "order_id": str,
                "status": str,                # "cancelled", "already_filled", "not_found"
                "error": Optional[str]
            }
        """
        pass

    @abstractmethod
    def cancel_all_orders(self, symbol: str = None) -> Dict[str, Any]:
        """
        Cancel all open orders (emergency function).

        Args:
            symbol: If provided, cancel only this symbol. If None, cancel all symbols.

        Returns:
            {
                "success": bool,
                "cancelled_count": int,       # Number of orders cancelled
                "failed_count": int,          # Number that failed to cancel
                "error": Optional[str]
            }
        """
        pass

    # ==================== POSITION MANAGEMENT METHODS ====================

    @abstractmethod
    def get_position_ws(self, symbol: str, timeout: float = 10.0) -> Dict[str, Any]:
        """
        Get current position via WebSocket (primary method).

        Args:
            symbol: Trading symbol
            timeout: Timeout in seconds

        Returns:
            {
                "symbol": str,
                "quantity": float,            # Positive = long, Negative = short, 0 = flat
                "entry_price": float,         # Average entry price
                "mark_price": float,          # Current mark price
                "unrealized_pnl": float,      # Unrealized profit/loss in USD
                "notional": float,            # |quantity * mark_price|
                "leverage": float,            # Current leverage
                "liquidation_price": Optional[float],
                "timestamp": float
            }
        """
        pass

    @abstractmethod
    def get_position_rest(self, symbol: str) -> Dict[str, Any]:
        """
        Fallback method using REST API.

        Args:
            symbol: Trading symbol

        Returns:
            Same format as get_position_ws()
        """
        pass

    def get_position(self, symbol: str) -> Dict[str, Any]:
        """
        Convenience method with automatic fallback.

        Args:
            symbol: Trading symbol

        Returns:
            Position data (same format as get_position_ws())
        """
        try:
            return self.get_position_ws(symbol, timeout=2.0)
        except (ConnectionError, TimeoutError) as e:
            self.logger.warning(f"WebSocket failed for {symbol} position, using REST: {e}")
            return self.get_position_rest(symbol)

    @abstractmethod
    def start_fill_stream(self, callback: Callable) -> str:
        """
        **CRITICAL**: Monitor order fills in real-time for immediate hedging.
        This is the most important method for XEMM strategy.

        Args:
            callback: Function called when order fills (or partially fills)
                     Signature: callback(fill_data: Dict[str, Any])

        Fill data format:
            {
                "order_id": str,
                "client_order_id": str,
                "symbol": str,
                "side": str,                  # "buy" or "sell"
                "filled_quantity": float,     # Quantity filled in THIS event
                "fill_price": float,          # Actual execution price
                "fee": float,                 # Fee paid (positive)
                "is_maker": bool,             # True if maker, False if taker
                "timestamp": float,
                "fill_id": str                # Unique fill identifier
            }

        Returns:
            subscription_id: String ID for later unsubscription

        Implementation Notes:
            - Subscribe to user events/trades WebSocket
            - Parse fill events immediately
            - Call callback with minimal latency (< 50ms ideal)
            - Handle both full and partial fills
            - Auto-reconnect on disconnection
        """
        pass

    @abstractmethod
    def stop_fill_stream(self, subscription_id: str) -> bool:
        """
        Stop fill monitoring.

        Args:
            subscription_id: ID returned by start_fill_stream()

        Returns:
            True if successfully unsubscribed
        """
        pass

    # ==================== ACCOUNT INFO METHODS ====================

    @abstractmethod
    def get_equity(self) -> float:
        """
        Get total account equity in USD.

        Returns:
            Account equity = balance + unrealized PnL
        """
        pass

    @abstractmethod
    def get_balance(self, asset: str = "USDC") -> float:
        """
        Get available balance for specific asset.

        Args:
            asset: Asset symbol (default: "USDC")

        Returns:
            Available balance (not including margin/positions)
        """
        pass

    @abstractmethod
    def get_market_specs(self, symbol: str) -> Dict[str, Any]:
        """
        Get market specifications (can be cached, doesn't change often).

        Args:
            symbol: Trading symbol

        Returns:
            {
                "symbol": str,
                "tick_size": float,           # Minimum price increment
                "lot_size": float,            # Minimum quantity increment
                "min_notional": float,        # Minimum order value (usually $10)
                "max_leverage": int,          # Maximum leverage allowed
                "maker_fee": float,           # Maker fee (e.g., -0.0001 = -1bp rebate)
                "taker_fee": float            # Taker fee (e.g., 0.0004 = 4bp)
            }
        """
        pass

    @abstractmethod
    def get_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """
        Get all open orders.

        Args:
            symbol: If provided, filter by symbol. If None, return all.

        Returns:
            List of open orders:
            [{
                "order_id": str,
                "client_order_id": str,
                "symbol": str,
                "side": str,
                "quantity": float,
                "filled_quantity": float,
                "remaining_quantity": float,
                "price": float,
                "status": str,                # "open", "partially_filled"
                "timestamp": float
            }]
        """
        pass

    # ==================== UTILITY METHODS ====================

    @abstractmethod
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
        pass

    @abstractmethod
    def round_quantity(self, quantity: float, symbol: str) -> float:
        """
        Round quantity to valid lot size.

        Args:
            quantity: Raw quantity
            symbol: Trading symbol

        Returns:
            Properly rounded quantity (always rounds down to avoid over-trading)
        """
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if WebSocket connections are healthy.

        Returns:
            True if all WebSocket connections are active
        """
        pass

    @abstractmethod
    def reconnect(self) -> bool:
        """
        Attempt to reconnect WebSocket connections.

        Returns:
            True if reconnection successful
        """
        pass
