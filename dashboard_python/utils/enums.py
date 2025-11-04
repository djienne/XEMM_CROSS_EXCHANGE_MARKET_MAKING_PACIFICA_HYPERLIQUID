"""
Shared enums for XEMM bot.

This module defines all enums used across the XEMM strategy to ensure
type safety and consistent values throughout the codebase.
"""

from enum import Enum


class OrderSide(Enum):
    """Order side enum - buy or sell"""
    BUY = "buy"
    SELL = "sell"

    def opposite(self) -> 'OrderSide':
        """Return the opposite side"""
        return OrderSide.SELL if self == OrderSide.BUY else OrderSide.BUY

    def __str__(self) -> str:
        return self.value


class OrderStatus(Enum):
    """Order status enum"""
    PENDING = "pending"           # Order submitted but not yet confirmed
    OPEN = "open"                 # Order confirmed and active
    PARTIALLY_FILLED = "partially_filled"  # Order partially executed
    FILLED = "filled"             # Order fully executed
    CANCELLED = "cancelled"       # Order cancelled by user
    REJECTED = "rejected"         # Order rejected by exchange
    EXPIRED = "expired"           # Order expired (time-based)

    def __str__(self) -> str:
        return self.value


class ExchangeName(Enum):
    """Exchange identifier enum"""
    HYPERLIQUID = "hyperliquid"
    PACIFICA = "pacifica"

    def __str__(self) -> str:
        return self.value


class OrderType(Enum):
    """Order type enum"""
    LIMIT = "limit"               # Limit order (maker)
    MARKET = "market"             # Market order (taker)
    STOP_LOSS = "stop_loss"       # Stop loss order
    TAKE_PROFIT = "take_profit"   # Take profit order

    def __str__(self) -> str:
        return self.value


class TimeInForce(Enum):
    """Time-in-force enum for orders"""
    GTC = "gtc"                   # Good till cancelled
    IOC = "ioc"                   # Immediate or cancel (for market orders)
    FOK = "fok"                   # Fill or kill
    ALO = "alo"                   # Add liquidity only (post-only for makers)

    def __str__(self) -> str:
        return self.value


class StrategyState(Enum):
    """Strategy state enum"""
    STARTING = "starting"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"

    def __str__(self) -> str:
        return self.value
