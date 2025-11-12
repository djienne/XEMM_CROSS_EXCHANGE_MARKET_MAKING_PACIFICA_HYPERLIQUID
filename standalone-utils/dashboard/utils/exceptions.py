"""
Custom exceptions for XEMM bot.

This module defines all custom exceptions used throughout the XEMM strategy
to provide clear error handling and meaningful error messages.
"""


class XEMMException(Exception):
    """Base exception for all XEMM-related errors"""
    pass


# ==================== Connection Exceptions ====================

class ConnectionError(XEMMException):
    """WebSocket or REST connection failed"""
    pass


class WebSocketConnectionError(ConnectionError):
    """Specific WebSocket connection failure"""
    pass


class RESTConnectionError(ConnectionError):
    """Specific REST API connection failure"""
    pass


class TimeoutError(XEMMException):
    """Operation timed out"""
    pass


class ReconnectionError(ConnectionError):
    """Failed to reconnect after multiple attempts"""
    pass


# ==================== Authentication Exceptions ====================

class AuthenticationError(XEMMException):
    """Invalid credentials or signature"""
    pass


class InvalidPrivateKeyError(AuthenticationError):
    """Private key is invalid or malformed"""
    pass


class SignatureError(AuthenticationError):
    """Failed to sign transaction or request"""
    pass


# ==================== Exchange Exceptions ====================

class ExchangeError(XEMMException):
    """General exchange-related error"""
    pass


class RateLimitError(ExchangeError):
    """Exchange rate limit exceeded"""
    pass


class InsufficientBalanceError(ExchangeError):
    """Not enough funds for order"""
    pass


class InvalidOrderError(ExchangeError):
    """Order parameters invalid (price/quantity/symbol)"""
    pass


class OrderNotFoundError(ExchangeError):
    """Order ID doesn't exist"""
    pass


class OrderRejectedError(ExchangeError):
    """Exchange rejected the order"""
    pass


class PositionNotFoundError(ExchangeError):
    """Position doesn't exist for symbol"""
    pass


# ==================== Market Data Exceptions ====================

class MarketDataError(XEMMException):
    """General market data error"""
    pass


class InvalidSymbolError(MarketDataError):
    """Trading symbol not found or invalid"""
    pass


class NoMarketDataError(MarketDataError):
    """No market data available (orderbook empty, etc.)"""
    pass


class StaleDataError(MarketDataError):
    """Market data is stale/outdated"""
    pass


# ==================== Configuration Exceptions ====================

class ConfigurationError(XEMMException):
    """Configuration file error"""
    pass


class InvalidConfigError(ConfigurationError):
    """Config parameters are invalid"""
    pass


class MissingConfigError(ConfigurationError):
    """Required config parameter missing"""
    pass


class MissingEnvironmentVariableError(ConfigurationError):
    """Required environment variable not set"""
    pass


# ==================== Strategy Exceptions ====================

class StrategyError(XEMMException):
    """General strategy execution error"""
    pass


class NoOpportunityError(StrategyError):
    """No profitable opportunity found"""
    pass


class InvalidOpportunityError(StrategyError):
    """Opportunity data is malformed or invalid"""
    pass


class HedgeExecutionError(StrategyError):
    """Failed to execute hedge order"""
    pass


class UnhedgedPositionError(StrategyError):
    """Critical: Position exists without corresponding hedge"""
    pass


# ==================== Risk Management Exceptions ====================

class RiskLimitError(XEMMException):
    """Risk limit exceeded"""
    pass


class PositionLimitError(RiskLimitError):
    """Position size exceeds maximum allowed"""
    pass


class LossLimitError(RiskLimitError):
    """Loss exceeds circuit breaker threshold"""
    pass


class EquityTooLowError(RiskLimitError):
    """Account equity below minimum required"""
    pass


class MaxDrawdownError(RiskLimitError):
    """Maximum drawdown threshold exceeded"""
    pass


# ==================== Data Validation Exceptions ====================

class ValidationError(XEMMException):
    """Data validation failed"""
    pass


class InvalidPriceError(ValidationError):
    """Price value is invalid (negative, zero, etc.)"""
    pass


class InvalidQuantityError(ValidationError):
    """Quantity value is invalid"""
    pass


class MinNotionalError(ValidationError):
    """Order value below minimum notional"""
    pass


# ==================== Monitoring Exceptions ====================

class MonitoringError(XEMMException):
    """Monitoring system error"""
    pass


class LoggingError(MonitoringError):
    """Failed to write log"""
    pass


class MetricsError(MonitoringError):
    """Failed to record metrics"""
    pass
