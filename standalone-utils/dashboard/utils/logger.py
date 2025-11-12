"""
Centralized logging configuration for XEMM bot.

This module provides a consistent logging setup across all components
with separate log files for different purposes (strategy, trades, errors, debug).
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime
from logging.handlers import RotatingFileHandler


# Log levels
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}


def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: str = "INFO",
    log_to_console: bool = True,
    log_to_file: bool = True
) -> logging.Logger:
    """
    Set up a logger with consistent formatting.

    Args:
        name: Logger name (typically __name__ of the module)
        log_file: Path to log file (relative to logs/ directory)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_console: Whether to output to console
        log_to_file: Whether to output to file

    Returns:
        Configured logger instance
    """
    # Check environment variable for global log level override
    import os
    env_log_level = os.getenv('LOG_LEVEL', '').upper()
    if env_log_level and env_log_level in LOG_LEVELS:
        effective_level = env_log_level
    else:
        effective_level = level.upper()

    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVELS.get(effective_level, logging.INFO))

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(LOG_LEVELS.get(effective_level, logging.INFO))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # File handler
    if log_to_file and log_file:
        # Ensure logs directory exists
        log_dir = Path(__file__).parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)

        file_path = log_dir / log_file

        # Use RotatingFileHandler to limit log file size
        # maxBytes=10MB keeps ~10 minutes of DEBUG logs (efficient, no CPU overhead)
        # backupCount=2 keeps 2 old files (total ~30 minutes of history)
        file_handler = RotatingFileHandler(
            file_path,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=2,
            encoding='utf-8'
        )
        file_handler.setLevel(LOG_LEVELS.get(effective_level, logging.INFO))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_strategy_logger(level: str = "INFO") -> logging.Logger:
    """
    Get the main strategy logger.

    Logs to: logs/strategy.log
    """
    return setup_logger(
        name="xemm.strategy",
        log_file="strategy.log",
        level=level
    )


def get_trade_logger(level: str = "INFO") -> logging.Logger:
    """
    Get the trade logger for recording all trades.

    Logs to: logs/trades.log
    """
    return setup_logger(
        name="xemm.trades",
        log_file="trades.log",
        level=level
    )


def get_error_logger(level: str = "ERROR") -> logging.Logger:
    """
    Get the error logger for recording errors only.

    Logs to: logs/errors.log
    """
    return setup_logger(
        name="xemm.errors",
        log_file="errors.log",
        level=level
    )


def get_debug_logger(level: str = "DEBUG") -> logging.Logger:
    """
    Get the debug logger for detailed debugging information.

    Logs to: logs/debug.log
    """
    return setup_logger(
        name="xemm.debug",
        log_file="debug.log",
        level=level
    )


def get_connector_logger(exchange_name: str, level: str = "INFO") -> logging.Logger:
    """
    Get a logger for exchange connector.

    Args:
        exchange_name: "hyperliquid" or "pacifica"
        level: Log level

    Logs to: logs/{exchange_name}_connector.log
    """
    return setup_logger(
        name=f"xemm.connector.{exchange_name}",
        log_file=f"{exchange_name}_connector.log",
        level=level
    )


def log_trade(
    logger: logging.Logger,
    symbol: str,
    maker_exchange: str,
    taker_exchange: str,
    maker_side: str,
    taker_side: str,
    entry_price: float,
    exit_price: float,
    quantity: float,
    profit_usd: float,
    profit_bps: float,
    fees_usd: float,
    hedge_latency_ms: float,
):
    """
    Log a completed trade with all details.

    Args:
        logger: Logger instance (typically trade_logger)
        symbol: Trading symbol (e.g., "BTC")
        maker_exchange: Exchange where limit order was placed
        taker_exchange: Exchange where hedge was executed
        side: Order side ("buy" or "sell")
        entry_price: Maker fill price
        exit_price: Taker hedge price
        quantity: Quantity traded
        profit_usd: Realized profit in USD
        profit_bps: Realized profit in basis points
        fees_usd: Total fees paid
        hedge_latency_ms: Time from fill to hedge in milliseconds
    """
    logger.info(
        f"TRADE | {symbol} | "
        f"Maker: {maker_exchange} {maker_side.upper()} @ {entry_price:.2f} | "
        f"Taker: {taker_exchange} {taker_side.upper()} @ {exit_price:.2f} | "
        f"Qty: {quantity:.4f} | "
        f"Profit: ${profit_usd:.2f} ({profit_bps:.2f} bps) | "
        f"Fees: ${fees_usd:.2f} | "
        f"Hedge Latency: {hedge_latency_ms:.0f}ms"
    )


def log_opportunity(
    logger: logging.Logger,
    symbol: str,
    direction: str,
    maker_exchange: str,
    taker_exchange: str,
    profit_bps: float,
    maker_price: float,
    taker_price: float
):
    """
    Log a profitable opportunity detected.

    Args:
        logger: Logger instance
        symbol: Trading symbol
        direction: Opportunity direction description
        maker_exchange: Where to place limit order
        taker_exchange: Where to hedge
        profit_bps: Expected profit in basis points
        maker_price: Calculated limit order price
        taker_price: Expected hedge price
    """
    logger.info(
        f"OPPORTUNITY | {symbol} | {direction} | "
        f"Maker: {maker_exchange} @ {maker_price:.2f} | "
        f"Taker: {taker_exchange} @ {taker_price:.2f} | "
        f"Profit: {profit_bps:.2f} bps"
    )


def set_global_log_level(level: str = "INFO"):
    """
    Reconfigure all existing loggers to use the specified log level.

    This is useful for dynamically changing log verbosity (e.g., DEBUG, INFO, ERROR)
    without restarting the application.

    Args:
        level: Log level to set (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    log_level = LOG_LEVELS.get(level.upper(), logging.INFO)

    # Update all XEMM loggers
    for logger_name in logging.root.manager.loggerDict:
        if logger_name.startswith("xemm."):
            logger = logging.getLogger(logger_name)
            logger.setLevel(log_level)

            # Update all handlers for this logger
            for handler in logger.handlers:
                handler.setLevel(log_level)

    # Silence noisy third-party loggers when in ERROR mode
    if level.upper() == "ERROR":
        # Silence common noisy libraries
        noisy_loggers = [
            "websockets",
            "websockets.client",
            "websockets.protocol",
            "urllib3",
            "urllib3.connectionpool",
            "requests",
            "hyperliquid",
            "asyncio"
        ]
        for logger_name in noisy_loggers:
            logging.getLogger(logger_name).setLevel(logging.ERROR)

    print(f"Global log level set to: {level.upper()}")


# Create default loggers
strategy_logger = get_strategy_logger()
trade_logger = get_trade_logger()
error_logger = get_error_logger()
debug_logger = get_debug_logger()
