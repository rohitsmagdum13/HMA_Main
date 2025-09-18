"""
Centralized logging configuration for the HMA ingestion system.

Provides functions to:
- Retrieve a standardized logger with console + rotating file handlers.
- Configure the root logger for third-party libraries.

Log format includes timestamp, severity, module, function, line number,
and message. Rotation ensures logs don’t grow indefinitely.
"""
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional

from .settings import settings

# Track configured loggers to avoid duplicate configuration
_configured_loggers = set()


def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger with standardized configuration.

    Args:
        name: Logger name (typically `__name__` of the module).

    Returns:
        Configured logger instance with console and rotating file handlers.
    """
    # Check if logger already configured
    if name in _configured_loggers:
        return logging.getLogger(name)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(settings.log_level)
    
    # Prevent duplicate handlers when logger is retrieved multiple times
    if logger.hasHandlers():
        return logger
    
    # Create log directory if it doesn't exist
    settings.log_dir.mkdir(exist_ok=True)
    
    # Define log format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console handler - outputs to stdout
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(settings.log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler - rotating log file
    log_file_path = settings.log_dir / settings.log_file
    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=10_485_760,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(settings.log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Mark logger as configured
    _configured_loggers.add(name)
    
    return logger


def setup_root_logger():
    """Configure the root logger for libraries that use it."""
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )