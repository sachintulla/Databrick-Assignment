"""
Centralized logging configuration for the SuperStore Analytics System.
Provides a structured logger with file and console handlers.
"""

import logging
import os
from datetime import datetime
from pathlib import Path


def get_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    """
    Create and return a configured logger instance.

    Args:
        name: Logger name (typically the module __name__).
        log_dir: Directory where log files will be written.

    Returns:
        Configured logging.Logger instance.
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)

    # Avoid duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler — INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # File handler — DEBUG and above, rotated daily by date
    log_filename = os.path.join(
        log_dir, f"superstore_{datetime.now().strftime('%Y%m%d')}.log"
    )
    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
