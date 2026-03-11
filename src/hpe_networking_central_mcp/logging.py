"""Structured JSON logging via structlog."""

from __future__ import annotations

import logging
import sys

import structlog


def setup_logging(level: str = "INFO") -> structlog.stdlib.BoundLogger:
    """Configure structlog for JSON output to stdout."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )

    # Silence noisy libraries
    logging.getLogger("ansible").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return structlog.get_logger("hpe-networking-central-mcp")
