"""Structured logging.

JSON in prod so Airflow task logs stay greppable and can be shipped to a log
sink; human readable colours locally.
"""

from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(level: str = "INFO", json_logs: bool = False) -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level.upper())

    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer()
        if json_logs
        else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(level.upper())
        ),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)  # type: ignore[no-any-return]
