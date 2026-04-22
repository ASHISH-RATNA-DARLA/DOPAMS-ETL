from __future__ import annotations

import logging
import os
import sys
import uuid


_RUN_ID = os.environ.get("RUN_ID") or uuid.uuid4().hex[:12]


def run_id() -> str:
    return _RUN_ID


def _resolve_level(level: int | None) -> int:
    if level is not None:
        return level

    raw = os.environ.get("ADDRESS_LOG_LEVEL") or os.environ.get("LOGLEVEL") or "INFO"
    if isinstance(raw, str) and raw.isdigit():
        return int(raw)
    return getattr(logging, str(raw).upper(), logging.INFO)


def setup_logger(name: str = "etl-address", level: int | None = None) -> logging.Logger:
    resolved_level = _resolve_level(level)
    fmt = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - [run=" + _RUN_ID + "] - %(name)s - %(message)s"
    )

    root = logging.getLogger()
    root.setLevel(resolved_level)
    if not root.handlers:
        root_handler = logging.StreamHandler(sys.stdout)
        root_handler.setFormatter(fmt)
        root.addHandler(root_handler)

    logger = logging.getLogger(name)
    if logger.handlers:
        logger.setLevel(resolved_level)
        return logger

    logger.setLevel(resolved_level)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(fmt)
    logger.addHandler(h)
    logger.propagate = False
    return logger
