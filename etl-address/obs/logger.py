from __future__ import annotations

import logging
import os
import sys
import uuid


_RUN_ID = os.environ.get("RUN_ID") or uuid.uuid4().hex[:12]


def run_id() -> str:
    return _RUN_ID


def setup_logger(name: str = "etl-address", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(level)
    fmt = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - [run=" + _RUN_ID + "] - %(name)s - %(message)s"
    )
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(fmt)
    logger.addHandler(h)
    logger.propagate = False
    return logger
