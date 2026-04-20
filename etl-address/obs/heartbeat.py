from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class Heartbeat:
    def __init__(self, interval_sec: int, emit: Callable[[], str]) -> None:
        self.interval = max(5, int(interval_sec))
        self.emit = emit
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="etl-address-heartbeat", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                msg = self.emit()
                if msg:
                    logger.info("HEARTBEAT %s", msg)
            except Exception as exc:
                logger.warning("heartbeat emit failed: %s", exc)
