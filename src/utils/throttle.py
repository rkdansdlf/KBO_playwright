from __future__ import annotations

import asyncio
import logging
import os
import random
import threading
import time

logger = logging.getLogger(__name__)


class AsyncThrottle:
    """A centralized throttling service with random jitter to prevent IP blocks.
    Respects global delays configured via environment variables.
    """

    def __init__(self, delay: float = 1.0, jitter: float = 0.3) -> None:
        # Override with env vars if present
        env_delay = os.getenv("KBO_REQUEST_DELAY")
        env_jitter = os.getenv("KBO_REQUEST_JITTER")

        self._default_delay = float(env_delay) if env_delay is not None else delay
        self.jitter = float(env_jitter) if env_jitter is not None else jitter
        self._last_request_times: dict[str, float] = {}
        self._locks: dict[int, asyncio.Lock] = {}
        self._locks_lock = threading.Lock()

    def _get_lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        loop_id = id(loop)
        if loop_id not in self._locks:
            with self._locks_lock:
                if loop_id not in self._locks:
                    self._locks[loop_id] = asyncio.Lock()
        return self._locks[loop_id]

    @property
    def default_delay(self) -> float:
        return self._default_delay

    @default_delay.setter
    def default_delay(self, val: float) -> None:
        self._default_delay = val

    def _get_target_delay(self) -> float:
        current_jitter = random.uniform(-self.jitter / 2, self.jitter)
        return max(0.0, self._default_delay + current_jitter)

    async def wait(self, host: str = "koreabaseball.com") -> None:
        """Wait for the required delay plus jitter since the last request to this host.
        Safe for concurrent use.
        """
        lock = self._get_lock()
        async with lock:
            now = time.monotonic()
            last_time = self._last_request_times.get(host, 0.0)
            elapsed = now - last_time

            target_delay = self._get_target_delay()
            sleep_time = target_delay - elapsed

            if sleep_time > 0:
                if sleep_time > self._default_delay * 5:
                    logger.warning("Heavy throttling for %s: sleeping for %.2fs", host, sleep_time)
                await asyncio.sleep(sleep_time)

            self._last_request_times[host] = time.monotonic()

    def wait_sync(self, host: str = "koreabaseball.com") -> None:
        """Synchronous version of wait.
        """
        now = time.monotonic()
        last_time = self._last_request_times.get(host, 0.0)
        elapsed = now - last_time

        target_delay = self._get_target_delay()
        sleep_time = target_delay - elapsed

        if sleep_time > 0:
            time.sleep(sleep_time)

        self._last_request_times[host] = time.monotonic()


# Global instance for shared rate limiting across concurrent crawlers
throttle = AsyncThrottle()
