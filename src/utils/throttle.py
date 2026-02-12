"""
Shared throttle service to enforce per-host request spacing.
Supports both synchronous and asynchronous usage.
"""
import asyncio
import random
import time
from typing import Dict, Optional


class AsyncThrottle:
    """
    A per-host throttle that ensures a minimum delay between requests to the same host.
    Uses a singleton-like pattern or shared instance to coordinate across crawlers.
    """
    _instance: Optional["AsyncThrottle"] = None

    def __init__(self, default_delay: float = 3.0, jitter: float = 0.5):
        self.default_delay = default_delay
        self.jitter = jitter
        self.last_request_time: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._sync_lock = None # Will be initialized if needed for sync usage

    @classmethod
    def get_instance(cls) -> "AsyncThrottle":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _get_delay(self, host: str) -> float:
        """Calculate the required delay for a host."""
        now = time.time()
        last_time = self.last_request_time.get(host, 0)
        
        # Add random jitter to the delay
        actual_delay = self.default_delay + random.uniform(-self.jitter, self.jitter)
        wait_time = max(0, last_time + actual_delay - now)
        return wait_time

    async def wait(self, host: str = "default"):
        """Async wait until the host is ready for another request."""
        async with self._lock:
            wait_time = self._get_delay(host)
            if wait_time > 0:
                print(f"[THROTTLE] Waiting {wait_time:.2f}s for {host}...")
                await asyncio.sleep(wait_time)
            self.last_request_time[host] = time.time()

    def wait_sync(self, host: str = "default"):
        """Synchronous wait until the host is ready for another request."""
        # Note: In a mixed sync/async environment, this might block the event loop 
        # if not called carefully. But for pure sync crawlers, it's fine.
        wait_time = self._get_delay(host)
        if wait_time > 0:
            print(f"[THROTTLE] Sync Waiting {wait_time:.2f}s for {host}...")
            time.sleep(wait_time)
        self.last_request_time[host] = time.time()

# Shared global throttle instance
throttle = AsyncThrottle.get_instance()
