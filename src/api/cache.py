"""In-memory TTL cache utility for FastAPI endpoints and services."""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable
from threading import RLock
from typing import TypeVar

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., object])


class InMemoryTTLCache:
    """Thread-safe lightweight In-Memory TTL Cache."""

    def __init__(self, default_ttl_seconds: float = 300.0, max_entries: int = 500) -> None:
        """Initialize in-memory cache instance."""
        self.default_ttl = default_ttl_seconds
        self.max_entries = max_entries
        self._cache: dict[str, tuple[float, object]] = {}
        self._lock = RLock()

    def get(self, key: str) -> object | None:
        """Retrieve value by key if not expired."""
        with self._lock:
            if key not in self._cache:
                return None
            expires_at, value = self._cache[key]
            if time.monotonic() >= expires_at:
                self._cache.pop(key, None)
                return None
            return value

    def set(self, key: str, value: object, ttl_seconds: float | None = None) -> None:
        """Store value with TTL."""
        with self._lock:
            if len(self._cache) >= self.max_entries:
                # Remove expired or arbitrary first key
                now = time.monotonic()
                expired_keys = [k for k, (exp, _) in self._cache.items() if now >= exp]
                if expired_keys:
                    for expired_key in expired_keys:
                        self._cache.pop(expired_key, None)
                elif self._cache:
                    self._cache.pop(next(iter(self._cache)))

            ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl
            self._cache[key] = (time.monotonic() + ttl, value)

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()


# Global API Cache instance
api_cache = InMemoryTTLCache(default_ttl_seconds=300.0, max_entries=1000)


def cached_api(ttl_seconds: float = 300.0, key_prefix: str = "") -> Callable[[F], F]:
    """Cache API endpoint return values with a given TTL.

    Args:
        ttl_seconds: Cache validity duration in seconds.
        key_prefix: Prefix for unique cache namespace.

    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: object, **kwargs: object) -> object:
            # Include positional and keyword arguments to prevent direct-call collisions.
            clean_kwargs = {k: v for k, v in kwargs.items() if not str(type(v)).startswith("<class 'fastapi.")}
            sorted_kwargs = sorted(clean_kwargs.items(), key=lambda item: item[0])
            cache_key = f"{key_prefix or func.__name__}:{args!r}:{sorted_kwargs!r}"
            cached_val = api_cache.get(cache_key)
            if cached_val is not None:
                logger.debug("Cache hit for key: %s", cache_key)
                return cached_val

            result = func(*args, **kwargs)
            api_cache.set(cache_key, result, ttl_seconds=ttl_seconds)
            return result

        return wrapper  # type: ignore[return-value]

    return decorator
