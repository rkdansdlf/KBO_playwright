"""Unit tests for the API in-memory cache."""

from __future__ import annotations

from src.api.cache import InMemoryTTLCache, api_cache, cached_api


def test_ttl_cache_expires_entries() -> None:
    """Test that an entry is unavailable after its TTL."""
    cache = InMemoryTTLCache()
    cache.set("key", "value", ttl_seconds=0)

    assert cache.get("key") is None


def test_cached_api_includes_positional_arguments() -> None:
    """Test that direct positional calls cannot share a cache key."""
    api_cache.clear()
    calls: list[int] = []

    @cached_api(ttl_seconds=300, key_prefix="test")
    def double(value: int) -> int:
        calls.append(value)
        return value * 2

    assert double(1) == 2
    assert double(2) == 4
    assert calls == [1, 2]
    api_cache.clear()
