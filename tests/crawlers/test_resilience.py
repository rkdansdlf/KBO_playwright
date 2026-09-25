"""Unit tests for src.crawlers.resilience."""

from __future__ import annotations

import pytest

from src.crawlers.resilience import AdaptiveRateLimiter


@pytest.mark.asyncio
async def test_adaptive_rate_limiter_waits_the_configured_delay() -> None:
    limiter = AdaptiveRateLimiter(
        base_delay_seconds=0.01,
        min_delay_seconds=0.005,
        max_delay_seconds=0.1,
    )

    waited = await limiter.acquire()
    assert waited >= 0.005


@pytest.mark.asyncio
async def test_adaptive_rate_limiter_honours_server_retry_after() -> None:
    limiter = AdaptiveRateLimiter(base_delay_seconds=0.01, max_delay_seconds=0.1)

    # A server-provided delay above max_delay must still be honoured: clamping
    # it would send us back sooner than the server asked and earn another 429.
    limiter.record_rate_limit(retry_after=45.0)
    assert limiter.current_delay == 45.0


@pytest.mark.asyncio
async def test_adaptive_rate_limiter_backoff_is_bounded_without_a_hint() -> None:
    limiter = AdaptiveRateLimiter(base_delay_seconds=0.01, max_delay_seconds=0.1)

    for _ in range(10):
        limiter.record_rate_limit()

    assert limiter.current_delay == pytest.approx(0.1)


@pytest.mark.asyncio
async def test_adaptive_rate_limiter_relaxes_after_success() -> None:
    limiter = AdaptiveRateLimiter(
        base_delay_seconds=0.01,
        min_delay_seconds=0.005,
        max_delay_seconds=0.1,
    )

    limiter.record_rate_limit(retry_after=0.05)
    assert limiter.current_delay == 0.05

    limiter.record_success()
    assert limiter.current_delay < 0.05
