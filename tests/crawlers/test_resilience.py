"""Unit tests for src.crawlers.resilience."""

from __future__ import annotations

import asyncio

import pytest

from src.crawlers.resilience import (
    AdaptiveRateLimiter,
    CircuitBreaker,
    CircuitBreakerState,
)


@pytest.mark.asyncio
async def test_circuit_breaker_transitions() -> None:
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout_seconds=0.05, name="test")
    cb.last_state_change = asyncio.get_event_loop().time()

    assert cb.allow_request() is True
    assert cb.state == CircuitBreakerState.CLOSED

    cb.record_failure()
    assert cb.state == CircuitBreakerState.CLOSED

    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    assert cb.allow_request() is False

    # Simulate timeout recovery
    cb.last_state_change -= 0.1
    assert cb.allow_request() is True
    assert cb.state == CircuitBreakerState.HALF_OPEN

    # Success closes circuit
    cb.record_success()
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb.allow_request() is True


@pytest.mark.asyncio
async def test_adaptive_rate_limiter() -> None:
    limiter = AdaptiveRateLimiter(
        base_delay_seconds=0.01,
        min_delay_seconds=0.005,
        max_delay_seconds=0.1,
    )

    waited = await limiter.acquire()
    assert waited >= 0.005

    limiter.record_rate_limit(retry_after=0.05)
    assert limiter.current_delay == 0.05

    limiter.record_success()
    assert limiter.current_delay < 0.05
