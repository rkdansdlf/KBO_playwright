"""Unit tests for AsyncCircuitBreaker lifecycle, transitions, and fault isolation."""

from __future__ import annotations

import asyncio
import time
import pytest

from src.api.circuit_breaker import AsyncCircuitBreaker, CircuitOpenError
from src.api.live_stream_dto import CircuitState


@pytest.mark.asyncio
async def test_circuit_breaker_initial_state_and_success() -> None:
    """Test circuit breaker starts CLOSED and handles successful executions."""
    cb = AsyncCircuitBreaker(name="test_cb", failure_threshold=2, recovery_timeout_seconds=0.1)
    assert cb.state == CircuitState.CLOSED

    async def successful_call() -> str:
        return "success"

    result = await cb.call_async(successful_call)
    assert result == "success"
    status = cb.get_status()
    assert status.success_count == 1
    assert status.failure_count == 0
    assert status.consecutive_failures == 0


@pytest.mark.asyncio
async def test_circuit_breaker_trips_to_open_on_threshold() -> None:
    """Test circuit breaker transitions to OPEN after consecutive failures reach threshold."""
    cb = AsyncCircuitBreaker(name="test_cb_trip", failure_threshold=2, recovery_timeout_seconds=0.1)

    async def failing_call() -> None:
        raise ValueError("Simulated network outage")

    # 1st failure
    with pytest.raises(ValueError, match="Simulated network outage"):
        await cb.call_async(failing_call)
    assert cb.state == CircuitState.CLOSED
    assert cb.get_status().consecutive_failures == 1

    # 2nd failure -> reaches threshold 2 -> TRIPS TO OPEN
    with pytest.raises(ValueError, match="Simulated network outage"):
        await cb.call_async(failing_call)
    assert cb.state == CircuitState.OPEN
    assert cb.get_status().consecutive_failures == 2

    # Subsequent call immediately rejected with CircuitOpenError
    with pytest.raises(CircuitOpenError) as exc_info:
        await cb.call_async(failing_call)
    assert "is OPEN" in str(exc_info.value)
    assert cb.get_status().rejection_count == 1


@pytest.mark.asyncio
async def test_circuit_breaker_half_open_recovery() -> None:
    """Test circuit transitions to HALF_OPEN after timeout and closes on trial success."""
    cb = AsyncCircuitBreaker(
        name="test_cb_recovery",
        failure_threshold=1,
        recovery_timeout_seconds=0.05,
        half_open_success_threshold=1,
    )

    # Trip to OPEN
    async def fail() -> None:
        raise RuntimeError("Service down")

    with pytest.raises(RuntimeError):
        await cb.call_async(fail)
    assert cb.state == CircuitState.OPEN

    # Wait for cooldown
    await asyncio.sleep(0.06)
    assert cb.state == CircuitState.HALF_OPEN

    # Trial probe succeeds
    async def succeed() -> str:
        return "restored"

    res = await cb.call_async(succeed)
    assert res == "restored"
    assert cb.state == CircuitState.CLOSED
    assert cb.get_status().consecutive_failures == 0


@pytest.mark.asyncio
async def test_circuit_breaker_half_open_re_trips_on_failure() -> None:
    """Test trial probe failure in HALF_OPEN immediately re-trips to OPEN."""
    cb = AsyncCircuitBreaker(
        name="test_cb_probe_fail",
        failure_threshold=1,
        recovery_timeout_seconds=0.05,
    )

    async def fail() -> None:
        raise RuntimeError("Fail")

    with pytest.raises(RuntimeError):
        await cb.call_async(fail)
    assert cb.state == CircuitState.OPEN

    await asyncio.sleep(0.06)
    assert cb.state == CircuitState.HALF_OPEN

    # Trial probe fails
    with pytest.raises(RuntimeError):
        await cb.call_async(fail)
    assert cb.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_circuit_breaker_fallback() -> None:
    """Test fallback is executed when circuit is OPEN."""

    async def fallback_handler(*args, **kwargs) -> str:
        return "fallback_payload"

    cb = AsyncCircuitBreaker(
        name="test_cb_fallback",
        failure_threshold=1,
        recovery_timeout_seconds=10.0,
        fallback=fallback_handler,
    )

    async def fail() -> None:
        raise RuntimeError("Fail")

    # First call fails and returns fallback
    res = await cb.call_async(fail)
    assert res == "fallback_payload"
    assert cb.state == CircuitState.OPEN

    # Subsequent call while OPEN also returns fallback
    res2 = await cb.call_async(fail)
    assert res2 == "fallback_payload"


@pytest.mark.asyncio
async def test_circuit_breaker_manual_trip_and_reset() -> None:
    """Test administrative manual trip and reset controls."""
    cb = AsyncCircuitBreaker(name="test_cb_admin", failure_threshold=5)
    assert cb.state == CircuitState.CLOSED

    # Manual trip
    new_state = await cb.trip("Maintenance isolation")
    assert new_state == CircuitState.OPEN
    assert cb.state == CircuitState.OPEN
    assert cb.get_status().last_failure_reason == "Maintenance isolation"

    # Manual reset
    new_state = await cb.reset()
    assert new_state == CircuitState.CLOSED
    assert cb.state == CircuitState.CLOSED
    assert cb.get_status().last_failure_reason is None


@pytest.mark.asyncio
async def test_circuit_breaker_decorator_protect() -> None:
    """Test protecting an async function via @cb.protect decorator."""
    cb = AsyncCircuitBreaker(name="test_decorator", failure_threshold=1, recovery_timeout_seconds=0.1)

    @cb.protect
    async def sample_endpoint(value: int) -> int:
        if value < 0:
            raise ValueError("Negative value not allowed")
        return value * 2

    # Success
    assert await sample_endpoint(5) == 10

    # Failure trips circuit
    with pytest.raises(ValueError):
        await sample_endpoint(-1)
    assert cb.state == CircuitState.OPEN

    # Rejected call
    with pytest.raises(CircuitOpenError):
        await sample_endpoint(10)
