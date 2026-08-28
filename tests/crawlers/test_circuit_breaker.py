"""Unit and integration tests for KBO Crawler Smart Circuit Breaker."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from src.crawlers.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerRegistry,
    circuit_breaker,
    circuit_registry,
)
from src.crawlers.circuit_breaker_dto import CircuitOpenError, CircuitState
from src.diagnostics.dto import DiagnosticSeverity
from src.diagnostics.engine import SystemDiagnosticsEngine


def test_circuit_breaker_closed_on_success() -> None:
    """Test circuit breaker remains CLOSED on successful execution."""
    cb = CircuitBreaker(name="test_success", failure_threshold=3, recovery_timeout_seconds=5.0)

    def success_fn(x: int) -> int:
        return x * 2

    res = cb.call(success_fn, 5)
    assert res == 10
    assert cb.get_state() == CircuitState.CLOSED
    stats = cb.get_stats()
    assert stats.success_count == 1
    assert stats.failure_count == 0
    assert stats.consecutive_failures == 0


def test_circuit_breaker_trips_to_open_on_threshold_failures() -> None:
    """Test circuit breaker trips from CLOSED to OPEN after consecutive threshold failures."""
    cb = CircuitBreaker(name="test_trips", failure_threshold=2, recovery_timeout_seconds=5.0)

    def failing_fn() -> None:
        msg = "Simulated network timeout"
        raise ConnectionError(msg)

    # 1st failure: remains CLOSED
    with pytest.raises(ConnectionError):
        cb.call(failing_fn)
    assert cb.get_state() == CircuitState.CLOSED
    assert cb.get_stats().consecutive_failures == 1

    # 2nd failure: threshold reached -> trips to OPEN
    with pytest.raises(ConnectionError):
        cb.call(failing_fn)
    assert cb.get_state() == CircuitState.OPEN
    assert cb.get_stats().consecutive_failures == 2

    # 3rd attempt: blocked by CircuitOpenError
    with pytest.raises(CircuitOpenError) as exc_info:
        cb.call(failing_fn)
    assert "Circuit breaker 'test_trips' is OPEN" in str(exc_info.value)


def test_circuit_breaker_fallback_execution() -> None:
    """Test circuit breaker automatically executes fallback when OPEN."""
    mock_fallback = MagicMock(return_value={"source": "cached_snapshot", "data": [1, 2, 3]})
    cb = CircuitBreaker(
        name="test_fallback",
        failure_threshold=1,
        recovery_timeout_seconds=10.0,
        fallback=mock_fallback,
    )

    def failing_fn() -> None:
        msg = "KBO site down"
        raise RuntimeError(msg)

    # First call fails and trips circuit to OPEN, then triggers fallback
    res1 = cb.call(failing_fn)
    assert res1 == {"source": "cached_snapshot", "data": [1, 2, 3]}
    assert cb.get_state() == CircuitState.OPEN

    # Subsequent call while OPEN immediately routes to fallback without invoking failing_fn
    res2 = cb.call(failing_fn)
    assert res2 == {"source": "cached_snapshot", "data": [1, 2, 3]}
    assert mock_fallback.call_count == 2


def test_circuit_breaker_half_open_probe_and_recovery() -> None:
    """Test transition from OPEN -> HALF_OPEN -> CLOSED upon successful probe."""
    cb = CircuitBreaker(name="test_recovery", failure_threshold=1, recovery_timeout_seconds=0.1)

    def failing_fn() -> None:
        msg = "Error"
        raise ValueError(msg)

    # Trip to OPEN
    with pytest.raises(ValueError):
        cb.call(failing_fn)
    assert cb.get_state() == CircuitState.OPEN

    # Wait for recovery timeout
    time.sleep(0.15)
    assert cb.get_state() == CircuitState.HALF_OPEN

    # Successful probe closes circuit
    def healed_fn() -> str:
        return "OK"

    res = cb.call(healed_fn)
    assert res == "OK"
    assert cb.get_state() == CircuitState.CLOSED
    assert cb.get_stats().consecutive_failures == 0


def test_circuit_breaker_half_open_probe_failure_re_opens() -> None:
    """Test failed probe in HALF_OPEN immediately re-opens circuit."""
    cb = CircuitBreaker(name="test_reopen", failure_threshold=1, recovery_timeout_seconds=0.1)

    def failing_fn() -> None:
        msg = "Still failing"
        raise RuntimeError(msg)

    # Trip to OPEN
    with pytest.raises(RuntimeError):
        cb.call(failing_fn)
    assert cb.get_state() == CircuitState.OPEN

    # Wait for recovery timeout -> HALF_OPEN
    time.sleep(0.15)
    assert cb.get_state() == CircuitState.HALF_OPEN

    # Probe fails -> immediately back to OPEN
    with pytest.raises(RuntimeError):
        cb.call(failing_fn)
    assert cb.get_state() == CircuitState.OPEN


def test_circuit_breaker_decorator() -> None:
    """Test @circuit_breaker decorator wrapping a function."""

    @circuit_breaker(name="decorated_crawler", failure_threshold=2, recovery_timeout_seconds=1.0)
    def fetch_data(success: bool) -> str:
        if not success:
            msg = "Parse error"
            raise ValueError(msg)
        return "Parsed data"

    assert fetch_data(True) == "Parsed data"

    with pytest.raises(ValueError):
        fetch_data(False)
    with pytest.raises(ValueError):
        fetch_data(False)

    # Circuit is now OPEN
    with pytest.raises(CircuitOpenError):
        fetch_data(True)


def test_circuit_breaker_registry_lifecycle() -> None:
    """Test CircuitBreakerRegistry management and stats aggregation."""
    reg = CircuitBreakerRegistry()
    b1 = reg.get_or_create("crawler_a", failure_threshold=3)
    b2 = reg.get_or_create("crawler_b", failure_threshold=5)

    assert reg.get("crawler_a") is b1
    assert reg.get("crawler_b") is b2
    assert len(reg.get_all_stats()) == 2

    # Trip b1
    b1.record_failure(Exception("Fail 1"))
    b1.record_failure(Exception("Fail 2"))
    b1.record_failure(Exception("Fail 3"))
    assert b1.get_state() == CircuitState.OPEN

    # Reset
    assert reg.reset("crawler_a") is True
    assert b1.get_state() == CircuitState.CLOSED

    # Reset all
    b2.record_failure(Exception("b2 fail"))
    assert reg.reset_all() == 2
    assert b2.get_stats().failure_count == 0


def test_diagnostics_engine_circuit_breaker_monitoring_and_auto_heal() -> None:
    """Test SystemDiagnosticsEngine detecting tripped circuit breakers and auto-healing."""
    circuit_registry.reset_all()
    cb = circuit_registry.get_or_create("kbo_test_crawler", failure_threshold=1)
    cb.reset()

    diag = SystemDiagnosticsEngine()

    # Initial check: Healthy
    checks = diag.diagnose_crawlers()
    cb_check = next(c for c in checks if c.name == "crawler_circuit_breakers")
    assert cb_check.severity == DiagnosticSeverity.HEALTHY
    assert "CLOSED" in cb_check.message

    # Trip circuit
    cb.record_failure(Exception("Test crawl fail"))
    assert cb.get_state() == CircuitState.OPEN

    # Diagnostics should now flag CRITICAL
    checks_tripped = diag.diagnose_crawlers()
    cb_tripped_check = next(c for c in checks_tripped if c.name == "crawler_circuit_breakers")
    assert cb_tripped_check.severity == DiagnosticSeverity.CRITICAL
    assert cb_tripped_check.status == "FAIL"

    # Auto-heal should reset circuit
    healed = diag.auto_heal("crawler")
    assert len(healed) >= 1
    assert "Reset" in healed[0]
    assert cb.get_state() == CircuitState.CLOSED
