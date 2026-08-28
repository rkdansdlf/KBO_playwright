"""Smart Circuit Breaker and fault isolation engine for KBO crawlers."""

from __future__ import annotations

import logging
import threading
import time
from functools import wraps
from typing import TYPE_CHECKING, Any, TypeVar

from src.crawlers.circuit_breaker_dto import CircuitBreakerStats, CircuitOpenError, CircuitState

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)
T = TypeVar("T")


class CircuitBreaker:
    """Thread-safe circuit breaker protecting external crawler HTTP/Playwright requests."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 60.0,
        fallback: Callable[..., Any] | None = None,
    ) -> None:
        """Initialize circuit breaker with name, threshold, and recovery timeout."""
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.fallback = fallback

        self._lock = threading.Lock()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._consecutive_failures = 0
        self._success_count = 0
        self._last_failure_time: float | None = None
        self._last_state_change = time.time()

    def get_state(self) -> CircuitState:
        """Inspect and return current circuit state, transitioning OPEN to HALF_OPEN if timeout elapsed."""
        with self._lock:
            if self._state == CircuitState.OPEN and self._last_failure_time is not None:
                elapsed = time.time() - self._last_failure_time
                if elapsed >= self.recovery_timeout_seconds:
                    self._state = CircuitState.HALF_OPEN
                    self._last_state_change = time.time()
                    logger.info("Circuit breaker '%s' transition: OPEN -> HALF_OPEN (Trial probe allowed)", self.name)
            return self._state

    def record_success(self) -> None:
        """Record a successful execution, resetting consecutive failures and closing HALF_OPEN circuit."""
        with self._lock:
            self._success_count += 1
            self._consecutive_failures = 0
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.CLOSED
                self._last_state_change = time.time()
                logger.info("Circuit breaker '%s' transition: HALF_OPEN -> CLOSED (Service restored)", self.name)

    def record_failure(self, exception: Exception | None = None) -> None:
        """Record a failed execution, incrementing failure counters and tripping circuit if threshold reached."""
        with self._lock:
            now = time.time()
            self._failure_count += 1
            self._consecutive_failures += 1
            self._last_failure_time = now

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._last_state_change = now
                logger.warning(
                    "Circuit breaker '%s' probe failed (%s): HALF_OPEN -> OPEN (Cooldown restarted)",
                    self.name,
                    exception,
                )
            elif self._consecutive_failures >= self.failure_threshold and self._state == CircuitState.CLOSED:
                self._state = CircuitState.OPEN
                self._last_state_change = now
                logger.error(
                    "Circuit breaker '%s' tripped after %d consecutive failures (%s): CLOSED -> OPEN (Blocking calls)",
                    self.name,
                    self._consecutive_failures,
                    exception,
                )

    def call(
        self,
        func: Callable[..., T],
        *args: Any,  # noqa: ANN401
        **kwargs: Any,  # noqa: ANN401
    ) -> T:
        """Execute callable wrapped by circuit breaker guard."""
        current_state = self.get_state()

        if current_state == CircuitState.OPEN:
            remaining = 0.0
            if self._last_failure_time is not None:
                elapsed = time.time() - self._last_failure_time
                remaining = max(0.0, self.recovery_timeout_seconds - elapsed)

            if self.fallback is not None:
                logger.warning("Circuit '%s' is OPEN. Invoking fallback handler.", self.name)
                return self.fallback(*args, **kwargs)  # type: ignore[no-any-return]

            raise CircuitOpenError(self.name, remaining)

        try:
            result = func(*args, **kwargs)
            self.record_success()
        except Exception as exc:
            self.record_failure(exc)
            if self.fallback is not None and self._state == CircuitState.OPEN:
                logger.warning("Circuit '%s' tripped on error. Invoking fallback handler.", self.name)
                return self.fallback(*args, **kwargs)  # type: ignore[no-any-return]
            raise
        else:
            return result

    def reset(self) -> None:
        """Reset all circuit breaker counters and restore CLOSED state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._consecutive_failures = 0
            self._success_count = 0
            self._last_failure_time = None
            self._last_state_change = time.time()
            logger.info("Circuit breaker '%s' manually reset to CLOSED.", self.name)

    def get_stats(self) -> CircuitBreakerStats:
        """Return snapshot statistics for this circuit breaker."""
        current_state = self.get_state()
        with self._lock:
            return CircuitBreakerStats(
                name=self.name,
                state=current_state,
                failure_count=self._failure_count,
                consecutive_failures=self._consecutive_failures,
                success_count=self._success_count,
                failure_threshold=self.failure_threshold,
                recovery_timeout_seconds=self.recovery_timeout_seconds,
                last_failure_time=self._last_failure_time,
                last_state_change=self._last_state_change,
            )


class CircuitBreakerRegistry:
    """Registry maintaining active named circuit breaker singletons."""

    def __init__(self) -> None:
        """Initialize registry."""
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get_or_create(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 60.0,
        fallback: Callable[..., Any] | None = None,
    ) -> CircuitBreaker:
        """Retrieve existing circuit breaker or instantiate a new one."""
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(
                    name=name,
                    failure_threshold=failure_threshold,
                    recovery_timeout_seconds=recovery_timeout_seconds,
                    fallback=fallback,
                )
            return self._breakers[name]

    def get(self, name: str) -> CircuitBreaker | None:
        """Retrieve named circuit breaker if registered."""
        with self._lock:
            return self._breakers.get(name)

    def get_all_stats(self) -> list[CircuitBreakerStats]:
        """Return stats for all registered circuit breakers."""
        with self._lock:
            breakers = list(self._breakers.values())
        return [b.get_stats() for b in breakers]

    def reset_all(self) -> int:
        """Reset all registered circuit breakers to CLOSED."""
        with self._lock:
            breakers = list(self._breakers.values())
        for b in breakers:
            b.reset()
        return len(breakers)

    def reset(self, name: str) -> bool:
        """Reset a specific circuit breaker by name."""
        breaker = self.get(name)
        if breaker:
            breaker.reset()
            return True
        return False


circuit_registry = CircuitBreakerRegistry()


def circuit_breaker(
    name: str | None = None,
    failure_threshold: int = 5,
    recovery_timeout_seconds: float = 60.0,
    fallback: Callable[..., Any] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Wrap and protect a function with a named smart circuit breaker."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        cb_name = name or f"{func.__module__}.{func.__qualname__}"
        cb = circuit_registry.get_or_create(
            name=cb_name,
            failure_threshold=failure_threshold,
            recovery_timeout_seconds=recovery_timeout_seconds,
            fallback=fallback,
        )

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:  # noqa: ANN401
            return cb.call(func, *args, **kwargs)

        return wrapper

    return decorator


__all__ = ["CircuitBreaker", "CircuitBreakerRegistry", "circuit_breaker", "circuit_registry"]
