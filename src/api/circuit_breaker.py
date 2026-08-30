"""Asynchronous native Circuit Breaker for real-time live streaming and WebSocket fault isolation."""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from functools import wraps
from typing import TYPE_CHECKING, Any, TypeVar

from src.api.live_stream_dto import CircuitState, CircuitStatusDTO

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

logger = logging.getLogger(__name__)
T = TypeVar("T")


class CircuitOpenError(Exception):
    """Exception raised when a call is rejected because the circuit is OPEN."""

    def __init__(self, name: str, retry_after_seconds: float, message: str | None = None) -> None:
        """Initialize CircuitOpenError with retry duration."""
        self.name = name
        self.retry_after_seconds = max(0.0, retry_after_seconds)
        msg = message or f"Circuit breaker '{name}' is OPEN. Retry trial after {self.retry_after_seconds:.1f}s"
        super().__init__(msg)


class AsyncCircuitBreaker:
    """Async-native and thread-safe Circuit Breaker for WebSocket live streams and API endpoints."""

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 30.0,
        half_open_success_threshold: int = 1,
        fallback: Callable[..., Any] | None = None,
        on_state_change: Callable[[str, CircuitState, CircuitState], Coroutine[Any, Any, None] | None] | None = None,
    ) -> None:
        """Initialize the async circuit breaker."""
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.half_open_success_threshold = half_open_success_threshold
        self.fallback = fallback
        self.on_state_change = on_state_change

        self._lock = asyncio.Lock()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._consecutive_failures = 0
        self._success_count = 0
        self._half_open_successes = 0
        self._rejection_count = 0
        self._last_failure_time: float | None = None
        self._last_state_change = time.time()
        self._last_failure_reason: str | None = None

    @property
    def state(self) -> CircuitState:
        """Read-only property for immediate synchronous state inspection."""
        now = time.time()
        if (
            self._state == CircuitState.OPEN
            and self._last_failure_time is not None
            and now - self._last_failure_time >= self.recovery_timeout_seconds
        ):
            return CircuitState.HALF_OPEN
        return self._state

    async def get_state(self) -> CircuitState:
        """Inspect and return the current state under lock, transitioning OPEN to HALF_OPEN if cooldown elapsed."""
        async with self._lock:
            now = time.time()
            if self._state == CircuitState.OPEN and self._last_failure_time is not None:
                elapsed = now - self._last_failure_time
                if elapsed >= self.recovery_timeout_seconds:
                    old_state = self._state
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_successes = 0
                    self._last_state_change = now
                    logger.info(
                        "Circuit breaker '%s' transition: OPEN -> HALF_OPEN (Trial probe permitted)",
                        self.name,
                    )
                    await self._notify_state_change(old_state, CircuitState.HALF_OPEN)
            return self._state

    async def record_success(self) -> None:
        """Record a successful execution, restoring closed state if trial probe succeeds."""
        async with self._lock:
            self._success_count += 1
            self._consecutive_failures = 0

            if self._state == CircuitState.HALF_OPEN:
                self._half_open_successes += 1
                if self._half_open_successes >= self.half_open_success_threshold:
                    old_state = self._state
                    self._state = CircuitState.CLOSED
                    self._half_open_successes = 0
                    self._last_state_change = time.time()
                    logger.info(
                        "Circuit breaker '%s' transition: HALF_OPEN -> CLOSED (Service fully restored)",
                        self.name,
                    )
                    await self._notify_state_change(old_state, CircuitState.CLOSED)

    async def record_failure(self, exception: Exception | None = None) -> None:
        """Record a failed execution, tripping circuit if consecutive failures exceed threshold."""
        async with self._lock:
            now = time.time()
            self._failure_count += 1
            self._consecutive_failures += 1
            self._last_failure_time = now
            self._last_failure_reason = str(exception) if exception else "Unknown failure"

            old_state = self._state
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._last_state_change = now
                logger.warning(
                    "Circuit breaker '%s' trial probe failed (%s): HALF_OPEN -> OPEN (Cooldown restarted)",
                    self.name,
                    self._last_failure_reason,
                )
                await self._notify_state_change(old_state, CircuitState.OPEN)
            elif self._consecutive_failures >= self.failure_threshold and self._state == CircuitState.CLOSED:
                self._state = CircuitState.OPEN
                self._last_state_change = now
                logger.error(
                    "Circuit breaker '%s' tripped after %d consecutive failures (%s): CLOSED -> OPEN",
                    self.name,
                    self._consecutive_failures,
                    self._last_failure_reason,
                )
                await self._notify_state_change(old_state, CircuitState.OPEN)

    async def trip(self, reason: str = "Manual admin trip") -> CircuitState:
        """Manually trip the circuit breaker into OPEN state."""
        async with self._lock:
            old_state = self._state
            self._state = CircuitState.OPEN
            self._last_failure_time = time.time()
            self._last_state_change = time.time()
            self._last_failure_reason = reason
            logger.warning("Circuit breaker '%s' manually tripped: %s", self.name, reason)
            await self._notify_state_change(old_state, CircuitState.OPEN)
            return self._state

    async def reset(self) -> CircuitState:
        """Manually reset the circuit breaker into CLOSED operational state."""
        async with self._lock:
            old_state = self._state
            self._state = CircuitState.CLOSED
            self._consecutive_failures = 0
            self._half_open_successes = 0
            self._last_state_change = time.time()
            self._last_failure_reason = None
            logger.info("Circuit breaker '%s' manually reset to CLOSED", self.name)
            await self._notify_state_change(old_state, CircuitState.CLOSED)
            return self._state

    def get_status(self) -> CircuitStatusDTO:
        """Export comprehensive telemetry status snapshot."""
        now = time.time()
        current_state = self.state
        return CircuitStatusDTO(
            name=self.name,
            state=current_state,
            failure_count=self._failure_count,
            consecutive_failures=self._consecutive_failures,
            success_count=self._success_count,
            rejection_count=self._rejection_count,
            failure_threshold=self.failure_threshold,
            recovery_timeout_seconds=self.recovery_timeout_seconds,
            time_in_current_state_seconds=round(now - self._last_state_change, 2),
            last_failure_reason=self._last_failure_reason,
        )

    async def call_async(
        self,
        coro_func: Callable[..., Coroutine[Any, Any, T]],
        *args: Any,  # noqa: ANN401
        **kwargs: Any,  # noqa: ANN401
    ) -> T:
        """Execute an async coroutine wrapped by circuit breaker protection."""
        current_state = await self.get_state()

        if current_state == CircuitState.OPEN:
            async with self._lock:
                self._rejection_count += 1
            retry_after = (
                self.recovery_timeout_seconds - (time.time() - (self._last_failure_time or time.time()))
                if self._last_failure_time
                else self.recovery_timeout_seconds
            )
            if self.fallback is not None:
                if inspect.iscoroutinefunction(self.fallback):
                    return await self.fallback(*args, **kwargs)
                return self.fallback(*args, **kwargs)
            msg = f"Circuit breaker '{self.name}' is OPEN. Retry trial after {retry_after:.1f}s"
            raise CircuitOpenError(self.name, retry_after, message=msg)

        try:
            result = await coro_func(*args, **kwargs)
        except Exception as e:
            await self.record_failure(e)
            if self.fallback is not None:
                if inspect.iscoroutinefunction(self.fallback):
                    return await self.fallback(*args, **kwargs)
                return self.fallback(*args, **kwargs)
            raise
        else:
            await self.record_success()
            return result

    def protect(
        self,
        func: Callable[..., Coroutine[Any, Any, T]],
    ) -> Callable[..., Coroutine[Any, Any, T]]:
        """Decorate an async function for automated circuit breaker protection."""

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:  # noqa: ANN401
            return await self.call_async(func, *args, **kwargs)

        return wrapper

    async def _notify_state_change(self, old_state: CircuitState, new_state: CircuitState) -> None:
        """Dispatch state change notification callback if configured."""
        if self.on_state_change is not None and old_state != new_state:
            try:
                res = self.on_state_change(self.name, old_state, new_state)
                if inspect.isawaitable(res):
                    await res
            except Exception as e:  # noqa: BLE001
                logger.warning("Error in circuit breaker on_state_change listener: %s", e)
