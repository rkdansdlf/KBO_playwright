"""Adaptive rate limiting, dynamic backoff, and circuit breaker resilience mechanisms."""

from __future__ import annotations

import asyncio
import logging
import random
from enum import StrEnum

logger = logging.getLogger(__name__)


class CircuitBreakerState(StrEnum):
    """Lifecycle states of the Circuit Breaker."""

    CLOSED = "CLOSED"  # Normal operation
    OPEN = "OPEN"  # Tripped, fast-failing all requests
    HALF_OPEN = "HALF_OPEN"  # Testing recovery with canary requests


class CircuitBreaker:
    """Protects external crawler targets by fast-failing during consecutive outages."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 30.0,
        name: str = "default",
    ) -> None:
        """Initialize the CircuitBreaker."""
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.name = name

        self.state = CircuitBreakerState.CLOSED
        self.consecutive_failures = 0
        self.last_state_change = 0.0

    def allow_request(self) -> bool:
        """Determine if a request should be permitted."""
        if self.state == CircuitBreakerState.CLOSED:
            return True

        if self.state == CircuitBreakerState.OPEN:
            elapsed = asyncio.get_event_loop().time() - self.last_state_change
            if elapsed >= self.recovery_timeout_seconds:
                logger.info("CircuitBreaker '%s' transitioning OPEN -> HALF_OPEN (timeout elapsed)", self.name)
                self.state = CircuitBreakerState.HALF_OPEN
                self.last_state_change = asyncio.get_event_loop().time()
                return True
            return False

        # HALF_OPEN allows test probe request
        return True

    def record_success(self) -> None:
        """Record a successful request execution."""
        if self.state in {CircuitBreakerState.HALF_OPEN, CircuitBreakerState.OPEN}:
            logger.info("CircuitBreaker '%s' recovered -> CLOSED", self.name)
            self.state = CircuitBreakerState.CLOSED
        self.consecutive_failures = 0
        self.last_state_change = asyncio.get_event_loop().time()

    def record_failure(self) -> None:
        """Record a failed request execution."""
        self.consecutive_failures += 1
        self.last_state_change = asyncio.get_event_loop().time()

        if self.state == CircuitBreakerState.HALF_OPEN:
            logger.warning("CircuitBreaker '%s' probe failed -> OPEN", self.name)
            self.state = CircuitBreakerState.OPEN
        elif self.consecutive_failures >= self.failure_threshold:
            logger.warning(
                "CircuitBreaker '%s' failure threshold reached (%d) -> OPEN",
                self.name,
                self.consecutive_failures,
            )
            self.state = CircuitBreakerState.OPEN


class AdaptiveRateLimiter:
    """Dynamically adjusts request throttling based on server responses and rate limits."""

    def __init__(
        self,
        base_delay_seconds: float = 1.0,
        min_delay_seconds: float = 0.0,
        max_delay_seconds: float = 15.0,
        backoff_factor: float = 1.5,
        jitter_factor: float = 0.1,
    ) -> None:
        """Initialize the AdaptiveRateLimiter."""
        self.base_delay = base_delay_seconds
        self.current_delay = base_delay_seconds
        self.min_delay = min(min_delay_seconds, base_delay_seconds)
        self.max_delay = max(max_delay_seconds, base_delay_seconds)
        self.backoff_factor = backoff_factor
        self.jitter_factor = jitter_factor

    def calculate_delay(self) -> float:
        """Calculate the next delay with randomized jitter."""
        if self.current_delay <= 0:
            return 0.0
        jitter_range = self.current_delay * self.jitter_factor
        jitter = random.uniform(-jitter_range, jitter_range)  # noqa: S311
        return max(self.min_delay, min(self.max_delay, self.current_delay + jitter))

    async def acquire(self) -> float:
        """Wait for the calculated throttle delay before permitting the request."""
        delay = self.calculate_delay()
        if delay > 0:
            await asyncio.sleep(delay)
        return delay

    def record_success(self) -> None:
        """Gradually decrease delay towards base_delay on successful responses."""
        if self.current_delay > self.base_delay:
            self.current_delay = max(self.base_delay, self.current_delay * 0.9)

    def record_rate_limit(self, retry_after: float | None = None) -> None:
        """Increase delay aggressively when 429 / 503 / throttling is detected."""
        if retry_after is not None and retry_after > 0:
            self.current_delay = min(self.max_delay, retry_after)
        else:
            self.current_delay = min(self.max_delay, self.current_delay * self.backoff_factor)
        logger.warning("AdaptiveRateLimiter backoff triggered: new delay = %.2fs", self.current_delay)
