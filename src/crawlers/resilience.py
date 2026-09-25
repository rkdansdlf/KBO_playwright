"""Adaptive rate limiting and dynamic backoff for crawler targets."""

from __future__ import annotations

import asyncio
import logging
import random

logger = logging.getLogger(__name__)


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
        """Increase delay when 429 / 503 / throttling is detected.

        A server-provided `Retry-After` is honoured verbatim. Clamping it to
        `max_delay` would only guarantee another 429, because we would come
        back sooner than the server asked. The caller is responsible for
        bounding the value; `max_delay` applies to the self-computed backoff.
        """
        if retry_after is not None and retry_after > 0:
            self.current_delay = retry_after
        else:
            self.current_delay = min(self.max_delay, self.current_delay * self.backoff_factor)
        logger.warning("AdaptiveRateLimiter backoff triggered: new delay = %.2fs", self.current_delay)
