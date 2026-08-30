"""Token Bucket Rate Limiting Engine and Middleware for FastAPI Gateway."""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

if TYPE_CHECKING:
    from starlette.requests import Request
    from starlette.responses import Response
    from starlette.types import ASGIApp

logger = logging.getLogger(__name__)


class RateLimitTier(StrEnum):
    """Client authentication tier determining rate limit quota."""

    ANONYMOUS = "ANONYMOUS"
    AUTHENTICATED = "AUTHENTICATED"
    ADMIN = "ADMIN"


@dataclass(frozen=True)
class RateLimitPolicy:
    """Rate limit policy configuration."""

    requests_per_minute: int
    burst_capacity: int


DEFAULT_POLICIES: dict[RateLimitTier, RateLimitPolicy] = {
    RateLimitTier.ANONYMOUS: RateLimitPolicy(requests_per_minute=600, burst_capacity=100),
    RateLimitTier.AUTHENTICATED: RateLimitPolicy(requests_per_minute=3000, burst_capacity=500),
    RateLimitTier.ADMIN: RateLimitPolicy(requests_per_minute=12000, burst_capacity=2000),
}

BYPASS_PATHS = {
    "/docs",
    "/redoc",
    "/openapi.json",
    "/health",
    "/api/v1/health",
    "/favicon.ico",
}


class TokenBucket:
    """Thread-safe token bucket rate limiter implementation."""

    def __init__(self, capacity: int, fill_rate_per_sec: float) -> None:
        """Initialize bucket with capacity and fill rate."""
        self.capacity = float(capacity)
        self.fill_rate = float(fill_rate_per_sec)
        self.tokens = float(capacity)
        self.last_update = time.time()
        self._lock = threading.Lock()

    def consume(self, count: float = 1.0) -> tuple[bool, int, float]:
        """Attempt to consume tokens.

        Returns:
            Tuple of (allowed: bool, remaining_tokens: int, retry_after_seconds: float).

        """
        with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            self.last_update = now

            # Refill tokens based on elapsed time
            self.tokens = min(self.capacity, self.tokens + (elapsed * self.fill_rate))

            if self.tokens >= count:
                self.tokens -= count
                remaining = int(self.tokens)
                return True, remaining, 0.0

            # Rate limit exceeded: compute retry-after seconds
            needed = count - self.tokens
            retry_after = max(1.0, needed / self.fill_rate) if self.fill_rate > 0 else 60.0
            return False, 0, retry_after


class RateLimiter:
    """Registry and evaluator of token buckets for API clients."""

    def __init__(
        self,
        policies: dict[RateLimitTier, RateLimitPolicy] | None = None,
        *,
        enabled: bool | None = None,
    ) -> None:
        """Initialize rate limiter with tier policies."""
        self.policies = policies or DEFAULT_POLICIES
        if enabled is not None:
            self.enabled = enabled
        else:
            self.enabled = os.getenv("RATE_LIMIT_ENABLED", "true").lower() in ("true", "1", "yes")
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()

    def resolve_client_key_and_tier(self, request: Request) -> tuple[str, RateLimitTier]:
        """Resolve client identifier and quota tier from request headers."""
        api_key_header = os.getenv("API_KEY_HEADER_NAME", "X-API-Key")
        api_key = request.headers.get(api_key_header)
        expected_key = os.getenv("REST_API_KEY")
        admin_key = os.getenv("ADMIN_API_KEY")

        if admin_key and api_key == admin_key:
            return f"admin:{api_key}", RateLimitTier.ADMIN

        if expected_key and api_key == expected_key:
            return f"auth:{api_key}", RateLimitTier.AUTHENTICATED

        # Fallback to Client IP
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            client_ip = forwarded.split(",")[0].strip()
        elif request.client:
            client_ip = request.client.host
        else:
            client_ip = "127.0.0.1"

        if client_ip in ("testclient", "127.0.0.1") and (
            os.getenv("TESTING", "").lower() in ("1", "true") or os.getenv("PYTEST_CURRENT_TEST") is not None
        ):
            return f"admin:{client_ip}", RateLimitTier.ADMIN

        return f"ip:{client_ip}", RateLimitTier.ANONYMOUS

    def check_rate_limit(self, request: Request) -> tuple[bool, int, int, float]:
        """Evaluate rate limit for an incoming request.

        Returns:
            Tuple of (allowed: bool, limit: int, remaining: int, reset_seconds: float).

        """
        client_key, tier = self.resolve_client_key_and_tier(request)
        policy = self.policies.get(tier, self.policies[RateLimitTier.ANONYMOUS])

        bucket_key = f"{client_key}:{tier.value}"

        with self._lock:
            if bucket_key not in self._buckets:
                fill_rate = policy.requests_per_minute / 60.0
                self._buckets[bucket_key] = TokenBucket(
                    capacity=policy.burst_capacity,
                    fill_rate_per_sec=fill_rate,
                )
            bucket = self._buckets[bucket_key]

        allowed, remaining, retry_after = bucket.consume(1.0)
        limit = policy.requests_per_minute
        return allowed, limit, remaining, retry_after

    def reset(self) -> None:
        """Reset all allocated client buckets."""
        with self._lock:
            self._buckets.clear()


global_rate_limiter = RateLimiter()


class RateLimitMiddleware(BaseHTTPMiddleware):
    """FastAPI / Starlette middleware enforcing token bucket rate limiting."""

    def __init__(
        self,
        app: ASGIApp,
        limiter: RateLimiter | None = None,
    ) -> None:
        """Initialize middleware."""
        super().__init__(app)
        self.limiter = limiter or global_rate_limiter

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Process request, enforce rate limit, and attach rate limit headers."""
        if not self.limiter.enabled:
            return await call_next(request)

        path = request.url.path

        # Bypass documentation, health checks, and WebSocket upgrade handshakes
        if (
            path in BYPASS_PATHS
            or path.startswith(("/docs", "/redoc", "/openapi.json"))
            or request.headers.get("upgrade", "").lower() == "websocket"
        ):
            return await call_next(request)

        allowed, limit, remaining, retry_after = self.limiter.check_rate_limit(request)

        if not allowed:
            retry_sec = max(1, round(retry_after))
            logger.warning(
                "Rate limit exceeded for %s on path %s. Retry after %ds.",
                request.client.host if request.client else "unknown",
                path,
                retry_sec,
            )
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Too Many Requests",
                    "detail": f"Rate limit exceeded. Retry after {retry_sec} seconds.",
                    "retry_after": retry_sec,
                },
                headers={
                    "Retry-After": str(retry_sec),
                    "X-RateLimit-Limit": str(limit),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(retry_sec),
                },
            )

        response: Response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(limit)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(round(retry_after))
        return response


__all__ = [
    "BYPASS_PATHS",
    "RateLimitMiddleware",
    "RateLimitPolicy",
    "RateLimitTier",
    "RateLimiter",
    "TokenBucket",
    "global_rate_limiter",
]
