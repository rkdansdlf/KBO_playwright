"""Tests for Token Bucket Rate Limiting Engine and FastAPI Middleware."""

from __future__ import annotations

import concurrent.futures
import time
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from src.api.app import app
from src.api.rate_limiter import (
    RateLimitMiddleware,
    RateLimitPolicy,
    RateLimitTier,
    RateLimiter,
    TokenBucket,
    global_rate_limiter,
)


def test_token_bucket_consume_and_refill() -> None:
    """Test token bucket consumption, depletion, and time-based refilling."""
    bucket = TokenBucket(capacity=3, fill_rate_per_sec=10.0)

    # 1st consume: allowed
    ok1, rem1, _ = bucket.consume(1.0)
    assert ok1 is True
    assert rem1 == 2

    # 2nd & 3rd consume: allowed
    ok2, rem2, _ = bucket.consume(1.0)
    ok3, rem3, _ = bucket.consume(1.0)
    assert ok2 is True and ok3 is True
    assert rem3 == 0

    # 4th consume: depleted -> rejected with retry_after
    ok4, rem4, retry4 = bucket.consume(1.0)
    assert ok4 is False
    assert rem4 == 0
    assert retry4 > 0.0

    # Sleep 0.2s -> refills 2.0 tokens
    time.sleep(0.25)
    ok5, rem5, _ = bucket.consume(1.0)
    assert ok5 is True


def test_token_bucket_thread_safety() -> None:
    """Test concurrent multi-threaded consumption on token bucket."""
    bucket = TokenBucket(capacity=100, fill_rate_per_sec=0.0)
    success_count = 0

    def worker() -> bool:
        ok, _, _ = bucket.consume(1.0)
        return ok

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(worker) for _ in range(120)]
        results = [f.result() for f in futures]
        success_count = sum(1 for r in results if r is True)

    assert success_count == 100


def test_rate_limiter_tier_resolution(monkeypatch) -> None:
    """Test resolving client IP and API key tiers."""
    monkeypatch.setenv("REST_API_KEY", "secret_key_123")
    monkeypatch.setenv("ADMIN_API_KEY", "admin_key_456")

    limiter = RateLimiter()

    # Anonymous IP
    req_anon = MagicMock()
    req_anon.headers = {}
    req_anon.client.host = "192.168.1.50"
    k_anon, t_anon = limiter.resolve_client_key_and_tier(req_anon)
    assert k_anon == "ip:192.168.1.50"
    assert t_anon == RateLimitTier.ANONYMOUS

    # Authenticated
    req_auth = MagicMock()
    req_auth.headers = {"X-API-Key": "secret_key_123"}
    k_auth, t_auth = limiter.resolve_client_key_and_tier(req_auth)
    assert t_auth == RateLimitTier.AUTHENTICATED

    # Admin
    req_admin = MagicMock()
    req_admin.headers = {"X-API-Key": "admin_key_456"}
    k_admin, t_admin = limiter.resolve_client_key_and_tier(req_admin)
    assert t_admin == RateLimitTier.ADMIN


def test_fastapi_rate_limiting_integration() -> None:
    """Test FastAPI middleware attaching rate limit headers and returning 429 on limit breach."""
    custom_limiter = RateLimiter(
        policies={
            RateLimitTier.ANONYMOUS: RateLimitPolicy(requests_per_minute=60, burst_capacity=2),
            RateLimitTier.AUTHENTICATED: RateLimitPolicy(requests_per_minute=300, burst_capacity=10),
            RateLimitTier.ADMIN: RateLimitPolicy(requests_per_minute=1200, burst_capacity=100),
        }
    )

    # Use a custom app with restricted limiter
    from fastapi import FastAPI

    test_app = FastAPI()
    test_app.add_middleware(RateLimitMiddleware, limiter=custom_limiter)

    @test_app.get("/api/v1/test_ping")
    def ping() -> dict[str, str]:
        return {"status": "pong"}

    client = TestClient(test_app)

    headers = {"X-Forwarded-For": "203.0.113.195"}

    # 1st request -> 200 OK + Headers
    resp1 = client.get("/api/v1/test_ping", headers=headers)
    assert resp1.status_code == 200
    assert "X-RateLimit-Limit" in resp1.headers
    assert resp1.headers["X-RateLimit-Limit"] == "60"
    assert resp1.headers["X-RateLimit-Remaining"] == "1"

    # 2nd request -> 200 OK
    resp2 = client.get("/api/v1/test_ping", headers=headers)
    assert resp2.status_code == 200
    assert resp2.headers["X-RateLimit-Remaining"] == "0"

    # 3rd request -> 429 Too Many Requests
    resp3 = client.get("/api/v1/test_ping", headers=headers)
    assert resp3.status_code == 429
    assert resp3.headers["X-RateLimit-Remaining"] == "0"
    assert "Retry-After" in resp3.headers
    data = resp3.json()
    assert data["error"] == "Too Many Requests"
    assert "Rate limit exceeded" in data["detail"]


def test_fastapi_bypass_paths() -> None:
    """Test that whitelisted paths like /health are unthrottled."""
    global_rate_limiter.reset()
    client = TestClient(app)

    # Calling health endpoint multiple times should never be blocked by rate limit
    for _ in range(5):
        resp = client.get("/health")
        assert resp.status_code in {200, 503}
