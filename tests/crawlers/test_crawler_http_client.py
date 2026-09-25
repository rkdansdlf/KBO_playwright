"""Contract tests for the shared crawler HTTP client.

These drive the real client through `httpx.MockTransport`, so the outcome
classification, the `Retry-After` handling, the adaptive backoff, and the
circuit breaker are all exercised through the same path a crawler uses.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest

from src.crawlers.circuit_breaker import circuit_registry
from src.crawlers.circuit_breaker_dto import CircuitState
from src.crawlers.http_client import (
    CircuitPolicy,
    CrawlerHttpClient,
    HttpPolicy,
)
from src.crawlers.result import CrawlOutcome
from src.crawlers.retry_after import MAX_RETRY_AFTER_SECONDS, parse_retry_after
from src.utils.throttle import throttle

Handler = Callable[[httpx.Request], httpx.Response]


@pytest.fixture(autouse=True)
def _no_throttle(monkeypatch):
    """Keep throttling out of the way so tests stay fast.

    The shared `throttle` singleton reads its configuration at import time,
    so the env vars have to be overridden on the instance, not just the
    environment.
    """
    monkeypatch.setenv("KBO_REQUEST_DELAY", "0")
    monkeypatch.setenv("KBO_REQUEST_JITTER", "0")
    monkeypatch.setattr(throttle, "default_delay", 0.0)
    monkeypatch.setattr(throttle, "jitter", 0.0)
    monkeypatch.setattr(throttle, "_last_request_times", {})


def _client(
    handler: Handler,
    *,
    name: str,
    max_attempts: int = 1,
    max_backoff_seconds: float = 0.0,
    **policy_kwargs,
) -> CrawlerHttpClient:
    """Build a client whose transport is a mock, with a clean circuit."""
    transport = httpx.MockTransport(handler)
    client = CrawlerHttpClient(
        name=name,
        policy=HttpPolicy(
            base_delay_seconds=0.0,
            max_attempts=max_attempts,
            max_backoff_seconds=max_backoff_seconds,
            **policy_kwargs,
        ),
    )

    @asynccontextmanager
    async def _mock_client():
        async with httpx.AsyncClient(
            headers=client.default_headers,
            timeout=client.timeout,
            transport=transport,
            follow_redirects=True,
        ) as raw:
            yield raw

    # Injecting the transport keeps the test offline; the URL validation hook
    # is covered separately in TestUrlValidation.
    client._client = _mock_client  # type: ignore[method-assign]
    circuit_registry.reset_all()
    return client


def test_successful_json_is_reported_as_success():
    client = _client(lambda request: httpx.Response(200, json={"rows": [1, 2]}), name="ok-case")
    result = asyncio.run(client.fetch_json("https://stat.koreabaseball.com/api/x"))
    assert result.outcome is CrawlOutcome.SUCCESS
    assert result.data == {"rows": [1, 2]}
    assert result.ok


def test_empty_payload_is_distinct_from_failure():
    client = _client(lambda request: httpx.Response(200, json=[]), name="empty-case")
    result = asyncio.run(client.fetch_json("https://stat.koreabaseball.com/api/x"))
    assert result.outcome is CrawlOutcome.EMPTY
    assert not result.ok
    assert not result.should_retry


def test_html_response_instead_of_json_is_schema_changed():
    client = _client(
        lambda request: httpx.Response(
            200, text="<html><body>no data</body></html>", headers={"Content-Type": "text/html"}
        ),
        name="drift-case",
    )
    result = asyncio.run(client.fetch_json("https://stat.koreabaseball.com/api/x"))
    assert result.outcome is CrawlOutcome.SCHEMA_CHANGED
    assert not result.should_retry


def test_undecodable_json_body_is_schema_changed():
    client = _client(
        lambda request: httpx.Response(200, content=b"{not json", headers={"Content-Type": "application/json"}),
        name="badjson-case",
    )
    result = asyncio.run(client.fetch_json("https://stat.koreabaseball.com/api/x"))
    assert result.outcome is CrawlOutcome.SCHEMA_CHANGED


def test_permanent_status_is_not_retried():
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(404, text="missing")

    client = _client(handler, name="notfound-case", max_attempts=3)
    result = asyncio.run(client.fetch_json("https://stat.koreabaseball.com/api/x"))
    assert result.outcome is CrawlOutcome.PERMANENT_ERROR
    assert len(calls) == 1


def test_retryable_status_is_retried_up_to_the_limit():
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(503, text="busy")

    client = _client(handler, name="retry-case", max_attempts=3, max_backoff_seconds=0.0)
    result = asyncio.run(client.fetch_json("https://stat.koreabaseball.com/api/x"))
    assert result.outcome is CrawlOutcome.RETRYABLE_ERROR
    assert result.attempts == 3
    assert len(calls) == 3


def test_retry_after_header_drives_the_adaptive_delay():
    client = _client(
        lambda request: httpx.Response(429, headers={"Retry-After": "0.25"}),
        name="ratelimit-case",
        max_attempts=1,
    )
    result = asyncio.run(client.fetch_json("https://stat.koreabaseball.com/api/x"))
    assert result.outcome is CrawlOutcome.RETRYABLE_ERROR
    assert result.retry_after == 0.25
    # A server that throttles us must push the adaptive delay up.
    assert client.rate_limiter.current_delay == 0.25


def test_successful_response_relaxes_the_adaptive_delay():
    state = {"first": True}

    def handler(request: httpx.Request) -> httpx.Response:
        if state["first"]:
            state["first"] = False
            return httpx.Response(429, headers={"Retry-After": "0.2"})
        return httpx.Response(200, json={"ok": True})

    client = _client(handler, name="relax-case", max_attempts=1)
    asyncio.run(client.fetch_json("https://stat.koreabaseball.com/api/x"))
    assert client.rate_limiter.current_delay == 0.2
    asyncio.run(client.fetch_json("https://stat.koreabaseball.com/api/x"))
    assert client.rate_limiter.current_delay < 0.2


def test_transport_error_is_retryable():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    client = _client(handler, name="connerr-case", max_attempts=1)
    result = asyncio.run(client.fetch_json("https://stat.koreabaseball.com/api/x"))
    assert result.outcome is CrawlOutcome.RETRYABLE_ERROR


def test_fetch_text_returns_the_body():
    client = _client(lambda request: httpx.Response(200, text="<html>ok</html>"), name="text-case")
    result = asyncio.run(client.fetch_text("https://www.giantsclub.com/food"))
    assert result.outcome is CrawlOutcome.SUCCESS
    assert result.data == "<html>ok</html>"


def test_fetch_text_treats_a_blank_body_as_empty():
    client = _client(lambda request: httpx.Response(200, text="   "), name="blank-case")
    result = asyncio.run(client.fetch_text("https://www.giantsclub.com/food"))
    assert result.outcome is CrawlOutcome.EMPTY


def test_open_circuit_fails_fast_without_a_request():
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(503)

    client = _client(
        handler,
        name="breaker-case",
        max_attempts=1,
        circuit=CircuitPolicy(failure_threshold=1, recovery_timeout_seconds=600.0),
    )
    first = asyncio.run(client.fetch_json("https://stat.koreabaseball.com/api/x"))
    assert first.outcome is CrawlOutcome.RETRYABLE_ERROR
    assert client.breaker.get_state() is CircuitState.OPEN

    second = asyncio.run(client.fetch_json("https://stat.koreabaseball.com/api/x"))
    assert second.outcome is CrawlOutcome.RETRYABLE_ERROR
    # The second call must not reach the network.
    assert len(calls) == 1


class TestParseRetryAfter:
    def test_delay_seconds_form(self):
        assert parse_retry_after("120") == 120.0

    def test_float_delay_form(self):
        assert parse_retry_after("1.5") == 1.5

    def test_http_date_form(self):
        future = format_datetime(datetime.now(UTC) + timedelta(seconds=90), usegmt=True)
        parsed = parse_retry_after(future)
        assert parsed is not None
        assert 0 < parsed <= MAX_RETRY_AFTER_SECONDS

    def test_http_date_in_the_past_is_ignored(self):
        assert parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") is None

    def test_value_is_bounded(self):
        assert parse_retry_after("999999") == MAX_RETRY_AFTER_SECONDS

    @pytest.mark.parametrize("raw", [None, "", "   ", "soon", "-5", "0"])
    def test_unusable_values_return_none(self, raw):
        assert parse_retry_after(raw) is None
