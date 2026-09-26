"""The single outbound HTTP path for crawlers.

Every crawler request funnels through this client so that URL validation,
throttling, retry, `Retry-After` handling, circuit breaking, and metrics
cannot be bypassed by a crawler that builds its own `httpx.AsyncClient`.
Each request returns a classified `CrawlResult` instead of `None`.
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import httpx

from src.crawlers.circuit_breaker import circuit_registry
from src.crawlers.circuit_breaker_dto import CircuitState
from src.crawlers.failure_taxonomy import FailureCode, failure_code_for_status
from src.crawlers.resilience import AdaptiveRateLimiter
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.crawlers.retry_after import parse_retry_after
from src.utils.throttle import throttle
from src.utils.url_validator import validate_url

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)

THROTTLE_STATUS_CODES = frozenset(
    {
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.SERVICE_UNAVAILABLE,
    }
)
"""Statuses that mean "slow down" rather than "you sent something wrong"."""

RETRYABLE_STATUS_CODES = frozenset(
    {
        HTTPStatus.REQUEST_TIMEOUT,
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
    }
)
"""Statuses a later attempt could plausibly resolve."""

MAX_INLINE_JSON_OFFSET = 512
"""How deep into a response body an embedded JSON payload may start."""

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "application/json, text/plain, */*",
}


@dataclass(frozen=True)
class CircuitPolicy:
    """Circuit breaker tuning for a crawler target."""

    failure_threshold: int = 5
    recovery_timeout_seconds: float = 60.0


@dataclass(frozen=True)
class HttpPolicy:
    """Timing, retry, and circuit tuning for a crawler target."""

    base_delay_seconds: float = 0.5
    """Baseline throttle delay."""

    timeout_seconds: float = 15.0
    """Per-request timeout."""

    max_attempts: int = 3
    """Total attempts for a retryable failure, including the first."""

    max_backoff_seconds: float = 30.0
    """Ceiling for the exponential backoff between attempts."""

    circuit: CircuitPolicy = CircuitPolicy()
    """Circuit breaker tuning."""


DEFAULT_HTTP_POLICY = HttpPolicy()


class CrawlerHttpClient:
    """A resilient HTTP client for crawler targets.

    The client owns the whole request lifecycle: it validates the URL (and
    every redirect target), waits for the adaptive rate limiter, retries
    retryable failures with exponential backoff, honours `Retry-After`, and
    reports the outcome as a `CrawlResult`.
    """

    def __init__(
        self,
        *,
        name: str = "default",
        policy: HttpPolicy = DEFAULT_HTTP_POLICY,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Initialize the client.

        Args:
            name: Circuit breaker identity, usually the crawler class name.
            policy: Timing, retry, and circuit tuning.
            headers: Default headers merged into every request.

        """
        self.name = name
        self.policy = policy
        self.timeout = policy.timeout_seconds
        self.max_attempts = max(1, policy.max_attempts)
        self.default_headers = {**DEFAULT_HEADERS, **(headers or {})}
        self.rate_limiter = AdaptiveRateLimiter(base_delay_seconds=policy.base_delay_seconds)
        self.breaker = circuit_registry.get_or_create(
            name=name,
            failure_threshold=policy.circuit.failure_threshold,
            recovery_timeout_seconds=policy.circuit.recovery_timeout_seconds,
        )

    @asynccontextmanager
    async def _client(self) -> AsyncIterator[httpx.AsyncClient]:
        async with httpx.AsyncClient(
            headers=self.default_headers,
            timeout=self.timeout,
            follow_redirects=True,
            event_hooks={"request": [self._validate_request_url]},
        ) as client:
            yield client

    async def _validate_request_url(self, request: httpx.Request) -> None:
        """Reject blocked targets, including redirect destinations."""
        ok, reason = await asyncio.to_thread(validate_url, str(request.url))
        if not ok:
            message = f"Blocked crawler request to {request.url}: {reason}"
            raise ValueError(message)

    async def _throttle(self, host: str) -> float:
        """Wait for both the shared per-host delay and the adaptive penalty.

        The per-host `AsyncThrottle` is what ten crawlers already rely on and
        what `KBO_REQUEST_DELAY` configures, so it stays authoritative. The
        adaptive limiter is layered on top so a server that throttles us
        pushes the delay up for subsequent requests.

        Args:
            host: Target host, used as the throttle key.

        Returns:
            Total seconds spent waiting on the adaptive component.

        """
        await throttle.wait(host)
        waited = await self.rate_limiter.acquire()
        if waited:
            logger.debug("[%s] adaptive backoff %.2fs for %s", self.name, waited, host)
        return waited

    @staticmethod
    def _host_of(url: str) -> str:
        return urlparse(url).hostname or "koreabaseball.com"

    async def fetch_text(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> CrawlResult[str]:
        """Fetch a page body, classifying the outcome.

        Args:
            url: Target URL.
            params: Optional query parameters.
            headers: Extra headers for this request only.

        Returns:
            A `CrawlResult` whose `data` is the response body on success.

        """
        return await self._fetch(url, params=params, headers=headers, decode_json=False)

    async def fetch_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> CrawlResult[Any]:
        """Fetch and parse a JSON payload, classifying the outcome.

        Args:
            url: Target URL.
            params: Optional query parameters.
            headers: Extra headers for this request only.

        Returns:
            A `CrawlResult` whose outcome distinguishes success, an empty
            result, a transient fault, a schema drift, and a permanent fault.

        """
        return await self._fetch(url, params=params, headers=headers, decode_json=True)

    async def _fetch(
        self,
        url: str,
        *,
        params: dict[str, Any] | None,
        headers: dict[str, str] | None,
        decode_json: bool,
    ) -> CrawlResult[Any]:
        """Run the retry loop and return the final classified result."""
        started = time.monotonic()
        last_result: CrawlResult[Any] | None = None

        for attempt in range(1, self.max_attempts + 1):
            last_result = await self._attempt(
                url,
                params=params,
                headers=headers,
                decode_json=decode_json,
            )
            if not last_result.should_retry or attempt >= self.max_attempts:
                break
            delay = last_result.retry_after if last_result.retry_after is not None else self._backoff(attempt)
            logger.warning(
                "[%s] %s on %s (attempt %d/%d), retrying in %.1fs",
                self.name,
                last_result.outcome,
                url,
                attempt,
                self.max_attempts,
                delay,
            )
            await asyncio.sleep(delay)

        assert last_result is not None  # noqa: S101 - max_attempts >= 1 guarantees a result
        elapsed = time.monotonic() - started
        return CrawlResult(
            outcome=last_result.outcome,
            data=last_result.data,
            http_status=last_result.http_status,
            attempts=attempt,
            elapsed_seconds=elapsed,
            retry_after=last_result.retry_after,
            error=last_result.error,
            # Carried forward explicitly: rebuilding the result field by field
            # would otherwise drop the classification, and a run that succeeded
            # only after a retry must not report the first attempt's error_code.
            error_code=last_result.error_code,
            url=url,
        )

    def _backoff(self, attempt: int) -> float:
        """Return the exponential backoff delay before the next attempt."""
        return min(2.0**attempt, self.policy.max_backoff_seconds)

    async def _attempt(
        self,
        url: str,
        *,
        params: dict[str, Any] | None,
        headers: dict[str, str] | None,
        decode_json: bool,
    ) -> CrawlResult[Any]:
        """Perform a single request and classify its outcome."""
        blocked = self._open_circuit_result(url)
        if blocked is not None:
            return blocked

        await self._throttle(self._host_of(url))
        request_headers = {**self.default_headers, **(headers or {})} if headers else None

        try:
            async with self._client() as client:
                response = await client.get(url, params=params, headers=request_headers)
        except ValueError as exc:
            # Raised by the URL validation hook for blocked or invalid targets.
            return CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error=str(exc),
                error_code=FailureCode.FETCH_BLOCKED.value,
                url=url,
            )
        except httpx.TimeoutException as exc:
            # Split from TransportError: a timeout is a distinct operational
            # signal from a connection-level fault, and collapsing them would
            # hide timeouts in the failure breakdown.
            self._record_failure(exc)
            return CrawlResult.failure(
                CrawlOutcome.RETRYABLE_ERROR,
                error=f"{type(exc).__name__}: {exc}",
                error_code=FailureCode.FETCH_TIMEOUT.value,
                url=url,
            )
        except httpx.TransportError as exc:
            self._record_failure(exc)
            return CrawlResult.failure(
                CrawlOutcome.RETRYABLE_ERROR,
                error=f"{type(exc).__name__}: {exc}",
                error_code=FailureCode.FETCH_HTTP_ERROR.value,
                url=url,
            )
        except httpx.HTTPError as exc:
            self._record_failure(exc)
            return CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error=f"{type(exc).__name__}: {exc}",
                error_code=FailureCode.FETCH_HTTP_ERROR.value,
                url=url,
            )

        result = self._classify(response, url=url, decode_json=decode_json)
        if result.ok or result.outcome is CrawlOutcome.EMPTY:
            self.breaker.record_success()
        else:
            self._record_failure(
                RuntimeError(result.error or str(result.outcome)),
                retry_after=result.retry_after,
            )
        return result

    def _record_failure(self, exc: Exception, *, retry_after: float | None = None) -> None:
        """Record a failed attempt against the circuit and the adaptive limiter."""
        self.breaker.record_failure(exc)
        self.rate_limiter.record_rate_limit(retry_after)

    def _open_circuit_result(self, url: str) -> CrawlResult[Any] | None:
        """Return a fast-fail result while the circuit is open.

        `get_state()` moves an expired OPEN circuit to HALF_OPEN, so this also
        acts as the probe gate: a HALF_OPEN circuit is allowed through.
        """
        stats = self.breaker.get_stats()
        if stats.state is not CircuitState.OPEN:
            return None
        remaining = 0.0
        if stats.last_failure_time is not None:
            elapsed = time.time() - stats.last_failure_time
            remaining = max(0.0, stats.recovery_timeout_seconds - elapsed)
        logger.warning(
            "[%s] circuit OPEN, skipping request to %s (cooldown %.1fs)",
            self.name,
            url,
            remaining,
        )
        return CrawlResult.failure(
            CrawlOutcome.RETRYABLE_ERROR,
            error=f"circuit breaker {stats.name} is OPEN (cooldown {remaining:.1f}s)",
            error_code=FailureCode.FETCH_HTTP_ERROR.value,
            url=url,
        )

    def _classify(self, response: httpx.Response, *, url: str, decode_json: bool) -> CrawlResult[Any]:
        """Map an HTTP response onto a `CrawlOutcome`."""
        status = response.status_code

        if status in THROTTLE_STATUS_CODES:
            retry_after = parse_retry_after(response.headers.get("Retry-After"))
            return CrawlResult.failure(
                CrawlOutcome.RETRYABLE_ERROR,
                error=f"throttled: HTTP {status}",
                error_code=failure_code_for_status(status).value,
                http_status=status,
                retry_after=retry_after,
                url=url,
            )

        if status in RETRYABLE_STATUS_CODES:
            return CrawlResult.failure(
                CrawlOutcome.RETRYABLE_ERROR,
                error=f"HTTP {status}",
                error_code=failure_code_for_status(status).value,
                http_status=status,
                url=url,
            )

        if not response.is_success:
            return CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error=f"HTTP {status}",
                error_code=failure_code_for_status(status).value,
                http_status=status,
                url=url,
            )

        self.rate_limiter.record_success()
        if decode_json:
            return self._parse_json_body(response, url=url, status=status)
        body = response.text
        if not body.strip():
            return CrawlResult.empty(http_status=status, url=url)
        return CrawlResult.success(body, http_status=status, url=url)

    def _parse_json_body(
        self,
        response: httpx.Response,
        *,
        url: str,
        status: int,
    ) -> CrawlResult[Any]:
        """Parse a successful response, treating an undecodable body as drift."""
        content_type = response.headers.get("Content-Type", "")
        body = response.text.lstrip()
        looks_like_html = "html" in content_type.lower() or body[:1] in {"<", ""}

        if looks_like_html and not self._extracts_json_from_html(body):
            # A 200 that is not the payload we asked for means the page
            # structure changed, not that the site is down.
            logger.error("[%s] schema drift suspected at %s (content-type=%s)", self.name, url, content_type)
            return CrawlResult.failure(
                CrawlOutcome.SCHEMA_CHANGED,
                error=f"expected JSON, received {content_type or 'unknown content type'}",
                error_code=FailureCode.PARSE_INVALID_FORMAT.value,
                http_status=status,
                url=url,
            )

        try:
            data = response.json()
        except (ValueError, TypeError) as exc:
            return CrawlResult.failure(
                CrawlOutcome.SCHEMA_CHANGED,
                error=f"JSON decode failed: {exc}",
                error_code=FailureCode.PARSE_INVALID_FORMAT.value,
                http_status=status,
                url=url,
            )

        if data is None or (isinstance(data, (list, dict, str)) and not data):
            return CrawlResult.empty(http_status=status, url=url)

        return CrawlResult.success(data, http_status=status, url=url)

    @staticmethod
    def _extracts_json_from_html(body: str) -> bool:
        """Return whether an HTML-looking body actually wraps a JSON payload."""
        start = body.find("{")
        if start == -1:
            start = body.find("[")
        return start != -1 and start <= MAX_INLINE_JSON_OFFSET


__all__ = [
    "DEFAULT_HTTP_POLICY",
    "MAX_INLINE_JSON_OFFSET",
    "RETRYABLE_STATUS_CODES",
    "THROTTLE_STATUS_CODES",
    "CircuitPolicy",
    "CrawlerHttpClient",
    "HttpPolicy",
]
