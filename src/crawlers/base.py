"""Base classes and common abstractions for KBO crawlers."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import httpx
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.crawlers.dto import CrawlExecutionStats
from src.crawlers.resilience import AdaptiveRateLimiter
from src.utils.playwright_pool import AsyncPlaywrightPool  # noqa: TC001
from src.utils.playwright_retry import NAV_TIMEOUT
from src.utils.request_policy import RequestPolicy

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

PLAYWRIGHT_RETRY_EXCEPTIONS = (PlaywrightTimeoutError, PlaywrightError)


class BaseCrawler:
    """Base crawler providing request policy, logging, and rate limiting."""

    def __init__(
        self,
        request_delay: float = 1.0,
        policy: RequestPolicy | None = None,
    ) -> None:
        """Initialize BaseCrawler.

        Args:
            request_delay: Delay between requests in seconds.
            policy: Optional RequestPolicy instance. If None, created with request_delay.

        """
        self.request_delay = request_delay
        self.policy = policy or RequestPolicy.with_delay(request_delay)
        self.logger = logging.getLogger(self.__class__.__module__)
        self.stats = CrawlExecutionStats()
        self.rate_limiter = AdaptiveRateLimiter(base_delay_seconds=request_delay)

    async def throttle(self) -> None:
        """Apply request throttling based on request_delay and adaptive rate limiting."""
        if self.request_delay > 0:
            waited = await self.rate_limiter.acquire()
            self.stats.record_throttle(waited)

    def get_stats(self) -> CrawlExecutionStats:
        """Return the accumulated execution metrics for this crawler."""
        return self.stats

    @property
    def crawler_name(self) -> str:
        """Return the name of this crawler class."""
        return self.__class__.__name__


class BasePlaywrightCrawler(BaseCrawler):
    """Base crawler for Playwright browser automation with pooled page management."""

    def __init__(
        self,
        request_delay: float = 1.0,
        pool: AsyncPlaywrightPool | None = None,
        policy: RequestPolicy | None = None,
        max_pages: int = 1,
    ) -> None:
        """Initialize BasePlaywrightCrawler.

        Args:
            request_delay: Delay between requests.
            pool: Existing Playwright pool or None to create an internal one.
            policy: Request policy instance.
            max_pages: Max pages if internal pool is created.

        """
        super().__init__(request_delay=request_delay, policy=policy)
        self.pool = pool
        self._max_pages = max_pages

    def _create_pool(self) -> AsyncPlaywrightPool:
        """Create an AsyncPlaywrightPool, resolving mock from caller module or base module if patched."""
        import sys

        from src.utils.playwright_pool import AsyncPlaywrightPool as RealAsyncPlaywrightPool

        base_mod = sys.modules.get("src.crawlers.base")
        base_pool = getattr(base_mod, "AsyncPlaywrightPool", None) if base_mod else None
        if base_pool is not None and base_pool is not RealAsyncPlaywrightPool:
            return base_pool(max_pages=self._max_pages)  # type: ignore[no-any-return]

        mod = sys.modules.get(self.__class__.__module__)
        if mod and hasattr(mod, "AsyncPlaywrightPool"):
            target_cls = mod.AsyncPlaywrightPool
            if target_cls is not None and target_cls is not RealAsyncPlaywrightPool:
                return target_cls(max_pages=self._max_pages)  # type: ignore[no-any-return]

        return RealAsyncPlaywrightPool(max_pages=self._max_pages)

    @asynccontextmanager
    async def page_context(self) -> AsyncIterator[Page]:
        """Async context manager that manages pool startup, page acquisition, and release."""
        pool = self.pool or self._create_pool()
        owns_pool = self.pool is None

        await pool.start()

        page = await pool.acquire()
        try:
            yield page
        finally:
            await pool.release(page)
            if owns_pool:
                await pool.close()

    async def goto_with_retry(  # noqa: PLR0913
        self,
        page: Page,
        url: str,
        *,
        wait_until: str = "networkidle",
        timeout: float = NAV_TIMEOUT,  # noqa: ASYNC109
        max_attempts: int = 3,
        min_wait: float = 2.0,
        max_wait: float = 10.0,
    ) -> None:
        """Navigate to a URL with exponential backoff retries on Playwright errors."""
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
            retry=retry_if_exception_type(PLAYWRIGHT_RETRY_EXCEPTIONS),
            reraise=True,
        ):
            with attempt:
                self.logger.debug("Navigating to %s (attempt %d)", url, attempt.retry_state.attempt_number)
                await page.goto(url, wait_until=wait_until, timeout=timeout)  # type: ignore[arg-type]


class BaseHttpCrawler(BaseCrawler):
    """Base crawler for HTTP/API requests using httpx.AsyncClient."""

    def __init__(
        self,
        request_delay: float = 0.5,
        policy: RequestPolicy | None = None,
        default_headers: dict[str, str] | None = None,
        timeout: float = 15.0,
    ) -> None:
        """Initialize BaseHttpCrawler.

        Args:
            request_delay: Delay between requests.
            policy: Request policy instance.
            default_headers: Default HTTP headers.
            timeout: Default HTTP timeout in seconds.

        """
        super().__init__(request_delay=request_delay, policy=policy)
        self.default_headers = default_headers or {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
        }
        self.timeout = timeout

    @asynccontextmanager
    async def http_client(
        self,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> AsyncIterator[httpx.AsyncClient]:
        """Async context manager that creates and closes an httpx.AsyncClient."""
        merged_headers = {**self.default_headers, **(headers or {})}
        client_timeout = timeout or self.timeout
        async with httpx.AsyncClient(
            headers=merged_headers,
            timeout=client_timeout,
            follow_redirects=True,
        ) as client:
            yield client

    async def fetch_json(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any] | None:
        """Fetch JSON payload safely with error logging."""
        await self.throttle()
        try:
            resp = await client.get(url, params=params)
            if resp.status_code != HTTPStatus.OK:
                self.logger.warning("HTTP %d when fetching %s", resp.status_code, url)
                return None
            return resp.json()
        except (httpx.HTTPError, ValueError, TypeError, KeyError):
            self.logger.exception("Failed to fetch JSON from %s", url)
            return None
