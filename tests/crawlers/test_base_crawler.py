"""Tests for BaseCrawler, BasePlaywrightCrawler, and BaseHttpCrawler."""

from __future__ import annotations

import asyncio
from http import HTTPStatus
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from src.crawlers.base import (
    BaseCrawler,
    BaseHttpCrawler,
    BasePlaywrightCrawler,
)
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.request_policy import RequestPolicy


class DummyCrawler(BaseCrawler):
    """Test concrete implementation of BaseCrawler."""


class DummyPlaywrightCrawler(BasePlaywrightCrawler):
    """Test concrete implementation of BasePlaywrightCrawler."""


class DummyHttpCrawler(BaseHttpCrawler):
    """Test concrete implementation of BaseHttpCrawler."""


class TestBaseCrawler:
    def test_init_defaults(self) -> None:
        crawler = DummyCrawler()
        assert crawler.request_delay == 1.0
        assert isinstance(crawler.policy, RequestPolicy)
        assert crawler.crawler_name == "DummyCrawler"

    def test_init_custom_policy(self) -> None:
        policy = RequestPolicy.with_delay(2.5)
        crawler = DummyCrawler(request_delay=2.5, policy=policy)
        assert crawler.request_delay == 2.5
        assert crawler.policy is policy

    @pytest.mark.asyncio
    async def test_throttle(self) -> None:
        crawler = DummyCrawler(request_delay=0.01)
        start = asyncio.get_event_loop().time()
        await crawler.throttle()
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed >= 0.005


class TestBasePlaywrightCrawler:
    def test_init_defaults(self) -> None:
        crawler = DummyPlaywrightCrawler()
        assert crawler.pool is None
        assert crawler.request_delay == 1.0

    @pytest.mark.asyncio
    async def test_page_context_with_own_pool(self) -> None:
        crawler = DummyPlaywrightCrawler()
        mock_pool = MagicMock(spec=AsyncPlaywrightPool)
        mock_pool.start = AsyncMock()
        mock_pool.close = AsyncMock()
        mock_page = MagicMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_pool.release = AsyncMock()

        with patch("src.crawlers.base.AsyncPlaywrightPool", return_value=mock_pool):
            async with crawler.page_context() as page:
                assert page is mock_page

        mock_pool.start.assert_awaited_once()
        mock_pool.acquire.assert_awaited_once()
        mock_pool.release.assert_awaited_once_with(mock_page)
        mock_pool.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_page_context_with_shared_pool(self) -> None:
        mock_pool = MagicMock(spec=AsyncPlaywrightPool)
        mock_page = MagicMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_pool.release = AsyncMock()
        mock_pool.start = AsyncMock()
        mock_pool.close = AsyncMock()

        crawler = DummyPlaywrightCrawler(pool=mock_pool)
        async with crawler.page_context() as page:
            assert page is mock_page

        mock_pool.acquire.assert_awaited_once()
        mock_pool.release.assert_awaited_once_with(mock_page)
        mock_pool.start.assert_awaited_once()
        mock_pool.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_goto_with_retry_success(self) -> None:
        crawler = DummyPlaywrightCrawler()
        mock_page = MagicMock()
        mock_page.goto = AsyncMock()

        await crawler.goto_with_retry(mock_page, "https://example.com", max_attempts=2, min_wait=0.01)
        mock_page.goto.assert_awaited_once_with("https://example.com", wait_until="networkidle", timeout=30000)

    @pytest.mark.asyncio
    async def test_goto_with_retry_recovers_after_timeout(self) -> None:
        crawler = DummyPlaywrightCrawler()
        mock_page = MagicMock()
        mock_page.goto = AsyncMock(side_effect=[PlaywrightTimeoutError("timeout"), None])

        await crawler.goto_with_retry(mock_page, "https://example.com", max_attempts=2, min_wait=0.01, max_wait=0.02)
        assert mock_page.goto.await_count == 2


class TestBaseHttpCrawler:
    def test_init_defaults(self) -> None:
        crawler = DummyHttpCrawler()
        assert crawler.request_delay == 0.5
        assert "User-Agent" in crawler.default_headers
        assert crawler.timeout == 15.0

    @pytest.mark.asyncio
    async def test_http_client_context(self) -> None:
        crawler = DummyHttpCrawler(default_headers={"X-Custom": "val"})
        async with crawler.http_client() as client:
            assert isinstance(client, httpx.AsyncClient)
            assert client.headers.get("x-custom") == "val"

    @pytest.mark.asyncio
    async def test_fetch_json_success(self) -> None:
        crawler = DummyHttpCrawler()
        mock_resp = MagicMock()
        mock_resp.status_code = HTTPStatus.OK
        mock_resp.json.return_value = {"key": "value"}

        mock_client = MagicMock(spec=httpx.AsyncClient)
        mock_client.get = AsyncMock(return_value=mock_resp)

        result = await crawler.fetch_json(mock_client, "https://api.example.com/data")
        assert result == {"key": "value"}

    @pytest.mark.asyncio
    async def test_fetch_json_non_200(self) -> None:
        crawler = DummyHttpCrawler()
        mock_resp = MagicMock()
        mock_resp.status_code = HTTPStatus.NOT_FOUND

        mock_client = MagicMock(spec=httpx.AsyncClient)
        mock_client.get = AsyncMock(return_value=mock_resp)

        result = await crawler.fetch_json(mock_client, "https://api.example.com/404")
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_json_exception(self) -> None:
        crawler = DummyHttpCrawler()
        mock_client = MagicMock(spec=httpx.AsyncClient)
        mock_client.get = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))

        result = await crawler.fetch_json(mock_client, "https://api.example.com/fail")
        assert result is None
