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

        with patch("src.crawlers.base.validate_url", return_value=(True, "OK")):
            await crawler.goto_with_retry(mock_page, "https://example.com", max_attempts=2, min_wait=0.01)
        mock_page.goto.assert_awaited_once_with("https://example.com", wait_until="networkidle", timeout=30000)

    @pytest.mark.asyncio
    async def test_goto_with_retry_recovers_after_timeout(self) -> None:
        crawler = DummyPlaywrightCrawler()
        mock_page = MagicMock()
        mock_page.goto = AsyncMock(side_effect=[PlaywrightTimeoutError("timeout"), None])

        with patch("src.crawlers.base.validate_url", return_value=(True, "OK")):
            await crawler.goto_with_retry(
                mock_page, "https://example.com", max_attempts=2, min_wait=0.01, max_wait=0.02
            )
        assert mock_page.goto.await_count == 2


class TestPlaywrightUrlValidation:
    @pytest.mark.parametrize(
        "url",
        [
            "file:///tmp/test.html",
            "ftp://example.com/file",
            "data:text/html,test",
            "javascript:void(0)",
            "http://127.0.0.1",
            "http://10.0.0.1",
            "http://192.168.1.1",
            "http://169.254.169.254",
            "http://[::1]",
            "http://[ff02::1]",
            "https://example.com:bad",
            "https://",
        ],
    )
    async def test_rejects_before_navigation(self, url):
        page = MagicMock()
        page.goto = AsyncMock()
        with pytest.raises(ValueError):
            await DummyPlaywrightCrawler().goto_with_retry(page, url)
        page.goto.assert_not_awaited()

    @pytest.mark.parametrize("addresses", [[], ["127.0.0.1"], ["8.8.8.8", "10.0.0.1"], ["::1"]])
    async def test_rejects_dns_results(self, monkeypatch, addresses):
        monkeypatch.setattr(
            "socket.getaddrinfo", lambda *args, **kwargs: [(2, 1, 6, "", (ip, 443)) for ip in addresses]
        )
        page = MagicMock()
        page.goto = AsyncMock()
        with pytest.raises(ValueError):
            await DummyPlaywrightCrawler().goto_with_retry(page, "https://example.com")
        page.goto.assert_not_awaited()

    @pytest.mark.parametrize("wait_until", ["load", "domcontentloaded", "networkidle"])
    async def test_public_navigation_preserves_options(self, monkeypatch, wait_until):
        monkeypatch.setattr("socket.getaddrinfo", lambda *args, **kwargs: [(2, 1, 6, "", ("8.8.8.8", 443))])
        page = MagicMock()
        page.goto = AsyncMock()
        await DummyPlaywrightCrawler().goto_with_retry(page, "https://example.com", wait_until=wait_until, timeout=1234)
        page.goto.assert_awaited_once_with("https://example.com", wait_until=wait_until, timeout=1234)

    async def test_revalidates_before_retry(self):
        page = MagicMock()
        page.goto = AsyncMock(side_effect=PlaywrightTimeoutError("timeout"))
        with patch("src.crawlers.base.validate_url", side_effect=[(True, "OK"), (False, "blocked")]) as validate:
            with pytest.raises(ValueError, match="blocked"):
                await DummyPlaywrightCrawler().goto_with_retry(page, "https://example.com", min_wait=0, max_wait=0)
        assert validate.call_count == 2
        page.goto.assert_awaited_once()

    async def test_dns_failure_never_navigates(self):
        import socket

        page = MagicMock()
        page.goto = AsyncMock()
        with patch("socket.getaddrinfo", side_effect=socket.gaierror("unavailable")):
            with pytest.raises(ValueError, match="DNS resolution failed"):
                await DummyPlaywrightCrawler().goto_with_retry(page, "https://example.com")
        page.goto.assert_not_awaited()

    async def test_validation_runs_off_event_loop(self):
        import threading

        main_thread = threading.get_ident()
        threads = []

        def validate(url):
            threads.append(threading.get_ident())
            return True, "OK"

        page = MagicMock()
        page.goto = AsyncMock()
        with patch("src.crawlers.base.validate_url", side_effect=validate):
            await DummyPlaywrightCrawler().goto_with_retry(page, "https://example.com")
        assert len(threads) == 1
        assert threads[0] != main_thread

    async def test_timeout_exhaustion_preserved(self):
        page = MagicMock()
        page.goto = AsyncMock(side_effect=PlaywrightTimeoutError("timeout"))
        with patch("src.crawlers.base.validate_url", return_value=(True, "OK")) as validate:
            with pytest.raises(PlaywrightTimeoutError):
                await DummyPlaywrightCrawler().goto_with_retry(
                    page, "https://example.com", max_attempts=2, min_wait=0, max_wait=0
                )
        assert validate.call_count == page.goto.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("redirect", [False, True])
async def test_http_client_blocks_private_targets(monkeypatch, redirect):
    import socket

    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [(2, 1, 6, "", ("8.8.8.8", 443))])
    requests = []

    async def handle(request):
        requests.append(str(request.url))
        return httpx.Response(302, headers={"Location": "http://127.0.0.1/internal"})

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        "src.crawlers.base.httpx.AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handle), **kwargs),
    )
    crawler = DummyHttpCrawler(request_delay=0)
    async with crawler.http_client() as client:
        with pytest.raises(ValueError, match="private/reserved"):
            await client.get("https://example.com" if redirect else "http://127.0.0.1/internal")
    assert requests == (["https://example.com"] if redirect else [])


@pytest.mark.asyncio
async def test_http_client_accepts_public_source(monkeypatch):
    import socket

    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [(2, 1, 6, "", ("8.8.8.8", 443))])
    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        "src.crawlers.base.httpx.AsyncClient",
        lambda **kwargs: real_client(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"ok": True})), **kwargs
        ),
    )
    crawler = DummyHttpCrawler(request_delay=0)
    async with crawler.http_client() as client:
        response = await client.get("https://example.com")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


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
