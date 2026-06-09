"""Tests for AsyncPlaywrightPool — browser/page pool."""

import asyncio
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from src.utils.playwright_pool import AsyncPlaywrightPool


class TestInit:
    def test_default_values(self):
        pool = AsyncPlaywrightPool()
        assert pool.max_pages == 1
        assert pool.headless is True
        assert pool.browser_type == "chromium"
        assert pool.block_resources is True
        assert pool._started is False
        assert pool._queue is None
        assert pool._pages == []

    def test_custom_values(self):
        pool = AsyncPlaywrightPool(max_pages=3, headless=False, block_resources=False, timeout_ms=5000)
        assert pool.max_pages == 3
        assert pool.headless is False
        assert pool.block_resources is False
        assert pool.timeout_ms == 5000

    def test_context_kwargs_defaults_to_empty(self):
        pool = AsyncPlaywrightPool()
        assert pool.context_kwargs == {}


class TestAcquireRelease:
    @pytest.mark.asyncio
    async def test_acquire_starts_pool_if_not_started(self):
        pool = AsyncPlaywrightPool(max_pages=1)
        pool.start = AsyncMock()
        pool._queue = asyncio.Queue()
        await pool._queue.put("mock_page")
        page = await pool.acquire()
        assert page == "mock_page"
        pool.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_acquire_raises_if_no_queue(self):
        pool = AsyncPlaywrightPool()
        pool._started = True
        with pytest.raises(RuntimeError, match="not initialized"):
            await pool.acquire()

    @pytest.mark.asyncio
    async def test_release_puts_back(self):
        pool = AsyncPlaywrightPool()
        pool._queue = asyncio.Queue(maxsize=1)
        page = MagicMock()
        page.is_closed.return_value = False
        await pool.release(page)
        assert pool._queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_release_replaces_closed_page(self):
        pool = AsyncPlaywrightPool()
        pool._queue = asyncio.Queue(maxsize=1)
        pool._context = MagicMock()
        pool._context.new_page = AsyncMock(return_value="new_page")
        closed_page = MagicMock()
        closed_page.is_closed.return_value = True
        await pool.release(closed_page)
        assert pool._queue.qsize() == 1
        assert "new_page" in pool._pages


class TestPageContextManager:
    @pytest.mark.asyncio
    async def test_acquire_and_release(self):
        pool = AsyncPlaywrightPool()
        pool.acquire = AsyncMock(return_value="mock_page")
        pool.release = AsyncMock()
        async with pool.page() as p:
            assert p == "mock_page"
        pool.release.assert_awaited_once_with("mock_page")

    @pytest.mark.asyncio
    async def test_release_on_exception(self):
        pool = AsyncPlaywrightPool()
        pool.acquire = AsyncMock(return_value="mock_page")
        pool.release = AsyncMock()
        with pytest.raises(ValueError):
            async with pool.page() as p:
                raise ValueError("test")
        pool.release.assert_awaited_once_with("mock_page")


class TestClose:
    @pytest.mark.asyncio
    async def test_close_not_started_does_nothing(self):
        pool = AsyncPlaywrightPool()
        await pool.close()
        assert not pool._started

    @pytest.mark.asyncio
    async def test_close_cleanup(self):
        pool = AsyncPlaywrightPool()
        pool._started = True
        page_mock = AsyncMock()
        pool._pages = [page_mock]
        ctx = AsyncMock()
        browser = AsyncMock()
        pw = MagicMock()
        pool._context = ctx
        pool._browser = browser
        pool._playwright = pw
        pool._queue = asyncio.Queue()
        await pool.close()
        page_mock.close.assert_awaited_once()
        ctx.close.assert_awaited_once()
        browser.close.assert_awaited_once()
        pw.stop.assert_called_once()
        assert pool._pages == []
        assert pool._context is None


class TestStart:
    @pytest.mark.asyncio
    async def test_start_without_auth(self):
        pool = AsyncPlaywrightPool(max_pages=1, block_resources=False)
        mock_playwright = MagicMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()

        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_playwright.chromium.launch = AsyncMock(return_value=mock_browser)

        async_pw = AsyncMock()
        async_pw.start = AsyncMock(return_value=mock_playwright)

        with patch("playwright.async_api.async_playwright", return_value=async_pw):
            await pool.start()
            assert pool._started
            assert pool._browser is not None
            assert pool._queue.qsize() == 1
