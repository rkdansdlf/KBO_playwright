"""Extended tests for AsyncPlaywrightPool — covers auth, stealth, options errors."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.utils.playwright_pool import AsyncPlaywrightPool, AsyncPlaywrightPoolOptions


class TestOptionsValidation:
    def test_overrides_only(self):
        pool = AsyncPlaywrightPool(max_pages=2, headless=False)
        assert pool.max_pages == 2
        assert pool.headless is False

    def test_options_object_only(self):
        opts = AsyncPlaywrightPoolOptions(max_pages=4, browser_type="firefox")
        pool = AsyncPlaywrightPool(opts)
        assert pool.max_pages == 4
        assert pool.browser_type == "firefox"

    def test_both_options_and_overrides_raises(self):
        opts = AsyncPlaywrightPoolOptions(max_pages=1)
        with pytest.raises(TypeError, match="not both"):
            AsyncPlaywrightPool(opts, max_pages=2)

    def test_context_kwargs_none_becomes_empty_dict(self):
        pool = AsyncPlaywrightPool()
        assert pool.context_kwargs == {}

    def test_context_kwargs_preserved(self):
        pool = AsyncPlaywrightPool(context_kwargs={"locale": "ko-KR"})
        assert pool.context_kwargs == {"locale": "ko-KR"}


class TestStealthScript:
    def test_stealth_script_returns_string(self):
        script = AsyncPlaywrightPool._stealth_script()
        assert isinstance(script, str)

    def test_stealth_script_contains_webdriver_mask(self):
        script = AsyncPlaywrightPool._stealth_script()
        assert "navigator.webdriver" in script

    def test_stealth_script_contains_chrome_mock(self):
        script = AsyncPlaywrightPool._stealth_script()
        assert "window.chrome" in script

    def test_stealth_script_contains_languages(self):
        script = AsyncPlaywrightPool._stealth_script()
        assert "navigator.languages" in script

    def test_stealth_script_contains_webgl_vendor(self):
        script = AsyncPlaywrightPool._stealth_script()
        assert "WebGLRenderingContext" in script


class TestPrepareAuthState:
    @pytest.mark.asyncio
    async def test_skips_when_not_requires_auth(self):
        pool = AsyncPlaywrightPool(requires_auth=False)
        with patch("src.utils.kbo_auth.KboAuthenticator") as mock_auth:
            await pool._prepare_auth_state()
            mock_auth.is_authenticated.assert_not_called()

    @pytest.mark.asyncio
    async def test_triggers_login_when_not_authenticated(self):
        pool = AsyncPlaywrightPool(requires_auth=True)
        with patch("src.utils.kbo_auth.KboAuthenticator") as mock_auth_cls:
            mock_auth_cls.is_authenticated = MagicMock(return_value=False)
            mock_auth = MagicMock()
            mock_auth.login = AsyncMock(return_value=True)
            mock_auth_cls.return_value = mock_auth
            await pool._prepare_auth_state()
            mock_auth.login.assert_awaited_once_with(headless=True)

    @pytest.mark.asyncio
    async def test_sets_storage_state_when_authenticated(self):
        pool = AsyncPlaywrightPool(requires_auth=True, headless=False)
        with patch("src.utils.kbo_auth.KboAuthenticator") as mock_auth_cls:
            mock_auth_cls.is_authenticated = MagicMock(return_value=True)
            mock_auth_cls.get_auth_state_path = MagicMock(return_value="/tmp/auth.json")
            await pool._prepare_auth_state()
            assert pool.context_kwargs["storage_state"] == "/tmp/auth.json"

    @pytest.mark.asyncio
    async def test_login_fails_proceeds_without_auth(self):
        pool = AsyncPlaywrightPool(requires_auth=True)
        with patch("src.utils.kbo_auth.KboAuthenticator") as mock_auth_cls:
            mock_auth_cls.is_authenticated = MagicMock(return_value=False)
            mock_auth = MagicMock()
            mock_auth.login = AsyncMock(return_value=False)
            mock_auth_cls.return_value = mock_auth
            await pool._prepare_auth_state()
            assert "storage_state" not in pool.context_kwargs


class TestStartBrowserContextWithUserAgent:
    @pytest.mark.asyncio
    async def test_rotates_user_agent_when_not_provided(self):
        pool = AsyncPlaywrightPool(block_resources=False)
        mock_playwright = MagicMock()
        mock_playwright.stop = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_playwright.chromium.launch = AsyncMock(return_value=mock_browser)

        async_pw = AsyncMock()
        async_pw.start = AsyncMock(return_value=mock_playwright)

        with patch("src.utils.playwright_pool.async_playwright", return_value=async_pw):
            with patch("src.utils.request_policy.RequestPolicy") as mock_policy_cls:
                mock_policy = MagicMock()
                mock_policy.random_user_agent = MagicMock(return_value="TestAgent/1.0")
                mock_policy_cls.return_value = mock_policy
                await pool._start_browser_context()

        mock_policy.random_user_agent.assert_called_once()
        call_kwargs = mock_browser.new_context.call_args[1]
        assert call_kwargs["user_agent"] == "TestAgent/1.0"

    @pytest.mark.asyncio
    async def test_uses_provided_user_agent(self):
        pool = AsyncPlaywrightPool(block_resources=False, context_kwargs={"user_agent": "CustomAgent/2.0"})
        mock_playwright = MagicMock()
        mock_playwright.stop = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_playwright.chromium.launch = AsyncMock(return_value=mock_browser)

        async_pw = AsyncMock()
        async_pw.start = AsyncMock(return_value=mock_playwright)

        with patch("src.utils.playwright_pool.async_playwright", return_value=async_pw):
            with patch("src.utils.request_policy.RequestPolicy") as mock_policy_cls:
                await pool._start_browser_context()

        mock_policy_cls.assert_not_called()
        call_kwargs = mock_browser.new_context.call_args[1]
        assert call_kwargs["user_agent"] == "CustomAgent/2.0"


class TestCreatePages:
    @pytest.mark.asyncio
    async def test_raises_if_no_context(self):
        pool = AsyncPlaywrightPool()
        pool._context = None
        with pytest.raises(RuntimeError, match="context not initialized"):
            await pool._create_pages()

    @pytest.mark.asyncio
    async def test_sets_timeout_on_pages(self):
        pool = AsyncPlaywrightPool(max_pages=2, timeout_ms=7500)
        mock_context = MagicMock()
        mock_page_1 = MagicMock()
        mock_page_2 = MagicMock()
        mock_context.new_page = AsyncMock(side_effect=[mock_page_1, mock_page_2])
        pool._context = mock_context

        await pool._create_pages()

        mock_page_1.set_default_timeout.assert_called_once_with(7500)
        mock_page_2.set_default_timeout.assert_called_once_with(7500)
        assert len(pool._pages) == 2
        assert pool._queue.qsize() == 2


class TestStartIdempotent:
    @pytest.mark.asyncio
    async def test_start_twice_skips_second(self):
        pool = AsyncPlaywrightPool(block_resources=False)
        pool._started = True
        with patch.object(pool, "_prepare_auth_state", new=AsyncMock()) as mock_prep:
            await pool.start()
            mock_prep.assert_not_awaited()


class TestContextManager:
    @pytest.mark.asyncio
    async def test_aenter_aexit(self):
        pool = AsyncPlaywrightPool()
        pool.start = AsyncMock()
        pool.close = AsyncMock()
        async with pool as p:
            assert p is pool
            pool.start.assert_awaited_once()
        pool.close.assert_awaited_once()


class TestReleaseWithTimeout:
    @pytest.mark.asyncio
    async def test_replaced_closed_page_gets_timeout(self):
        pool = AsyncPlaywrightPool(timeout_ms=3000)
        pool._queue = asyncio.Queue(maxsize=1)
        pool._context = MagicMock()
        new_page = MagicMock()
        pool._context.new_page = AsyncMock(return_value=new_page)
        closed_page = MagicMock()
        closed_page.is_closed.return_value = True

        await pool.release(closed_page)

        new_page.set_default_timeout.assert_called_once_with(3000)
        assert pool._queue.qsize() == 1
