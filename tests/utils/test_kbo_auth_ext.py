from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.utils.kbo_auth import KboAuthenticator, main


def _make_mock_playwright(login_content="<html>로그아웃</html>", goto_side_effect=None):
    mock_page = MagicMock()
    mock_page.goto = AsyncMock(side_effect=goto_side_effect)
    mock_page.fill = AsyncMock()
    mock_page.click = AsyncMock()
    mock_page.wait_for_load_state = AsyncMock()
    mock_page.content = AsyncMock(return_value=login_content)
    mock_page.evaluate = AsyncMock()

    mock_context = MagicMock()
    mock_context.new_page = AsyncMock(return_value=mock_page)
    mock_context.add_init_script = AsyncMock()
    mock_context.storage_state = AsyncMock(return_value={})
    mock_context.close = AsyncMock()

    mock_browser = MagicMock()
    mock_browser.new_context = AsyncMock(return_value=mock_context)
    mock_browser.close = AsyncMock()

    mock_playwright = MagicMock()
    mock_playwright.chromium = MagicMock()
    mock_playwright.chromium.launch = AsyncMock(return_value=mock_browser)

    return mock_playwright, mock_page


class TestLoginWarmUpError:
    @pytest.mark.asyncio
    async def test_warmup_timeout_still_saves_state(self):
        with patch.dict("os.environ", {"KBO_USER_ID": "user", "KBO_USER_PWD": "pwd"}, clear=True):
            auth = KboAuthenticator()
            mock_pw, mock_page = _make_mock_playwright()

            from playwright.async_api import Error as PlaywrightError

            call_count = 0

            async def goto_side_effect(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 2:
                    raise PlaywrightError("timeout navigating to game center")

            mock_page.goto = AsyncMock(side_effect=goto_side_effect)

            with (
                patch("src.utils.kbo_auth.async_playwright") as mock_async_pw,
                patch("asyncio.sleep", AsyncMock()),
            ):
                mock_async_pw.return_value.__aenter__ = AsyncMock(return_value=mock_pw)
                mock_async_pw.return_value.__aexit__ = AsyncMock(return_value=None)
                result = await auth.login(headless=True)
                assert result is True


class TestLoginException:
    @pytest.mark.asyncio
    async def test_playwright_error_returns_false(self):
        with patch.dict("os.environ", {"KBO_USER_ID": "user", "KBO_USER_PWD": "pwd"}, clear=True):
            auth = KboAuthenticator()
            mock_pw, mock_page = _make_mock_playwright()

            from playwright.async_api import Error as PlaywrightError

            mock_page.goto = AsyncMock(side_effect=PlaywrightError("navigation failed"))

            with patch("src.utils.kbo_auth.async_playwright") as mock_async_pw:
                mock_async_pw.return_value.__aenter__ = AsyncMock(return_value=mock_pw)
                mock_async_pw.return_value.__aexit__ = AsyncMock(return_value=None)
                result = await auth.login(headless=True)
                assert result is False

    @pytest.mark.asyncio
    async def test_os_error_returns_false(self):
        with patch.dict("os.environ", {"KBO_USER_ID": "user", "KBO_USER_PWD": "pwd"}, clear=True):
            auth = KboAuthenticator()
            mock_pw, mock_page = _make_mock_playwright()

            mock_page.goto = AsyncMock(side_effect=OSError("network error"))

            with patch("src.utils.kbo_auth.async_playwright") as mock_async_pw:
                mock_async_pw.return_value.__aenter__ = AsyncMock(return_value=mock_pw)
                mock_async_pw.return_value.__aexit__ = AsyncMock(return_value=None)
                result = await auth.login(headless=True)
                assert result is False


class TestLoginNoLogoutButton:
    @pytest.mark.asyncio
    async def test_returns_false_when_logout_not_found(self):
        with patch.dict("os.environ", {"KBO_USER_ID": "user", "KBO_USER_PWD": "pwd"}, clear=True):
            auth = KboAuthenticator()
            mock_pw, mock_page = _make_mock_playwright(login_content="<html>Login Page</html>")

            with (
                patch("src.utils.kbo_auth.async_playwright") as mock_async_pw,
                patch("asyncio.sleep", AsyncMock()),
            ):
                mock_async_pw.return_value.__aenter__ = AsyncMock(return_value=mock_pw)
                mock_async_pw.return_value.__aexit__ = AsyncMock(return_value=None)
                result = await auth.login(headless=True)
                assert result is False


class TestMain:
    @pytest.mark.asyncio
    async def test_main_success(self):
        with (
            patch.dict("os.environ", {"KBO_USER_ID": "u", "KBO_USER_PWD": "p"}, clear=True),
            patch.object(KboAuthenticator, "login", new_callable=AsyncMock, return_value=True),
            patch("src.utils.kbo_auth.logger"),
        ):
            await main()

    @pytest.mark.asyncio
    async def test_main_failure(self):
        with (
            patch.dict("os.environ", {}, clear=True),
            patch.object(KboAuthenticator, "login", new_callable=AsyncMock, return_value=False),
            patch("src.utils.kbo_auth.logger"),
        ):
            await main()
