"""Tests for kbo_auth — KBO login authentication."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.utils.kbo_auth import KboAuthenticator


class TestInit:
    def test_uses_env_when_no_args(self):
        with patch.dict("os.environ", {"KBO_USER_ID": "test_user", "KBO_USER_PWD": "test_pwd"}, clear=True):
            auth = KboAuthenticator()
            assert auth.user_id == "test_user"
            assert auth.user_pwd == "test_pwd"

    def test_constructor_overrides_env(self):
        with patch.dict("os.environ", {"KBO_USER_ID": "env_user", "KBO_USER_PWD": "env_pwd"}, clear=True):
            auth = KboAuthenticator(user_id="arg_user", user_pwd="arg_pwd")
            assert auth.user_id == "arg_user"
            assert auth.user_pwd == "arg_pwd"

    def test_empty_creds(self):
        with patch.dict("os.environ", {}, clear=True):
            auth = KboAuthenticator()
            assert auth.user_id is None
            assert auth.user_pwd is None

    def test_constants(self):
        assert "Login.aspx" in KboAuthenticator.LOGIN_URL
        assert "kbo_auth_state.json" in KboAuthenticator.AUTH_STATE_PATH


class TestLogin:
    @pytest.mark.asyncio
    async def test_returns_false_when_no_creds(self):
        with patch.dict("os.environ", {}, clear=True):
            auth = KboAuthenticator()
            result = await auth.login()
            assert not result

    @pytest.mark.asyncio
    async def test_returns_false_when_no_user_id(self):
        with patch.dict("os.environ", {"KBO_USER_PWD": "pwd"}, clear=True):
            auth = KboAuthenticator()
            result = await auth.login()
            assert not result

    @pytest.mark.asyncio
    async def test_login_attempt_with_creds(self):
        with patch.dict("os.environ", {"KBO_USER_ID": "user", "KBO_USER_PWD": "pwd"}, clear=True):
            auth = KboAuthenticator()

            mock_page = MagicMock()
            mock_page.goto = AsyncMock()
            mock_page.fill = AsyncMock()
            mock_page.click = AsyncMock()
            mock_page.wait_for_load_state = AsyncMock()
            mock_page.content = AsyncMock(return_value="<html>로그아웃</html>")
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

            with (
                patch("src.utils.kbo_auth.async_playwright") as mock_async_pw,
                patch("asyncio.sleep", AsyncMock()),
                patch("os.makedirs"),
            ):
                mock_async_pw.return_value.__aenter__ = AsyncMock(return_value=mock_playwright)
                mock_async_pw.return_value.__aexit__ = AsyncMock(return_value=None)

                result = await auth.login(headless=True)
                assert result
                mock_page.goto.assert_called()
                mock_page.fill.assert_called()
                mock_page.click.assert_called()
