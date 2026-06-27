"""Extended tests for playwright_retry — covers error paths and remaining constants."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeout

from src.utils import playwright_retry


class _FailingNavigationPage:
    def __init__(self, goto_exc=None, load_exc=None):
        self.goto_exc = goto_exc
        self.load_exc = load_exc
        self.goto_calls = []
        self.load_calls = []

    def goto(self, url, wait_until=None, timeout=None):
        self.goto_calls.append((url, wait_until, timeout))
        if self.goto_exc is not None:
            raise self.goto_exc

    def wait_for_load_state(self, state, timeout=None):
        self.load_calls.append((state, timeout))
        if self.load_exc is not None:
            raise self.load_exc


class _ReloadPage:
    def __init__(self, selector_failures=0, click_exc=None):
        self.selector_failures = selector_failures
        self.click_exc = click_exc
        self.selector_calls = []
        self.click_calls = []
        self.reload_calls = []

    def wait_for_selector(self, selector, timeout=None, state=None):
        self.selector_calls.append((selector, timeout, state))
        if len(self.selector_calls) <= self.selector_failures:
            raise PlaywrightTimeout("selector timeout")

    def click(self, selector, timeout=None):
        self.click_calls.append((selector, timeout))
        if self.click_exc is not None:
            raise self.click_exc

    def reload(self, wait_until=None, timeout=None):
        self.reload_calls.append((wait_until, timeout))


class TestRetryNavigationPlaywrightError:
    def test_playwright_error_then_succeed(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _FailingNavigationPage(goto_exc=PlaywrightError("net::ERR_FAILED"))
        page.goto = MagicMock(side_effect=[PlaywrightError("fail"), None])
        page.wait_for_load_state = MagicMock()
        ok = playwright_retry.retry_navigation(page, "https://example.test", max_retries=3)
        assert ok is True
        assert page.goto.call_count == 2

    def test_playwright_error_all_attempts_fail(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = MagicMock()
        page.goto = MagicMock(side_effect=PlaywrightError("fail"))
        page.wait_for_load_state = MagicMock()
        ok = playwright_retry.retry_navigation(page, "https://example.test", max_retries=2)
        assert ok is False
        assert page.goto.call_count == 2

    def test_load_state_timeout_returns_false(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _FailingNavigationPage(load_exc=PlaywrightTimeout("load timeout"))
        ok = playwright_retry.retry_navigation(page, "https://example.test", max_retries=1)
        assert ok is False


class TestRetryClickReload:
    def test_reload_on_timeout_then_succeed(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _ReloadPage(selector_failures=1)
        ok = playwright_retry.retry_click(page, "#btn", max_retries=3)
        assert ok is True
        assert len(page.reload_calls) == 1
        assert page.reload_calls[0][0] == "networkidle"

    def test_reload_on_timeout_all_fail(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _ReloadPage(selector_failures=5)
        ok = playwright_retry.retry_click(page, "#btn", max_retries=2)
        assert ok is False
        assert len(page.reload_calls) == 1

    def test_reload_suppresses_errors(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _ReloadPage(selector_failures=5)
        page.reload = MagicMock(side_effect=RuntimeError("reload failed"))
        ok = playwright_retry.retry_click(page, "#btn", max_retries=1)
        assert ok is False


class TestRetryWaitForSelectorReload:
    def test_reload_on_timeout_then_succeed(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _ReloadPage(selector_failures=1)
        ok = playwright_retry.retry_wait_for_selector(page, "#el", max_retries=3)
        assert ok is True
        assert len(page.reload_calls) == 1

    def test_reload_on_timeout_all_fail(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _ReloadPage(selector_failures=5)
        ok = playwright_retry.retry_wait_for_selector(page, "#el", max_retries=2)
        assert ok is False
        assert len(page.reload_calls) == 1

    def test_reload_suppresses_errors(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _ReloadPage(selector_failures=5)
        page.reload = MagicMock(side_effect=PlaywrightTimeout("reload timeout"))
        ok = playwright_retry.retry_wait_for_selector(page, "#el", max_retries=1)
        assert ok is False


class TestRetryClickCustomTimeouts:
    def test_uses_custom_timeouts(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _ReloadPage(selector_failures=0)
        ok = playwright_retry.retry_click(page, "#btn", max_retries=2, timeout=9000, pre_wait_timeout=4000)
        assert ok is True
        assert page.selector_calls[0][1] == 4000
        assert page.click_calls[0][1] == 9000


class TestConstantsExtended:
    def test_resp_timeout_positive(self):
        assert playwright_retry.RESP_TIMEOUT > 0

    def test_short_timeout_positive(self):
        assert playwright_retry.SHORT_TIMEOUT > 0

    def test_long_timeout_gte_nav(self):
        assert playwright_retry.LONG_TIMEOUT >= playwright_retry.NAV_TIMEOUT

    def test_click_timeout_eq_selector(self):
        assert playwright_retry.CLICK_TIMEOUT == playwright_retry.SEL_TIMEOUT

    def test_short_less_than_nav(self):
        assert playwright_retry.SHORT_TIMEOUT < playwright_retry.NAV_TIMEOUT
