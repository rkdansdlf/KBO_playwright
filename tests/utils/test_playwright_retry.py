from unittest.mock import MagicMock

from playwright.sync_api import TimeoutError as PlaywrightTimeout

from src.utils import playwright_retry


class _NavigationPage:
    def __init__(self, goto_failures=0, load_failures=0):
        self.goto_failures = goto_failures
        self.load_failures = load_failures
        self.goto_calls = []
        self.load_calls = []

    def goto(self, url, wait_until=None, timeout=None):
        self.goto_calls.append((url, wait_until, timeout))
        if len(self.goto_calls) <= self.goto_failures:
            raise PlaywrightTimeout("goto timeout")

    def wait_for_load_state(self, state, timeout=None):
        self.load_calls.append((state, timeout))
        if len(self.load_calls) <= self.load_failures:
            raise PlaywrightTimeout("load timeout")


class _SelectorPage:
    def __init__(self, failures=0):
        self.failures = failures
        self.selector_calls = []
        self.click_calls = []
        self.reload_calls = []

    def wait_for_selector(self, selector, timeout=None, state=None):
        self.selector_calls.append((selector, timeout, state))
        if len(self.selector_calls) <= self.failures:
            raise PlaywrightTimeout("selector timeout")

    def click(self, selector, timeout=None):
        self.click_calls.append((selector, timeout))

    def reload(self, wait_until=None, timeout=None):
        self.reload_calls.append((wait_until, timeout))


class TestRetryNavigation:
    def test_success_first_attempt(self, monkeypatch):
        delays = []
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: delays.append(None))
        page = _NavigationPage(goto_failures=0)
        ok = playwright_retry.retry_navigation(page, "https://example.test", max_retries=3)
        assert ok is True
        assert len(delays) == 0

    def test_retry_then_succeed(self, monkeypatch):
        delays = []
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: delays.append(None))
        page = _NavigationPage(goto_failures=1)
        ok = playwright_retry.retry_navigation(page, "https://example.test", max_retries=3)
        assert ok is True
        assert len(delays) == 1

    def test_all_attempts_fail(self, monkeypatch):
        delays = []
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: delays.append(None))
        page = _NavigationPage(goto_failures=5)
        ok = playwright_retry.retry_navigation(page, "https://example.test", max_retries=2)
        assert ok is False
        assert len(delays) == 2


class TestRetryClick:
    def test_success_first_attempt(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _SelectorPage(failures=0)
        ok = playwright_retry.retry_click(page, "#btn", max_retries=3)
        assert ok is True
        assert len(page.click_calls) == 1

    def test_retry_then_succeed(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _SelectorPage(failures=1)
        ok = playwright_retry.retry_click(page, "#btn", max_retries=3)
        assert ok is True
        assert len(page.click_calls) == 2

    def test_all_attempts_fail(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _SelectorPage(failures=5)
        ok = playwright_retry.retry_click(page, "#btn", max_retries=2)
        assert ok is False


class TestRetryWaitForSelector:
    def test_success_first_attempt(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _SelectorPage(failures=0)
        ok = playwright_retry.retry_wait_for_selector(page, "#el", max_retries=3)
        assert ok is True

    def test_retry_then_succeed(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _SelectorPage(failures=1)
        ok = playwright_retry.retry_wait_for_selector(page, "#el", max_retries=3)
        assert ok is True

    def test_all_attempts_fail(self, monkeypatch):
        monkeypatch.setattr(playwright_retry._policy, "delay", lambda: None)
        page = _SelectorPage(failures=5)
        ok = playwright_retry.retry_wait_for_selector(page, "#el", max_retries=2)
        assert ok is False


class TestConstants:
    def test_timeout_defaults(self):
        assert playwright_retry.SEL_TIMEOUT > 0
        assert playwright_retry.NAV_TIMEOUT > 0
        assert playwright_retry.CLICK_TIMEOUT > 0
        assert playwright_retry.LONG_TIMEOUT >= playwright_retry.NAV_TIMEOUT
