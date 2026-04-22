from playwright.sync_api import TimeoutError as PlaywrightTimeout

from src.utils import playwright_retry


class _NavigationPage:
    def __init__(self, goto_failures: int = 0, load_failures: int = 0):
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
    def __init__(self, failures: int):
        self.failures = failures
        self.selector_calls = []
        self.reload_calls = []

    def wait_for_selector(self, selector, timeout=None, state=None):
        self.selector_calls.append((selector, timeout, state))
        if len(self.selector_calls) <= self.failures:
            raise PlaywrightTimeout("selector timeout")

    def reload(self, wait_until=None, timeout=None):
        self.reload_calls.append((wait_until, timeout))


def test_retry_navigation_retries_then_succeeds(monkeypatch):
    sleeps = []
    page = _NavigationPage(goto_failures=1)
    monkeypatch.setattr(playwright_retry.time, "sleep", lambda delay: sleeps.append(delay))

    ok = playwright_retry.retry_navigation(
        page,
        "https://example.test",
        max_retries=3,
        timeout=123,
        wait_until="domcontentloaded",
    )

    assert ok is True
    assert page.goto_calls == [
        ("https://example.test", "domcontentloaded", 123),
        ("https://example.test", "domcontentloaded", 123),
    ]
    assert page.load_calls == [("networkidle", 123)]
    assert sleeps == [2]


def test_retry_navigation_returns_false_after_final_failure(monkeypatch):
    sleeps = []
    page = _NavigationPage(goto_failures=3)
    monkeypatch.setattr(playwright_retry.time, "sleep", lambda delay: sleeps.append(delay))

    ok = playwright_retry.retry_navigation(page, "https://example.test", max_retries=3)

    assert ok is False
    assert len(page.goto_calls) == 3
    assert sleeps == [2, 4]


def test_retry_wait_for_selector_reloads_between_timeouts(monkeypatch):
    sleeps = []
    page = _SelectorPage(failures=1)
    monkeypatch.setattr(playwright_retry.time, "sleep", lambda delay: sleeps.append(delay))

    ok = playwright_retry.retry_wait_for_selector(
        page,
        "table.stats",
        max_retries=2,
        timeout=456,
        state="attached",
    )

    assert ok is True
    assert page.selector_calls == [
        ("table.stats", 456, "attached"),
        ("table.stats", 456, "attached"),
    ]
    assert page.reload_calls == [("networkidle", 456)]
    assert sleeps == [2]


def test_retry_wait_for_selector_returns_false_without_extra_reload(monkeypatch):
    sleeps = []
    page = _SelectorPage(failures=2)
    monkeypatch.setattr(playwright_retry.time, "sleep", lambda delay: sleeps.append(delay))

    ok = playwright_retry.retry_wait_for_selector(page, "table.stats", max_retries=2)

    assert ok is False
    assert len(page.selector_calls) == 2
    assert len(page.reload_calls) == 1
    assert sleeps == [2]
