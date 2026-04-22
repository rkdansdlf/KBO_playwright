import asyncio

from src.crawlers.futures import futures_batting
from src.crawlers.futures.profile import FuturesProfileCrawler
from src.crawlers.futures import profile as futures_profile


class _FakeCompliance:
    def __init__(self, allowed: bool):
        self.allowed = allowed
        self.urls = []

    async def is_allowed(self, url: str):
        self.urls.append(url)
        return self.allowed


class _FakeThrottle:
    default_delay = 1.0

    def __init__(self):
        self.calls = 0

    async def wait(self):
        self.calls += 1


class _FakePool:
    def __init__(self, page):
        self.page = page
        self.started = False
        self.released = False
        self.closed = False

    async def start(self):
        self.started = True

    async def acquire(self):
        return self.page

    async def release(self, page):
        assert page is self.page
        self.released = True

    async def close(self):
        self.closed = True


class _NoNavigationPage:
    def __init__(self):
        self.goto_called = False

    async def goto(self, *_args, **_kwargs):
        self.goto_called = True
        raise AssertionError("navigation should be blocked by compliance")


def test_futures_batting_returns_empty_when_compliance_blocks(monkeypatch):
    page = _NoNavigationPage()
    pool = _FakePool(page)
    compliance = _FakeCompliance(allowed=False)
    throttle = _FakeThrottle()

    monkeypatch.setattr(futures_batting, "compliance", compliance)
    monkeypatch.setattr(futures_batting, "throttle", throttle)

    rows = asyncio.run(
        futures_batting.fetch_and_parse_futures_batting(
            "12345",
            "https://www.koreabaseball.com/Futures/Player/HitterTotal.aspx?playerId=12345",
            pool=pool,
        )
    )

    assert rows == []
    assert compliance.urls == [
        "https://www.koreabaseball.com/Futures/Player/HitterTotal.aspx?playerId=12345"
    ]
    assert page.goto_called is False
    assert throttle.calls == 0
    assert pool.started is True
    assert pool.released is True
    assert pool.closed is False


def test_futures_profile_returns_none_when_compliance_blocks(monkeypatch):
    page = _NoNavigationPage()
    compliance = _FakeCompliance(allowed=False)
    throttle = _FakeThrottle()

    monkeypatch.setattr(futures_profile, "compliance", compliance)
    monkeypatch.setattr(futures_profile, "throttle", throttle)

    crawler = FuturesProfileCrawler(request_delay=1.5)
    result = asyncio.run(
        crawler._scrape_profile(
            page,
            "https://www.koreabaseball.com/Futures/Player/HitterDetail.aspx",
            "12345",
        )
    )

    assert result is None
    assert compliance.urls == [
        "https://www.koreabaseball.com/Futures/Player/HitterDetail.aspx?playerId=12345"
    ]
    assert page.goto_called is False
    assert throttle.calls == 0


def test_futures_profile_wait_preserves_request_delay(monkeypatch):
    throttle = _FakeThrottle()
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(futures_profile, "throttle", throttle)
    monkeypatch.setattr(futures_profile.asyncio, "sleep", fake_sleep)

    crawler = FuturesProfileCrawler(request_delay=1.5)
    asyncio.run(crawler._wait())

    assert throttle.calls == 1
    assert sleeps == [0.5]
