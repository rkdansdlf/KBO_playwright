import asyncio

from src.crawlers.retire import detail as retired_detail
from src.crawlers.retire import listing as retired_listing
from src.crawlers.retire.detail import RetiredPlayerDetailCrawler
from src.crawlers.retire.listing import RetiredPlayerListingCrawler


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


class _NoNavigationPage:
    def __init__(self):
        self.goto_called = False

    async def goto(self, *_args, **_kwargs):
        self.goto_called = True
        raise AssertionError("navigation should be blocked by compliance")


def test_retired_listing_blocks_navigation_when_compliance_disallows(monkeypatch):
    page = _NoNavigationPage()
    compliance = _FakeCompliance(allowed=False)
    throttle = _FakeThrottle()

    monkeypatch.setattr(retired_listing, "compliance", compliance)
    monkeypatch.setattr(retired_listing, "throttle", throttle)

    crawler = RetiredPlayerListingCrawler(request_delay=1.5)
    ids = asyncio.run(
        crawler._crawl_record_page_ids(
            page,
            "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx",
            2024,
        )
    )

    assert ids == set()
    assert compliance.urls == [
        "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
    ]
    assert page.goto_called is False
    assert throttle.calls == 0


def test_retired_detail_blocks_navigation_when_compliance_disallows(monkeypatch):
    page = _NoNavigationPage()
    compliance = _FakeCompliance(allowed=False)
    throttle = _FakeThrottle()

    monkeypatch.setattr(retired_detail, "compliance", compliance)
    monkeypatch.setattr(retired_detail, "throttle", throttle)

    crawler = RetiredPlayerDetailCrawler(request_delay=1.5)
    payload = asyncio.run(
        crawler._fetch_page(
            page,
            "https://www.koreabaseball.com/Record/Retire/Hitter.aspx",
            "12345",
        )
    )

    assert payload is None
    assert compliance.urls == [
        "https://www.koreabaseball.com/Record/Retire/Hitter.aspx?playerId=12345"
    ]
    assert page.goto_called is False
    assert throttle.calls == 0


def test_retired_crawlers_preserve_request_delay(monkeypatch):
    listing_throttle = _FakeThrottle()
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(retired_listing, "throttle", listing_throttle)
    monkeypatch.setattr(retired_listing.asyncio, "sleep", fake_sleep)

    listing = RetiredPlayerListingCrawler(request_delay=1.5)
    asyncio.run(listing._wait())

    assert listing_throttle.calls == 1
    assert sleeps == [0.5]

    detail_throttle = _FakeThrottle()
    monkeypatch.setattr(retired_detail, "throttle", detail_throttle)
    monkeypatch.setattr(retired_detail.asyncio, "sleep", fake_sleep)

    detail = RetiredPlayerDetailCrawler(request_delay=2.0)
    asyncio.run(detail._wait())

    assert detail_throttle.calls == 1
    assert sleeps == [0.5, 1.0]


def test_determine_inactive_player_ids_diffs_historical_and_active(monkeypatch):
    calls = []

    class FakeCrawler(RetiredPlayerListingCrawler):
        async def collect_historical_player_ids(self, seasons):
            calls.append(("historical", list(seasons)))
            return {"1", "2", "3"}

        async def collect_player_ids_for_year(self, season_year):
            calls.append(("active", season_year))
            return {"2"}

    crawler = FakeCrawler()
    inactive = asyncio.run(
        crawler.determine_inactive_player_ids(
            start_year=2020,
            end_year=2021,
            active_year=2024,
        )
    )

    assert inactive == {"1", "3"}
    assert calls == [
        ("historical", [2020, 2021]),
        ("active", 2024),
    ]
