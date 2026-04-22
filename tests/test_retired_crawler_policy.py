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


class _FakePagingButton:
    def __init__(self, text=None, on_click=None):
        self.text = text
        self.on_click = on_click
        self.clicks = 0

    async def inner_text(self):
        return self.text

    async def is_visible(self):
        return True

    async def click(self):
        self.clicks += 1
        if self.on_click:
            self.on_click()


class _FakeRecordPage:
    def __init__(self):
        self.page_index = 0
        self.goto_calls = []
        self.selector_waits = []
        self.select_options = []
        self.load_states = []
        self.timeouts = []
        self.query_selectors = []
        self.change_dispatches = []

    async def goto(self, url, wait_until, timeout):
        self.goto_calls.append((url, wait_until, timeout))

    async def wait_for_selector(self, selector, timeout):
        self.selector_waits.append((selector, timeout))

    async def select_option(self, selector, value):
        self.select_options.append((selector, value))

    async def wait_for_load_state(self, state, timeout=None):
        self.load_states.append((state, timeout))

    async def wait_for_timeout(self, timeout):
        self.timeouts.append(timeout)

    async def evaluate(self, _script, selector=None):
        if selector is not None:
            self.change_dispatches.append(selector)
            return True
        if self.page_index == 0:
            return ["10001", "10002"]
        return ["20001"]

    async def query_selector(self, selector):
        self.query_selectors.append(selector)
        if selector == "div.paging span.on, div.paging a.on":
            return _FakePagingButton(str(self.page_index + 1))
        if selector == "div.paging a:has-text('2')" and self.page_index == 0:
            return _FakePagingButton(on_click=self._advance_page)
        return None

    def _advance_page(self):
        self.page_index += 1


class _FakeNextOnlyRecordPage(_FakeRecordPage):
    async def query_selector(self, selector):
        self.query_selectors.append(selector)
        if selector == "div.paging span.on, div.paging a.on":
            return _FakePagingButton(str(self.page_index + 1))
        if selector == "div.paging a:has-text('다음')" and self.page_index == 0:
            return _FakePagingButton(on_click=self._advance_page)
        return None


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


def test_retired_listing_uses_flexible_selectors_and_paginates(monkeypatch):
    page = _FakeRecordPage()
    compliance = _FakeCompliance(allowed=True)
    throttle = _FakeThrottle()

    monkeypatch.setattr(retired_listing, "compliance", compliance)
    monkeypatch.setattr(retired_listing, "throttle", throttle)

    crawler = RetiredPlayerListingCrawler(request_delay=1.0)
    ids = asyncio.run(
        crawler._crawl_record_page_ids(
            page,
            "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx",
            2024,
        )
    )

    assert ids == {"10001", "10002", "20001"}
    assert page.goto_calls == [
        (
            "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx",
            "load",
            30000,
        )
    ]
    assert page.selector_waits == [
        ('select[id$="ddlSeason_ddlSeason"], select[name*="ddlSeason"]', 15000)
    ]
    assert page.select_options == [
        ('select[id$="ddlSeason_ddlSeason"], select[name*="ddlSeason"]', "2024"),
        ('select[id$="ddlSeries_ddlSeries"], select[name*="ddlSeries"]', "0"),
    ]
    assert page.change_dispatches == [
        'select[id$="ddlSeason_ddlSeason"], select[name*="ddlSeason"]',
        'select[id$="ddlSeries_ddlSeries"], select[name*="ddlSeries"]',
    ]
    assert page.load_states == [
        ("load", 10000),
        ("load", 10000),
        ("load", 10000),
    ]
    assert page.timeouts == [1000, 500, 1000]
    assert "div.paging span.on, div.paging a.on" in page.query_selectors
    assert "div.paging a:has-text('2')" in page.query_selectors
    assert throttle.calls == 2


def test_retired_listing_uses_next_button_when_numeric_page_is_absent(monkeypatch):
    page = _FakeNextOnlyRecordPage()
    compliance = _FakeCompliance(allowed=True)
    throttle = _FakeThrottle()

    monkeypatch.setattr(retired_listing, "compliance", compliance)
    monkeypatch.setattr(retired_listing, "throttle", throttle)

    crawler = RetiredPlayerListingCrawler(request_delay=1.0)
    ids = asyncio.run(
        crawler._crawl_record_page_ids(
            page,
            "https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx",
            2024,
        )
    )

    assert ids == {"10001", "10002", "20001"}
    assert "div.paging a:has-text('2')" in page.query_selectors
    assert "div.paging a[id$='btnNext']" in page.query_selectors
    assert "div.paging a:has(img[alt='다음'])" in page.query_selectors
    assert "div.paging a:has-text('다음')" in page.query_selectors
    assert throttle.calls == 2


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


def test_collect_historical_player_ids_unions_years_and_skips_failures():
    calls = []

    class FakeCrawler(RetiredPlayerListingCrawler):
        async def collect_player_ids_for_year(self, season_year):
            calls.append(season_year)
            if season_year == 2021:
                raise RuntimeError("temporary listing failure")
            return {str(season_year), "shared"}

    crawler = FakeCrawler()
    ids = asyncio.run(crawler.collect_historical_player_ids([2020, 2021, 2022]))

    assert ids == {"2020", "2022", "shared"}
    assert sorted(calls) == [2020, 2021, 2022]


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
