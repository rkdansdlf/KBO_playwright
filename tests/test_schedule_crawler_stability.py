from __future__ import annotations

import asyncio

from src.crawlers import schedule_crawler as schedule_module
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.utils.game_status import GAME_STATUS_COMPLETED


class _FakeCompliance:
    def __init__(self):
        self.urls = []

    async def is_allowed(self, url: str) -> bool:
        self.urls.append(url)
        return True


class _FakePolicy:
    def __init__(self):
        self.delay_hosts = []
        self.retry_calls = 0

    async def delay_async(self, *, host: str = "koreabaseball.com") -> None:
        self.delay_hosts.append(host)

    async def run_with_retry_async(self, func, *args, **kwargs):
        self.retry_calls += 1
        return await func(*args, **kwargs)


class _FakeSchedulePage:
    def __init__(self, raw_games=None):
        self.url = "about:blank"
        self.raw_games = raw_games or []
        self.goto_calls = []
        self.selector_calls = []
        self.selected = {"#ddlYear": "2025", "#ddlMonth": "04", "#ddlSeries": "0"}
        self.select_calls = []

    async def goto(self, url: str, **kwargs):
        self.url = url
        self.goto_calls.append((url, kwargs))

    async def wait_for_selector(self, selector: str, **kwargs):
        self.selector_calls.append((selector, kwargs))

    async def eval_on_selector_all(self, selector: str, _script: str):
        assert selector == "#ddlSeries option"
        return [{"text": "정규시즌", "value": "0"}]

    async def eval_on_selector(self, selector: str, _script: str):
        return self.selected[selector]

    async def select_option(self, selector: str, value: str):
        self.selected[selector] = value
        self.select_calls.append((selector, value))

    async def wait_for_load_state(self, *_args, **_kwargs):
        return None

    async def wait_for_timeout(self, *_args, **_kwargs):
        return None

    async def evaluate(self, _script: str, arg=None, _extra=None):
        if arg is None:
            return []
        return self.raw_games

    async def content(self):
        return "<html><table class='tbl'><tbody></tbody></table></html>"


def test_navigate_schedule_page_uses_compliance_delay_retry_and_selector(monkeypatch):
    fake_compliance = _FakeCompliance()
    policy = _FakePolicy()
    page = _FakeSchedulePage()
    crawler = ScheduleCrawler(policy=policy)

    monkeypatch.setattr(schedule_module, "compliance", fake_compliance)

    ok, reason = asyncio.run(crawler._navigate_schedule_page(page))

    assert ok is True
    assert reason == "ok"
    assert fake_compliance.urls == [crawler.base_url]
    assert policy.retry_calls == 1
    assert policy.delay_hosts == ["www.koreabaseball.com"]
    assert page.goto_calls[0][0] == crawler.base_url
    assert page.selector_calls[0][0] == "#ddlYear, #ddlMonth, #ddlSeries, .tbl"


def test_extract_games_filters_invalid_rows_and_normalizes_status():
    page = _FakeSchedulePage(
        raw_games=[
            {
                "game_id": "20250401LGSS0",
                "game_date": "20250401",
                "season_year": 2025,
                "season_type": "regular",
                "away_segment": "LG",
                "home_segment": "SS",
                "doubleheader_no": 0,
                "game_status": "경기종료",
                "crawl_status": "link_parsed",
                "game_time": "18:30",
                "stadium": "잠실",
                "url_suffix": "/Schedule/GameCenter/Main.aspx?gameId=20250401LGSS0",
            },
            {
                "game_id": "20250402LGSS0",
                "game_date": "20250401",
                "season_year": 2025,
                "season_type": "regular",
                "away_segment": "LG",
                "home_segment": "SS",
                "doubleheader_no": 0,
                "game_status": "SCHEDULED",
                "crawl_status": "link_parsed",
                "game_time": "18:30",
                "stadium": "잠실",
                "url_suffix": "",
            },
        ],
    )
    crawler = ScheduleCrawler(policy=_FakePolicy())

    games = asyncio.run(crawler._extract_games(page, 2025, 4))

    assert len(games) == 1
    assert games[0]["game_id"] == "20250401LGSS0"
    assert games[0]["game_status"] == GAME_STATUS_COMPLETED
    assert games[0]["stadium"] == "잠실"


def test_crawl_month_records_schedule_empty_reason(monkeypatch):
    fake_compliance = _FakeCompliance()
    page = _FakeSchedulePage(raw_games=[])
    crawler = ScheduleCrawler(policy=_FakePolicy())

    monkeypatch.setattr(schedule_module, "compliance", fake_compliance)

    games = asyncio.run(crawler._crawl_month(page, 2025, 4))

    assert games == []
    assert crawler.get_last_failure_reason("2025-04:all") == "schedule_empty"
