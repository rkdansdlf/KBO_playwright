from __future__ import annotations

import asyncio

import src.crawlers.game_detail_crawler as game_detail_module
from src.crawlers.game_detail_crawler import GameDetailCrawler


class _FakeCompliance:
    def __init__(self, allowed: bool = True):
        self.allowed = allowed
        self.urls = []

    async def is_allowed(self, url: str):
        self.urls.append(url)
        return self.allowed


class _FakePolicy:
    def __init__(self):
        self.delay_hosts = []
        self.retry_calls = 0

    async def delay_async(self, host="koreabaseball.com"):
        self.delay_hosts.append(host)

    async def run_with_retry_async(self, func, *args, **kwargs):
        self.retry_calls += 1
        return await func(*args, **kwargs)


class _FakePage:
    def __init__(self):
        self.url = "about:blank"
        self.goto_calls = []
        self.selector_calls = []
        self.evaluate_calls = []

    async def goto(self, url, wait_until=None, timeout=None):
        self.url = url
        self.goto_calls.append((url, wait_until, timeout))

    async def wait_for_selector(self, selector, timeout=None):
        self.selector_calls.append((selector, timeout))

    async def evaluate(self, script, *args):
        self.evaluate_calls.append((script, args))
        if "li.tab-tit.on" in script:
            return "HITTER"
        return None


def _team_info():
    return {
        "away": {"code": "LG", "score": 2, "line_score": [1, 1], "hits": 3, "errors": 0},
        "home": {"code": "SS", "score": 1, "line_score": [0, 1], "hits": 2, "errors": 0},
    }


def _hitter(side: str):
    return {
        "player_id": 1001 if side == "away" else 2001,
        "player_name": f"{side} hitter",
        "team_side": side,
        "stats": {"hits": 1, "at_bats": 3},
    }


def _pitcher(side: str):
    return {
        "player_id": 3001 if side == "away" else 4001,
        "player_name": f"{side} pitcher",
        "team_side": side,
        "is_starting": True,
        "stats": {"innings_outs": 18},
    }


def test_navigate_section_uses_compliance_delay_retry_and_selector(monkeypatch):
    compliance = _FakeCompliance()
    monkeypatch.setattr(game_detail_module, "compliance", compliance)

    crawler = GameDetailCrawler()
    policy = _FakePolicy()
    crawler.policy = policy
    page = _FakePage()

    ok, reason, url = asyncio.run(
        crawler._navigate_section(
            page,
            "20250401LGSS0",
            "20250401",
            "HITTER",
            required_selector="#tblAwayHitter",
            timeout=12345,
            selector_timeout=6789,
            extra_delay=0,
        )
    )

    assert ok is True
    assert reason == "ok"
    assert compliance.urls == [url]
    assert policy.retry_calls == 1
    assert policy.delay_hosts == ["www.koreabaseball.com"]
    assert page.goto_calls == [(url, "domcontentloaded", 12345)]
    assert page.selector_calls == [("#tblAwayHitter", 6789)]


def test_crawl_single_uses_review_fallback_when_direct_sections_are_empty(monkeypatch):
    crawler = GameDetailCrawler()
    sections = []

    async def fake_navigate(page, game_id, game_date, section, **kwargs):
        sections.append(section)
        return True, "ok", crawler._section_url(game_id, game_date, section)

    async def fake_wait(page, lightweight=False):
        return True, "ok"

    async def fake_roster(page, game_id, game_date, review_url):
        return {"away hitter": [{"id": "1001", "uniform": None}]}

    async def fake_team_info(*_args):
        return _team_info()

    async def fake_metadata(*_args):
        return {}

    async def fake_summary(*_args):
        return []

    async def fake_hitters(
        page, team_side, team_code, season_year, roster_map=None, use_hitter_section=False, db_session=None
    ):
        if not use_hitter_section:
            # First try on REVIEW page is mocked empty
            return [], {}
        # Fallback to dedicated section returns actual data
        return [_hitter(team_side)], {"hits": 1, "at_bats": 3}

    async def fake_pitchers(
        page, team_side, team_code, season_year, roster_map=None, use_pitcher_section=False, db_session=None
    ):
        if not use_pitcher_section:
            # First try on REVIEW page is mocked empty
            return []
        # Fallback to dedicated section returns actual data
        return [_pitcher(team_side)]

    monkeypatch.setattr(crawler, "_navigate_section", fake_navigate)
    monkeypatch.setattr(crawler, "_wait_for_boxscore", fake_wait)
    monkeypatch.setattr(crawler, "_load_roster_map_from_lineup", fake_roster)
    monkeypatch.setattr(crawler, "_extract_team_info", fake_team_info)
    monkeypatch.setattr(crawler, "_extract_metadata", fake_metadata)
    monkeypatch.setattr(crawler, "_extract_game_summary", fake_summary)
    monkeypatch.setattr(crawler, "_extract_hitters", fake_hitters)
    monkeypatch.setattr(crawler, "_extract_pitchers", fake_pitchers)

    payload = asyncio.run(crawler._crawl_single(object(), "20250401LGSS0", "20250401"))

    assert payload is not None
    assert sections == ["REVIEW", "HITTER", "PITCHER"]
    assert payload["hitters"]["away"] == [_hitter("away")]
    assert payload["hitters"]["home"] == [_hitter("home")]
    assert payload["pitchers"]["away"] == [_pitcher("away")]
    assert payload["pitchers"]["home"] == [_pitcher("home")]
    assert crawler.get_last_failure_reason("20250401LGSS0") is None


def test_crawl_single_marks_incomplete_detail_when_fallback_is_empty(monkeypatch):
    crawler = GameDetailCrawler()

    async def fake_navigate(page, game_id, game_date, section, **kwargs):
        return True, "ok", crawler._section_url(game_id, game_date, section)

    async def fake_wait(page, lightweight=False):
        return True, "ok"

    async def fake_roster(*_args):
        return {}

    async def fake_team_info(*_args):
        return {
            "away": {"code": "LG", "score": None, "line_score": [], "hits": None, "errors": None},
            "home": {"code": "SS", "score": None, "line_score": [], "hits": None, "errors": None},
        }

    async def fake_metadata(*_args):
        return {}

    async def fake_summary(*_args):
        return []

    async def empty_hitters(*_args, **_kwargs):
        return [], {}

    async def empty_pitchers(*_args, **_kwargs):
        return []

    monkeypatch.setattr(crawler, "_navigate_section", fake_navigate)
    monkeypatch.setattr(crawler, "_wait_for_boxscore", fake_wait)
    monkeypatch.setattr(crawler, "_load_roster_map_from_lineup", fake_roster)
    monkeypatch.setattr(crawler, "_extract_team_info", fake_team_info)
    monkeypatch.setattr(crawler, "_extract_metadata", fake_metadata)
    monkeypatch.setattr(crawler, "_extract_game_summary", fake_summary)
    monkeypatch.setattr(crawler, "_extract_hitters", empty_hitters)
    monkeypatch.setattr(crawler, "_extract_pitchers", empty_pitchers)

    payload = asyncio.run(crawler._crawl_single(object(), "20250402LGSS0", "20250402"))

    assert payload is None
    assert crawler.get_last_failure_reason("20250402LGSS0") == "incomplete_detail"
