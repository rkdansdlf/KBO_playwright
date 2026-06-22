import asyncio

import src.crawlers.player_search_crawler as module
from src.crawlers.player_search_crawler import PlayerRow, PlayerSearchCrawler


class _FakeCompliance:
    def __init__(self, allowed=True):
        self.allowed = allowed
        self.urls = []

    async def is_allowed(self, url):
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
    def __init__(self, rows=None):
        self.rows = rows or []
        self.goto_calls = []
        self.selector_calls = []

    async def goto(self, url, wait_until=None, timeout=None):
        self.goto_calls.append((url, wait_until, timeout))

    async def wait_for_selector(self, selector, timeout=None):
        self.selector_calls.append((selector, timeout))

    async def evaluate(self, *_args):
        return self.rows


def test_navigate_search_page_uses_compliance_delay_retry_and_selector(monkeypatch):
    compliance = _FakeCompliance()
    monkeypatch.setattr(module, "compliance", compliance)

    crawler = PlayerSearchCrawler(request_delay=0)
    policy = _FakePolicy()
    crawler.policy = policy
    page = _FakePage()

    ok, reason = asyncio.run(
        crawler._navigate_search_page(
            page,
            required_selector=module.TABLE_ROWS,
            timeout=12345,
            selector_timeout=6789,
        )
    )

    assert ok is True
    assert reason == "ok"
    assert compliance.urls == [module.SEARCH_URL]
    assert policy.retry_calls == 1
    assert policy.delay_hosts == ["www.koreabaseball.com"]
    assert page.goto_calls == [(module.SEARCH_URL, "domcontentloaded", 12345)]
    assert page.selector_calls == [(module.TABLE_ROWS, 6789)]


def test_collect_page_rows_filters_invalid_player_payloads():
    rows = [
        {"cells": ["1"], "linkHref": "x?playerId=1001"},
        {"cells": ["1", "정상", "LG", "투수", "2000.01.01", "180cm/80kg", "고교"], "linkHref": None},
        {"cells": ["1", "", "LG", "투수", "2000.01.01", "180cm/80kg", "고교"], "linkHref": "x?playerId=1002"},
        {
            "cells": ["1", "Unknown Player", "LG", "투수", "2000.01.01", "180cm/80kg", "고교"],
            "linkHref": "x?playerId=1003",
        },
        {"cells": ["1", "홍길동", "LG", "투수", "2000.01.01", "180cm/80kg", "고교"], "linkHref": "x?playerId=1004"},
    ]
    crawler = PlayerSearchCrawler(request_delay=0)

    parsed = asyncio.run(crawler._collect_page_rows(_FakePage(rows)))

    assert parsed == [
        PlayerRow(
            player_id=1004,
            uniform_no="1",
            name="홍길동",
            team="LG",
            position="투수",
            birth_date="2000.01.01",
            height_cm=180,
            weight_kg=80,
            career="고교",
        )
    ]
    assert crawler.get_failure_summary() == {
        "insufficient_columns": 1,
        "invalid_player_id": 1,
        "missing_player_name": 1,
        "unknown_player_name": 1,
    }


def test_collect_page_rows_keeps_players_with_unparseable_height_weight():
    rows = [
        {"cells": ["1", "홍길동", "LG", "투수", "2000.01.01", "미상", "고교"], "linkHref": "x?playerId=1004"},
    ]
    crawler = PlayerSearchCrawler(request_delay=0)

    parsed = asyncio.run(crawler._collect_page_rows(_FakePage(rows)))

    assert parsed == [
        PlayerRow(
            player_id=1004,
            uniform_no="1",
            name="홍길동",
            team="LG",
            position="투수",
            birth_date="2000.01.01",
            height_cm=None,
            weight_kg=None,
            career="고교",
        )
    ]


def test_merge_rows_tracks_duplicate_player_ids(monkeypatch):
    crawler = PlayerSearchCrawler(request_delay=0)
    row = PlayerRow(1001, None, "홍길동", "LG", "투수", None, None, None, None)

    async def fake_paginate(_page):
        return [row, row]

    monkeypatch.setattr(crawler, "_paginate_current_tab", fake_paginate)
    all_rows = []
    seen_ids = set()

    done = asyncio.run(crawler._merge_rows(object(), all_rows, seen_ids, limit=None))

    assert done is False
    assert all_rows == [row]
    assert crawler.get_failure_summary() == {"duplicate_player_id": 1}
