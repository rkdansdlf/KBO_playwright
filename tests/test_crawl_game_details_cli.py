from __future__ import annotations

import argparse
import asyncio

import src.cli.crawl_game_details as cli


def test_run_pipeline_injects_player_id_resolver(monkeypatch):
    calls: dict[str, object] = {}

    class FakeScheduleCrawler:
        async def crawl_schedule(self, year: int, month: int):
            calls["schedule"] = (year, month)
            return [{"game_id": "20260426KTSK0", "game_date": "20260426"}]

    class FakeSession:
        def close(self):
            calls["session_closed"] = True

    class FakeResolver:
        def __init__(self, session):
            calls["resolver_session"] = session

        def preload_season_index(self, year: int):
            calls["preload_year"] = year

    class FakeDetailCrawler:
        def __init__(self, *, request_delay: float, resolver):
            calls["detail_delay"] = request_delay
            calls["detail_resolver"] = resolver

    async def fake_crawl_and_save_game_details(games, *, detail_crawler, **kwargs):
        calls["games"] = games
        calls["detail_crawler"] = detail_crawler
        calls["force"] = kwargs["force"]
        calls["concurrency"] = kwargs["concurrency"]

        return argparse.Namespace(
            detail_saved=1,
            detail_targets=1,
            detail_failed=0,
            detail_skipped_existing=0,
            relay_saved_games=0,
            relay_rows_saved=0,
            relay_skipped_existing=0,
        )

    monkeypatch.setattr(cli, "ScheduleCrawler", FakeScheduleCrawler)
    monkeypatch.setattr(cli, "SessionLocal", lambda: FakeSession())
    monkeypatch.setattr(cli, "PlayerIdResolver", FakeResolver)
    monkeypatch.setattr(cli, "GameDetailCrawler", FakeDetailCrawler)
    monkeypatch.setattr(cli, "crawl_and_save_game_details", fake_crawl_and_save_game_details)

    asyncio.run(
        cli.run_pipeline(
            argparse.Namespace(
                year=2026,
                month=4,
                limit=None,
                delay=1.5,
                relay=False,
                force=True,
                concurrency=1,
            )
        )
    )

    assert calls["schedule"] == (2026, 4)
    assert calls["preload_year"] == 2026
    assert calls["detail_resolver"].__class__ is FakeResolver
    assert calls["detail_delay"] == 1.5
    assert calls["force"] is True
    assert calls["concurrency"] == 1
    assert calls["session_closed"] is True
