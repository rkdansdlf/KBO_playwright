from __future__ import annotations

import argparse
import asyncio

import src.cli.collect_games as cli
import src.services.player_id_resolver as pid_module


def test_collect_uses_player_id_resolver_and_closes_session(monkeypatch):
    calls: dict[str, object] = {}

    class FakeSession:
        def __init__(self):
            pass

        def close(self):
            calls["session_closed"] = True

    class FakeResolver:
        def __init__(self, session, **_kwargs):
            calls["resolver_session"] = session

        def preload_season_index(self, year: int):
            calls["preload_year"] = year

    class FakeDetailCrawler:
        def __init__(self, *, request_delay: float, resolver):
            calls["detail_delay"] = request_delay
            calls["detail_resolver"] = resolver

    async def fake_crawl_and_save_game_details(targets, *, detail_crawler, **kwargs):
        calls["targets"] = targets
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

    def fake_load_targets(*, year, month, game_ids):
        calls["load_year"] = year
        calls["load_month"] = month
        calls["load_game_ids"] = game_ids
        return [argparse.Namespace(game_id="20260426KTSK0", game_date="20260426")]

    monkeypatch.setattr(cli, "SessionLocal", FakeSession)
    monkeypatch.setattr(
        cli, "load_game_targets_from_db", lambda y, m: fake_load_targets(year=y, month=m, game_ids=None)
    )
    monkeypatch.setattr(
        cli, "load_game_targets_by_ids", lambda gids: fake_load_targets(year=None, month=None, game_ids=gids)
    )
    monkeypatch.setattr(pid_module, "PlayerIdResolver", FakeResolver)
    monkeypatch.setattr(cli, "GameDetailCrawler", FakeDetailCrawler)
    monkeypatch.setattr(cli, "crawl_and_save_game_details", fake_crawl_and_save_game_details)

    asyncio.run(cli.collect_games(year=2026, month=4, force=True, concurrency=1))

    assert calls["load_year"] == 2026
    assert calls["load_month"] == 4
    assert calls["preload_year"] == 2026
    assert calls["detail_resolver"].__class__ is FakeResolver
    assert calls["detail_delay"] == 1.0
    assert calls["force"] is True
    assert calls["concurrency"] == 1
    assert calls["session_closed"] is True
