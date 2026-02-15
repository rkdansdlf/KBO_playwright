from __future__ import annotations

import asyncio

import src.cli.run_daily_update as run_daily_update
from src.repositories.game_repository import GAME_STATUS_CANCELLED, GAME_STATUS_UNRESOLVED


class _FakeSession:
    def close(self):
        return None


class _FakeResolver:
    created = []

    def __init__(self, session):
        self.session = session
        self.preloaded_years = []
        _FakeResolver.created.append(self)

    def preload_season_index(self, year: int):
        self.preloaded_years.append(year)


class _FakeScheduleCrawler:
    async def crawl_schedule(self, year: int, month: int):
        game_date = f"{year}{month:02d}01"
        return [
            {
                "game_id": f"{game_date}LGSS0",
                "game_date": game_date,
                "home_team_code": "SS",
                "away_team_code": "LG",
                "season_year": year,
                "season_type": "regular",
            }
        ]


class _FakeDetailCrawlerCancelled:
    received_resolver = None

    def __init__(self, resolver=None, **_kwargs):
        _FakeDetailCrawlerCancelled.received_resolver = resolver

    async def crawl_game(self, game_id: str, game_date: str):
        return None

    def get_last_failure_reason(self, game_id: str):
        return "cancelled"


class _FakeDetailCrawlerMissing:
    def __init__(self, resolver=None, **_kwargs):
        self.resolver = resolver

    async def crawl_game(self, game_id: str, game_date: str):
        return None

    def get_last_failure_reason(self, game_id: str):
        return "missing"


async def _noop_to_thread(func, *args, **kwargs):
    return None


def test_run_update_injects_resolver_and_marks_cancelled(monkeypatch):
    _FakeResolver.created = []
    updates = []

    monkeypatch.setattr(run_daily_update, "ScheduleCrawler", _FakeScheduleCrawler)
    monkeypatch.setattr(run_daily_update, "SessionLocal", lambda: _FakeSession())
    monkeypatch.setattr(run_daily_update, "PlayerIdResolver", _FakeResolver)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerCancelled)
    monkeypatch.setattr(run_daily_update, "save_schedule_game", lambda _game: True)
    monkeypatch.setattr(run_daily_update, "save_game_detail", lambda _detail: True)
    monkeypatch.setattr(
        run_daily_update,
        "update_game_status",
        lambda game_id, status: updates.append((game_id, status)) or True,
    )
    monkeypatch.setattr(
        run_daily_update,
        "refresh_game_status_for_date",
        lambda *_args, **_kwargs: {"total": 1, "updated": 0, "status_counts": {}},
    )
    monkeypatch.setattr(run_daily_update.asyncio, "to_thread", _noop_to_thread)

    asyncio.run(run_daily_update.run_update("20250101", sync=False, headless=True, limit=None))

    assert _FakeResolver.created
    assert _FakeResolver.created[0].preloaded_years == [2025]
    assert _FakeDetailCrawlerCancelled.received_resolver is _FakeResolver.created[0]
    assert ("20250101LGSS0", GAME_STATUS_CANCELLED) in updates


def test_run_update_marks_unresolved_when_detail_missing_for_past_date(monkeypatch):
    _FakeResolver.created = []
    updates = []

    monkeypatch.setattr(run_daily_update, "ScheduleCrawler", _FakeScheduleCrawler)
    monkeypatch.setattr(run_daily_update, "SessionLocal", lambda: _FakeSession())
    monkeypatch.setattr(run_daily_update, "PlayerIdResolver", _FakeResolver)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerMissing)
    monkeypatch.setattr(run_daily_update, "save_schedule_game", lambda _game: True)
    monkeypatch.setattr(run_daily_update, "save_game_detail", lambda _detail: True)
    monkeypatch.setattr(
        run_daily_update,
        "update_game_status",
        lambda game_id, status: updates.append((game_id, status)) or True,
    )
    monkeypatch.setattr(
        run_daily_update,
        "refresh_game_status_for_date",
        lambda *_args, **_kwargs: {"total": 1, "updated": 0, "status_counts": {}},
    )
    monkeypatch.setattr(run_daily_update.asyncio, "to_thread", _noop_to_thread)

    asyncio.run(run_daily_update.run_update("20200101", sync=False, headless=True, limit=None))

    assert ("20200101LGSS0", GAME_STATUS_UNRESOLVED) in updates
