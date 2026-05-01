from __future__ import annotations

import asyncio
from datetime import date
from types import SimpleNamespace

import src.cli.auto_healer as auto_healer
from src.repositories.game_repository import GAME_STATUS_CANCELLED, GAME_STATUS_UNRESOLVED


class _FakeSession:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeResolver:
    created = []

    def __init__(self, session):
        self.session = session
        self.preloaded_years = []
        _FakeResolver.created.append(self)

    def preload_season_index(self, year: int):
        self.preloaded_years.append(year)


class _FakeDetailCrawler:
    created = []

    def __init__(self, request_delay, resolver):
        self.request_delay = request_delay
        self.resolver = resolver
        _FakeDetailCrawler.created.append(self)


def test_run_healer_async_uses_shared_collection_and_applies_statuses(monkeypatch):
    _FakeResolver.created = []
    _FakeDetailCrawler.created = []
    updates = []
    alerts = []
    seen = {}
    stuck_games = [
        SimpleNamespace(game_id="20250101LGSS0", game_date=date(2025, 1, 1)),
        SimpleNamespace(game_id="20250102LGSS0", game_date=date(2025, 1, 2)),
        SimpleNamespace(game_id="20250103LGSS0", game_date=date(2025, 1, 3)),
    ]

    monkeypatch.setattr(auto_healer, "_find_stuck_games", lambda: stuck_games)
    monkeypatch.setattr(auto_healer, "SessionLocal", lambda: _FakeSession())
    monkeypatch.setattr(auto_healer, "PlayerIdResolver", _FakeResolver)
    monkeypatch.setattr(auto_healer, "GameDetailCrawler", _FakeDetailCrawler)
    monkeypatch.setattr(auto_healer, "update_game_status", lambda game_id, status: updates.append((game_id, status)) or True)
    monkeypatch.setattr(auto_healer.SlackWebhookClient, "send_alert", lambda *args, **kwargs: alerts.append((args, kwargs)))

    async def _fake_collect(games, *, detail_crawler, force, concurrency, log, **_kwargs):
        seen["games"] = list(games)
        seen["crawler"] = detail_crawler
        seen["force"] = force
        seen["concurrency"] = concurrency
        return SimpleNamespace(
            items={
                "20250101LGSS0": SimpleNamespace(detail_saved=True, failure_reason=None),
                "20250102LGSS0": SimpleNamespace(detail_saved=False, failure_reason="cancelled"),
                "20250103LGSS0": SimpleNamespace(detail_saved=False, failure_reason="missing"),
            }
        )

    monkeypatch.setattr(auto_healer, "crawl_and_save_game_details", _fake_collect)

    rc = asyncio.run(auto_healer.run_healer_async(dry_run=False))

    assert rc == 1
    assert _FakeResolver.created[0].preloaded_years == [2025]
    assert _FakeDetailCrawler.created[0].resolver is _FakeResolver.created[0]
    assert _FakeDetailCrawler.created[0].request_delay == 1.0
    assert seen["games"] == [
        {"game_id": "20250101LGSS0", "game_date": "20250101"},
        {"game_id": "20250102LGSS0", "game_date": "20250102"},
        {"game_id": "20250103LGSS0", "game_date": "20250103"},
    ]
    assert seen["crawler"] is _FakeDetailCrawler.created[0]
    assert seen["force"] is True
    assert seen["concurrency"] == 1
    assert ("20250102LGSS0", GAME_STATUS_CANCELLED) in updates
    assert ("20250103LGSS0", GAME_STATUS_UNRESOLVED) in updates
    assert len(alerts) == 2


def test_run_healer_async_dry_run_skips_collection_and_updates(monkeypatch):
    collect_called = False
    updates = []
    stuck_games = [SimpleNamespace(game_id="20250101LGSS0", game_date=date(2025, 1, 1))]

    monkeypatch.setattr(auto_healer, "_find_stuck_games", lambda: stuck_games)
    monkeypatch.setattr(auto_healer, "SessionLocal", lambda: _FakeSession())
    monkeypatch.setattr(auto_healer, "PlayerIdResolver", _FakeResolver)
    monkeypatch.setattr(auto_healer, "GameDetailCrawler", _FakeDetailCrawler)
    monkeypatch.setattr(auto_healer, "update_game_status", lambda game_id, status: updates.append((game_id, status)) or True)
    monkeypatch.setattr(auto_healer.SlackWebhookClient, "send_alert", lambda *args, **kwargs: None)

    async def _fake_collect(*args, **kwargs):
        nonlocal collect_called
        collect_called = True

    monkeypatch.setattr(auto_healer, "crawl_and_save_game_details", _fake_collect)

    rc = asyncio.run(auto_healer.run_healer_async(dry_run=True))

    assert rc == 0
    assert collect_called is False
    assert updates == []
