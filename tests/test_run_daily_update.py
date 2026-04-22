from __future__ import annotations

import asyncio
from types import SimpleNamespace

import src.cli.run_daily_update as run_daily_update
from src.repositories.game_repository import GAME_STATUS_CANCELLED, GAME_STATUS_UNRESOLVED


class _FakeSession:
    def close(self):
        return None

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


class _FakeDetailCrawlerSuccess:
    def __init__(self, resolver=None, **_kwargs):
        self.resolver = resolver

    async def crawl_game(self, game_id: str, game_date: str):
        return {"game_id": game_id, "game_date": game_date}

    def get_last_failure_reason(self, game_id: str):
        return None


class _FakeMovementCrawler:
    async def crawl_years(self, start_year: int, end_year: int):
        return []


class _FakeRosterCrawler:
    async def crawl_date_range(self, start_date: str, end_date: str):
        return []


async def _noop_async(*_args, **_kwargs):
    return None


async def _noop_to_thread(func, *args, **kwargs):
    return None


async def _fake_crawl_and_save_game_details(
    games,
    *,
    detail_crawler,
    force,
    concurrency,
    log,
):
    items = {}
    processed_game_ids = []
    detail_saved = 0
    detail_failed = 0
    for game in games:
        game_id = game["game_id"]
        if isinstance(detail_crawler, _FakeDetailCrawlerSuccess):
            items[game_id] = SimpleNamespace(
                detail_saved=True,
                detail_status="saved",
                failure_reason=None,
            )
            processed_game_ids.append(game_id)
            detail_saved += 1
        else:
            reason = detail_crawler.get_last_failure_reason(game_id)
            items[game_id] = SimpleNamespace(
                detail_saved=False,
                detail_status="crawl_failed",
                failure_reason=reason,
            )
            detail_failed += 1

    return SimpleNamespace(
        items=items,
        processed_game_ids=processed_game_ids,
        detail_saved=detail_saved,
        detail_failed=detail_failed,
    )


def _fake_save_schedule_games(games, **_kwargs):
    game_list = list(games)
    return SimpleNamespace(games=game_list, discovered=len(game_list), saved=len(game_list), failed=0)


class _FakeSyncer:
    created = []

    def __init__(self, oci_url, sqlite_session):
        self.oci_url = oci_url
        self.sqlite_session = sqlite_session
        self.synced_games = []
        self.calls = []
        self.closed = False
        _FakeSyncer.created.append(self)

    def sync_specific_game(self, game_id: str):
        self.synced_games.append(game_id)

    def sync_standings(self, *, year: int):
        self.calls.append(("standings", year))

    def sync_matchups(self, *, year: int):
        self.calls.append(("matchups", year))

    def sync_stat_rankings(self, *, year: int):
        self.calls.append(("rankings", year))

    def sync_player_season_batting(self):
        self.calls.append(("season_batting", None))

    def sync_player_season_pitching(self):
        self.calls.append(("season_pitching", None))

    def sync_player_movements(self):
        self.calls.append(("player_movements", None))

    def sync_daily_rosters(self):
        self.calls.append(("daily_rosters", None))

    def sync_players(self):
        self.calls.append(("players", None))

    def close(self):
        self.closed = True


def _patch_common(monkeypatch):
    monkeypatch.setattr(run_daily_update, "ScheduleCrawler", _FakeScheduleCrawler)
    monkeypatch.setattr(run_daily_update, "SessionLocal", lambda: _FakeSession())
    monkeypatch.setattr(run_daily_update, "PlayerIdResolver", _FakeResolver)
    monkeypatch.setattr(run_daily_update, "save_schedule_games", _fake_save_schedule_games)
    monkeypatch.setattr(run_daily_update, "crawl_and_save_game_details", _fake_crawl_and_save_game_details)
    monkeypatch.setattr(
        run_daily_update,
        "refresh_game_status_for_date",
        lambda *_args, **_kwargs: {"total": 1, "updated": 0, "status_counts": {}},
    )
    monkeypatch.setattr(run_daily_update.asyncio, "to_thread", _noop_to_thread)
    monkeypatch.setattr(run_daily_update, "run_healer_async", _noop_async)
    monkeypatch.setattr(run_daily_update, "PlayerMovementCrawler", _FakeMovementCrawler)
    monkeypatch.setattr(run_daily_update, "DailyRosterCrawler", _FakeRosterCrawler)
    monkeypatch.setattr(run_daily_update, "write_refresh_manifest", lambda **_kwargs: "manifest.json")


def test_run_update_injects_resolver_marks_cancelled_and_uses_step_runner(monkeypatch):
    _FakeResolver.created = []
    updates = []
    commands: list[list[str]] = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerCancelled)
    monkeypatch.setattr(
        run_daily_update,
        "update_game_status",
        lambda game_id, status: updates.append((game_id, status)) or True,
    )

    asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=False,
            headless=True,
            limit=None,
            step_runner=lambda argv: commands.append(list(argv)),
            seed_tomorrow_preview=True,
        )
    )

    assert _FakeResolver.created
    assert _FakeResolver.created[0].preloaded_years == [2025]
    assert _FakeDetailCrawlerCancelled.received_resolver is _FakeResolver.created[0]
    assert ("20250101LGSS0", GAME_STATUS_CANCELLED) in updates
    assert ["scripts/fetch_kbo_pbp.py", "--date", "20250101"] in commands
    assert ["-m", "src.cli.daily_review_batch", "--date", "20250101", "--no-sync"] in commands
    assert ["-m", "src.cli.calculate_standings", "--year", "2025"] in commands
    assert ["-m", "src.cli.calculate_matchups", "--year", "2025"] in commands
    assert ["-m", "src.cli.calculate_rankings", "--year", "2025"] in commands
    assert ["-m", "src.cli.daily_preview_batch", "--date", "20250102", "--no-sync"] in commands


def test_run_update_can_skip_auto_healer_for_scoped_backfill(monkeypatch):
    healer_calls = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)

    async def _record_healer(*_args, **_kwargs):
        healer_calls.append(True)
        return 0

    monkeypatch.setattr(run_daily_update, "run_healer_async", _record_healer)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)

    asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=False,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
            run_auto_healer=False,
        )
    )

    assert healer_calls == []


def test_run_update_marks_unresolved_when_detail_missing_for_past_date(monkeypatch):
    _FakeResolver.created = []
    updates = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerMissing)
    monkeypatch.setattr(
        run_daily_update,
        "update_game_status",
        lambda game_id, status: updates.append((game_id, status)) or True,
    )

    asyncio.run(
        run_daily_update.run_update(
            "20200101",
            sync=False,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
        )
    )

    assert ("20200101LGSS0", GAME_STATUS_UNRESOLVED) in updates


def test_run_update_syncs_only_target_games_after_freshness_gate(monkeypatch):
    _FakeResolver.created = []
    _FakeSyncer.created = []
    commands: list[list[str]] = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "OCISync", _FakeSyncer)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setenv("OCI_DB_URL", "postgresql://example")

    asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=True,
            headless=True,
            limit=None,
            step_runner=lambda argv: commands.append(list(argv)),
        )
    )

    assert ["-m", "src.cli.daily_review_batch", "--date", "20250101", "--no-sync"] in commands
    assert ["-m", "src.cli.freshness_gate", "--date", "20250101"] in commands
    assert ["-m", "src.cli.quality_gate_check", "--year", "2025"] in commands
    gate_index = commands.index(["-m", "src.cli.freshness_gate", "--date", "20250101"])
    quality_index = commands.index(["-m", "src.cli.quality_gate_check", "--year", "2025"])
    standings_index = commands.index(["-m", "src.cli.calculate_standings", "--year", "2025"])
    assert standings_index < gate_index
    assert gate_index < quality_index

    assert len(_FakeSyncer.created) == 1
    syncer = _FakeSyncer.created[0]
    assert syncer.synced_games == ["20250101LGSS0"]
    assert ("standings", 2025) in syncer.calls
    assert ("matchups", 2025) in syncer.calls
    assert ("rankings", 2025) in syncer.calls
    assert ("season_batting", None) in syncer.calls
    assert ("season_pitching", None) in syncer.calls
    assert ("player_movements", None) in syncer.calls
    assert ("daily_rosters", None) in syncer.calls
    assert ("players", None) in syncer.calls
    assert syncer.closed is True
