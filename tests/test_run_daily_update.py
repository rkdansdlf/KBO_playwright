from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import src.cli.run_daily_update as run_daily_update
from src.utils import alerting as alerting_module
from src.repositories.game_repository import GAME_STATUS_CANCELLED, GAME_STATUS_UNRESOLVED
import scripts.verification.audit_game_logic as audit_game_logic_module


class _FakeSession:
    def query(self, *_args, **_kwargs):
        return _FakeQuery()

    def execute(self, *_args, **_kwargs):
        return _FakeExecuteResult()

    def close(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeQuery:
    def filter(self, *_args, **_kwargs):
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def all(self):
        return []

    def one_or_none(self):
        return None


class _FakeExecuteResult:
    def scalars(self):
        return self

    def all(self):
        return []


class _FakeResolver:
    created = []

    def __init__(self, session, **_kwargs):
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


class _FakeScheduleCrawlerCancelled:
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
                "game_status": "CANCELLED",
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


class _FakeDetailCrawlerIncomplete:
    def __init__(self, resolver=None, **_kwargs):
        self.resolver = resolver

    async def crawl_game(self, game_id: str, game_date: str):
        return None

    def get_last_failure_reason(self, game_id: str):
        return "incomplete_detail"


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


class _FakeRosterTransactionCrawler:
    async def run(self, save: bool = False, target_date: str | None = None):
        return []


class _FakeTeamEventCrawler:
    created = []

    def __init__(self, days_back: int = 30):
        self.days_back = days_back
        self.calls = []
        _FakeTeamEventCrawler.created.append(self)

    async def run(self, save: bool = False, team_filter: str | None = None):
        self.calls.append({"save": save, "team_filter": team_filter})
        return [{"title": "event-1"}, {"title": "event-2"}]


class _FakeTicketCrawler:
    created = []

    def __init__(self):
        self.calls = []
        _FakeTicketCrawler.created.append(self)

    async def run(self, save: bool = False, season: int | None = None):
        self.calls.append({"save": save, "season": season})
        return [{"seat_grade": "blue"}]


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
    **_kwargs,
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


async def _fake_reconcile_postgame_range(*_args, **_kwargs):
    return SimpleNamespace(candidates=0, changes=[], changed_game_ids=[])


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
        self.calls.append(("specific_game", game_id))
        self.synced_games.append(game_id)

    def sync_games(self, *, filters=None, batch_size=10000, limit=None):
        self.calls.append(("games", filters, batch_size, limit))
        return 0

    def sync_standings(self, *, year: int):
        self.calls.append(("standings", year))

    def sync_matchups(self, *, year: int):
        self.calls.append(("matchups", year))

    def sync_stat_rankings(self, *, year: int):
        self.calls.append(("rankings", year))

    def sync_player_season_batting(self, *, year=None):
        self.calls.append(("season_batting", year))

    def sync_player_season_pitching(self, *, year=None):
        self.calls.append(("season_pitching", year))

    def sync_player_movements(self):
        self.calls.append(("player_movements", None))

    def sync_daily_rosters(self, *, start_date=None, end_date=None):
        self.calls.append(("daily_rosters", {"start_date": start_date, "end_date": end_date}))

    def sync_player_basic(self):
        self.calls.append(("player_basic", None))

    def sync_players(self):
        self.calls.append(("players", None))

    def close(self):
        self.closed = True


class _FakeSyncerWithSkips(_FakeSyncer):
    def sync_specific_game(self, game_id: str):
        super().sync_specific_game(game_id)
        return {"skipped_empty_relay": 1}


class _FakeSyncerWithSupportingFailure(_FakeSyncer):
    def sync_player_season_batting(self, *, year=None):
        self.calls.append(("season_batting", year))
        raise RuntimeError("supporting sync failed")


def _patch_common(monkeypatch):
    _FakeTeamEventCrawler.created = []
    _FakeTicketCrawler.created = []
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
    monkeypatch.setattr(run_daily_update, "RosterTransactionCrawler", _FakeRosterTransactionCrawler)
    monkeypatch.setattr(run_daily_update, "TeamEventCrawler", _FakeTeamEventCrawler)
    monkeypatch.setattr(run_daily_update, "TicketCrawler", _FakeTicketCrawler)
    monkeypatch.setattr(
        run_daily_update,
        "DEFAULT_DAILY_SUMMARY_DIR",
        Path("/private/tmp/kbo_daily_update_summary_tests"),
    )
    monkeypatch.setattr(run_daily_update, "write_refresh_manifest", lambda **_kwargs: "manifest.json")
    monkeypatch.setattr(run_daily_update, "reconcile_postgame_range", _fake_reconcile_postgame_range)
    monkeypatch.setattr(run_daily_update, "format_reconciliation_report", lambda changes: "")
    monkeypatch.setitem(
        sys.modules,
        "scripts.verification.audit_game_logic",
        SimpleNamespace(audit_game_logic=lambda *_args, **_kwargs: []),
    )
    monkeypatch.setattr(
        run_daily_update,
        "build_p0_readiness",
        lambda *_args, **_kwargs: {
            "target_date": "test",
            "summary": {"ok": True, "failure_count": 0, "critical_failure_count": 0, "warning_count": 0},
        },
    )
    monkeypatch.setattr(run_daily_update, "format_p0_readiness_summary", lambda _payload: "ok")
    monkeypatch.setattr(run_daily_update, "_run_game_status_integrity_audit", lambda: None)
    monkeypatch.setattr(run_daily_update, "_run_oci_parity_quality_gate", lambda: {"ok": True, "failures": []})
    monkeypatch.setattr(alerting_module.TelegramBotClient, "send_message", staticmethod(lambda _message: False))
    monkeypatch.setattr(alerting_module.SlackWebhookClient, "send_alert", staticmethod(lambda *_args, **_kwargs: True))
    monkeypatch.setattr(
        alerting_module.SlackWebhookClient,
        "send_error_alert",
        staticmethod(lambda *_args, **_kwargs: True),
    )
    monkeypatch.setattr(audit_game_logic_module, "audit_game_logic", lambda **_kwargs: [])


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
    assert not any(command[:1] == ["scripts/fetch_kbo_pbp.py"] for command in commands)
    assert ["-m", "src.cli.daily_review_batch", "--date", "20250101", "--no-sync"] in commands
    assert ["-m", "src.cli.daily_story_batch", "--date", "20250101", "--no-sync"] in commands
    assert [
        "-m",
        "src.cli.backfill_starting_pitchers_from_stats",
        "--start-date",
        "20250101",
        "--end-date",
        "20250101",
    ] in commands
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


def test_run_update_marks_unresolved_when_detail_incomplete_for_past_date(monkeypatch):
    _FakeResolver.created = []
    updates = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerIncomplete)
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


def test_run_update_summarizes_detail_failure_reasons(monkeypatch, capsys):
    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerIncomplete)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)

    asyncio.run(
        run_daily_update.run_update(
            "20200101",
            sync=False,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
        )
    )

    output = capsys.readouterr().out
    assert "Detail failure reasons: incomplete_detail=1" in output
    assert "detail_failures=incomplete_detail=1" in output


def test_run_update_writes_detail_failure_summary_json_and_manifest(monkeypatch, tmp_path):
    manifest_kwargs = {}

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerIncomplete)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        run_daily_update,
        "write_refresh_manifest",
        lambda **kwargs: manifest_kwargs.update(kwargs) or tmp_path / "manifest.json",
    )

    result = asyncio.run(
        run_daily_update.run_update(
            "20200101",
            sync=False,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
            summary_dir=tmp_path,
        )
    )

    summary_path = tmp_path / "20200101.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    stability = payload["stability"]

    assert result["summary_path"] == str(summary_path)
    assert payload["manifest_path"] == str(tmp_path / "manifest.json")
    assert stability["detail"]["failure_counts"] == {"incomplete_detail": 1}
    assert stability["detail"]["failure_game_ids"] == {"incomplete_detail": ["20200101LGSS0"]}
    assert stability["retry_candidates"]["detail"] == ["20200101LGSS0"]
    assert stability["affected_game_ids"] == ["20200101LGSS0"]
    assert manifest_kwargs["stability"] == stability


def test_run_update_skips_cancelled_schedule_games_before_detail(monkeypatch):
    detail_batches = []
    updates = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "ScheduleCrawler", _FakeScheduleCrawlerCancelled)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerMissing)
    monkeypatch.setattr(
        run_daily_update,
        "update_game_status",
        lambda game_id, status: updates.append((game_id, status)) or True,
    )

    async def _capture_details(games, **kwargs):
        detail_batches.append(list(games))
        return await _fake_crawl_and_save_game_details(games, **kwargs)

    monkeypatch.setattr(run_daily_update, "crawl_and_save_game_details", _capture_details)

    asyncio.run(
        run_daily_update.run_update(
            "20200101",
            sync=False,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
        )
    )

    assert detail_batches == [[]]
    assert updates == []


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
    assert ["-m", "src.cli.daily_story_batch", "--date", "20250101", "--no-sync"] in commands
    assert any(cmd[:3] == ["scripts/fetch_kbo_pbp.py", "--game-ids", "20250101LGSS0"] for cmd in commands)
    backfill_command = [
        "-m",
        "src.cli.backfill_starting_pitchers_from_stats",
        "--start-date",
        "20250101",
        "--end-date",
        "20250101",
        "--sync",
    ]
    assert backfill_command in commands
    assert ["-m", "src.cli.freshness_gate", "--date", "20250101"] in commands
    assert [
        "-m",
        "src.cli.freshness_gate",
        "--date",
        "20250101",
        "--source-url-env",
        "OCI_DB_URL",
    ] in commands
    assert ["-m", "src.cli.quality_gate_check", "--year", "2025"] in commands
    backfill_index = commands.index(backfill_command)
    gate_index = commands.index(["-m", "src.cli.freshness_gate", "--date", "20250101"])
    oci_gate_index = commands.index(
        [
            "-m",
            "src.cli.freshness_gate",
            "--date",
            "20250101",
            "--source-url-env",
            "OCI_DB_URL",
        ]
    )
    quality_index = commands.index(["-m", "src.cli.quality_gate_check", "--year", "2025"])
    standings_index = commands.index(["-m", "src.cli.calculate_standings", "--year", "2025"])
    assert backfill_index < standings_index
    assert standings_index < gate_index
    assert gate_index < quality_index
    assert quality_index < oci_gate_index

    assert len(_FakeSyncer.created) == 1
    syncer = _FakeSyncer.created[0]
    assert syncer.synced_games == ["20250101LGSS0"]
    assert syncer.calls[:2] == [("player_basic", None), ("players", None)]
    call_names = [call[0] for call in syncer.calls]
    assert call_names.index("players") < call_names.index("specific_game")
    assert call_names.index("specific_game") < call_names.index("games")
    assert ("standings", 2025) in syncer.calls
    assert ("matchups", 2025) in syncer.calls
    assert ("rankings", 2025) in syncer.calls
    assert ("season_batting", 2025) in syncer.calls
    assert ("season_pitching", 2025) in syncer.calls
    assert ("player_movements", None) in syncer.calls
    assert ("daily_rosters", {"start_date": "2025-01-01", "end_date": "2025-01-01"}) in syncer.calls
    assert ("player_basic", None) in syncer.calls
    assert ("players", None) in syncer.calls
    assert syncer.closed is True


def test_run_update_records_non_p0_supporting_sync_failure(monkeypatch):
    _FakeSyncer.created = []
    commands: list[list[str]] = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "OCISync", _FakeSyncerWithSupportingFailure)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setenv("OCI_DB_URL", "postgresql://example")

    result = asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=True,
            headless=True,
            limit=None,
            step_runner=lambda argv: commands.append(list(argv)),
        )
    )

    assert result["stability"]["oci"]["skip_counts"] == {"non_p0_supporting_sync_failed": 1}
    assert result["stability"]["oci"]["skip_game_ids"] == {"non_p0_supporting_sync_failed": ["season:2025"]}
    assert _FakeSyncer.created[0].closed is True


def test_run_update_can_skip_oci_supporting_sync(monkeypatch):
    _FakeSyncer.created = []
    commands: list[list[str]] = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "OCISync", _FakeSyncer)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setenv("OCI_DB_URL", "postgresql://example")

    result = asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=True,
            headless=True,
            limit=None,
            step_runner=lambda argv: commands.append(list(argv)),
            skip_oci_supporting_sync=True,
        )
    )

    syncer = _FakeSyncer.created[0]
    call_names = [call[0] for call in syncer.calls]
    assert "specific_game" in call_names
    assert "games" not in call_names
    assert "daily_rosters" not in call_names
    assert result["stability"]["oci"]["skip_counts"] == {"oci_supporting_sync_skipped": 1}
    assert result["stability"]["oci"]["skip_game_ids"] == {"oci_supporting_sync_skipped": ["season:2025"]}


def test_run_update_can_skip_season_stats(monkeypatch, capsys):
    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        run_daily_update,
        "crawl_series_batting_stats",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("batting stats should be skipped")),
    )
    monkeypatch.setattr(
        run_daily_update,
        "crawl_pitcher_series",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("pitching stats should be skipped")),
    )

    asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=False,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
            skip_season_stats=True,
        )
    )

    assert "Season stats update skipped by operator flag" in capsys.readouterr().out


def test_run_update_proactive_recovery_includes_missing_wpa_games(monkeypatch):
    class _ProactiveExecuteResult:
        def scalars(self):
            return self

        def all(self):
            return ["20241231LGSS0", "20250101LGSS0"]

    class _ProactiveSession(_FakeSession):
        def execute(self, *_args, **_kwargs):
            return _ProactiveExecuteResult()

    _FakeSyncer.created = []
    commands: list[list[str]] = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "SessionLocal", lambda: _ProactiveSession())
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

    assert any(cmd[:3] == ["scripts/fetch_kbo_pbp.py", "--game-ids", "20250101LGSS0"] for cmd in commands)
    assert any(cmd[:3] == ["scripts/fetch_kbo_pbp.py", "--game-ids", "20241231LGSS0"] for cmd in commands)
    assert len(_FakeSyncer.created) == 1
    syncer = _FakeSyncer.created[0]
    assert syncer.synced_games == ["20241231LGSS0", "20250101LGSS0"]


def test_run_update_prints_oci_skip_and_relay_summary(monkeypatch, capsys):
    _FakeResolver.created = []
    _FakeSyncer.created = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "OCISync", _FakeSyncerWithSkips)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setenv("OCI_DB_URL", "postgresql://example")

    asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=True,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
        )
    )

    output = capsys.readouterr().out
    assert "Relay candidates=1" in output
    assert "OCI skip summary: skipped_empty_relay=1" in output
    assert "Stability summary:" in output
    assert "relay_targets=1" in output
    assert "oci_skips=skipped_empty_relay=1" in output


def test_run_update_writes_oci_skip_summary_json(monkeypatch, tmp_path):
    manifest_kwargs = {}
    _FakeResolver.created = []
    _FakeSyncer.created = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "OCISync", _FakeSyncerWithSkips)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        run_daily_update,
        "write_refresh_manifest",
        lambda **kwargs: manifest_kwargs.update(kwargs) or tmp_path / "manifest.json",
    )
    monkeypatch.setenv("OCI_DB_URL", "postgresql://example")

    result = asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=True,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
            summary_dir=tmp_path,
        )
    )

    payload = json.loads((tmp_path / "20250101.json").read_text(encoding="utf-8"))
    stability = payload["stability"]

    assert result["stability"] == stability
    assert stability["detail"]["failure_counts"] == {}
    assert stability["relay"]["target_count"] == 1
    assert stability["relay"]["target_game_ids"] == ["20250101LGSS0"]
    assert stability["oci"]["skip_counts"] == {"skipped_empty_relay": 1}
    assert stability["oci"]["skip_game_ids"] == {"skipped_empty_relay": ["20250101LGSS0"]}
    assert stability["retry_candidates"]["relay"] == ["20250101LGSS0"]
    assert stability["affected_game_ids"] == ["20250101LGSS0"]
    assert manifest_kwargs["stability"] == stability


def test_run_update_writes_empty_stability_summary_for_clean_run(monkeypatch, tmp_path):
    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        run_daily_update,
        "write_refresh_manifest",
        lambda **_kwargs: tmp_path / "manifest.json",
    )

    asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=False,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
            summary_dir=tmp_path,
        )
    )

    stability = json.loads((tmp_path / "20250101.json").read_text(encoding="utf-8"))["stability"]

    assert stability["detail"]["failure_counts"] == {}
    assert stability["detail"]["failure_game_ids"] == {}
    assert stability["oci"]["skip_counts"] == {}
    assert stability["quality_gates"] == {"non_p0_failure_counts": {}, "non_p0_failure_ids": {}}
    assert stability["p0_non_game"] == {
        "counts": {"roster_transactions": 0, "team_events": 2, "ticket_prices": 1},
        "errors": {},
    }
    assert stability["retry_candidates"] == {"detail": [], "relay": []}
    assert stability["affected_game_ids"] == []


def test_run_update_runs_p0_non_game_crawlers_and_records_manifest(monkeypatch, tmp_path):
    manifest_kwargs = {}

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        run_daily_update,
        "write_refresh_manifest",
        lambda **kwargs: manifest_kwargs.update(kwargs) or tmp_path / "manifest.json",
    )

    result = asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=False,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
            summary_dir=tmp_path,
        )
    )

    assert len(_FakeTeamEventCrawler.created) == 1
    assert _FakeTeamEventCrawler.created[0].days_back == 3
    assert _FakeTeamEventCrawler.created[0].calls == [{"save": True, "team_filter": None}]
    assert len(_FakeTicketCrawler.created) == 1
    assert _FakeTicketCrawler.created[0].calls == [{"save": True, "season": 2025}]
    assert result["stability"]["p0_non_game"] == {
        "counts": {"roster_transactions": 0, "team_events": 2, "ticket_prices": 1},
        "errors": {},
    }
    assert "team_events" in manifest_kwargs["datasets"]
    assert "ticket_prices" in manifest_kwargs["datasets"]
    assert "ticket_open_rules" in manifest_kwargs["datasets"]
    assert "roster_transactions" in manifest_kwargs["datasets"]


def test_run_update_records_p0_non_game_errors_without_aborting(monkeypatch, tmp_path):
    class _FailingTeamEventCrawler:
        def __init__(self, days_back: int = 30):
            self.days_back = days_back

        async def run(self, save: bool = False, team_filter: str | None = None):
            raise RuntimeError("event source down")

    class _FailingTicketCrawler:
        async def run(self, save: bool = False, season: int | None = None):
            raise RuntimeError("ticket source down")

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "TeamEventCrawler", _FailingTeamEventCrawler)
    monkeypatch.setattr(run_daily_update, "TicketCrawler", _FailingTicketCrawler)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)

    result = asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=False,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
            summary_dir=tmp_path,
        )
    )

    assert result["phase"] == "postgame_finalize"
    assert result["stability"]["p0_non_game"]["counts"] == {"roster_transactions": 0}
    assert result["stability"]["p0_non_game"]["errors"] == {
        "team_events": "event source down",
        "ticket_prices": "ticket source down",
    }


def test_run_update_can_skip_p0_non_game_crawlers(monkeypatch, tmp_path):
    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)

    result = asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=False,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
            summary_dir=tmp_path,
            run_p0_non_game=False,
        )
    )

    assert _FakeTeamEventCrawler.created == []
    assert _FakeTicketCrawler.created == []
    assert result["stability"]["p0_non_game"] == {
        "counts": {"roster_transactions": 0, "skipped": 1},
        "errors": {},
    }


def test_run_update_records_non_p0_quality_gate_failures(monkeypatch):
    _FakeSyncer.created = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "OCISync", _FakeSyncer)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setenv("OCI_DB_URL", "postgresql://example")

    def _runner(argv):
        command = list(argv)
        if command[:3] == ["-m", "src.cli.quality_gate_check", "--year"]:
            raise run_daily_update.subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(
        run_daily_update,
        "_run_oci_parity_quality_gate",
        lambda: (_ for _ in ()).throw(RuntimeError("oci parity failed")),
    )

    result = asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=True,
            headless=True,
            limit=None,
            step_runner=_runner,
        )
    )

    assert result["stability"]["quality_gates"] == {
        "non_p0_failure_counts": {
            "non_p0_oci_parity_quality_gate_failed": 1,
            "non_p0_statistical_quality_gate_failed": 1,
        },
        "non_p0_failure_ids": {
            "non_p0_oci_parity_quality_gate_failed": ["oci"],
            "non_p0_statistical_quality_gate_failed": ["season:2025"],
        },
    }


def test_run_update_syncs_auto_healer_recovery_targets(monkeypatch):
    _FakeResolver.created = []
    _FakeSyncer.created = []
    commands: list[list[str]] = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "OCISync", _FakeSyncer)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        run_daily_update,
        "_collect_past_scheduled_recovery_targets",
        lambda _today: [{"game_id": "20241231LGSS0", "game_date": "20241231"}],
    )
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

    assert any(cmd[:3] == ["scripts/fetch_kbo_pbp.py", "--game-ids", "20250101LGSS0"] for cmd in commands)
    assert any(cmd[:3] == ["scripts/fetch_kbo_pbp.py", "--game-ids", "20241231LGSS0"] for cmd in commands)
    assert ["-m", "src.cli.freshness_gate", "--date", "20250101"] in commands
    assert ["-m", "src.cli.freshness_gate", "--date", "20241231"] in commands
    assert [
        "-m",
        "src.cli.freshness_gate",
        "--date",
        "20250101",
        "--source-url-env",
        "OCI_DB_URL",
    ] in commands
    assert [
        "-m",
        "src.cli.freshness_gate",
        "--date",
        "20241231",
        "--source-url-env",
        "OCI_DB_URL",
    ] in commands

    assert len(_FakeSyncer.created) == 1
    syncer = _FakeSyncer.created[0]
    assert syncer.synced_games == ["20241231LGSS0", "20250101LGSS0"]


def test_run_update_syncs_games_seen_by_status_refresh(monkeypatch):
    _FakeResolver.created = []
    _FakeSyncer.created = []

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "OCISync", _FakeSyncer)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        run_daily_update,
        "refresh_game_status_for_date",
        lambda *_args, **_kwargs: {
            "total": 2,
            "updated": 1,
            "status_counts": {"COMPLETED": 2},
            "game_ids": ["20250101LGSS0", "20250101HHKT0"],
        },
    )
    monkeypatch.setenv("OCI_DB_URL", "postgresql://example")

    asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=True,
            headless=True,
            limit=None,
            step_runner=lambda _argv: None,
        )
    )

    assert len(_FakeSyncer.created) == 1
    syncer = _FakeSyncer.created[0]
    assert syncer.synced_games == ["20250101HHKT0", "20250101LGSS0"]


def test_run_update_syncs_postgame_reconciliation_changes(monkeypatch):
    _FakeResolver.created = []
    _FakeSyncer.created = []
    commands: list[list[str]] = []
    seen: dict[str, object] = {}

    _patch_common(monkeypatch)
    monkeypatch.setattr(run_daily_update, "GameDetailCrawler", _FakeDetailCrawlerSuccess)
    monkeypatch.setattr(run_daily_update, "OCISync", _FakeSyncer)
    monkeypatch.setattr(run_daily_update, "update_game_status", lambda *_args, **_kwargs: True)
    monkeypatch.setenv("OCI_DB_URL", "postgresql://example")

    async def _fake_reconcile(start_date, end_date, *, detail_crawler, concurrency, log, **_kwargs):
        seen["range"] = (start_date, end_date)
        seen["crawler"] = detail_crawler
        seen["concurrency"] = concurrency
        return SimpleNamespace(
            candidates=1,
            changes=[SimpleNamespace(game_id="20241230LGSS0", game_date="20241230")],
            changed_game_ids=["20241230LGSS0"],
        )

    monkeypatch.setattr(run_daily_update, "reconcile_postgame_range", _fake_reconcile)
    monkeypatch.setattr(run_daily_update, "format_reconciliation_report", lambda _changes: "report")

    asyncio.run(
        run_daily_update.run_update(
            "20250101",
            sync=True,
            headless=True,
            limit=None,
            step_runner=lambda argv: commands.append(list(argv)),
            postgame_reconcile_lookback_days=2,
        )
    )

    assert seen["range"] == ("20241230", "20250101")
    assert seen["crawler"].__class__ is _FakeDetailCrawlerSuccess
    assert seen["concurrency"] == 1
    assert any(cmd[:3] == ["scripts/fetch_kbo_pbp.py", "--game-ids", "20241230LGSS0,20250101LGSS0"] for cmd in commands)
    assert ["-m", "src.cli.freshness_gate", "--date", "20241230"] in commands
    assert [
        "-m",
        "src.cli.freshness_gate",
        "--date",
        "20241230",
        "--source-url-env",
        "OCI_DB_URL",
    ] in commands
    assert len(_FakeSyncer.created) == 1
    syncer = _FakeSyncer.created[0]
    assert syncer.synced_games == ["20241230LGSS0", "20250101LGSS0"]
