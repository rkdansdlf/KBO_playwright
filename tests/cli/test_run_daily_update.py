from __future__ import annotations

import asyncio
import subprocess
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.cli.run_daily_update as daily
from src.cli.run_daily_update import DailyUpdateOptions, main
from src.models.game import Game
from src.services.game_write_contract import GameWriteContract
from src.services.recovery_manager import RecoveryManager
from src.utils.game_status import GAME_STATUS_CANCELLED, GAME_STATUS_SCHEDULED, GAME_STATUS_UNRESOLVED


import pytest

pytestmark = pytest.mark.integration


class TestRunDailyUpdateCLI:
    def test_main_default_date(self):
        with (
            patch("src.cli.run_daily_update.run_update", new_callable=AsyncMock) as mock_update,
            patch("src.cli.run_daily_update.datetime") as mock_dt,
        ):
            mock_now = MagicMock()
            mock_now.strftime.return_value = "20251014"
            mock_dt.now.return_value = mock_now
            mock_dt.timedelta = MagicMock(return_value=MagicMock())
            mock_dt.strptime = __import__("datetime").datetime.strptime

            result = main([])
            assert result == mock_update.return_value
            mock_update.assert_called_once()

    def test_main_with_date(self):
        with patch("src.cli.run_daily_update.run_update", new_callable=AsyncMock) as mock_update:
            result = main(["--date", "20251015"])
            assert result == mock_update.return_value
            mock_update.assert_called_once_with(
                "20251015",
                DailyUpdateOptions(),
            )

    def test_main_with_sync(self):
        with patch("src.cli.run_daily_update.run_update", new_callable=AsyncMock) as mock_update:
            main(["--date", "20251015", "--sync"])
            mock_update.assert_called_once_with(
                "20251015",
                DailyUpdateOptions(sync=True),
            )


def _build_run_context(tmp_path, *, target_date: str, today_kst: date) -> daily._RunContext:
    return daily._RunContext(
        target_date=target_date,
        sync=False,
        year=int(target_date[:4]),
        month=int(target_date[4:6]),
        today_kst=today_kst,
        runner=lambda _args: None,
        write_contract=GameWriteContract(run_label=f"test:{target_date}", log=lambda _message: None),
        detail_recovery_queue=RecoveryManager(str(tmp_path / "detail_recovery_queue.json")),
    )


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_detail_recovery_escalates_repeated_recoverable_failures(monkeypatch, tmp_path):
    game_id = "20260607LGSS0"
    ctx = _build_run_context(tmp_path, target_date="20260607", today_kst=date(2026, 6, 7))
    ctx.detail_games_by_id[game_id] = {"game_id": game_id, "game_date": "20260607"}
    ctx.detail_recovery_attempts[game_id] = 0
    calls = []
    alerts = []

    async def _fake_crawl(games, **_kwargs):
        game_list = list(games)
        calls.append(game_list)
        return SimpleNamespace(
            items={
                game_id: SimpleNamespace(
                    detail_saved=False,
                    detail_status="crawl_failed",
                    failure_reason="incomplete_detail",
                ),
            },
        )

    monkeypatch.setattr(daily, "DETAIL_RECOVERY_MAX_ROUNDS", 3)
    monkeypatch.setattr(daily, "DETAIL_RECOVERY_RETRY_ALERT_THRESHOLD", 2)
    monkeypatch.setattr(daily, "crawl_and_save_game_details", _fake_crawl)
    monkeypatch.setattr(daily.SlackWebhookClient, "send_alert", lambda message, **_kwargs: alerts.append(message))

    detail_results = asyncio.run(daily._collect_detail_results(ctx, object()))
    daily._finalize_detail_results(ctx, detail_results, set())

    assert len(calls) == 3
    assert [call[0]["game_id"] for call in calls] == [game_id, game_id, game_id]
    assert ctx.detail_recovery_passes == 2
    assert ctx.detail_recovery_attempts[game_id] == 3
    assert ctx.detail_still_missing == {game_id}
    assert ctx.detail_retry_escalation_game_ids == [game_id]
    assert ctx.detail_failure_counts == {"incomplete_detail": 1}
    assert ctx.detail_failure_game_ids == {"incomplete_detail": [game_id]}
    queue_entry = ctx.detail_recovery_queue.state["detail_recovery_queue"][f"20260607:{game_id}"]
    assert queue_entry["reason"] == "incomplete_detail"
    assert queue_entry["attempts"] == 1
    assert alerts and game_id in alerts[0]


def test_detail_step_exception_preserves_cancelled_and_tracks_queued_targets(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(daily, "SessionLocal", SessionLocal)
    updates = []

    def _fake_update_game_status(game_id: str, status: str) -> bool:
        updates.append((game_id, status))
        with SessionLocal() as session:
            game = session.query(Game).filter(Game.game_id == game_id).one()
            game.game_status = status
            session.commit()
        return True

    monkeypatch.setattr(daily, "update_game_status", _fake_update_game_status)

    with SessionLocal() as session:
        session.add_all(
            [
                Game(
                    game_id="20260527SSLG0",
                    game_date=date(2026, 5, 27),
                    game_status=GAME_STATUS_CANCELLED,
                ),
                Game(
                    game_id="20260527HHNC0",
                    game_date=date(2026, 5, 27),
                    game_status=GAME_STATUS_SCHEDULED,
                ),
            ],
        )
        session.commit()

    ctx = _build_run_context(tmp_path, target_date="20260527", today_kst=date(2026, 6, 12))
    ctx.detail_games = [{"game_id": "20260527SSLG0", "game_date": "20260527"}]
    ctx.detail_games_by_id = {
        "20260527SSLG0": {"game_id": "20260527SSLG0", "game_date": "20260527"},
        "20260527HHNC0": {"game_id": "20260527HHNC0", "game_date": "20260527"},
    }

    daily._handle_detail_step_exception(ctx)

    assert ctx.detail_failure_counts == {"exception": 2}
    assert ctx.detail_failure_game_ids == {"exception": ["20260527HHNC0", "20260527SSLG0"]}
    assert ctx.detail_still_missing == {"20260527HHNC0", "20260527SSLG0"}
    assert updates == [("20260527HHNC0", GAME_STATUS_UNRESOLVED)]
    with SessionLocal() as session:
        cancelled = session.query(Game).filter(Game.game_id == "20260527SSLG0").one()
        queued = session.query(Game).filter(Game.game_id == "20260527HHNC0").one()
        assert cancelled.game_status == GAME_STATUS_CANCELLED
        assert queued.game_status == GAME_STATUS_UNRESOLVED


def test_recalculate_season_aggregates_runs_player_then_team_recalc(tmp_path):
    calls = []
    ctx = _build_run_context(tmp_path, target_date="20260612", today_kst=date(2026, 6, 12))
    ctx.runner = lambda args: calls.append(args)

    daily._recalculate_season_aggregates_for_quality_gate(ctx)

    assert calls == [
        ["-m", "src.cli.recalc_player_stats", "--season", "2026"],
        ["-m", "src.cli.recalc_team_stats", "--season", "2026"],
    ]


def test_resolve_null_player_ids_before_quality_gate_runs_conservative_resolver(tmp_path):
    calls = []
    ctx = _build_run_context(tmp_path, target_date="20260612", today_kst=date(2026, 6, 12))
    ctx.runner = lambda args: calls.append(args)

    daily._resolve_null_player_ids_before_quality_gate(ctx)

    assert calls == [
        [
            "-m",
            "scripts.maintenance.resolve_null_player_ids_conservative",
            "--years",
            "2026",
            "--apply",
            "--no-backup",
            "--delete-duplicates",
        ],
    ]


def test_resolve_null_player_ids_before_quality_gate_records_failure(tmp_path):
    ctx = _build_run_context(tmp_path, target_date="20260612", today_kst=date(2026, 6, 12))

    def _fail(args):
        raise subprocess.CalledProcessError(1, args)

    ctx.runner = _fail

    daily._resolve_null_player_ids_before_quality_gate(ctx)

    assert ctx.non_p0_quality_gate_counts == {"non_p0_null_player_id_resolution_failed": 1}
    assert ctx.non_p0_quality_gate_ids == {"non_p0_null_player_id_resolution_failed": ["season:2026"]}
